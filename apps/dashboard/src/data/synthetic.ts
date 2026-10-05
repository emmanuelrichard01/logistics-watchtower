// Seeded synthetic fleet for console development (SYNTHETIC: no real trucks,
// shipments or values). Replaced by simulator recordings (apps/dashboard-fixtures)
// and later by the API stream; all three produce the same Timeline contract.

import { CORRIDORS, inDeadZone, positionAt } from '../domain/corridors'
import { assessRisk } from '../domain/risk'
import type {
  Aspect,
  CargoProfile,
  Frame,
  Incident,
  IncidentType,
  ProbeStatus,
  SeriesPoint,
  Severity,
  Shipment,
  Timeline,
  VehicleState,
} from '../domain/types'

const STEP_MS = 15_000
const DT_MIN = STEP_MS / 60_000
const START = Date.UTC(2026, 9, 5, 7, 30) // 08:30 WAT
const STEPS = 3 * 60 * 4 // three hours
const PRE_ROLL = 60 * 4 // one hour settles every unit at steady state before the timeline starts

function mulberry32(seed: number) {
  let a = seed >>> 0
  return () => {
    a = (a + 0x6d2b79f5) >>> 0
    let t = a
    t = Math.imul(t ^ (t >>> 15), t | 1)
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61)
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}

const PROFILES: Record<string, CargoProfile> = {
  frozenFish: { name: 'Frozen fish', setpointC: -20, minC: -25, maxC: -15, valuePerShipmentNgn: 18_500_000 },
  vaccines: { name: 'Vaccines', setpointC: 5, minC: 2, maxC: 8, valuePerShipmentNgn: 62_000_000 },
  bananas: { name: 'Bananas', setpointC: 13, minC: 12, maxC: 14.5, valuePerShipmentNgn: 7_200_000 },
  iceCream: { name: 'Ice cream', setpointC: -22, minC: -28, maxC: -18, valuePerShipmentNgn: 11_000_000 },
  produce: { name: 'Fresh produce', setpointC: 4, minC: 0, maxC: 8, valuePerShipmentNgn: 5_400_000 },
  chicken: { name: 'Frozen chicken', setpointC: -18, minC: -24, maxC: -12, valuePerShipmentNgn: 21_000_000 },
}

type Script = {
  degradeFromMin?: number // compressor health falls linearly to 0.15 over 60 min
  hardFailAtMin?: number
  stuckProbeFromMin?: number
  doorOpenAtMin?: [number, number]
  defrostAtMin?: number
}

interface Spec {
  vehicleId: string
  corridorId: string
  startFrac: number
  speed: number
  profile: CargoProfile
  script: Script
}

const FLEET: Spec[] = [
  { vehicleId: 'TRK-101', corridorId: 'RT-LAG-ABJ', startFrac: 0.42, speed: 68, profile: PROFILES.frozenFish, script: { degradeFromMin: 35 } },
  { vehicleId: 'TRK-102', corridorId: 'RT-LAG-ABJ', startFrac: 0.08, speed: 52, profile: PROFILES.chicken, script: {} },
  { vehicleId: 'TRK-103', corridorId: 'RT-LAG-ABJ', startFrac: 0.27, speed: 71, profile: PROFILES.produce, script: {} },
  { vehicleId: 'TRK-110', corridorId: 'RT-LAG-ABJ', startFrac: 0.15, speed: 63, profile: PROFILES.iceCream, script: { doorOpenAtMin: [96, 103] } },
  { vehicleId: 'TRK-111', corridorId: 'RT-LAG-ABJ', startFrac: 0.66, speed: 74, profile: PROFILES.frozenFish, script: {} },
  { vehicleId: 'TRK-104', corridorId: 'RT-PHC-MKD', startFrac: 0.2, speed: 58, profile: PROFILES.vaccines, script: { hardFailAtMin: 70 } },
  { vehicleId: 'TRK-105', corridorId: 'RT-PHC-MKD', startFrac: 0.55, speed: 61, profile: PROFILES.produce, script: {} },
  { vehicleId: 'TRK-112', corridorId: 'RT-PHC-MKD', startFrac: 0.05, speed: 55, profile: PROFILES.chicken, script: { defrostAtMin: 62 } },
  { vehicleId: 'TRK-106', corridorId: 'RT-PHC-MKD', startFrac: 0.74, speed: 66, profile: PROFILES.vaccines, script: {} },
  { vehicleId: 'TRK-107', corridorId: 'RT-BEN-ABJ', startFrac: 0.18, speed: 60, profile: PROFILES.bananas, script: { stuckProbeFromMin: 55 } },
  { vehicleId: 'TRK-108', corridorId: 'RT-BEN-ABJ', startFrac: 0.38, speed: 70, profile: PROFILES.frozenFish, script: {} },
  { vehicleId: 'TRK-109', corridorId: 'RT-BEN-ABJ', startFrac: 0.03, speed: 49, profile: PROFILES.iceCream, script: {} },
  { vehicleId: 'TRK-113', corridorId: 'RT-BEN-ABJ', startFrac: 0.6, speed: 72, profile: PROFILES.produce, script: {} },
  { vehicleId: 'TRK-114', corridorId: 'RT-LAG-ABJ', startFrac: 0.85, speed: 57, profile: PROFILES.chicken, script: {} },
]

interface Truth extends SeriesPoint {
  receivedAt: number
  km: number
  speed: number
  compressor: VehicleState['compressor']
  cargoProbe: ProbeStatus
}

function ambientC(tMs: number): number {
  const hourWat = ((tMs / 3_600_000 + 1) % 24 + 24) % 24
  return 30 + 5 * Math.sin(((hourWat - 9) / 24) * 2 * Math.PI)
}

function simulateTruck(spec: Spec, seed: number): Truth[] {
  const rnd = mulberry32(seed)
  const noise = (s: number) => (rnd() - 0.5) * 2 * s
  const corridor = CORRIDORS.find((c) => c.id === spec.corridorId)!
  const sp = spec.profile.setpointC
  let ta = sp + noise(0.4)
  let tc = sp + noise(0.3)
  let cooling = true
  let km = spec.startFrac * corridor.lengthKm
  let speed = spec.speed
  let outageStartIdx: number | null = null
  const out: Truth[] = []

  for (let i = -PRE_ROLL; i < STEPS; i++) {
    const t = START + i * STEP_MS
    const min = i * DT_MIN
    const s = spec.script
    let h = 1
    if (s.degradeFromMin !== undefined && min > s.degradeFromMin) h = Math.max(0.15, 1 - ((min - s.degradeFromMin) / 60) * 0.85)
    const fault = s.hardFailAtMin !== undefined && min >= s.hardFailAtMin
    if (fault) h = 0
    const defrost = s.defrostAtMin !== undefined && min >= s.defrostAtMin && min < s.defrostAtMin + 12
    const doorOpen = s.doorOpenAtMin !== undefined && min >= s.doorOpenAtMin[0] && min < s.doorOpenAtMin[1]

    // Two-node thermal model (illustrative coefficients, per minute).
    if (ta > sp + 0.6) cooling = true
    else if (ta < sp - 0.6) cooling = false
    const amb = ambientC(t)
    const cool = defrost ? -0.35 : cooling ? 2.0 * h : 0
    const doorHeat = doorOpen ? 0.06 * (amb - ta) : 0
    ta += (0.02 * (amb - ta) + 0.05 * (tc - ta) - cool + doorHeat) * DT_MIN
    tc += 0.012 * (ta - tc) * DT_MIN

    // Movement with gentle speed variation; trucks hold at the line's end.
    if (i < 0) continue // pre-roll: thermal state only
    speed = Math.max(0, spec.speed + noise(6) + (doorOpen ? -10 : 0))
    km = Math.min(corridor.lengthKm, km + (speed / 60) * DT_MIN)
    if (km >= corridor.lengthKm) speed = 0

    const zone = inDeadZone(corridor, km)
    if (zone && outageStartIdx === null) outageStartIdx = i
    if (!zone && outageStartIdx !== null) {
      // Coverage is back: the device replays its buffer now.
      for (let j = outageStartIdx; j < i; j++) out[j].receivedAt = t
      outageStartIdx = null
    }

    const stuck = s.stuckProbeFromMin !== undefined && min >= s.stuckProbeFromMin
    out.push({
      t,
      receivedAt: zone ? Number.POSITIVE_INFINITY : t,
      km,
      speed,
      cargoC: stuck ? sp + 0.1 : tc + noise(0.05),
      returnAirC: ta + noise(0.15),
      supplyAirC: ta - (cooling && !defrost ? 2.2 * h : 0) + noise(0.15),
      door: doorOpen,
      defrost,
      gap: false,
      compressor: fault ? 'FAULT' : cooling && !defrost ? 'RUNNING' : 'OFF',
      cargoProbe: stuck && min >= s.stuckProbeFromMin! + 12 ? 'suspect' : 'ok',
    })
  }
  return out
}

const SEVERITY_FOR: Record<Aspect, Severity | null> = {
  danger: 'CRITICAL',
  caution1: 'CRITICAL',
  caution2: 'HIGH',
  unknown: null,
  clear: null,
}

const ACTIONS: Record<IncidentType, string[]> = {
  CARGO_TEMP_BREACH: ['Divert to nearest cold store', 'Call driver', 'Transfer cargo'],
  BREACH_FORECAST: ['Call driver', 'Switch to genset power', 'Divert to nearest cold store'],
  DOOR_OPEN_MOVING: ['Call driver', 'Check door seal'],
  SENSOR_FAULT: ['Schedule probe check', 'Call driver to read the unit display'],
  TELEMETRY_GAP: ['Wait for coverage', 'Call driver'],
  COMPRESSOR_FAULT: ['Call driver', 'Switch to genset power', 'Divert to nearest cold store'],
}

export function buildSyntheticTimeline(seed = 20261005): Timeline {
  const truths = new Map(FLEET.map((spec, i) => [spec.vehicleId, simulateTruck(spec, seed + i * 7919)]))
  const frames: Frame[] = []
  const markers: Timeline['markers'] = []
  const live = new Map<string, Incident>() // dedup key -> incident
  const quietSince = new Map<string, number>()
  const lastAspect = new Map<string, Aspect>()

  for (let i = 0; i < STEPS; i++) {
    const t = START + i * STEP_MS
    const vehicles: VehicleState[] = []
    const shipments: Shipment[] = []
    const active = new Map<string, { type: IncidentType; severity: Severity; summary: string; since: number; vehicleId: string; shipmentId: string }>()

    FLEET.forEach((spec, n) => {
      const truth = truths.get(spec.vehicleId)!
      const corridor = CORRIDORS.find((c) => c.id === spec.corridorId)!
      // What the console knows at time t: the newest reading received by then.
      let k = i
      while (k > 0 && truth[k].receivedAt > t) k--
      const known = truth[k]
      const ageS = (t - known.t) / 1000
      const estimated = ageS > 0
      const km = estimated ? Math.min(corridor.lengthKm, known.km + (known.speed / 3600) * ageS) : known.km
      const pos = positionAt(corridor, km)
      const vehicle: VehicleState = {
        vehicleId: spec.vehicleId,
        corridorId: spec.corridorId,
        km,
        lat: pos.lat,
        lon: pos.lon,
        speedKmh: known.speed,
        headingDeg: pos.headingDeg,
        cargoC: known.cargoC,
        returnAirC: known.returnAirC,
        supplyAirC: known.supplyAirC,
        setpointC: spec.profile.setpointC,
        door: known.door ? 'OPEN' : 'CLOSED',
        compressor: known.compressor,
        defrost: known.defrost,
        lastFixAgeS: ageS,
        estimated,
        probes: { cargo: known.cargoProbe, returnAir: 'ok', supplyAir: 'ok' },
      }
      const recent = truth
        .slice(Math.max(0, k - 80), k + 1)
        .filter((p) => p.receivedAt <= t && p.cargoC !== null)
        .map((p) => ({ tMin: (p.t - START) / 60_000, c: p.cargoC as number }))
      const remainingKm = corridor.lengthKm - km
      let lastDefrost = -1
      for (let j = k; j >= Math.max(0, k - 120); j--) if (truth[j].defrost && truth[j].receivedAt <= t) { lastDefrost = j; break }
      const minutesSinceDefrost = lastDefrost < 0 ? Number.POSITIVE_INFINITY : (known.t - truth[lastDefrost].t) / 60_000
      const risk = assessRisk(vehicle, spec.profile, recent, (remainingKm / Math.max(30, spec.speed)) * 60, minutesSinceDefrost)
      const shipmentId = `SHP-${24100 + n * 37}`
      vehicles.push(vehicle)
      shipments.push({ id: shipmentId, vehicleId: spec.vehicleId, cargo: spec.profile, destination: corridor.stations.at(-1)!.name, risk })

      if (lastAspect.get(spec.vehicleId) !== risk.aspect && risk.aspect !== 'clear') {
        markers.push({ t, aspect: risk.aspect, vehicleId: spec.vehicleId })
      }
      lastAspect.set(spec.vehicleId, risk.aspect)

      const sev = SEVERITY_FOR[risk.aspect]
      const range = risk.ttbP10Min === null ? '' : `, breach in ${risk.ttbP10Min}–${risk.ttbP90Min} min`
      const cargoTxt = vehicle.cargoC === null ? 'cargo unknown' : `cargo ${vehicle.cargoC.toFixed(1)} °C`
      if (risk.aspect === 'danger') {
        // Event-time start: the first reading above the limit, even if it arrived late.
        const first = truth.findIndex((p, j) => j <= k && p.receivedAt <= t && (p.cargoC ?? -99) > spec.profile.maxC)
        active.set(`${spec.vehicleId}:CARGO_TEMP_BREACH`, { type: 'CARGO_TEMP_BREACH', severity: 'CRITICAL', summary: `${cargoTxt}, limit ${spec.profile.maxC} °C`, since: truth[Math.max(0, first)].t, vehicleId: spec.vehicleId, shipmentId })
      } else if (sev) {
        active.set(`${spec.vehicleId}:BREACH_FORECAST`, { type: 'BREACH_FORECAST', severity: sev, summary: `${cargoTxt}${range}`, since: t, vehicleId: spec.vehicleId, shipmentId })
      }
      if (vehicle.door === 'OPEN' && vehicle.speedKmh > 5) {
        active.set(`${spec.vehicleId}:DOOR_OPEN_MOVING`, { type: 'DOOR_OPEN_MOVING', severity: 'CRITICAL', summary: `Door open at ${Math.round(vehicle.speedKmh)} km/h`, since: t, vehicleId: spec.vehicleId, shipmentId })
      }
      if (vehicle.probes.cargo !== 'ok') {
        active.set(`${spec.vehicleId}:SENSOR_FAULT`, { type: 'SENSOR_FAULT', severity: 'MEDIUM', summary: 'Cargo probe flat while air probes warm', since: t, vehicleId: spec.vehicleId, shipmentId })
      }
      if (ageS > 300) {
        const zone = inDeadZone(corridor, km) ?? 'coverage gap'
        active.set(`${spec.vehicleId}:TELEMETRY_GAP`, { type: 'TELEMETRY_GAP', severity: 'LOW', summary: `No signal for ${Math.round(ageS / 60)} min (${zone})`, since: known.t, vehicleId: spec.vehicleId, shipmentId })
      }
      if (vehicle.compressor === 'FAULT') {
        active.set(`${spec.vehicleId}:COMPRESSOR_FAULT`, { type: 'COMPRESSOR_FAULT', severity: 'CRITICAL', summary: 'Compressor fault code reported', since: t, vehicleId: spec.vehicleId, shipmentId })
      }
    })

    for (const [key, a] of active) {
      const existing = live.get(key)
      if (existing && existing.state !== 'AUTO_CLEARED') {
        existing.severity = a.severity
        existing.summary = a.summary
        existing.lastSeenAt = t
        // Dedup counts re-triggers after a quiet spell, not every reading.
        if (quietSince.has(key)) existing.occurrences += 1
      } else {
        live.set(key, {
          id: `INC-${key.replace(':', '-')}-${Math.round((a.since - START) / 60_000)}`,
          type: a.type,
          severity: a.severity,
          state: 'OPEN',
          vehicleId: a.vehicleId,
          shipmentId: a.shipmentId,
          openedAt: a.since,
          lastSeenAt: t,
          occurrences: 1,
          summary: a.summary,
          actions: ACTIONS[a.type],
        })
      }
      quietSince.delete(key)
    }
    for (const [key, inc] of live) {
      if (active.has(key) || inc.state === 'AUTO_CLEARED') continue
      const since = quietSince.get(key) ?? t
      quietSince.set(key, since)
      if (t - since >= 5 * 60_000) inc.state = 'AUTO_CLEARED'
    }
    // Snapshot copies: frames are immutable history.
    const incidents = [...live.values()]
      .filter((inc) => inc.state !== 'AUTO_CLEARED' || t - inc.lastSeenAt < 20 * 60_000)
      .map((inc) => ({ ...inc }))
    frames.push({ t, vehicles, shipments, incidents })
  }

  return {
    start: START,
    end: START + (STEPS - 1) * STEP_MS,
    stepMs: STEP_MS,
    corridors: CORRIDORS,
    markers,
    provenance: 'Simulated fleet (synthetic)',
    frameAt(t: number) {
      const idx = Math.max(0, Math.min(STEPS - 1, Math.round((t - START) / STEP_MS)))
      return frames[idx]
    },
    series(vehicleId: string, from: number, to: number) {
      const truth = truths.get(vehicleId) ?? []
      return truth
        .filter((p) => p.t >= from && p.t <= to)
        .map((p) =>
          p.receivedAt <= to
            ? { t: p.t, cargoC: p.cargoC, returnAirC: p.returnAirC, supplyAirC: p.supplyAirC, door: p.door, defrost: p.defrost, gap: false }
            : { t: p.t, cargoC: null, returnAirC: null, supplyAirC: null, door: false, defrost: false, gap: true },
        )
    },
  }
}
