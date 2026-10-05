// Turns per-vehicle reading histories into the console's Timeline contract.
// Shared by the synthetic generator and the simulator recordings, so both get
// the same knowledge rule (only readings received by time t are visible),
// the same provisional risk, and the same incident derivation.

import { inDeadZone, positionAt } from '../domain/corridors'
import { assessRisk } from '../domain/risk'
import type {
  Aspect,
  CargoProfile,
  Corridor,
  Frame,
  Incident,
  IncidentType,
  ProbeStatus,
  Risk,
  SeriesPoint,
  Severity,
  Shipment,
  Timeline,
  VehicleState,
} from '../domain/types'

/** One reading as the device took it, plus when the console received it. */
export interface Truth {
  t: number
  receivedAt: number // Infinity while still buffered on the device
  km: number
  speed: number
  cargoC: number | null
  returnAirC: number | null
  supplyAirC: number | null
  door: boolean
  defrost: boolean
  compressor: VehicleState['compressor']
  cargoProbe: ProbeStatus
  /** Recorded position and distance from the planned route, when the source has them. */
  lat?: number
  lon?: number
  offRouteKm?: number
  /** No shipment aboard (before a handover, after the last drop): the probe reads box air, not cargo. */
  empty?: boolean
}

const EMPTY_RISK = { aspect: 'clear', ttbP10Min: null, ttbP90Min: null, confidence: 0.95, reasons: ['No cargo aboard'], expectedLossNgn: 0, ruleVersion: 0 } as const satisfies Risk

/** Past this, the recorded position is shown instead of a point on the route. */
const OFF_ROUTE_KM = 0.5

export interface VehicleSource {
  vehicleId: string
  corridorId: string
  profile: CargoProfile
  shipmentId: string
  destination: string
  cruiseKmh: number // for ETA when the vehicle is momentarily stopped
  truth: Truth[] // one entry per timeline step, aligned to start + i * stepMs
}

/** Devices report every 15-30 s; only a real gap makes a position an estimate. */
export const ESTIMATE_AFTER_S = 90

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
  ROUTE_DEVIATION: ['Call driver', 'Alert security partner', 'Share last position with police'],
}

/** Compass bearing of travel into reading k, from recorded positions. */
function headingFrom(truth: Truth[], k: number): number {
  for (let j = k - 1; j >= Math.max(0, k - 20); j--) {
    const a = truth[j]
    const b = truth[k]
    if (a.lat === undefined || a.lon === undefined || b.lat === undefined || b.lon === undefined) break
    const dx = (b.lon - a.lon) * Math.cos((b.lat * Math.PI) / 180)
    const dy = b.lat - a.lat
    if (Math.hypot(dx, dy) > 1e-5) return ((Math.atan2(dx, dy) * 180) / Math.PI + 360) % 360
  }
  return 0
}

export function buildTimeline(opts: {
  start: number
  stepMs: number
  steps: number
  corridors: Corridor[]
  vehicles: VehicleSource[]
  provenance: string
}): Timeline {
  const { start, stepMs, steps, corridors, vehicles: sources, provenance } = opts
  const frames: Frame[] = []
  const markers: Timeline['markers'] = []
  const live = new Map<string, Incident>() // dedup key -> incident
  const quietSince = new Map<string, number>()
  const lastAspect = new Map<string, Aspect>()
  const corridorOf = new Map(corridors.map((c) => [c.id, c]))

  for (let i = 0; i < steps; i++) {
    const t = start + i * stepMs
    const vehicles: VehicleState[] = []
    const shipments: Shipment[] = []
    const active = new Map<string, { type: IncidentType; severity: Severity; summary: string; since: number; vehicleId: string; shipmentId: string }>()

    for (const src of sources) {
      const truth = src.truth
      const corridor = corridorOf.get(src.corridorId)!
      // What the console knows at time t: the newest reading received by then.
      let k = Math.min(i, truth.length - 1)
      while (k > 0 && truth[k].receivedAt > t) k--
      const known = truth[k]
      const ageS = Math.max(0, (t - known.t) / 1000)
      const estimated = ageS > ESTIMATE_AFTER_S
      // Always dead-reckon from the last fix; label it an estimate only past the threshold.
      const offRoute = (known.offRouteKm ?? 0) > OFF_ROUTE_KM && known.lat !== undefined && known.lon !== undefined
      const km = offRoute ? known.km : Math.min(corridor.lengthKm, known.km + (known.speed / 3600) * ageS)
      // Off the route, dead reckoning along it would be a lie: hold the last recorded fix.
      const pos = offRoute ? { lat: known.lat!, lon: known.lon!, headingDeg: headingFrom(truth, k) } : positionAt(corridor, km)
      const vehicle: VehicleState = {
        vehicleId: src.vehicleId,
        corridorId: src.corridorId,
        km,
        lat: pos.lat,
        lon: pos.lon,
        speedKmh: known.speed,
        headingDeg: pos.headingDeg,
        cargoC: known.empty ? null : known.cargoC,
        returnAirC: known.returnAirC,
        supplyAirC: known.supplyAirC,
        setpointC: src.profile.setpointC,
        door: known.door ? 'OPEN' : 'CLOSED',
        compressor: known.compressor,
        defrost: known.defrost,
        lastFixAgeS: ageS,
        estimated,
        probes: { cargo: known.cargoProbe, returnAir: 'ok', supplyAir: 'ok' },
      }
      const recent = truth
        .slice(Math.max(0, k - 80), k + 1)
        .filter((p) => p.receivedAt <= t && p.cargoC !== null && !p.empty)
        .map((p) => ({ tMin: (p.t - start) / 60_000, c: p.cargoC as number }))
      const remainingKm = corridor.lengthKm - km
      let lastDefrost = -1
      for (let j = k; j >= Math.max(0, k - 120); j--) {
        if (truth[j].defrost && truth[j].receivedAt <= t) {
          lastDefrost = j
          break
        }
      }
      const minutesSinceDefrost = lastDefrost < 0 ? Number.POSITIVE_INFINITY : (known.t - truth[lastDefrost].t) / 60_000
      const risk = known.empty
        ? EMPTY_RISK
        : assessRisk(vehicle, src.profile, recent, (remainingKm / Math.max(src.cruiseKmh * 0.6, vehicle.speedKmh)) * 60, minutesSinceDefrost)
      vehicles.push(vehicle)
      shipments.push({ id: src.shipmentId, vehicleId: src.vehicleId, cargo: src.profile, destination: src.destination, risk })

      if (lastAspect.get(src.vehicleId) !== risk.aspect && risk.aspect !== 'clear') {
        markers.push({ t, aspect: risk.aspect, vehicleId: src.vehicleId })
      }
      lastAspect.set(src.vehicleId, risk.aspect)

      const sev = SEVERITY_FOR[risk.aspect]
      const range = risk.ttbP10Min === null ? '' : `, breach in ${risk.ttbP10Min}–${risk.ttbP90Min} min`
      const cargoTxt = vehicle.cargoC === null ? 'cargo unknown' : `cargo ${vehicle.cargoC.toFixed(1)} °C`
      const base = { vehicleId: src.vehicleId, shipmentId: src.shipmentId }
      if (risk.aspect === 'danger') {
        // Event-time start: the first reading above the limit, even if it arrived late.
        const first = truth.findIndex((p, j) => j <= k && p.receivedAt <= t && !p.empty && (p.cargoC ?? -99) > src.profile.maxC)
        active.set(`${src.vehicleId}:CARGO_TEMP_BREACH`, { ...base, type: 'CARGO_TEMP_BREACH', severity: 'CRITICAL', summary: `${cargoTxt}, limit ${src.profile.maxC} °C`, since: truth[Math.max(0, first)].t })
      } else if (sev) {
        active.set(`${src.vehicleId}:BREACH_FORECAST`, { ...base, type: 'BREACH_FORECAST', severity: sev, summary: `${cargoTxt}${range}`, since: t })
      }
      if (vehicle.door === 'OPEN' && vehicle.speedKmh > 5) {
        active.set(`${src.vehicleId}:DOOR_OPEN_MOVING`, { ...base, type: 'DOOR_OPEN_MOVING', severity: 'CRITICAL', summary: `Door open at ${Math.round(vehicle.speedKmh)} km/h`, since: t })
      }
      if (vehicle.probes.cargo !== 'ok') {
        active.set(`${src.vehicleId}:SENSOR_FAULT`, { ...base, type: 'SENSOR_FAULT', severity: 'MEDIUM', summary: 'Cargo probe flat while air probes move', since: t })
      }
      if (ageS > 300) {
        const zone = inDeadZone(corridor, km) ?? 'coverage gap'
        active.set(`${src.vehicleId}:TELEMETRY_GAP`, { ...base, type: 'TELEMETRY_GAP', severity: 'LOW', summary: `No signal for ${Math.round(ageS / 60)} min (${zone})`, since: known.t })
      }
      if (offRoute) {
        const firstOff = truth.findLastIndex((p, j) => j <= k && (p.offRouteKm ?? 0) <= OFF_ROUTE_KM) + 1
        active.set(`${src.vehicleId}:ROUTE_DEVIATION`, { ...base, type: 'ROUTE_DEVIATION', severity: 'HIGH', summary: `${known.offRouteKm!.toFixed(1)} km off the planned route`, since: truth[firstOff].t })
      }
      if (vehicle.compressor === 'FAULT') {
        active.set(`${src.vehicleId}:COMPRESSOR_FAULT`, { ...base, type: 'COMPRESSOR_FAULT', severity: 'CRITICAL', summary: 'Compressor fault code reported', since: t })
      }
    }

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
          id: `INC-${key.replace(':', '-')}-${Math.round((a.since - start) / 60_000)}`,
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

  const byVehicle = new Map(sources.map((s) => [s.vehicleId, s.truth]))
  return {
    start,
    end: start + (steps - 1) * stepMs,
    stepMs,
    corridors,
    markers,
    provenance,
    frameAt(t: number) {
      const idx = Math.max(0, Math.min(steps - 1, Math.round((t - start) / stepMs)))
      return frames[idx]
    },
    series(vehicleId: string, from: number, to: number): SeriesPoint[] {
      const truth = byVehicle.get(vehicleId) ?? []
      return truth
        .filter((p) => p.t >= from && p.t <= to)
        .map((p) =>
          p.receivedAt <= to
            ? { t: p.t, cargoC: p.empty ? null : p.cargoC, returnAirC: p.returnAirC, supplyAirC: p.supplyAirC, door: p.door, defrost: p.defrost, gap: false }
            : { t: p.t, cargoC: null, returnAirC: null, supplyAirC: null, door: false, defrost: false, gap: true },
        )
    },
  }
}
