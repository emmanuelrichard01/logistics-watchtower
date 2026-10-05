// Seeded synthetic fleet for console development (SYNTHETIC: no real trucks,
// shipments or values). Replaced by simulator recordings (apps/dashboard-fixtures)
// and later by the API stream; all three produce the same Timeline contract.

import { CORRIDORS, inDeadZone } from '../domain/corridors'
import type { CargoProfile, Timeline } from '../domain/types'
import { buildTimeline, type Truth } from './frames'

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
      compressor: fault ? 'FAULT' : cooling && !defrost ? 'RUNNING' : 'OFF',
      cargoProbe: stuck && min >= s.stuckProbeFromMin! + 12 ? 'suspect' : 'ok',
    })
  }
  return out
}

export function buildSyntheticTimeline(seed = 20261005): Timeline {
  return buildTimeline({
    start: START,
    stepMs: STEP_MS,
    steps: STEPS,
    corridors: CORRIDORS,
    provenance: 'Simulated fleet (synthetic)',
    vehicles: FLEET.map((spec, n) => {
      const corridor = CORRIDORS.find((c) => c.id === spec.corridorId)!
      return {
        vehicleId: spec.vehicleId,
        corridorId: spec.corridorId,
        profile: spec.profile,
        shipmentId: `SHP-${24100 + n * 37}`,
        destination: corridor.stations.at(-1)!.name,
        cruiseKmh: spec.speed,
        truth: simulateTruck(spec, seed + n * 7919),
      }
    }),
  })
}
