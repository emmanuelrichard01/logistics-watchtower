import { positionAt } from '../../domain/corridors'
import type { Corridor, Timeline, VehicleState } from '../../domain/types'

export type LngLat = [number, number]

/** Path along a corridor between two km marks, through every station in between. */
export function pathBetween(corridor: Corridor, fromKm: number, toKm: number): LngLat[] {
  const a = Math.max(0, Math.min(fromKm, toKm))
  const b = Math.min(corridor.lengthKm, Math.max(fromKm, toKm))
  const start = positionAt(corridor, a)
  const end = positionAt(corridor, b)
  const inner = corridor.stations.filter((s) => s.km > a && s.km < b).map((s): LngLat => [s.lon, s.lat])
  return [[start.lon, start.lat], ...inner, [end.lon, end.lat]]
}

export interface AnimatedVehicle {
  vehicle: VehicleState
  position: LngLat
  bearing: number
}

function lerpAngle(a: number, b: number, f: number): number {
  const d = ((b - a + 540) % 360) - 180
  return (a + d * f + 360) % 360
}

/** Fleet at a continuous time: positions glide between timeline steps instead of jumping. */
export function fleetAt(timeline: Timeline, t: number): AnimatedVehicle[] {
  const step = timeline.stepMs
  const q = timeline.start + Math.floor((t - timeline.start) / step) * step
  const f = Math.max(0, Math.min(1, (t - q) / step))
  const a = timeline.frameAt(q)
  const b = timeline.frameAt(Math.min(timeline.end, q + step))
  return a.vehicles.map((va, i) => {
    const vb = b.vehicles[i]?.vehicleId === va.vehicleId ? b.vehicles[i] : va
    return {
      vehicle: f < 0.5 ? va : vb,
      position: [va.lon + (vb.lon - va.lon) * f, va.lat + (vb.lat - va.lat) * f],
      bearing: lerpAngle(va.headingDeg, vb.headingDeg, f),
    }
  })
}

/** Where a vehicle has been over the last `minutes`, one point per minute. */
export function trailOf(timeline: Timeline, vehicleId: string, t: number, minutes = 60): LngLat[] {
  const out: LngLat[] = []
  for (let m = minutes; m >= 0; m--) {
    const at = t - m * 60_000
    if (at < timeline.start) continue
    const v = timeline.frameAt(at).vehicles.find((x) => x.vehicleId === vehicleId)
    if (v) out.push([v.lon, v.lat])
  }
  return out
}

export function fleetBounds(corridors: Corridor[]): [LngLat, LngLat] {
  const lons = corridors.flatMap((c) => c.stations.map((s) => s.lon))
  const lats = corridors.flatMap((c) => c.stations.map((s) => s.lat))
  return [
    [Math.min(...lons), Math.min(...lats)],
    [Math.max(...lons), Math.max(...lats)],
  ]
}

/** Read a design token as an RGB tuple for deck.gl. */
export function tokenRgb(name: string): [number, number, number] {
  const raw = getComputedStyle(document.documentElement).getPropertyValue(name).trim()
  const hex = raw.startsWith('#') ? raw.slice(1) : null
  if (hex && (hex.length === 6 || hex.length === 3)) {
    const full = hex.length === 3 ? [...hex].map((c) => c + c).join('') : hex
    return [0, 2, 4].map((i) => parseInt(full.slice(i, i + 2), 16)) as [number, number, number]
  }
  const m = raw.match(/\d+(\.\d+)?/g)
  return m ? [Number(m[0]), Number(m[1]), Number(m[2])] : [128, 128, 128]
}
