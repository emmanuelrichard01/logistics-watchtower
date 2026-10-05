// Simulator recordings as the console's Timeline (apps/dashboard-fixtures, README there).
// Two recordings are time-aligned into one shift: inter-state trucks with a
// degrading compressor, and Lagos city rounds in the morning rush. Every value is
// synthetic. Only what a real console could know is used: readings become
// visible when the gateway would have received them (from last_fix_age_s), and
// probe trust is inferred from the readings, never from the simulator's
// ground-truth fault labels.

import routesUrl from '../../../dashboard-fixtures/routes.geojson?url'
import truckUrl from '../../../dashboard-fixtures/compressor_gradual_degradation.fleet.jsonl.gz?url'
import cityUrl from '../../../dashboard-fixtures/lagos_last_mile_morning_rush.fleet.jsonl.gz?url'
import { cumulativeKm } from '../domain/corridors'
import type { CargoProfile, Corridor, Station, Timeline } from '../domain/types'
import { buildTimeline, type Truth, type VehicleSource } from './frames'

const STEP_MS = 15_000

interface Row {
  t: string
  vehicle_id: string
  route_id: string
  km_along: number
  speed_kmh: number
  vehicle_class: 'trailer' | 'van' | 'trike'
  cargo_profile: string
  shipments: { shipment_id: string; profile: string; receiver: string }[]
  setpoint_c: number
  min_c: number
  max_c: number
  pallets: number
  cargo_c: number
  air_c: number
  supply_air_c: number
  door: 'OPEN' | 'CLOSED'
  compressor: 'RUNNING' | 'OFF' | 'FAULT'
  defrost: boolean
  last_fix_age_s: number | null
}

interface Feature {
  geometry: { type: 'LineString'; coordinates: [number, number][] } | { type: 'Point'; coordinates: [number, number] }
  properties: Record<string, unknown>
}

const PROFILE_NAME: Record<string, string> = {
  frozen: 'Frozen food',
  pharma_2_8: 'Vaccines & pharma',
  bananas: 'Bananas',
  fresh_produce: 'Fresh produce',
}
// Illustrative value of a full 26-pallet trailer load, in naira (synthetic).
const FULL_LOAD_NGN: Record<string, number> = { frozen: 18_500_000, pharma_2_8: 62_000_000, bananas: 7_200_000, fresh_produce: 5_400_000 }
const CRUISE_KMH = { trailer: 70, van: 22, trike: 18 } as const

async function fetchText(url: string): Promise<string> {
  const res = await fetch(url)
  if (!res.ok) throw new Error(`fixture ${url}: HTTP ${res.status}`)
  const bytes = new Uint8Array(await res.arrayBuffer())
  // Some servers decode .gz on the way (Content-Encoding); check the magic bytes.
  if (bytes[0] === 0x1f && bytes[1] === 0x8b) {
    const stream = new Blob([bytes]).stream().pipeThrough(new DecompressionStream('gzip'))
    return new Response(stream).text()
  }
  return new TextDecoder().decode(bytes)
}

const parseRows = (text: string): Row[] =>
  text
    .trim()
    .split('\n')
    .map((line) => JSON.parse(line) as Row)

function routesToCorridors(features: Feature[]): Corridor[] {
  const stops = features.filter((f) => f.geometry.type === 'Point')
  return features
    .filter((f): f is Feature & { geometry: { type: 'LineString'; coordinates: [number, number][] } } => f.geometry.type === 'LineString')
    .map((f) => {
      const p = f.properties
      const id = String(p.id)
      const lengthKm = Number(p.length_km)
      const path = f.geometry.coordinates
      const stations: Station[] = stops
        .filter((s) => s.properties.route_id === id || s.properties.corridor_id === id)
        .map((s) => {
          const [lon, lat] = s.geometry.coordinates as [number, number]
          return {
            name: String(s.properties.name),
            km: Math.round(Number(s.properties.km_along) * 10) / 10,
            lat,
            lon,
            depot: Boolean(s.properties.depot),
            type: s.properties.type ? String(s.properties.type) : undefined,
            window: s.properties.window ? String(s.properties.window) : undefined,
          }
        })
        .sort((a, b) => a.km - b.km)
      const zones = (p.dead_zones as { name: string; from_km: number; to_km: number }[] | undefined) ?? []
      return {
        id,
        name: String(p.name),
        kind: p.kind === 'urban' ? 'urban' : 'corridor',
        city: p.city ? String(p.city) : undefined,
        lengthKm: Math.round(lengthKm * 10) / 10,
        stations,
        deadZones: zones.map((z) => ({ name: z.name, fromKm: z.from_km, toKm: z.to_km })),
        path,
        pathKm: cumulativeKm(path, lengthKm),
      } satisfies Corridor
    })
}

/** When each reading would have reached the console, from the gateway's staleness. */
function receivedTimes(ts: number[], ages: (number | null)[]): number[] {
  const out = new Array<number>(ts.length).fill(Number.POSITIVE_INFINITY)
  let p = 0
  for (let j = 0; j < ts.length; j++) {
    const freshest = ts[j] - (ages[j] ?? 0) * 1000
    while (p <= j && ts[p] <= freshest) out[p++] = ts[j]
  }
  return out
}

function toSource(rows: Row[], shiftMs: number, corridors: Map<string, Corridor>): VehicleSource {
  const first = rows[0]
  const ts = rows.map((r) => Date.parse(r.t) + shiftMs)
  const received = receivedTimes(ts, rows.map((r) => r.last_fix_age_s))
  const truth: Truth[] = rows.map((r, i) => {
    return {
      t: ts[i],
      receivedAt: received[i],
      km: r.km_along,
      speed: r.speed_kmh,
      cargoC: r.cargo_c,
      returnAirC: r.air_c,
      supplyAirC: r.supply_air_c,
      door: r.door === 'OPEN',
      defrost: r.defrost,
      compressor: r.compressor,
      // The recording carries true product temperature, noise-free, not probe
      // readings; a stuck-probe rule would fire on every steady truck. Probe
      // trust waits for probe-level data in the recording.
      cargoProbe: 'ok',
    }
  })
  const corridor = corridors.get(first.route_id)!
  const profile: CargoProfile = {
    name: PROFILE_NAME[first.cargo_profile] ?? first.cargo_profile,
    setpointC: first.setpoint_c,
    minC: first.min_c,
    maxC: first.max_c,
    valuePerShipmentNgn: Math.round(((FULL_LOAD_NGN[first.cargo_profile] ?? 10_000_000) * Math.max(1, first.pallets)) / 26),
  }
  const destination = corridor.kind === 'urban' ? `${corridor.city ?? 'City'} round, ${corridor.stations.length - 1} drops` : corridor.stations.at(-1)!.name
  return {
    vehicleId: first.vehicle_id,
    corridorId: first.route_id,
    profile,
    shipmentId: first.shipments[0]?.shipment_id ?? `${first.vehicle_id}-LOAD`,
    destination,
    cruiseKmh: CRUISE_KMH[first.vehicle_class] ?? 40,
    truth,
  }
}

export async function loadRecordingTimeline(): Promise<Timeline> {
  const [routesText, truckText, cityText] = await Promise.all([fetchText(routesUrl), fetchText(truckUrl), fetchText(cityUrl)])
  const corridors = routesToCorridors((JSON.parse(routesText) as { features: Feature[] }).features)
  const byId = new Map(corridors.map((c) => [c.id, c]))
  const recordings = [parseRows(truckText), parseRows(cityText)]

  // Align every recording to the city recording's start: one shift, one clock.
  const starts = recordings.map((rows) => Date.parse(rows[0].t))
  const start = starts[1]
  const steps = Math.min(...recordings.map((rows) => new Set(rows.map((r) => r.t)).size))
  const vehicles: VehicleSource[] = []
  recordings.forEach((rows, n) => {
    const groups = new Map<string, Row[]>()
    for (const r of rows) {
      const list = groups.get(r.vehicle_id)
      if (list) list.push(r)
      else groups.set(r.vehicle_id, [r])
    }
    for (const group of [...groups.values()].sort((a, b) => a[0].vehicle_id.localeCompare(b[0].vehicle_id))) {
      vehicles.push(toSource(group.slice(0, steps), start - starts[n], byId))
    }
  })
  const used = new Set(vehicles.map((v) => v.corridorId))
  return buildTimeline({
    start,
    stepMs: STEP_MS,
    steps,
    corridors: corridors.filter((c) => used.has(c.id)),
    vehicles,
    provenance: 'Simulator recording (synthetic)',
  })
}
