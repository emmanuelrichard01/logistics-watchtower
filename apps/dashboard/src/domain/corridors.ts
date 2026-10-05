import type { Corridor, Station } from './types'

// Waypoints carried over from v1 (legacy/v1/src/producer.py). Road distance is
// approximated as 1.25 x great-circle distance until the simulator's GeoJSON
// road geometry replaces it. Dead zones are illustrative, not coverage data.
const ROAD_FACTOR = 1.25

type Waypoint = [name: string, lat: number, lon: number, depot?: boolean]

const RAW: { id: string; name: string; waypoints: Waypoint[]; deadZones: [string, number, number][] }[] = [
  {
    id: 'RT-LAG-ABJ',
    name: 'Lagos – Abuja',
    waypoints: [
      ['Lagos', 6.455, 3.3941, true],
      ['Ikeja', 6.5962, 3.3683],
      ['Ikorodu', 6.8256, 3.6472],
      ['Sagamu', 6.8926, 3.7196],
      ['Ibadan', 7.3768, 3.9398, true],
      ['Oshogbo', 7.7027, 4.4984],
      ['Ilorin', 8.4904, 4.5522, true],
      ['Mokwa', 8.85, 5.9667],
      ['Abuja', 9.0563, 7.4985, true],
    ],
    deadZones: [
      ['Jebba stretch', 0.6, 0.68],
      ['Mokwa–Bida', 0.8, 0.88],
    ],
  },
  {
    id: 'RT-PHC-MKD',
    name: 'Port Harcourt – Makurdi',
    waypoints: [
      ['Port Harcourt', 4.8156, 7.0498, true],
      ['Elele', 5.1117, 7.3678],
      ['Owerri', 5.4851, 7.0354],
      ['Okigwe', 6.0072, 7.1194],
      ['Enugu', 6.4584, 7.5464, true],
      ['Nsukka', 6.8833, 7.3833],
      ['Makurdi', 7.7322, 8.5218, true],
    ],
    deadZones: [['Okigwe hills', 0.36, 0.44]],
  },
  {
    id: 'RT-BEN-ABJ',
    name: 'Benin – Abuja',
    waypoints: [
      ['Benin City', 6.3392, 5.6175, true],
      ['Ekpoma', 6.7428, 6.0922],
      ['Auchi', 7.1706, 6.136],
      ['Okene', 7.5629, 6.2343],
      ['Lokoja', 8.0069, 6.7455, true],
      ['Abaji', 8.5, 7.15],
      ['Abuja', 9.06, 7.48, true],
    ],
    deadZones: [['Okene–Lokoja', 0.5, 0.62]],
  },
]

export function haversineKm(lat1: number, lon1: number, lat2: number, lon2: number): number {
  const r = (d: number) => (d * Math.PI) / 180
  const dLat = r(lat2 - lat1)
  const dLon = r(lon2 - lon1)
  const a = Math.sin(dLat / 2) ** 2 + Math.cos(r(lat1)) * Math.cos(r(lat2)) * Math.sin(dLon / 2) ** 2
  return 6371 * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
}

function build(raw: (typeof RAW)[number]): Corridor {
  let km = 0
  const stations: Station[] = raw.waypoints.map(([name, lat, lon, depot], i) => {
    if (i > 0) {
      const [, plat, plon] = raw.waypoints[i - 1]
      km += haversineKm(plat, plon, lat, lon) * ROAD_FACTOR
    }
    return { name, lat, lon, km: Math.round(km), depot: depot ?? false }
  })
  const lengthKm = stations[stations.length - 1].km
  return {
    id: raw.id,
    name: raw.name,
    stations,
    lengthKm,
    deadZones: raw.deadZones.map(([name, from, to]) => ({
      name,
      fromKm: Math.round(from * lengthKm),
      toKm: Math.round(to * lengthKm),
    })),
  }
}

export const CORRIDORS: Corridor[] = RAW.map(build)

/** Position along a corridor: linear between stations (good enough for the schematic). */
export function positionAt(corridor: Corridor, km: number): { lat: number; lon: number; headingDeg: number } {
  const s = corridor.stations
  const clamped = Math.max(0, Math.min(corridor.lengthKm, km))
  let i = 0
  while (i < s.length - 2 && s[i + 1].km < clamped) i++
  const a = s[i]
  const b = s[i + 1]
  const f = b.km === a.km ? 0 : (clamped - a.km) / (b.km - a.km)
  const lat = a.lat + (b.lat - a.lat) * f
  const lon = a.lon + (b.lon - a.lon) * f
  const y = Math.sin(((b.lon - a.lon) * Math.PI) / 180) * Math.cos((b.lat * Math.PI) / 180)
  const x =
    Math.cos((a.lat * Math.PI) / 180) * Math.sin((b.lat * Math.PI) / 180) -
    Math.sin((a.lat * Math.PI) / 180) * Math.cos((b.lat * Math.PI) / 180) * Math.cos(((b.lon - a.lon) * Math.PI) / 180)
  const headingDeg = ((Math.atan2(y, x) * 180) / Math.PI + 360) % 360
  return { lat, lon, headingDeg }
}

export function inDeadZone(corridor: Corridor, km: number): string | null {
  return corridor.deadZones.find((z) => km >= z.fromKm && km <= z.toKm)?.name ?? null
}
