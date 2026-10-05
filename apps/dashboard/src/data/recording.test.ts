import { readFileSync } from 'node:fs'
import { gunzipSync } from 'node:zlib'
import { describe, expect, it } from 'vitest'
import type { IncidentType } from '../domain/types'
import { timelineFromText } from './recording'

const fixture = (name: string) => new URL(`../../../dashboard-fixtures/${name}`, import.meta.url)
const tl = timelineFromText(readFileSync(fixture('routes.geojson'), 'utf8'), gunzipSync(readFileSync(fixture('console_showcase.fleet.jsonl.gz'))).toString('utf8'))

const firstSeen = new Map<string, number>() // `${vehicle}:${type}` -> minutes after start
for (let t = tl.start; t <= tl.end; t += tl.stepMs) {
  for (const inc of tl.frameAt(t).incidents) {
    const key = `${inc.vehicleId}:${inc.type}`
    if (!firstSeen.has(key)) firstSeen.set(key, (t - tl.start) / 60_000)
  }
}
const seen = (vehicle: string, type: IncidentType) => firstSeen.get(`${vehicle}:${type}`)

// Story beats from apps/dashboard-fixtures/README.md, as the console can know them.
describe('showcase recording in the console', () => {
  it('has ten vehicles on one three-hour clock', () => {
    expect(tl.frameAt(tl.start).vehicles).toHaveLength(10)
    expect((tl.end - tl.start) / 3_600_000).toBeCloseTo(3, 1)
  })

  it('warns about the degrading compressor before the cargo breaches', () => {
    const forecast = seen('TRK-101', 'BREACH_FORECAST')
    const breach = seen('TRK-101', 'CARGO_TEMP_BREACH')
    expect(forecast).toBeDefined()
    expect(breach).toBeDefined()
    // The docs quote this lead (docs/console/README.md): about 33 minutes.
    expect(breach! - forecast!).toBeGreaterThanOrEqual(30)
  })

  it('sees the door opened at highway speed', () => {
    expect(seen('TRK-103', 'DOOR_OPEN_MOVING')).toBeDefined()
  })

  it('flags the flatlined cargo probe from the readings alone', () => {
    expect(seen('TRK-104', 'SENSOR_FAULT')).toBeDefined()
    // Every other probe has real noise, so none of them is flagged.
    for (const [key] of firstSeen) if (key.endsWith(':SENSOR_FAULT')) expect(key).toBe('TRK-104:SENSOR_FAULT')
  })

  it('raises the hijacked truck as off route, then loses its signal', () => {
    const off = seen('TRK-105', 'ROUTE_DEVIATION')
    expect(off).toBeDefined()
    expect(seen('TRK-105', 'TELEMETRY_GAP')).toBeGreaterThan(off!)
    // Off the route, the map shows the recorded position, not a point on the road.
    const v = tl.frameAt(tl.start + (off! + 10) * 60_000).vehicles.find((x) => x.vehicleId === 'TRK-105')!
    expect(v.lat).not.toBeNaN()
  })

  it('never calls an empty box a cargo breach', () => {
    // VAN-ABJ2 waits empty for the cross-dock; TRIKE-LAG3 is empty after its last drop.
    expect(seen('VAN-ABJ2', 'CARGO_TEMP_BREACH')).toBeUndefined()
    expect(seen('TRIKE-LAG3', 'CARGO_TEMP_BREACH')).toBeUndefined()
  })

  it('opens no route deviations for vehicles that stay on their routes', () => {
    const deviating = [...firstSeen.keys()].filter((k) => k.endsWith(':ROUTE_DEVIATION')).map((k) => k.split(':')[0])
    expect(deviating).toEqual(['TRK-105'])
  })
})
