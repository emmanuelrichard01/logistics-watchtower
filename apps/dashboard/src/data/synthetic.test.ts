import { describe, expect, it } from 'vitest'
import { buildSyntheticTimeline } from './synthetic'

const tl = buildSyntheticTimeline()
const all = (fn: (t: number) => boolean) => {
  for (let t = tl.start; t <= tl.end; t += tl.stepMs) if (!fn(t)) return false
  return true
}
const incidentsOf = (vehicleId: string) => {
  const seen = new Map<string, { type: string; openedAt: number; firstSeenAt: number }>()
  for (let t = tl.start; t <= tl.end; t += tl.stepMs) {
    for (const inc of tl.frameAt(t).incidents) {
      if (inc.vehicleId === vehicleId && !seen.has(inc.id)) seen.set(inc.id, { type: inc.type, openedAt: inc.openedAt, firstSeenAt: t })
    }
  }
  return [...seen.values()]
}
const types = (vehicleId: string) => new Set(incidentsOf(vehicleId).map((i) => i.type))

describe('synthetic timeline', () => {
  it('is deterministic for a seed', () => {
    const again = buildSyntheticTimeline()
    expect(JSON.stringify(again.frameAt(tl.end))).toBe(JSON.stringify(tl.frameAt(tl.end)))
  })

  it('gradual compressor degradation warns before it breaches', () => {
    const first = (aspect: string) => tl.markers.find((m) => m.vehicleId === 'TRK-101' && m.aspect === aspect)?.t
    expect(first('caution2')).toBeDefined()
    expect(first('danger')).toBeDefined()
    expect(first('caution2')!).toBeLessThan(first('danger')!)
  })

  it('a dead-zone failure opens a gap first, then a breach dated at its true start', () => {
    const incs = incidentsOf('TRK-104')
    const gap = incs.find((i) => i.type === 'TELEMETRY_GAP')
    const breach = incs.find((i) => i.type === 'CARGO_TEMP_BREACH')
    expect(gap).toBeDefined()
    expect(breach).toBeDefined()
    expect(breach!.openedAt).toBeLessThan(breach!.firstSeenAt) // learned late, dated correctly
  })

  it('door open at speed and a stuck probe raise their own incidents', () => {
    expect(types('TRK-110').has('DOOR_OPEN_MOVING')).toBe(true)
    expect(types('TRK-107').has('SENSOR_FAULT')).toBe(true)
  })

  it('a normal defrost cycle raises nothing', () => {
    expect(types('TRK-112').size).toBe(0)
  })

  it('healthy trucks raise nothing but coverage gaps (dead zones are normal)', () => {
    for (const v of ['TRK-102', 'TRK-103', 'TRK-105', 'TRK-108', 'TRK-113']) {
      expect([...types(v)].filter((t) => t !== 'TELEMETRY_GAP'), v).toEqual([])
    }
  })

  it('never shows a projected position as a live fix', () => {
    expect(all((t) => tl.frameAt(t).vehicles.every((v) => v.estimated === v.lastFixAgeS > 0))).toBe(true)
  })
})
