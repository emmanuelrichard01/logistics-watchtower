import type { Aspect, CargoProfile, Risk, VehicleState } from './types'

// PROVISIONAL estimator for console development only. The real time-to-breach
// comes from the risk engine (risk.assessments.v1, plan section 9). This one
// fits a straight line to recent cargo temperatures, which is enough to drive
// realistic aspects and ranges through the UI.
export const PROVISIONAL_RULE_VERSION = 0

export const ASPECT_THRESHOLDS = { caution1Min: 15, caution2Min: 45, minConfidence: 0.4 } as const

export function slopePerMin(points: { tMin: number; c: number }[]): number {
  const n = points.length
  if (n < 3) return 0
  const mt = points.reduce((s, p) => s + p.tMin, 0) / n
  const mc = points.reduce((s, p) => s + p.c, 0) / n
  let num = 0
  let den = 0
  for (const p of points) {
    num += (p.tMin - mt) * (p.c - mc)
    den += (p.tMin - mt) ** 2
  }
  return den === 0 ? 0 : num / den
}

export function aspectFor(ttbP10Min: number | null, confidence: number, breaching: boolean): Aspect {
  if (breaching) return 'danger'
  if (confidence < ASPECT_THRESHOLDS.minConfidence) return 'unknown'
  if (ttbP10Min === null) return 'clear'
  if (ttbP10Min < ASPECT_THRESHOLDS.caution1Min) return 'caution1'
  if (ttbP10Min < ASPECT_THRESHOLDS.caution2Min) return 'caution2'
  return 'clear'
}

export const MIN_FIT_POINTS = 40 // ten minutes of 15-second readings

export function assessRisk(
  vehicle: VehicleState,
  cargo: CargoProfile,
  recent: { tMin: number; c: number }[],
  minutesToArrival: number,
  minutesSinceDefrost = Number.POSITIVE_INFINITY,
): Risk {
  const reasons: string[] = []
  let confidence = 0.88
  if (vehicle.probes.cargo !== 'ok') {
    confidence -= vehicle.probes.cargo === 'faulty' ? 0.55 : 0.3
    reasons.push(vehicle.probes.cargo === 'faulty' ? 'Cargo probe faulty: cargo temperature uncertain' : 'Cargo probe suspect')
  }
  if (vehicle.estimated) {
    const gapMin = vehicle.lastFixAgeS / 60
    confidence -= Math.min(0.4, gapMin * 0.012)
    reasons.push(`No signal for ${Math.round(gapMin)} min: values projected`)
  }
  confidence = Math.max(0.05, Math.min(0.95, confidence))

  const current = vehicle.cargoC
  const breaching = current !== null && current > cargo.maxC
  // A defrost cycle warms the air on purpose; forecasting through it would
  // raise false alarms (plan section 8, defrost_cycle scenario).
  const defrosting = vehicle.defrost || minutesSinceDefrost < 15
  if (defrosting) reasons.push('Defrost cycle: forecast paused')
  const slope = defrosting || recent.length < MIN_FIT_POINTS ? 0 : slopePerMin(recent)
  let p10: number | null = null
  let p90: number | null = null

  if (breaching) {
    reasons.unshift(`Cargo ${current.toFixed(1)} °C is above the ${cargo.maxC} °C limit`)
  } else if (current !== null && slope > 0.01) {
    const ttb = (cargo.maxC - current) / slope
    const spread = 0.2 + (1 - confidence) * 0.6
    p10 = Math.max(0, ttb * (1 - spread))
    p90 = ttb * (1 + spread * 1.6)
    if (p10 < 240) reasons.unshift(`Cargo warming ${(slope * 60).toFixed(1)} °C/h toward ${cargo.maxC} °C`)
    else p10 = p90 = null
  }
  if (vehicle.compressor === 'FAULT') reasons.unshift('Compressor reports a fault code')
  if (vehicle.door === 'OPEN' && vehicle.speedKmh > 5) reasons.unshift('Door open while moving')

  const aspect = aspectFor(p10, confidence, breaching)
  const pBreach = breaching ? 1 : p10 === null ? 0.02 : Math.min(0.97, Math.max(0.05, minutesToArrival / (p10 + minutesToArrival)))
  const spoiledFraction = breaching ? 0.6 : 0.35
  if (reasons.length === 0) reasons.push('Holding setpoint; no breach forecast')

  return {
    aspect,
    ttbP10Min: p10 === null ? null : Math.round(p10),
    ttbP90Min: p90 === null ? null : Math.round(p90),
    confidence: Math.round(confidence * 100) / 100,
    reasons,
    expectedLossNgn: Math.round(pBreach * cargo.valuePerShipmentNgn * spoiledFraction),
    ruleVersion: PROVISIONAL_RULE_VERSION,
  }
}

export const ASPECT_RANK: Record<Aspect, number> = { danger: 0, caution1: 1, caution2: 2, unknown: 3, clear: 4 }

export const ASPECT_LABEL: Record<Aspect, string> = {
  danger: 'Breaching',
  caution1: 'Breach within 15 min',
  caution2: 'Breach within 45 min',
  unknown: 'Confidence too low',
  clear: 'On schedule',
}
