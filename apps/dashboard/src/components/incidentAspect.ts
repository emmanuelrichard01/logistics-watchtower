import type { Aspect, Incident } from '../domain/types'

export function aspectForIncident(inc: Incident, shipmentAspect: Aspect | undefined): Aspect {
  if (inc.type === 'CARGO_TEMP_BREACH') return 'danger'
  if (inc.type === 'TELEMETRY_GAP' || inc.type === 'SENSOR_FAULT') return shipmentAspect ?? 'unknown'
  return shipmentAspect && shipmentAspect !== 'clear' ? shipmentAspect : inc.severity === 'CRITICAL' ? 'caution1' : 'caution2'
}
