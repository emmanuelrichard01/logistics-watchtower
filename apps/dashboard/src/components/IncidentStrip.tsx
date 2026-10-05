import { Check, ChevronRight, Undo2, Wrench } from 'lucide-react'
import type { Incident, IncidentState } from '../domain/types'
import { fmtAge, useAppStore, useView } from '../state/context'
import { useStore } from '../state/store'
import { aspectForIncident } from './incidentAspect'
import { SignalHead } from './SignalHead'

const TYPE_LABEL: Record<Incident['type'], string> = {
  CARGO_TEMP_BREACH: 'Cargo above limit',
  BREACH_FORECAST: 'Breach forecast',
  DOOR_OPEN_MOVING: 'Door open while moving',
  SENSOR_FAULT: 'Sensor fault',
  TELEMETRY_GAP: 'No signal',
  COMPRESSOR_FAULT: 'Compressor fault',
  ROUTE_DEVIATION: 'Off route',
}

const STATE_LABEL: Record<IncidentState, string> = {
  OPEN: 'Open',
  ACKNOWLEDGED: 'Acknowledged',
  MITIGATING: 'Mitigating',
  RESOLVED: 'Resolved',
  AUTO_CLEARED: 'Cleared',
}


const NEXT: Partial<Record<IncidentState, { to: IncidentState; label: string; icon: typeof Check }>> = {
  OPEN: { to: 'ACKNOWLEDGED', label: 'Acknowledge', icon: Check },
  ACKNOWLEDGED: { to: 'MITIGATING', label: 'Start mitigation', icon: Wrench },
  MITIGATING: { to: 'RESOLVED', label: 'Resolve', icon: Check },
}

export function IncidentStrip({ incident, compact = false, focused = false }: { incident: Incident; compact?: boolean; focused?: boolean }) {
  const store = useAppStore()
  const view = useView()
  const playhead = useStore(store, (s) => s.playhead)
  const mode = useStore(store, (s) => s.mode)
  const selected = useStore(store, (s) => s.selected)
  const override = useStore(store, (s) => s.overrides[incident.id])
  const shipment = view.shipmentFor(incident.vehicleId)
  const aspect = aspectForIncident(incident, shipment?.risk.aspect)
  const next = NEXT[incident.state]
  const escalating = incident.state === 'OPEN' && incident.severity === 'CRITICAL' && playhead - incident.openedAt > 3 * 60_000
  const replay = mode !== 'live'
  const canUndo = override && playhead - override.at < 5 * 60_000 && !replay

  return (
    <article
      className={`strip strip--${incident.state.toLowerCase()}${selected === incident.vehicleId ? ' strip--selected' : ''}${focused ? ' strip--focused' : ''}${compact ? ' strip--compact' : ''}`}
      aria-label={`${TYPE_LABEL[incident.type]}, ${incident.vehicleId}, ${STATE_LABEL[incident.state]}`}
    >
      <button type="button" className="strip__open" onClick={() => store.select(incident.vehicleId)}>
        <SignalHead aspect={aspect} escalating={escalating} />
        <span className="strip__main">
          <span className="strip__title">
            <span>{TYPE_LABEL[incident.type]}</span>
            <span className="strip__vehicle">{incident.vehicleId}</span>
          </span>
          <span className="strip__summary">{incident.summary}</span>
          <span className="strip__meta num">
            <span className={`state-chip state-chip--${incident.state.toLowerCase()}`}>{STATE_LABEL[incident.state]}</span>
            <span>{fmtAge(playhead - incident.openedAt)}</span>
            {incident.occurrences > 1 && <span>×{incident.occurrences}</span>}
            {escalating && <span className="strip__escalated">Unacknowledged</span>}
          </span>
        </span>
        <ChevronRight className="strip__chevron" size={16} aria-hidden="true" />
      </button>
      {!compact && (next || canUndo) && (
        <div className="strip__actions">
          {next && (
            <button
              type="button"
              className={incident.state === 'OPEN' ? 'btn btn--primary' : 'btn'}
              disabled={replay}
              title={replay ? 'Actions are disabled while replaying' : undefined}
              onClick={() => store.act(incident, next.to)}
            >
              <next.icon size={14} aria-hidden="true" />
              {next.label}
            </button>
          )}
          {incident.state === 'ACKNOWLEDGED' && incident.actions[0] && <span className="strip__playbook">Next: {incident.actions[0]}</span>}
          {canUndo && (
            <button type="button" className="btn btn--quiet" onClick={() => store.undo(incident.id)}>
              <Undo2 size={14} aria-hidden="true" />
              Undo
            </button>
          )}
        </div>
      )}
    </article>
  )
}
