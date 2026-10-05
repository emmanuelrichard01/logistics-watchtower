import { IncidentStrip } from '../components/IncidentStrip'
import { LaneTrack } from '../components/LaneTrack'
import { SignalHead } from '../components/SignalHead'
import { ASPECT_RANK } from '../domain/risk'
import { fmtTime, useAppStore, useView } from '../state/context'
import { useStore } from '../state/store'

export function StatusLine() {
  const view = useView()
  const store = useAppStore()
  const playhead = useStore(store, (s) => s.playhead)
  const attention = view.shipments.filter((s) => s.risk.aspect !== 'clear').length
  const onSchedule = view.shipments.length - attention
  const worst = view.shipmentsByRisk[0]
  return (
    <div className="status-line">
      <h1>
        {attention === 0 ? (
          'Every shipment on schedule'
        ) : (
          <button type="button" className="status-line__action" onClick={() => worst && store.select(worst.vehicleId)} title={worst ? `Open ${worst.vehicleId}, the most urgent` : undefined}>
            {`${attention} shipment${attention === 1 ? ' needs' : 's need'} attention`}
          </button>
        )}
        {attention > 0 && <span className="status-line__quiet"> · {onSchedule} on schedule</span>}
      </h1>
      <p className="status-line__meta num">Updated {fmtTime(playhead)} WAT</p>
    </div>
  )
}

export function LanesView() {
  const store = useAppStore()
  const view = useView()
  const selected = useStore(store, (s) => s.selected)

  const lanes = store.timeline.corridors
    .map((corridor) => {
      const items = view.vehicles
        .filter((v) => v.corridorId === corridor.id)
        .map((vehicle) => ({ vehicle, shipment: view.shipmentFor(vehicle.vehicleId)! }))
      const worst = items.reduce((w, i) => Math.min(w, ASPECT_RANK[i.shipment.risk.aspect]), 4)
      return { corridor, items, worst }
    })
    .sort((a, b) => a.worst - b.worst)

  const openIncidents = view.incidentsByUrgency.filter((i) => i.state !== 'RESOLVED' && i.state !== 'AUTO_CLEARED')
  const recent = view.incidentsByUrgency.filter((i) => i.state === 'RESOLVED' || i.state === 'AUTO_CLEARED').slice(0, 3)

  return (
    <div className="board">
      <div className="board__main">
        <StatusLine />
        <div className="lanes">
          {lanes.map(({ corridor, items }) => {
            const attention = items.filter((i) => i.shipment.risk.aspect !== 'clear')
            const worstAspect = [...items].sort((a, b) => ASPECT_RANK[a.shipment.risk.aspect] - ASPECT_RANK[b.shipment.risk.aspect])[0]?.shipment.risk.aspect ?? 'clear'
            return (
              <section key={corridor.id} className="lane" aria-labelledby={`lane-${corridor.id}`}>
                <header className="lane__head">
                  <SignalHead aspect={worstAspect} size={0.8} />
                  <h2 id={`lane-${corridor.id}`}>{corridor.name}</h2>
                  <p className="lane__meta num">
                    {corridor.kind === 'urban' ? `City round · ${corridor.city} · ` : ''}
                    {items.length} {items.length === 1 ? 'vehicle' : 'vehicles'} · {corridor.lengthKm} km
                    {attention.length > 0 && <span className="lane__attention"> · {attention.length} need attention</span>}
                  </p>
                </header>
                <LaneTrack corridor={corridor} items={items} selected={selected} onSelect={(id) => store.select(id === selected ? null : id)} />
              </section>
            )
          })}
        </div>
        <p className="board__legend">
          <span>
            <svg width="22" height="10" aria-hidden="true">
              <pattern id="legend-hatch" width="5" height="5" patternUnits="userSpaceOnUse" patternTransform="rotate(45)">
                <line x1="0" y1="0" x2="0" y2="5" stroke="var(--hatch)" strokeWidth="1.5" />
              </pattern>
              <rect width="22" height="10" rx="2" fill="url(#legend-hatch)" />
            </svg>
            No signal
          </span>
          <span>
            <svg width="10" height="10" aria-hidden="true">
              <rect x="1" y="1" width="8" height="8" rx="2" fill="var(--surface)" stroke="var(--ink-2)" strokeWidth="1.5" />
            </svg>
            Depot with cold storage
          </span>
          <span>
            <svg width="10" height="10" aria-hidden="true">
              <circle cx="5" cy="5" r="4" fill="var(--surface)" stroke="var(--ink-2)" strokeWidth="1.5" />
            </svg>
            Position estimated
          </span>
        </p>
      </div>

      <aside className="board__side" aria-label="Incidents">
        <header className="side__head">
          <h2>Incidents</h2>
          <span className="side__count num">{openIncidents.length} open</span>
        </header>
        <div className="side__list">
          {openIncidents.length === 0 && <p className="empty">Nothing needs a decision right now. New incidents appear here with their recommended action.</p>}
          {openIncidents.map((inc) => (
            <IncidentStrip key={inc.id} incident={inc} />
          ))}
          {recent.length > 0 && <h3 className="side__sub">Recently cleared</h3>}
          {recent.map((inc) => (
            <IncidentStrip key={inc.id} incident={inc} compact />
          ))}
        </div>
      </aside>
    </div>
  )
}
