import { useEffect, useState } from 'react'
import { IncidentStrip } from '../components/IncidentStrip'
import type { IncidentState } from '../domain/types'
import { useAppStore, useView } from '../state/context'
import { useStore } from '../state/store'

const COLUMNS: { state: IncidentState[]; title: string; empty: string }[] = [
  { state: ['OPEN'], title: 'Open', empty: 'No open incidents. New ones arrive here first.' },
  { state: ['ACKNOWLEDGED'], title: 'Acknowledged', empty: 'Acknowledge an open incident to take ownership of it.' },
  { state: ['MITIGATING'], title: 'Mitigating', empty: 'Incidents move here once a playbook action is under way.' },
  { state: ['RESOLVED', 'AUTO_CLEARED'], title: 'Resolved', empty: 'Resolved and auto-cleared incidents stay here for 20 minutes.' },
]

const KEY_TO_STATE: Record<string, IncidentState> = { a: 'ACKNOWLEDGED', m: 'MITIGATING', r: 'RESOLVED' }

/** The incident queue is the record (PRODUCT.md principle 4). Keyboard triage: J/K, A, M, R, Enter. */
export function IncidentsView() {
  const store = useAppStore()
  const view = useView()
  const mode = useStore(store, (s) => s.mode)
  const [cursor, setCursor] = useState(0)
  const ordered = COLUMNS.flatMap((c) => view.incidentsByUrgency.filter((i) => c.state.includes(i.state)))
  const focused = ordered[Math.min(cursor, ordered.length - 1)]

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.target instanceof HTMLInputElement || e.metaKey || e.ctrlKey) return
      const k = e.key.toLowerCase()
      if (k === 'j') setCursor((c) => Math.min(ordered.length - 1, c + 1))
      else if (k === 'k') setCursor((c) => Math.max(0, c - 1))
      else if (k === 'enter' && focused) store.select(focused.vehicleId)
      else if (KEY_TO_STATE[k] && focused && mode === 'live') store.act(focused, KEY_TO_STATE[k])
      else return
      e.preventDefault()
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [ordered, focused, mode, store])

  const open = view.incidents.filter((i) => i.state === 'OPEN').length
  return (
    <div className="incidents-view">
      <div className="status-line">
        <h1>
          {open === 0 ? 'No incident waiting for a decision' : `${open} incident${open === 1 ? '' : 's'} waiting for a decision`}
        </h1>
        <p className="status-line__meta">
          <kbd>J</kbd> <kbd>K</kbd> move · <kbd>A</kbd> acknowledge · <kbd>M</kbd> mitigate · <kbd>R</kbd> resolve · <kbd>Enter</kbd> open
        </p>
      </div>
      <div className="columns">
        {COLUMNS.map((col) => {
          const items = view.incidentsByUrgency.filter((i) => col.state.includes(i.state))
          return (
            <section key={col.title} className="column" aria-labelledby={`col-${col.title}`}>
              <header className="column__head">
                <h2 id={`col-${col.title}`}>{col.title}</h2>
                <span className="num">{items.length}</span>
              </header>
              <div className="column__list">
                {items.length === 0 && <p className="empty">{col.empty}</p>}
                {items.map((inc) => (
                  <IncidentStrip key={inc.id} incident={inc} focused={focused?.id === inc.id} compact={col.title === 'Resolved'} />
                ))}
              </div>
            </section>
          )
        })}
      </div>
    </div>
  )
}
