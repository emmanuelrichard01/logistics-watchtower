import { useNavigate } from '@tanstack/react-router'
import { CornerDownLeft, Search } from 'lucide-react'
import { useEffect, useMemo, useRef, useState } from 'react'
import { ASPECT_LABEL } from '../domain/risk'
import { useAppStore, useView } from '../state/context'
import { useStore } from '../state/store'
import { SignalHead } from './SignalHead'

interface Command {
  id: string
  group: string
  label: string
  hint?: string
  run: () => void
  aspect?: Parameters<typeof SignalHead>[0]['aspect']
}

/** ⌘K: jump to any truck, shipment, incident or view, or run a command. */
export function CommandPalette() {
  const store = useAppStore()
  const view = useView()
  const navigate = useNavigate()
  const open = useStore(store, (s) => s.paletteOpen)
  const theme = useStore(store, (s) => s.theme)
  const [q, setQ] = useState('')
  const [index, setIndex] = useState(0)
  const dialog = useRef<HTMLDialogElement>(null)

  useEffect(() => {
    const d = dialog.current
    if (!d) return
    if (open && !d.open) {
      setQ('')
      setIndex(0)
      d.showModal()
    } else if (!open && d.open) d.close()
  }, [open])

  const commands = useMemo<Command[]>(() => {
    const close = (fn: () => void) => () => {
      store.setPalette(false)
      fn()
    }
    return [
      ...view.shipmentsByRisk.map((s) => ({
        id: `truck-${s.vehicleId}`,
        group: 'Trucks',
        label: `${s.vehicleId} · ${s.cargo.name}`,
        hint: `${ASPECT_LABEL[s.risk.aspect]} · to ${s.destination} · ${s.id}`,
        aspect: s.risk.aspect,
        run: close(() => store.select(s.vehicleId)),
      })),
      ...view.incidentsByUrgency
        .filter((i) => i.state !== 'AUTO_CLEARED' && i.state !== 'RESOLVED')
        .map((i) => ({ id: `inc-${i.id}`, group: 'Incidents', label: `${i.vehicleId}: ${i.summary}`, hint: i.state.toLowerCase(), run: close(() => store.select(i.vehicleId)) })),
      { id: 'v-lanes', group: 'Go to', label: 'Lanes', run: close(() => navigate({ to: '/' })) },
      { id: 'v-map', group: 'Go to', label: 'Map', run: close(() => navigate({ to: '/map' })) },
      { id: 'v-inc', group: 'Go to', label: 'Incidents', run: close(() => navigate({ to: '/incidents' })) },
      { id: 'v-health', group: 'Go to', label: 'Health', run: close(() => navigate({ to: '/health' })) },
      { id: 'c-live', group: 'Commands', label: 'Go live', hint: 'L', run: close(store.goLive) },
      { id: 'c-back', group: 'Commands', label: 'Rewind 30 minutes', run: close(() => store.seek(store.getState().playhead - 30 * 60_000)) },
      { id: 'c-theme', group: 'Commands', label: theme === 'light' ? 'Switch to Operating Centre (dark)' : 'Switch to Enamel (light)', run: close(() => store.setTheme(theme === 'light' ? 'dark' : 'light')) },
    ]
  }, [view, store, navigate, theme])

  const needle = q.trim().toLowerCase()
  const results = needle ? commands.filter((c) => `${c.label} ${c.hint ?? ''} ${c.group}`.toLowerCase().includes(needle)) : commands.filter((c) => c.group !== 'Incidents').slice(0, 12)
  const active = results[Math.min(index, results.length - 1)]

  return (
    <dialog ref={dialog} className="palette" aria-label="Search and commands" onClose={() => store.setPalette(false)} onClick={(e) => e.target === dialog.current && store.setPalette(false)}>
      <div className="palette__box">
        <label className="palette__input">
          <Search size={17} aria-hidden="true" />
          <span className="visually-hidden">Search trucks, incidents, views and commands</span>
          <input
            autoFocus
            value={q}
            placeholder="Search trucks, incidents, views…"
            onChange={(e) => {
              setQ(e.target.value)
              setIndex(0)
            }}
            onKeyDown={(e) => {
              if (e.key === 'ArrowDown') setIndex((i) => Math.min(results.length - 1, i + 1))
              else if (e.key === 'ArrowUp') setIndex((i) => Math.max(0, i - 1))
              else if (e.key === 'Enter' && active) active.run()
              else return
              e.preventDefault()
            }}
            role="combobox"
            aria-expanded="true"
            aria-controls="palette-list"
            aria-activedescendant={active ? `cmd-${active.id}` : undefined}
          />
          <kbd>Esc</kbd>
        </label>
        <ul id="palette-list" className="palette__list" role="listbox">
          {results.map((c, i) => (
            <li key={c.id} id={`cmd-${c.id}`} role="option" aria-selected={c === active} className={c === active ? 'is-active' : ''} onMouseMove={() => setIndex(i)} onClick={c.run}>
              {(i === 0 || results[i - 1].group !== c.group) && <span className="palette__group">{c.group}</span>}
              <span className="palette__row">
                {c.aspect && <SignalHead aspect={c.aspect} size={0.7} />}
                <span className="palette__label">{c.label}</span>
                {c.hint && <span className="palette__hint">{c.hint}</span>}
                {c === active && <CornerDownLeft size={14} aria-hidden="true" className="palette__enter" />}
              </span>
            </li>
          ))}
          {results.length === 0 && <li className="empty">Nothing matches “{q}”. Try a truck number such as TRK-104.</li>}
        </ul>
      </div>
    </dialog>
  )
}
