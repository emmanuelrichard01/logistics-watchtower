import { Search } from 'lucide-react'
import { useState } from 'react'
import { Sheet } from '../../components/Sheet'
import { SignalHead } from '../../components/SignalHead'
import { fmtAge, fmtTemp, useAppStore, useView } from '../../state/context'
import { useStore } from '../../state/store'

/** The map's companion list: search, triage order, and the accessible equivalent of the map. */
export function FleetList() {
  const store = useAppStore()
  const view = useView()
  const selected = useStore(store, (s) => s.selected)
  const [q, setQ] = useState('')
  const needle = q.trim().toLowerCase()
  const rows = view.shipmentsByRisk.filter(
    (s) => !needle || s.vehicleId.toLowerCase().includes(needle) || s.cargo.name.toLowerCase().includes(needle) || s.destination.toLowerCase().includes(needle),
  )
  const attention = view.shipments.filter((s) => s.risk.aspect !== 'clear').length
  if (selected && window.matchMedia('(max-width: 760px)').matches) return null

  return (
    <Sheet className="fleet-list" label="Fleet">
      <header className="fleet-list__head">
        <h2>Fleet</h2>
        <p className="num">
          {view.shipments.length} trucks · {attention} need attention
        </p>
      </header>
      <label className="field">
        <Search size={15} aria-hidden="true" />
        <span className="visually-hidden">Filter trucks</span>
        <input value={q} onChange={(e) => setQ(e.target.value)} placeholder="Truck, cargo or destination" />
      </label>
      <ul className="fleet-list__rows">
        {rows.map((s) => {
          const v = view.vehicles.find((x) => x.vehicleId === s.vehicleId)!
          return (
            <li key={s.vehicleId}>
              <button type="button" className={`fleet-row${selected === s.vehicleId ? ' fleet-row--selected' : ''}`} onClick={() => store.select(s.vehicleId, false)} aria-pressed={selected === s.vehicleId}>
                <SignalHead aspect={s.risk.aspect} size={0.8} />
                <span className="fleet-row__main">
                  <span className="fleet-row__id">{s.vehicleId}</span>
                  <span className="fleet-row__sub">
                    {s.cargo.name} → {s.destination}
                  </span>
                </span>
                <span className="fleet-row__value num">
                  {s.risk.ttbP10Min !== null ? `${s.risk.ttbP10Min}–${s.risk.ttbP90Min}m` : v.estimated ? `est. ${fmtAge(v.lastFixAgeS * 1000)}` : fmtTemp(v.cargoC)}
                </span>
              </button>
            </li>
          )
        })}
        {rows.length === 0 && <li className="empty">No truck matches “{q}”.</li>}
      </ul>
    </Sheet>
  )
}
