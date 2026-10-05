import { useNavigate } from '@tanstack/react-router'
import { MapPin, Phone, X } from 'lucide-react'
import { useEffect } from 'react'
import { ASPECT_LABEL } from '../domain/risk'
import { fmtAge, fmtNgn, fmtTemp, fmtTime, useAppStore, useView } from '../state/context'
import { useStore } from '../state/store'
import { IncidentStrip } from './IncidentStrip'
import { SignalHead } from './SignalHead'
import { TempChart } from './TempChart'

const PROBE_LABEL = { ok: 'OK', suspect: 'Suspect', faulty: 'Faulty' } as const

/** The hinged evidence layer: it opens over the board without leaving it. */
export function ShipmentLayer() {
  const store = useAppStore()
  const view = useView()
  const navigate = useNavigate()
  const selected = useStore(store, (s) => (s.detail ? s.selected : null))
  const playhead = useStore(store, (s) => s.playhead)

  useEffect(() => {
    if (!selected) return
    const onKey = (e: KeyboardEvent) => e.key === 'Escape' && store.openDetail(false)
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [selected, store])

  if (!selected) return null
  const vehicle = view.vehicles.find((v) => v.vehicleId === selected)
  const shipment = view.shipmentFor(selected)
  if (!vehicle || !shipment) return null
  const { risk, cargo } = shipment
  const incidents = view.incidentsFor(selected).filter((i) => i.state !== 'AUTO_CLEARED')
  const corridor = store.timeline.corridors.find((c) => c.id === vehicle.corridorId)!
  const from = playhead - 90 * 60_000
  const to = playhead + 45 * 60_000
  const points = store.timeline.series(selected, from, playhead)
  const breachValue = vehicle.cargoC !== null && risk.aspect === 'danger' ? vehicle.cargoC - cargo.maxC : null

  return (
    <aside className="layer" aria-label={`Shipment ${shipment.id}`} key={selected}>
      <header className="layer__head">
        <div className="layer__ids">
          <h2>
            {vehicle.vehicleId}
            <span className="layer__sub">
              {shipment.id} · {cargo.name} · to {shipment.destination}
            </span>
          </h2>
        </div>
        <button type="button" className="icon-btn" onClick={() => store.openDetail(false)} aria-label="Close shipment">
          <X size={18} aria-hidden="true" />
        </button>
      </header>

      <section className={`verdict verdict--${risk.aspect}`}>
        <SignalHead aspect={risk.aspect} size={1.4} />
        <div>
          <p className="verdict__label">{ASPECT_LABEL[risk.aspect]}</p>
          {risk.ttbP10Min !== null ? (
            <p className="verdict__figure">
              {risk.ttbP10Min}–{risk.ttbP90Min}
              <span> min to breach</span>
            </p>
          ) : breachValue !== null ? (
            <p className="verdict__figure">
              +{breachValue.toFixed(1)}
              <span> °C over the {cargo.maxC} °C limit</span>
            </p>
          ) : (
            <p className="verdict__figure verdict__figure--quiet">
              {fmtTemp(vehicle.cargoC)}
              <span> cargo, limit {cargo.maxC} °C</span>
            </p>
          )}
          <p className="verdict__meta num">
            Confidence {Math.round(risk.confidence * 100)}% · expected loss {fmtNgn(risk.expectedLossNgn)} ·{' '}
            {vehicle.estimated ? `estimated, last heard ${fmtAge(vehicle.lastFixAgeS * 1000)} ago` : `reading at ${fmtTime(playhead)}`}
          </p>
        </div>
      </section>

      <TempChart points={points} now={playhead} from={from} to={to} cargo={cargo} risk={risk} />

      <section className="layer__section">
        <h3>Why this score</h3>
        <ol className="reasons">
          {risk.reasons.map((r) => (
            <li key={r}>{r}</li>
          ))}
        </ol>
        <p className="layer__fine">
          Rule set v{risk.ruleVersion} (provisional console estimator) · evidence: readings from {fmtTime(Math.max(store.timeline.start, playhead - 20 * 60_000))} to {fmtTime(playhead)}
        </p>
      </section>

      <section className="layer__section layer__grid">
        <div>
          <h3>Probes</h3>
          <dl className="probes">
            {(
              [
                ['Cargo', vehicle.probes.cargo, vehicle.cargoC],
                ['Return air', vehicle.probes.returnAir, vehicle.returnAirC],
                ['Supply air', vehicle.probes.supplyAir, vehicle.supplyAirC],
              ] as const
            ).map(([label, status, value]) => (
              <div key={label}>
                <dt>{label}</dt>
                <dd className="num">
                  {fmtTemp(value)} <span className={`probe-chip probe-chip--${status}`}>{PROBE_LABEL[status]}</span>
                </dd>
              </div>
            ))}
          </dl>
        </div>
        <div>
          <h3>Unit</h3>
          <dl className="probes">
            <div>
              <dt>Compressor</dt>
              <dd>{vehicle.defrost ? 'Defrosting' : vehicle.compressor.toLowerCase()}</dd>
            </div>
            <div>
              <dt>Door</dt>
              <dd>{vehicle.door === 'OPEN' ? 'Open' : 'Closed'}</dd>
            </div>
            <div>
              <dt>Position</dt>
              <dd className="num">
                km {Math.round(vehicle.km)} of {corridor.lengthKm} · {Math.round(vehicle.speedKmh)} km/h
              </dd>
            </div>
          </dl>
        </div>
      </section>

      {incidents.length > 0 && (
        <section className="layer__section">
          <h3>Incidents</h3>
          <div className="layer__strips">
            {incidents.map((inc) => (
              <IncidentStrip key={inc.id} incident={inc} />
            ))}
          </div>
        </section>
      )}

      <footer className="layer__foot">
        <button type="button" className="btn">
          <Phone size={14} aria-hidden="true" />
          Call driver
        </button>
        <button
          type="button"
          className="btn"
          onClick={() => {
            store.openDetail(false)
            store.setFollow(true)
            navigate({ to: '/map' })
          }}
        >
          <MapPin size={14} aria-hidden="true" />
          Show on map
        </button>
      </footer>
    </aside>
  )
}
