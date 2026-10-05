import { FileText, Navigation, X } from 'lucide-react'
import { Sheet } from '../../components/Sheet'
import { SignalHead } from '../../components/SignalHead'
import { ASPECT_LABEL } from '../../domain/risk'
import { fmtAge, fmtClock, fmtTemp, useAppStore, useView } from '../../state/context'
import { useStore } from '../../state/store'

/** Uber-style trip card for the selected truck. */
export function TripCard({ onFollow }: { onFollow: () => void }) {
  const store = useAppStore()
  const view = useView()
  const selected = useStore(store, (s) => s.selected)
  const follow = useStore(store, (s) => s.follow)
  const playhead = useStore(store, (s) => s.playhead)
  const vehicle = view.vehicles.find((v) => v.vehicleId === selected)
  const shipment = selected ? view.shipmentFor(selected) : undefined
  if (!vehicle || !shipment) return null
  const corridor = store.timeline.corridors.find((c) => c.id === vehicle.corridorId)!
  const remainingKm = Math.max(0, corridor.lengthKm - vehicle.km)
  const pace = Math.max(35, vehicle.speedKmh || 55)
  const eta = playhead + (remainingKm / pace) * 3_600_000
  const next = corridor.stations.find((s) => s.km > vehicle.km + 1)
  const progress = vehicle.km / corridor.lengthKm
  const { risk } = shipment

  return (
    <Sheet className="trip-card" label={`Trip ${vehicle.vehicleId}`} initial="half">
      <header className="trip-card__head">
        <SignalHead aspect={risk.aspect} />
        <div className="trip-card__title">
          <h2>{vehicle.vehicleId}</h2>
          <p>
            {shipment.cargo.name} · {corridor.name}
          </p>
        </div>
        <button type="button" className="icon-btn" onClick={() => store.select(null)} aria-label="Close trip">
          <X size={18} aria-hidden="true" />
        </button>
      </header>

      <p className={`trip-card__aspect trip-card__aspect--${risk.aspect}`}>
        {ASPECT_LABEL[risk.aspect]}
        {risk.ttbP10Min !== null && <span className="num"> · breach in {risk.ttbP10Min}–{risk.ttbP90Min} min</span>}
      </p>

      <dl className="trip-stats num">
        <div>
          <dt>Arrives</dt>
          <dd>{remainingKm < 1 ? 'Arrived' : fmtClock(eta)}</dd>
        </div>
        <div>
          <dt>Remaining</dt>
          <dd>{Math.round(remainingKm)} km</dd>
        </div>
        <div>
          <dt>Cargo</dt>
          <dd>{fmtTemp(vehicle.cargoC)}</dd>
        </div>
        <div>
          <dt>Speed</dt>
          <dd>{Math.round(vehicle.speedKmh)} km/h</dd>
        </div>
      </dl>

      <div className="trip-progress" aria-label={`${Math.round(progress * 100)}% of the route complete`}>
        <div className="trip-progress__bar">
          <span style={{ transform: `scaleX(${progress})` }} />
          {corridor.deadZones.map((z) => (
            <i key={z.name} style={{ left: `${(z.fromKm / corridor.lengthKm) * 100}%`, width: `${((z.toKm - z.fromKm) / corridor.lengthKm) * 100}%` }} title={`${z.name}: no signal`} />
          ))}
          {corridor.stations.map((s) => (
            <b key={s.name} style={{ left: `${(s.km / corridor.lengthKm) * 100}%` }} className={s.km <= vehicle.km ? 'passed' : ''} />
          ))}
        </div>
        <p className="trip-progress__labels">
          <span title={corridor.stations[0].name}>{corridor.stations[0].name}</span>
          {next && (
            <span className="trip-progress__next num" title={next.name}>
              Next: {next.name}, {Math.round(next.km - vehicle.km)} km
            </span>
          )}
          <span title={corridor.stations.at(-1)?.name}>{corridor.stations.at(-1)?.name ?? shipment.destination}</span>
        </p>
      </div>

      {vehicle.estimated && <p className="trip-card__note">Position estimated: last heard {fmtAge(vehicle.lastFixAgeS * 1000)} ago in a coverage gap.</p>}

      <div className="trip-card__actions">
        <button type="button" className={follow ? 'btn btn--primary' : 'btn'} aria-pressed={follow} onClick={onFollow}>
          <Navigation size={14} aria-hidden="true" />
          {follow ? 'Following' : 'Follow'}
        </button>
        <button type="button" className="btn" onClick={() => store.openDetail(true)}>
          <FileText size={14} aria-hidden="true" />
          Evidence
        </button>
      </div>
    </Sheet>
  )
}
