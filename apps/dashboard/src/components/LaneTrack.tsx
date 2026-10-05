import { useLayoutEffect, useRef, useState } from 'react'
import { ASPECT_LABEL, ASPECT_RANK } from '../domain/risk'
import type { Corridor, Shipment, VehicleState } from '../domain/types'
import { fmtAge, fmtTemp } from '../state/context'
import { SignalHead } from './SignalHead'

const PAD_X = 28
const TAG_W = 176
const TAG_H = 44
const ROW_GAP = 10
const TRACK_GAP = 22 // between the lowest tag row and the track
const CROWDED = 6 // above this many shipments, Clear ones collapse to ticks

interface Item {
  vehicle: VehicleState
  shipment: Shipment
}

function useWidth<T extends HTMLElement>() {
  const ref = useRef<T>(null)
  const [width, setWidth] = useState(0)
  useLayoutEffect(() => {
    const el = ref.current
    if (!el) return
    const ro = new ResizeObserver(([e]) => setWidth(e.contentRect.width))
    ro.observe(el)
    return () => ro.disconnect()
  }, [])
  return [ref, width] as const
}

/** Greedy row packing so tags never overlap; worst aspects get the row nearest the track. */
function layoutTags(items: (Item & { x: number })[], width: number) {
  const rows: number[] = [] // right edge of the last tag in each row
  return items.map((item) => {
    const left = Math.max(0, Math.min(width - TAG_W, item.x - TAG_W / 2))
    let row = rows.findIndex((right) => right + 8 <= left)
    if (row === -1) {
      rows.push(0)
      row = rows.length - 1
    }
    rows[row] = left + TAG_W
    return { ...item, left, row }
  })
}

export function LaneTrack({
  corridor,
  items,
  selected,
  onSelect,
}: {
  corridor: Corridor
  items: Item[]
  selected: string | null
  onSelect: (vehicleId: string) => void
}) {
  const [ref, width] = useWidth<HTMLDivElement>()
  const scale = (km: number) => PAD_X + (km / corridor.lengthKm) * Math.max(1, width - PAD_X * 2)

  // Narrow screens hold fewer tags before Clear shipments collapse to ticks.
  const crowded = items.length > (width < 600 ? 2 : CROWDED)
  const tagged = items.filter((i) => !crowded || i.shipment.risk.aspect !== 'clear' || i.vehicle.vehicleId === selected)
  const ticks = items.filter((i) => !tagged.includes(i))

  const placed = layoutTags(
    tagged
      .map((i) => ({ ...i, x: scale(i.vehicle.km) }))
      .sort((a, b) => ASPECT_RANK[a.shipment.risk.aspect] - ASPECT_RANK[b.shipment.risk.aspect] || a.x - b.x),
    width,
  )

  const rowCount = Math.max(1, ...placed.map((p) => p.row + 1))
  const trackY = rowCount * (TAG_H + ROW_GAP) + TRACK_GAP
  const height = trackY + 58
  const tagTop = (row: number) => trackY - TRACK_GAP - (row + 1) * TAG_H - row * ROW_GAP
  const sel = items.find((i) => i.vehicle.vehicleId === selected)

  // Station labels never collide: ends and depots claim space first; a station
  // that loses keeps its marker and names itself in a tooltip.
  const labelled = new Set<string>()
  const taken: [number, number][] = []
  const last = corridor.stations.length - 1
  const priority = corridor.stations
    .map((s, i) => ({ s, i, rank: i === 0 || i === last ? 0 : s.depot ? 1 : 2 }))
    .sort((a, b) => a.rank - b.rank || a.i - b.i)
  for (const { s, i } of priority) {
    const w = s.name.length * 7 + 8
    const x = scale(s.km)
    const box: [number, number] = i === 0 ? [x, x + w] : i === last ? [x - w, x] : [x - w / 2, x + w / 2]
    if (taken.every(([a, b]) => box[1] + 10 < a || box[0] - 10 > b)) {
      taken.push(box)
      labelled.add(s.name)
    }
  }

  return (
    <div className="lane-track" ref={ref} style={{ height }}>
      <svg className="lane-track__svg" width={width} height={height} aria-hidden="true">
        <defs>
          <pattern id={`hatch-${corridor.id}`} width="6" height="6" patternUnits="userSpaceOnUse" patternTransform="rotate(45)">
            <line x1="0" y1="0" x2="0" y2="6" stroke="var(--hatch)" strokeWidth="1.5" />
          </pattern>
        </defs>
        {/* Km posts every 50 km */}
        {Array.from({ length: Math.floor(corridor.lengthKm / 50) + 1 }, (_, i) => i * 50).map((km) => (
          <line key={km} x1={scale(km)} x2={scale(km)} y1={trackY - 4} y2={trackY + 4} stroke="var(--track)" strokeWidth="1" />
        ))}
        <line x1={scale(0)} x2={scale(corridor.lengthKm)} y1={trackY} y2={trackY} stroke="var(--track)" strokeWidth="2" strokeLinecap="round" />
        {corridor.deadZones.map((z) => (
          <rect key={z.name} x={scale(z.fromKm)} y={trackY - 5} width={scale(z.toKm) - scale(z.fromKm)} height="10" rx="2" fill={`url(#hatch-${corridor.id})`}>
            <title>{`${z.name}: no signal, km ${z.fromKm}–${z.toKm}`}</title>
          </rect>
        ))}
        {sel && (
          <line
            className="lane-track__route"
            x1={scale(sel.vehicle.km)}
            x2={scale(corridor.lengthKm)}
            y1={trackY}
            y2={trackY}
            stroke="var(--cobalt)"
            strokeWidth="3"
            strokeLinecap="round"
          />
        )}
        {corridor.stations.map((s, i) => {
          const x = scale(s.km)
          const anchor = i === 0 ? 'start' : i === corridor.stations.length - 1 ? 'end' : 'middle'
          return (
            <g key={s.name}>
              <rect x={x - (s.depot ? 5 : 3)} y={trackY - (s.depot ? 5 : 3)} width={s.depot ? 10 : 6} height={s.depot ? 10 : 6} rx={s.depot ? 2 : 3} fill="var(--surface)" stroke="var(--ink-2)" strokeWidth="1.5">
                <title>{`${s.name} · km ${s.km}${s.depot ? ' · depot with cold storage' : ''}`}</title>
              </rect>
              {labelled.has(s.name) && (
                <>
                  <text x={x} y={trackY + 24} textAnchor={anchor} className="lane-track__station">
                    {s.name}
                  </text>
                  <text x={x} y={trackY + 40} textAnchor={anchor} className="lane-track__km">
                    {s.km} km
                  </text>
                </>
              )}
            </g>
          )
        })}
        {ticks.map(({ vehicle }) => (
          <circle key={vehicle.vehicleId} cx={scale(vehicle.km)} cy={trackY} r="3.5" fill="var(--ink-3)" stroke="var(--surface)" strokeWidth="2" />
        ))}
        {placed.map((p) => (
          <g key={p.vehicle.vehicleId}>
            <line x1={p.x} x2={p.x} y1={tagTop(p.row) + TAG_H} y2={trackY - 6} stroke={p.vehicle.vehicleId === selected ? 'var(--cobalt)' : 'var(--track)'} strokeWidth="1" strokeDasharray={p.vehicle.estimated ? '3 3' : undefined} />
            <circle cx={p.x} cy={trackY} r="5" fill={p.vehicle.estimated ? 'var(--surface)' : 'var(--ink)'} stroke={p.vehicle.estimated ? 'var(--ink-2)' : 'var(--surface)'} strokeWidth="2" />
          </g>
        ))}
      </svg>

      {placed.map((p) => {
        const { vehicle, shipment } = p
        const risk = shipment.risk
        const isSel = vehicle.vehicleId === selected
        const range = risk.ttbP10Min === null ? null : `${risk.ttbP10Min}–${risk.ttbP90Min}m`
        return (
          <button
            key={vehicle.vehicleId}
            type="button"
            className={`tag tag--${risk.aspect}${isSel ? ' tag--selected' : ''}${vehicle.estimated ? ' tag--estimated' : ''}`}
            style={{ left: p.left, top: tagTop(p.row), width: TAG_W, height: TAG_H }}
            onClick={() => onSelect(vehicle.vehicleId)}
            aria-pressed={isSel}
            aria-label={`${vehicle.vehicleId}, ${shipment.cargo.name}, ${ASPECT_LABEL[risk.aspect]}${range ? `, breach in ${range}` : ''}, cargo ${fmtTemp(vehicle.cargoC)}${vehicle.estimated ? `, estimated, last heard ${fmtAge(vehicle.lastFixAgeS * 1000)} ago` : ''}`}
          >
            <SignalHead aspect={risk.aspect} size={0.9} />
            <span className="tag__body">
              <span className="tag__id">{vehicle.vehicleId}</span>
              <span className="tag__meta num">
                {vehicle.estimated ? `est. ${fmtAge(vehicle.lastFixAgeS * 1000)}` : fmtTemp(vehicle.cargoC)}
              </span>
            </span>
            {range && <span className="tag__ttb num">{range}</span>}
          </button>
        )
      })}
    </div>
  )
}
