import { useLayoutEffect, useRef, useState } from 'react'
import type { CargoProfile, Risk, SeriesPoint } from '../domain/types'
import { fmtClock, fmtTemp } from '../state/context'

const SERIES = [
  { key: 'cargoC', label: 'Cargo', color: 'var(--series-cargo)' },
  { key: 'returnAirC', label: 'Return air', color: 'var(--series-return)' },
  { key: 'supplyAirC', label: 'Supply air', color: 'var(--series-supply)' },
] as const

const M = { top: 16, right: 84, bottom: 34, left: 40 }

function niceTicks(min: number, max: number, count = 5): number[] {
  const span = max - min
  const step = [1, 2, 2.5, 5, 10].map((m) => m * 10 ** Math.floor(Math.log10(span / count))).find((s) => span / s <= count) ?? span / count
  const out: number[] = []
  for (let v = Math.ceil(min / step) * step; v <= max + 1e-9; v += step) out.push(Math.round(v * 100) / 100)
  return out
}

function minuteMeans(raw: SeriesPoint[]): SeriesPoint[] {
  const buckets = new Map<number, SeriesPoint[]>()
  for (const p of raw) {
    const m = Math.floor(p.t / 60_000) * 60_000
    const list = buckets.get(m)
    if (list) list.push(p)
    else buckets.set(m, [p])
  }
  const mean = (xs: (number | null)[]) => {
    const v = xs.filter((x): x is number => x !== null)
    return v.length ? v.reduce((s, x) => s + x, 0) / v.length : null
  }
  return [...buckets.entries()].map(([t, ps]) => ({
    t: t + 30_000,
    cargoC: mean(ps.map((p) => p.cargoC)),
    returnAirC: mean(ps.map((p) => p.returnAirC)),
    supplyAirC: mean(ps.map((p) => p.supplyAirC)),
    door: ps.some((p) => p.door),
    defrost: ps.some((p) => p.defrost),
    gap: ps.every((p) => p.gap),
  }))
}

export function TempChart({
  points,
  now,
  from,
  to,
  cargo,
  risk,
}: {
  points: SeriesPoint[]
  now: number
  from: number
  to: number
  cargo: CargoProfile
  risk: Risk
}) {
  const ref = useRef<HTMLDivElement>(null)
  const [width, setWidth] = useState(0)
  const [hover, setHover] = useState<SeriesPoint | null>(null)
  useLayoutEffect(() => {
    const el = ref.current
    if (!el) return
    const ro = new ResizeObserver(([e]) => setWidth(e.contentRect.width))
    ro.observe(el)
    return () => ro.disconnect()
  }, [])

  // Plot minute means: the same minute buckets the processor keeps. Raw 15 s
  // readings show every thermostat cycle and bury the trend.
  points = minuteMeans(points)
  const height = 260
  const plotW = Math.max(10, width - M.left - M.right)
  const plotH = height - M.top - M.bottom
  const values = points.flatMap((p) => [p.cargoC, p.returnAirC, p.supplyAirC]).filter((v): v is number => v !== null)
  const lo = Math.min(cargo.minC, ...values) - 1
  const hi = Math.max(cargo.maxC, ...values) + 1
  const x = (t: number) => M.left + ((t - from) / (to - from)) * plotW
  const y = (c: number) => M.top + (1 - (c - lo) / (hi - lo)) * plotH
  const ticks = niceTicks(lo, hi)

  const path = (key: (typeof SERIES)[number]['key']) => {
    let d = ''
    let pen = false
    for (const p of points) {
      const v = p[key]
      if (v === null) {
        pen = false
        continue
      }
      d += `${pen ? 'L' : 'M'}${x(p.t).toFixed(1)},${y(v).toFixed(1)}`
      pen = true
    }
    return d
  }

  // Gap spans: contiguous runs of not-yet-received readings.
  const gaps: [number, number][] = []
  for (const p of points) {
    if (!p.gap) continue
    const last = gaps.at(-1)
    if (last && p.t - last[1] <= 16_000) last[1] = p.t
    else gaps.push([p.t, p.t])
  }
  const doorTicks = points.filter((p, i) => p.door && !points[i - 1]?.door)
  const defrostTicks = points.filter((p, i) => p.defrost && !points[i - 1]?.defrost)
  const lastKnown = [...points].reverse().find((p) => p.cargoC !== null)

  const fan =
    risk.ttbP10Min !== null && risk.ttbP90Min !== null && lastKnown?.cargoC != null
      ? {
          d: `M${x(now)},${y(lastKnown.cargoC)} L${x(Math.min(to, now + risk.ttbP10Min * 60_000))},${y(cargo.maxC)} L${x(Math.min(to, now + risk.ttbP90Min * 60_000))},${y(cargo.maxC)} Z`,
          mid: x(Math.min(to, now + ((risk.ttbP10Min + risk.ttbP90Min) / 2) * 60_000)),
        }
      : null

  const onMove = (e: React.PointerEvent<SVGRectElement>) => {
    const box = e.currentTarget.getBoundingClientRect()
    const t = from + ((e.clientX - box.left) / box.width) * (to - from)
    let best: SeriesPoint | null = null
    for (const p of points) if (!best || Math.abs(p.t - t) < Math.abs(best.t - t)) best = p
    setHover(best && best.t <= now ? best : null)
  }

  return (
    <figure className="temp-chart" ref={ref}>
      <figcaption className="temp-chart__legend">
        {SERIES.map((s) => (
          <span key={s.key}>
            <i style={{ background: s.color }} aria-hidden="true" />
            {s.label}
          </span>
        ))}
        <span>
          <i className="temp-chart__legend-fan" aria-hidden="true" />
          Forecast range
        </span>
        <span>
          <i className="temp-chart__legend-gap" aria-hidden="true" />
          No signal
        </span>
      </figcaption>
      {width > 0 && (
        <svg width={width} height={height} role="img" aria-label={`Temperatures from ${fmtClock(from)} to ${fmtClock(now)} WAT against the ${cargo.maxC} °C limit`}>
          <defs>
            <pattern id="chart-hatch" width="6" height="6" patternUnits="userSpaceOnUse" patternTransform="rotate(45)">
              <line x1="0" y1="0" x2="0" y2="6" stroke="var(--hatch)" strokeWidth="1.2" />
            </pattern>
          </defs>
          {ticks.map((v) => (
            <g key={v}>
              <line x1={M.left} x2={M.left + plotW} y1={y(v)} y2={y(v)} stroke="var(--hairline)" />
              <text x={M.left - 8} y={y(v) + 4} textAnchor="end" className="axis-text">
                {v}
              </text>
            </g>
          ))}
          {/* Future: everything right of now is forecast, never measurement. */}
          <rect x={x(now)} y={M.top} width={M.left + plotW - x(now)} height={plotH} fill="var(--surface-sunk)" />
          {gaps.map(([a, b]) => (
            <rect key={a} x={x(a)} y={M.top} width={Math.max(2, x(b) - x(a))} height={plotH} fill="url(#chart-hatch)" opacity="0.7" />
          ))}
          <line x1={M.left} x2={M.left + plotW} y1={y(cargo.maxC)} y2={y(cargo.maxC)} stroke="var(--ink-2)" strokeWidth="1" />
          <text x={M.left + plotW + 8} y={y(cargo.maxC) + 4} className="axis-text axis-text--strong">
            Limit {cargo.maxC} °C
          </text>
          {fan && (
            <>
              <path d={fan.d} fill="var(--series-cargo)" opacity="0.12" />
              <line x1={x(now)} y1={y(lastKnown!.cargoC!)} x2={fan.mid} y2={y(cargo.maxC)} stroke="var(--series-cargo)" strokeWidth="1.5" strokeDasharray="4 4" />
            </>
          )}
          {SERIES.map((s) => (
            <path key={s.key} d={path(s.key)} fill="none" stroke={s.color} strokeWidth="2" strokeLinejoin="round" strokeLinecap="round" />
          ))}
          {lastKnown &&
            SERIES.map((s, i) => {
              const v = lastKnown[s.key]
              if (v === null) return null
              return (
                <g key={s.key}>
                  <circle cx={x(lastKnown.t)} cy={y(v)} r="4" fill={s.color} stroke="var(--surface)" strokeWidth="2" />
                  {i === 0 && (
                    <text x={x(lastKnown.t) + 9} y={y(v) - 8} className="axis-text axis-text--strong">
                      {fmtTemp(v)}
                    </text>
                  )}
                </g>
              )
            })}
          {[...doorTicks.map((p) => ({ p, label: 'Door' })), ...defrostTicks.map((p) => ({ p, label: 'Defrost' }))].map(({ p, label }) => (
            <g key={`${label}-${p.t}`}>
              <line x1={x(p.t)} x2={x(p.t)} y1={M.top + plotH} y2={M.top + plotH - 8} stroke="var(--ink-2)" strokeWidth="1.5" />
              <text x={x(p.t)} y={M.top + plotH - 12} textAnchor="middle" className="axis-text">
                {label}
              </text>
            </g>
          ))}
          <line x1={x(now)} x2={x(now)} y1={M.top - 4} y2={M.top + plotH} stroke="var(--ink)" strokeWidth="1" />
          <text x={x(now)} y={height - 8} textAnchor="middle" className="axis-text axis-text--strong">
            Now
          </text>
          <text x={M.left} y={height - 8} className="axis-text">
            {fmtClock(from)}
          </text>
          <text x={M.left + plotW} y={height - 8} textAnchor="end" className="axis-text">
            +{Math.round((to - now) / 60_000)} min
          </text>
          {hover && (
            <g pointerEvents="none">
              <line x1={x(hover.t)} x2={x(hover.t)} y1={M.top} y2={M.top + plotH} stroke="var(--ink-3)" strokeWidth="1" />
              {SERIES.map((s) => (hover[s.key] === null ? null : <circle key={s.key} cx={x(hover.t)} cy={y(hover[s.key]!)} r="4" fill={s.color} stroke="var(--surface)" strokeWidth="2" />))}
            </g>
          )}
          <rect x={M.left} y={M.top} width={plotW} height={plotH} fill="transparent" onPointerMove={onMove} onPointerLeave={() => setHover(null)} />
        </svg>
      )}
      {hover && (
        <div className="chart-tooltip num" style={{ left: Math.min(width - 180, x(hover.t) + 12), top: 40 }} role="status">
          <strong>{fmtClock(hover.t)} WAT</strong>
          {hover.gap ? (
            <span>No signal</span>
          ) : (
            SERIES.map((s) => (
              <span key={s.key}>
                <i style={{ background: s.color }} aria-hidden="true" />
                {s.label} <b>{fmtTemp(hover[s.key])}</b>
              </span>
            ))
          )}
        </div>
      )}
    </figure>
  )
}
