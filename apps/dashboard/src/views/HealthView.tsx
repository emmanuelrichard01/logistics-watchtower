import { useLayoutEffect, useRef, useState } from 'react'
import { SignalHead } from '../components/SignalHead'
import type { Aspect } from '../domain/types'
import { useAppStore } from '../state/context'
import { useStore } from '../state/store'

// The pipeline drawn in the same grammar as the lanes: stages are stations,
// lag sets each section's aspect, dead-letter queues are sidings.
// SIMULATED metrics until the services export them (plan section 14).

interface Stage {
  id: string
  label: string
  sub: string
}

const MAIN: Stage[] = [
  { id: 'devices', label: 'Devices', sub: 'edge buffers' },
  { id: 'gateway', label: 'Gateway', sub: 'validate · quarantine' },
  { id: 'input', label: 'wt.input.v1', sub: '12 partitions' },
  { id: 'processor', label: 'Processor', sub: 'trust · risk · alerts' },
  { id: 'projector', label: 'Projector', sub: 'Postgres' },
  { id: 'api', label: 'API', sub: 'push · REST' },
]

function wobble(t: number, seed: number, base: number, amp: number) {
  return Math.max(0, base + amp * Math.sin(t / 97_000 + seed) + (amp / 2) * Math.sin(t / 23_000 + seed * 3))
}

function aspectForLag(sec: number, warn: number, crit: number): Aspect {
  return sec >= crit ? 'danger' : sec >= warn ? 'caution1' : 'clear'
}

export function HealthView() {
  const store = useAppStore()
  const playhead = useStore(store, (s) => s.playhead)
  const ref = useRef<HTMLDivElement>(null)
  const [width, setWidth] = useState(0)
  useLayoutEffect(() => {
    const ro = new ResizeObserver(([e]) => setWidth(e.contentRect.width))
    if (ref.current) ro.observe(ref.current)
    return () => ro.disconnect()
  }, [])

  const lags = [wobble(playhead, 1, 0.4, 0.3), wobble(playhead, 2, 0.6, 0.4), wobble(playhead, 3, 1.4, 1.1), wobble(playhead, 4, 2.2, 2.4), wobble(playhead, 5, 0.3, 0.2)]
  const dlq = { processor: Math.round(wobble(playhead, 6, 0.4, 1.2)), projector: 0 }
  const quarantine = Math.round(wobble(playhead, 7, 3, 3))
  const pad = 40
  const x = (i: number) => pad + (i / (MAIN.length - 1)) * Math.max(1, width - pad * 2)
  const y = 92

  const metrics = [
    { label: 'Freshness, Bronze', value: `${Math.round(wobble(playhead, 8, 40, 18))} s`, aspect: 'clear' as Aspect },
    { label: 'Freshness, Gold', value: `${Math.round(wobble(playhead, 9, 6, 3))} min`, aspect: 'clear' as Aspect },
    { label: 'Late events (1 h)', value: `${Math.round(wobble(playhead, 10, 120, 80))}`, aspect: 'clear' as Aspect },
    { label: 'Sequence gaps (1 h)', value: `${Math.round(wobble(playhead, 11, 2, 2))}`, aspect: 'clear' as Aspect },
    { label: 'Quarantined (1 h)', value: `${quarantine}`, aspect: (quarantine > 5 ? 'caution1' : 'clear') as Aspect },
    { label: 'Dead letters waiting', value: `${dlq.processor}`, aspect: (dlq.processor > 0 ? 'caution1' : 'clear') as Aspect },
  ]

  return (
    <div className="health-view">
      <div className="status-line">
        <h1>
          Data is fresh and complete
          <span className="status-line__quiet"> · {dlq.processor > 0 ? `${dlq.processor} dead letter${dlq.processor === 1 ? '' : 's'} to review` : 'no dead letters'}</span>
        </h1>
        <p className="status-line__meta">Simulated pipeline metrics until services export them</p>
      </div>

      <section className="lane health-diagram" aria-label="Pipeline">
        <header className="lane__head">
          <h2>Pipeline</h2>
          <p className="lane__meta">Lag in seconds between stages</p>
        </header>
        <div ref={ref} className="health-diagram__canvas">
          {width > 0 && (
            <svg width={width} height="190" role="img" aria-label="Pipeline stages with lag between them">
              {MAIN.slice(0, -1).map((s, i) => {
                const aspect = aspectForLag(lags[i], 5, 30)
                return (
                  <g key={s.id}>
                    <line x1={x(i)} x2={x(i + 1)} y1={y} y2={y} stroke={aspect === 'clear' ? 'var(--track)' : aspect === 'danger' ? 'var(--danger)' : 'var(--caution)'} strokeWidth="3" />
                    <text x={(x(i) + x(i + 1)) / 2} y={y - 12} textAnchor="middle" className="axis-text num">
                      {lags[i].toFixed(1)} s
                    </text>
                  </g>
                )
              })}
              {/* Sidings: dead-letter queues and quarantine */}
              {[
                { at: 1, label: `Quarantine · ${quarantine}` },
                { at: 3, label: `DLQ · ${dlq.processor}` },
                { at: 4, label: `DLQ · ${dlq.projector}` },
              ].map((s) => (
                <g key={s.label}>
                  <path d={`M${x(s.at)},${y} q 18,0 30,34 l 40,0`} fill="none" stroke="var(--track)" strokeWidth="2" />
                  <text x={x(s.at) + 74} y={y + 38} className="axis-text num">
                    {s.label}
                  </text>
                </g>
              ))}
              {/* Archive branch */}
              <path d={`M${x(2)},${y} q 0,-50 40,-56 l 60,0`} fill="none" stroke="var(--track)" strokeWidth="2" />
              <text x={x(2) + 106} y={y - 52} className="axis-text">
                Archiver → Bronze (Parquet)
              </text>
              {MAIN.map((s, i) => (
                <g key={s.id}>
                  <rect x={x(i) - 6} y={y - 6} width="12" height="12" rx="3" fill="var(--surface)" stroke="var(--ink)" strokeWidth="2" />
                  <text x={x(i)} y={y + 66} textAnchor="middle" className="lane-track__station">
                    {s.label}
                  </text>
                  <text x={x(i)} y={y + 82} textAnchor="middle" className="lane-track__km">
                    {s.sub}
                  </text>
                </g>
              ))}
            </svg>
          )}
        </div>
      </section>

      <dl className="health-metrics">
        {metrics.map((m) => (
          <div key={m.label} className="health-metric">
            <SignalHead aspect={m.aspect} size={0.8} />
            <dt>{m.label}</dt>
            <dd className="num">{m.value}</dd>
          </div>
        ))}
      </dl>
    </div>
  )
}
