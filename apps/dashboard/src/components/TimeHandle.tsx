import { Pause, Play, Radio } from 'lucide-react'
import { useRef } from 'react'
import { fmtClock, fmtTime, useAppStore } from '../state/context'
import { REPLAY_SPEEDS, useStore } from '../state/store'

/**
 * The one time handle that drives every view. Dragging left is replay; the
 * live edge sits at the right. Built on a native range input for keyboard and
 * screen-reader support, with the track drawn around it.
 */
export function TimeHandle() {
  const store = useAppStore()
  const { timeline } = store
  const playhead = useStore(store, (s) => s.playhead)
  const mode = useStore(store, (s) => s.mode)
  const playing = useStore(store, (s) => s.playing)
  const speed = useStore(store, (s) => s.speed)
  const liveEdge = store.liveEdge()
  const span = timeline.end - timeline.start
  const pct = (t: number) => ((t - timeline.start) / span) * 100
  const trackRef = useRef<HTMLDivElement>(null)

  const hours: number[] = []
  for (let t = Math.ceil(timeline.start / 3_600_000) * 3_600_000; t <= timeline.end; t += 1_800_000) hours.push(t)

  return (
    <div className={`time-handle${mode === 'replay' ? ' time-handle--replay' : ''}`}>
      <button
        type="button"
        className="icon-btn"
        onClick={mode === 'live' ? undefined : store.togglePlay}
        disabled={mode === 'live'}
        aria-label={mode === 'live' ? 'Live' : playing ? 'Pause replay' : 'Play replay'}
      >
        {mode === 'live' ? <Radio size={16} aria-hidden="true" /> : playing ? <Pause size={16} aria-hidden="true" /> : <Play size={16} aria-hidden="true" />}
      </button>

      <div className="time-handle__readout">
        <span className={`time-handle__mode${mode === 'live' ? ' is-live' : ''}`}>{mode === 'live' ? 'Live' : 'Replay'}</span>
        <span className="time-handle__clock num">{fmtTime(playhead)} WAT</span>
      </div>

      <div className="time-handle__track" ref={trackRef} data-hint={mode === 'live' ? 'Drag to replay' : 'Drag to scrub · L for live'}>
        <div className="time-handle__known" style={{ width: `${pct(liveEdge)}%` }} />
        <div className="time-handle__played" style={{ width: `${pct(playhead)}%` }} />
        {timeline.markers
          .filter((m) => m.t <= liveEdge)
          .map((m) => (
            <span key={`${m.vehicleId}-${m.t}`} className={`time-handle__marker time-handle__marker--${m.aspect}`} style={{ left: `${pct(m.t)}%` }} title={`${m.vehicleId} · ${fmtClock(m.t)}`} />
          ))}
        <span className="time-handle__live-edge" style={{ left: `${pct(liveEdge)}%` }} aria-hidden="true" />
        <span className="time-handle__thumb" style={{ left: `${pct(playhead)}%` }} aria-hidden="true" />
        <input
          type="range"
          className="time-handle__input"
          min={timeline.start}
          max={timeline.end}
          step={timeline.stepMs}
          value={playhead}
          aria-label="Time"
          aria-valuetext={`${fmtTime(playhead)} WAT${mode === 'live' ? ', live' : ', replay'}`}
          onChange={(e) => store.seek(Number(e.target.value))}
        />
        <div className="time-handle__hours" aria-hidden="true">
          {hours.map((t) => (
            <span key={t} style={{ left: `${pct(t)}%` }}>
              {fmtClock(t)}
            </span>
          ))}
        </div>
      </div>

      {mode === 'replay' ? (
        <div className="time-handle__end">
          <div className="segmented" role="group" aria-label="Replay speed">
            {REPLAY_SPEEDS.map((s) => (
              <button key={s} type="button" aria-pressed={speed === s} onClick={() => store.setSpeed(s)}>
                {s}×
              </button>
            ))}
          </div>
          <button type="button" className="btn btn--primary" onClick={store.goLive}>
            Go live
          </button>
        </div>
      ) : (
        <span className="time-handle__provenance">{timeline.provenance} · 15×</span>
      )}
    </div>
  )
}
