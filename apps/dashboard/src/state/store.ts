import { useSyncExternalStore } from 'react'
import type { Incident, IncidentState, Timeline } from '../domain/types'

// One time handle drives every view (direction contract, signature interaction).
// Live advances the playhead at the recording's pace; replay is any other time.

export const REPLAY_SPEEDS = [1, 4, 16] as const
const LIVE_RATE = 15 // simulated seconds per real second, shown in the chrome

export interface Override {
  state: IncidentState
  at: number
  note?: string
}

interface State {
  /** Quantised to the timeline step so React re-renders at most once per step. */
  playhead: number
  mode: 'live' | 'replay'
  playing: boolean
  speed: (typeof REPLAY_SPEEDS)[number]
  selected: string | null // vehicleId
  detail: boolean // shipment evidence layer open
  follow: boolean // map camera follows the selected vehicle
  overrides: Record<string, Override>
  theme: 'light' | 'dark'
  paletteOpen: boolean
}

type Listener = () => void

function initialTheme(): 'light' | 'dark' {
  try {
    const saved = localStorage.getItem('wt-theme')
    if (saved === 'light' || saved === 'dark') return saved
  } catch {
    /* storage unavailable: fall through */
  }
  return window.matchMedia?.('(prefers-color-scheme: dark)').matches ? 'dark' : 'light'
}

export function createStore(timeline: Timeline) {
  // Start partway in so the board has history to show, leaving most of the run live.
  const liveStart = timeline.start + Math.min(95 * 60_000, (timeline.end - timeline.start) * 0.4)
  let state: State = {
    playhead: liveStart - ((liveStart - timeline.start) % timeline.stepMs),
    mode: 'live',
    playing: true,
    speed: 4,
    selected: null,
    detail: false,
    follow: false,
    overrides: {},
    theme: initialTheme(),
    paletteOpen: false,
  }
  let liveEdge = liveStart
  let precise = liveStart // continuous playhead for animation loops (never React)
  const listeners = new Set<Listener>()
  const emit = () => listeners.forEach((l) => l())
  const set = (patch: Partial<State>) => {
    state = { ...state, ...patch }
    emit()
  }
  const quantise = (t: number) => timeline.start + Math.floor((t - timeline.start) / timeline.stepMs) * timeline.stepMs
  const movePlayhead = (t: number, patch: Partial<State> = {}) => {
    precise = t
    const q = quantise(t)
    if (q !== state.playhead || Object.keys(patch).length > 0) set({ playhead: q, ...patch })
  }

  let last = performance.now()
  const tick = (now: number) => {
    const dt = (now - last) / 1000
    last = now
    liveEdge = Math.min(timeline.end, liveEdge + dt * LIVE_RATE * 1000)
    if (state.mode === 'live') {
      movePlayhead(liveEdge)
    } else if (state.playing) {
      const next = Math.min(liveEdge, precise + dt * LIVE_RATE * state.speed * 1000)
      if (next >= liveEdge) movePlayhead(liveEdge, { mode: 'live' })
      else movePlayhead(next)
    }
    requestAnimationFrame(tick)
  }
  requestAnimationFrame(tick)

  return {
    timeline,
    getState: () => state,
    liveEdge: () => liveEdge,
    precise: () => precise,
    subscribe(l: Listener) {
      listeners.add(l)
      return () => listeners.delete(l)
    },
    seek(t: number) {
      const clamped = Math.max(timeline.start, Math.min(liveEdge, t))
      if (clamped >= liveEdge - timeline.stepMs) movePlayhead(liveEdge, { mode: 'live' })
      else movePlayhead(clamped, { mode: 'replay', playing: false })
    },
    goLive: () => movePlayhead(liveEdge, { mode: 'live', playing: true }),
    togglePlay: () => set({ playing: !state.playing }),
    setSpeed: (speed: State['speed']) => set({ speed }),
    /** Select a vehicle; `detail` also opens the evidence layer. */
    select: (selected: string | null, detail = true) => set({ selected, detail: selected !== null && detail, follow: selected === null ? false : state.follow }),
    openDetail: (detail: boolean) => set({ detail }),
    setFollow: (follow: boolean) => set({ follow }),
    setTheme(theme: State['theme']) {
      try {
        localStorage.setItem('wt-theme', theme)
      } catch {
        /* non-essential */
      }
      set({ theme })
    },
    setPalette: (paletteOpen: boolean) => set({ paletteOpen }),
    act(incident: Incident, next: IncidentState, note?: string) {
      if (state.mode !== 'live') return // actions are disabled in replay
      set({ overrides: { ...state.overrides, [incident.id]: { state: next, at: state.playhead, note } } })
    },
    undo(incidentId: string) {
      const { [incidentId]: _drop, ...rest } = state.overrides
      void _drop
      set({ overrides: rest })
    },
  }
}

export type Store = ReturnType<typeof createStore>

export function useStore<T>(store: Store, select: (s: State) => T): T {
  return useSyncExternalStore(store.subscribe, () => select(store.getState()))
}

/** Operator actions layered over the stream's incident state. */
export function withOverrides(incidents: Incident[], overrides: Record<string, Override>): Incident[] {
  return incidents.map((inc) => {
    const o = overrides[inc.id]
    if (!o || inc.state === 'AUTO_CLEARED') return inc
    return { ...inc, state: o.state }
  })
}
