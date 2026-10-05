import { createContext, useContext, useMemo } from 'react'
import { ASPECT_RANK } from '../domain/risk'
import type { Frame, Incident, Shipment } from '../domain/types'
import { type Store, useStore, withOverrides } from './store'

export const StoreContext = createContext<Store | null>(null)

export function useAppStore(): Store {
  const store = useContext(StoreContext)
  if (!store) throw new Error('StoreContext missing')
  return store
}

const SEVERITY_RANK = { CRITICAL: 0, HIGH: 1, MEDIUM: 2, LOW: 3 } as const
const STATE_RANK = { OPEN: 0, ACKNOWLEDGED: 1, MITIGATING: 2, RESOLVED: 3, AUTO_CLEARED: 4 } as const

export interface View extends Frame {
  shipmentsByRisk: Shipment[]
  incidentsByUrgency: Incident[]
  shipmentFor(vehicleId: string): Shipment | undefined
  incidentsFor(vehicleId: string): Incident[]
}

/** The frame at the playhead, with operator actions applied and sorted for display. */
export function useView(): View {
  const store = useAppStore()
  const playhead = useStore(store, (s) => s.playhead)
  const overrides = useStore(store, (s) => s.overrides)
  const frame = store.timeline.frameAt(playhead)
  return useMemo(() => {
    const incidents = withOverrides(frame.incidents, overrides)
    const shipmentsByRisk = [...frame.shipments].sort(
      (a, b) => ASPECT_RANK[a.risk.aspect] - ASPECT_RANK[b.risk.aspect] || b.risk.expectedLossNgn - a.risk.expectedLossNgn,
    )
    const incidentsByUrgency = [...incidents].sort(
      (a, b) =>
        STATE_RANK[a.state] - STATE_RANK[b.state] ||
        SEVERITY_RANK[a.severity] - SEVERITY_RANK[b.severity] ||
        a.openedAt - b.openedAt,
    )
    return {
      ...frame,
      incidents,
      shipmentsByRisk,
      incidentsByUrgency,
      shipmentFor: (id) => frame.shipments.find((s) => s.vehicleId === id),
      incidentsFor: (id) => incidentsByUrgency.filter((i) => i.vehicleId === id),
    }
  }, [frame, overrides])
}

export const fmtTime = (t: number) =>
  new Intl.DateTimeFormat('en-GB', { timeZone: 'Africa/Lagos', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false }).format(t)

export const fmtClock = (t: number) =>
  new Intl.DateTimeFormat('en-GB', { timeZone: 'Africa/Lagos', hour: '2-digit', minute: '2-digit', hour12: false }).format(t)

export function fmtAge(ms: number): string {
  const min = Math.max(0, Math.round(ms / 60_000))
  if (min < 1) return 'now'
  if (min < 60) return `${min}m`
  return `${Math.floor(min / 60)}h ${String(min % 60).padStart(2, '0')}m`
}

export const fmtNgn = (n: number) =>
  n >= 1_000_000 ? `₦${(n / 1_000_000).toFixed(1)}M` : n >= 1000 ? `₦${Math.round(n / 1000)}K` : `₦${n}`

export const fmtTemp = (c: number | null) => (c === null ? '—' : `${c.toFixed(1)} °C`)
