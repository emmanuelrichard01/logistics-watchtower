import { type ReactNode, useEffect, useRef, useState } from 'react'

const SNAPS = ['peek', 'half', 'full'] as const
export type Snap = (typeof SNAPS)[number]

function useIsPhone() {
  const query = '(max-width: 760px)'
  const [phone, setPhone] = useState(() => window.matchMedia(query).matches)
  useEffect(() => {
    const mq = window.matchMedia(query)
    const on = () => setPhone(mq.matches)
    mq.addEventListener('change', on)
    return () => mq.removeEventListener('change', on)
  }, [])
  return phone
}

/**
 * A floating panel on desktop; on phones, a draggable bottom sheet with peek,
 * half and full snap points. Dragging follows the finger; release snaps to the
 * nearest point, biased by flick velocity.
 */
export function Sheet({ children, className = '', label, initial = 'peek' }: { children: ReactNode; className?: string; label: string; initial?: Snap }) {
  const phone = useIsPhone()
  const [snap, setSnap] = useState<Snap>(initial)
  const [drag, setDrag] = useState<number | null>(null)
  const start = useRef<{ y: number; t: number; base: number } | null>(null)
  const ref = useRef<HTMLElement>(null)

  const heightFor = (s: Snap) => {
    const vh = window.innerHeight
    return s === 'peek' ? 168 : s === 'half' ? vh * 0.5 : vh * 0.88
  }

  if (!phone) {
    return (
      <section className={`panel ${className}`} aria-label={label}>
        {children}
      </section>
    )
  }

  // The sheet is always full height and slides; translating avoids animating layout.
  const visible = drag ?? heightFor(snap)
  return (
    <section
      ref={ref}
      className={`sheet ${className}${drag !== null ? ' sheet--dragging' : ''}`}
      style={{ transform: `translateY(calc(88vh - ${visible}px))` }}
      aria-label={label}
    >
      <button
        type="button"
        className="sheet__handle"
        aria-label={`Resize panel, currently ${snap}`}
        onClick={() => setSnap(SNAPS[(SNAPS.indexOf(snap) + 1) % SNAPS.length])}
        onPointerDown={(e) => {
          e.currentTarget.setPointerCapture(e.pointerId)
          start.current = { y: e.clientY, t: performance.now(), base: heightFor(snap) }
        }}
        onPointerMove={(e) => {
          if (!start.current) return
          const h = start.current.base + (start.current.y - e.clientY)
          setDrag(Math.max(96, Math.min(window.innerHeight * 0.92, h)))
        }}
        onPointerUp={(e) => {
          if (!start.current) return
          const dy = start.current.y - e.clientY
          const v = dy / Math.max(1, performance.now() - start.current.t) // px/ms, up is positive
          const h = start.current.base + dy + v * 180
          start.current = null
          if (Math.abs(dy) > 6) {
            const nearest = SNAPS.reduce((best, s) => (Math.abs(heightFor(s) - h) < Math.abs(heightFor(best) - h) ? s : best), snap)
            setSnap(nearest)
          }
          setDrag(null)
        }}
      >
        <span aria-hidden="true" />
      </button>
      <div className="sheet__body">{children}</div>
    </section>
  )
}
