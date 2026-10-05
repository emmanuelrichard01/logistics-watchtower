import { ASPECT_LABEL } from '../domain/risk'
import type { Aspect } from '../domain/types'

// A signal head: lamp position and count carry the aspect, so meaning never
// depends on colour alone. Top: danger red / upper yellow. Middle: yellow.
// Bottom: clear green. Unknown: every lamp hollow.
const LIT: Record<Aspect, ('top' | 'mid' | 'bottom')[]> = {
  danger: ['top'],
  caution2: ['top', 'mid'],
  caution1: ['mid'],
  clear: ['bottom'],
  unknown: [],
}

const Y = { top: 6, mid: 15, bottom: 24 } as const

function lampColor(aspect: Aspect, pos: keyof typeof Y): string {
  if (!LIT[aspect].includes(pos)) return 'var(--lamp-off)'
  if (aspect === 'danger') return 'var(--danger)'
  if (aspect === 'clear') return 'var(--clear)'
  return 'var(--caution)'
}

export function SignalHead({ aspect, size = 1, escalating = false }: { aspect: Aspect; size?: number; escalating?: boolean }) {
  const w = 12 * size
  const h = 30 * size
  return (
    <span className={`signal-head${escalating ? ' signal-head--escalating' : ''}`} data-aspect={aspect} role="img" aria-label={ASPECT_LABEL[aspect]}>
      <svg width={w} height={h} viewBox="0 0 12 30" aria-hidden="true">
        <rect x="0" y="0" width="12" height="30" rx="6" fill="var(--signal-head)" />
        {(Object.keys(Y) as (keyof typeof Y)[]).map((pos) =>
          aspect === 'unknown' ? (
            <circle key={pos} cx="6" cy={Y[pos]} r="2.6" fill="none" stroke="var(--ink-3)" strokeWidth="1.2" />
          ) : (
            <circle key={pos} cx="6" cy={Y[pos]} r="3.2" fill={lampColor(aspect, pos)} />
          ),
        )}
      </svg>
      <span key={aspect} className="signal-head__proving" aria-hidden="true" />
    </span>
  )
}
