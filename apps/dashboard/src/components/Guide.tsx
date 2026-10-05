import { CircleHelp, X } from 'lucide-react'
import { useEffect, useRef, useState } from 'react'
import { ASPECT_LABEL } from '../domain/risk'
import type { Aspect } from '../domain/types'
import { SignalHead } from './SignalHead'

const SEEN_KEY = 'wt-guide-seen'

const ASPECTS: { aspect: Aspect; detail: string }[] = [
  { aspect: 'danger', detail: 'Cargo is beyond its limit now. Top lamp, red.' },
  { aspect: 'caution1', detail: 'Forecast to breach within 15 minutes. Middle lamp.' },
  { aspect: 'caution2', detail: 'Forecast to breach within 45 minutes. Two lamps.' },
  { aspect: 'unknown', detail: 'Too little trustworthy data to call. The reason is always shown.' },
  { aspect: 'clear', detail: 'Holding temperature, no breach forecast. Bottom lamp.' },
]

const SHORTCUTS: [string, string][] = [
  ['⌘K', 'Search trucks, incidents and commands'],
  ['L', 'Jump back to live'],
  ['Esc', 'Close the open panel'],
  ['J / K', 'Move through incidents'],
  ['A · M · R', 'Acknowledge, mitigate, resolve'],
]

function readSeen(): boolean {
  try {
    return localStorage.getItem(SEEN_KEY) === '1'
  } catch {
    return true // storage blocked: never nag
  }
}

/** How to read the board. Opens once on first visit, then from the header. */
export function Guide() {
  const [open, setOpen] = useState(() => !readSeen())
  const panel = useRef<HTMLDivElement>(null)

  const close = () => {
    setOpen(false)
    try {
      localStorage.setItem(SEEN_KEY, '1')
    } catch {
      /* non-essential */
    }
  }

  useEffect(() => {
    if (!open) return
    const onKey = (e: KeyboardEvent) => e.key === 'Escape' && close()
    const onDown = (e: PointerEvent) => {
      const target = e.target as Node
      if (panel.current && !panel.current.contains(target) && !(target as HTMLElement).closest?.('.guide-btn')) close()
    }
    window.addEventListener('keydown', onKey)
    window.addEventListener('pointerdown', onDown)
    return () => {
      window.removeEventListener('keydown', onKey)
      window.removeEventListener('pointerdown', onDown)
    }
  }, [open])

  return (
    <>
      <button type="button" className="icon-btn guide-btn" aria-expanded={open} aria-controls="guide" onClick={() => (open ? close() : setOpen(true))} aria-label="How to read the board">
        <CircleHelp size={17} aria-hidden="true" />
      </button>
      {open && (
        <div id="guide" ref={panel} className="guide" role="dialog" aria-label="How to read the board">
          <header className="guide__head">
            <h2>Reading the board</h2>
            <button type="button" className="icon-btn" onClick={close} aria-label="Close guide">
              <X size={16} aria-hidden="true" />
            </button>
          </header>
          <p className="guide__lede">Every lane is a track. Each truck carries a signal: the lamp's position tells you how soon its cargo breaches, so it reads the same without colour.</p>
          <ul className="guide__aspects">
            {ASPECTS.map(({ aspect, detail }) => (
              <li key={aspect}>
                <SignalHead aspect={aspect} />
                <span>
                  <strong>{ASPECT_LABEL[aspect]}</strong>
                  {detail}
                </span>
              </li>
            ))}
          </ul>
          <div className="guide__symbols">
            <span>
              <i className="guide__hatch" aria-hidden="true" />
              No signal: devices buffer and replay later
            </span>
            <span>
              <i className="guide__estimated" aria-hidden="true" />
              Dashed outline: position estimated, not measured
            </span>
            <span>
              <i className="guide__depot" aria-hidden="true" />
              Square: depot with cold storage
            </span>
          </div>
          <p className="guide__lede">Drag the time handle at the bottom to replay any moment; every view follows it.</p>
          <dl className="guide__keys">
            {SHORTCUTS.map(([k, v]) => (
              <div key={k}>
                <dt>
                  <kbd>{k}</kbd>
                </dt>
                <dd>{v}</dd>
              </div>
            ))}
          </dl>
          <button type="button" className="btn btn--primary guide__done" onClick={close}>
            Got it
          </button>
        </div>
      )}
    </>
  )
}
