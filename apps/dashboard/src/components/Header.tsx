import { Link, useRouterState } from '@tanstack/react-router'
import { Moon, Search, Sun } from 'lucide-react'
import { useLayoutEffect, useRef, useState } from 'react'
import { useAppStore } from '../state/context'
import { useStore } from '../state/store'
import { Guide } from './Guide'
import { NAV } from './nav'


export function Wordmark() {
  return (
    <span className="wordmark">
      <svg width="14" height="22" viewBox="0 0 12 30" aria-hidden="true">
        <rect width="12" height="30" rx="6" fill="var(--ink)" />
        <circle cx="6" cy="6" r="3.2" fill="var(--surface)" opacity="0.35" />
        <circle cx="6" cy="15" r="3.2" fill="var(--surface)" opacity="0.35" />
        <circle cx="6" cy="24" r="3.2" fill="var(--cobalt)" />
      </svg>
      Watchtower
    </span>
  )
}

/** Segmented pill nav with a sliding indicator (state-bearing motion only). */
function NavPills() {
  const path = useRouterState({ select: (s) => s.location.pathname })
  const listRef = useRef<HTMLDivElement>(null)
  const [indicator, setIndicator] = useState<{ x: number; w: number; navW: number } | null>(null)
  useLayoutEffect(() => {
    const nav = listRef.current
    const active = nav?.querySelector<HTMLElement>('[data-active="true"]')
    if (nav && active) setIndicator({ x: active.offsetLeft, w: active.offsetWidth, navW: nav.offsetWidth })
  }, [path])
  return (
    <nav className="nav-pills" aria-label="Views" ref={listRef}>
      {indicator && (
        <span
          className="nav-pills__indicator"
          style={{ clipPath: `inset(0 ${indicator.navW - 8 - (indicator.x - 4) - indicator.w}px 0 ${indicator.x - 4}px round 999px)` }}
          aria-hidden="true"
        />
      )}
      {NAV.map(({ to, label, icon: Icon }) => {
        const active = to === '/' ? path === '/' : path.startsWith(to)
        return (
          <Link key={to} to={to} className="nav-pills__item" data-active={active} aria-current={active ? 'page' : undefined}>
            <Icon size={15} aria-hidden="true" />
            {label}
          </Link>
        )
      })}
    </nav>
  )
}

export function LivePill() {
  const store = useAppStore()
  const mode = useStore(store, (s) => s.mode)
  return mode === 'live' ? (
    <span className="live-pill" role="status">
      <span className="live-pill__dot" aria-hidden="true" />
      Live
    </span>
  ) : (
    <button type="button" className="live-pill live-pill--replay" onClick={store.goLive}>
      Replay · back to live
    </button>
  )
}

export function ThemeToggle() {
  const store = useAppStore()
  const theme = useStore(store, (s) => s.theme)
  const next = theme === 'light' ? 'dark' : 'light'
  return (
    <button type="button" className="icon-btn" onClick={() => store.setTheme(next)} aria-label={`Switch to ${next === 'dark' ? 'Operating Centre (dark)' : 'Enamel (light)'} theme`}>
      {theme === 'light' ? <Moon size={17} aria-hidden="true" /> : <Sun size={17} aria-hidden="true" />}
    </button>
  )
}

export function Header() {
  const store = useAppStore()
  return (
    <header className="header">
      <Wordmark />
      <NavPills />
      <div className="header__end">
        <button type="button" className="search-btn" onClick={() => store.setPalette(true)}>
          <Search size={15} aria-hidden="true" />
          <span>Search</span>
          <kbd>⌘K</kbd>
        </button>
        <Guide />
        <ThemeToggle />
        <LivePill />
      </div>
    </header>
  )
}
