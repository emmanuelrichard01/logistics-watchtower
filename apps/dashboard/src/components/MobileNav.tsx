import { Link, useRouterState } from '@tanstack/react-router'
import { useView } from '../state/context'
import { NAV } from './nav'

/** Phone tab bar (hidden on wider screens via CSS). */
export function MobileNav() {
  const path = useRouterState({ select: (s) => s.location.pathname })
  const view = useView()
  const open = view.incidents.filter((i) => i.state === 'OPEN').length
  return (
    <nav className="mobile-nav" aria-label="Views">
      {NAV.map(({ to, label, icon: Icon }) => {
        const active = to === '/' ? path === '/' : path.startsWith(to)
        return (
          <Link key={to} to={to} className="mobile-nav__item" aria-current={active ? 'page' : undefined}>
            <span className="mobile-nav__icon">
              <Icon size={20} aria-hidden="true" />
              {to === '/incidents' && open > 0 && <span className="mobile-nav__badge num">{open}</span>}
            </span>
            {label}
          </Link>
        )
      })}
    </nav>
  )
}
