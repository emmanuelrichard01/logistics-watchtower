import { createRootRoute, createRoute, createRouter, Outlet, RouterProvider } from '@tanstack/react-router'
import { lazy, Suspense, useEffect } from 'react'
import { CommandPalette } from './components/CommandPalette'
import { Header } from './components/Header'
import { MobileNav } from './components/MobileNav'
import { ShipmentLayer } from './components/ShipmentLayer'
import { TimeHandle } from './components/TimeHandle'
import { StoreContext, useAppStore } from './state/context'
import { type Store, useStore } from './state/store'
import { HealthView } from './views/HealthView'
import { IncidentsView } from './views/IncidentsView'
import { LanesView } from './views/LanesView'

// The map pulls in MapLibre and deck.gl; keep it out of the first load.
const MapView = lazy(() => import('./views/MapView').then((m) => ({ default: m.MapView })))

function Shell() {
  const store = useAppStore()
  const theme = useStore(store, (s) => s.theme)
  const layerOpen = useStore(store, (s) => s.detail && s.selected !== null)

  useEffect(() => {
    document.documentElement.dataset.theme = theme
  }, [theme])

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k') {
        e.preventDefault()
        store.setPalette(true)
      }
      const typing = e.target instanceof HTMLInputElement || e.target instanceof HTMLTextAreaElement
      if (!typing && e.key === 'l' && !e.metaKey && !e.ctrlKey) store.goLive()
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [store])

  return (
    <div className={`app${layerOpen ? ' app--layer-open' : ''}`}>
      <a className="skip-link" href="#main">
        Skip to content
      </a>
      <Header />
      <main id="main" className="app__main">
        <Suspense fallback={<div className="map-loading">Loading map…</div>}>
          <Outlet />
        </Suspense>
      </main>
      <ShipmentLayer />
      <TimeHandle />
      <MobileNav />
      <CommandPalette />
    </div>
  )
}

const root = createRootRoute({ component: Shell })
const routeTree = root.addChildren([
  createRoute({ getParentRoute: () => root, path: '/', component: LanesView }),
  createRoute({ getParentRoute: () => root, path: '/map', component: MapView }),
  createRoute({ getParentRoute: () => root, path: '/incidents', component: IncidentsView }),
  createRoute({ getParentRoute: () => root, path: '/health', component: HealthView }),
])

const router = createRouter({ routeTree, defaultViewTransition: true })

declare module '@tanstack/react-router' {
  interface Register {
    router: typeof router
  }
}

export function App({ store }: { store: Store }) {
  return (
    <StoreContext.Provider value={store}>
      <RouterProvider router={router} />
    </StoreContext.Provider>
  )
}
