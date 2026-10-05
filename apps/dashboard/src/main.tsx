import '@fontsource-variable/geist'
import '@fontsource-variable/geist-mono'
import './styles/tokens.css'
import './styles/base.css'
import './styles/app.css'
import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { App } from './App'
import { loadRecordingTimeline } from './data/recording'
import { buildSyntheticTimeline } from './data/synthetic'
import type { Timeline } from './domain/types'
import { createStore } from './state/store'

const root = createRoot(document.getElementById('root')!)

// The simulator recordings are the default data source; `?data=synthetic`
// (or a failed fixture load) falls back to the seeded synthetic fleet.
async function load(): Promise<Timeline> {
  if (new URLSearchParams(location.search).get('data') === 'synthetic') return buildSyntheticTimeline()
  try {
    return await loadRecordingTimeline()
  } catch (err) {
    console.warn('[console] recordings unavailable, using the synthetic fleet', err)
    return buildSyntheticTimeline()
  }
}

load().then((timeline) => {
  root.render(
    <StrictMode>
      <App store={createStore(timeline)} />
    </StrictMode>,
  )
})
