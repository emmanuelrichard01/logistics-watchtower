import '@fontsource-variable/geist'
import '@fontsource-variable/geist-mono'
import './styles/tokens.css'
import './styles/base.css'
import './styles/app.css'
import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { App } from './App'
import { buildSyntheticTimeline } from './data/synthetic'
import { createStore } from './state/store'

const store = createStore(buildSyntheticTimeline())

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App store={store} />
  </StrictMode>,
)
