import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

export default defineConfig({
  plugins: [react()],
  // MapLibre loads its worker from a sibling file; pre-bundling moves the
  // module into .vite/deps and breaks that relative URL.
  optimizeDeps: { exclude: ['maplibre-gl'] },
  // MapLibre starts its worker as a module worker.
  worker: { format: 'es' },
  build: {
    // The map chunk (MapLibre + deck.gl) is lazy-loaded; keep the warning meaningful for the shell.
    chunkSizeWarningLimit: 2000,
  },
})
