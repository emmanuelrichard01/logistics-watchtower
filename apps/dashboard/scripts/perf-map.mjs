// Map page frame-rate probe: rAF frames/s and long-task time over a fixed window.
import { chromium } from '@playwright/test'
const url = process.argv[2] ?? 'http://localhost:5173/map'
const browser = await chromium.launch({ args: (process.env.GL ?? 'gpu') === 'gpu' ? ['--enable-gpu', '--use-angle=d3d11', '--ignore-gpu-blocklist'] : ['--use-angle=swiftshader', '--enable-unsafe-swiftshader'] })
const page = await browser.newPage({ viewport: { width: 1440, height: 900 } })
await page.goto(url, { waitUntil: 'load' })
await page.waitForTimeout(4000) // warm: compile, tiles
const r = await page.evaluate(
  () =>
    new Promise((resolve) => {
      let frames = 0
      let longMs = 0
      const obs = new PerformanceObserver((l) => l.getEntries().forEach((e) => (longMs += e.duration)))
      obs.observe({ type: 'longtask', buffered: false })
      const t0 = performance.now()
      const tick = () => {
        frames++
        if (performance.now() - t0 < 6000) requestAnimationFrame(tick)
        else {
          obs.disconnect()
          resolve({ fps: +(frames / ((performance.now() - t0) / 1000)).toFixed(1), longTaskMs: Math.round(longMs) })
        }
      }
      requestAnimationFrame(tick)
    }),
)
console.log(JSON.stringify(r))
await browser.close()
