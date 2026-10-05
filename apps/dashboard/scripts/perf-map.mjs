// Map page frame-rate probe, on a cold browser profile (no shader or HTTP cache),
// which is what a first-time visitor gets.
//   settleMs: from navigation until frames hold >= 50 fps for a full second
//   fps, longTaskMs: rAF frames/s and long-task time over the next 6 s
// Env: GL=swiftshader (no GPU), GUIDE=open (first-visit guide over the map),
//      SELECT=1 (select a vehicle first: trail, route ahead, halo, label).
import { chromium } from '@playwright/test'
const url = process.argv[2] ?? 'http://localhost:5173/map'
const browser = await chromium.launch({ args: (process.env.GL ?? 'gpu') === 'gpu' ? ['--enable-gpu', '--use-angle=d3d11', '--ignore-gpu-blocklist'] : ['--use-angle=swiftshader', '--enable-unsafe-swiftshader'] })
const context = await browser.newContext({ viewport: { width: 1440, height: 900 } })
// Measure the map itself: the first-visit guide would otherwise sit over it.
if (process.env.GUIDE !== 'open') await context.addInitScript(() => localStorage.setItem('wt-guide-seen', '1'))
await context.addInitScript(() => {
  // Frame timestamps from the very first frame, for the settle time.
  const frames = (window.__frames = [])
  const tick = (t) => {
    frames.push(t)
    requestAnimationFrame(tick)
  }
  requestAnimationFrame(tick)
})
const page = await context.newPage()
await page.goto(url, { waitUntil: 'load' })
if (process.env.SELECT) {
  await page.locator('.fleet-row').first().click()
  await page.waitForTimeout(1500) // camera ease
}
const settleMs = await page.evaluate(
  () =>
    new Promise((resolve) => {
      const check = () => {
        const f = window.__frames
        // First frame at which the preceding second held >= 50 frames.
        for (let i = 50; i < f.length; i++) if (f[i] - f[i - 50] <= 1000) return resolve(Math.round(f[i]))
        if (performance.now() > 60_000) return resolve(null)
        setTimeout(check, 250)
      }
      check()
    }),
)
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
console.log(JSON.stringify({ settleMs, ...r }))
await browser.close()
