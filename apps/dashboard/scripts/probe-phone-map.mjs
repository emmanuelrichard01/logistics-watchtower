// Probe: does the phone map draw its basemap under mobile emulation?
// Measures real pixel variety of the map canvas region, not request counts.
import { chromium } from '@playwright/test'
const browser = await chromium.launch({ args: ['--enable-gpu', '--use-angle=d3d11', '--ignore-gpu-blocklist'] })
for (const mobile of [true, false]) {
  const ctx = await browser.newContext({ viewport: { width: 390, height: 844 }, isMobile: mobile, hasTouch: mobile, deviceScaleFactor: 2 })
  await ctx.addInitScript(() => { localStorage.setItem('wt-theme', 'dark'); localStorage.setItem('wt-guide-seen', '1') })
  const page = await ctx.newPage()
  await page.goto(process.env.URL ?? 'http://localhost:5173/map', { waitUntil: 'load' })
  for (const wait of [4000, 8000, 8000]) {
    await page.waitForTimeout(wait)
    const info = await page.evaluate(() => {
      const m = window.__wtMap
      const tm = m?.style?.tileManagers ?? m?.style?.sourceCaches ?? {}
      const src = tm.openmaptiles
      const ids = src?.getIds?.() ?? Object.keys(src?._tiles ?? {})
      return { loaded: m?.loaded(), tilesLoaded: m?.areTilesLoaded(), vectorTiles: ids.length }
    })
    const png = await page.screenshot({ clip: { x: 0, y: 70, width: 390, height: 400 } })
    console.log(`isMobile=${mobile} t+${wait} ${JSON.stringify(info)} pngBytes=${png.length}`)
  }
  await ctx.close()
}
await browser.close()
