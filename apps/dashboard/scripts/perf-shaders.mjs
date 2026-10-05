// WebGL program link time per shader, on a cold profile (what a first visit pays).
// Each line: milliseconds blocked on LINK_STATUS, then the shader name (deck.gl
// shaders carry SHADER_NAME; MapLibre programs show their source length).
// Usage: node scripts/perf-shaders.mjs http://localhost:4173/map
import { chromium } from '@playwright/test'
const browser = await chromium.launch({ args: ['--enable-gpu', '--use-angle=d3d11', '--ignore-gpu-blocklist'] })
const context = await browser.newContext({ viewport: { width: 1440, height: 900 } })
await context.addInitScript(() => {
  localStorage.setItem('wt-guide-seen', '1')
  window.__t = []
  const P = WebGL2RenderingContext.prototype
  const names = new WeakMap()
  const ss = P.shaderSource
  P.shaderSource = function (sh, src) { names.set(sh, (src.match(/SHADER_NAME\s+(\S+)/)?.[1]) ?? src.length); return ss.call(this, sh, src) }
  const att = P.attachShader
  P.attachShader = function (p, sh) { (p.__n ??= []).push(names.get(sh)); return att.call(this, p, sh) }
  const link = P.linkProgram
  P.linkProgram = function (p) { link.call(this, p); const t = performance.now(); this.getProgramParameter(p, this.LINK_STATUS); window.__t.push([String(p.__n?.[0]), Math.round(performance.now() - t)]) }
})
const page = await context.newPage()
await page.goto(process.argv[2] ?? 'http://localhost:4173/map', { waitUntil: 'load' })
await page.waitForTimeout(30000)
console.log((await page.evaluate(() => window.__t)).map(([n, ms]) => `${String(ms).padStart(6)} ${n}`).join('\n'))
await browser.close()
