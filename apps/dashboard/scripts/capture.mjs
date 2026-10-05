// Review captures for the design finish pass: desktop and phone, both themes.
// Usage: node scripts/capture.mjs [baseUrl] [outDir]
import { chromium } from '@playwright/test'
import { mkdirSync } from 'node:fs'

const base = process.argv[2] ?? 'http://localhost:5173'
const out = process.argv[3] ?? '../../.impeccable/review'
mkdirSync(out, { recursive: true })

const shots = [
  { name: 'desktop', path: '/', vp: { width: 1440, height: 900 }, theme: 'light' },
  { name: 'desktop-guide', path: '/', vp: { width: 1440, height: 900 }, theme: 'light', guide: true },
  { name: 'desktop-dark', path: '/', vp: { width: 1440, height: 900 }, theme: 'dark' },
  { name: 'desktop-layer', path: '/', vp: { width: 1440, height: 900 }, theme: 'light', click: '.tag--danger, .tag--caution1, .tag--caution2' },
  { name: 'desktop-map', path: '/map', vp: { width: 1440, height: 900 }, theme: 'light', wait: 4500, click: '.fleet-row' },
  { name: 'desktop-map-dark', path: '/map', vp: { width: 1440, height: 900 }, theme: 'dark', wait: 4500 },
  { name: 'desktop-incidents', path: '/incidents', vp: { width: 1440, height: 900 }, theme: 'light' },
  { name: 'desktop-health', path: '/health', vp: { width: 1440, height: 900 }, theme: 'light' },
  { name: 'mobile', path: '/', vp: { width: 390, height: 844 }, theme: 'light', mobile: true },
  { name: 'mobile-incidents', path: '/incidents', vp: { width: 390, height: 844 }, theme: 'light', mobile: true },
  { name: 'mobile-map', path: '/map', vp: { width: 390, height: 844 }, theme: 'dark', mobile: true, wait: 4500 },
]

const browser = await chromium.launch()
for (const s of shots) {
  const context = await browser.newContext({ viewport: s.vp, deviceScaleFactor: 1, isMobile: !!s.mobile, hasTouch: !!s.mobile, reducedMotion: 'reduce' })
  await context.addInitScript(([theme, guide]) => {
    localStorage.setItem('wt-theme', theme)
    if (!guide) localStorage.setItem('wt-guide-seen', '1')
  }, [s.theme, !!s.guide])
  const page = await context.newPage()
  const errors = []
  page.on('pageerror', (e) => errors.push(e.message))
  page.on('console', (m) => m.type() === 'error' && errors.push(m.text()))
  await page.goto(base + s.path, { waitUntil: 'networkidle' })
  await page.waitForTimeout(s.wait ?? 1200)
  if (s.click) {
    const el = page.locator(s.click).first()
    if (await el.count()) await el.click()
    await page.waitForTimeout(1500)
  }
  await page.screenshot({ path: `${out}/${s.name}.png`, fullPage: false })
  console.log(`${s.name}: ${errors.length ? 'ERRORS ' + errors.slice(0, 3).join(' | ') : 'ok'}`)
  await context.close()
}
await browser.close()
