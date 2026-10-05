// Documentation media: screenshots and a walkthrough recording of the console.
//
// Usage (from apps/dashboard, with a production preview on :4173):
//   npm run build && npx vite preview --port 4173 &
//   node scripts/media.mjs [baseUrl] [outDir]
//
// Writes PNGs, walkthrough.mp4 (H.264) and walkthrough.gif (palette-optimised)
// into docs/media. Needs ffmpeg on PATH. Uses the GPU (ANGLE/D3D11) like
// scripts/perf-map.mjs so the recording is smooth.
import { chromium } from '@playwright/test'
import { execFileSync } from 'node:child_process'
import { mkdirSync, readdirSync, rmSync, statSync, writeFileSync } from 'node:fs'
import { join, resolve } from 'node:path'

const base = process.argv[2] ?? 'http://localhost:4173'
const out = resolve(process.argv[3] ?? '../../docs/media')
mkdirSync(out, { recursive: true })

const GPU_ARGS = ['--enable-gpu', '--use-angle=d3d11', '--ignore-gpu-blocklist']
const DESKTOP = { width: 1440, height: 900 }
const PHONE = { width: 390, height: 844 }
const AT_RISK = '.tag--danger, .tag--caution1, .tag--caution2'

const shots = [
  { name: 'guide', path: '/', vp: DESKTOP, theme: 'light', guide: true },
  { name: 'lanes-light', path: '/', vp: DESKTOP, theme: 'light' },
  { name: 'lanes-dark', path: '/', vp: DESKTOP, theme: 'dark' },
  { name: 'evidence-light', path: '/', vp: DESKTOP, theme: 'light', click: AT_RISK },
  { name: 'evidence-dark', path: '/', vp: DESKTOP, theme: 'dark', click: AT_RISK },
  { name: 'map-light', path: '/map', vp: DESKTOP, theme: 'light', wait: 6000, click: '.fleet-row' },
  { name: 'map-dark', path: '/map', vp: DESKTOP, theme: 'dark', wait: 6000, click: '.fleet-row' },
  { name: 'incidents-light', path: '/incidents', vp: DESKTOP, theme: 'light' },
  { name: 'incidents-dark', path: '/incidents', vp: DESKTOP, theme: 'dark' },
  { name: 'health-light', path: '/health', vp: DESKTOP, theme: 'light' },
  { name: 'health-dark', path: '/health', vp: DESKTOP, theme: 'dark' },
  { name: 'phone-lanes', path: '/', vp: PHONE, theme: 'light', phone: true },
  // Phone tiles take longer on a cold cache under touch emulation; give the map time to load.
  { name: 'phone-map', path: '/map', vp: PHONE, theme: 'dark', phone: true, wait: 10000 },
  { name: 'phone-incidents', path: '/incidents', vp: PHONE, theme: 'light', phone: true },
]

async function newContext(browser, { vp, theme, guide = false, phone = false }) {
  const context = await browser.newContext({
    viewport: vp,
    deviceScaleFactor: phone ? 2 : 1,
    isMobile: phone,
    hasTouch: phone,
  })
  await context.addInitScript(
    ([t, showGuide]) => {
      localStorage.setItem('wt-theme', t)
      if (!showGuide) localStorage.setItem('wt-guide-seen', '1')
    },
    [theme, guide],
  )
  return context
}

const browser = await chromium.launch({ args: GPU_ARGS })
const failures = []

for (const s of shots) {
  const context = await newContext(browser, s)
  const page = await context.newPage()
  // View Transitions abort when phone emulation resizes the viewport: benign.
  page.on('pageerror', (e) => !e.message.includes('Viewport size changed') && failures.push(`${s.name}: ${e.message}`))
  await page.goto(base + s.path, { waitUntil: 'load' })
  await page.waitForTimeout(s.wait ?? 1800)
  if (s.path === '/map') {
    // A fresh context starts with a cold tile cache; reload so tiles come from it.
    await page.reload({ waitUntil: 'load' })
    await page.waitForTimeout(s.wait ?? 1800)
  }
  if (s.click) {
    const el = page.locator(s.click).first()
    if (await el.count()) {
      await el.click({ timeout: 4000 })
      await page.waitForTimeout(s.path === '/map' ? 2500 : 1200)
    } else failures.push(`${s.name}: nothing matched ${s.click}`)
  }
  await page.screenshot({ path: join(out, `${s.name}.png`) })
  console.log(`shot ${s.name}`)
  await context.close()
}

// Walkthrough: lanes → open an at-risk truck → replay → map → select → follow.
// Recorded with Chromium's CDP screencast: timestamped frames, assembled by
// ffmpeg with their real durations (Playwright's own video needs a bundled
// ffmpeg binary, which antivirus on the reference machine removed).
const videoDir = join(out, '.video-tmp')
rmSync(videoDir, { recursive: true, force: true })
mkdirSync(videoDir, { recursive: true })
const context = await newContext(browser, { vp: DESKTOP, theme: 'light' })
const page = await context.newPage()
const frames = []
const cdp = await context.newCDPSession(page)
cdp.on('Page.screencastFrame', async ({ data, metadata, sessionId }) => {
  const file = join(videoDir, `f${String(frames.length).padStart(5, '0')}.jpg`)
  writeFileSync(file, Buffer.from(data, 'base64'))
  frames.push({ file, t: metadata.timestamp })
  await cdp.send('Page.screencastFrameAck', { sessionId }).catch(() => {})
})
// Warm this context's tile cache so the map segment doesn't record tiles loading.
await page.goto(base + '/map', { waitUntil: 'load' })
await page.waitForTimeout(6000)
await page.goto(base + '/', { waitUntil: 'load' })
await page.waitForTimeout(1200)
await cdp.send('Page.startScreencast', { format: 'jpeg', quality: 88, maxWidth: DESKTOP.width, maxHeight: DESKTOP.height, everyNthFrame: 1 })
await page.waitForTimeout(800)
const tag = page.locator(AT_RISK).first()
if (await tag.count()) await tag.click({ timeout: 4000 })
await page.waitForTimeout(2200)
// Drag the time handle back about 40 minutes.
const track = page.locator('.time-handle__track')
const box = await track.boundingBox()
if (box) {
  const from = await page.locator('.time-handle__thumb').boundingBox()
  const startX = from ? from.x + from.width / 2 : box.x + box.width * 0.6
  await page.mouse.move(startX, box.y + 13)
  await page.mouse.down()
  for (let i = 1; i <= 20; i++) {
    await page.mouse.move(startX - (box.width * 0.22 * i) / 20, box.y + 13)
    await page.waitForTimeout(45)
  }
  await page.mouse.up()
}
await page.waitForTimeout(1600)
await page.keyboard.press('Escape')
await page.locator('a[href="/map"]').first().click({ timeout: 4000 })
await page.waitForTimeout(3200)
const row = page.locator('.fleet-row').first()
if (await row.count()) await row.click({ timeout: 4000 })
await page.waitForTimeout(1500)
const follow = page.getByRole('button', { name: /follow/i }).first()
if (await follow.count()) await follow.click({ timeout: 4000 })
await page.waitForTimeout(2600)
await cdp.send('Page.stopScreencast')
await context.close()
await browser.close()

// Concat list with each frame held until the next one arrived (screencast only
// sends frames when pixels change).
const lines = []
frames.forEach((f, i) => {
  const next = frames[i + 1]?.t ?? f.t + 0.5
  lines.push(`file '${f.file.replaceAll('\\', '/')}'`, `duration ${Math.max(0.001, next - f.t).toFixed(4)}`)
})
lines.push(`file '${frames.at(-1).file.replaceAll('\\', '/')}'`)
const list = join(videoDir, 'frames.txt')
writeFileSync(list, lines.join('\n'))
const rawVideo = join(videoDir, 'raw.mp4')
console.log(`walkthrough: ${frames.length} frames over ${(frames.at(-1).t - frames[0].t).toFixed(1)} s`)

const ff0 = (args) => execFileSync('ffmpeg', ['-hide_banner', '-loglevel', 'error', '-y', ...args], { stdio: 'inherit' })
ff0(['-f', 'concat', '-safe', '0', '-i', list, '-vf', 'fps=30,format=yuv420p', '-c:v', 'libx264', '-preset', 'veryfast', '-crf', '12', rawVideo])
const mp4 = join(out, 'walkthrough.mp4')
const gif = join(out, 'walkthrough.gif')
const ff = (args) => execFileSync('ffmpeg', ['-hide_banner', '-loglevel', 'error', '-y', ...args], { stdio: 'inherit' })
ff(['-i', rawVideo, '-vf', 'fps=30,scale=1440:-2:flags=lanczos', '-c:v', 'libx264', '-preset', 'slow', '-crf', '28', '-pix_fmt', 'yuv420p', '-movflags', '+faststart', '-an', mp4])
const palette = join(videoDir, 'palette.png')
ff(['-i', rawVideo, '-vf', 'fps=12,scale=960:-1:flags=lanczos,palettegen=max_colors=128:stats_mode=diff', palette])
ff(['-i', rawVideo, '-i', palette, '-lavfi', 'fps=12,scale=960:-1:flags=lanczos[x];[x][1:v]paletteuse=dither=bayer:bayer_scale=4:diff_mode=rectangle', gif])
rmSync(videoDir, { recursive: true, force: true })

for (const f of readdirSync(out).sort()) {
  console.log(`${f.padEnd(28)} ${(statSync(join(out, f)).size / 1024).toFixed(0).padStart(6)} KB`)
}
if (failures.length) {
  console.error(`\n${failures.length} problem(s):\n${failures.join('\n')}`)
  process.exitCode = 1
}
