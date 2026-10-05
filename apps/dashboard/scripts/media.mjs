// Documentation media: retina screenshots and three short story clips.
//
// Usage (from apps/dashboard, with a production preview on :4173):
//   npm run build && npx vite preview --port 4173 &
//   node scripts/media.mjs [baseUrl] [outDir]
//   ONLY=shots|clips node scripts/media.mjs   (one half only)
//   CLIPS=lanes,map node scripts/media.mjs     (some clips only)
//
// Writes into docs/media:
//   *.png              screenshots at 2x (desktop) and 3x (phone)
//   tour-*.mp4         H.264 clips at full resolution, for the docs site
//   tour-*.gif         palette-optimised 960 px GIFs that play inline on GitHub
// Clips show a rendered pointer and a caption bar added below the frame, so the
// product image itself is untouched.
// Needs ffmpeg on PATH. Uses the GPU (ANGLE/D3D11) like scripts/perf-map.mjs.
import { chromium } from '@playwright/test'
import { execFileSync } from 'node:child_process'
import { copyFileSync, existsSync, mkdirSync, readdirSync, rmSync, statSync, writeFileSync } from 'node:fs'
import { join, resolve } from 'node:path'

const base = process.argv[2] ?? 'http://localhost:4173'
const out = resolve(process.argv[3] ?? '../../docs/media')
const only = process.env.ONLY
mkdirSync(out, { recursive: true })

const GPU_ARGS = ['--enable-gpu', '--use-angle=d3d11', '--ignore-gpu-blocklist']
const DESKTOP = { width: 1440, height: 900 }
const PHONE = { width: 390, height: 844 }
const AT_RISK = '.tag--danger, .tag--caution1, .tag--caution2'
const FONT = ['C:/Windows/Fonts/seguisb.ttf', '/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf', '/System/Library/Fonts/SFNS.ttf'].find(existsSync)

const browser = await chromium.launch({ args: GPU_ARGS })
const failures = []

async function newContext({ vp, theme, guide = false, phone = false, scale }) {
  const context = await browser.newContext({ viewport: vp, deviceScaleFactor: scale ?? (phone ? 3 : 2), isMobile: phone, hasTouch: phone })
  await context.addInitScript(
    ([t, showGuide]) => {
      localStorage.setItem('wt-theme', t)
      if (!showGuide) localStorage.setItem('wt-guide-seen', '1')
    },
    [theme, guide],
  )
  return context
}

/** Load the map once so the context's tile cache is warm, then the target page. */
async function open(page, path, { warmMap = false } = {}) {
  if (warmMap) {
    await page.goto(base + '/map', { waitUntil: 'load' })
    await page.waitForTimeout(7000)
  }
  await page.goto(base + path, { waitUntil: 'load' })
}

// ---------------------------------------------------------------- screenshots

const shots = [
  { name: 'guide', path: '/', vp: DESKTOP, theme: 'light', guide: true },
  { name: 'lanes-light', path: '/', vp: DESKTOP, theme: 'light' },
  { name: 'lanes-dark', path: '/', vp: DESKTOP, theme: 'dark' },
  { name: 'evidence-light', path: '/', vp: DESKTOP, theme: 'light', act: (p) => p.locator(AT_RISK).first().click() },
  { name: 'evidence-dark', path: '/', vp: DESKTOP, theme: 'dark', act: (p) => p.locator(AT_RISK).first().click() },
  { name: 'map-city', path: '/map', vp: DESKTOP, theme: 'light', act: (p) => p.locator('.fleet-row', { hasText: 'VAN-LAG1' }).click() },
  {
    name: 'map-3d',
    path: '/map',
    vp: DESKTOP,
    theme: 'dark',
    act: async (p) => {
      await p.getByRole('button', { name: '3D' }).click()
      await p.waitForTimeout(1500)
    },
  },
  { name: 'incidents-light', path: '/incidents', vp: DESKTOP, theme: 'light' },
  { name: 'incidents-dark', path: '/incidents', vp: DESKTOP, theme: 'dark' },
  { name: 'health-light', path: '/health', vp: DESKTOP, theme: 'light' },
  { name: 'health-dark', path: '/health', vp: DESKTOP, theme: 'dark' },
  { name: 'phone-lanes', path: '/', vp: PHONE, theme: 'light', phone: true },
  { name: 'phone-map', path: '/map', vp: PHONE, theme: 'dark', phone: true },
  { name: 'phone-incidents', path: '/incidents', vp: PHONE, theme: 'light', phone: true },
]

if (only !== 'clips') {
  for (const s of shots) {
    const context = await newContext(s)
    const page = await context.newPage()
    // View Transitions abort when phone emulation resizes the viewport: benign.
    page.on('pageerror', (e) => !e.message.includes('Viewport size changed') && failures.push(`${s.name}: ${e.message}`))
    await open(page, s.path, { warmMap: s.path === '/map' })
    await page.waitForTimeout(s.path === '/map' ? 4000 : 1500)
    if (s.act) {
      try {
        await s.act(page)
        await page.waitForTimeout(s.path === '/map' ? 3000 : 1200)
      } catch (e) {
        failures.push(`${s.name}: ${e.message.split('\n')[0]}`)
      }
    }
    await page.screenshot({ path: join(out, `${s.name}.png`) })
    console.log(`shot ${s.name}`)
    await context.close()
  }
}

// ---------------------------------------------------------------------- clips

// Clips are rendered frame by frame on a virtual clock, not screen-recorded.
// Playwright's fake clock drives requestAnimationFrame, timers and
// performance.now, so the app (store, MapLibre camera, deck.gl) advances exactly
// one frame per capture, and the video is smooth at 30 fps however slow the
// machine is. A real-time screencast managed about 10 fps on the lanes and 1.5
// on the WebGL map of the reference laptop. CSS animations run on the
// compositor's own clock, so their playback rate is matched to the capture rate.
const FPS = 30
const FRAME_MS = 1000 / FPS

// A visible pointer and click ripple, drawn by the page from real input events.
const POINTER = () => {
  const install = () => {
    const css = document.createElement('style')
    css.textContent = `
      #demo-pointer{position:fixed;left:0;top:0;z-index:2147483647;pointer-events:none;transform:translate(-100px,-100px);filter:drop-shadow(0 1px 1.5px rgb(0 0 0 / .4))}
      .demo-ripple{position:fixed;z-index:2147483646;pointer-events:none;width:38px;height:38px;margin:-19px 0 0 -19px;border-radius:50%;border:2px solid #1f4bff;background:rgb(31 75 255 / .12);animation:demo-ripple 480ms ease-out forwards}
      @keyframes demo-ripple{from{transform:scale(.35);opacity:1}to{transform:scale(1.2);opacity:0}}`
    document.head.append(css)
    const p = document.createElement('div')
    p.id = 'demo-pointer'
    // The classic arrow cursor, so it never reads as a vehicle on the map.
    p.innerHTML = '<svg width="20" height="26" viewBox="0 0 20 26"><path d="M2 2v19.5l5-4.6 3.4 7.4 3.3-1.5-3.3-7.2h6.9z" fill="#111" stroke="#fff" stroke-width="1.6" stroke-linejoin="round"/></svg>'
    document.body.append(p)
    addEventListener('pointermove', (e) => (p.style.transform = `translate(${e.clientX - 2}px, ${e.clientY - 2}px)`), true)
    addEventListener(
      'pointerdown',
      (e) => {
        const r = document.createElement('div')
        r.className = 'demo-ripple'
        r.style.left = `${e.clientX}px`
        r.style.top = `${e.clientY}px`
        document.body.append(r)
        r.addEventListener('animationend', () => r.remove())
      },
      true,
    )
  }
  if (document.body) install()
  else addEventListener('DOMContentLoaded', install)
}

async function record(name, theme, script) {
  if (process.env.CLIPS && !process.env.CLIPS.split(',').includes(name)) return
  const dir = join(out, `.tmp-${name}`)
  rmSync(dir, { recursive: true, force: true })
  mkdirSync(dir, { recursive: true })
  const context = await newContext({ vp: DESKTOP, theme, scale: 1 })
  await context.addInitScript(POINTER)
  const page = await context.newPage()
  page.on('pageerror', (e) => failures.push(`${name}: ${e.message}`))
  const cdp = await context.newCDPSession(page)
  await cdp.send('Animation.enable')
  await page.clock.install()

  let frame = 0
  let realMs = 120 // running estimate of real time per captured frame
  const captions = [] // { text, at } in video seconds
  const pointer = { x: DESKTOP.width * 0.55, y: DESKTOP.height * 0.62 }

  /** Advance one frame of virtual time and capture it. */
  const step = async () => {
    const t0 = performance.now()
    await page.clock.runFor(FRAME_MS)
    await page.screenshot({ path: join(dir, `f${String(frame).padStart(5, '0')}.jpg`), type: 'jpeg', quality: 92 })
    frame++
    realMs = realMs * 0.9 + (performance.now() - t0) * 0.1
    if (frame % 15 === 0) await cdp.send('Animation.setPlaybackRate', { playbackRate: Math.min(1, FRAME_MS / realMs) })
  }
  const api = {
    page,
    /** Capture ms of video, everything animating in virtual time. */
    hold: async (ms) => {
      for (let i = 0; i < Math.round(ms / FRAME_MS); i++) await step()
    },
    say: (text) => captions.push({ text, at: frame / FPS }),
    /** Move the pointer along an eased path, one position per frame. */
    glide: async (x, y, ms = 650) => {
      const n = Math.max(2, Math.round(ms / FRAME_MS))
      const { x: x0, y: y0 } = pointer
      for (let i = 1; i <= n; i++) {
        const k = i / n
        const e = k < 0.5 ? 4 * k ** 3 : 1 - (-2 * k + 2) ** 3 / 2
        await page.mouse.move(x0 + (x - x0) * e, y0 + (y - y0) * e)
        await step()
      }
      pointer.x = x
      pointer.y = y
    },
    click: async (locator, settleMs = 600) => {
      const box = await locator.boundingBox()
      if (!box) throw new Error('click target not found')
      await api.glide(box.x + box.width / 2, box.y + box.height / 2)
      await api.hold(150)
      await page.mouse.click(pointer.x, pointer.y)
      await api.hold(settleMs)
    },
    key: async (k, settleMs = 600) => {
      await page.keyboard.press(k)
      await api.hold(settleMs)
    },
    /** Drag the time handle left by a fraction of its track. */
    scrub: async (fraction, ms = 900) => {
      const track = await page.locator('.time-handle__track').boundingBox()
      const thumb = await page.locator('.time-handle__thumb').boundingBox()
      const y = track.y + track.height / 2
      await api.glide(thumb.x + thumb.width / 2, y)
      await page.mouse.down()
      const x0 = pointer.x
      const x1 = x0 - track.width * fraction
      const n = Math.round(ms / FRAME_MS)
      for (let i = 1; i <= n; i++) {
        const k = i / n
        await page.mouse.move(x0 + (x1 - x0) * (1 - (1 - k) ** 2), y)
        await step()
      }
      await page.mouse.up()
      pointer.x = x1
    },
    /** Let the page load in real time (tiles, data), then stop the clock. */
    settle: async (ms) => {
      await page.waitForTimeout(ms)
      await page.clock.pauseAt(Date.now() + 1000)
      await page.mouse.move(pointer.x, pointer.y)
    },
  }
  try {
    await script(api)
  } catch (e) {
    failures.push(`${name}: ${e.message.split('\n')[0]}`)
  }
  await context.close()
  if (frame < FPS) {
    failures.push(`${name}: only ${frame} frames`)
    return
  }
  encode(name, dir, frame, captions)
  rmSync(dir, { recursive: true, force: true })
}

const ff = (args, cwd) => execFileSync('ffmpeg', ['-hide_banner', '-loglevel', 'error', '-y', ...args], { stdio: 'inherit', cwd })

function encode(name, dir, frames, captions) {
  const duration = frames / FPS
  // Caption bar under the frame: one text file per caption avoids filter escaping.
  const BAR = 72
  const filters = [`pad=iw:ih+${BAR}:0:0:color=0x0E1116`]
  if (FONT) copyFileSync(FONT, join(dir, 'font.ttf'))
  captions.forEach((c, i) => {
    const from = c.at
    const to = captions[i + 1]?.at ?? duration
    writeFileSync(join(dir, `cap${i}.txt`), c.text)
    const fade = `if(lt(t,${from + 0.25}),(t-${from})/0.25,if(gt(t,${to - 0.25}),(${to}-t)/0.25,1))`
    filters.push(`drawtext=${FONT ? 'fontfile=font.ttf:' : ''}textfile=cap${i}.txt:fontsize=26:fontcolor=0xF2F4F7:x=(w-text_w)/2:y=h-${BAR / 2}-text_h/2:enable='between(t,${from.toFixed(3)},${to.toFixed(3)})':alpha='${fade}'`)
  })
  filters.push('format=yuv420p')
  ff(['-framerate', String(FPS), '-i', 'f%05d.jpg', '-vf', filters.join(','), '-c:v', 'libx264', '-preset', 'veryfast', '-crf', '10', 'master.mp4'], dir)
  ff(['-i', 'master.mp4', '-c:v', 'libx264', '-preset', 'slow', '-crf', '23', '-pix_fmt', 'yuv420p', '-movflags', '+faststart', '-an', join(out, `tour-${name}.mp4`)], dir)
  // A moving basemap changes most pixels every frame: fewer GIF frames keep it near 5 MB.
  const gifFps = name === 'map' ? 12 : 15
  ff(['-i', 'master.mp4', '-vf', `fps=${gifFps},scale=960:-1:flags=lanczos,palettegen=max_colors=192:stats_mode=diff`, 'palette.png'], dir)
  ff(['-i', 'master.mp4', '-i', 'palette.png', '-lavfi', `fps=${gifFps},scale=960:-1:flags=lanczos[x];[x][1:v]paletteuse=dither=sierra2_4a:diff_mode=rectangle`, join(out, `tour-${name}.gif`)], dir)
  console.log(`clip ${name}: ${frames} frames, ${duration.toFixed(1)} s, ${captions.length} captions`)
}

if (only !== 'shots') {
  // 1. The board: what needs attention, why, and replaying how it happened.
  await record('lanes', 'light', async ({ page, hold, say, click, key, scrub, settle }) => {
    await open(page, '/', { warmMap: true })
    await settle(1500)
    say('Every lane is a track; each truck carries a signal for its time to breach')
    await hold(2800)
    say('One truck is breaching: open it to see the evidence')
    await click(page.locator('.tag--danger').first(), 1800)
    say('The breach began inside a dead zone, and is dated when it really started')
    await hold(2800)
    await key('Escape', 500)
    say('Drag the time handle back: every view replays together')
    await scrub(0.24)
    await hold(2200)
    say('Press L to return to live')
    await key('l', 2600)
  })

  // 2. The map: inter-state lanes, the breaching truck, 3D, a city round.
  await record('map', 'light', async ({ page, hold, say, click, settle }) => {
    await open(page, '/map', { warmMap: true })
    await settle(4000)
    say('Live fleet on real road geometry across four states')
    await hold(2600)
    say('Select the breaching truck: trail behind, route ahead, next stop')
    await click(page.locator('.fleet-row').first(), 2800)
    say('3D raises a signal mast over every truck that needs attention')
    await click(page.getByRole('button', { name: '3D' }), 3000)
    await click(page.getByRole('button', { name: '2D' }), 900)
    say('City rounds too: a Lagos van on its morning drops, at street level')
    await click(page.locator('.fleet-row', { hasText: 'VAN-LAG1' }), 3400)
  })

  // 3. The incident workflow, from the keyboard.
  await record('incidents', 'dark', async ({ page, hold, say, key, settle }) => {
    await open(page, '/incidents')
    await settle(1800)
    say('Incidents wait for a decision, oldest critical first')
    await hold(2600)
    say('A acknowledges: the operator takes ownership')
    await key('a', 2200)
    say('M starts a playbook action; the incident moves to Mitigating')
    await key('m', 2400)
    say('J moves to the next incident; Enter opens its evidence')
    await key('j', 900)
    await key('Enter', 2800)
  })
}

await browser.close()

for (const f of readdirSync(out).sort()) {
  if (f.startsWith('.')) continue
  console.log(`${f.padEnd(28)} ${(statSync(join(out, f)).size / 1024).toFixed(0).padStart(6)} KB`)
}
if (failures.length) {
  console.error(`\n${failures.length} problem(s):\n${failures.join('\n')}`)
  process.exitCode = 1
}
