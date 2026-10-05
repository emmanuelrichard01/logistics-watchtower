import type { StyleSpecification } from 'maplibre-gl'

// Both themes are derived from OpenFreeMap Positron (open tiles, no key) and
// recoloured to the console's tokens at load time, so the basemap belongs to
// the product instead of fighting it. Positron's greys are near-neutral, so a
// lightness remap carries the whole palette.

const SOURCE = 'https://tiles.openfreemap.org/styles/positron'

type Rgba = [number, number, number, number]

function parseColor(s: string): Rgba | null {
  const t = s.trim().toLowerCase()
  if (t.startsWith('#')) {
    const h = t.slice(1)
    const full = h.length === 3 || h.length === 4 ? [...h].map((c) => c + c).join('') : h
    if (!/^[0-9a-f]{6}([0-9a-f]{2})?$/.test(full)) return null
    const n = (i: number) => parseInt(full.slice(i, i + 2), 16)
    return [n(0), n(2), n(4), full.length === 8 ? n(6) / 255 : 1]
  }
  const m = t.match(/^(rgba?|hsla?)\(([^)]+)\)$/)
  if (!m) return null
  const parts = m[2].split(/[\s,/]+/).filter(Boolean).map((p) => parseFloat(p))
  if (parts.length < 3 || parts.some(Number.isNaN)) return null
  const a = parts[3] ?? 1
  if (m[1].startsWith('rgb')) return [parts[0], parts[1], parts[2], a]
  // HSL to RGB
  const [h, sat, l] = [parts[0] / 360, parts[1] / 100, parts[2] / 100]
  const q = l < 0.5 ? l * (1 + sat) : l + sat - l * sat
  const p = 2 * l - q
  const hue = (x: number) => {
    const k = (x + 1) % 1
    if (k < 1 / 6) return p + (q - p) * 6 * k
    if (k < 1 / 2) return q
    if (k < 2 / 3) return p + (q - p) * (2 / 3 - k) * 6
    return p
  }
  return [hue(h + 1 / 3) * 255, hue(h) * 255, hue(h - 1 / 3) * 255, a]
}

const toCss = ([r, g, b, a]: Rgba) => `rgba(${Math.round(r)}, ${Math.round(g)}, ${Math.round(b)}, ${Math.round(a * 1000) / 1000})`

function mix(a: Rgba, b: Rgba, t: number): Rgba {
  return [a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t, a[2] + (b[2] - a[2]) * t, a[3]]
}

/** Map Positron's lightness onto the theme's ramp: paper → ground, ink → ink. */
function remap(theme: 'light' | 'dark') {
  const ramp =
    theme === 'light'
      ? { paper: [244, 245, 247, 1] as Rgba, ink: [91, 100, 114, 1] as Rgba, water: [214, 222, 232, 1] as Rgba }
      : { paper: [16, 19, 23, 1] as Rgba, ink: [138, 147, 160, 1] as Rgba, water: [26, 33, 43, 1] as Rgba }
  return (c: Rgba, isWater: boolean): Rgba => {
    if (isWater) return [...ramp.water.slice(0, 3), c[3]] as Rgba
    const lum = (0.2126 * c[0] + 0.7152 * c[1] + 0.0722 * c[2]) / 255 // 1 = paper, 0 = ink
    return [...mix(ramp.ink, ramp.paper, lum).slice(0, 3), c[3]] as Rgba
  }
}

function recolour(value: unknown, fn: (c: Rgba) => Rgba): unknown {
  if (typeof value === 'string') {
    const c = parseColor(value)
    return c ? toCss(fn(c)) : value
  }
  if (Array.isArray(value)) return value.map((v) => recolour(v, fn))
  if (value && typeof value === 'object') return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, recolour(v, fn)]))
  return value
}

/**
 * Resolve vector sources declared by TileJSON URL into inline tile templates.
 * Measured: with the URL form, a style applied after construction requested
 * no tiles until the camera moved; inline templates load immediately.
 */
async function inlineTileJson(style: StyleSpecification): Promise<StyleSpecification> {
  const sources = await Promise.all(
    Object.entries(style.sources).map(async ([id, src]) => {
      if (src.type !== 'vector' || !('url' in src) || !src.url) return [id, src] as const
      const tj = (await (await fetch(src.url)).json()) as { tiles: string[]; minzoom?: number; maxzoom?: number; attribution?: string }
      const { url: _url, ...rest } = src
      void _url
      return [id, { ...rest, tiles: tj.tiles, minzoom: tj.minzoom ?? 0, maxzoom: tj.maxzoom ?? 14, attribution: tj.attribution ?? rest.attribution }] as const
    }),
  )
  return { ...style, sources: Object.fromEntries(sources) as StyleSpecification['sources'] }
}

const cache = new Map<string, Promise<StyleSpecification>>()

export function themedBasemap(theme: 'light' | 'dark'): Promise<StyleSpecification> {
  const hit = cache.get(theme)
  if (hit) return hit
  const promise = fetch(SOURCE)
    .then((r) => r.json() as Promise<StyleSpecification>)
    .then(inlineTileJson)
    .then((style) => {
      const fn = remap(theme)
      return {
        ...style,
        layers: style.layers.map((layer) => {
          const isWater = /water|ocean|river|lake/.test(layer.id) && layer.type !== 'symbol'
          // The shaded-relief raster reads as noise on both themes; keep it faint.
          if (layer.type === 'raster') return { ...layer, paint: { ...layer.paint, 'raster-opacity': theme === 'light' ? 0.18 : 0.08 } }
          return 'paint' in layer && layer.paint ? { ...layer, paint: recolour(layer.paint, (c) => fn(c, isWater)) as typeof layer.paint } : layer
        }),
      } as StyleSpecification
    })
  cache.set(theme, promise)
  return promise
}
