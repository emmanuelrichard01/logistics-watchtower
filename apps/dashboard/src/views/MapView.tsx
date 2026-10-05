import { IconLayer } from '@deck.gl/layers'
import { MapboxOverlay } from '@deck.gl/mapbox'
import { Box, Compass, Crosshair, Map as MapIcon, Minus, Plus } from 'lucide-react'
import * as maplibregl from 'maplibre-gl'
// MapLibre locates its worker with a computed URL Vite cannot see, so the worker
// was missing from production builds ("Worker failed to load": no vector tiles).
// Bundle it explicitly, with its shared chunk, and hand MapLibre the URL.
import maplibreWorkerUrl from 'maplibre-gl/dist/maplibre-gl-worker.mjs?worker&url'
import 'maplibre-gl/dist/maplibre-gl.css'
import { useEffect, useLayoutEffect, useRef, useState } from 'react'
import { inDeadZone } from '../domain/corridors'
import type { Aspect, Shipment, Timeline } from '../domain/types'
import { useAppStore, useView } from '../state/context'
import { useStore } from '../state/store'
import { themedBasemap } from './map/basemap'
import { FleetList } from './map/FleetList'
import { fleetAt, fleetBounds, pathBetween, tokenRgb, trailOf, type AnimatedVehicle, type LngLat } from './map/geometry'
import { TripCard } from './map/TripCard'


maplibregl.setWorkerUrl(maplibreWorkerUrl)

/** Camera padding that keeps the fleet clear of the floating panels. */
function panelPadding(): maplibregl.PaddingOptions {
  if (window.matchMedia('(max-width: 760px)').matches) {
    return { top: 64, left: 24, right: 64, bottom: Math.round(window.innerHeight * 0.5) }
  }
  return { top: 80, bottom: 140, left: 380, right: 440 }
}

// 3D signal masts: taller the sooner the breach, so urgency reads as height.
const MAST_PX: Record<Exclude<Aspect, 'clear'>, number> = { danger: 112, caution1: 92, caution2: 76, unknown: 56 }
const LIT: Record<Aspect, number[]> = { danger: [0], caution2: [0, 1], caution1: [1], clear: [2], unknown: [] }

interface Palette {
  ink: [number, number, number]
  ink3: [number, number, number]
  surface: [number, number, number]
  cobalt: [number, number, number]
  danger: [number, number, number]
  caution: [number, number, number]
  track: [number, number, number]
  hatch: [number, number, number]
  clear: [number, number, number]
  head: [number, number, number]
  lampOff: [number, number, number]
}

function readPalette(): Palette {
  return {
    ink: tokenRgb('--ink'),
    ink3: tokenRgb('--ink-3'),
    surface: tokenRgb('--surface'),
    cobalt: tokenRgb('--cobalt'),
    danger: tokenRgb('--danger'),
    caution: tokenRgb('--caution'),
    track: tokenRgb('--track'),
    hatch: tokenRgb('--hatch'),
    clear: tokenRgb('--clear'),
    head: tokenRgb('--signal-head'),
    lampOff: tokenRgb('--lamp-off'),
  }
}

const rgb = (c: [number, number, number]) => `rgb(${c.join(',')})`
const lines = (paths: LngLat[][], props: (i: number) => Record<string, unknown> = () => ({})): GeoJSON.FeatureCollection => ({
  type: 'FeatureCollection',
  features: paths.map((coordinates, i) => ({ type: 'Feature', properties: props(i), geometry: { type: 'LineString', coordinates } })),
})
const EMPTY = lines([])

// Lines and stops are drawn by MapLibre, not deck.gl. deck's PathLayer shader took ~3.2 s
// per variant to compile on ANGLE/D3D11 (two variants: plain and dashed), a
// first-visit freeze; MapLibre's line programs compile in tens of milliseconds.
// deck.gl keeps only what moves every frame.
// Whether the style itself has loaded. map.isStyleLoaded() also waits for every
// tile, so a guard on it skips the work whenever tiles are in flight.
const styleReady = (map: maplibregl.Map) => Boolean((map as unknown as { style?: { _loaded?: boolean } }).style?._loaded)

function ensureRouteLayers(map: maplibregl.Map, p: Palette, { corridors, deadZones, ...stops }: ReturnType<typeof routeGeometry>) {
  if (!styleReady(map) || map.getSource('wt-corridors')) return
  map.addSource('wt-corridors', { type: 'geojson', data: corridors })
  map.addSource('wt-dead-zones', { type: 'geojson', data: deadZones })
  map.addSource('wt-trail', { type: 'geojson', data: EMPTY })
  map.addSource('wt-ahead', { type: 'geojson', data: EMPTY })
  // Under the basemap's place names, so town labels stay readable.
  const below = map.getStyle().layers.find((l) => l.type === 'symbol')?.id
  const add = (layer: maplibregl.AddLayerObject) => map.addLayer(layer, below)
  const round = { 'line-cap': 'round', 'line-join': 'round' } as const
  add({ id: 'wt-corridors', type: 'line', source: 'wt-corridors', layout: round, paint: { 'line-color': rgb(p.track), 'line-width': 4 } })
  add({ id: 'wt-dead-zones', type: 'line', source: 'wt-dead-zones', paint: { 'line-color': rgb(p.hatch), 'line-opacity': 0.9, 'line-width': 8, 'line-dasharray': [1.2, 1.2] } })
  add({ id: 'wt-trail', type: 'line', source: 'wt-trail', layout: round, paint: { 'line-color': rgb(p.ink3), 'line-opacity': 0.55, 'line-width': 3 } })
  add({ id: 'wt-ahead', type: 'line', source: 'wt-ahead', layout: round, paint: { 'line-color': rgb(p.cobalt), 'line-width': 6 } })
  map.addSource('wt-customers', { type: 'geojson', data: stops.customers })
  map.addSource('wt-depots', { type: 'geojson', data: stops.depots })
  add({ id: 'wt-customers', type: 'circle', source: 'wt-customers', paint: { 'circle-radius': 5, 'circle-color': rgb(p.surface), 'circle-stroke-color': rgb(p.ink3), 'circle-stroke-width': 2 } })
  add({ id: 'wt-depots', type: 'circle', source: 'wt-depots', paint: { 'circle-radius': 6, 'circle-color': rgb(p.surface), 'circle-stroke-color': rgb(p.ink), 'circle-stroke-width': 2 } })
}

const svg = (size: number, body: string) => `data:image/svg+xml;utf8,${encodeURIComponent(`<svg xmlns="http://www.w3.org/2000/svg" width="${size}" height="${size}" viewBox="0 0 ${size} ${size}">${body}</svg>`)}`

// One icon per vehicle: the disc and its north-pointing arrow, coloured per
// aspect. A disc is round, so rotating the whole icon by bearing is harmless,
// and the map needs a single deck.gl shader for its vehicles.
function vehicleIcon(p: Palette, aspect: Aspect, estimated: boolean, theme: string) {
  const ring = estimated ? `stroke="${rgb(p.ink3)}" stroke-dasharray="7 5"` : `stroke="${rgb(p.surface)}"`
  const arrow = `<path transform="translate(12 12) scale(0.625)" d="M32 6 L52 54 Q32 44 12 54 Z" fill="${rgb(aspectColor(p, aspect))}" stroke="${rgb(aspectColor(p, aspect))}" stroke-width="4" stroke-linejoin="round" opacity="${estimated ? 0.5 : 1}"/>`
  return { id: `${theme}-${aspect}-${estimated ? 'est' : 'fix'}`, url: svg(64, `<circle cx="32" cy="32" r="28" fill="${rgb(p.surface)}" stroke-width="3" ${ring}/>${arrow}`), width: 64, height: 64 }
}

// A billboarded signal mast: the console's signal head (lamp position carries
// the aspect, as in SignalHead) on a stem standing on the vehicle. Drawn by the
// icon shader the vehicles already use; a 3D column layer cost ~1 s of shader
// compilation the first time 3D was switched on.
function mastIcon(p: Palette, aspect: Exclude<Aspect, 'clear'>, theme: string) {
  const h = MAST_PX[aspect] * 2
  const lamp = (i: number) => {
    const cy = 12 + i * 18
    if (aspect === 'unknown') return `<circle cx="14" cy="${cy}" r="5.2" fill="none" stroke="${rgb(p.ink3)}" stroke-width="2.4"/>`
    const lit = LIT[aspect].includes(i)
    return `<circle cx="14" cy="${cy}" r="6.4" fill="${rgb(lit ? (aspect === 'danger' ? p.danger : p.caution) : p.lampOff)}"/>`
  }
  const body = `<rect x="12.5" y="58" width="3" height="${h - 58}" fill="${rgb(p.ink3)}"/><rect x="2" y="1.5" width="24" height="57" rx="12" fill="${rgb(p.head)}" stroke="${rgb(p.surface)}" stroke-width="3"/>${[0, 1, 2].map(lamp).join('')}`
  return { id: `${theme}-mast-${aspect}`, url: `data:image/svg+xml;utf8,${encodeURIComponent(`<svg xmlns="http://www.w3.org/2000/svg" width="28" height="${h}" viewBox="0 0 28 ${h}">${body}</svg>`)}`, width: 28, height: h, anchorX: 14, anchorY: h }
}

const haloIcon = (p: Palette, theme: string) => ({
  id: `${theme}-halo`,
  url: svg(104, `<circle cx="52" cy="52" r="48" fill="${rgb(p.cobalt)}" fill-opacity="0.16" stroke="${rgb(p.cobalt)}" stroke-width="5"/>`),
  width: 104,
  height: 104,
})

const aspectColor = (p: Palette, a: Aspect) => (a === 'danger' ? p.danger : a === 'caution1' || a === 'caution2' ? p.caution : a === 'unknown' ? p.ink3 : p.ink)

const points = (rows: { position: LngLat; [k: string]: unknown }[]): GeoJSON.FeatureCollection => ({
  type: 'FeatureCollection',
  features: rows.map(({ position, ...properties }) => ({ type: 'Feature', properties, geometry: { type: 'Point', coordinates: position } })),
})

function routeGeometry(timeline: Timeline) {
  const zones = timeline.corridors.flatMap((c) => c.deadZones.map((z) => ({ name: z.name, path: pathBetween(c, z.fromKm, z.toKm) })))
  return {
    depots: points(timeline.corridors.flatMap((c) => c.stations.filter((s) => s.depot).map((s) => ({ name: s.name, position: [s.lon, s.lat] as LngLat })))),
    // Customer drops on city rounds: what a multi-drop route is made of.
    customers: points(
      timeline.corridors
        .filter((c) => c.kind === 'urban')
        .flatMap((c) => c.stations.filter((s) => !s.depot).map((s) => ({ name: s.name, type: s.type ?? 'stop', window: s.window ?? '', position: [s.lon, s.lat] as LngLat }))),
    ),
    corridors: lines(timeline.corridors.map((c) => pathBetween(c, 0, c.lengthKm))),
    deadZones: lines(
      zones.map((z) => z.path),
      (i) => ({ name: zones[i].name }),
    ),
  }
}

export function MapView() {
  const store = useAppStore()
  const view = useView()
  const theme = useStore(store, (s) => s.theme)
  const selected = useStore(store, (s) => s.selected)
  const follow = useStore(store, (s) => s.follow)
  const container = useRef<HTMLDivElement>(null)
  const mapRef = useRef<maplibregl.Map | null>(null)
  const overlayRef = useRef<MapboxOverlay | null>(null)
  const labelsRef = useRef<HTMLDivElement | null>(null)
  const [mode3d, setMode3d] = useState(false)
  const [bearing, setBearing] = useState(0)
  // The animation loop reads these; sync them after render, never during it.
  const shipmentsRef = useRef<Map<string, Shipment>>(new Map())
  const stateRef = useRef({ selected, follow, mode3d })
  useLayoutEffect(() => {
    shipmentsRef.current = new Map(view.shipments.map((s) => [s.vehicleId, s]))
    stateRef.current = { selected, follow, mode3d }
  })

  const styleTheme = useRef(theme)
  // Map + overlay lifecycle. The themed style is built first and handed to the
  // constructor: swapping styles asynchronously after construction proved
  // fragile (tiles not requested until the camera moved).
  const [ready, setReady] = useState(false)
  useEffect(() => {
    let cancelled = false
    let map: maplibregl.Map | null = null
    themedBasemap(store.getState().theme).then((style) => {
      if (cancelled || !container.current) return
      map = new maplibregl.Map({
        container: container.current,
        style,
        bounds: fleetBounds(store.timeline.corridors),
        fitBoundsOptions: { padding: panelPadding() },
        attributionControl: { compact: true },
        maxPitch: 70,
      })
      const overlay = new MapboxOverlay({ interleaved: false, layers: [], getCursor: ({ isHovering }) => (isHovering ? 'pointer' : 'grab') })
      map.addControl(overlay)
      const m = map
      const geometry = routeGeometry(store.timeline)
      const addRoutes = () => ensureRouteLayers(m, readPalette(), geometry)
      // 'style.load' also fires after a theme swap, which drops custom layers. A
      // style object can finish loading inside the constructor, so try now too.
      m.on('style.load', addRoutes)
      m.on('load', addRoutes)
      addRoutes()
      // Zones and stops are MapLibre features, so their hover notes are ours to draw.
      const tip = document.createElement('div')
      tip.className = 'map-tooltip map-tooltip--zone'
      tip.hidden = true
      container.current.append(tip)
      m.on('mousemove', (e) => {
        const box: [maplibregl.PointLike, maplibregl.PointLike] = [
          [e.point.x - 4, e.point.y - 4],
          [e.point.x + 4, e.point.y + 4],
        ]
        const layers = ['wt-depots', 'wt-customers', 'wt-dead-zones'].filter((id) => m.getLayer(id))
        const f = layers.length ? m.queryRenderedFeatures(box, { layers })[0] : undefined
        tip.hidden = !f
        if (f) {
          const { name, type, window } = f.properties as { name: string; type?: string; window?: string }
          tip.textContent =
            f.layer.id === 'wt-dead-zones' ? `${name}: no signal` : f.layer.id === 'wt-depots' ? `${name} depot: cold storage` : `${name}\n${(type ?? 'stop').replace('_', ' ')}${window ? ` · delivery window ${window}` : ''}`
          tip.style.transform = `translate(${e.point.x + 12}px, ${e.point.y + 12}px)`
        }
      })
      m.on('mouseout', () => (tip.hidden = true))
      const labels = document.createElement('div')
      labels.className = 'map-labels'
      labels.setAttribute('aria-hidden', 'true')
      container.current.append(labels)
      labelsRef.current = labels
      m.on('rotate', () => setBearing(m.getBearing()))
      m.on('dragstart', () => stateRef.current.follow && store.setFollow(false))
      // Basemap failures arrive as map 'error' events, never on the console.
      m.on('error', (e) => console.warn('[map]', e.error?.message ?? e))
      mapRef.current = m
      overlayRef.current = overlay
      // Dev-only handle for probes (scripts/); never present in production builds.
      if (import.meta.env.DEV) (window as unknown as { __wtMap?: maplibregl.Map }).__wtMap = m
      styleTheme.current = store.getState().theme
      setReady(true)
    })
    return () => {
      cancelled = true
      map?.remove()
      mapRef.current = null
      overlayRef.current = null
      setReady(false)
    }
  }, [store])

  // Selecting a truck brings it into the clear area beside the panels.
  useEffect(() => {
    const map = mapRef.current
    if (!map || !selected) return
    const v = fleetAt(store.timeline, store.precise()).find((x) => x.vehicle.vehicleId === selected)
    if (!v) return
    // City rounds need street level; inter-state trucks read best at region level.
    const urban = store.timeline.corridors.find((c) => c.id === v.vehicle.corridorId)?.kind === 'urban'
    map.easeTo({ center: v.position, zoom: urban ? Math.max(map.getZoom(), 12.5) : Math.max(map.getZoom(), 7.5), padding: panelPadding(), duration: 900 })
  }, [selected, store, ready])

  // Theme: swap the basemap only when the theme really changes. Re-setting the
  // URL the map was built with, while its first load is in flight, aborts it.
  useEffect(() => {
    const map = mapRef.current
    if (!map || styleTheme.current === theme) return
    styleTheme.current = theme
    themedBasemap(theme).then((style) => mapRef.current === map && map.setStyle(style))
  }, [theme, ready])

  // 3D: pitch the camera and extrude buildings.
  useEffect(() => {
    const map = mapRef.current
    if (!map) return
    // Must run only on 'style.load': adding a layer from 'styledata' (which
    // fires mid-load) throws inside MapLibre's loader and aborts the basemap.
    const ensureBuildings = () => {
      if (!styleReady(map) || !map.getSource('openmaptiles') || map.getLayer('wt-buildings')) return
      const p = readPalette()
      map.addLayer({
        id: 'wt-buildings',
        type: 'fill-extrusion',
        source: 'openmaptiles',
        'source-layer': 'building',
        minzoom: 12,
        layout: { visibility: stateRef.current.mode3d ? 'visible' : 'none' },
        paint: {
          'fill-extrusion-color': `rgb(${p.track.join(',')})`,
          'fill-extrusion-height': ['coalesce', ['get', 'render_height'], 6],
          'fill-extrusion-base': ['coalesce', ['get', 'render_min_height'], 0],
          'fill-extrusion-opacity': 0.55,
        },
      })
    }
    ensureBuildings()
    map.on('style.load', ensureBuildings)
    if (map.getLayer('wt-buildings')) map.setLayoutProperty('wt-buildings', 'visibility', mode3d ? 'visible' : 'none')
    map.easeTo({ pitch: mode3d ? 58 : 0, bearing: mode3d ? -18 : 0, duration: 900 })
    return () => {
      map.off('style.load', ensureBuildings)
    }
  }, [mode3d, ready])

  // Animation loop. Per frame only vehicle positions and bearings change: the
  // vehicle array is stable and mutated in place, static layers are built once,
  // and trails, routes and labels recompute only when the step, selection or
  // zoom bucket changes. (Rebuilding everything each frame measured 2.6-10.6 fps.)
  useEffect(() => {
    if (!ready) return
    let raf = 0
    const timeline = store.timeline
    let palette = readPalette()
    let paletteTheme = store.getState().theme
    let frameNo = 0
    let lastFollow = 0
    let slowKey = ''
    // Arrays handed to deck.gl stay the same object between steps: a new array
    // makes deck rebuild the layer (for text, a full re-layout) on every frame.
    let slow: { labelled: AnimatedVehicle[]; halo: AnimatedVehicle[]; posts: AnimatedVehicle[] } = { labelled: [], halo: [], posts: [] }

    const vehicles: AnimatedVehicle[] = fleetAt(timeline, store.precise())
    const byId = new Map(vehicles.map((v) => [v.vehicle.vehicleId, v]))
    // Labels are HTML pills placed with map.project: crisp type in the app's own
    // font, and no deck.gl text shader to compile.
    const labelEls = new Map<string, HTMLSpanElement>()
    const aspectOf = (v: AnimatedVehicle) => shipmentsRef.current.get(v.vehicle.vehicleId)?.risk.aspect ?? 'clear'

    const tooltip = ({ object, layer }: { object?: unknown; layer?: { id: string } | null }) => {
      if (!object || !layer) return null
      const v = (object as Partial<AnimatedVehicle>).vehicle
      if (!v) return null
      const s = shipmentsRef.current.get(v.vehicleId)
      const corridor = timeline.corridors.find((c) => c.id === v.corridorId)
      const dz = corridor ? inDeadZone(corridor, v.km) : null
      return {
        text: `${v.vehicleId} · ${s?.cargo.name ?? ''}\n${v.cargoC === null ? 'cargo —' : `cargo ${v.cargoC.toFixed(1)} °C`} · ${Math.round(v.speedKmh)} km/h${dz ? `\nNo signal (${dz})` : ''}`,
        className: 'map-tooltip',
      }
    }

    const frame = () => {
      raf = requestAnimationFrame(frame)
      const overlay = overlayRef.current
      const map = mapRef.current
      if (!overlay || !map) return
      frameNo++
      if (store.getState().theme !== paletteTheme) {
        paletteTheme = store.getState().theme
        palette = readPalette()
      }
      const p = palette
      const t = store.precise()
      const { selected: sel, follow: fol, mode3d: is3d } = stateRef.current

      // Per frame: glide positions in place.
      for (const next of fleetAt(timeline, t)) {
        const v = byId.get(next.vehicle.vehicleId)
        if (!v) continue
        v.vehicle = next.vehicle
        v.position = next.position
        v.bearing = next.bearing
      }

      // Per step, selection or zoom bucket: everything that does not glide.
      const step = Math.floor((t - timeline.start) / timeline.stepMs)
      const zoom = map.getZoom()
      const key = `${step}|${sel}|${Math.floor(zoom * 2)}|${paletteTheme}`
      if (key !== slowKey) {
        slowKey = key
        const sv = sel ? byId.get(sel) : undefined
        const corridor = sv && timeline.corridors.find((c) => c.id === sv.vehicle.corridorId)
        const source = (id: string) => map.getSource(id) as maplibregl.GeoJSONSource | undefined
        source('wt-trail')?.setData(lines(sv ? [trailOf(timeline, sv.vehicle.vehicleId, t)] : []))
        source('wt-ahead')?.setData(lines(sv && corridor ? [pathBetween(corridor, sv.vehicle.km, corridor.lengthKm)] : []))
        slow = {
          labelled: vehicles.filter((v) => v.vehicle.vehicleId === sel || (zoom >= 6.2 && aspectOf(v) !== 'clear')),
          halo: sv ? [sv] : [],
          posts: vehicles.filter((v) => aspectOf(v) !== 'clear'),
        }
        const keep = new Set(slow.labelled.map((v) => v.vehicle.vehicleId))
        for (const [id, el] of labelEls) {
          if (keep.has(id)) continue
          el.remove()
          labelEls.delete(id)
        }
        for (const id of keep) {
          if (labelEls.has(id)) continue
          const el = document.createElement('span')
          el.className = 'map-label'
          el.textContent = id
          labelsRef.current?.append(el)
          labelEls.set(id, el)
        }
      }
      for (const v of slow.labelled) {
        const pt = map.project(v.position)
        const el = labelEls.get(v.vehicle.vehicleId)
        // In 3D a mast stands on the vehicle; the label sits above its head.
        const a = aspectOf(v)
        const lift = is3d && a !== 'clear' ? MAST_PX[a] + 6 : 22
        if (el) el.style.transform = `translate(${pt.x.toFixed(1)}px, ${(pt.y - lift).toFixed(1)}px) translate(-50%, -100%)`
      }
      const selectedV = sel ? byId.get(sel) : undefined

      overlay.setProps({
        getTooltip: tooltip,
        layers: [
          ...(selectedV
            ? [
                new IconLayer({
                  id: 'selected-halo',
                  data: slow.halo,
                  getPosition: (v: AnimatedVehicle) => v.position,
                  getIcon: () => haloIcon(p, paletteTheme),
                  getSize: 52,
                  sizeUnits: 'pixels',
                  billboard: false,
                  updateTriggers: { getPosition: frameNo, getIcon: paletteTheme },
                }),
              ]
            : []),
          ...(is3d
            ? [
                new IconLayer({
                  id: 'signal-posts',
                  data: slow.posts,
                  getPosition: (v: AnimatedVehicle) => v.position,
                  getIcon: (v: AnimatedVehicle) => mastIcon(p, aspectOf(v) as Exclude<Aspect, 'clear'>, paletteTheme),
                  getSize: (v: AnimatedVehicle) => MAST_PX[aspectOf(v) as Exclude<Aspect, 'clear'>],
                  sizeUnits: 'pixels',
                  billboard: true,
                  updateTriggers: { getPosition: frameNo, getIcon: slowKey, getSize: slowKey },
                }),
              ]
            : []),
          new IconLayer({
            id: 'vehicles',
            data: vehicles,
            getPosition: (v: AnimatedVehicle) => v.position,
            getIcon: (v: AnimatedVehicle) => vehicleIcon(p, aspectOf(v), v.vehicle.estimated, paletteTheme),
            getSize: 32,
            sizeUnits: 'pixels',
            getAngle: (v: AnimatedVehicle) => -v.bearing,
            billboard: false,
            pickable: true,
            onClick: ({ object }: { object?: AnimatedVehicle }) => object && store.select(object.vehicle.vehicleId, false),
            updateTriggers: { getPosition: frameNo, getAngle: frameNo, getIcon: slowKey },
          }),
        ],
      })

      // Follow camera: bearing-up, smoothly, at most twice a second.
      if (fol && selectedV && performance.now() - lastFollow > 500) {
        lastFollow = performance.now()
        map.easeTo({ center: selectedV.position, bearing: is3d ? selectedV.bearing : 0, zoom: Math.max(zoom, 9.5), padding: panelPadding(), duration: 600, easing: (x: number) => 1 - (1 - x) ** 3 })
      }
    }
    raf = requestAnimationFrame(frame)
    return () => cancelAnimationFrame(raf)
  }, [store, ready])

  const zoomBy = (d: number) => mapRef.current?.easeTo({ zoom: mapRef.current.getZoom() + d, duration: 300 })
  const recenter = () => {
    store.setFollow(false)
    mapRef.current?.fitBounds(fleetBounds(store.timeline.corridors), { padding: panelPadding(), duration: 900, pitch: mode3d ? 58 : 0 })
  }

  return (
    <div className="map-view">
      <div ref={container} className="map-view__canvas" />
      <FleetList />
      <div className="map-controls" role="toolbar" aria-label="Map controls">
        <div className="segmented segmented--map" role="group" aria-label="Map mode">
          <button type="button" aria-pressed={!mode3d} onClick={() => setMode3d(false)}>
            <MapIcon size={14} aria-hidden="true" /> 2D
          </button>
          <button type="button" aria-pressed={mode3d} onClick={() => setMode3d(true)}>
            <Box size={14} aria-hidden="true" /> 3D
          </button>
        </div>
        <div className="map-controls__stack">
          <button type="button" className="map-btn" onClick={() => zoomBy(1)} aria-label="Zoom in">
            <Plus size={16} aria-hidden="true" />
          </button>
          <button type="button" className="map-btn" onClick={() => zoomBy(-1)} aria-label="Zoom out">
            <Minus size={16} aria-hidden="true" />
          </button>
        </div>
        <button type="button" className="map-btn" onClick={() => mapRef.current?.easeTo({ bearing: 0, pitch: mode3d ? 58 : 0, duration: 500 })} aria-label="Reset bearing to north">
          <Compass size={16} style={{ transform: `rotate(${-bearing}deg)` }} aria-hidden="true" />
        </button>
        <button type="button" className="map-btn" onClick={recenter} aria-label="Show the whole fleet">
          <Crosshair size={16} aria-hidden="true" />
        </button>
      </div>
      {selected && <TripCard onFollow={() => store.setFollow(!follow)} />}
      <ul className="map-legend" aria-label="Map legend">
        <li>
          <i className="map-legend__arrow" aria-hidden="true" /> On schedule
        </li>
        <li>
          <i className="map-legend__arrow map-legend__arrow--caution" aria-hidden="true" /> Breach forecast
        </li>
        <li>
          <i className="map-legend__arrow map-legend__arrow--danger" aria-hidden="true" /> Breaching
        </li>
        <li>
          <i className="map-legend__dash" aria-hidden="true" /> No signal
        </li>
      </ul>
    </div>
  )
}
