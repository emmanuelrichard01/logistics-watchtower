import { PathStyleExtension } from '@deck.gl/extensions'
import { ColumnLayer, IconLayer, PathLayer, ScatterplotLayer, TextLayer } from '@deck.gl/layers'
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
import type { Aspect, Shipment } from '../domain/types'
import { useAppStore, useView } from '../state/context'
import { useStore } from '../state/store'
import { themedBasemap } from './map/basemap'
import { FleetList } from './map/FleetList'
import { fleetAt, fleetBounds, pathBetween, tokenRgb, trailOf, type AnimatedVehicle, type LngLat } from './map/geometry'
import { TripCard } from './map/TripCard'

// Navigation arrow pointing north; tinted per aspect (mask icon).
const ARROW = `data:image/svg+xml;utf8,${encodeURIComponent(
  '<svg xmlns="http://www.w3.org/2000/svg" width="64" height="64" viewBox="0 0 64 64"><path d="M32 6 L52 54 Q32 44 12 54 Z" fill="#fff" stroke="#fff" stroke-width="4" stroke-linejoin="round"/></svg>',
)}`

maplibregl.setWorkerUrl(maplibreWorkerUrl)

/** Camera padding that keeps the fleet clear of the floating panels. */
function panelPadding(): maplibregl.PaddingOptions {
  if (window.matchMedia('(max-width: 760px)').matches) {
    return { top: 64, left: 24, right: 64, bottom: Math.round(window.innerHeight * 0.5) }
  }
  return { top: 80, bottom: 140, left: 380, right: 440 }
}

const URGENCY: Record<Aspect, number> = { danger: 1, caution1: 0.72, caution2: 0.46, unknown: 0.26, clear: 0 }

interface Palette {
  ink: [number, number, number]
  ink3: [number, number, number]
  surface: [number, number, number]
  cobalt: [number, number, number]
  danger: [number, number, number]
  caution: [number, number, number]
  track: [number, number, number]
  hatch: [number, number, number]
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
  }
}

const aspectColor = (p: Palette, a: Aspect) => (a === 'danger' ? p.danger : a === 'caution1' || a === 'caution2' ? p.caution : a === 'unknown' ? p.ink3 : p.ink)

export function MapView() {
  const store = useAppStore()
  const view = useView()
  const theme = useStore(store, (s) => s.theme)
  const selected = useStore(store, (s) => s.selected)
  const follow = useStore(store, (s) => s.follow)
  const container = useRef<HTMLDivElement>(null)
  const mapRef = useRef<maplibregl.Map | null>(null)
  const overlayRef = useRef<MapboxOverlay | null>(null)
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
      if (!map.isStyleLoaded() || !map.getSource('openmaptiles') || map.getLayer('wt-buildings')) return
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
    let slow: { trail: LngLat[]; ahead: LngLat[]; labelIds: Set<string> } = { trail: [], ahead: [], labelIds: new Set() }

    const vehicles: AnimatedVehicle[] = fleetAt(timeline, store.precise())
    const byId = new Map(vehicles.map((v) => [v.vehicle.vehicleId, v]))
    const corridorPaths = timeline.corridors.map((c) => ({ id: c.id, path: pathBetween(c, 0, c.lengthKm) }))
    const deadZones = timeline.corridors.flatMap((c) => c.deadZones.map((z) => ({ name: z.name, path: pathBetween(c, z.fromKm, z.toKm) })))
    const depots = timeline.corridors.flatMap((c) => c.stations.filter((s) => s.depot).map((s) => ({ name: s.name, position: [s.lon, s.lat] as LngLat })))
    // Customer drops on city rounds: what a multi-drop route is made of.
    const customers = timeline.corridors
      .filter((c) => c.kind === 'urban')
      .flatMap((c) => c.stations.filter((s) => !s.depot).map((s) => ({ name: s.name, type: s.type ?? 'stop', window: s.window, position: [s.lon, s.lat] as LngLat })))
    const aspectOf = (v: AnimatedVehicle) => shipmentsRef.current.get(v.vehicle.vehicleId)?.risk.aspect ?? 'clear'

    const staticLayers = (p: Palette) => [
      new PathLayer({ id: 'corridors', data: corridorPaths, getPath: (d) => d.path, getColor: [...p.track, 255], widthUnits: 'pixels', getWidth: 4, capRounded: true, jointRounded: true }),
      new PathLayer({
        id: 'dead-zones',
        data: deadZones,
        getPath: (d) => d.path,
        getColor: [...p.hatch, 230],
        widthUnits: 'pixels',
        getWidth: 8,
        getDashArray: [1.2, 1.2],
        dashJustified: true,
        extensions: [new PathStyleExtension({ dash: true })],
        pickable: true,
      }),
      new ScatterplotLayer({ id: 'customers', data: customers, getPosition: (d) => d.position, getRadius: 5, radiusUnits: 'pixels', getFillColor: [...p.surface, 255], getLineColor: [...p.ink3, 255], lineWidthUnits: 'pixels', getLineWidth: 2, stroked: true, pickable: true }),
      new ScatterplotLayer({ id: 'depots', data: depots, getPosition: (d) => d.position, getRadius: 6, radiusUnits: 'pixels', getFillColor: [...p.surface, 255], getLineColor: [...p.ink, 255], lineWidthUnits: 'pixels', getLineWidth: 2, stroked: true, pickable: true }),
    ]
    let statics = staticLayers(palette)

    const tooltip = ({ object, layer }: { object?: unknown; layer?: { id: string } | null }) => {
      if (!object || !layer) return null
      const o = object as { name?: string; type?: string; window?: string; vehicle?: AnimatedVehicle['vehicle'] }
      if (layer.id === 'customers') return { text: `${o.name}
${(o.type ?? 'stop').replace('_', ' ')}${o.window ? ` · delivery window ${o.window}` : ''}`, className: 'map-tooltip' }
      if (layer.id === 'dead-zones') return { text: `${o.name}: no signal`, className: 'map-tooltip' }
      if (layer.id === 'depots') return { text: `${o.name} depot: cold storage`, className: 'map-tooltip' }
      const v = o.vehicle
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
        statics = staticLayers(palette)
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
        slow = {
          trail: sv ? trailOf(timeline, sv.vehicle.vehicleId, t) : [],
          ahead: sv && corridor ? pathBetween(corridor, sv.vehicle.km, corridor.lengthKm) : [],
          labelIds: new Set(vehicles.filter((v) => v.vehicle.vehicleId === sel || (zoom >= 6.2 && aspectOf(v) !== 'clear')).map((v) => v.vehicle.vehicleId)),
        }
      }
      const selectedV = sel ? byId.get(sel) : undefined
      const labelled = vehicles.filter((v) => slow.labelIds.has(v.vehicle.vehicleId))

      overlay.setProps({
        getTooltip: tooltip,
        layers: [
          ...statics,
          ...(selectedV
            ? [
                new PathLayer({ id: 'trail', data: [slow.trail], getPath: (d: LngLat[]) => d, getColor: [...p.ink3, 140], widthUnits: 'pixels', getWidth: 3, capRounded: true, jointRounded: true }),
                new PathLayer({ id: 'route-ahead', data: [slow.ahead], getPath: (d: LngLat[]) => d, getColor: [...p.cobalt, 255], widthUnits: 'pixels', getWidth: 6, capRounded: true, jointRounded: true }),
                new ScatterplotLayer({ id: 'selected-halo', data: [selectedV], getPosition: (v: AnimatedVehicle) => v.position, getRadius: 24, radiusUnits: 'pixels', getFillColor: [...p.cobalt, 40], getLineColor: [...p.cobalt, 255], lineWidthUnits: 'pixels', getLineWidth: 2.5, stroked: true, updateTriggers: { getPosition: frameNo } }),
              ]
            : []),
          ...(is3d
            ? [
                new ColumnLayer({
                  id: 'signal-posts',
                  data: vehicles,
                  getPosition: (v: AnimatedVehicle) => v.position,
                  radius: 3200,
                  diskResolution: 12,
                  extruded: true,
                  getElevation: (v: AnimatedVehicle) => URGENCY[aspectOf(v)] * 60_000,
                  getFillColor: (v: AnimatedVehicle) => [...aspectColor(p, aspectOf(v)), URGENCY[aspectOf(v)] > 0 ? 210 : 0],
                  updateTriggers: { getPosition: frameNo, getElevation: slowKey, getFillColor: slowKey },
                }),
              ]
            : []),
          new ScatterplotLayer({
            id: 'vehicle-discs',
            data: vehicles,
            getPosition: (v: AnimatedVehicle) => v.position,
            getRadius: 14,
            radiusUnits: 'pixels',
            getFillColor: [...p.surface, 255],
            getLineColor: (v: AnimatedVehicle) => (v.vehicle.estimated ? [...p.ink3, 255] : [...p.surface, 255]),
            lineWidthUnits: 'pixels',
            getLineWidth: 1.5,
            stroked: true,
            pickable: true,
            updateTriggers: { getPosition: frameNo, getLineColor: slowKey },
          }),
          new IconLayer({
            id: 'vehicles',
            data: vehicles,
            getPosition: (v: AnimatedVehicle) => v.position,
            getIcon: () => ({ id: 'arrow', url: ARROW, width: 64, height: 64, mask: true }),
            getSize: 20,
            sizeUnits: 'pixels',
            getAngle: (v: AnimatedVehicle) => -v.bearing,
            getColor: (v: AnimatedVehicle) => [...aspectColor(p, aspectOf(v)), v.vehicle.estimated ? 130 : 255],
            billboard: false,
            pickable: true,
            onClick: ({ object }: { object?: AnimatedVehicle }) => object && store.select(object.vehicle.vehicleId, false),
            updateTriggers: { getPosition: frameNo, getAngle: frameNo, getColor: slowKey },
          }),
          new TextLayer({
            id: 'labels',
            data: labelled,
            getPosition: (v: AnimatedVehicle) => v.position,
            getText: (v: AnimatedVehicle) => v.vehicle.vehicleId,
            getPixelOffset: [0, -26],
            getSize: 12,
            fontFamily: 'Geist Variable, system-ui, sans-serif',
            fontWeight: 600,
            getColor: [...p.ink, 255],
            background: true,
            getBackgroundColor: [...p.surface, 235],
            backgroundPadding: [6, 3],
            updateTriggers: { getPosition: frameNo },
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
