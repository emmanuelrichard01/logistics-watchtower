// The console's input contract. The API's WebSocket stream (plan section 12:
// snapshot then deltas) will deliver exactly these shapes; until then the
// synthetic timeline and the simulator recordings produce them.

/** Signal aspect: what the lamps on a describer tag show (docs/design/console.md). */
export type Aspect = 'clear' | 'caution2' | 'caution1' | 'danger' | 'unknown'

export interface Station {
  name: string
  km: number
  lat: number
  lon: number
  depot?: boolean // cold storage available
  /** Urban customers: hub, supermarket, hospital, pharmacy, qsr, open_market, hotel. */
  type?: string
  /** Delivery window, local time, e.g. "07:30-10:00". */
  window?: string
}

export interface DeadZone {
  name: string
  fromKm: number
  toKm: number
}

export interface Corridor {
  id: string
  name: string
  stations: Station[]
  lengthKm: number
  deadZones: DeadZone[]
  /** 'corridor' for inter-state lanes, 'urban' for city delivery rounds. */
  kind?: 'corridor' | 'urban'
  city?: string
  /** Real road geometry [lon, lat] with cumulative km at each vertex. */
  path?: [number, number][]
  pathKm?: number[]
}

export interface CargoProfile {
  name: string
  setpointC: number
  minC: number
  maxC: number
  valuePerShipmentNgn: number
}

export type ProbeStatus = 'ok' | 'suspect' | 'faulty'

export interface VehicleState {
  vehicleId: string
  corridorId: string
  km: number
  lat: number
  lon: number
  speedKmh: number
  headingDeg: number
  cargoC: number | null
  returnAirC: number | null
  supplyAirC: number | null
  setpointC: number
  door: 'OPEN' | 'CLOSED'
  compressor: 'RUNNING' | 'OFF' | 'FAULT'
  defrost: boolean
  /** Seconds since the last reading actually received (0 when live). */
  lastFixAgeS: number
  /** True while the device is out of coverage and positions are dead-reckoned. */
  estimated: boolean
  probes: { cargo: ProbeStatus; returnAir: ProbeStatus; supplyAir: ProbeStatus }
}

export interface Risk {
  aspect: Aspect
  /** Time-to-breach range in minutes; null when no breach is forecast. */
  ttbP10Min: number | null
  ttbP90Min: number | null
  confidence: number
  /** Why the score is what it is, most important first. */
  reasons: string[]
  expectedLossNgn: number
  ruleVersion: number
}

export interface Shipment {
  id: string
  vehicleId: string
  cargo: CargoProfile
  destination: string
  risk: Risk
}

export type Severity = 'LOW' | 'MEDIUM' | 'HIGH' | 'CRITICAL'
export type IncidentState = 'OPEN' | 'ACKNOWLEDGED' | 'MITIGATING' | 'RESOLVED' | 'AUTO_CLEARED'
export type IncidentType =
  | 'CARGO_TEMP_BREACH'
  | 'BREACH_FORECAST'
  | 'DOOR_OPEN_MOVING'
  | 'SENSOR_FAULT'
  | 'TELEMETRY_GAP'
  | 'COMPRESSOR_FAULT'
  | 'ROUTE_DEVIATION'

export interface Incident {
  id: string
  type: IncidentType
  severity: Severity
  state: IncidentState
  vehicleId: string
  shipmentId: string
  openedAt: number
  lastSeenAt: number
  occurrences: number
  summary: string
  /** Recommended playbook action, most useful first. */
  actions: string[]
}

export interface Frame {
  t: number
  vehicles: VehicleState[]
  shipments: Shipment[]
  incidents: Incident[]
}

export interface SeriesPoint {
  t: number
  cargoC: number | null
  returnAirC: number | null
  supplyAirC: number | null
  door: boolean
  defrost: boolean
  gap: boolean
}

export interface Timeline {
  start: number
  end: number
  stepMs: number
  corridors: Corridor[]
  frameAt(t: number): Frame
  series(vehicleId: string, from: number, to: number): SeriesPoint[]
  /** Moments worth marking on the time handle. */
  markers: { t: number; aspect: Aspect; vehicleId: string }[]
  /** Where the data came from, shown in the chrome. */
  provenance: string
}
