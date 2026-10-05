import { Activity, Map as MapIcon, Rows3, Siren } from 'lucide-react'

export const NAV = [
  { to: '/', label: 'Lanes', icon: Rows3 },
  { to: '/map', label: 'Map', icon: MapIcon },
  { to: '/incidents', label: 'Incidents', icon: Siren },
  { to: '/health', label: 'Health', icon: Activity },
] as const
