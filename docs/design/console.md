# Operator Console: Design Brief

Status: **direction chosen, brief awaiting confirmation** (5 Oct 2026). Built in week 9 (first slice) and Phase 5: Experience (weeks 10-11), per ADR-0010. Product truth lives in `PRODUCT.md`; this brief covers only the console surface. The public case-study page has its own short brief at the end.

## 1. Job and audience

Mode: **Operate.** People arrive in the middle of a task, and the tool should disappear into it.

| User | Scene | What they need from the first glance |
| --- | --- | --- |
| Control-room operator | Lagos operations room, long shift, dim light, two large screens | Which shipment breaches soonest, how sure we are, and what to do |
| Supervisor in the field | Depot yard, phone in hard sunlight, one hand free | The same answer and the same actions, on a 390 px screen |
| Reliability manager | Desk, weekly review | Which lanes and units keep losing cargo |
| QA officer | Desk, audit or dispute | One shipment's full history, gaps included, as evidence |
| On-call engineer | Anywhere, often paged | Whether the data itself can be trusted right now |

The two real lighting scenes decide the themes: a dark **Operating Centre** theme for the control room and a light **Enamel** theme for daylight. Neither is decorative, and both are complete.

## 2. Outcome and proof

- **Primary task:** from a cold start, an operator finds the shipment that will breach soonest, sees why, and takes a playbook action, in under 10 seconds and three interactions.
- **Success signals** (measured in Gate 6): the Playwright incident workflow passes on desktop and phone; INP stays under 100 ms; the map holds 60 fps with 1,000 live vehicles on the reference laptop (i7-10510U).
- **Product-specific truth the UI must show:** a time-to-breach *range* with confidence, sensor faults kept separate from excursions, dead zones as normal events with an honest "last heard", estimates never drawn as measurements, and any incident replayable.

## 3. Selected direction: Signal Box

**Thesis.** Every lane is a track diagram. Shipments occupy sections of the line, and time-to-breach sets the signal aspect. The category default (a KPI tile row, a map in the middle, a list down the side, navy with glowing dots) is refused; the map is one view of the line, not the whole console.

**Lineage.** Railway signal-box NX panels and modern rail operating-centre mimic displays. These are interfaces built for people who must notice the one thing that changed, all shift long.

**Signal aspects encode time-to-breach.** Shape and count carry the meaning, so colour never works alone (WCAG):

| Aspect | Lamps | Meaning |
| --- | --- | --- |
| Clear | one green, lower position | No breach forecast in the planning horizon |
| Double yellow | two yellow, stacked | Breach forecast within 45 min (confidence ≥ 0.6) |
| Single yellow | one yellow, upper position | Breach forecast within 15 min |
| Danger | one red, top position | Cargo temperature beyond its limit now |
| Unknown | hollow lamp outline | Confidence too low to call; the reason is always shown |

**Raises carried from the round** (each borrowed discipline, not borrowed clothes):

- *From Design Annual Plates:* structure comes from 1 px hairlines and registration marks alone. No card boxes, no drop shadows, no glass.
- *From Vertical Feed:* on phones each incident owns the whole screen, with actions on a thumb rail and the next incident one swipe away.
- *From Crouwel Grid Specimen:* numerals sit on a visible cell grid. Times, temperatures and km posts align down every lane in tabular figures.
- *From Acetate Tab Manual:* evidence opens as a hinged layer over the board. Layer depth is the only visual rank.
- *From Variable Font Specimen:* one time handle drives every view at once. Lanes, map, charts and queue scrub together, and that same control is replay.

**Own world** (proposed tokens; contrast is validated at build):

| Role | Operating Centre (dark) | Enamel (light) |
| --- | --- | --- |
| Ground | `#111312` | `#E9EAE4` |
| Panel layer | `#181B19` | `#DFE1D9` |
| Hairline | `#2A2E2B` | `#B9BDB2` |
| Track (idle line) | `#59605A` | `#6B7068` |
| Route set (selection, focus, primary action) | `#E9EAE4` | `#121413` |
| Text / secondary text | `#ECEDE8` / `#9AA19B` | `#121413` / `#4E544D` |
| Danger / caution / clear lamps | `#E5402F` / `#F2B33D` / `#3FAE6A` | `#C4321F` / `#B97E06` / `#2E8B53` |

- **Colour strategy: Restrained.** Neutrals plus the lamps. Selection is shown as a *lit route*, in white on dark and black on light, never as a brand blue. A lamp colour only ever means an aspect.
- **Type: one family, Barlow** (open licence, drawn from highway signage), with Barlow Condensed for describer tags and km posts. Tabular figures everywhere. High contrast comes from scale: a 40 px condensed time-to-breach against 12 px labels. The rem scale uses a 1.2 ratio and is not fluid.
- **Material:** hairline track, lamp discs, hatched dead-zone sections, km-post ticks. Nothing glows. Elevation exists only as the hinged evidence layer.

## 4. Scope and boundaries

- **Fidelity:** production screens for five views plus app chrome, on desktop, tablet and phone, in both themes.
- **Views:** Lanes board (home), Map (2D/3D), Incidents, Shipment detail (as a layer, deep-linkable), Data health.
- **Untouched:** product truth and terminology from `PRODUCT.md`; the lifecycle states; the RBAC rules.
- **Anti-goals:**
  - KPI tile rows.
  - Glowing or pulsing markers at rest.
  - Toasts as the only record.
  - Modals as a first resort.
  - Orchestrated load animations.
  - Scroll hijacking.
  - Red-versus-green-only states.
  - Any projection drawn like a measurement.

## 5. First viewport (desktop, 1440 × 900, Operating Centre theme)

- **Left, 56 px:** a nav rail with five glyph-and-label stops. ⌘K opens the command palette (jump to a vehicle, shipment, alert or command).
- **Centre, about 1000 px: the Lanes board.** Each active lane is a horizontal track diagram, for example *Lagos — Ibadan — Ilorin — Mokwa — Abuja*. Towns sit at their km posts, and dead-zone sections are hatched along the line. Each shipment is a **describer tag** (vehicle, cargo profile, cargo temperature) sitting on its position, with its signal lamp at the tag's leading edge. Lanes are ordered by their worst aspect, then by expected loss. About six lanes fit before scrolling.
- **Right, 360 px: incident strips.** Open and escalating incidents, one strip each: aspect, vehicle, time-to-breach range, lifecycle state, age clock. The strips are the incident queue in miniature, not toasts.
- **Bottom, full width, 48 px: the time handle.** A ruled timeline with the live edge at the right. Dragging left enters replay. The handle carries the current time in WAT and the replay speed.
- **Focal moment:** the single red or yellow lamp in an otherwise quiet board. The resting state is deliberately calm, so the one change is unmissable (PRODUCT.md principle 5).
- **Primary action:** the top strip's playbook action (for example "Call driver"), reachable with one keypress (A to acknowledge, Enter to open).

## 6. Views and interaction

### Lanes board

- Hovering a tag lights its route (the section turns route-set white) and shows a hairline readout: speed, last fix, signal age.
- Selecting a tag pins that state and opens the shipment layer.
- A section with no fix for longer than the stale limit turns the tag hollow and adds "est. 4 min" in the secondary colour. The projected position is drawn dashed along the track.
- Aspect changes cross-fade the lamp (200 ms). A lane whose worst aspect rises moves up the board with a FLIP animation (280 ms), and its new red lamp shows one "proving" ring, once.
- An unacknowledged CRITICAL repeats the ring once every 10 s until acknowledged. Nothing loops at rest.

### Map (2D and 3D)

- **2D:** MapLibre GL with a muted basemap restyled to the active theme, north-up. The route corridor is drawn as track, and depots appear as station marks.
- **3D:** 55° pitch with terrain. Each at-risk vehicle stands as a slim **signal post** whose height scales with urgency; the corridor is a raised track ribbon. The 2D/3D toggle is a two-state control plus the 3 key.
- **Live tracking:** positions are dead-reckoned between 250 ms delta batches from the last speed and heading. Measured fixes are solid tags; estimated positions are hollow and labelled with their age. A dead zone shows the last fix plus a dashed projected path.
- **Follow mode:** the camera locks to a vehicle, bearing-up, and is released by any pan.
- **Shared selection:** switching between Lanes and Map morphs the selected tag into its map marker with a View Transition. The selection identity survives every view change.
- **Camera moves:** 600 ms ease-out (the one exception to the short-duration rule, because spatial moves need time to keep orientation). With reduced motion, the camera jumps.
- **Overlays:** dead-zone signal coverage along routes, depots with cold storage, and route-deviation corridors.

### Shipment layer (hinged over the board)

- **Header:** shipment, cargo profile and its limits, aspect, and time-to-breach as a p10-p90 range (for example "breach in 18-31 min, confidence 0.72").
- **Chart:** cargo, return-air and supply-air temperatures against the profile limit band. A "now" rule, with the forecast fan ahead of it. Data gaps hatched, and events (door, defrost, compressor, reboot) as ticks.
- **"Why this score":** contributing factors, each with its evidence event IDs and the rule version.
- **Sensor trust:** per-probe status (OK, suspect, faulty). "Cargo temperature uncertain" is stated plainly when it applies.
- **Actions:** playbook actions in priority order; Resolve asks for an outcome. Optimistic updates carry idempotency keys, and acknowledgements can be undone for 5 s.

### Incidents

- The strip board grouped by lifecycle state: Open, Acknowledged, Mitigating, recently Resolved or Auto-cleared.
- Keyboard triage: J/K to move, A acknowledge, G assign, M mitigate, R resolve, Enter open.
- Deduplicated incidents show their occurrence count.
- Viewers see the actions disabled, with the reason ("Operator role required").

### Data health

The pipeline drawn in the same grammar: gateway → raw → processor → clean → projector → Postgres, plus the archive branch.

- Each section carries its lag in seconds.
- Dead-letter queues are drawn as **sidings** with their depth.
- Quarantine reasons hang off the gateway section.

### Phone (390 px) and tablet

- **Phone:**
  - A bottom bar (Board, Map, Incidents, More) and the Enamel theme quick toggle in the header for daylight.
  - **Board:** a vertical list of compact lane strips.
  - **Incidents:** full-screen snap pages, one incident each, with a right-hand thumb rail (Acknowledge, Call, Mitigate, Open). Swipe to the next incident.
  - **Map:** full screen with a bottom sheet (peek, half, full) for the selected vehicle. The time handle moves into the sheet.
- **Tablet:** two panes (board or map, plus the shipment layer).
- Every workflow completes on every class. Parity is tested, not assumed.

### Replay

- Dragging the time handle, or opening an incident's "Replay" action, enters replay. The time handle shows "REPLAY 07:12 WAT ×8" and a live-edge marker to jump back.
- In replay, lifecycle actions are disabled. The board, map, charts and queue all render the past state from the same deterministic data the backend replays.
- No tint washes the screen; the time handle's state is the signal.

## 7. States and ranges

| Dimension | Range the design must hold |
| --- | --- |
| Fleet | 1 to 25,000 vehicles. Above about 2,000 on the map, zoomed out, vehicles render as a density layer and individual tags appear on zoom |
| Lanes | 3 seeded corridors at launch, scaling to dozens; the board virtualises |
| Incidents | 0 to hundreds open; strips virtualise; dedup counts up to thousands |
| Connectivity | Live; WebSocket reconnecting (banner with last-heard, auto-resume from sequence); projector lagging ("data delayed" banner) |
| First run / empty | No fleet streaming: the board teaches its own grammar (a sample lane with labelled parts) and, in development, offers "Start a scenario" |
| Loading | Skeleton track lines, not spinners |
| Permissions | Viewer, operator, admin; disabled actions explain themselves |
| Errors | An action failure reverts the optimistic state and keeps the strip highlighted with a retry; never a silent loss |

## 8. Motion grammar

- **Durations:** 120 ms for press and hover; 200 ms for state changes; 280 ms for layer hinges and list reordering; 600 ms for camera only.
- **Easing:** a single curve, `cubic-bezier(0.2, 0, 0, 1)`. No bounce, no glow, no parallax, no load choreography.
- **Rule:** motion only ever conveys state (change, arrival, focus, continuity across views).
- **Reduced motion:** opacity-only transitions and instant camera moves.
- **Numbers:** time-to-breach updates once per minute, not per second, to stay calm. The tabular figures never jitter.
- **Scrolling:** native everywhere in the console.

## 9. Constraints, budgets and build approach

- **Stack** (per `PRODUCT.md`):
  - React, TypeScript, Vite; TanStack Query and Router.
  - MapLibre GL for 2D and 3D terrain; deck.gl only if MapLibre's own extrusions can't hold the 1,000-vehicle budget.
  - The WebSocket client in a Web Worker (decode and batch off the main thread).
  - CSS and the Web Animations API for motion; View Transitions for cross-view continuity.
- **Tiles:** a self-hosted Protomaps PMTiles extract of Nigeria (open, no API key, works offline, can live in MinIO). Open DEM terrain tiles. Availability and licences are to be verified at build and recorded in ADR-0013.
- **Budgets:**
  - Console shell under 300 KB gzipped JavaScript, with the map chunk lazy-loaded.
  - LCP under 2.5 s on a mid-range phone.
  - INP under 100 ms.
  - 60 fps map with 1,000 live vehicles on the reference laptop.
  - All measured in CI or in the week 11 performance pass, never claimed.
- **Accessibility:** WCAG 2.2 AA. Aspects are shape-coded. Fully keyboard-operable. Each lane is exposed to assistive technology as an ordered list of shipments with their aspect in words. The map has an equivalent list view.
- **Development:** the UI is built against **recorded simulator scenarios** (fleet state and alert event streams captured to fixtures), so it never waits on the backend. The same fixtures drive Playwright and visual-regression snapshots.
- **Data honesty:** everything is synthetic, and a quiet "Simulated fleet" mark sits in the chrome.
- **Times:** WAT (Africa/Lagos), 24-hour clock.

## 10. Public case-study page (separate surface)

- **Mode:** Persuade, built in week 14. It inherits the Signal Box world at full expressive range.
- **Shape:** a scroll-driven narrative. The problem (heat and dead zones), the mechanism (forecast with uncertainty), the proof (benchmark and alert-quality numbers, each linked to its committed run), then a recorded incident replaying in the real lane renderer.
- **Motion:** CSS scroll-driven animations for reveals. A 3D hero of the Lagos–Abuja corridor with the track drawn over the terrain. Smooth, intentional scroll pacing belongs here, and only here.
- It gets its own direction confirmation before build.

## 11. Open decisions (the builder must not invent these)

1. **Final typeface:** confirm Barlow, or name an alternative with tabular figures and a condensed width.
2. **Audible escalation** for unacknowledged CRITICAL alerts in the control room: yes or no, and with what mute rules.
3. **3D renderer:** MapLibre-only versus adding deck.gl, decided by the week 10 performance spike.
4. **Tile and DEM sources:** confirmed at build, with licences recorded.
5. **Phone push notifications** for field supervisors: in scope for the notifier (plan section 13), or later?
