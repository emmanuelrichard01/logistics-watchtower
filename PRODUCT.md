# Product

<!-- impeccable:product-schema 1 -->

## Platform

web

## Stack

React, TypeScript and Vite; MapLibre GL for maps; TanStack Query for REST; one WebSocket client with resume (rebuild plan, sections 5 and 13). TypeScript types are generated from the API's OpenAPI document. Map tiles must be open and need no API key.

## Users

Four people, one decision each (plan section 3):

- **Control-room operator.** Which truck do I call first, and what do I tell the driver? Works long shifts watching a live fleet; acts under time pressure on CRITICAL alerts.
- **Fleet reliability manager.** Which trucks, routes and drivers cost us cargo? Reviews trends weekly.
- **Compliance or QA officer.** Can I prove this load stayed in range? Needs a shipment's full temperature history, exposure and data gaps as evidence.
- **On-call engineer.** Is the platform itself healthy and trustworthy? Watches freshness, lag, dead letters and quarantine.

Operators and supervisors also act from phones and tablets away from the desk: full workflow parity on mobile is required.

## Product Purpose

A cold-chain risk decision platform for refrigerated truck fleets. It tells an operator which shipment is at risk, how long remains before the cargo is compromised, how confident the system is, what evidence supports that, and what to do next. Success is cargo saved by earlier, better-targeted intervention, with every figure traceable to evidence.

## Positioning

Most fleet dashboards answer "is this truck over the threshold right now?" Watchtower answers "when will this shipment breach, how sure are we, and why?" It forecasts time-to-breach with an uncertainty range, separates sensor faults from real excursions, treats dead zones as normal (devices buffer and replay), and can replay any incident deterministically. Alerts are durable incidents with a lifecycle and evidence, not toasts.

## Operating Context

- Nigerian lanes (Lagos, Ibadan, Ilorin, Abuja, Port Harcourt, Benin, Makurdi): 30-40 °C ambient, long cellular dead zones.
- Alert lifecycle: OPEN → ACKNOWLEDGED → MITIGATING → RESOLVED, or AUTO_CLEARED. Unacknowledged CRITICAL alerts escalate after 3 minutes.
- Playbook actions: call driver, check door seal, switch to shore or genset power, divert to the nearest depot with cold storage, transfer cargo.
- Roles: viewer, operator, admin (RBAC).
- Times are stored in UTC and shown in Africa/Lagos.

## Capabilities and Constraints

- Views: fleet board ranked by expected loss; shipment detail with cargo and air temperatures, forecast range, events and data gaps; incident queue with lifecycle actions; live map; data-health view (plan section 13).
- The live map has 2D and 3D modes and live tracking.
- WebSocket deltas are batched every 250 ms; long lists are virtualised; the client resumes from its last sequence number.
- Must stay smooth with thousands of vehicles (benchmarks target up to 25,000 simulated trucks).
- A separate public case-study page presents the project. Scroll-driven storytelling belongs there, not in the console.
- Schedule: the programme is extended to 14 weeks (ends about 10 Jan 2027) to fund the dashboard.

## Evidence on Hand

- `docs/audit/v1-baseline.md`: measured v1 defects and runtime baseline.
- `legacy/v1/doc/images/demo.gif`: the v1 dashboard, kept as an anti-reference.
- All fleet data is synthetic from the simulator. No real customers, shipments or drivers exist, and none may be implied.

## Product Principles

1. **Decide, don't decorate.** Every element must help one of the four users make their decision.
2. **Never show a projection as a measurement.** Estimated values are labelled with their age and confidence; low confidence always shows its reason.
3. **Evidence one step away.** Any risk figure opens to the readings, rule version and events behind it.
4. **The incident queue is the record.** Notifications are a convenience and never the only place an alert lives.
5. **Calm until it matters.** The resting state is quiet so that a real breach is unmistakable.

## Accessibility & Inclusion

WCAG 2.2 AA. Colour never carries meaning alone (no red-versus-green-only states). Motion respects `prefers-reduced-motion`. Fully keyboard-operable for operators on long shifts.
