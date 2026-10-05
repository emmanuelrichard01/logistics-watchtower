---
version: 1
slug: "apps-dashboard-index-html"
primary_target: "apps/dashboard/index.html"
related_targets: []
---

# Surface brief: operator console

Scope: the operator console SPA (`apps/dashboard`). Visitor mode: **Operate**. Full spec: `docs/design/console.md`. Owner's references (`~/Downloads/inspos`, 7 SaaS pages) pin the material register: light, airy, one confident blue, rounded soft surfaces, two-tone headlines.

Audience and job: a control-room operator (desk) and a field supervisor (phone, sunlight) must find the shipment that breaches soonest, see why, and act within 10 s.

Memorable moment: dragging the time handle rewinds every view at once. Lanes, map, chart and queue all replay the incident.

Unresolved: audible escalation, 3D renderer choice, tile source, phone push.

## Direction contract

THESIS: Every lane is a track diagram. Shipments sit on the line as describer tags, and time-to-breach sets a signal aspect whose lamp count carries the meaning. It refuses the KPI-tile-row, map-in-the-middle, glowing-dots console.

OWN-WORLD: Porcelain ground #F4F5F7 with white #FFFFFF panels (14 px radius, one soft offset shadow, no borders on panels), ink #0E1116, secondary #5B6472, hairline track #C9CED6. One cobalt #1F4BFF lights the selected route, focus and primary actions. Lamps (#D7372A danger, #E29A00 caution, #1E9E5A clear) appear only as aspects, always as shaped lamp clusters. Dark Operating Centre: #0B0D10 / #14171C / #E8EAEE, cobalt #5B7CFF. Geist with tabular figures; Geist Mono only for IDs.

STORY: The operator sees the one lit lamp in a calm board, understands which shipment breaches when and how sure we are, opens the hinged evidence layer, and acts (acknowledge, call driver) without leaving the board.

FIRST VIEWPORT: A 64 px header (wordmark left, a pill segmented nav Lanes · Map · Incidents · Health centred, ⌘K search, theme toggle, Live pill right). A 32 px two-tone status line ("2 shipments need attention · 14 on schedule"). Left ~70%: stacked lane panels, each a full-width horizontal track with town stations at km posts, hatched dead zones and describer tags with lamp clusters, worst lane first. Right 380 px: the incident strip column, with the top strip's primary action in cobalt. Bottom: a floating pill time handle with the live edge at right.

FORM: Signal Box (rail signal-box mimic diagram), position 3 of the ordered list, seed key 3939752a; materials translated to the owner's references.

FINISH: unreviewed and undocumented is unfinished; this build ends with the finish review, the verdict, DESIGN.md, and every shipping raster carrying its provenance
