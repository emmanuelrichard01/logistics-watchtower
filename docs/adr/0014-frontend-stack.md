---
status: accepted
date: 2026-10-05
---

# 0014: Frontend stack for the console and the case-study page

## Context and Problem Statement

Two front-end surfaces exist:

- **The operator console.** Authenticated, WebSocket-driven, map-heavy, full mobile parity, 2D/3D maps with up to thousands of moving vehicles.
- **The public case-study page.** Static, SEO-relevant, scroll-driven storytelling, embedding a live demo of the lane renderer.

Which stack serves each?

## Considered Options

1. **Vite SPA** (React + TypeScript) for the console; **Astro** for the case study.
2. **Next.js** for both.
3. **SvelteKit or SolidStart.**

## Decision Outcome

Chosen option: **1**.

**Console: Vite SPA**, with React 19 and TypeScript; deck.gl renders vehicles over a MapLibre basemap (binary attributes, GPU dead reckoning; review 2026-10-05); TanStack Router (type-safe routes, search-param state for deep links) and TanStack Query (REST, once the API exists).

- The WebSocket client runs in a Web Worker.
- MapLibre GL handles 2D and 3D.
- Motion uses CSS and the Web Animations API, with View Transitions for continuity across views.

The console has nothing to server-render: every screen is behind auth and driven by a live stream. Next.js would add a Node runtime to deploy and operate, server components the console can't use, and route handlers that don't hold WebSockets. The build output is static files that FastAPI or any static host can serve.

**Case-study page and docs: one Astro Starlight site** (amended 5 Oct 2026 after the architecture review). It replaces MkDocs, so the project has two web surfaces, not three. The case study is a page in that site: static HTML with zero JavaScript by default, with the real React lane renderer embedded as an island.

**Shared design tokens** are extracted into one package when the second front end starts, so both surfaces stay identical.

Option 2 was rejected for the console for the reasons above. Option 3 would trade the React ecosystem (MapLibre bindings, deck.gl, testing tools) for small runtime gains the console doesn't need.

### Consequences

- Good: one static artifact for the console; no server rendering to secure or scale.
- Good: the case study gets real SEO and near-zero JavaScript.
- Bad: two front-end build tools (Vite and Astro). Astro is Vite-based, so the cost is small.

### Confirmation

Gate 6 performance budgets (`docs/design/console.md` section 9) are measured against the built SPA.
