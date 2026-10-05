# Watchtower console

The operator console: React 19, TypeScript and Vite, with MapLibre for the basemap and deck.gl for vehicles. The design direction ("Signal Box") is in [`docs/design/console.md`](../../docs/design/console.md), and the stack decision in [ADR-0014](../../docs/adr/0014-frontend-stack.md).

## Run

```bash
make console       # production build at http://localhost:4173 (smooth, use for demos)
make console-dev   # dev server with hot reload at http://localhost:5173
make console-check # lint, type check, tests, production build (what CI runs)
```

The dev server is noticeably slower on the map: React development mode, StrictMode double rendering and on-demand compilation. Measured on the reference laptop's GPU, the production build holds 59.8-59.9 fps on the map. The dev server ranged from 0.2 to 47.5 fps.

## Data

Until the API stream exists, the console replays **simulator recordings** (`src/data/recording.ts`, fixtures in `apps/dashboard-fixtures`): inter-state trucks and Lagos city rounds on real road geometry. `?data=synthetic` switches to the **seeded synthetic timeline** (`src/data/synthetic.ts`), whose scripted scenarios are pinned by tests. Both go through one frame builder (`src/data/frames.ts`), so knowledge, risk and incidents behave the same. `src/domain/risk.ts` is a **provisional** time-to-breach estimator for UI development only; the real one is the risk engine (plan section 9).

## Scripts

- `npm run capture`: desktop and phone screenshots of every view, in both themes (needs `npm run preview`).
- `npm run perf:map`: frame rate and long-task time on the map page.
