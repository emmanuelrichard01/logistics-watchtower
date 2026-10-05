# Watchtower console

The operator console: React 19, TypeScript and Vite, with MapLibre for the basemap and deck.gl for vehicles. The design direction ("Signal Box") is in [`docs/design/console.md`](../../docs/design/console.md), and the stack decision in [ADR-0014](../../docs/adr/0014-frontend-stack.md).

## Run

```bash
make console       # production build at http://localhost:4173 (smooth, use for demos)
make console-dev   # dev server with hot reload at http://localhost:5173
make console-check # lint, type check, tests, production build (what CI runs)
```

The dev server is noticeably slower on the map: React development mode, StrictMode double rendering and on-demand compilation. Measured on the reference laptop's GPU with a cold browser profile, the production map settles in 5.8-6.7 s and then holds 58.9-59.4 fps. Before the shader work it took 23-27 s to settle. The dev server ranged from 0.2 to 47.5 fps. Details are in [docs/console](../../docs/console/README.md#performance).

## Data

Until the API stream exists, the console replays **simulator recordings** (`src/data/recording.ts`, fixtures in `apps/dashboard-fixtures`): inter-state trucks and Lagos city rounds on real road geometry. `?data=synthetic` switches to the **seeded synthetic timeline** (`src/data/synthetic.ts`), whose scripted scenarios are pinned by tests. Both go through one frame builder (`src/data/frames.ts`), so knowledge, risk and incidents behave the same. `src/domain/risk.ts` is a **provisional** time-to-breach estimator for UI development only; the real one is the risk engine (plan section 9).

## Scripts

- `npm run capture`: desktop and phone screenshots of every view, in both themes (needs `npm run preview`).
- `npm run perf:map`: cold-start settle time, then frame rate and long-task time on the map page. `SELECT=1` selects a vehicle first; `GUIDE=open` keeps the first-visit guide open.
- `node scripts/perf-shaders.mjs <url>`: WebGL program link time per shader. deck.gl shaders cost seconds each on ANGLE/D3D11, so check this before adding a deck.gl layer type.
