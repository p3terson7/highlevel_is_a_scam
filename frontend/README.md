# React Frontend

This directory contains the incrementally deployed React migration:

- FastAPI serves `app/templates/ui.html` and `app/templates/ui_assets/*` by default, even when a Vite build exists.
- React islands mount into that shell only when `UI_REACT_ISLAND_ENABLED=true` and a Vite manifest exists.
- The full React-owned shell is explicitly enabled with `UI_REACT_APP_SHELL_ENABLED=true`.
- If both React flags are enabled, the full React shell wins.
- `UI_LEGACY_SHELL_ENABLED=true` overrides both React modes for emergency rollback.
- If the build or manifest is missing, either React mode falls back to the legacy shell.

## Commands

```bash
npm install
npm run typecheck
npm run test
npm run build
```

To preview React islands inside the existing FastAPI `/ui` shell, build the frontend and run the app with `UI_REACT_ISLAND_ENABLED=true`.

To preview the full React app shell, build the frontend and run FastAPI with `UI_REACT_APP_SHELL_ENABLED=true`. Running without a rollout flag continues to serve the legacy shell.
