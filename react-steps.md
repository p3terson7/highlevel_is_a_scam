> Historical migration plan: this document records the original assessment and phased approach. The implementation has since progressed; use `README.md` and the current test/CI configuration for rollout flags, commands, and current behavior. Baseline counts and file-inventory statements below describe the repository at the time of the assessment.

**1. Executive Summary**
Current architecture: FastAPI serves a single HTML shell from [`ui.html`](/Users/petersarateanu/Documents/prog/fghl/highlevel_is_a_scam/app/templates/ui.html), plus hand-written JS/CSS from [`ui_assets`](/Users/petersarateanu/Documents/prog/fghl/highlevel_is_a_scam/app/templates/ui_assets). The UI is already SPA-like: client-side routing, JSON API calls, localStorage state, and a FastAPI fallback for deep links.

Safest strategy: migrate incrementally to `React + TypeScript + Vite`, served by the existing Python backend. Keep FastAPI as the app server, keep existing `/ui` URLs, keep existing APIs initially, and replace one UI area at a time.

Main risks: auth tokens live in localStorage, existing API responses are inconsistent/unwrapped, current JS contains lots of implicit DOM/data-action coupling, and visual/performance work has become entangled with app structure.

Baseline: `python -m pytest` passes, `124 passed`.

**2. Current Architecture Map**
- Backend: FastAPI in [`app/main.py`](/Users/petersarateanu/Documents/prog/fghl/highlevel_is_a_scam/app/main.py), SQLAlchemy models, Alembic migrations, RQ worker.
- UI shell: [`app/api/ui/shell.py`](/Users/petersarateanu/Documents/prog/fghl/highlevel_is_a_scam/app/api/ui/shell.py) serves `/ui`, `/ui/`, `/ui/assets/{asset}`.
- Routing: `/`, `/dashboard`, `/calendar`, etc. fall back to the same shell unless path starts with `admin`, `api`, `docs`, `health`, `metrics`, `sms`, `webhooks`.
- Frontend: one large static HTML file plus `ui-core.js`, `ui-navigation.js`, `ui-views.js`, `ui-actions.js`, `ui-dashboard.js`, `ui-i18n.js`, `ui-bootstrap.js`, and `ui.css`.
- Auth: admin uses `X-Admin-Token`; client portal uses `X-Portal-Token`; both stored in `localStorage`.
- CSRF: no explicit CSRF middleware found. Current header-token auth avoids cookie CSRF, but token exposure via XSS is a risk.
- Tenant scoping: server-side through `_resolve_ui_actor`, `_load_lead_for_actor`, `_client_for_actor`, etc. in [`shared.py`](/Users/petersarateanu/Documents/prog/fghl/highlevel_is_a_scam/app/api/ui/shared.py).
- Tests: Pytest suite covers UI APIs, auth, SMS, webhooks, booking, knowledge, agent behavior.
- Deploy: Python-only Dockerfile and docker-compose. No `package.json`, Vite, frontend build, or CI config found.

**3. Functional Inventory**
| Area | Current route/template | Backend | Actions | Auth | Data | Difficulty | Phase |
|---|---|---|---|---|---|---|---|
| Shell/login/nav | `/ui`, fallback paths | `shell.py`, `session_routes.py` | login, logout, route changes | admin/client | session, clients | Medium | 2-3 |
| Dashboard | `/dashboard`, `#dashboard` | `dashboard_routes.py` | drill-through, CTA buttons | admin/client | leads, tasks, bookings, logs | Medium | 4 |
| Clients/admin | `/clients` | `client_routes.py`, `/admin/clients` | create/edit client, webhooks, provider config | admin mostly | clients, runtime, logs | High | 7 |
| Inbox | `/conversations` | `conversation_routes.py` | send SMS/MMS, notes, handoff, archive, delete | admin/client scoped | leads, messages, attachments | High | 6 |
| Pipeline | `/crm` | `crm_routes.py` | stage changes, open record, contact drawer | admin/client scoped | leads, tags | Medium | 5 |
| Records/lead detail | `/leads` | `crm_routes.py`, `conversation_routes.py` | notes, tags, tasks, stage, archive | admin/client scoped | lead, messages, logs | High | 5 |
| Calendar | `/calendar` | `client_routes.py` | create/update/delete meetings | admin/client scoped | bookings, leads | Medium | 5 |
| Tasks | `/tasks` | `crm_routes.py` | create/update/toggle tasks | admin/client scoped | lead_tasks | Medium | 5 |
| Logs | `/logs` | `/admin/clients/*/audit-logs` | read/open related lead | admin | audit_logs | Low | 8 |
| Settings/client portal | `/settings` | `owner/*`, runtime/admin routes | AI context, FAQ, provider config, calendar | admin/client scoped | client settings, runtime | High | 7 |
| Test Lab | `/test-lab` | `sandbox_routes.py` | start sandbox, send messages | admin | client, lead, messages, agent | Medium | 8 |
| Media | `/media/public/{token}` | `conversation_routes.py` | file download | public token | attachment file | Medium security | 6 |

**4. Recommended Target Architecture**
Use `React + TypeScript + Vite`, not Next.js. The Python backend should remain the app server.

Recommended structure:
```text
frontend/
  package.json
  vite.config.ts
  tsconfig.json
  src/
    app/
    api/
    components/
    features/
    hooks/
    lib/
    styles/
    test/
```

Use:
- React Router for browser routes.
- TanStack Query for server state once API calls spread across views.
- Local state or URL params for UI state.
- React Hook Form only for complex forms like client settings and manual meeting creation.
- Plain CSS/design tokens first, reusing current theme concepts. Avoid Tailwind/component libraries for now.
- Vitest + React Testing Library for component tests.
- Playwright later for critical end-to-end flows.

**5. API Boundary Plan**
Current `/ui/api/*` endpoints are already the practical API boundary. Do not rewrite them first. Build a typed client around existing shapes, then improve consistency gradually.

Initial API client groups:
| API group | Endpoints | React use | Notes |
|---|---|---|---|
| Session | `GET /ui/api/session`, `POST /ui/api/login/client` | auth bootstrap | Preserve headers first |
| Dashboard | `GET /ui/api/dashboard` | dashboard React page | Read-only first island |
| Conversations | `/ui/api/conversations`, `/thread`, `/messages/manual`, `/manual-media`, `/archive`, `/agent-control` | inbox | Needs upload + optimistic UI later |
| CRM | `/ui/api/crm/leads`, `/crm/leads/{id}`, stage/tags/notes/tasks | records/pipeline/tasks | Core migration target |
| Clients/settings | `/ui/api/clients`, `/admin/clients`, `/owner/{client_key}` | admin/settings | High risk due secrets/config |
| Calendar | `/clients/{key}/calendar`, `/calendar/meetings/{id}` | calendar | Good mid-phase migration |
| Sandbox | `/owner/{key}/sandbox/start`, `/sandbox/messages` | test lab | Can stay static until late |
| Media | `/media/public/{token}` | attachments | Keep backend file handling |

Recommended new response convention for new or revised APIs:
```json
{ "data": { "...": "..." } }
```
and errors:
```json
{ "error": { "code": "VALIDATION_ERROR", "message": "...", "fields": {} } }
```
But existing endpoints should remain compatible until their consuming UI is migrated.

**6. Migration Strategy**
Choose **E. Hybrid transitional approach**.

Start with React mounted inside the existing `/ui` shell, then progressively replace static DOM sections. This is safer than a separate SPA deployment because:
- Existing URLs keep working.
- Existing backend auth/scoping stays untouched.
- Docker deploy can remain one service.
- Old JS can coexist temporarily.
- Rollback is easy: remove React mount and keep current static UI.

Avoid a big-bang rewrite. The current UI is too feature-rich for that.

**7. Route Compatibility Plan**
- Keep `/ui`, `/ui/`, `/dashboard`, `/conversations`, `/crm`, `/leads`, `/calendar`, `/tasks`, `/settings`, `/test-lab`.
- React Router should use the same path model currently implemented in `ui-navigation.js`.
- FastAPI fallback in [`app/main.py`](/Users/petersarateanu/Documents/prog/fghl/highlevel_is_a_scam/app/main.py) should continue serving the shell.
- Existing bookmarks continue working.
- API routes under `/ui/api`, `/admin`, `/sms`, `/webhooks`, `/media` remain backend-owned.
- Auth failures stay server-returned `401`; React handles by showing login overlay.

**8. Authentication and Security Plan**
Initial migration:
- Preserve `X-Admin-Token` and `X-Portal-Token` headers.
- React auth provider reads/writes the same localStorage keys for compatibility.
- React calls `GET /ui/api/session` on bootstrap.
- Permission checks remain server-side.
- Tenant isolation remains in `_resolve_ui_actor` and related loaders.

Security improvements to plan later:
- Move portal session to HttpOnly SameSite cookie.
- Add CSRF only if cookie auth is introduced.
- Keep admin token out of frontend bundles and logs.
- Normalize API error responses.
- Use React escaping by default instead of string-built HTML.
- File uploads remain backend-validated through existing `/manual-media` path.

**9. Testing Strategy**
Existing command:
```bash
python -m pytest
```
Current result: `124 passed`.

Add:
- `npm run build` for Vite production build.
- `npm run typecheck`.
- `npm run test` with Vitest.
- Component tests for login shell, dashboard, records, inbox composer, calendar.
- API tests around any new JSON endpoints.
- Playwright later for login, dashboard load, inbox send message, create lead, archive/restore, calendar meeting, client portal scoping.

Important regression flows:
- Login/logout.
- Dashboard load.
- Client portal scoped access.
- Inbox thread load.
- Manual SMS/MMS send.
- Lead create/edit/archive.
- Stage/tag/task/note actions.
- Calendar create/update/delete.
- Settings AI context/FAQ update.
- Permission denied for cross-client access.

**10. Step-By-Step Implementation Plan**
Phase 0: Baseline verification  
Goal: freeze known-good behavior.  
Files likely touched: none.  
Implementation details: run tests, document current branch/commit, optionally record screenshots.  
Risks: none.  
Tests to add: none.  
Tests to run: `python -m pytest`.  
Acceptance criteria: passing baseline.  
Rollback plan: none.

Phase 1: Frontend tooling scaffold  
Goal: add Vite React TS without changing UI behavior.  
Files likely touched: `frontend/*`, `Dockerfile`, `.gitignore`, maybe `README.md`.  
Implementation details: create minimal React app that builds but is not mounted by default.  
Risks: deploy complexity.  
Tests to add: basic React smoke test.  
Tests to run: `npm run build`, `npm run typecheck`, `python -m pytest`.  
Acceptance criteria: no product behavior change.  
Rollback plan: delete `frontend/`.

Phase 2: Python asset integration  
Goal: serve Vite-built assets safely.  
Files likely touched: `app/api/ui/shell.py`, `app/templates/ui.html`, Dockerfile.  
Implementation details: add manifest loader, hashed asset loading, dev/prod handling.  
Risks: broken `/ui` load.  
Tests to add: shell includes built asset when enabled.  
Tests to run: Python tests + frontend build.  
Acceptance criteria: current UI still works.  
Rollback plan: revert shell asset loader.

Phase 3: React mount behind feature flag  
Goal: mount a tiny React island without replacing core UI.  
Files likely touched: `ui.html`, `frontend/src/app`.  
Implementation details: add `<div id="react-root">` or one isolated mount target.  
Risks: CSS collisions.  
Tests to add: shell smoke test.  
Acceptance criteria: page loads same with and without flag.  
Rollback plan: disable flag.

Phase 4: API client and auth provider  
Goal: centralize frontend API calls.  
Files likely touched: `frontend/src/api`, `frontend/src/features/auth`.  
Implementation details: typed `apiClient`, auth headers, 401 handling, session query.  
Risks: token handling bugs.  
Tests to add: API client tests.  
Acceptance criteria: React can fetch session/dashboard.  
Rollback plan: static JS still owns UI.

Phase 5: Dashboard React page  
Goal: migrate read-mostly dashboard first.  
Files likely touched: dashboard React feature, maybe `ui-dashboard.js` only for disabling old renderer.  
Implementation details: consume `GET /ui/api/dashboard`; preserve CTAs/routes.  
Risks: chart/visual regressions.  
Tests to add: dashboard component tests.  
Acceptance criteria: same data and drill-through behavior.  
Rollback plan: re-enable old dashboard renderer.

Phase 6: Records/Pipeline/Tasks/Calendar cluster  
Goal: migrate CRM workflows with shared lead/task/calendar components.  
Files likely touched: `features/records`, `features/pipeline`, `features/tasks`, `features/calendar`.  
Implementation details: reuse typed API client; keep server validation.  
Risks: state synchronization.  
Tests to add: create lead, stage update, archive, task update, calendar update.  
Acceptance criteria: all existing flows pass.  
Rollback plan: route specific views back to static JS.

Phase 7: Inbox  
Goal: migrate highest-interaction UI.  
Files likely touched: `features/inbox`, upload handling, chat components.  
Implementation details: preserve polling, media upload, notes, handoff, archive. Consider virtualization.  
Risks: message send/media regressions.  
Tests to add: manual SMS, MMS upload, scoped access, 401 handling.  
Acceptance criteria: no loss in workflow.  
Rollback plan: old inbox view remains until verified.

Phase 8: Clients/Settings/Test Lab  
Goal: migrate admin/client configuration surfaces.  
Files likely touched: `features/clients`, `features/settings`, `features/testLab`.  
Implementation details: handle provider secrets carefully; never expose saved secrets.  
Risks: provider config and client portal settings.  
Tests to add: settings update, AI context, knowledge ingest, sandbox start.  
Acceptance criteria: config flows match current behavior.  
Rollback plan: old settings/test lab stay available.

Phase 9: Remove static JS views  
Goal: delete replaced legacy assets safely.  
Files likely touched: `ui-*.js`, `ui.html`, tests.  
Implementation details: remove only views fully replaced and covered.  
Risks: orphaned actions.  
Tests to add: route coverage.  
Acceptance criteria: no references to deleted IDs/actions.  
Rollback plan: restore deleted assets from branch.

**11. Cleanup and Deprecation Plan**
Keep during migration:
- Existing `/ui/api/*`.
- Existing static JS/CSS.
- Existing shell/fallback.
- Existing admin and webhook routes.

Delete later:
- Replaced `ui-views.js`, `ui-actions.js`, `ui-dashboard.js` sections.
- Dead HTML sections in `ui.html`.
- Manual asset allowlist once Vite manifest is source of truth.

Avoid duplicate logic by:
- Moving API calls first into a React client.
- Keeping backend services as source of truth.
- Migrating one feature at a time.
- Adding tests before deleting legacy renderers.

**12. Open Questions**
- Should admin auth remain header-token/localStorage long-term? Recommended default: preserve initially, later replace with HttpOnly cookie.
- Should the glass design branch be the React baseline? Recommended default: yes, continue on `glass_design` or `frontend-rebuild`, while keeping `og_design` as fallback.
- Do we want React to replace all pages or only the operator app? Recommended default: only `/ui` first; public webhooks/admin APIs stay backend-only.
- Add TanStack Query immediately? Recommended default: yes after API client scaffold, before dashboard migration.
- Add Playwright now or later? Recommended default: later, after React shell exists.

**13. Initial Low-Risk First Patch Recommendation**
First patch should be: **add React + TypeScript + Vite scaffold without mounting it into production UI yet**.

That gives us:
- `frontend/` structure.
- `npm run build`, `npm run typecheck`, `npm run test`.
- A tiny `App.tsx` smoke component.
- No behavior change.
- No backend route changes except maybe docs/build instructions.
- Existing `python -m pytest` remains the production baseline.
