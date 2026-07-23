# Lead Conversion SMS Agent (Starter)

Production-minded FastAPI starter for a reusable multi-tenant Lead Conversion SMS Agent SaaS.

## Quick How It Works (Current Repo)

1. **Lead webhooks arrive per client key**
   - Zapier: `POST /webhooks/zapier/{client_key}` in `app/api/routes_webhooks.py` (`zapier_webhook`)
   - Website forms: `POST /webhooks/form/{client_key}` in `app/api/routes_webhooks.py` (`website_form_webhook`)
   - Audit logs retain authentication mode, size, lead counts, and a SHA-256 fingerprint; the submitted lead data is persisted on the lead instead of being duplicated in the audit log. Background processing is then enqueued with `enqueue_process_webhook(...)`.

2. **Lead normalization + persistence + initial SMS**
   - Worker logic is in `app/workers/tasks.py` (`process_webhook_payload_task`, `send_initial_sms_task`).
   - Normalization is in `app/services/lead_intake.py` (`normalize_webhook_payload`, `upsert_lead`).
   - State/data is stored in PostgreSQL tables from `app/db/models.py`:
     - `clients`, `leads`, `messages`, `conversation_states`, `audit_logs`, `runtime_settings`.
   - Idempotency is enforced by unique constraint on `(client_id, external_lead_id)` in `Lead`.

3. **Inbound SMS from Twilio**
   - Endpoint: `POST /sms/inbound/{client_key}` in `app/api/routes_sms.py` (`inbound_sms`).
   - Twilio signature verification uses `verify_twilio_signature(...)` in `app/core/security.py`.
   - Production callbacks fail closed unless that client has a Twilio auth token and the request has a valid Twilio signature.
   - STOP/HELP and rate-limits are handled in `app/services/compliance.py`.
   - Messages are saved in `messages`, decisions are saved in `audit_logs`, and state transitions in `conversation_states`.

4. **AI response + booking/handoff**
   - Agent is in `app/services/llm_agent.py` (`LLMAgent.next_reply`).
   - Booking/handoff action helpers are in `app/services/booking.py` (`ensure_booking_link`, `handoff_suffix`).
   - Reply SMS is sent by `SMSService` from `app/services/sms_service.py`.

5. **Provider/runtime config path**
   - Env defaults come from `app/core/config.py`.
   - Global OpenAI overrides are persisted in `runtime_settings`.
   - Twilio, Zapier, language, and public URL settings are tenant-scoped and persisted in each client's `provider_config`.

---

## Get Started (Linear, Actionable)

This path assumes:
- repo already cloned
- Docker installed

### 1) Prepare env

```bash
cp .env.example .env
```

Minimum to set in `.env`:
- `ADMIN_TOKEN`

Generate a long random value (for example, run `openssl rand -hex 32`) and paste the result into `ADMIN_TOKEN`. The API intentionally refuses to start with a value shorter than 32 characters or a common placeholder such as the old `change-me` value.

Production requires `SETTINGS_ENCRYPTION_KEYS` to contain a Fernet key that is independent of `ADMIN_TOKEN`; startup fails when it is missing. Generate one with `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`. During key rotation, list the new key first and retain old keys until all stored secrets have been rewritten.

You can leave OpenAI blank and configure the global AI provider in the UI. Configure Twilio and optional Zapier settings on each client from `Clients > Edit`.

### 2) Start everything with one command

```bash
./run.sh
```

This starts Postgres, Redis, runs Alembic migrations, then starts the API, the default workflow worker, and an isolated website-knowledge worker.
Equivalent command:

```bash
docker compose up --build
```

Non-Compose deployments must run the default queue as `rq worker --with-scheduler`, because automated SMS pacing and after-hours follow-ups use scheduled jobs. They must also run `rq worker knowledge` and use a private Redis deployment with persistence enabled. Remote website extraction is intentionally kept off both the API process and the default SMS/webhook queue. Compose enables Redis AOF persistence and stores it in the `redis_data` volume.

Website knowledge ingestion accepts up to 12 owner-managed public HTTP(S) URLs per run and 48 stored sources per workspace, discovers a bounded set of same-site service/about/capability pages, and extracts readable HTML, form labels/options, metadata, and JSON-LD. Settings reports queued/running/partial results and can explicitly clear all derived knowledge; clearing also supersedes an active crawl so it cannot repopulate the deleted data. A failed refresh keeps last-successful chunks available for source-labelled, query-specific retrieval for at most 30 days, but stale facts are excluded from always-on business memory. Production crawl URLs are encrypted in Redis, query credentials are never returned as source citations, and query-bearing URLs must use HTTPS. Query strings are used for that crawl but are not persisted, so a signed URL must be re-entered for a later refresh. The crawler intentionally does not execute JavaScript, so content available only after client-side rendering needs a server-rendered source URL before it can be indexed.

Demo data is disabled by default. Set `ENABLE_DEMO_SEED=true` only for an intentional local demo environment.

### 3) Open admin UI

- `http://localhost:8000/ui`
- Enter `ADMIN_TOKEN` from `.env`
- A new database starts empty unless you explicitly enabled or manually ran a demo seed

### 4) Configure the global AI provider in UI

Set and save:
- OpenAI: `API Key`, `Model`
- `AI Mode`: `auto` or `heuristic`

These values are saved server-side in `runtime_settings`. Secret values are encrypted at rest, write-only, and are not returned to the browser after save. `heuristic` is the AI off-switch: it prevents OpenAI provider selection even when an environment or stored API key exists.

### 5) Create and configure a client in UI

In **Clients > Edit**:
- Create client (or select existing)
- Set:
  - business name
  - tone
  - timezone
  - qualification questions
  - AI Context / Business Playbook (how the business sells, differentiators, do/don't say rules)
  - FAQ/context (authoritative service/pricing/policy facts)
  - booking URL
  - booking mode: `internal`
  - internal calendar availability (weekly windows, slot length, notice, horizon)
  - operating hours
  - handoff number
  - template overrides (JSON)
  - Twilio `Account SID`, `Auth Token`, and `From Number`
  - `Public Base URL` for your tunnel or deployed host, for example `https://abc123.ngrok-free.app`
  - optional Zapier webhook settings

These tenant-specific channel values are saved in the client's `provider_config`. Secret values are write-only in API and UI responses.

In **Settings > AI Context / Business Playbook** (admin or client portal):
- Update `AI context` and optional `FAQ context` for the selected client without editing code
- Changes apply to new AI replies immediately

### 6) Copy webhook URLs from UI

For selected client key `{client_key}`:
- Zapier lead URL: `https://<your-public-host>/webhooks/zapier/{client_key}`
- Website form URL: `https://<your-public-host>/webhooks/form/{client_key}`
- Twilio inbound URL: `https://<your-public-host>/sms/inbound/{client_key}`

For local testing with real providers, expose API using ngrok/cloud tunnel.

### 7) Configure external providers

#### Twilio
- Phone number webhook (incoming messages):
  - `POST https://<your-public-host>/sms/inbound/{client_key}`
- In production, configure the client's Twilio auth token (or an explicit deployment fallback) and use Twilio-signed callbacks; unsigned callbacks are rejected.
- Authenticated inbound callbacks are admitted through atomic Redis limits before a new lead, MMS job, AI turn, or reply can be created. Limits apply per tenant and per shared Twilio account. Redis outages fail closed outside local/test environments, while known-lead STOP/START consent changes remain available and suppress their reply.

#### Zapier and website forms
- Send lead JSON to the client-specific Zapier or website-form URL shown in the UI.
- CRM intake fails closed unless `crm_webhook_secret` is configured for the client (or as the `CRM_WEBHOOK_SECRET` deployment fallback). The old `zapier_webhook_secret` key remains an inbound-only compatibility alias so existing form relays can be migrated without downtime.
- `ENV=dev` or `ENV=local` never makes intake unsigned by itself. Manual unsigned testing requires `ALLOW_UNSIGNED_CRM_WEBHOOKS=true` and a request whose direct network origin is a loopback address; never enable this on a shared or deployed environment.
- Preferred authentication is `X-CRM-Webhook-Timestamp: <unix-seconds>` plus `X-CRM-Webhook-Signature: sha256=<hex>`, where the signature is HMAC-SHA256 over `<timestamp>.<exact raw request body>`. Signatures outside the five-minute replay window are rejected.
- Existing server integrations may use one of the header-only compatibility forms: `X-CRM-Webhook-Secret`, `X-Zapier-Webhook-Secret`, or `X-Zapier-Token`. Query-string secrets are not accepted.
- Requests are limited to 128 KiB, 10 normalized leads, bounded JSON depth/field sizes, and 60 authenticated requests per client endpoint per minute. Empty or non-actionable leads are rejected.
- SMS consent is opt-in. Include explicit consent evidence (for example `{"consent":{"sms":true,"method":"explicit_checkbox","captured_at":"...","text":"..."}}`) to authorize an initial SMS; omitted consent is treated as not provided and does not withdraw permission already captured for an existing lead. Withdrawal must be explicit.
- The bundled PHP landing forms require an explicit HTTPS `CRM_WEBHOOK_URL` and server-side `CRM_WEBHOOK_SECRET` matching the client secret for CRM relay. Configure `CRM_UPLOAD_TMP_DIR` outside the web root; `CRM_MAIL_FROM` sets the fixed envelope sender, and `CRM_TRUSTED_PROXY_IPS` controls which exact proxy IPs may supply client addresses.
- Set `CRM_FORM_ENV=production` (or `CRM_FORM_PRODUCTION=true`) on the public PHP host. Production form posts fail with HTTP 503 unless both `TURNSTILE_SITE_KEY` and the server-only `TURNSTILE_SECRET_KEY` are configured, `CRM_RATE_LIMIT_REDIS_URL` is a valid `redis://` or `rediss://` URL, and the phpredis extension is installed (`rediss://` requires phpredis 5.3+ for an explicitly verified TLS stream context). `TURNSTILE_EXPECTED_HOSTNAMES` can contain an exact comma-separated hostname allowlist for an additional Siteverify response check.
- When a Turnstile site key is configured, the contact and quote pages render Cloudflare's official widget and submit `cf-turnstile-response`. Every form handler validates the token through the fixed Siteverify endpoint before email or CRM delivery, checks the per-form action, uses the trustworthy client IP when available, follows no redirects, and fails closed on timeouts or invalid responses. Cloudflare documents that server validation is mandatory and tokens are single-use, expire after five minutes, and are limited to 2,048 characters: [Turnstile server-side validation](https://developers.cloudflare.com/turnstile/get-started/server-side-validation/) and [widget embedding](https://developers.cloudflare.com/turnstile/get-started/client-side-rendering/).
- For local development, leave `CRM_FORM_ENV` blank (it inherits `APP_ENV`/`ENV`) or set it to `local`, and omit the Turnstile/Redis settings; the widget stays hidden and the locked file limiter uses `CRM_RATE_LIMIT_DIR` outside the web root. The file limiter is deliberately refused in production because it cannot coordinate multiple PHP workers or hosts. A managed WAF remains useful defense-in-depth.
- Outbound booking delivery accepts only `https://hooks.zapier.com/hooks/catch/...`, uses a stable `event_id` and durable reservation, and retries only delivery failures known to be safe. Configure the Zapier workflow to deduplicate on `event_id`.
- Outbound booking signing uses the distinct `zapier_booking_webhook_secret` client setting (or `ZAPIER_BOOKING_WEBHOOK_SECRET` deployment fallback). When configured, booking JSON includes `X-LeadOps-Timestamp`, `X-LeadOps-Event-Id`, and an HMAC `X-LeadOps-Signature` over the exact request body. Inbound CRM secrets are deliberately never reused for outbound signing.
- Direct Meta and LinkedIn provider callbacks are retired; route those form submissions through Zapier or the website-form endpoint.

### 8) End-to-end test with current endpoints

#### A) Simulate lead intake (Zapier)

```bash
body='{"id":"zapier-lead-001","full_name":"Jane Prospect","phone_number":"+1 (555) 123-4567","email":"jane@example.com","city":"Austin","consent":{"sms":true,"method":"explicit_checkbox","captured_at":"2026-07-13T12:00:00Z","text":"I agree to receive text messages about this request."}}'
timestamp="$(date +%s)"
signature="$(printf '%s' "${timestamp}.${body}" | openssl dgst -sha256 -hmac '<client-webhook-secret>' -hex | awk '{print $2}')"
curl -X POST http://localhost:8000/webhooks/zapier/<client_key> \
  -H "Content-Type: application/json" \
  -H "X-CRM-Webhook-Timestamp: ${timestamp}" \
  -H "X-CRM-Webhook-Signature: sha256=${signature}" \
  --data-binary "$body"
```

Expected:
- lead created/updated
- worker sends initial SMS
- events appear in UI (`Dashboard`, `Conversations`, `Logs`)

#### B) Simulate inbound SMS

Unsigned local callbacks are disabled by default. For this manual curl test only, use a client with no saved Twilio auth token, set `ALLOW_UNSIGNED_TWILIO_WEBHOOKS=true` in the local `.env`, and restart the API. Keep this flag `false` whenever the app is exposed publicly or connected to Twilio.

```bash
curl -X POST http://localhost:8000/sms/inbound/<client_key> \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "From=+15551234567" \
  --data-urlencode "Body=Can I book this week?" \
  --data-urlencode "MessageSid=SM-IN-001"
```

Expected:
- inbound message stored
- AI decision created
- outbound SMS reply sent
- booking link included when action is `send_booking_link`

### 9) Real lead flow checklist

When fully configured with public URLs and provider credentials:
- Real form submit -> webhook route receives event -> worker normalizes lead -> initial SMS sent
- Real SMS reply -> Twilio inbound route -> AI response -> booking link or handoff sent

---

## UI Tour

The UI is now a compact operator workspace with two roles:
- `Admin`: full overview across all clients
- `Client portal`: scoped inbox for one business owner, limited to that client's own leads
- Left sidebar with icon-first navigation: `Dashboard`, `Clients`, `Conversations`, `CRM`, `Leads`, `Calendar`, `Tasks`, `Logs`, `Settings`, `Test Lab`
- Top bar with global search, current client selector, environment/runtime badges, refresh, and dark/light theme toggle
- Dense panels, small controls, monospace metadata, and split panes optimized for desktop operations
- The full React shell is the default UI. The tested legacy shell remains available as an emergency rollback while the migration is monitored.
- Set `UI_LEGACY_SHELL_ENABLED=true` to force the legacy shell. `UI_REACT_ISLAND_ENABLED=true` is retained for targeted migration testing when `UI_REACT_APP_SHELL_ENABLED=false`; the full-shell flag wins if both React flags are enabled.
- `UI_LEGACY_SHELL_ENABLED=true` is an emergency override and takes precedence over both React flags. If React is selected but the Vite manifest/build is missing, the UI fails closed with HTTP 503; it does not silently downgrade. Set the legacy override explicitly for emergency rollback.
- Browser login establishes a signed `SameSite=Strict` session cookie that is `HttpOnly`. `UI_SECURE_COOKIES=auto` makes it `Secure` outside explicit local/test environments; production refuses an insecure override. Unsafe `/ui/api/*` and `/admin/*` browser requests also require the matching `leadops_csrf` cookie in `X-CSRF-Token`; the frontend supplies it automatically.
- `/ui/api/login/client` is cookie-only and never returns a reusable bearer token. Non-browser server integrations that still require `X-Portal-Token` must temporarily set `ENABLE_LEGACY_PORTAL_TOKEN_LOGIN=true` and obtain it from `POST /ui/api/login/client/token`. That compatibility endpoint is disabled by default and should remain inaccessible to browser code.
- Non-browser server integrations may continue using the explicit `X-Admin-Token` or gated `X-Portal-Token` bearer-style headers. Those headers are intended for controlled server clients and do not require the browser cookie/CSRF flow when no valid browser session cookie accompanies the request.
- Rebuild the image after frontend changes so Docker and Compose serve the same hashed assets.

`[Screenshot placeholder: Lead Ops Console shell with sidebar and top bar]`

### Dashboard
- Status cards for live conversation counts, booking states, and handoffs
- Operator queue preview for fast jumps into active threads
- Compact onboarding hints and runtime snapshot badges

`[Screenshot placeholder: Dashboard with stat cards and onboarding hints]`

### Clients
- Left pane: dense client index
- Right pane: client workspace with tabs for `Overview`, `Edit`, and `Webhooks`
- `Overview` shows onboarding, recent conversations, and recent logs
- `Edit` is the tenant settings form
- `Webhooks` is the copy-friendly provider panel
- `AI Context / Business Playbook` in `Edit` is injected into the AI prompt immediately for new replies (no code changes required)

`[Screenshot placeholder: Clients view with list on the left and edit pane on the right]`

### Conversations
- Three-pane workspace:
  - left: inbox list with filters
  - center: thread timeline and manual outbound composer
  - right: lead details, tags, notes, audit trail, and quick actions
- Drag the pane dividers to resize the inbox and details columns
- The first available conversation auto-opens so the workspace is not blank on load
- Use the top search bar to filter the current view without opening another page
- `Delete` permanently removes the lead and its full conversation history

`[Screenshot placeholder: Conversations split-pane workspace with thread open]`

### CRM
- Pipeline board grouped by CRM stage:
  - `New Lead`
  - `Contacted`
  - `Qualified`
  - `Meeting Booked`
  - `Meeting Completed`
  - `Won`
  - `Lost`
- Stage counts are shown as compact badges in the toolbar
- Click any card to open the full CRM lead record

`[Screenshot placeholder: CRM board by stage]`

### Leads
- Full CRM lead record view with:
  - contact/source fields
  - CRM stage + AI conversation state
  - lead summary + normalized form answers
  - internal notes
  - tasks/follow-ups
  - tags
  - conversation preview
  - activity timeline (messages, state changes, stage updates, booking events, notes, task events)
- Stage can be updated directly from this page

`[Screenshot placeholder: Lead detail CRM record view]`

### Tasks
- Follow-up queue across all leads
- Filters:
  - client
  - status (`open` / `done`)
- Row actions:
  - mark done/reopen
  - jump to linked lead record

`[Screenshot placeholder: Tasks list view]`

### Calendar
- Internal meeting calendar for the selected client
- Shows upcoming meetings booked by the SMS AI flow
- Use `Clients > Edit > Internal calendar` to configure weekly availability that powers slot offers

`[Screenshot placeholder: Calendar view with upcoming booked meetings]`

### Logs
- Selected-client event summary cards
- Dense audit log table with local search from the top bar
- Fast jump from a log row back into the matching conversation

`[Screenshot placeholder: Logs view with event cards and audit table]`

### Settings
- Global OpenAI settings and selected-client setup status
- Selected-client webhook URLs and demo-data controls in the right column
- Selected-client `AI Context / Business Playbook` and internal calendar availability are editable here (admin and client portal)
- Demo seed/reset stays in Settings to avoid another low-frequency page
- Saved secrets are write-only; the console shows configured/not-configured status without returning credential values

`[Screenshot placeholder: Settings view with runtime config and demo controls]`

### Test Lab
- Live test-contact launcher for texting a real phone
- Copy-ready Zapier webhook URL for the selected client
- Zapier POST console showing latest ingestion events/results from webhook through lead normalization
- Safe simulations for lead intake and inbound SMS
- Provider probes for Twilio test SMS and AI response testing
- Compact output console for the latest response payload

Zapier webhook step tips:
- Method: `POST`
- Payload type: `JSON`
- `Wrap Request In Array`: `No`
- Send either structured fields (`id`, `full_name`, `phone_number`, `email`, etc.) or a text blob; the backend now parses common `Key : "Value"` blobs into normalized lead context fields.
- Parsed lead context is persisted on the lead record and included in:
  - the `Lead summary` block in the thread details pane
  - the AI context payload (`lead_form_answers` + `lead_summary`) for booking conversations

`[Screenshot placeholder: Test Lab with live-contact form and output console]`

## Client Portal

The same `/ui` entry point now supports a client-only role:
- Admin signs in with `ADMIN_TOKEN`
- Client signs in with the portal email/password configured on that client record
- Client access is restricted to that business's own leads and conversations
- The client portal defaults to the `Conversations` workspace and hides the admin-wide overview/config screens

Admin setup for a client login:
- Open `Clients`
- Select a client
- Open the `Edit` tab
- Set `Portal display name`, `Portal email`, `Portal password`, and `Portal enabled`
- Save the client

Seeded demo portal credentials:
- `owner@demo-roofing.demo`
- `owner@demo-medspa.demo`
- `owner@demo-legal.demo`
- shared password: `demo-portal-2026`

These are intentionally known demo credentials. Never enable demo seeding in production or on an internet-accessible environment.

## Demo Seed Data

Automatic dev behavior:
- `docker compose up --build` runs migrations and invokes the seed command, which is a no-op unless `ENABLE_DEMO_SEED=true`
- If demo data is already present, the seed step skips re-creating it
- Seeded data includes CRM-ready records:
  - mixed CRM stages
  - internal notes
  - tags
  - open/done tasks
  - activity timeline events (messages, transitions, stage updates)

Manual commands:

```bash
python -m app.scripts.seed_demo
python -m app.scripts.seed_demo --reset
python -m app.scripts.seed_demo --reset-only
```

StackLeads showcase seed:

```bash
python -m app.scripts.seed_stackleads_demo --reset
python -m app.scripts.seed_stackleads_demo --reset-portal
```

3D PreciScan showcase seed:

```bash
python -m app.scripts.seed_preciscan_demo --reset
python -m app.scripts.seed_preciscan_demo --reset-portal
```

When running the app through Docker, run those inside the API container:

```bash
docker compose exec api python -m app.scripts.seed_stackleads_demo --reset
docker compose exec api python -m app.scripts.seed_stackleads_demo --reset-portal
docker compose exec api python -m app.scripts.seed_preciscan_demo --reset
docker compose exec api python -m app.scripts.seed_preciscan_demo --reset-portal
```

For direct local SQLite runs, migrate the database first and pass it explicitly:

```bash
DATABASE_URL=sqlite:///./local.db alembic upgrade head
python -m app.scripts.seed_preciscan_demo --reset --database-url sqlite:///./local.db
```

Default StackLeads portal login after `--reset` or `--reset-portal`:
- email: `demo@stackleads.local`
- password: `StackLeadsDemo2026!`

Regular StackLeads reseeds preserve a password changed through the UI. Use `--reset-portal` only when you want to force the known demo login back.

Default 3D PreciScan portal login after `--reset` or `--reset-portal`:
- email: `demo@3dpreciscan.local`
- password: `PreciScanDemo2026!`

UI controls:
- Open `/ui`
- Go to `Settings`
- Use `Seed Demo Data`, `Reseed Demo Data`, or `Reset Demo Data`
- For your own business tenant (for example `prototype`), use `Seed selected client` / `Reseed selected client` to inject realistic showcase leads, conversations, tasks, tags, and logs directly into the currently selected client.

Seeded portal logins:
- Demo clients are created with client-portal access enabled
- Use the credentials in `Client Portal` above to test the scoped view immediately

## CRM Stage Mapping

CRM stages are business-facing pipeline labels and remain separate from AI conversation states.

- `New Lead`: captured, no meaningful outreach yet
- `Contacted`: outbound sent (auto/manual)
- `Qualified`: meaningful response captured
- `Meeting Booked`: booking confirmed or set manually
- `Meeting Completed`: post-meeting follow-up phase
- `Won`: closed as customer
- `Lost`: closed-lost or opted out

How stages relate to AI states:
- AI states (`NEW`, `GREETED`, `QUALIFYING`, `BOOKING_SENT`, `BOOKED`, `HANDOFF`, `OPTED_OUT`) continue to drive the SMS workflow.
- CRM stages drive pipeline operations in the `CRM`, `Leads`, and `Tasks` pages.
- Conservative auto-updates are enabled:
  - first outbound -> `Contacted`
  - meaningful inbound -> `Qualified`
  - booking confirmation -> `Meeting Booked`

## Live Phone Testing

To test the conversation AI with your own phone:
- Create or select a client
- Configure that client's Twilio credentials and `Public Base URL` in `Clients > Edit`
- In `Settings` or `Clients > Webhooks`, copy that client's Twilio inbound webhook URL and set it on your Twilio phone number
- Open `Test Lab` and enter your personal phone number in `Live test contact`
- Send the first outbound SMS
- Reply from your phone

What happens next:
- The first outbound SMS is sent from `Test Lab`
- Your phone reply hits `POST /sms/inbound/{client_key}`
- The existing inbound route runs compliance checks and AI reply generation without waiting for business-hour windows
- The `Conversations` workspace updates with inbound/outbound messages and state transitions

If Twilio is not configured:
- The UI still lets you test the flow in mock mode
- Outbound messages are logged locally instead of going to a real device

Important:
- Twilio cannot call `http://localhost:8000/...`
- If you are developing locally, expose the API with ngrok or a Cloudflare tunnel and use that HTTPS URL as `Public Base URL`
- If your OpenAI key is quota-blocked or you want deterministic local testing, switch `AI Mode` to `heuristic`

## UI Admin/Settings Coverage

`/ui` now supports:
- Global OpenAI runtime config and client-scoped Twilio/Zapier config
- Client-scoped provider config per business owner (`Clients > Edit > Provider credentials`)
- Client provider readiness and configured-status visibility in workspace cards and top badges
- Client create + dense client workspace editing
- Client portal credential management per client
- Three-pane conversation inbox with thread view and quick actions
- CRM board view (`CRM`) with per-stage lead cards
- CRM lead record page (`Leads`) with stage controls, notes, tags, tasks, and timeline
- CRM task queue (`Tasks`) with open/done workflow and lead jump links
- Lead summary block + normalized form answers in thread details
- Hard delete for conversations from the thread details pane
- Internal notes stored in `audit_logs`
- Copy/paste webhook URLs for selected client
- Logs workspace for selected-client event review
- Test Lab actions for intake/SMS/AI
- Demo seed + reset controls in dev
- Recent event summary and audit log preview per client

Security behavior:
- Provider secrets are write-only; authenticated API/UI responses expose only configured-status flags and safe display values
- Provider secrets are not logged by these admin routes
- Production Twilio callbacks require an effective auth token and a valid signature; client settings override the optional deployment fallback

---

## Environment Variables (`.env.example`)

Current variables used by the app:
- `APP_NAME`
- `ENV`
- `LOG_LEVEL`
- `AUTO_CREATE_TABLES`
- `DATABASE_URL`
- `REDIS_URL`
- `POSTGRES_PASSWORD` (Compose)
- `TWILIO_ACCOUNT_SID`
- `TWILIO_AUTH_TOKEN`
- `TWILIO_FROM_NUMBER`
- `PUBLIC_BASE_URL`
- `SMS_PROVIDER_MODE` (`auto`, `twilio`, or explicit `mock`)
- `SETTINGS_ENCRYPTION_KEYS`
- `ALLOW_UNSIGNED_TWILIO_WEBHOOKS`
- `ALLOW_UNSIGNED_CRM_WEBHOOKS`
- `CRM_WEBHOOK_SECRET`
- `ZAPIER_BOOKING_WEBHOOK_SECRET`
- `OPENAI_API_KEY`
- `OPENAI_MODEL`
- `AI_PROVIDER_MODE`
- `ADMIN_TOKEN`
- `ENABLE_DEMO_SEED`
- `UI_REACT_ISLAND_ENABLED`
- `UI_REACT_APP_SHELL_ENABLED`
- `UI_LEGACY_SHELL_ENABLED`
- `UI_SECURE_COOKIES` (`auto` by default; secure outside local/test environments)
- `ENABLE_LEGACY_PORTAL_TOKEN_LOGIN` (temporary server-client compatibility; disabled by default)
- `RQ_EAGER`
- `TWILIO_INBOUND_TENANT_LIMIT` (aggregate authenticated callbacks per tenant)
- `TWILIO_INBOUND_ACCOUNT_LIMIT` (shared cap when multiple tenants use one Twilio account)
- `TWILIO_INBOUND_WINDOW_SECONDS` (Redis-coordinated admission window; deployed outages fail closed)
- `RATE_LIMIT_COUNT`
- `RATE_LIMIT_WINDOW_MINUTES`
- `AUTOMATED_SMS_DELAY_SECONDS` (default `20`; delays initial and automated reply SMS, `0` disables pacing)
- `AFTER_HOURS_FOLLOWUP_MINUTES`
- `REQUEST_TIMEOUT_SECONDS`
- `REQUEST_BODY_MAX_BYTES`
- `MESSAGE_MEDIA_MAX_BYTES`
- `MESSAGE_MEDIA_STORAGE_DIR`
- `STACKLEADS_ZAPIER_BOOKING_WEBHOOK_URL` (optional local demo only)
- `CRM_FORM_ENV` / `CRM_FORM_PRODUCTION` (PHP public-form deployment mode)
- `TURNSTILE_SITE_KEY`, `TURNSTILE_SECRET_KEY`, `TURNSTILE_EXPECTED_HOSTNAMES` (PHP bot verification)
- `CRM_RATE_LIMIT_REDIS_URL`, `CRM_RATE_LIMIT_REDIS_PREFIX` (PHP distributed form limiter)
- `CRM_RATE_LIMIT_DIR`, `CRM_UPLOAD_TMP_DIR`, `CRM_TRUSTED_PROXY_IPS`, `CRM_MAIL_FROM` (PHP form hardening)

Note: global OpenAI UI overrides are stored in `runtime_settings`; per-client Twilio, Zapier, language, and public URL values are stored in `clients.provider_config`. Provider secrets and Calendly tokens are encrypted before persistence. Existing plaintext rows are upgraded on application startup.

---

## Troubleshooting (Most Common)

### 1) `403 Invalid Twilio signature`
- Confirm Twilio webhook URL matches exactly the public URL receiving requests.
- Ensure the selected client's Twilio auth token, or the deployment fallback when no client override exists, matches the Twilio console.
- Production rejects callbacks when the effective Twilio auth token is missing or the signature is invalid. In local `dev`/`test`, unsigned manual curl requests require both a blank effective Twilio token and the explicit `ALLOW_UNSIGNED_TWILIO_WEBHOOKS=true` opt-in.
- If you are using ngrok or another tunnel, set `Public Base URL` in `/ui` to that exact HTTPS origin.

### 1b) Twilio auto-replies with `Thanks for the message. Configure your number's SMS URL...`
- Twilio is not sending inbound SMS to your app yet.
- Set the phone number's incoming message webhook to the full `https://.../sms/inbound/{client_key}` URL from `Settings` or `Clients > Webhooks`.
- Do not use `localhost`; use your tunnel or deployed host.
- After updating the number webhook, send a fresh inbound SMS and watch the app logs for `POST /sms/inbound/{client_key}`.

### 2) Webhook accepted but no SMS sent
- Check the default worker is running (included in `docker compose up --build`)
- For website knowledge ingestion, also check the dedicated `rq worker knowledge` process
- Check Redis reachable from API/worker
- Check client has phone in normalized payload
- Check the lead has explicit SMS consent evidence; consent defaults to false
- Check lead isn’t opted out and didn’t already receive initial SMS

### 3) ngrok/public URL changed
- Update URLs in Twilio, Zapier, and website-form integrations
- Re-test with UI Test Lab

### 4) DB migration / table errors
- Ensure DB is up
- Run:
  - `alembic upgrade head`
- Confirm `runtime_settings` table exists (migration `20260226_0002`)

### 5) AI responses not using OpenAI
- Check runtime status shows AI configured
- Ensure valid OpenAI key
- If not configured, app falls back to heuristic provider by design

---

## Tests

```bash
python -m pip install -r requirements-dev.txt
python -m pytest -q
python scripts/run_chatbot_evals.py --agent v3 --suite smoke --provider replay
```

Production images install only `requirements.txt`; test, lint, and dependency-audit tooling stays in `requirements-dev.txt`.

The chatbot evaluation command runs synthetic conversations through the real inbound Agent V3 pipeline with fake SMS/calendar boundaries, so it requires no lead creation, provider credentials, or network access. See [the chatbot evaluation guide](evals/chatbot/README.md) for regression, journey, live-model, and model-judge runs.

Current tests cover:
- webhook intake -> lead creation -> initial outbound message
- inbound SMS -> agent decision -> outbound reply and state transition
