# Lead Conversion SMS Agent (Starter)

Production-minded FastAPI starter for a reusable multi-tenant Lead Conversion SMS Agent SaaS.

## Quick How It Works (Current Repo)

1. **Lead webhooks arrive per client key**
   - Meta: `GET/POST /webhooks/meta/{client_key}` in `app/api/routes_webhooks.py` (`verify_meta_webhook`, `meta_webhook`)
   - LinkedIn: `POST /webhooks/linkedin/{client_key}` in `app/api/routes_webhooks.py` (`linkedin_webhook`)
   - Webhook payloads are saved to `audit_logs`, then background processing is enqueued with `enqueue_process_webhook(...)`.

2. **Lead normalization + persistence + initial SMS**
   - Worker logic is in `app/workers/tasks.py` (`process_webhook_payload_task`, `send_initial_sms_task`).
   - Normalization is in `app/services/lead_intake.py` (`normalize_webhook_payload`, `upsert_lead`).
   - State/data is stored in PostgreSQL tables from `app/db/models.py`:
     - `clients`, `leads`, `messages`, `conversation_states`, `audit_logs`, `runtime_settings`.
   - Idempotency is enforced by unique constraint on `(client_id, external_lead_id)` in `Lead`.

3. **Inbound SMS from Twilio**
   - Endpoint: `POST /sms/inbound/{client_key}` in `app/api/routes_sms.py` (`inbound_sms`).
   - Twilio signature verification uses `verify_twilio_signature(...)` in `app/core/security.py`.
   - STOP/HELP and rate-limits are handled in `app/services/compliance.py`.
   - Messages are saved in `messages`, decisions are saved in `audit_logs`, and state transitions in `conversation_states`.

4. **AI response + booking/handoff**
   - Agent is in `app/services/llm_agent.py` (`LLMAgent.next_reply`).
   - Booking/handoff action helpers are in `app/services/booking.py` (`ensure_booking_link`, `handoff_suffix`).
   - Reply SMS is sent by `SMSService` from `app/services/sms_service.py`.

5. **Provider/runtime config path**
   - Env defaults come from `app/core/config.py`.
   - Runtime UI-saved overrides are persisted in `runtime_settings` table and used by webhook/SMS routes, workers, and provider builders.

---

## Get Started (Linear, Actionable)

This path assumes:
- repo already cloned
- Docker installed

### 1) Prepare env (optional but recommended)

```bash
cp .env.example .env
```

Minimum to set in `.env`:
- `ADMIN_TOKEN`

You can leave Twilio/OpenAI/Meta/LinkedIn vars blank and set them from the UI later.

### 2) Start everything with one command

```bash
./run.sh
```

This starts Postgres, Redis, runs Alembic migrations, then starts API + worker.
Equivalent command:

```bash
docker compose up --build
```

In the default dev compose setup, demo clients and seeded conversations are also populated automatically.

### 3) Open admin UI

- `http://localhost:8000/ui`
- Enter `ADMIN_TOKEN` from `.env`
- The first load should already contain seeded demo clients and conversations in dev

### 4) Configure providers in UI (Runtime Provider Settings)

Set and save:
- Twilio: `Account SID`, `Auth Token`, `From Number`
- `Public Base URL` for your tunnel or deployed host, for example `https://abc123.ngrok-free.app`
- OpenAI: `API Key`, `Model`
- `AI Mode`: `auto` or `heuristic`
- Meta verify token
- LinkedIn verify token

These values are saved server-side (table: `runtime_settings`) and are not returned to browser after save.

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
  - operating hours
  - handoff number
  - template overrides (JSON)

In **Settings > AI Context / Business Playbook** (admin or client portal):
- Update `AI context` and optional `FAQ context` for the selected client without editing code
- Changes apply to new AI replies immediately

### 6) Copy webhook URLs from UI

For selected client key `{client_key}`:
- Meta verify URL: `https://<your-public-host>/webhooks/meta/{client_key}`
- Meta events URL: `https://<your-public-host>/webhooks/meta/{client_key}`
- LinkedIn URL: `https://<your-public-host>/webhooks/linkedin/{client_key}`
- Twilio inbound URL: `https://<your-public-host>/sms/inbound/{client_key}`

For local testing with real providers, expose API using ngrok/cloud tunnel.

### 7) Configure external providers

#### Twilio
- Phone number webhook (incoming messages):
  - `POST https://<your-public-host>/sms/inbound/{client_key}`

#### Meta Lead Ads
- Verify callback:
  - `GET https://<your-public-host>/webhooks/meta/{client_key}?hub.mode=subscribe&hub.verify_token=<META_VERIFY_TOKEN>&hub.challenge=<challenge>`
- Event callback:
  - `POST https://<your-public-host>/webhooks/meta/{client_key}`

#### LinkedIn Lead Gen
- Event callback:
  - `POST https://<your-public-host>/webhooks/linkedin/{client_key}`

### 8) End-to-end test with current endpoints

#### A) Simulate lead intake (Meta)

```bash
curl -X POST http://localhost:8000/webhooks/meta/<client_key> \
  -H "Content-Type: application/json" \
  -d '{
    "entry": [{
      "changes": [{
        "value": {
          "leadgen_id": "meta-lead-001",
          "field_data": [
            {"name": "full_name", "values": ["Jane Prospect"]},
            {"name": "phone_number", "values": ["+1 (555) 123-4567"]},
            {"name": "email", "values": ["jane@example.com"]},
            {"name": "city", "values": ["Austin"]}
          ]
        }
      }]
    }]
  }'
```

Expected:
- lead created/updated
- worker sends initial SMS
- events appear in UI (`Dashboard`, `Conversations`, `Logs`)

#### B) Simulate inbound SMS

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
- Left sidebar with icon-first navigation: `Dashboard`, `Clients`, `Conversations`, `Logs`, `Settings`, `Test Lab`
- Top bar with global search, current client selector, environment/runtime badges, refresh, and dark/light theme toggle
- Dense panels, small controls, monospace metadata, and split panes optimized for desktop operations
- If you were already on `/ui`, hard-refresh the page after updating because the app ships the UI as one inline HTML/JS template

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

### Logs
- Selected-client event summary cards
- Dense audit log table with local search from the top bar
- Fast jump from a log row back into the matching conversation

`[Screenshot placeholder: Logs view with event cards and audit table]`

### Settings
- Runtime provider settings in the left column (global fallback defaults)
- Selected-client webhook URLs and demo-data controls in the right column
- Demo seed/reset stays in Settings to avoid another low-frequency page
- Saved provider keys and tokens remain visible to the authenticated admin in this console

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

## Demo Seed Data

Automatic dev behavior:
- `docker compose up --build` runs migrations and then runs `python -m app.scripts.seed_demo`
- If demo data is already present, the seed step skips re-creating it

Manual commands:

```bash
python -m app.scripts.seed_demo
python -m app.scripts.seed_demo --reset
python -m app.scripts.seed_demo --reset-only
```

UI controls:
- Open `/ui`
- Go to `Settings`
- Use `Seed Demo Data`, `Reseed Demo Data`, or `Reset Demo Data`

Seeded portal logins:
- Demo clients are created with client-portal access enabled
- Use the credentials in `Client Portal` above to test the scoped view immediately

## Live Phone Testing

To test the conversation AI with your own phone:
- Configure Twilio runtime settings in `/ui`
- Set `Public Base URL` in `/ui` to your public tunnel or deployed host
- Create or select a client
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
- Runtime provider config save (Twilio/OpenAI/Meta/LinkedIn tokens/settings)
- Client-scoped provider config per business owner (`Clients > Edit > Provider credentials`)
- Runtime source visibility (`client overrides` vs `global fallback`) in client workspace cards and top badges
- Client create + dense client workspace editing
- Client portal credential management per client
- Three-pane conversation inbox with thread view and quick actions
- Lead summary block + normalized form answers in thread details
- Hard delete for conversations from the thread details pane
- Internal notes stored in `audit_logs`
- Copy/paste webhook URLs for selected client
- Logs workspace for selected-client event review
- Test Lab actions for intake/SMS/AI
- Demo seed + reset controls in dev
- Recent event summary and audit log preview per client

Security behavior (MVP):
- Runtime settings are visible again to an authenticated admin session in `/ui`
- Secret fields are re-populated after save for operator convenience
- Secrets are not logged by these admin routes
- Existing signature verification behavior remains in place

---

## Environment Variables (`.env.example`)

Current variables used by the app:
- `APP_NAME`
- `ENV`
- `LOG_LEVEL`
- `AUTO_CREATE_TABLES`
- `DATABASE_URL`
- `REDIS_URL`
- `TWILIO_ACCOUNT_SID`
- `TWILIO_AUTH_TOKEN`
- `TWILIO_FROM_NUMBER`
- `OPENAI_API_KEY`
- `OPENAI_MODEL`
- `AI_PROVIDER_MODE`
- `META_VERIFY_TOKEN`
- `LINKEDIN_VERIFY_TOKEN`
- `ADMIN_TOKEN`
- `ENABLE_DEMO_SEED`
- `RQ_EAGER`
- `RATE_LIMIT_COUNT`
- `RATE_LIMIT_WINDOW_MINUTES`
- `AFTER_HOURS_FOLLOWUP_MINUTES`

Note: UI runtime settings can override Twilio/OpenAI/Meta/LinkedIn values without editing `.env`.

---

## Troubleshooting (Most Common)

### 1) `403 Invalid Twilio signature`
- Confirm Twilio webhook URL matches exactly the public URL receiving requests.
- Ensure Twilio auth token in runtime settings matches Twilio console.
- For local manual curl tests, keep Twilio auth token blank or use signed requests.
- If you are using ngrok or another tunnel, set `Public Base URL` in `/ui` to that exact HTTPS origin.

### 1b) Twilio auto-replies with `Thanks for the message. Configure your number's SMS URL...`
- Twilio is not sending inbound SMS to your app yet.
- Set the phone number's incoming message webhook to the full `https://.../sms/inbound/{client_key}` URL from `Settings` or `Clients > Webhooks`.
- Do not use `localhost`; use your tunnel or deployed host.
- After updating the number webhook, send a fresh inbound SMS and watch the app logs for `POST /sms/inbound/{client_key}`.

### 2) Meta verify challenge fails (`403 Verification failed`)
- Verify query params are present:
  - `hub.mode=subscribe`
  - `hub.verify_token=<configured token>`
  - `hub.challenge=<value>`
- Ensure Meta verify token in runtime settings (or env) matches what Meta sends.

### 3) Webhook accepted but no SMS sent
- Check worker is running (included in `docker compose up --build`)
- Check Redis reachable from API/worker
- Check client has phone in normalized payload
- Check lead isn’t opted out and didn’t already receive initial SMS

### 4) ngrok/public URL changed
- Update URLs in Twilio/Meta/LinkedIn
- Re-test with UI Test Lab

### 5) DB migration / table errors
- Ensure DB is up
- Run:
  - `alembic upgrade head`
- Confirm `runtime_settings` table exists (migration `20260226_0002`)

### 6) AI responses not using OpenAI
- Check runtime status shows AI configured
- Ensure valid OpenAI key
- If not configured, app falls back to heuristic provider by design

---

## Tests

```bash
python -m pytest -q
```

Current tests cover:
- webhook intake -> lead creation -> initial outbound message
- inbound SMS -> agent decision -> outbound reply and state transition
