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

### 1) Prepare env

```bash
cp .env.example .env
```

Minimum to set in `.env`:
- `DATABASE_URL`
- `REDIS_URL`
- `ADMIN_TOKEN`

You can leave Twilio/OpenAI/Meta/LinkedIn vars blank and set them from the UI later.

### 2) Start stack

```bash
docker compose up -d postgres redis
```

### 3) Run DB migrations

```bash
alembic upgrade head
```

### 4) Start API

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

### 5) Start worker

```bash
rq worker --with-scheduler
```

### 6) Open admin UI

- `http://localhost:8000/ui`
- Enter `ADMIN_TOKEN` from `.env`
- Load Console

### 7) Configure providers in UI (Runtime Provider Settings)

Set and save:
- Twilio: `Account SID`, `Auth Token`, `From Number`
- OpenAI: `API Key`, `Model`
- Meta verify token
- LinkedIn verify token

These values are saved server-side (table: `runtime_settings`) and are not returned to browser after save.

### 8) Create and configure a client in UI

In **Create / Edit Client**:
- Create client (or select existing)
- Set:
  - business name
  - tone
  - timezone
  - qualification questions
  - booking URL
  - operating hours
  - handoff number
  - template overrides (JSON)

### 9) Copy webhook URLs from UI

For selected client key `{client_key}`:
- Meta verify URL: `https://<your-public-host>/webhooks/meta/{client_key}`
- Meta events URL: `https://<your-public-host>/webhooks/meta/{client_key}`
- LinkedIn URL: `https://<your-public-host>/webhooks/linkedin/{client_key}`
- Twilio inbound URL: `https://<your-public-host>/sms/inbound/{client_key}`

For local testing with real providers, expose API using ngrok/cloud tunnel.

### 10) Configure external providers

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

### 11) End-to-end test with current endpoints

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
- events appear in UI (`Event Summary`, `Recent Audit Logs`)

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

### 12) Real lead flow checklist

When fully configured with public URLs and provider credentials:
- Real form submit -> webhook route receives event -> worker normalizes lead -> initial SMS sent
- Real SMS reply -> Twilio inbound route -> AI response -> booking link or handoff sent

---

## UI Admin/Settings Coverage

`/ui` now supports:
- Runtime provider config save (Twilio/OpenAI/Meta/LinkedIn tokens/settings)
- Client create + edit (multi-tenant settings)
- Copy/paste webhook URLs for selected client
- Test actions:
  - simulate lead intake
  - simulate inbound SMS
  - send Twilio test SMS
  - run AI test response
- Recent event summary + audit log feed per client

Security behavior (MVP):
- Secrets are never returned by status endpoints
- Secret fields are not re-populated after save
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
- `META_VERIFY_TOKEN`
- `LINKEDIN_VERIFY_TOKEN`
- `ADMIN_TOKEN`
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

### 2) Meta verify challenge fails (`403 Verification failed`)
- Verify query params are present:
  - `hub.mode=subscribe`
  - `hub.verify_token=<configured token>`
  - `hub.challenge=<value>`
- Ensure Meta verify token in runtime settings (or env) matches what Meta sends.

### 3) Webhook accepted but no SMS sent
- Check worker is running: `rq worker --with-scheduler`
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
