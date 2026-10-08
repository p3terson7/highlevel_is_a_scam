# Chatbot V4 Parallel Implementation Plan

## Status

- Planning branch: `codex/chatbot-v4-architecture`
- Branched from: `frontend-rebuild` at `a664307`
- Current production agent: Agent V3
- V4 production default: disabled
- Scope of this document: backend conversation architecture, evaluation, rollout, and operational handoff
- Phase 1 status: in progress. The first reviewable slice now includes the typed local runner, real V3 adapter, fake external boundaries, deterministic/model graders, reports, CLI, CI smoke gate, and 10 starter scenarios.
- Phase 1 remaining: expand to the planned 30 checkpoints and 10 journeys, run the opt-in live/provider capability spike, record token/latency/cost samples, and calibrate semantic judging against blinded human labels.

This plan deliberately does not modify or remove Agent V3. V4 will be built beside it and selected through a server-side router. Turning off the V4 master switch must return all traffic to V3 without a database downgrade or deployment rollback.

## Objective

Build a dialogue-centered support and booking agent that:

- follows the lead's current goal and language;
- answers support questions before selling;
- moves toward a meeting only when that is a useful next step;
- preserves conversational continuity across topic changes and corrections;
- keeps verified facts, permissions, booking mutations, consent, and external side effects under deterministic Python control;
- is measurable through repeatable transcript evaluations and production telemetry.

The implementation must preserve existing Twilio transport, consent, opt-out, idempotency, booking, calendar, CRM, webhook, audit, and agent-pause behavior.

## Non-goals

- Do not rewrite or delete Agent V3 during the V4 rollout.
- Do not rewrite the booking engine.
- Do not move business rules, permissions, booking logic, or tenant policy into React.
- Do not change the configured production model merely because V4 is introduced.
- Do not fine-tune a model before the evaluation corpus demonstrates a specific need.
- Do not promise arbitrary-language support until deterministic copy, retrieval, and evaluation cover those languages.
- Do not depend on OpenAI-hosted conversation persistence or the hosted Evals platform.
- Do not synchronously run a live shadow model on every production turn during the first rollout stages.

## Architectural invariants

1. **V3 stays runnable.** Existing imports, direct `LLMAgent(...)` construction in tests, and current callers remain compatible.
2. **The model proposes; Python authorizes.** Consent, opt-out, booking, handoff, provider-backed availability, state mutations, and all external writes remain authoritative in Python.
3. **One component owns the discourse plan.** V4 must not recreate overlapping regex, prompt, CTA, and state-machine authorities.
4. **The model owns natural wording.** Post-generation checks may reject and regenerate unsafe output, but may not perform broad sentence surgery.
5. **Dialogue state is separate from CRM funnel state.** `QUALIFYING` and `BOOKED` remain workflow states; they are not substitutes for the lead's active topic, question, objection, or language.
6. **A write boundary is final for the turn.** After V4 starts a consequential tool, the router may not invoke V3 for the same turn.
7. **No externally consequential side effects in shadow or evaluation mode.** Twilio, booking writes, Zapier, and handoff notifications use fakes or no-op adapters. Isolated fixture/Test Lab records may be written for state inspection.
8. **No silent version switching mid-conversation.** Live assignment is sticky per lead, while the global kill switch always wins.
9. **No chain-of-thought persistence.** Store compact structured decisions and evidence, never hidden reasoning or full prompts.
10. **Every phase is independently deployable and reversible.** Use additive files, additive migrations, feature flags, and small reviewable changes.

## Existing compatibility seam

The repository already has a narrow interface suitable for a parallel agent:

- `run_turn(client, lead, inbound_text, history, booking_service, db)` for normal inbound turns;
- `resolve_booking_selection(...)` for ambiguous replies to an active offer;
- `next_reply(...)` for initial outreach and direct/sandbox callers;
- `AgentResponse` as the response consumed by the existing inbound workflow.

V4 will implement those methods and continue returning the current `AgentResponse` compatibility shape. This allows SMS delivery, CRM state transitions, webhook delivery, booking persistence, and audit handling to remain outside the new agent.

During the compatibility window, V4 must continue emitting the legacy values consumed downstream:

- existing `ConversationStateEnum` values;
- `action`, `next_question_key`, and `collected_fields`;
- `pending_step` and active-offer metadata;
- `booking_offer`, `calendar_booking`, and `booking_confirmation_unknown`;
- provider-backed slot identifiers and timestamps;
- `provider` as `openai` or `fallback`.

Agent, model, prompt, policy, and state-schema versions belong in turn metadata rather than changing the meaning of `provider`.

For live V4 turns, the orchestration layer also dual-writes the legacy `Lead.raw_payload` and outbound-message projections still read by V3, including language, qualification memory, pending booking step, active offer, CTA state, handoff, and agent control. A flag-only `V4 -> V3` rollback must resume from those fields without a conversion job.

## Target turn flow

```mermaid
flowchart LR
    A["Authenticated inbound message"] --> B["Consent, opt-out, pause and idempotency"]
    B --> C["Agent router"]
    C --> D["Dialogue-state compiler"]
    D --> E["Turn planner"]
    E --> F["Deterministic policy authorization"]
    F --> G["Read or write tool adapter"]
    G --> H["Response renderer"]
    H --> I["Narrow factual and safety validation"]
    I --> J["Reserve outbound idempotently"]
    J --> K["Twilio send"]
    K --> L["Persist successful turn, state and trace"]
    L --> M["Existing downstream workflows"]
```

The planner chooses one primary objective per turn:

- answer support;
- clarify one point;
- nurture;
- offer scheduling;
- operate booking or rescheduling;
- confirm an outcome;
- hand off.

Support and booking are parallel concerns. A scheduling conversation can pause to answer a support question and then resume from explicit state rather than re-inferring the prior topic from keywords.

## Proposed code layout

```text
app/services/
  agent_contract.py
  agent_router.py
  agent_analytics.py
  agent_observability.py
  agent_v4/
    __init__.py
    agent.py
    context.py
    policy.py
    provider.py
    renderer.py
    state.py
    tools.py
    types.py

app/api/ui/
  eval_routes.py

evals/chatbot/
  README.md
  schema.py
  runner.py
  report.py
  adapters/
    v3.py
    v4.py
  graders/
    deterministic.py
    human_labels.py
    model_judge.py
  fixtures/
    smoke/
    regression/
    journeys/
  rubrics/
    booking_safety.json
    dialogue_quality.json
    support_quality.json

scripts/
  backfill_agent_assignments.py
  run_chatbot_evals.py
  purge_agent_traces.py

app/tests/chatbot_v4/
  test_agent_router.py
  test_dialogue_state.py
  test_eval_graders.py
  test_eval_schema.py
  test_eval_smoke.py
  test_tool_authorization.py
  test_v3_v4_continuity.py
```

Agent V3 files stay in place. Common booking code is reused through adapters rather than moved during the first V4 phases.

## Core contracts

### `ConversationAgent`

Introduce a protocol matching the three existing entry points. `llm_agent.py` remains the public compatibility module. It should:

- keep `LLMAgent = LLMAgentV3` for existing direct imports and tests;
- export the new routed factory for application dependency injection;
- avoid forcing current V3 tests to instantiate the router.

### `DialogueStateV1`

The state is a versioned Pydantic model persisted as JSON. At minimum it contains:

- `schema_version` and optimistic `revision`;
- selected agent/policy version;
- lead language, locale, confidence, explicit preference, and recent switch evidence;
- active lead goal and active topic;
- unresolved support question;
- open loops and expected next conversational move;
- known facts with source, confidence, and correction status;
- objections, preferences, and refusals;
- sentiment and repair state;
- assistant commitments and facts already explained;
- CTA stance: not offered, offered, accepted, ignored, or refused;
- booking dialogue state and active offer reference;
- handoff state reference;
- compact rolling summary;
- last observed inbound and outbound message IDs.

Authoritative consent, opt-out, calendar bookings, provider availability, permissions, CRM stage, and external-delivery state must not be copied into this model as truth. The compiler reads them from existing models each turn.

### `TurnPlan`

The planner returns a strict typed object, not customer-facing prose mixed with mutable workflow state. It contains:

- detected language and confidence;
- primary intent and optional secondary intent;
- current support need;
- response moves such as acknowledge, answer, clarify, transition, or confirm;
- grounded facts/source identifiers to use;
- proposed state changes;
- at most one useful question;
- optional typed tool proposal;
- handoff reason when applicable;
- renderer guidance such as tone and brevity, not a complete reply.

The application rejects an invalid plan. It does not repair malformed JSON by asking the model to reinterpret its own invalid output repeatedly.

The first V4 implementation explicitly chooses this contract:

`strict TurnPlan.tool_proposal -> Python authorization/execution -> response renderer`

It does not also run a native function-tool loop in the same turn. Combining both contracts would make tool ownership ambiguous and could require an unbounded third model call. Native function calling may be evaluated later behind the same provider-independent `TurnPlan` and tool-authorization boundary.

### `ToolAuthorizationResult`

Every tool proposal becomes one of:

- `authorized`;
- `rejected` with a safe reason;
- `clarification_required`;
- `already_completed` with an idempotent result.

The result includes whether a consequential side effect has started. The router uses this to prevent unsafe cross-engine fallback.

### `RenderedReply`

The renderer receives the plan, real role-based recent messages, compact dialogue state, relevant grounded facts, and the actual tool result. It produces lead-facing text only.

Validation is narrow:

- the reply uses only authorized facts and visible provider-backed slots;
- it does not claim a booking or handoff that did not occur;
- it matches the selected supported language;
- it respects consent and fixed compliance copy;
- it does not expose secrets or internal policy;
- it stays within the configured channel limit.

On validation failure, perform one bounded regeneration. If that fails, use a deterministic result-specific fallback. Do not remove arbitrary clauses or translate phrases with regexes.

## OpenAI provider design

V4 gets a separate provider adapter so Agent V3's Chat Completions behavior remains unchanged.

The implementation spike must verify the configured model and pinned OpenAI SDK support the selected Responses API features before wiring runtime traffic. The intended design is:

- app-owned, role-based conversation inputs;
- `store=False` on model calls;
- strict Structured Outputs for `TurnPlan`;
- server-side Pydantic/JSON-schema tool proposals and results;
- captured request ID, model identifier, usage, latency, retries, and error class;
- no more than two normal model calls per turn: plan, then render;
- no unbounded repair loop;
- the existing configured model remains the default until evaluations justify changing it.

OpenAI's Conversations API persists conversation items as a durable object, while response storage has separate retention behavior. For this CRM, the initial V4 implementation will keep the canonical dialogue state and transcript references in the application database and continue using `store=False`: [OpenAI conversation-state documentation](https://developers.openai.com/api/docs/guides/conversation-state#openai-apis-for-conversation-state).

If a later measured iteration adopts native function calling, its definitions must use strict JSON schemas rather than embedding an informal schema in prompt text: [OpenAI function-calling documentation](https://developers.openai.com/api/docs/guides/function-calling#defining-functions).

The V4 agent does not commit arbitrary database state. It returns a proposed state transition and structured trace to the existing orchestration layer. Normal replies preserve the current ordering: reserve the outbound operation, send through Twilio, then persist the successful outbound turn. Consequential tool outcomes remain authoritative even if customer acknowledgement delivery fails, so dialogue state can be reconstructed from existing booking/handoff truth without retrying the write.

## Persistence design

Use two small linear Alembic revisions after the current head. Rebase the revisions if another branch advances the migration head before implementation.

### Revision A: dialogue foundation

#### `clients.agent_config`

Add a generic JSON column with server default `{}`. Keep agent behavior separate from `provider_config`, which is channel and credential oriented.

Validated application shape:

```json
{
  "mode": "inherit",
  "policy_version": "v1",
  "supported_languages": ["en", "fr"],
  "handoff_sla_minutes": 30
}
```

Allowed modes are `inherit`, `v3`, `v4_shadow`, and `v4_live`. Global deployment configuration caps tenant configuration, so a tenant cannot enable V4 when the global switch is off.

#### `lead_agent_assignments`

- `lead_id` as the primary key with `ON DELETE CASCADE`;
- `client_id` with `ON DELETE CASCADE`;
- assigned engine: `v3` or `v4`;
- assignment source: default, client, canary, or explicit operator action;
- cohort identifier and assignment timestamp;
- created/updated timestamps.

Assignment is separate from dialogue state so existing V3 conversations can be pinned before V4 has created any state. New conversations receive an assignment once and retain it. Before increasing the first live canary percentage, run a bounded activation job that pins existing active conversations to V3; a higher percentage must never silently migrate an in-progress V3 conversation to V4.

#### `lead_dialogue_states`

- `id`
- `client_id` with `ON DELETE CASCADE`
- `lead_id` with `ON DELETE CASCADE`
- `track`: `live` or `shadow`
- `schema_version`
- `revision`
- `state_json`
- `last_observed_message_id`
- created/updated timestamps
- unique `(lead_id, track)`

Use string status fields rather than new PostgreSQL enums, and generic SQLAlchemy JSON for PostgreSQL/SQLite test compatibility.

Shadow state never reads from or advances live state. Existing leads are initialized lazily from authoritative models and recent messages; the migration performs no LLM calls and no bulk transformation of `Lead.raw_payload`.

#### `dialogue_turns`

- stable `turn_key`, normally `inbound-message:<id>`;
- `client_id`, `lead_id`, inbound/outbound message references;
- engine version and execution mode;
- input/output dialogue-state revisions;
- prompt, policy, and state-schema versions;
- exact provider/model identifier and request ID;
- structured plan and tool proposal/result metadata;
- authorization and validator flags;
- call count, retry count, latency, and token usage;
- status and bounded error code;
- optional redacted shadow candidate with an expiry timestamp;
- unique `(lead_id, turn_key, execution_mode)`.

Engine version remains metadata, not part of live-turn idempotency. A deployment or model-version change must not permit a second live response for the same inbound message. Offline evaluation reruns use a separate evaluation-run identifier rather than weakening production uniqueness.

Do not duplicate live delivered transcript text; reference existing `Message` rows. Never store full prompts, secrets, hidden reasoning, or full knowledge documents. Shadow candidate text is optional, redacted, access-controlled, and short-lived.

Use revision compare-and-swap when advancing dialogue state. The existing per-lead worker lock remains the primary serialization mechanism, but a stale model result must not overwrite a newer state revision.

#### `dialogue_turn_feedback`

- `id`, `client_id`, and `dialogue_turn_id`;
- reviewer role/identifier;
- one allowlisted label such as good, awkward, incorrect fact, repetitive, pushy, wrong language, or incorrect booking;
- optional bounded comment;
- created/updated timestamps;
- tenant-scoped indexes and deletion cascade from the owning turn/client.

Feedback is first-class persistence rather than another unstructured audit entry. Access and mutation require the same tenant isolation as conversation data.

### Revision B: operational handoff

#### `handoff_cases`

- `client_id` and `lead_id`;
- trigger message and dialogue-turn references;
- unique idempotency key;
- status: `open`, `acknowledged`, `resolved`, or `cancelled`;
- severity, reason, and bounded summary JSON;
- assignee, `due_at`, acknowledgement/resolution actor and timestamps;
- optional linked `LeadTask`;
- one active case per lead;
- indexes on `(client_id, status, due_at)` and `(lead_id, status)`.

Initially dual-write the current `HANDOFF` state, `raw_payload["handoff"]`, agent-control data, and audits while creating the new case. In the same transaction, create an idempotent visible `LeadTask`. Optional notifications use the existing `OutboundRequest` outbox with `request_kind="handoff_notification"`; notification failure must not hide or roll back the case.

Lead/client ownership foreign keys use `ON DELETE CASCADE`. Optional references to messages, dialogue turns, and tasks use `ON DELETE SET NULL`, so retention cleanup or task deletion cannot remove an active case. Trace purging must explicitly refuse to cascade into handoff cases.

Handoff ordering intentionally differs from an ordinary conversational reply: commit the idempotent case, pause/state, task, and audit first; only then reserve and send the customer acknowledgement and operator notification. Add explicit recovery dispatch for `handoff_ack_sms` and `handoff_notification`, because the current outbound recovery registry does not support those request kinds. Delivery failure leaves the case visible and actionable.

Operator acknowledgement, resolution, and agent resume update the case/task while retaining existing state-transition behavior.

## Routing and rollback controls

Add deployment settings with safe defaults:

```text
AGENT_V4_ENABLED=false
AGENT_DEFAULT_VERSION=v3
AGENT_V4_CANARY_PERCENT=0
```

Selection precedence:

1. Global master switch off: V3.
2. Explicit Test Lab selection.
3. Explicit per-client `v3`: forced tenant rollback, including existing sticky V4 leads.
4. Existing sticky lead assignment.
5. Explicit per-client `v4_live` or `v4_shadow` for new/unassigned conversations.
6. Deterministic lead-ID canary cohort for new/unassigned conversations.
7. Global default: V3.

Agent selection happens after the `Client` is known, not inside the current globally cached provider builder.

All new settings must be added consistently to `Settings`, `.env.example`, Docker Compose's explicit environment block, and README deployment documentation. Because API and RQ workers cache configuration/provider objects, a flag change is not considered active until the documented worker drain/restart completes.

Production shadow execution is asynchronous. After V3 completes, enqueue an immutable snapshot containing message references, bounded state, tenant policy version, and fake/read-only tool world. A dedicated V4 shadow entry point consumes that snapshot, writes only the `shadow` dialogue track and shadow turn trace, and receives no-op write tools. It must never call `process_inbound_turn`, because that orchestration can send SMS, mutate CRM, book, or trigger downstream webhooks.

Failure rules:

- V4 failure before any tool or mutation: the router may invoke V3 for that turn.
- V4 failure after a read-only tool: the router may use V3 only if no write or externally visible state was created.
- V4 failure once a write tool starts: never invoke V3; use the authoritative tool result or deterministic safe response.
- Renderer failure after a successful booking: use the existing backend booking confirmation/proposal copy.
- Once loaded by restarted API/RQ workers, the V4 global kill switch overrides every sticky assignment. V4 state remains stored but ignored.

Operational rollback is a configuration change to V3, not an Alembic downgrade. Keep additive tables during rollback. Drain and restart long-lived RQ workers before enabling or disabling live V4 so mixed-version workers cannot make inconsistent decisions.

## Tenant playbook

V4 initially compiles a typed playbook from existing client fields:

- business name and assistant identity;
- `tone`;
- `qualification_questions`;
- `ai_context` and `faq_context` with distinct trust semantics;
- booking mode and actual available booking capabilities;
- operating hours and timezone;
- supported languages;
- pricing and escalation boundaries.

The playbook must separate:

- trusted server-side policy;
- tenant-authored business guidance;
- retrieved factual knowledge;
- untrusted lead and website content.

The configurable qualification questions become optional conversational goals, not a mandatory interrogation sequence. The planner asks one only when it advances the lead's current goal.

## Language and retrieval strategy

The first compatibility release remains fully tested for English and French, but replaces sticky-language keywords with explicit language state:

- BCP-47 language/locale;
- detection confidence;
- explicit lead preference;
- recent-turn evidence;
- strong-switch versus ambiguous-short-reply handling;
- tone/register preference.

Strong evidence of a language switch updates the state. Ambiguous replies retain the established language. Deterministic time, compliance, and fallback copy must have tested locale support before a language is marked fully supported.

Introduce a `KnowledgeRetriever` protocol around the current lexical implementation. Benchmark multilingual query expansion and semantic retrieval against the eval corpus before adding an embedding/vector dependency. Retrieval changes must preserve source identifiers and tenant isolation.

## Evaluation system

Build a repository-local runner. OpenAI currently recommends task-specific, production-like evals, continuous evaluation, automated scoring, logging, and human calibration: [OpenAI evaluation best practices](https://developers.openai.com/api/docs/guides/evaluation-best-practices).

Two fixture types are required:

1. **Checkpoint:** frozen role-based history, dialogue state, one inbound lead message, fake tool world, and expected semantic behavior.
2. **Journey:** multiple scripted lead turns and deterministic tool results used to validate state evolution and side effects.

Do not snapshot exact model wording except legally fixed compliance text. Assert semantic behavior such as:

- required answer facts;
- forbidden unsupported claims;
- allowed turn objectives;
- tool name, arguments, and maximum call count;
- language and switching behavior;
- maximum questions and CTA count;
- expected state transitions;
- booking and handoff idempotency.

Runner interface:

```bash
python scripts/run_chatbot_evals.py \
  --agent v3|v4|both \
  --suite smoke|regression|journeys \
  --provider replay|live \
  --samples 3 \
  --judge off|model \
  --output artifacts/chatbot-evals/
```

CI smoke tests use replay/fake providers, have no secrets or network, and fail nonzero on hard-gate violations. Live-model evaluations run manually or on a budgeted schedule and pin the production and judge model identifiers.

Add `artifacts/chatbot-evals/` to `.gitignore`. CI uploads only deidentified reports with an explicit short artifact-retention period; raw live prompts/responses are not retained as CI artifacts.

Deterministic graders cover:

- unauthorized side-effect prevention;
- correct tool selection, arguments, and call count;
- valid offered and selected slots;
- opt-out and consent behavior;
- no invented availability, pricing, or business facts;
- no repeated qualification question or CTA;
- booking, handoff, and delivery idempotency;
- tenant isolation.

Semantic graders cover:

- support helpfulness and factual relevance;
- context continuity and memory;
- language/register adaptation;
- naturalness versus scripted interrogation;
- answer-first behavior;
- appropriate booking timing and pressure;
- correction and misunderstanding recovery;
- handoff quality.

Automated semantic graders must be calibrated against blinded human labels before they become release-blocking. Pairwise V3/V4 grading runs with candidate order reversed to reduce position bias.

## Quality gates

Hard safety gates are release-blocking from the start:

- zero unauthorized writes;
- zero wrong-slot or duplicate bookings;
- zero opt-out reply violations;
- zero secret exposure;
- 100% shadow side-effect isolation;
- 100% provider-backed booking claims.

Initial quality targets before live canary:

- tool choice and argument accuracy at least 99%;
- supported-language match at least 98%;
- answer-first and non-repetition at least 95%;
- each semantic category at least 90%, rather than one blended average;
- automated/human grader agreement at least 85%;
- no material regression in booking completion or handoff correctness;
- V4 pairwise losses on naturalness/support below 15%;
- latency and token/call cost measured and inside an explicitly approved budget.

Do not optimize booking conversion by itself. Review it alongside support quality, opt-outs, frustration, repeated CTAs, and human escalation.

## Observability and privacy

Every V4 turn receives a durable `turn_id` propagated through worker logs, dialogue state, tools, outbound messages, and audit records.

Capture:

- agent version and rollout cohort;
- policy/prompt/state-schema version and prompt hash;
- exact provider/model and provider request ID;
- model, tool, and total latency;
- call count, retries, structured-output failures, and fallback reason;
- input/output/cached/reasoning token counts when returned;
- retrieved source IDs/count, not full source text;
- selected language and dialogue objective;
- proposed versus authorized tool and validation outcome;
- guardrail/validator flags;
- links to inbound/outbound messages and booking/handoff outcomes.

All trace content passes through an allowlist serializer. It admits only named bounded fields and safe error codes; it rejects arbitrary provider errors, tool arguments/results, tenant configuration, and nested payloads by default. Redaction is defense in depth, not the primary schema boundary.

Keep Prometheus labels low-cardinality. Lead IDs, turn IDs, request IDs, and prompt hashes belong in traces/logs, never metric labels.

The current process-local `/metrics` counters are not sufficient for rollout decisions. Before live canary, add durable SQL aggregation/query services over dialogue turns, messages, bookings, handoffs, and feedback, plus a version/cohort/language dashboard with an assigned operational owner. Alert rules and rollback checks must read this durable source or a real telemetry backend, not per-process counters.

Every rate defines its eligible denominator. Safety events roll back immediately at any sample size. Percentage-based promotion gates require at least 200 eligible turns per engine and at least 30 in each reported supported-language segment, then continue until the pre-registered power calculation is met. Compare proportions with 95% confidence intervals and declare non-inferiority margins before looking at candidate results; do not promote from raw percentages alone.

Privacy requirements:

- committed fixtures are synthetic and manually reviewed;
- production transcripts are never automatically exported into Git;
- production traces link to message rows rather than duplicating full text;
- no chain-of-thought, full prompt, secret, or full tenant configuration is stored;
- shadow candidate text, if enabled, is redacted and expires quickly;
- trace APIs are tenant-scoped and tested;
- lead/client deletion cascades into V4 state and traces;
- a retention policy and purge job ship before production shadow capture.

Existing V3 `agent_decision` audit records currently duplicate full inbound/outbound text. V4 audits will store message references plus allowlisted metadata rather than duplicate text. Before live canary, adopt and implement an explicit retention policy for both legacy audit text and new traces; this is not deferred merely because V3 remains available.

## Phased implementation

### Phase 0: isolation and baseline — completed for planning

Deliverables:

- create the parallel branch;
- record the starting commit and current test state;
- add this plan;
- leave runtime code unchanged.

Exit gate:

- branch starts from the current clean implementation;
- only planning documentation changes.

### Phase 1: evaluation foundation and provider capability spike

Deliverables:

- add eval schemas, replay adapters, deterministic graders, reports, and CLI;
- curate at least 30 synthetic checkpoints and 10 multi-turn journeys;
- cover English, French, code-switching, terse replies, support-first behavior, objections, correction, booking, rescheduling, refusal, frustration, and handoff;
- record a V3 baseline without changing V3;
- verify Responses API, strict `TurnPlan` structured output, `store=False`, and usage/request metadata with the pinned SDK and configured model;
- validate that server-side tool-proposal schemas round-trip without introducing a native function-call loop;
- document latency and per-turn call/token baseline.

Exit gate:

- offline smoke suite runs in under approximately one minute without network or secrets;
- every fixture has an owner, risk level, explicit expectations, and no real PII;
- hard safety checks are reproducible;
- live evaluation is opt-in and budget-limited.

### Phase 2: contract, router, additive state, and telemetry

Deliverables:

- add `ConversationAgent`, router, and observability abstractions;
- add Revision A and model relationships;
- add global flags and validated `Client.agent_config`;
- implement durable deterministic sticky assignment, forced tenant rollback, and kill-switch precedence;
- add the activation/backfill command that pins existing active conversations to V3 before canary percentages can increase;
- keep every production and Test Lab request routed to V3;
- preserve `LLMAgent = LLMAgentV3` direct construction;
- add stale-revision and duplicate-turn protection.

Exit gate:

- default configuration executes V3 exactly as before;
- migrations upgrade from empty PostgreSQL, downgrade to the previous head, re-upgrade, and pass `alembic check`;
- deletion cascades and tenant scoping are tested;
- no V4 model request or side effect is possible.

### Phase 3: V4 support-only path in Test Lab

Deliverables:

- implement dialogue state, context compiler, tenant playbook, planner, renderer, and narrow validators;
- pass recent history as actual lead/assistant roles plus compact state;
- expose explicit V3/V4 selection only in Test Lab;
- allow knowledge lookup but no write tools;
- persist separate Test Lab dialogue state and traces;
- show plan, language, sources, validator results, latency, and token usage in Test Lab.

Exit gate:

- V4 itself cannot propose or initiate booking writes, Twilio, Zapier, handoff notification, or CRM writes; existing Test Lab/inbound lifecycle bookkeeping may continue around it;
- support, continuity, language, and naturalness eval thresholds pass;
- renderer failure produces safe deterministic text without regex rewriting;
- current V3 Test Lab behavior remains available.

### Phase 4: deterministic booking and rescheduling tools

Deliverables:

- advertise only tools supported by the client's actual booking configuration;
- connect strict V4 tool adapters to the existing `BookingService`;
- preserve active offer, pending step, calendar booking, and booking webhook payload contracts;
- keep pre/post handoff policy during compatibility rollout;
- implement safe failure behavior before and after write boundaries;
- cover link-only, internal calendar, Calendly, no-provider, and ambiguous-provider modes.

Exit gate:

- existing booking and SMS reliability suites pass unchanged;
- V4 passes find-slots, selected-slot, exact-time, ambiguity, rescheduling, provider rejection, unknown provider outcome, and duplicate-delivery tests;
- `V4 -> V3` rollback works during an active slot offer and after a confirmed booking;
- zero hard safety failures in evals.

### Phase 5: tenant-aware qualification, language switching, and retrieval

Deliverables:

- use configured qualification questions and actual tenant goals;
- remove the universal decision-maker/urgency threshold from V4;
- implement explicit language confidence and switch state;
- validate English and French deterministic copy end to end;
- add retrieval abstraction and benchmark query expansion/semantic options;
- add operator feedback labels: good, awkward, incorrect fact, repetitive, pushy, wrong language, or incorrect booking.

Exit gate:

- tenant-specific playbooks pass isolated tests;
- supported-language and code-switch gates pass;
- retrieval factuality improves without tenant leakage;
- no new major retrieval dependency is added without benchmark evidence and an operations plan.

### Phase 6: unified outbound policy and operational handoff

Deliverables:

- route initial outreach and scheduled follow-ups through the same dialogue policy;
- cancel/suppress follow-ups after replies, booking, refusal, opt-out, handoff, or topic changes as applicable;
- add Revision B and the handoff case lifecycle;
- persist the case/pause/task/audit before attempting acknowledgement delivery;
- create idempotent CRM tasks and optional outbox-backed acknowledgements/notifications with registered recovery workers;
- add acknowledgement, resolution, SLA, and explicit resume semantics.

Exit gate:

- duplicate inbound processing creates one turn, reply, handoff case, and task;
- notification failure leaves the handoff visible and actionable;
- resume resolves the case/task without losing conversation state;
- static automation cannot send a contradictory booking CTA.

### Phase 7: shadow, canary, and production rollout

Rollout stages:

1. Offline V3/V4 replay.
2. Explicit Test Lab V4.
3. Internal or authorized asynchronous shadow mode with no write tools.
4. One low-volume opted-in tenant.
5. Sticky 5% cohort.
6. Sticky 25% cohort.
7. Sticky 50% cohort.
8. Opt-in/default V4 only after evidence supports it.

At each live stage, define a minimum sample size and observation window. Retain V3 and the kill switch for at least one complete production release cycle after V4 becomes the default. Remove nothing until rollback, booking, handoff, and privacy-retention requirements are proven.

Canary promotion uses the durable version/cohort/language analytics service, declared denominators, minimum sample rules, confidence intervals, and named dashboard/alert ownership defined above.

Immediate rollback triggers:

- any unauthorized write, wrong-slot booking, duplicate booking, opt-out violation, or secret exposure;
- material provider-confirmed booking regression;
- V4 fallback rate approximately doubling versus baseline;
- p95 total turn latency increasing more than 30% without explicit approval;
- statistically credible increases in opt-outs, frustration, repeated CTA behavior, or failed human escalation;
- mixed-version worker behavior or stale dialogue-state writes.

## Suggested reviewable change sequence

Each item should be its own reviewable pull request or commit series:

1. Eval contracts, fixtures, replay runner, and V3 baseline.
2. Agent protocol, router, safe configuration, and default-V3 tests.
3. Dialogue/turn migration and repository with no runtime V4 calls.
4. V4 support-only planner/renderer in Test Lab.
5. Booking tool adapters and compatibility tests.
6. Tenant playbook, language state, and retrieval adapter.
7. Unified initial/follow-up policy.
8. Handoff cases, tasks, notifications, and SLA UI.
9. Shadow telemetry and controlled canary.

Do not combine the state migration, model behavior, booking tools, handoff workflow, and production activation in one change.

## Validation commands

Run the relevant subset after every change and the full set before a rollout stage:

```bash
python -m ruff check --select E9,F63,F7,F82 app scripts evals
python -m pytest -q
python scripts/check_migration_heads.py
python -m alembic upgrade head
python -m alembic check

cd frontend
npm run typecheck
npm run test
npm run build
```

Additional V4 gates:

```bash
python scripts/run_chatbot_evals.py --agent both --suite smoke --provider replay --judge off
python scripts/run_chatbot_evals.py --agent both --suite regression --provider live --samples 3 --judge model
```

## Baseline recorded on this branch

Local environment:

- Python `3.10.9`
- FastAPI `0.116.1`
- Starlette `0.47.3`
- pytest `8.4.1`

Focused chatbot baseline:

```text
71 passed in 2.77s
```

Command:

```bash
python -m pytest -q \
  app/tests/test_llm_agent.py \
  app/tests/test_agent_hardening.py \
  app/tests/test_i18n_support.py \
  app/tests/test_sms_flow.py
```

Full local baseline:

```text
286 passed, 2 failed in 18.41s
```

The two pre-existing/local-environment failures are:

1. `test_inbound_media_download_uses_remaining_aggregate_time`: Python 3.10 exposes `asyncio.TimeoutError` differently from the CI-targeted Python 3.11 behavior expected by the test.
2. `test_webhook_rejects_empty_oversized_and_excessive_batch_payloads`: the local Starlette `0.47.3` lacks `HTTP_422_UNPROCESSABLE_CONTENT`; the repository pins a newer dependency set than the active local environment.

Before implementation begins, create/use the repository's Python 3.11 dependency environment and rerun the full baseline. These failures must be kept separate from V4 changes rather than silently accepted or fixed inside an unrelated V4 patch.

## Definition of done

V4 is ready to replace V3 as the default only when:

- every hard safety gate has zero failures;
- conversational quality beats or is non-inferior to V3 across the approved corpus and human review;
- language switching and tenant-specific policy meet their gates;
- booking, rescheduling, webhook, delivery, and handoff behavior remain compatible;
- production traces attribute every turn to an agent, model, prompt, policy, and state version;
- operational handoffs are acknowledged and measurable;
- latency/cost and retention policies are approved;
- canary metrics remain healthy through the agreed observation window;
- a tested flag-only rollback to V3 works without data loss or duplicate side effects.
