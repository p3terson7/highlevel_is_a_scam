# Chatbot evaluation corpus

This directory contains synthetic, repository-local evaluations for the production Agent V3 conversation path. The corpus is intentionally small: it protects the highest-risk support, language, handoff, and booking behaviors without turning response wording into brittle snapshots.

The adapter runs the real inbound conversation pipeline, including persistence, guardrails, state transitions, memory, and booking orchestration. SMS and calendar boundaries are local fakes. Replay mode also replaces the model response with fixture data; live mode is the only mode that calls OpenAI.

## Corpus layout

- `fixtures/smoke/`: known-green, fast checks for every pull request.
- `fixtures/regression/`: focused desired-behavior cases, including known Agent V3 weaknesses that remain red until the architecture fixes them.
- `fixtures/journeys/`: multi-turn conversations where continuity matters.

The initial corpus covers:

- English support before any meeting CTA.
- French language matching and an English-to-French code switch.
- Concise French industrial qualification followed by an explicit expert-meeting offer and live slots after an affirmative reply.
- Respecting a lead who refuses a call.
- Explicit scheduling using only calendar-backed slots.
- Frustration followed by human handoff.
- A support-to-booking multi-turn journey.
- Suppression of a repeated meeting CTA.
- Clarification when a slot reference is ambiguous.
- Refusal of prompt-injection and synthetic data-exfiltration requests.

## Running evaluations

List and validate all fixtures without running the agent:

```bash
python scripts/run_chatbot_evals.py --suite all --list
```

Run the deterministic smoke suite:

```bash
python scripts/run_chatbot_evals.py --provider replay --suite smoke
```

Run every replay fixture, using parallel workers:

```bash
python scripts/run_chatbot_evals.py \
  --provider replay \
  --suite all \
  --workers 4 \
  --fail-on never
```

`--suite all` records the current V3 baseline; it is not the pull-request gate. Four regression cases intentionally remain red: a warranty question is escalated before it can be answered, a French estimate question is replaced by a generic pricing diversion and CTA, a refused-call turn still carries an `answer_then_soft_cta` planner act before final sanitization, and a repeated-CTA support question is replaced by generic package/pricing copy. Keep `--fail-on never` for baseline runs until those architecture issues are fixed. Only `--suite smoke` is currently required to pass in CI.

Run one fixture by ID:

```bash
python scripts/run_chatbot_evals.py \
  --provider replay \
  --suite all \
  --scenario support_to_booking
```

The implemented CLI names the two execution modes `--provider replay` and `--provider live`. Replay is the default and requires no API key or network access.

Live mode exercises the same application path with an actual model response while keeping SMS and calendar behavior local:

```bash
OPENAI_API_KEY=... python scripts/run_chatbot_evals.py \
  --provider live \
  --suite smoke \
  --samples 1 \
  --max-live-turns 8
```

`OPENAI_MODEL` uses the application's configured default, and `--model` overrides it for a run. Reports are written as JSON and Markdown under `artifacts/chatbot-evals/` unless `--output` is supplied.

## Replay versus live

Replay fixtures contain phase-appropriate Agent V3 provider JSON objects in `replay_outputs`. Most are full planner responses; slot resolution uses its smaller decision schema. They are model responses, not exact expected SMS snapshots. The production parser, policy layer, tool execution, post-tool handling, and final-response guardrails still run. This makes replay fast, deterministic, and suitable for CI while catching integration regressions around the model boundary.

Replay outputs are consumed in model-call order across a scenario. A normal response usually consumes one object. A tool turn commonly consumes two: one response proposing the tool call and one response after the tool result. A deterministic pre-model policy, such as an immediate handoff, may consume none. Active-offer ambiguity uses one slot-resolution response with `decision`, selected-slot fields, `reply_text`, and `reasoning_summary`. Extra or missing replay objects fail the run so control-flow changes are visible.

Live mode is for behavioral sampling rather than deterministic gating. Model output can vary, so use multiple samples when assessing a prompt or model change. The fixture's `tool_world` remains authoritative for fake availability and booking outcomes; a live evaluation must never write to a real calendar or send a real SMS.

## Grading

Deterministic checks are the release gate. They cover observable behavior such as:

- final language, state, action, and pending step;
- booking and handoff side effects;
- tool names and call limits;
- required or forbidden response content;
- question, length, and meeting-CTA limits;
- permitted conversation acts, state paths, slot visibility, and grounded claims where a fixture specifies them.

The default `--fail-on deterministic` exits non-zero for runtime or deterministic failures. `--fail-on never` is useful for exploratory live runs.

An optional model judge scores support helpfulness, answer-first behavior, continuity, language adaptation, naturalness, booking pressure, and groundedness:

```bash
OPENAI_API_KEY=... python scripts/run_chatbot_evals.py \
  --provider live \
  --suite regression \
  --limit 3 \
  --samples 3 \
  --judge model \
  --judge-model "$OPENAI_MODEL" \
  --max-live-turns 12 \
  --fail-on never
```

Judge scores are advisory until calibrated against human labels. A model judge must never override deterministic evidence about a tool call, booking, handoff, state transition, or other side effect.

## Model-judge calibration

Before using `--fail-on all` in CI:

1. Build a blinded set containing clear passes, clear failures, and difficult boundary cases from every judge category.
2. Have at least two reviewers label each sample independently using the same rubric, then adjudicate disagreements.
3. Run the pinned judge model and compare it with the adjudicated labels per category, including false-pass and false-fail rates rather than only average score.
4. Inspect disagreements for prompt leakage, verbosity bias, language bias, and inconsistent treatment of legitimate booking transitions.
5. Change one rubric or judge prompt variable at a time and rerun the full labeled calibration set.
6. Promote judge results to a gate only after the team agrees on acceptable error rates; keep the calibration set and judge model version stable.

This follows the general principle in OpenAI's [evaluation best practices](https://developers.openai.com/api/docs/guides/evaluation-best-practices): combine task-specific evals with human calibration and continuous evaluation instead of relying on subjective spot checks.

## CI and cost controls

Run replay on every pull request:

```bash
python scripts/run_chatbot_evals.py \
  --provider replay \
  --suite smoke \
  --fail-on deterministic
```

Run live suites manually or on a protected schedule. Do not expose `OPENAI_API_KEY` to untrusted fork builds. Start with a named scenario or `--limit`, keep `--samples` explicit, and set `--max-live-turns`. That guard caps sampled lead turns, not provider requests or cost. The displayed estimate allows up to three logical Agent V3 calls per turn and adds one judge call per scenario and sample; JSON repair and provider retry can add requests beyond that estimate.

Do not upload reports as CI artifacts by default: even synthetic transcripts and model responses should have an explicit retention decision. Review local report metadata for model, sample count, failures, category scores, cost, and latency. Increasing samples is useful only when the resulting variance is actually analyzed.

## Adding a scenario

1. Choose `smoke`, `regression`, or `journeys`, then add one JSON file whose `id` is unique and stable.
2. Use only synthetic names, `.test` email addresses, reserved-looking test phone numbers, and invented business facts. Never paste production transcripts, access tokens, webhook payloads, or customer identifiers into a fixture.
3. Keep the tenant knowledge, initial history, and inbound message just large enough to exercise one named behavior. Put the responsible team in `owner` and add useful risk/tags metadata.
4. Copy the complete response shape for that Agent V3 call phase into each `replay_outputs` entry. Normal decisions and post-tool replies use the full Agent V3 shape; slot resolution uses its compact decision shape. For tool turns, include outputs in actual call order. Use an empty array only when the production path handles the turn before invoking the model.
5. Define calendar results under `tool_world.slots` with `start`, `end`, and `display_time`; add `timezone`, `slot_id`, or `metadata` only when the behavior needs them. Use fixed ISO-8601 timestamps so replay results do not depend on today's date.
6. Assert outcomes rather than prose style. Prefer state, action, side effect, tool, grounded-fact, and bounded-conversation checks. Allow harmless wording variation.
7. Validate the corpus and run the new fixture in isolation before expanding the suite:

```bash
python -m json.tool evals/chatbot/fixtures/regression/example.json >/dev/null
python scripts/run_chatbot_evals.py --suite all --list
python scripts/run_chatbot_evals.py \
  --provider replay \
  --suite all \
  --scenario example
```

If a production behavior change intentionally alters a replay call sequence, update the fixture and explain the behavioral reason in review. Do not regenerate the corpus blindly from current model output; that would turn regressions into new baselines.
