# Booking Proposal Restructure

## Goal
Replace the fragile booking-time proposal behavior with a deterministic, testable subsystem that preserves what the lead actually asked for before selecting slots.

## Checklist
- [x] Document the old failure mode: weekday-only extraction caused `next Monday` to become every Monday.
- [x] Add a booking request parser that preserves date scope, time scope, change-of-mind signals, and debug reasoning.
- [x] Add a booking slot planner that chooses slots by request strategy instead of one generic spread helper.
- [x] Add booking proposal copy that clearly says the times are for a call.
- [x] Wire the new parser/planner/copy path into `BookingService.find_slots` without removing existing booking creation.
- [x] Pass the latest inbound message into booking searches from Agent V3 so natural language requests are parsed directly.
- [x] Add raw payload debug metadata for request parsing, planner strategy, match mode, and candidate counts.
- [x] Add unit tests for parser and planner edge cases.
- [x] Add a regression test for `next Monday` so the planner never returns one slot from each future Monday again.
- [x] Expand Test Lab UI to expose booking planner debug metadata in a client-friendly way.
- [x] Add a pre-LLM booking selection layer so clear slot replies book before conversational generation.
- [x] Add sticky lead language memory so short replies like `10h00` do not flip French conversations back to English.
- [x] Add reschedule confirmation state so an already-booked lead must approve canceling the first call before a new one is created.
- [x] Add an LLM-first conversation planner contract so the model chooses a semantic act before the backend validates tools.
- [x] Add backend planner validation so call/scheduling intent cannot end in a passive `yes, I can help` reply.
- [x] Add always-on website-derived business memory in addition to per-question knowledge retrieval.
- [x] Add active booking offer state so the backend resolves against the same slots the lead actually saw.
- [x] Add LLM slot resolution for fuzzy confirmations like `yes lock it in` before falling back to legacy deterministic prompts.
- [x] Force post-tool slot replies to use the backend-rendered offer text so visible times and structured booking payloads cannot diverge.
- [ ] Remove or shrink legacy weekday-only helpers after all old tests and flows are migrated.

## Design
The new flow is:

`lead message + tool args -> BookingTimeRequest -> BookingSlotPlanner -> BookingProposalCopy -> SlotOffer`

The agent can still ask for `find_slots`, and the backend can still create real bookings. The difference is that slot selection is now deterministic and explainable before the LLM writes any message.

Slot selection now has a separate pre-LLM path:

`pending booking offer + clear slot reply -> BookingService.handle_slot_selection -> persisted outbound/CRM/audit/webhook`

If the lead gives a specific time that does not match the current menu, the handler returns control to the agent/tool flow so `find_slots` can check that exact requested time. This prevents the old “I could not match that to the current options” dead end.

Slot confirmation now has a second, LLM-assisted resolver:

`active booking offer + fuzzy confirmation + visible last outbound -> LLM slot resolver -> BookingService.handle_slot_selection`

This handles human replies such as `yes lock it in`, `that one works`, or `sure` after the previous message clearly singled out a slot. The LLM can interpret the conversation, but it cannot create or book an invented time. The backend still validates the selected slot against the active structured offer before creating the booking.

The currently active offer is stored on the lead as `active_booking_offer`. New slot offers replace it, successful bookings clear it, and legacy `booking_offer` remains for backward compatibility. This prevents the UI from showing one slot while the backend tries to resolve against an older five-slot menu.

Rescheduling is intentionally a two-step flow:

`already booked + new slot selected -> ask to cancel old call -> yes/no reply -> cancel/create or keep original`

While waiting for confirmation, the lead remains in `BOOKED` because the original meeting is still active.

The agent conversation layer now has a separate planning contract:

`lead message + context -> conversation_act + lead_intent + reply/tool request -> backend validation -> optional booking tool`

The LLM decides the conversational move first, but the backend still owns tool safety. If the planner says the lead wants slots, asks for a call, or describes an immediate booking intent but forgets `find_slots`, the backend forces a live slot lookup. If the lead asks a factual/pricing question, refuses a call, or only needs education, the backend blocks accidental slot offers and strips repetitive CTAs.

Website context now has two layers:

- `business_profile_context`: compact website-derived business memory passed to every Agent V3 turn.
- `knowledge_context`: query-matched source snippets for specific lead questions.

This keeps broad questions from seeing an empty website context just because retrieval did not match the exact words.

## Key Behavior Rules
- `next Monday` means one specific calendar date, not every Monday in the horizon.
- A bare day like `Monday` is treated as the next upcoming occurrence unless the wording clearly means recurring Mondays.
- If the lead gives one day and says `all day`, offer multiple call times on that day.
- If the lead gives an exact time outside the last offered options, check that exact time instead of rejecting it as unrelated.
- If exact time is unavailable, offer closest same-day alternatives before jumping to other days.
- Broad first offers should show spread-out coverage, not three adjacent slots.
- Proposal text must say `call` or `consultation call` so the lead does not confuse it with a site visit.
- Short commitment replies like `lock it in` can book the only current slot, but must ask which option when multiple slots are still active.
- Fuzzy confirmations like `yes lock it in` should book the slot that was clearly singled out in the last outbound message, after backend validation.
- French-style time replies like `10h00` and `9 h 30` are valid slot selections and booking time requests.
- If a lead already has an internal calendar booking, selecting a new time asks for confirmation before canceling the first booking.
- If the lead says they want a call or are interested in a call, Agent V3 must send real available times when a booking backend is available.
- If the lead refuses a call, that refusal overrides scheduling keyword detection.
- If the lead asks a factual question, answer first and do not append a generic meeting CTA unless the lead explicitly asks for next steps or scheduling.
- Use `business_profile_context` before saying a general business fact is unknown.

## Debug Metadata
Each `booking_offer` payload now includes:

- `request`: normalized request scope, dates, weekdays, time window, exact time, and reasoning notes.
- `planner`: selected strategy, match mode, candidate counts, and fallback reason when applicable.
- legacy fields like `preferred_day`, `exact_time`, and `match_mode` for backward compatibility.
- `active_booking_offer`: the current canonical offer used to resolve later booking replies.
- `booking_resolution`: the LLM slot resolver decision when a fuzzy booking reply needed semantic interpretation.

## Implemented Files
- `app/services/booking_request.py`: parses natural language booking availability requests into a structured `BookingTimeRequest`.
- `app/services/booking_planner.py`: selects candidate slots using request-aware strategies.
- `app/services/booking_copy.py`: renders clear, call-specific booking proposal copy.
- `app/services/booking.py`: wires the parser/planner/copy flow into internal and external slot offers while preserving booking creation.
- `app/services/inbound_sms.py`: stores active booking offers, routes clear slot selections and reschedule confirmations before the main LLM, and uses the LLM slot resolver for fuzzy confirmations.
- `app/services/i18n.py`: remembers lead language and detects French 24-hour SMS time notation.
- `app/services/agent_v3.py`: passes the raw inbound lead message into slot lookup calls, builds the LLM-first planner prompt, validates planner/tool alignment, resolves fuzzy slot confirmations, and keeps post-tool slot copy aligned with backend payloads.
- `app/services/agent_v3_types.py`: defines planner output fields (`conversation_act`, `lead_intent`, confidence, reasoning summary, knowledge usage).
- `app/services/agent_v3_helpers.py`: applies CTA suppression, answer-first guardrails, and planner debug metadata.
- `app/services/knowledge.py`: builds and refreshes always-on `business_profile_context` from ingested website sources.
- `app/api/ui/sandbox_routes.py`: returns booking planner debug metadata from sandbox turns.
- `app/templates/ui.html` and `app/templates/ui_assets/ui-actions.js`: show booking debug output in Test Lab.
- `app/templates/ui_assets/ui-views.js`: shows the business profile context in the knowledge UI for testing.
- `app/tests/test_booking_time_request.py`: parser coverage.
- `app/tests/test_booking_slot_planner.py`: planner coverage.
- `app/tests/test_booking_internal.py`: real `BookingService.find_slots` regression coverage.
- `app/tests/test_llm_agent.py`: Agent V3 planner, CTA, pricing, refusal, and call-interest regression coverage.
- `app/tests/test_sms_flow.py`: inbound booking flow coverage, including fuzzy `lock it in` resolution against the visible single-slot reply.
- `app/tests/test_knowledge.py`: retrieval and always-on business memory coverage.

## Verification
- Focused booking/agent/sandbox suite: `pytest app/tests/test_booking_time_request.py app/tests/test_booking_slot_planner.py app/tests/test_booking_internal.py app/tests/test_llm_agent.py app/tests/test_ui_admin_experience.py -q`
- Full suite: `pytest -q`
- Latest focused result: `48 passed in 2.10s`
- Latest full result: `115 passed in 8.40s`
