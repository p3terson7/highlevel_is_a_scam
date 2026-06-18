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
- [ ] Remove or shrink legacy weekday-only helpers after all old tests and flows are migrated.

## Design
The new flow is:

`lead message + tool args -> BookingTimeRequest -> BookingSlotPlanner -> BookingProposalCopy -> SlotOffer`

The agent can still ask for `find_slots`, and the backend can still create real bookings. The difference is that slot selection is now deterministic and explainable before the LLM writes any message.

Slot selection now has a separate pre-LLM path:

`pending booking offer + clear slot reply -> BookingService.handle_slot_selection -> persisted outbound/CRM/audit/webhook`

If the lead gives a specific time that does not match the current menu, the handler returns control to the agent/tool flow so `find_slots` can check that exact requested time. This prevents the old “I could not match that to the current options” dead end.

Rescheduling is intentionally a two-step flow:

`already booked + new slot selected -> ask to cancel old call -> yes/no reply -> cancel/create or keep original`

While waiting for confirmation, the lead remains in `BOOKED` because the original meeting is still active.

## Key Behavior Rules
- `next Monday` means one specific calendar date, not every Monday in the horizon.
- A bare day like `Monday` is treated as the next upcoming occurrence unless the wording clearly means recurring Mondays.
- If the lead gives one day and says `all day`, offer multiple call times on that day.
- If the lead gives an exact time outside the last offered options, check that exact time instead of rejecting it as unrelated.
- If exact time is unavailable, offer closest same-day alternatives before jumping to other days.
- Broad first offers should show spread-out coverage, not three adjacent slots.
- Proposal text must say `call` or `consultation call` so the lead does not confuse it with a site visit.
- Short commitment replies like `lock it in` can book the only current slot, but must ask which option when multiple slots are still active.
- French-style time replies like `10h00` and `9 h 30` are valid slot selections and booking time requests.
- If a lead already has an internal calendar booking, selecting a new time asks for confirmation before canceling the first booking.

## Debug Metadata
Each `booking_offer` payload now includes:

- `request`: normalized request scope, dates, weekdays, time window, exact time, and reasoning notes.
- `planner`: selected strategy, match mode, candidate counts, and fallback reason when applicable.
- legacy fields like `preferred_day`, `exact_time`, and `match_mode` for backward compatibility.

## Implemented Files
- `app/services/booking_request.py`: parses natural language booking availability requests into a structured `BookingTimeRequest`.
- `app/services/booking_planner.py`: selects candidate slots using request-aware strategies.
- `app/services/booking_copy.py`: renders clear, call-specific booking proposal copy.
- `app/services/booking.py`: wires the parser/planner/copy flow into internal and external slot offers while preserving booking creation.
- `app/services/inbound_sms.py`: routes clear slot selections and reschedule confirmations through deterministic booking before invoking the LLM.
- `app/services/i18n.py`: remembers lead language and detects French 24-hour SMS time notation.
- `app/services/agent_v3.py`: passes the raw inbound lead message into slot lookup calls.
- `app/api/ui/sandbox_routes.py`: returns booking planner debug metadata from sandbox turns.
- `app/templates/ui.html` and `app/templates/ui_assets/ui-actions.js`: show booking debug output in Test Lab.
- `app/tests/test_booking_time_request.py`: parser coverage.
- `app/tests/test_booking_slot_planner.py`: planner coverage.
- `app/tests/test_booking_internal.py`: real `BookingService.find_slots` regression coverage.

## Verification
- Focused booking/agent/sandbox suite: `pytest app/tests/test_booking_time_request.py app/tests/test_booking_slot_planner.py app/tests/test_booking_internal.py app/tests/test_llm_agent.py app/tests/test_ui_admin_experience.py -q`
- Full suite: `pytest -q`
- Latest result: `102 passed in 8.59s`
