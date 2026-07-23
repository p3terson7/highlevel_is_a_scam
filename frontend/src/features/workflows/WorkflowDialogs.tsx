import { FormEvent, KeyboardEvent as ReactKeyboardEvent, useEffect, useRef, useState } from "react";
import {
  clearOutboundRequestKey,
  fetchLeadDetail,
  outboundRequestKey,
  sendBookingLink,
  sendManualMessage,
  updateAgentControl
} from "../../api/client";
import type { LeadDetailPayload, LeadListItem, ManualMeetingCreatePayload } from "../../api/types";

type ContactActionDialogProps = {
  leadId: number | null;
  onClose: () => void;
  onCreateMeeting: (leadId: number, clientKey: string) => void;
  onOpenRecord: (leadId: number, clientKey: string) => void;
  onOpenThread: (leadId: number, clientKey: string) => void;
};

export function ContactActionDialog({ leadId, onClose, onCreateMeeting, onOpenRecord, onOpenThread }: ContactActionDialogProps) {
  const [detail, setDetail] = useState<LeadDetailPayload | null>(null);
  const [status, setStatus] = useState("");
  const [message, setMessage] = useState("");
  const [pauseAfterSend, setPauseAfterSend] = useState(false);
  const [busy, setBusy] = useState(false);
  const [retryOutboundScope, setRetryOutboundScope] = useState<string | null>(null);
  const dialogRef = useRef<HTMLElement>(null);
  const closeRef = useRef<HTMLButtonElement>(null);

  useEffect(() => {
    if (!leadId) return;
    const previousFocus = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    const timer = window.setTimeout(() => closeRef.current?.focus(), 0);
    return () => {
      window.clearTimeout(timer);
      previousFocus?.focus();
    };
  }, [leadId]);

  useEffect(() => {
    if (!leadId) {
      setDetail(null);
      setStatus("");
      setMessage("");
      setRetryOutboundScope(null);
      return;
    }
    let cancelled = false;
    setDetail(null);
    setStatus("Loading contact...");
    setRetryOutboundScope(null);
    fetchLeadDetail(leadId)
      .then((payload) => {
        if (cancelled) return;
        setDetail(payload);
        setPauseAfterSend(Boolean(payload.lead.agent_control?.paused));
        setStatus("");
      })
      .catch((caught: unknown) => {
        if (!cancelled) setStatus(messageFor(caught, "Contact actions are unavailable."));
      });
    return () => {
      cancelled = true;
    };
  }, [leadId]);

  useEffect(() => {
    if (!leadId) return;
    const handleKeyDown = (event: globalThis.KeyboardEvent) => {
      if (event.key === "Escape") {
        event.preventDefault();
        onClose();
      }
      if (event.key !== "Tab" || !dialogRef.current) return;
      const focusable = Array.from(dialogRef.current.querySelectorAll<HTMLElement>(
        "button:not([disabled]), input:not([disabled]), textarea:not([disabled]), select:not([disabled]), [href], [tabindex]:not([tabindex='-1'])"
      ));
      if (!focusable.length) return;
      const first = focusable[0];
      const last = focusable[focusable.length - 1];
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first.focus();
      }
    };
    document.addEventListener("keydown", handleKeyDown);
    return () => document.removeEventListener("keydown", handleKeyDown);
  }, [leadId, onClose]);

  if (!leadId) return null;

  async function reload() {
    if (!leadId) return;
    setDetail(await fetchLeadDetail(leadId));
  }

  async function runAction(start: string, success: string, action: () => Promise<void>, onError?: () => void) {
    if (busy) return;
    setBusy(true);
    setStatus(start);
    setRetryOutboundScope(null);
    try {
      await action();
      setStatus(success);
    } catch (caught) {
      setStatus(messageFor(caught, "Action failed."));
      onError?.();
    } finally {
      setBusy(false);
    }
  }

  async function submitMessage(event: FormEvent) {
    event.preventDefault();
    if (!leadId || !message.trim()) return;
    const body = message.trim();
    const requestScope = `contact-message-${leadId}`;
    const requestKey = outboundRequestKey(requestScope, JSON.stringify({ body, pauseAfterSend }));
    await runAction(
      "Sending message...",
      pauseAfterSend ? "Message sent and AI paused." : "Message sent.",
      async () => {
        await sendManualMessage(leadId, body, pauseAfterSend, requestKey);
        clearOutboundRequestKey(requestScope);
        setMessage("");
        await reload().catch(() => undefined);
      },
      () => setRetryOutboundScope(requestScope)
    );
  }

  async function setPaused(paused: boolean) {
    if (!leadId) return;
    await runAction(paused ? "Pausing AI..." : "Resuming AI...", paused ? "AI paused for this contact." : "AI resumed for this contact.", async () => {
      await updateAgentControl(
        leadId,
        paused,
        paused ? "operator_paused" : "operator_resumed",
        paused ? "Paused from contact actions." : "Resumed from contact actions."
      );
      await reload();
    });
  }

  async function sendLink() {
    if (!leadId) return;
    const message = "Here is the booking link whenever you are ready.";
    const requestScope = `contact-booking-link-${leadId}`;
    const requestKey = outboundRequestKey(requestScope, message);
    await runAction(
      "Sending booking link...",
      "Booking link sent.",
      async () => {
        await sendBookingLink(leadId, message, requestKey);
        clearOutboundRequestKey(requestScope);
        await reload().catch(() => undefined);
      },
      () => setRetryOutboundScope(requestScope)
    );
  }

  function startNewOutboundAttempt() {
    if (!retryOutboundScope) return;
    const confirmed = window.confirm(
      "Verify the conversation and provider activity first. Start a new outbound attempt only if the previous message was not sent."
    );
    if (!confirmed) return;
    clearOutboundRequestKey(retryOutboundScope);
    setRetryOutboundScope(null);
    setStatus("A new outbound attempt is ready. Review the message, then send again.");
  }

  const control = detail?.lead.agent_control;
  const paused = Boolean(control?.paused);
  const optedOut = control?.mode === "opted_out";
  const canMessage = Boolean(detail?.lead.phone) && !optedOut;
  const clientKey = detail?.client.client_key || "";

  return (
    <div
      className="react-dialog-backdrop"
      onMouseDown={(event) => {
        if (event.target === event.currentTarget) onClose();
      }}
    >
      <section
        className="react-contact-dialog"
        role="dialog"
        aria-modal="true"
        aria-labelledby="contact-action-title"
        ref={dialogRef}
      >
        <header className="react-dialog-header">
          <div>
            <div className="scaffold-eyebrow">Contact actions</div>
            <h2 id="contact-action-title">{detail?.lead.display_name || "Contact"}</h2>
            <div className="meta-text">
              {detail ? [detail.lead.phone, detail.lead.email, detail.client.business_name].filter(Boolean).join(" · ") : "Loading contact details..."}
            </div>
          </div>
          <button className="small ghost" type="button" onClick={onClose} ref={closeRef}>Close</button>
        </header>

        {detail ? (
          <div className="react-dialog-body stack">
            <section className="detail-card stack">
              <div className="item-title-row">
                <div>
                  <div className="title">Manual message</div>
                  <div className="meta-text">Send a direct SMS without leaving your current view.</div>
                </div>
                <span className={`tag ${paused ? "warn" : "ok"}`}>{paused ? "AI paused" : "AI active"}</span>
              </div>
              <form className="stack" onSubmit={(event) => void submitMessage(event)}>
                <label htmlFor="contact-action-message">Message</label>
                <textarea
                  id="contact-action-message"
                  value={message}
                  disabled={busy || !canMessage}
                  onChange={(event) => setMessage(event.currentTarget.value)}
                  placeholder={canMessage ? "Write a direct SMS to this contact." : optedOut ? "This contact opted out." : "A phone number is required."}
                />
                <label className="checkbox-inline">
                  <input type="checkbox" checked={pauseAfterSend} disabled={busy} onChange={(event) => setPauseAfterSend(event.currentTarget.checked)} />
                  <span>Pause AI after sending</span>
                </label>
                <div className="actions">
                  <button className="primary small" type="submit" disabled={busy || !canMessage || !message.trim()}>Send message</button>
                  <button className="small ghost" type="button" disabled={busy || paused} onClick={() => void setPaused(true)}>Pause AI</button>
                  <button className="small ghost" type="button" disabled={busy || !paused || optedOut} onClick={() => void setPaused(false)}>Resume AI</button>
                </div>
              </form>
            </section>

            <section className="detail-card stack">
              <div className="title">Next actions</div>
              <div className="drawer-action-grid">
                <button className="small ghost" type="button" onClick={() => onOpenThread(leadId, clientKey)}>Open thread</button>
                <button className="small ghost" type="button" onClick={() => onOpenRecord(leadId, clientKey)}>Open record</button>
                <button className="small ghost" type="button" onClick={() => onCreateMeeting(leadId, clientKey)}>Create meeting</button>
                <button
                  className="small ghost"
                  type="button"
                  disabled={busy || !canMessage || !detail.client.booking_url}
                  title={detail.client.booking_url ? "Send the configured booking link" : "Configure a booking link first"}
                  onClick={() => void sendLink()}
                >
                  Send booking link
                </button>
              </div>
            </section>
          </div>
        ) : <div className="empty-state" aria-live="polite">{status || "Loading contact..."}</div>}
        {detail && status ? <div className="react-dialog-status meta-text" role="status">{status}</div> : null}
        {detail && retryOutboundScope ? (
          <button className="small warn" type="button" disabled={busy} onClick={startNewOutboundAttempt}>
            Start a new outbound attempt
          </button>
        ) : null}
      </section>
    </div>
  );
}

type CalendarMeetingFormProps = {
  busy: boolean;
  clientKey: string;
  defaultLeadId?: number | null;
  leads: LeadListItem[];
  selectedDateKey: string;
  timeZone: string;
  onCancel: () => void;
  onSubmit: (payload: ManualMeetingCreatePayload) => Promise<void>;
};

type MeetingLeadMode = "existing" | "new";

export function CalendarMeetingForm({ busy, clientKey, defaultLeadId, leads, selectedDateKey, timeZone, onCancel, onSubmit }: CalendarMeetingFormProps) {
  const [mode, setMode] = useState<MeetingLeadMode>(leads.length ? "existing" : "new");
  const [leadId, setLeadId] = useState(defaultLeadId || leads[0]?.lead_id || 0);
  const [title, setTitle] = useState("Discovery meeting");
  const [startAt, setStartAt] = useState(() => defaultMeetingStart(selectedDateKey));
  const [duration, setDuration] = useState(30);
  const [notes, setNotes] = useState("");
  const [name, setName] = useState("");
  const [phone, setPhone] = useState("");
  const [email, setEmail] = useState("");
  const [city, setCity] = useState("");
  const [validation, setValidation] = useState("");
  const titleRef = useRef<HTMLInputElement>(null);
  const leadOptionsKey = leads.map((lead) => lead.lead_id).join(",");

  useEffect(() => {
    setMode(leads.length ? "existing" : "new");
    setLeadId(defaultLeadId && leads.some((lead) => lead.lead_id === defaultLeadId) ? defaultLeadId : leads[0]?.lead_id || 0);
    setStartAt(defaultMeetingStart(selectedDateKey));
    window.setTimeout(() => titleRef.current?.focus(), 0);
  }, [clientKey, defaultLeadId, leadOptionsKey, selectedDateKey]);

  async function submit(event: FormEvent) {
    event.preventDefault();
    if (!clientKey || !title.trim() || !startAt || !timeZone) {
      setValidation("Client, title, date/time, and timezone are required.");
      return;
    }
    if (mode === "existing" && !leadId) {
      setValidation("Choose an existing contact.");
      return;
    }
    if (mode === "new" && !name.trim()) {
      setValidation("A name is required for the new contact.");
      return;
    }
    setValidation("");
    await onSubmit({
      duration_minutes: duration,
      lead_id: mode === "existing" ? leadId : undefined,
      new_lead: mode === "new" ? {
        city: cleanOptional(city),
        email: cleanOptional(email),
        full_name: name.trim(),
        phone: cleanOptional(phone)
      } : undefined,
      notes: cleanOptional(notes),
      start_at: startAt,
      timezone: timeZone,
      title: title.trim()
    });
  }

  return (
    <form className="manual-panel react-meeting-form" aria-labelledby="react-meeting-form-title" onSubmit={(event) => void submit(event)}>
      <div className="manual-panel-head">
        <div>
          <div className="title" id="react-meeting-form-title">Add meeting</div>
          <div className="meta-text">Book an existing contact or create a contact inline.</div>
        </div>
        <button className="small ghost" type="button" onClick={onCancel} disabled={busy}>Close</button>
      </div>

      <fieldset className="react-segmented-fieldset">
        <legend>Contact</legend>
        <label className="checkbox-inline">
          <input type="radio" name="meeting-lead-mode" value="existing" checked={mode === "existing"} disabled={!leads.length || busy} onChange={() => setMode("existing")} />
          <span>Existing contact</span>
        </label>
        <label className="checkbox-inline">
          <input type="radio" name="meeting-lead-mode" value="new" checked={mode === "new"} disabled={busy} onChange={() => setMode("new")} />
          <span>New contact</span>
        </label>
      </fieldset>

      {mode === "existing" ? (
        <label>
          Existing contact
          <select value={leadId} disabled={busy} onChange={(event) => setLeadId(Number(event.currentTarget.value))} required>
            <option value={0}>Choose contact</option>
            {leads.map((lead) => <option value={lead.lead_id} key={lead.lead_id}>{lead.lead_name || lead.phone || `Contact ${lead.lead_id}`}</option>)}
          </select>
        </label>
      ) : (
        <div className="form-grid-3">
          <label>Name<input value={name} disabled={busy} onChange={(event) => setName(event.currentTarget.value)} required /></label>
          <label>Phone<input value={phone} disabled={busy} onChange={(event) => setPhone(event.currentTarget.value)} placeholder="+15551234567" /></label>
          <label>Email<input type="email" value={email} disabled={busy} onChange={(event) => setEmail(event.currentTarget.value)} /></label>
          <label>City<input value={city} disabled={busy} onChange={(event) => setCity(event.currentTarget.value)} /></label>
        </div>
      )}

      <div className="form-grid-3">
        <label>Title<input ref={titleRef} value={title} disabled={busy} onChange={(event) => setTitle(event.currentTarget.value)} required /></label>
        <label>Date and time<input type="datetime-local" value={startAt} disabled={busy} onChange={(event) => setStartAt(event.currentTarget.value)} required /></label>
        <label>Duration
          <select value={duration} disabled={busy} onChange={(event) => setDuration(Number(event.currentTarget.value))}>
            {[15, 30, 45, 60, 90, 120].map((minutes) => <option value={minutes} key={minutes}>{minutes} minutes</option>)}
          </select>
        </label>
        <label>Timezone<input value={timeZone} readOnly aria-readonly="true" /></label>
      </div>
      <label>Notes<textarea value={notes} disabled={busy} onChange={(event) => setNotes(event.currentTarget.value)} placeholder="Optional agenda or preparation notes." /></label>
      {validation ? <div className="meta-text" role="alert">{validation}</div> : null}
      <div className="actions">
        <button className="primary" type="submit" disabled={busy}>{busy ? "Adding meeting..." : "Add meeting"}</button>
        <button className="small ghost" type="button" onClick={onCancel} disabled={busy}>Cancel</button>
      </div>
    </form>
  );
}

export function PaneResizer({ edge, value, onKeyboard, onPointerDown }: {
  edge: "left" | "right";
  value: number;
  onKeyboard: (edge: "left" | "right", delta: number) => void;
  onPointerDown: (edge: "left" | "right", clientX: number) => void;
}) {
  function keyDown(event: ReactKeyboardEvent<HTMLDivElement>) {
    if (event.key !== "ArrowLeft" && event.key !== "ArrowRight") return;
    event.preventDefault();
    const separatorDelta = event.key === "ArrowLeft" ? -20 : 20;
    onKeyboard(edge, edge === "left" ? separatorDelta : -separatorDelta);
  }
  return (
    <div
      className="drag-handle react-pane-resizer"
      role="separator"
      aria-label={`Resize ${edge === "left" ? "conversation list" : "contact details"}`}
      aria-orientation="vertical"
      aria-valuemin={240}
      aria-valuemax={edge === "left" ? 520 : 420}
      aria-valuenow={value}
      tabIndex={0}
      onKeyDown={keyDown}
      onPointerDown={(event) => {
        event.preventDefault();
        onPointerDown(edge, event.clientX);
      }}
    />
  );
}

function defaultMeetingStart(dateKey: string) {
  const fallback = new Date();
  fallback.setMinutes(0, 0, 0);
  fallback.setHours(fallback.getHours() + 1);
  const fallbackDate = `${fallback.getFullYear()}-${String(fallback.getMonth() + 1).padStart(2, "0")}-${String(fallback.getDate()).padStart(2, "0")}`;
  const selected = /^\d{4}-\d{2}-\d{2}$/.test(dateKey) ? dateKey : fallbackDate;
  const time = selected === fallbackDate ? `${String(fallback.getHours()).padStart(2, "0")}:00` : "09:00";
  return `${selected}T${time}`;
}

function cleanOptional(value: string) {
  const cleaned = value.trim();
  return cleaned || undefined;
}

function messageFor(error: unknown, fallback: string) {
  return error instanceof Error ? error.message : fallback;
}
