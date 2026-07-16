import { FormEvent, useEffect, useState } from "react";
import type { Dispatch, ReactNode, SetStateAction } from "react";
import {
  createClient,
  fetchAuditLogs,
  fetchAutomationHealth,
  fetchClientDetail,
  fetchClients,
  fetchOwnerKnowledge,
  fetchOwnerWorkspace,
  fetchRuntimeStatus,
  ingestOwnerKnowledge,
  startSandbox,
  updateClient,
  updateOwnerAiContext,
  updateOwnerCalendar,
  updateRuntimeConfig
} from "../../api/client";
import type {
  AuditLogItem,
  AutomationHealthPayload,
  ChecklistItem,
  ClientDetailPayload,
  ClientSummary,
  KnowledgePayload,
  OwnerCalendarAvailabilityRow,
  OwnerCalendarConfig,
  OwnerWorkspacePayload,
  RuntimeConfigStatus,
  SandboxStartResponse,
} from "../../api/types";
import { useAuth } from "../auth/AuthProvider";
import { AutomationDiagnosticsPanel, DemoDataPanel, IntakeProbePanel, ProviderProbePanel } from "./AdminTools";

type AdminPageProps = {
  onReadyChange?: (ready: boolean) => void;
};

type AsyncStatus = "idle" | "loading" | "ready" | "error";
type ClientTab = "overview" | "edit" | "webhooks";
type ClientWizardStep = "business" | "channels" | "agent" | "booking" | "portal" | "review";

const CLIENT_WIZARD_STEPS: ClientWizardStep[] = ["business", "channels", "agent", "booking", "portal", "review"];

const CLIENT_WIZARD_COPY: Record<ClientWizardStep, { title: string; hint: string }> = {
  business: {
    title: "Business setup",
    hint: "Name the workspace, choose language, tone, timezone, and activation state."
  },
  channels: {
    title: "Channels and handoff",
    hint: "Configure client-scoped Twilio, Zapier, public URL, consent, and handoff details."
  },
  agent: {
    title: "Agent guidance",
    hint: "Shape qualification questions, business context, FAQ, AI playbook, and optional templates."
  },
  booking: {
    title: "Booking rules",
    hint: "Choose the booking mode and define the internal calendar availability the agent can use."
  },
  portal: {
    title: "Client portal",
    hint: "Create the client login so the business can manage inbox, pipeline, calendar, and settings."
  },
  review: {
    title: "Review setup",
    hint: "Confirm the workspace is presentation-ready before saving the client."
  }
};

const DEFAULT_AVAILABILITY: OwnerCalendarAvailabilityRow[] = [
  { day: 0, start: "09:00", end: "17:00", enabled: true },
  { day: 1, start: "09:00", end: "17:00", enabled: true },
  { day: 2, start: "09:00", end: "17:00", enabled: true },
  { day: 3, start: "09:00", end: "17:00", enabled: true },
  { day: 4, start: "09:00", end: "17:00", enabled: true },
  { day: 5, start: "10:00", end: "14:00", enabled: false },
  { day: 6, start: "10:00", end: "14:00", enabled: false }
];

const DEFAULT_CALENDAR: OwnerCalendarConfig = {
  slot_minutes: 30,
  notice_minutes: 120,
  horizon_days: 14,
  availability: DEFAULT_AVAILABILITY
};

const DEFAULT_TEST_ANSWERS = [
  { question: "Timeline", answer: "Within 2 weeks" },
  { question: "Service interest", answer: "I want to understand options and next steps" },
  { question: "Main goal", answer: "Find the right solution without wasting time" },
  { question: "Decision role", answer: "Owner" }
];

export function ClientsPage({ onReadyChange }: AdminPageProps) {
  const auth = useAuth();
  const [status, setStatus] = useState<AsyncStatus>("idle");
  const [error, setError] = useState("");
  const [clients, setClients] = useState<ClientSummary[]>([]);
  const [selectedClientKey, setSelectedClientKey] = useState("");
  const [detail, setDetail] = useState<ClientDetailPayload | null>(null);
  const [health, setHealth] = useState<AutomationHealthPayload | null>(null);
  const [creating, setCreating] = useState(false);
  const [form, setForm] = useState<ClientFormState>(() => emptyClientForm());
  const [saveStatus, setSaveStatus] = useState("");
  const [clientSearch, setClientSearch] = useState("");
  const [clientTab, setClientTab] = useState<ClientTab>(() => readClientTab());
  const [wizardStep, setWizardStep] = useState<ClientWizardStep>(() => readClientWizardStep());

  const filteredClients = filterClients(clients, clientSearch);
  const selectedSummary = clients.find((client) => client.client_key === selectedClientKey) || null;
  const wizardCopy = CLIENT_WIZARD_COPY[wizardStep];
  const wizardIndex = CLIENT_WIZARD_STEPS.indexOf(wizardStep);

  useEffect(() => {
    if (auth.status !== "ready") {
      onReadyChange?.(false);
      return;
    }
    if (auth.session.role !== "admin") {
      setStatus("ready");
      onReadyChange?.(true);
      return;
    }
    let cancelled = false;
    setStatus("loading");
    setError("");
    onReadyChange?.(false);
    fetchClients()
      .then((items) => {
        if (cancelled) return;
        setClients(items);
        const selected = chooseClientKey(items);
        setSelectedClientKey(selected);
        if (!selected) {
          setStatus("ready");
          onReadyChange?.(true);
        }
      })
      .catch((caught: unknown) => {
        if (cancelled) return;
        setError(messageFor(caught, "Clients unavailable"));
        setStatus("error");
        onReadyChange?.(false);
      });
    return () => {
      cancelled = true;
    };
  }, [auth.status]);

  useEffect(() => {
    if (auth.status !== "ready" || auth.session.role !== "admin" || !selectedClientKey || creating) return;
    let cancelled = false;
    setStatus("loading");
    setError("");
    onReadyChange?.(false);
    rememberClientKey(selectedClientKey);
    Promise.all([fetchClientDetail(selectedClientKey), fetchAutomationHealth(selectedClientKey)])
      .then(([clientDetail, automationHealth]) => {
        if (cancelled) return;
        setDetail(clientDetail);
        setHealth(automationHealth);
        setForm(formFromDetail(clientDetail));
        setSaveStatus("");
        setStatus("ready");
        onReadyChange?.(true);
      })
      .catch((caught: unknown) => {
        if (cancelled) return;
        setError(messageFor(caught, "Client workspace unavailable"));
        setStatus("error");
        onReadyChange?.(false);
      });
    return () => {
      cancelled = true;
    };
  }, [auth.status, selectedClientKey, creating]);

  if (auth.status === "ready" && auth.session.role !== "admin") {
    return <AdminOnlyPage title="Clients" onReadyChange={onReadyChange} />;
  }
  if (status !== "ready" && clients.length === 0) return null;

  async function saveClient(event: FormEvent) {
    event.preventDefault();
    setSaveStatus("Saving client...");
    try {
      const payload = clientFormPayload(form, creating, detail?.client);
      const result = creating ? await createClient(payload) : await updateClient(selectedClientKey, payload);
      const nextKey = result.client_key || selectedClientKey;
      const refreshedClients = await fetchClients();
      setClients(refreshedClients);
      setCreating(false);
      setSelectedClientKey(nextKey);
      rememberClientKey(nextKey);
      const [clientDetail, automationHealth] = await Promise.all([fetchClientDetail(nextKey), fetchAutomationHealth(nextKey)]);
      setDetail(clientDetail);
      setHealth(automationHealth);
      setForm(formFromDetail(clientDetail));
      setSaveStatus("Saved.");
      setStatus("ready");
      onReadyChange?.(true);
    } catch (caught: unknown) {
      setSaveStatus(messageFor(caught, "Save failed"));
    }
  }

  async function refreshClients() {
    setSaveStatus("Refreshing clients...");
    try {
      const refreshedClients = await fetchClients();
      setClients(refreshedClients);
      if (selectedClientKey && !creating) {
        const [clientDetail, automationHealth] = await Promise.all([fetchClientDetail(selectedClientKey), fetchAutomationHealth(selectedClientKey)]);
        setDetail(clientDetail);
        setHealth(automationHealth);
        setForm(formFromDetail(clientDetail));
      }
      setSaveStatus("Refreshed.");
    } catch (caught: unknown) {
      setSaveStatus(messageFor(caught, "Refresh failed"));
    }
  }

  function chooseClientTab(tab: ClientTab) {
    setClientTab(tab);
    window.localStorage.setItem("lead-ui-client-tab", tab);
  }

  function chooseWizardStep(step: ClientWizardStep) {
    setWizardStep(step);
    window.localStorage.setItem("lead-ui-client-wizard-step", step);
  }

  function moveWizard(delta: number) {
    const nextIndex = Math.max(0, Math.min(CLIENT_WIZARD_STEPS.length - 1, wizardIndex + delta));
    chooseWizardStep(CLIENT_WIZARD_STEPS[nextIndex]);
  }

  function startNewClient() {
    setCreating(true);
    setDetail(null);
    setHealth(null);
    setForm(emptyClientForm());
    setStatus("ready");
    setSaveStatus("");
    chooseClientTab("edit");
    chooseWizardStep("business");
    onReadyChange?.(true);
  }

  return (
    <div className="react-admin-page react-clients-page stack" data-testid="react-clients-page">
      <section className="surface react-clients-command">
        <div className="surface-header">
          <div className="surface-title">
            <div>
              <h2>Clients</h2>
              <div className="surface-subtitle">Dense tenant index on the left, client workspace on the right.</div>
            </div>
          </div>
          <div className="actions">
            <button className="small" type="button" onClick={startNewClient}>New client</button>
            <button className="small ghost" type="button" onClick={() => void refreshClients()}>Refresh</button>
          </div>
        </div>
      </section>

      <div className="two-column-shell split-shell react-clients-shell">
        <aside className="pane">
        <div className="pane-header">
          <div className="pane-title">
            <h3>Client Index</h3>
          </div>
          <span className="badge">{filteredClients.length}</span>
        </div>
        <div className="pane-body">
          <div className="records-toolbar">
            <input
              aria-label="Search clients"
              type="search"
              value={clientSearch}
              onChange={(event) => setClientSearch(event.currentTarget.value)}
              placeholder="Search clients..."
            />
          </div>
          <div className="compact-list">
            {filteredClients.length ? (
              filteredClients.map((client) => (
                <button
                  className={`client-item ${client.client_key === selectedClientKey && !creating ? "active" : ""}`}
                  key={client.client_key}
                  type="button"
                  onClick={() => {
                    setCreating(false);
                    setSelectedClientKey(client.client_key);
                  }}
                >
                  <div className="item-title-row">
                    <div className="item-title">{client.business_name}</div>
                    {renderBadge(client.is_active ? "active" : "inactive", client.is_active ? "ok" : "warn")}
                  </div>
                  <div className="item-subtitle">{client.client_key}</div>
                  <div className="lead-list-meta">
                    <span>{client.lead_count ?? 0} records</span>
                    <span>{formatDateTime(client.last_activity_at || "")}</span>
                  </div>
                </button>
              ))
            ) : (
              <div className="empty-state">{clientSearch ? "No clients match that search." : "No clients created yet."}</div>
            )}
          </div>
        </div>
      </aside>

      <section className="pane focus-surface">
        <div className="pane-header">
          <div className="pane-title">
            <h3>{creating ? "New client" : detail?.client.business_name ? `${detail.client.business_name} workspace` : "Client workspace"}</h3>
          </div>
          <div className="tab-bar" role="tablist" aria-label="Client workspace sections">
            <button className={`tab-btn ${clientTab === "overview" ? "active" : ""}`} type="button" role="tab" aria-selected={clientTab === "overview"} onClick={() => chooseClientTab("overview")}>Overview</button>
            <button className={`tab-btn ${clientTab === "edit" ? "active" : ""}`} type="button" role="tab" aria-selected={clientTab === "edit"} onClick={() => chooseClientTab("edit")}>Edit</button>
            <button className={`tab-btn ${clientTab === "webhooks" ? "active" : ""}`} type="button" role="tab" aria-selected={clientTab === "webhooks"} onClick={() => chooseClientTab("webhooks")}>Webhooks</button>
          </div>
        </div>
        <div className="pane-body stack">
          {error ? <div className="empty-state">{error}</div> : null}
          {saveStatus ? <div className="meta-text" role="status">{saveStatus}</div> : null}

          <div className={`tab-panel ${clientTab === "overview" ? "active" : ""}`} role="tabpanel">
            {creating ? (
              <div className="empty-state">Create the client first, then the overview fills in automatically.</div>
            ) : detail ? (
              <>
              <div className="grid cols-4">
                <StatCard label="State" value={detail.client.is_active ? "active" : "inactive"} />
                <StatCard label="Timezone" value={detail.client.timezone || "-"} />
                <StatCard label="Booking" value={formatBookingMode(detail.client.booking_mode || "link")} />
                <StatCard label="Runtime" value={detail.provider_runtime?.source === "client" ? "client overrides" : "global fallback"} />
              </div>
              <div className="grid cols-4">
                <StatCard label="Records" value={selectedSummary?.lead_count ?? detail.counts?.leads ?? "-"} />
                <StatCard label="Open conversations" value={selectedSummary?.open_conversations ?? detail.counts?.open_conversations ?? "-"} />
                <StatCard label="Last activity" value={formatDateTime(selectedSummary?.last_activity_at || "") || "-"} />
                <StatCard label="Last webhook" value={formatDateTime(selectedSummary?.last_webhook_received_at || "") || "-"} />
              </div>
              <section className="detail-card stack">
                <div className="title">Automation readiness</div>
                <Checklist items={activeChecklist(health?.automations.map((item) => ({ label: item.label, done: item.status === "healthy", detail: item.detail })) || detail.onboarding || [])} />
              </section>
              <div className="grid cols-2">
                <section className="detail-card stack">
                  <div className="title">Recent conversations</div>
                  <ConversationPreviewList items={detail.recent_conversations || []} />
                </section>
                <section className="detail-card stack">
                  <div className="title">Recent audit logs</div>
                  <AuditList logs={detail.recent_logs || []} />
                </section>
              </div>
              </>
            ) : (
              <div className="empty-state">Select a client from the left pane.</div>
            )}
          </div>

          <form className={`tab-panel ${clientTab === "edit" ? "active" : ""}`} role="tabpanel" onSubmit={(event) => void saveClient(event)}>
            <div className="client-wizard">
              <div className="client-wizard-head">
                <div>
                  <div className="dashboard-kicker">Client onboarding</div>
                  <div className="client-wizard-title">{wizardCopy.title}</div>
                  <div className="surface-subtitle">{wizardCopy.hint}</div>
                </div>
                <div className="client-wizard-nav">
                  <button className="small ghost" type="button" disabled={wizardIndex <= 0} onClick={() => moveWizard(-1)}>Previous</button>
                  <button className="small" type="button" disabled={wizardIndex >= CLIENT_WIZARD_STEPS.length - 1} onClick={() => moveWizard(1)}>{wizardIndex >= CLIENT_WIZARD_STEPS.length - 2 ? "Review" : "Next step"}</button>
                </div>
              </div>
              <div className="client-wizard-steps" role="list">
                {CLIENT_WIZARD_STEPS.map((step) => (
                  <button
                    className={`client-wizard-step ${wizardStep === step ? "active" : ""}`}
                    key={step}
                    type="button"
                    aria-current={wizardStep === step ? "step" : undefined}
                    onClick={() => chooseWizardStep(step)}
                  >
                    {titleize(step)}
                  </button>
                ))}
              </div>
            </div>
            {wizardStep === "business" ? (
              <>
                <div className="form-grid-2">
                  <Field label="Business name">
                    <input value={form.business_name} onChange={(event) => setFormField(setForm, "business_name", event.currentTarget.value)} required />
                  </Field>
                  <Field label="Client key">
                    <input className="mono" value={form.client_key} onChange={(event) => setFormField(setForm, "client_key", event.currentTarget.value)} disabled={!creating} placeholder="demo-roofing" />
                  </Field>
                </div>
                <div className="form-grid-3">
                  <Field label="Tone">
                    <input value={form.tone} onChange={(event) => setFormField(setForm, "tone", event.currentTarget.value)} />
                  </Field>
                  <Field label="Timezone">
                    <input value={form.timezone} onChange={(event) => setFormField(setForm, "timezone", event.currentTarget.value)} />
                  </Field>
                  <Field label="Workspace language">
                    <select value={form.language} onChange={(event) => setFormField(setForm, "language", event.currentTarget.value)}>
                      <option value="en">English</option>
                      <option value="fr">Français</option>
                    </select>
                  </Field>
                </div>
                <div className="form-grid-3">
                  <Field label="Active">
                    <select value={String(form.is_active)} onChange={(event) => setFormField(setForm, "is_active", event.currentTarget.value === "true")}>
                      <option value="true">true</option>
                      <option value="false">false</option>
                    </select>
                  </Field>
                </div>
              </>
            ) : null}

            {wizardStep === "channels" ? (
              <>
                <section className="detail-card stack">
                  <div className="title">Provider credentials (client scoped)</div>
                  <div className="surface-subtitle">Each business gets isolated Twilio, CRM intake, and Zapier booking credentials. OpenAI is centralized globally.</div>
                  <div className="meta-text">Leave any field blank to keep the current saved value.</div>
                  <div className="form-grid-3">
                    <Field label="Twilio SID">
                      <input className="mono" disabled={form.clear_twilio_credentials} value={form.twilio_account_sid} onChange={(event) => setFormField(setForm, "twilio_account_sid", event.currentTarget.value)} placeholder="AC..." />
                    </Field>
                    <Field label="Twilio token">
                      <input className="mono" disabled={form.clear_twilio_credentials} type="password" autoComplete="off" value={form.twilio_auth_token} onChange={(event) => setFormField(setForm, "twilio_auth_token", event.currentTarget.value)} placeholder={detail?.provider_runtime?.twilio_configured ? "configured" : "token"} />
                    </Field>
                    <Field label="Twilio from number">
                      <input className="mono" disabled={form.clear_twilio_credentials} value={form.twilio_from_number} onChange={(event) => setFormField(setForm, "twilio_from_number", event.currentTarget.value)} placeholder="+15551234567" />
                    </Field>
                  </div>
                  <div className="form-grid-3">
                    <Field label="CRM intake webhook secret">
                      <input className="mono" disabled={form.clear_zapier_credentials} type="password" autoComplete="off" value={form.crm_webhook_secret} onChange={(event) => setFormField(setForm, "crm_webhook_secret", event.currentTarget.value)} placeholder={hasCrmWebhookSecret(detail) ? "configured" : "shared secret"} />
                    </Field>
                    <Field label="Zapier booking signing secret">
                      <input className="mono" disabled={form.clear_zapier_credentials} type="password" autoComplete="off" value={form.zapier_booking_webhook_secret} onChange={(event) => setFormField(setForm, "zapier_booking_webhook_secret", event.currentTarget.value)} placeholder={hasProviderValue(detail, "zapier_booking_webhook_secret") ? "configured" : "signing secret"} />
                    </Field>
                    <Field label="Zapier booking webhook URL">
                      <input className="mono" disabled={form.clear_zapier_credentials} type="password" autoComplete="off" value={form.zapier_booking_webhook_url} onChange={(event) => setFormField(setForm, "zapier_booking_webhook_url", event.currentTarget.value)} placeholder={hasProviderValue(detail, "zapier_booking_webhook_url") ? "configured" : "https://hooks.zapier.com/..."} />
                    </Field>
                  </div>
                  <div className="form-grid-3">
                    <Field label="Public base URL">
                      <input className="mono" value={form.public_base_url} onChange={(event) => setFormField(setForm, "public_base_url", event.currentTarget.value)} placeholder="https://example.ngrok-free.app" />
                    </Field>
                  </div>
                  {!creating ? (
                    <div className="stack compact-stack">
                      <label className="checkbox-inline">
                        <input
                          type="checkbox"
                          checked={form.clear_twilio_credentials}
                          onChange={(event) => {
                            const checked = event.currentTarget.checked;
                            setForm((current) => ({
                              ...current,
                              clear_twilio_credentials: checked,
                              ...(checked ? { twilio_account_sid: "", twilio_auth_token: "", twilio_from_number: "" } : {})
                            }));
                          }}
                        />
                        Remove this client's saved Twilio credentials on save
                      </label>
                      <label className="checkbox-inline">
                        <input
                          type="checkbox"
                          checked={form.clear_zapier_credentials}
                          onChange={(event) => {
                            const checked = event.currentTarget.checked;
                            setForm((current) => ({
                              ...current,
                              clear_zapier_credentials: checked,
                              ...(checked ? { crm_webhook_secret: "", zapier_booking_webhook_secret: "", zapier_booking_webhook_url: "" } : {})
                            }));
                          }}
                        />
                        Remove this client's saved CRM/Zapier webhook credentials on save
                      </label>
                      <div className="meta-text">A configured deployment-level Twilio fallback can still apply after client credentials are removed.</div>
                    </div>
                  ) : null}
                </section>
                <div className="form-grid-2">
                  <Field label="Handoff number">
                    <input className="mono" value={form.fallback_handoff_number} onChange={(event) => setFormField(setForm, "fallback_handoff_number", event.currentTarget.value)} placeholder="+15551234567" />
                  </Field>
                </div>
                <Field label="Consent text">
                  <input value={form.consent_text} onChange={(event) => setFormField(setForm, "consent_text", event.currentTarget.value)} />
                </Field>
              </>
            ) : null}

            {wizardStep === "agent" ? (
              <>
                <Field label="Qualification questions">
                  <textarea value={form.qualification_questions} onChange={(event) => setFormField(setForm, "qualification_questions", event.currentTarget.value)} />
                </Field>
                <Field label="FAQ / factual context">
                  <textarea value={form.faq_context} onChange={(event) => setFormField(setForm, "faq_context", event.currentTarget.value)} />
                </Field>
                <Field label="AI Context / Business Playbook">
                  <div className="meta-text">Define what you sell, differentiators, pricing rules, guarantees, process, do/don't say guidance, and preferred tone. This is the same per-client field shown in Settings.</div>
                  <textarea value={form.ai_context} onChange={(event) => setFormField(setForm, "ai_context", event.currentTarget.value)} />
                </Field>
                <Field label="Template overrides JSON">
                  <textarea className="mono" value={form.template_overrides} onChange={(event) => setFormField(setForm, "template_overrides", event.currentTarget.value)} />
                </Field>
              </>
            ) : null}

            {wizardStep === "booking" ? (
              <>
                <div className="form-grid-2">
                  <Field label="Booking URL">
                    <input className="mono" value={form.booking_url} onChange={(event) => setFormField(setForm, "booking_url", event.currentTarget.value)} placeholder="https://example.com/book" />
                  </Field>
                  <Field label="Booking mode">
                    <select value={form.booking_mode} onChange={(event) => setFormField(setForm, "booking_mode", event.currentTarget.value)}>
                      <option value="link">link_only</option>
                      <option value="internal">internal_calendar</option>
                      <option value="calendly">calendly_auto</option>
                    </select>
                  </Field>
                </div>
                <section className="detail-card stack">
                  <div className="title">Internal calendar</div>
                  <div className="surface-subtitle">Set weekly availability windows. The AI will offer these times and book directly into this app.</div>
                  <div className="form-grid-3">
                    <Field label="Meeting length (minutes)">
                      <input type="number" min="15" max="180" step="5" value={form.booking_calendar.slot_minutes} onChange={(event) => setClientCalendarField(setForm, "slot_minutes", Number(event.currentTarget.value))} />
                    </Field>
                    <Field label="Minimum notice (minutes)">
                      <input type="number" min="0" max="1440" step="5" value={form.booking_calendar.notice_minutes} onChange={(event) => setClientCalendarField(setForm, "notice_minutes", Number(event.currentTarget.value))} />
                    </Field>
                    <Field label="Scheduling horizon (days)">
                      <input type="number" min="1" max="60" step="1" value={form.booking_calendar.horizon_days} onChange={(event) => setClientCalendarField(setForm, "horizon_days", Number(event.currentTarget.value))} />
                    </Field>
                  </div>
                  <ClientAvailabilityEditor form={form} setForm={setForm} />
                </section>
              </>
            ) : null}

            {wizardStep === "portal" ? (
              <section className="detail-card stack">
                <div className="title">Client portal login</div>
                <div className="form-grid-3">
                  <Field label="Portal display name">
                    <input value={form.portal_display_name} onChange={(event) => setFormField(setForm, "portal_display_name", event.currentTarget.value)} placeholder="Display name" />
                  </Field>
                  <Field label="Portal email">
                    <input type="email" value={form.portal_email} onChange={(event) => setFormField(setForm, "portal_email", event.currentTarget.value)} placeholder="owner@example.com" />
                  </Field>
                  <Field label="Portal enabled">
                    <select value={String(form.portal_enabled)} onChange={(event) => setFormField(setForm, "portal_enabled", event.currentTarget.value === "true")}>
                      <option value="true">true</option>
                      <option value="false">false</option>
                    </select>
                  </Field>
                </div>
                <Field label="Portal password">
                  <input type="password" autoComplete="new-password" value={form.portal_password} onChange={(event) => setFormField(setForm, "portal_password", event.currentTarget.value)} placeholder={detail?.client.portal_password_configured ? "Leave blank to keep current password" : "Set portal password"} />
                </Field>
                <div className="meta-text">
                  {detail?.client.portal_password_configured ? "Password is configured. Set a new value to rotate it." : "No client portal password configured yet."}
                </div>
              </section>
            ) : null}

            {wizardStep === "review" ? <ClientWizardReview form={form} detail={detail} /> : null}
            <div className="actions">
              <button className="primary" type="submit">Save client</button>
              <button className="ghost" type="button" onClick={() => setForm(detail ? formFromDetail(detail) : emptyClientForm())}>Reset form</button>
            </div>
          </form>

          <div className={`tab-panel ${clientTab === "webhooks" ? "active" : ""}`} role="tabpanel">
            {creating ? (
              <div className="empty-state">Client-specific webhooks appear after creation.</div>
            ) : detail ? (
              <section className="detail-card stack">
                <div className="title">Provider webhooks</div>
                <WebhookRows rows={activeWebhookRows(detail.webhook_urls)} />
              </section>
            ) : (
              <div className="empty-state">Select a client from the left pane.</div>
            )}
          </div>
        </div>
      </section>
      </div>
    </div>
  );
}

function ClientAvailabilityEditor({ form, setForm }: { form: ClientFormState; setForm: Dispatch<SetStateAction<ClientFormState>> }) {
  return (
    <div className="calendar-availability-grid">
      {form.booking_calendar.availability.map((row, index) => (
        <div className="calendar-row" key={row.day}>
          <label className="checkbox-inline">
            <input
              type="checkbox"
              checked={row.enabled}
              onChange={(event) => setForm((current) => ({
                ...current,
                booking_calendar: updateCalendarRow(current.booking_calendar, index, { enabled: event.currentTarget.checked })
              }))}
            />
            {dayName(row.day)}
          </label>
          <input
            type="time"
            value={row.start}
            onChange={(event) => setForm((current) => ({
              ...current,
              booking_calendar: updateCalendarRow(current.booking_calendar, index, { start: event.currentTarget.value })
            }))}
          />
          <input
            type="time"
            value={row.end}
            onChange={(event) => setForm((current) => ({
              ...current,
              booking_calendar: updateCalendarRow(current.booking_calendar, index, { end: event.currentTarget.value })
            }))}
          />
        </div>
      ))}
    </div>
  );
}

function ClientWizardReview({ form, detail }: { form: ClientFormState; detail: ClientDetailPayload | null }) {
  const enabledDays = form.booking_calendar.availability.filter((row) => row.enabled).length;
  const questions = form.qualification_questions.split(/\n+/).map((item) => item.trim()).filter(Boolean);
  const cards = [
    {
      title: "Business",
      rows: [
        ["Name", form.business_name || "-"],
        ["Key", form.client_key || "-"],
        ["Language", form.language || "en"],
        ["Timezone", form.timezone || "-"],
        ["Active", String(form.is_active)]
      ]
    },
    {
      title: "Channels",
      rows: [
        ["Twilio", statusLabel(form.twilio_account_sid || detail?.provider_runtime?.twilio_configured)],
        ["CRM intake", statusLabel(form.crm_webhook_secret || hasCrmWebhookSecret(detail))],
        ["Zapier signing", statusLabel(form.zapier_booking_webhook_secret || hasProviderValue(detail, "zapier_booking_webhook_secret"))],
        ["Zapier booking URL", statusLabel(form.zapier_booking_webhook_url || hasProviderValue(detail, "zapier_booking_webhook_url"))],
        ["Handoff", statusLabel(form.fallback_handoff_number)]
      ]
    },
    {
      title: "Agent",
      rows: [
        ["Tone", form.tone || "-"],
        ["Questions", `${questions.length}`],
        ["Playbook", form.ai_context ? `${form.ai_context.length} chars` : "missing"],
        ["FAQ", statusLabel(form.faq_context)]
      ]
    },
    {
      title: "Booking",
      rows: [
        ["Mode", form.booking_mode || "-"],
        ["Booking URL", statusLabel(form.booking_url)],
        ["Meeting length", `${form.booking_calendar.slot_minutes} min`],
        ["Available days", `${enabledDays}`]
      ]
    },
    {
      title: "Portal",
      rows: [
        ["Enabled", String(form.portal_enabled)],
        ["Email", form.portal_email || "-"],
        ["Display name", form.portal_display_name || "-"],
        ["Password", statusLabel(form.portal_password || detail?.client.portal_password_configured)]
      ]
    }
  ];

  return (
    <div className="client-wizard-review">
      <div className="client-wizard-review-grid">
        {cards.map((card) => (
          <section className="client-wizard-review-card" key={card.title}>
            <div className="client-wizard-review-title">{card.title}</div>
            {card.rows.map(([label, value]) => (
              <div className="client-wizard-review-row" key={label}>
                <span>{label}</span>
                <strong>{value}</strong>
              </div>
            ))}
          </section>
        ))}
      </div>
    </div>
  );
}

export function LogsPage({ onReadyChange }: AdminPageProps) {
  const auth = useAuth();
  const [status, setStatus] = useState<AsyncStatus>("idle");
  const [clients, setClients] = useState<ClientSummary[]>([]);
  const [clientKey, setClientKey] = useState("");
  const [logs, setLogs] = useState<AuditLogItem[]>([]);
  const [error, setError] = useState("");
  const [retryVersion, setRetryVersion] = useState(0);

  useEffect(() => {
    if (auth.status !== "ready") {
      onReadyChange?.(false);
      return;
    }
    if (auth.session.role !== "admin") {
      setStatus("ready");
      onReadyChange?.(true);
      return;
    }
    let cancelled = false;
    setStatus("loading");
    setError("");
    onReadyChange?.(false);
    fetchClients()
      .then((items) => {
        if (cancelled) return;
        setClients(items);
        const selected = chooseClientKey(items);
        setClientKey(selected);
        if (!selected) {
          setStatus("ready");
          onReadyChange?.(true);
        }
      })
      .catch((caught: unknown) => {
        if (cancelled) return;
        setError(messageFor(caught, "Logs unavailable"));
        setStatus("error");
        onReadyChange?.(false);
      });
    return () => {
      cancelled = true;
    };
  }, [auth.status, retryVersion]);

  useEffect(() => {
    if (auth.status !== "ready" || auth.session.role !== "admin" || !clientKey) return;
    let cancelled = false;
    setStatus("loading");
    setError("");
    onReadyChange?.(false);
    rememberClientKey(clientKey);
    fetchAuditLogs(clientKey, 100)
      .then((items) => {
        if (cancelled) return;
        setLogs(items);
        setStatus("ready");
        onReadyChange?.(true);
      })
      .catch((caught: unknown) => {
        if (cancelled) return;
        setError(messageFor(caught, "Audit logs unavailable"));
        setStatus("error");
        onReadyChange?.(false);
      });
    return () => {
      cancelled = true;
    };
  }, [auth.status, clientKey, retryVersion]);

  if (auth.status === "ready" && auth.session.role !== "admin") {
    return <AdminOnlyPage title="Logs" onReadyChange={onReadyChange} />;
  }
  if (status !== "ready") {
    return <AdminLoadState title="Logs" status={status} error={error} onRetry={() => setRetryVersion((current) => current + 1)} />;
  }

  return (
    <div className="react-admin-page react-logs-page" data-testid="react-logs-page">
      <section className="surface">
        <div className="surface-header">
          <div className="surface-title">
            <div>
              <h2>Logs</h2>
              <div className="surface-subtitle">Selected-client audit stream from backend-owned events.</div>
            </div>
          </div>
          {clients.length ? (
            <select value={clientKey} onChange={(event) => setClientKey(event.currentTarget.value)}>
              {clients.map((client) => (
                <option key={client.client_key} value={client.client_key}>{client.business_name}</option>
              ))}
            </select>
          ) : null}
        </div>
      </section>
      <section className="surface stack focus-surface react-table-surface">
        {error ? <div className="empty-state">{error}</div> : null}
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Time</th>
                <th>Event</th>
                <th>Record</th>
                <th>Decision</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {logs.length ? logs.map((log) => (
                <tr key={log.id}>
                  <td data-label="Time">{formatDateTime(log.created_at)}</td>
                  <td data-label="Event">{formatEvent(log.event_type)}</td>
                  <td data-label="Record">{log.lead_id || "-"}</td>
                  <td data-label="Decision"><span className="mono">{shortJson(log.decision || {})}</span></td>
                  <td>{log.lead_id ? <button className="small ghost" type="button" data-action="open-thread" data-lead-id={log.lead_id}>Open</button> : null}</td>
                </tr>
              )) : (
                <tr><td colSpan={5}>No audit rows yet.</td></tr>
              )}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}

export function SettingsPage({ onReadyChange }: AdminPageProps) {
  const auth = useAuth();
  const [status, setStatus] = useState<AsyncStatus>("idle");
  const [error, setError] = useState("");
  const [clients, setClients] = useState<ClientSummary[]>([]);
  const [clientKey, setClientKey] = useState("");
  const [runtime, setRuntime] = useState<RuntimeConfigStatus | null>(null);
  const [workspace, setWorkspace] = useState<OwnerWorkspacePayload | null>(null);
  const [clientDetail, setClientDetail] = useState<ClientDetailPayload | null>(null);
  const [aiContext, setAiContext] = useState("");
  const [faqContext, setFaqContext] = useState("");
  const [openAiKey, setOpenAiKey] = useState("");
  const [openAiModel, setOpenAiModel] = useState("gpt-5.4-mini");
  const [calendar, setCalendar] = useState<OwnerCalendarConfig>(DEFAULT_CALENDAR);
  const [knowledgeUrls, setKnowledgeUrls] = useState("");
  const [actionStatus, setActionStatus] = useState("");
  const [retryVersion, setRetryVersion] = useState(0);

  const isAdmin = auth.status === "ready" && auth.session.role === "admin";

  useEffect(() => {
    if (auth.status !== "ready") {
      onReadyChange?.(false);
      return;
    }
    let cancelled = false;
    setStatus("loading");
    setError("");
    onReadyChange?.(false);
    const baseLoad = auth.session.role === "client"
      ? Promise.resolve({ clients: [] as ClientSummary[], key: auth.session.client_key || "", runtime: null })
      : Promise.all([fetchClients(), fetchRuntimeStatus()]).then(([items, runtimeStatus]) => ({ clients: items, key: chooseClientKey(items), runtime: runtimeStatus }));

    baseLoad
      .then((result) => {
        if (cancelled) return;
        setClients(result.clients);
        setRuntime(result.runtime);
        setOpenAiModel(result.runtime?.openai_model || "gpt-5.4-mini");
        setClientKey(result.key);
        if (!result.key) {
          setStatus("ready");
          onReadyChange?.(true);
        }
      })
      .catch((caught: unknown) => {
        if (cancelled) return;
        setError(messageFor(caught, "Settings unavailable"));
        setStatus("error");
        onReadyChange?.(false);
      });
    return () => {
      cancelled = true;
    };
  }, [auth.status, retryVersion]);

  useEffect(() => {
    if (auth.status !== "ready" || !clientKey) return;
    let cancelled = false;
    setStatus("loading");
    setError("");
    onReadyChange?.(false);
    rememberClientKey(clientKey);
    Promise.all([
      fetchOwnerWorkspace(clientKey),
      auth.session.role === "admin" ? fetchClientDetail(clientKey) : Promise.resolve(null)
    ])
      .then(([ownerWorkspace, detail]) => {
        if (cancelled) return;
        setWorkspace(ownerWorkspace);
        setClientDetail(detail);
        setAiContext(ownerWorkspace.client.ai_context || "");
        setFaqContext(ownerWorkspace.client.faq_context || "");
        setKnowledgeUrls((ownerWorkspace.knowledge.sources || []).map((source) => source.url).filter(Boolean).join("\n"));
        setCalendar(calendarFromClient(ownerWorkspace.client.booking_config));
        setActionStatus("");
        setStatus("ready");
        onReadyChange?.(true);
      })
      .catch((caught: unknown) => {
        if (cancelled) return;
        setError(messageFor(caught, "Workspace settings unavailable"));
        setStatus("error");
        onReadyChange?.(false);
      });
    return () => {
      cancelled = true;
    };
  }, [auth.status, clientKey, retryVersion]);

  if (status !== "ready" && !workspace) {
    return <AdminLoadState title="Settings" status={status} error={error} onRetry={() => setRetryVersion((current) => current + 1)} />;
  }

  async function saveRuntime(event: FormEvent) {
    event.preventDefault();
    setActionStatus("Saving AI provider...");
    try {
      const payload: { openai_api_key?: string; openai_model?: string } = {
        openai_model: openAiModel.trim() || "gpt-5.4-mini"
      };
      if (openAiKey.trim()) payload.openai_api_key = openAiKey.trim();
      const result = await updateRuntimeConfig(payload);
      const nextRuntime = await fetchRuntimeStatus();
      setRuntime(nextRuntime);
      setOpenAiKey("");
      setActionStatus(`Updated ${result.updated_keys.join(", ") || "settings"}.`);
    } catch (caught: unknown) {
      setActionStatus(messageFor(caught, "AI provider save failed"));
    }
  }

  async function saveAiContext(event: FormEvent) {
    event.preventDefault();
    if (!clientKey) return;
    setActionStatus("Saving assistant guidance...");
    try {
      const result = await updateOwnerAiContext(clientKey, { ai_context: aiContext, faq_context: faqContext });
      setAiContext(result.ai_context || "");
      setFaqContext(result.faq_context || "");
      setActionStatus(`Saved at ${formatDateTime(result.updated_at)}.`);
    } catch (caught: unknown) {
      setActionStatus(messageFor(caught, "AI context save failed"));
    }
  }

  async function ingestKnowledge(event: FormEvent) {
    event.preventDefault();
    if (!clientKey) return;
    const urls = knowledgeUrls.split(/\n+/).map((line) => line.trim()).filter(Boolean);
    if (!urls.length) {
      setActionStatus("Add at least one URL.");
      return;
    }
    setActionStatus("Fetching and extracting website text...");
    try {
      const result = await ingestOwnerKnowledge(clientKey, { urls, replace: true });
      setWorkspace((current) => current ? { ...current, knowledge: result } : current);
      if (result.status === "queued") {
        setActionStatus(`Queued ${urls.length} source${urls.length === 1 ? "" : "s"}. Use Refresh extraction to see progress.`);
      } else {
        setActionStatus(`Ingested ${result.total_sources || urls.length} sources and ${result.total_chunks || 0} chunks.`);
      }
    } catch (caught: unknown) {
      setActionStatus(messageFor(caught, "Knowledge ingest failed"));
    }
  }

  async function refreshKnowledge() {
    if (!clientKey) return;
    setActionStatus("Refreshing extracted knowledge...");
    try {
      const result = await fetchOwnerKnowledge(clientKey);
      setWorkspace((current) => current ? { ...current, knowledge: result } : current);
      setKnowledgeUrls((result.sources || []).map((source) => source.url).filter(Boolean).join("\n"));
      setActionStatus(`Loaded ${result.total_sources || 0} sources.`);
    } catch (caught: unknown) {
      setActionStatus(messageFor(caught, "Knowledge refresh failed"));
    }
  }

  async function saveCalendar(event: FormEvent) {
    event.preventDefault();
    if (!clientKey) return;
    setActionStatus("Saving booking availability...");
    try {
      const result = await updateOwnerCalendar(clientKey, calendar);
      setCalendar(result.internal_calendar);
      setActionStatus(`Saved at ${formatDateTime(result.updated_at)}.`);
    } catch (caught: unknown) {
      setActionStatus(messageFor(caught, "Calendar save failed"));
    }
  }

  async function refreshAfterDemoMutation() {
    try {
      const items = await fetchClients();
      setClients(items);
      const nextClientKey = items.some((client) => client.client_key === clientKey)
        ? clientKey
        : chooseClientKey(items);
      if (nextClientKey !== clientKey) {
        setWorkspace(null);
        setClientDetail(null);
        setClientKey(nextClientKey);
        if (!nextClientKey) {
          setStatus("ready");
          onReadyChange?.(true);
        }
        return;
      }
      if (!nextClientKey) return;
      const [ownerWorkspace, detail] = await Promise.all([
        fetchOwnerWorkspace(nextClientKey),
        fetchClientDetail(nextClientKey)
      ]);
      setWorkspace(ownerWorkspace);
      setClientDetail(detail);
    } catch (caught: unknown) {
      setActionStatus(`Demo data changed, but the settings refresh failed: ${messageFor(caught, "refresh unavailable")}`);
    }
  }

  const checklist = activeChecklist(workspace?.live_test_checklist || []);
  const webhookRows = activeWebhookRows(clientDetail?.webhook_urls || (workspace?.client.client_key ? defaultWebhookRows(workspace.client.client_key) : {}));

  return (
    <div className="react-admin-page react-settings-page" data-testid="react-settings-page">
      <section className="surface">
        <div className="surface-header">
          <div className="surface-title">
            <div>
              <h2>Settings</h2>
              <div className="surface-subtitle">Provider defaults, business guidance, knowledge, and booking availability.</div>
            </div>
          </div>
          {clients.length ? (
            <select value={clientKey} onChange={(event) => setClientKey(event.currentTarget.value)}>
              {clients.map((client) => (
                <option key={client.client_key} value={client.client_key}>{client.business_name}</option>
              ))}
            </select>
          ) : null}
        </div>
      </section>

      {error ? <AdminInlineError message={error} onRetry={() => setRetryVersion((current) => current + 1)} /> : null}
      <SettingsReadinessStrip isAdmin={isAdmin} runtime={runtime} workspace={workspace} />
      {actionStatus ? <div className="meta-text" role="status">{actionStatus}</div> : null}
      <div className={`settings-layout ${isAdmin ? "" : "single-column"}`}>
        {isAdmin ? (
          <form className="surface stack settings-card" onSubmit={(event) => void saveRuntime(event)}>
            <div className="surface-title">
              <div>
                <h3>Global AI provider</h3>
                <div className="surface-subtitle">Central OpenAI configuration. Saved API keys are not exposed in React; paste a new key only when rotating.</div>
              </div>
            </div>
            <div className="form-grid-3">
              <Field label="OpenAI key">
                <input className="mono" type="password" autoComplete="off" value={openAiKey} onChange={(event) => setOpenAiKey(event.currentTarget.value)} placeholder={runtime?.openai_api_key_configured ? "Configured; leave blank to keep current key" : "sk-..."} />
              </Field>
              <Field label="OpenAI model">
                <input value={openAiModel} onChange={(event) => setOpenAiModel(event.currentTarget.value)} />
              </Field>
              <Field label="AI mode">
                <input value={runtime?.ai_provider_mode || "Not reported"} disabled />
              </Field>
            </div>
            <div className="actions">
              <button className="primary" type="submit">Save AI settings</button>
              {renderBadge(runtime?.openai_api_key_configured ? "OpenAI configured" : "OpenAI missing", runtime?.openai_api_key_configured ? "ok" : "warn")}
            </div>
          </form>
        ) : null}

        <div className="settings-column">
          <section className="surface stack settings-card settings-guide-card">
            <div className="surface-title">
              <div>
                <h3>{workspace?.client.business_name || "Workspace"} setup</h3>
                <div className="surface-subtitle">The pieces that affect live conversations and bookings.</div>
              </div>
            </div>
            <Checklist items={checklist} />
          </section>

          {isAdmin ? (
            <section className="surface stack settings-card">
              <div className="surface-title">
                <div>
                  <h3>Selected-client webhooks</h3>
                  <div className="surface-subtitle">Copy these into provider dashboards. Public base URL is managed in Clients.</div>
                </div>
              </div>
              <WebhookRows rows={webhookRows} />
            </section>
          ) : null}

          <form className="surface stack settings-card" onSubmit={(event) => void saveAiContext(event)}>
            <div className="surface-title">
              <div>
                <h3>{isAdmin ? "AI Context / Business Playbook" : "AI Assistant Guidance"}</h3>
                <div className="surface-subtitle">Business-specific wording belongs here, not in backend agent code.</div>
              </div>
            </div>
            <div className="settings-ai-context-grid">
              <Field label="AI context">
                <textarea value={aiContext} onChange={(event) => setAiContext(event.currentTarget.value)} placeholder="Offer, tone, claims to avoid, qualifying approach, escalation rules." />
              </Field>
              <Field label="FAQ / factual context">
                <textarea value={faqContext} onChange={(event) => setFaqContext(event.currentTarget.value)} placeholder="Services, process, pricing ranges if allowed, policies, service areas." />
              </Field>
            </div>
            <div className="actions">
              <button className="primary" type="submit">{isAdmin ? "Save AI context" : "Save assistant guidance"}</button>
            </div>
          </form>

          <form className="surface stack settings-card" onSubmit={(event) => void ingestKnowledge(event)}>
            <div className="surface-title">
              <div>
                <h3>Website knowledge</h3>
                <div className="surface-subtitle">Add URLs the assistant can use as factual context.</div>
              </div>
            </div>
            <Field label="Source URLs">
              <textarea className="mono" value={knowledgeUrls} onChange={(event) => setKnowledgeUrls(event.currentTarget.value)} placeholder={"https://example.com/services\nhttps://example.com/pricing"} />
            </Field>
            <div className="actions">
              <button className="primary" type="submit">Ingest URLs</button>
              <button className="ghost" type="button" onClick={() => void refreshKnowledge()}>Refresh extraction</button>
            </div>
            <KnowledgeSummary knowledge={workspace?.knowledge || null} />
          </form>

          <form className="surface stack settings-card settings-calendar-card" onSubmit={(event) => void saveCalendar(event)}>
            <div className="surface-title">
              <div>
                <h3>{isAdmin ? "Internal calendar availability" : "Booking availability"}</h3>
                <div className="surface-subtitle">AI uses these windows when offering meeting times.</div>
              </div>
            </div>
            <div className="form-grid-3 settings-calendar-controls">
              <Field label="Meeting length">
                <input type="number" min={15} max={180} step={5} value={calendar.slot_minutes} onChange={(event) => setCalendarField(setCalendar, "slot_minutes", Number(event.currentTarget.value))} />
              </Field>
              <Field label="Minimum notice">
                <input type="number" min={0} max={1440} step={5} value={calendar.notice_minutes} onChange={(event) => setCalendarField(setCalendar, "notice_minutes", Number(event.currentTarget.value))} />
              </Field>
              <Field label="Horizon days">
                <input type="number" min={1} max={60} step={1} value={calendar.horizon_days} onChange={(event) => setCalendarField(setCalendar, "horizon_days", Number(event.currentTarget.value))} />
              </Field>
            </div>
            <AvailabilityEditor calendar={calendar} setCalendar={setCalendar} />
            <div className="settings-calendar-footer">
              <button className="primary" type="submit">Save calendar availability</button>
            </div>
          </form>

          {isAdmin ? (
            <DemoDataPanel
              canSeed={auth.session.can_seed_demo}
              demoDataPresent={auth.session.demo_data_present}
              clients={clients}
              selectedClientKey={clientKey}
              onChanged={refreshAfterDemoMutation}
            />
          ) : null}

        </div>
      </div>
    </div>
  );
}

export function TestLabPage({ onReadyChange }: AdminPageProps) {
  const auth = useAuth();
  const [status, setStatus] = useState<AsyncStatus>("idle");
  const [clients, setClients] = useState<ClientSummary[]>([]);
  const [clientKey, setClientKey] = useState("");
  const [workspace, setWorkspace] = useState<OwnerWorkspacePayload | null>(null);
  const [mode, setMode] = useState("gpt_only");
  const [fullName, setFullName] = useState("Strategy Call Contact");
  const [phone, setPhone] = useState("");
  const [email, setEmail] = useState("contact@example.com");
  const [city, setCity] = useState("Toronto");
  const [answers, setAnswers] = useState(DEFAULT_TEST_ANSWERS);
  const [result, setResult] = useState<SandboxStartResponse | null>(null);
  const [labStatus, setLabStatus] = useState("");
  const [labFailed, setLabFailed] = useState(false);
  const [sandboxBusy, setSandboxBusy] = useState(false);
  const [retryVersion, setRetryVersion] = useState(0);

  useEffect(() => {
    if (auth.status !== "ready") {
      onReadyChange?.(false);
      return;
    }
    if (auth.session.role !== "admin") {
      setStatus("ready");
      onReadyChange?.(true);
      return;
    }
    let cancelled = false;
    setStatus("loading");
    onReadyChange?.(false);
    fetchClients()
      .then((items) => {
        if (cancelled) return;
        setClients(items);
        const selected = chooseClientKey(items);
        setClientKey(selected);
        if (!selected) {
          setStatus("ready");
          onReadyChange?.(true);
        }
      })
      .catch((caught: unknown) => {
        if (cancelled) return;
        setLabStatus(messageFor(caught, "Test Lab unavailable"));
        setStatus("error");
        onReadyChange?.(false);
      });
    return () => {
      cancelled = true;
    };
  }, [auth.status, retryVersion]);

  useEffect(() => {
    if (auth.status !== "ready" || auth.session.role !== "admin" || !clientKey) return;
    let cancelled = false;
    setStatus("loading");
    setLabStatus("");
    onReadyChange?.(false);
    rememberClientKey(clientKey);
    fetchOwnerWorkspace(clientKey)
      .then((payload) => {
        if (cancelled) return;
        setWorkspace(payload);
        setStatus("ready");
        onReadyChange?.(true);
      })
      .catch((caught: unknown) => {
        if (cancelled) return;
        setLabStatus(messageFor(caught, "Client sandbox context unavailable"));
        setStatus("error");
        onReadyChange?.(false);
      });
    return () => {
      cancelled = true;
    };
  }, [auth.status, clientKey, retryVersion]);

  if (auth.status === "ready" && auth.session.role !== "admin") {
    return <AdminOnlyPage title="Test Lab" onReadyChange={onReadyChange} />;
  }
  if (status !== "ready" && clients.length === 0) {
    return <AdminLoadState title="Test Lab" status={status} error={labStatus} onRetry={() => setRetryVersion((current) => current + 1)} />;
  }

  async function submit() {
    if (!clientKey) {
      setLabFailed(true);
      setLabStatus("Select a client first.");
      return;
    }
    if (!["gpt_only", "gpt_zapier"].includes(mode)) {
      setLabFailed(true);
      setLabStatus("Choose a supported GPT sandbox mode.");
      return;
    }
    const formAnswers = answers.map((row) => ({ question: row.question.trim(), answer: row.answer.trim() })).filter((row) => row.question && row.answer);
    if (!formAnswers.length) {
      setLabFailed(true);
      setLabStatus("Add at least one form question and answer.");
      return;
    }
    setLabFailed(false);
    setSandboxBusy(true);
    setLabStatus("Creating the test contact and asking the agent for the first reply...");
    try {
      const payload = await startSandbox(clientKey, {
        mode,
        full_name: fullName,
        phone,
        email,
        city,
        form_answers: formAnswers
      });
      setResult(payload);
      window.localStorage.setItem("lead-ui-active-lead", String(payload.lead_id));
      setLabStatus(`Sandbox started. Contact ${payload.lead_id} is ready in Conversations.`);
    } catch (caught: unknown) {
      setLabFailed(true);
      setLabStatus(messageFor(caught, "Sandbox failed"));
    } finally {
      setSandboxBusy(false);
    }
  }

  return (
    <div className="react-admin-page react-test-lab-page">
    <div className="test-lab-shell" data-testid="react-test-lab-page">
      <div className="test-lab-main">
        <section className="surface stack test-lab-section">
          <StepTitle step="1" title="Choose client" detail="The sandbox contact uses this client's AI training, runtime settings, and business context." />
          <Field label="Client">
            <select value={clientKey} disabled={sandboxBusy} onChange={(event) => {
              setWorkspace(null);
              setResult(null);
              setLabStatus("");
              setClientKey(event.currentTarget.value);
            }}>
              {clients.map((client) => (
                <option key={client.client_key} value={client.client_key}>{client.business_name}</option>
              ))}
            </select>
          </Field>
          <div className="test-lab-summary">
            <StatCard label="Client" value={workspace?.client.business_name || "-"} />
            <StatCard label="AI" value={workspace?.runtime.ai_configured ? "Configured" : "Mock/offline"} />
            <StatCard label="Booking" value={formatBookingMode(workspace?.client.booking_mode || "link")} />
          </div>
        </section>

        <section className="surface stack test-lab-section">
          <StepTitle step="2" title="Customize lead details and form answers" detail="Use realistic context so the strategy-call demo mirrors a real inbound lead." />
          <div className="form-grid-2">
            <Field label="Contact name"><input value={fullName} onChange={(event) => setFullName(event.currentTarget.value)} /></Field>
            <Field label="Phone"><input className="mono" value={phone} onChange={(event) => setPhone(event.currentTarget.value)} placeholder="+15551234567" /></Field>
          </div>
          <div className="form-grid-2">
            <Field label="Email"><input value={email} onChange={(event) => setEmail(event.currentTarget.value)} /></Field>
            <Field label="City"><input value={city} onChange={(event) => setCity(event.currentTarget.value)} /></Field>
          </div>
          <div className="item-title-row">
            <label style={{ margin: 0 }}>Form questions and answers</label>
            <button className="small ghost" type="button" onClick={() => setAnswers((current) => [...current, { question: "", answer: "" }])}>Add question</button>
          </div>
          <div className="test-lab-answer-list">
            {answers.map((answer, index) => (
              <div className="test-lab-answer-row" key={index}>
                <Field label="Question">
                  <input value={answer.question} onChange={(event) => setAnswers((current) => updateAnswer(current, index, "question", event.currentTarget.value))} />
                </Field>
                <Field label="Answer">
                  <input value={answer.answer} onChange={(event) => setAnswers((current) => updateAnswer(current, index, "answer", event.currentTarget.value))} />
                </Field>
                <button className="small ghost" type="button" onClick={() => setAnswers((current) => current.filter((_, rowIndex) => rowIndex !== index))}>Remove</button>
              </div>
            ))}
          </div>
        </section>
        <ProviderProbePanel clientKey={clientKey} workspace={workspace} />
      </div>

      <div className="test-lab-side">
        <section className="surface stack test-lab-section">
          <StepTitle step="3" title="Sandbox configuration" detail="Run GPT alone, or exercise the booking Zapier webhook while keeping Twilio out of the sandbox." />
          <div className="test-lab-mode-grid">
            <ModeCard active={mode === "gpt_only"} disabled={sandboxBusy} kicker="Ready now" title="GPT only" detail="Creates the sandbox contact and lets you answer inside the app." onClick={() => { setMode("gpt_only"); setResult(null); }} />
            <ModeCard active={mode === "gpt_zapier"} disabled={sandboxBusy} kicker="Ready now" title="GPT + Zapier" detail="Bypasses Twilio and posts the booking payload when this sandbox lead books." onClick={() => { setMode("gpt_zapier"); setResult(null); }} />
          </div>
        </section>
        <section className="surface stack test-lab-start-card">
          <StepTitle step="4" title="Start sandbox" detail="Creates the lead and initiates the agent conversation." />
          <button className="primary" type="button" disabled={!clientKey || status === "loading" || sandboxBusy} onClick={() => void submit()}>{sandboxBusy ? "Starting sandbox..." : mode === "gpt_zapier" ? "Start GPT + Zapier sandbox" : "Start GPT sandbox"}</button>
          {labStatus ? <div className="meta-text" role={labFailed ? "alert" : "status"}>{labStatus}</div> : null}
          {result ? (
            <div className="actions">
              <button className="small ghost" type="button" data-action="open-thread" data-lead-id={result.lead_id}>Open thread</button>
              <span className="badge">Contact {result.lead_id}</span>
            </div>
          ) : null}
        </section>
        <section className="surface stack test-lab-start-card">
          <StepTitle step="5" title="Sandbox debug output" detail="Shows the agent response, booking planner data, and booking-webhook status returned by the sandbox." />
          {result ? <pre className="code-output">{JSON.stringify({
            mode: result.mode,
            state: result.state,
            body: result.body,
            booking_debug: result.booking_debug || null,
            zapier_booking_webhook: result.zapier_booking_webhook || null
          }, null, 2)}</pre> : <div className="empty-state">Start a sandbox to inspect its result.</div>}
        </section>
        <IntakeProbePanel
          clientKey={clientKey}
          webhookSecretConfigured={Boolean(
            workspace?.runtime.crm_webhook_secret_configured
            || workspace?.runtime.zapier_webhook_secret_configured
          )}
        />
        <AutomationDiagnosticsPanel clientKey={clientKey} />
      </div>
    </div>
    </div>
  );
}

function AdminOnlyPage({ title, onReadyChange }: { title: string; onReadyChange?: (ready: boolean) => void }) {
  useEffect(() => onReadyChange?.(true), [onReadyChange]);
  return (
    <section className="surface stack react-admin-page" data-testid="react-admin-only-page">
      <div className="surface-title">
        <div>
          <h2>{title}</h2>
          <div className="surface-subtitle">This workspace is available to admins only.</div>
        </div>
      </div>
    </section>
  );
}

function AdminLoadState({
  title,
  status,
  error,
  onRetry
}: {
  title: string;
  status: AsyncStatus;
  error: string;
  onRetry: () => void;
}) {
  const failed = status === "error";
  return (
    <div className="react-admin-page">
      <section className="surface stack" aria-live="polite">
        <h2>{title}</h2>
        <div className="empty-state">
          <div>{failed ? error || `${title} unavailable` : `Loading ${title.toLowerCase()}...`}</div>
          {failed ? <button className="small" type="button" onClick={onRetry}>Retry</button> : null}
        </div>
      </section>
    </div>
  );
}

function AdminInlineError({ message, onRetry }: { message: string; onRetry: () => void }) {
  return (
    <div className="empty-state compact" role="alert">
      <span>{message}</span>{" "}
      <button className="small ghost" type="button" onClick={onRetry}>Retry</button>
    </div>
  );
}

function Checklist({ items }: { items: ChecklistItem[] }) {
  if (!items.length) return <div className="empty-state">No readiness checks available yet.</div>;
  return (
    <div className="compact-list settings-step-list">
      {items.map((item) => (
        <div className="check-item" key={`${item.label}-${item.detail || ""}`}>
          <div className="item-title-row">
            <div>
              <div className="item-title">{item.label}</div>
              {item.detail ? <div className="meta-text">{item.detail}</div> : null}
            </div>
            {renderBadge(item.done ? "ready" : "needs setup", item.done ? "ok" : "warn")}
          </div>
        </div>
      ))}
    </div>
  );
}

function KnowledgeSummary({ knowledge }: { knowledge: KnowledgePayload | null }) {
  const sources = knowledge?.sources || [];
  if (!sources.length) return <div className="empty-state">No website knowledge sources yet.</div>;
  return (
    <div className="compact-list">
      {sources.map((source, index) => (
        <div className="preview-item" key={`${source.url || source.normalized_url || index}`}>
          <div className="item-title-row">
            <div className="item-title">{source.title || source.normalized_url || source.url || "Knowledge source"}</div>
            {renderBadge(source.status || "pending", source.status === "ok" ? "ok" : "warn")}
          </div>
          <div className="item-subtitle mono">{source.url}</div>
          <div className="item-snippet">{source.status === "ok" ? `${source.chunk_count || 0} chunks · ${formatDateTime(source.last_crawled_at || "")}` : source.error_message || "Waiting for extraction."}</div>
        </div>
      ))}
    </div>
  );
}

function SettingsReadinessStrip({ isAdmin, runtime, workspace }: { isAdmin: boolean; runtime: RuntimeConfigStatus | null; workspace: OwnerWorkspacePayload | null }) {
  if (!workspace && !runtime) return null;
  const twilioConfigured = Boolean(workspace?.runtime.twilio_configured);
  const aiConfigured = Boolean(workspace?.runtime.ai_configured || runtime?.openai_api_key_configured);
  const bookingReady = Boolean(workspace?.client.booking_mode === "internal" || workspace?.client.booking_url);
  return (
    <section className="surface settings-readiness-strip" aria-label="Settings readiness">
      <StatCard label="Twilio" value={twilioConfigured ? "configured" : "needs setup"} />
      <StatCard label="AI" value={aiConfigured ? workspace?.runtime.openai_model || runtime?.openai_model || "configured" : "needs setup"} />
      <StatCard label="Automated booking" value={bookingReady ? formatBookingMode(workspace?.client.booking_mode || "link") : "needs setup"} />
      <StatCard label={isAdmin ? "Runtime source" : "Workspace"} value={isAdmin ? workspace?.runtime.source || "global" : workspace?.client.business_name || "-"} />
    </section>
  );
}

function ConversationPreviewList({ items }: { items: NonNullable<ClientDetailPayload["recent_conversations"]> }) {
  if (!items.length) return <div className="empty-state">No recent conversations.</div>;
  return (
    <div className="compact-list">
      {items.slice(0, 8).map((item) => (
        <div className="preview-item" key={item.lead_id}>
          <div className="item-title-row">
            <div>
              <div className="item-title">{item.lead_name || item.phone || `Contact ${item.lead_id}`}</div>
              <div className="item-snippet">{item.last_message_snippet || "No recent message."}</div>
            </div>
            <button className="small ghost" type="button" data-action="open-thread" data-lead-id={item.lead_id}>Open</button>
          </div>
          <div className="lead-list-meta">
            <span>{item.client_name || formatSource(item.source || "")}</span>
            <span>{formatDateTime(item.last_activity_at)}</span>
          </div>
        </div>
      ))}
    </div>
  );
}

function AvailabilityEditor({ calendar, setCalendar }: { calendar: OwnerCalendarConfig; setCalendar: Dispatch<SetStateAction<OwnerCalendarConfig>> }) {
  return (
    <div className="calendar-availability-grid settings-calendar-availability">
      {calendar.availability.map((row, index) => (
        <div className={`calendar-row settings-calendar-day ${row.enabled ? "" : "disabled"}`} key={row.day}>
          <label className="settings-day-toggle">
            <input
              type="checkbox"
              checked={row.enabled}
              onChange={(event) => setCalendar((current) => updateCalendarRow(current, index, { enabled: event.currentTarget.checked }))}
            />
            <span className="settings-day-switch" aria-hidden="true"></span>
            <span className="settings-day-name">{dayName(row.day)}</span>
          </label>
          <label className="settings-time-field">
            <span className="settings-time-label">Start</span>
            <input type="time" value={row.start} onChange={(event) => setCalendar((current) => updateCalendarRow(current, index, { start: event.currentTarget.value }))} />
          </label>
          <label className="settings-time-field">
            <span className="settings-time-label">End</span>
            <input type="time" value={row.end} onChange={(event) => setCalendar((current) => updateCalendarRow(current, index, { end: event.currentTarget.value }))} />
          </label>
          <div className="settings-day-range">{row.enabled ? `${row.start} to ${row.end}` : "Off"}</div>
        </div>
      ))}
    </div>
  );
}

function Field({ label, children }: { label: string; children: ReactNode }) {
  return (
    <label>
      <span className="react-field-label">{label}</span>
      {children}
    </label>
  );
}

function StepTitle({ step, title, detail }: { step: string; title: string; detail: string }) {
  return (
    <div className="test-lab-section-header">
      <span className="test-lab-step">{step}</span>
      <div>
        <h3 className="test-lab-section-title">{title}</h3>
        <div className="surface-subtitle">{detail}</div>
      </div>
    </div>
  );
}

function ModeCard({ active, disabled = false, kicker, title, detail, onClick }: { active?: boolean; disabled?: boolean; kicker: string; title: string; detail: string; onClick: () => void }) {
  return (
    <button type="button" aria-label={title} className={`test-lab-mode-card ${active ? "active" : ""} ${disabled ? "disabled" : ""}`} disabled={disabled} onClick={onClick}>
      <div className="test-lab-mode-kicker">{kicker}</div>
      <strong>{title}</strong>
      <span>{detail}</span>
    </button>
  );
}

function StatCard({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="surface stat-card test-lab-summary-card">
      <div className="label">{label}</div>
      <div className="value">{value || "-"}</div>
    </div>
  );
}

function AuditList({ logs }: { logs: AuditLogItem[] }) {
  if (!logs.length) return <div className="empty-state">No recent audit logs.</div>;
  return (
    <div className="compact-list">
      {logs.slice(0, 8).map((log) => (
        <div className="preview-item" key={log.id}>
          <div className="item-title-row">
            <div className="item-title">{formatEvent(log.event_type)}</div>
            {log.lead_id ? <button className="small ghost" type="button" data-action="open-thread" data-lead-id={log.lead_id}>Open</button> : null}
          </div>
          <div className="meta-text">{formatDateTime(log.created_at)}</div>
        </div>
      ))}
    </div>
  );
}

function WebhookRows({ rows }: { rows: Record<string, string> }) {
  const entries = Object.entries(rows || {});
  if (!entries.length) return <div className="empty-state">No webhook URLs available.</div>;
  return (
    <div className="compact-list">
      {entries.map(([key, value]) => (
        <div className="preview-item" key={key}>
          <div className="item-title">{titleize(key)}</div>
          <div className="item-subtitle mono">{value}</div>
        </div>
      ))}
    </div>
  );
}

function activeWebhookRows(rows: Record<string, string>) {
  return Object.fromEntries(
    Object.entries(rows || {}).filter(([key]) => !key.toLowerCase().includes("meta") && !key.toLowerCase().includes("linkedin"))
  );
}

function activeChecklist(items: ChecklistItem[]) {
  return items.filter((item) => {
    const label = item.label.toLowerCase();
    return !label.includes("meta") && !label.includes("linkedin");
  });
}

type ClientFormState = {
  business_name: string;
  client_key: string;
  tone: string;
  timezone: string;
  language: string;
  is_active: boolean;
  booking_url: string;
  booking_mode: string;
  public_base_url: string;
  twilio_account_sid: string;
  twilio_auth_token: string;
  twilio_from_number: string;
  crm_webhook_secret: string;
  zapier_booking_webhook_secret: string;
  zapier_booking_webhook_url: string;
  clear_twilio_credentials: boolean;
  clear_zapier_credentials: boolean;
  fallback_handoff_number: string;
  consent_text: string;
  portal_display_name: string;
  portal_email: string;
  portal_enabled: boolean;
  portal_password: string;
  qualification_questions: string;
  faq_context: string;
  ai_context: string;
  template_overrides: string;
  booking_calendar: OwnerCalendarConfig;
};

function emptyClientForm(): ClientFormState {
  return {
    business_name: "",
    client_key: "",
    tone: "friendly",
    timezone: "America/New_York",
    language: "en",
    is_active: true,
    booking_url: "",
    booking_mode: "link",
    public_base_url: "",
    twilio_account_sid: "",
    twilio_auth_token: "",
    twilio_from_number: "",
    crm_webhook_secret: "",
    zapier_booking_webhook_secret: "",
    zapier_booking_webhook_url: "",
    clear_twilio_credentials: false,
    clear_zapier_credentials: false,
    fallback_handoff_number: "",
    consent_text: "Reply STOP to opt out. Msg/data rates may apply.",
    portal_display_name: "",
    portal_email: "",
    portal_enabled: false,
    portal_password: "",
    qualification_questions: "What are you hoping to solve?\nWhen do you want to get started?",
    faq_context: "",
    ai_context: "",
    template_overrides: "{}",
    booking_calendar: DEFAULT_CALENDAR
  };
}

function formFromDetail(detail: ClientDetailPayload): ClientFormState {
  const provider = detail.client.provider_config || {};
  return {
    ...emptyClientForm(),
    business_name: detail.client.business_name || "",
    client_key: detail.client.client_key || "",
    tone: detail.client.tone || "friendly",
    timezone: detail.client.timezone || "America/New_York",
    language: stringProvider(provider, "language") || "en",
    is_active: detail.client.is_active !== false,
    booking_url: detail.client.booking_url || "",
    booking_mode: detail.client.booking_mode || "link",
    public_base_url: stringProvider(provider, "public_base_url"),
    twilio_from_number: stringProvider(provider, "twilio_from_number"),
    fallback_handoff_number: detail.client.fallback_handoff_number || "",
    consent_text: detail.client.consent_text || "Reply STOP to opt out. Msg/data rates may apply.",
    portal_display_name: detail.client.portal_display_name || "",
    portal_email: detail.client.portal_email || "",
    portal_enabled: Boolean(detail.client.portal_enabled),
    qualification_questions: (detail.client.qualification_questions || []).join("\n"),
    faq_context: detail.client.faq_context || "",
    ai_context: detail.client.ai_context || "",
    template_overrides: JSON.stringify(detail.client.template_overrides || {}, null, 2),
    booking_calendar: calendarFromClient(detail.client.booking_config)
  };
}

function clientFormPayload(
  form: ClientFormState,
  includeClientKey: boolean,
  existingClient?: ClientDetailPayload["client"]
): Record<string, unknown> {
  if (!form.business_name.trim()) throw new Error("Business name is required.");
  let templateOverrides: Record<string, unknown> = {};
  try {
    templateOverrides = JSON.parse(form.template_overrides.trim() || "{}") as Record<string, unknown>;
  } catch {
    throw new Error("Template overrides must be valid JSON.");
  }
  const providerConfig = compactObject({
    language: form.language || "en",
    public_base_url: form.public_base_url || stringProvider(existingClient?.provider_config || {}, "public_base_url"),
    twilio_account_sid: form.clear_twilio_credentials
      ? ""
      : form.twilio_account_sid || stringProvider(existingClient?.provider_config || {}, "twilio_account_sid"),
    twilio_auth_token: form.clear_twilio_credentials
      ? ""
      : form.twilio_auth_token || stringProvider(existingClient?.provider_config || {}, "twilio_auth_token"),
    twilio_from_number: form.clear_twilio_credentials
      ? ""
      : form.twilio_from_number || stringProvider(existingClient?.provider_config || {}, "twilio_from_number"),
    crm_webhook_secret: form.clear_zapier_credentials ? "" : form.crm_webhook_secret,
    zapier_booking_webhook_secret: form.clear_zapier_credentials ? "" : form.zapier_booking_webhook_secret,
    zapier_booking_webhook_url: form.clear_zapier_credentials
      ? ""
      : form.zapier_booking_webhook_url || stringProvider(existingClient?.provider_config || {}, "zapier_booking_webhook_url")
  });
  const payload: Record<string, unknown> = {
    business_name: form.business_name.trim(),
    tone: form.tone.trim() || "friendly",
    timezone: form.timezone.trim() || "America/New_York",
    booking_url: form.booking_url.trim(),
    booking_mode: form.booking_mode,
    booking_config: {
      ...editableBookingConfig(existingClient?.booking_config),
      internal_calendar: form.booking_calendar
    },
    fallback_handoff_number: form.fallback_handoff_number.trim(),
    consent_text: form.consent_text.trim(),
    portal_display_name: form.portal_display_name.trim(),
    portal_email: form.portal_email.trim(),
    portal_enabled: form.portal_enabled,
    provider_config: providerConfig,
    qualification_questions: form.qualification_questions.split(/\n+/).map((item) => item.trim()).filter(Boolean),
    faq_context: form.faq_context.trim(),
    ai_context: form.ai_context.trim(),
    template_overrides: templateOverrides,
    is_active: form.is_active
  };
  if (form.portal_password.trim()) payload.portal_password = form.portal_password;
  if (!includeClientKey) {
    const clearKeys = [
      ...(form.clear_twilio_credentials ? ["twilio_account_sid", "twilio_auth_token", "twilio_from_number"] : []),
      ...(form.clear_zapier_credentials
        ? ["crm_webhook_secret", "zapier_booking_webhook_secret", "zapier_webhook_secret", "zapier_booking_webhook_url"]
        : form.crm_webhook_secret.trim()
          ? ["zapier_webhook_secret"]
          : [])
    ];
    if (clearKeys.length) payload.provider_config_clear_keys = clearKeys;
  }
  if (includeClientKey && form.client_key.trim()) payload.client_key = form.client_key.trim();
  return payload;
}

function calendarFromClient(rawConfig: Record<string, unknown> | undefined): OwnerCalendarConfig {
  const internal = rawConfig?.internal_calendar;
  if (!internal || typeof internal !== "object") return DEFAULT_CALENDAR;
  const config = internal as Partial<OwnerCalendarConfig>;
  const availability = Array.isArray(config.availability) && config.availability.length
    ? config.availability.map((row, index) => ({
        day: Number(row.day ?? index),
        start: String(row.start || DEFAULT_AVAILABILITY[index]?.start || "09:00"),
        end: String(row.end || DEFAULT_AVAILABILITY[index]?.end || "17:00"),
        enabled: row.enabled !== false
      }))
    : DEFAULT_AVAILABILITY;
  return {
    slot_minutes: Number(config.slot_minutes || DEFAULT_CALENDAR.slot_minutes),
    notice_minutes: Number(config.notice_minutes || DEFAULT_CALENDAR.notice_minutes),
    horizon_days: Number(config.horizon_days || DEFAULT_CALENDAR.horizon_days),
    availability
  };
}

function editableBookingConfig(rawConfig: Record<string, unknown> | undefined) {
  const output: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(rawConfig || {})) {
    if (key.endsWith("_configured") || key === "calendly_personal_access_token" || key === "internal_calendar") continue;
    output[key] = value;
  }
  return output;
}

function updateCalendarRow(current: OwnerCalendarConfig, index: number, patch: Partial<OwnerCalendarAvailabilityRow>): OwnerCalendarConfig {
  return {
    ...current,
    availability: current.availability.map((row, rowIndex) => rowIndex === index ? { ...row, ...patch } : row)
  };
}

function updateAnswer(rows: typeof DEFAULT_TEST_ANSWERS, index: number, field: "question" | "answer", value: string) {
  return rows.map((row, rowIndex) => rowIndex === index ? { ...row, [field]: value } : row);
}

function setFormField<K extends keyof ClientFormState>(setForm: Dispatch<SetStateAction<ClientFormState>>, key: K, value: ClientFormState[K]) {
  setForm((current) => ({ ...current, [key]: value }));
}

function setCalendarField<K extends keyof Omit<OwnerCalendarConfig, "availability">>(setCalendar: Dispatch<SetStateAction<OwnerCalendarConfig>>, key: K, value: OwnerCalendarConfig[K]) {
  setCalendar((current) => ({ ...current, [key]: value }));
}

function setClientCalendarField<K extends keyof Omit<OwnerCalendarConfig, "availability">>(setForm: Dispatch<SetStateAction<ClientFormState>>, key: K, value: OwnerCalendarConfig[K]) {
  setForm((current) => ({
    ...current,
    booking_calendar: {
      ...current.booking_calendar,
      [key]: value
    }
  }));
}

function chooseClientKey(clients: ClientSummary[]) {
  const saved = window.localStorage.getItem("lead-ui-selected-client") || "";
  return clients.some((client) => client.client_key === saved) ? saved : clients[0]?.client_key || "";
}

function readClientTab(): ClientTab {
  const saved = window.localStorage.getItem("lead-ui-client-tab");
  return saved === "edit" || saved === "webhooks" || saved === "overview" ? saved : "overview";
}

function readClientWizardStep(): ClientWizardStep {
  const saved = window.localStorage.getItem("lead-ui-client-wizard-step");
  return isClientWizardStep(saved) ? saved : "business";
}

function isClientWizardStep(value: string | null): value is ClientWizardStep {
  return CLIENT_WIZARD_STEPS.includes(value as ClientWizardStep);
}

function filterClients(clients: ClientSummary[], query: string) {
  const needle = query.trim().toLowerCase();
  if (!needle) return clients;
  return clients.filter((client) => {
    const haystack = [
      client.business_name,
      client.client_key,
      client.timezone || "",
      client.booking_url || ""
    ].join(" ").toLowerCase();
    return haystack.includes(needle);
  });
}

function rememberClientKey(clientKey: string) {
  if (!clientKey) return;
  const previous = window.localStorage.getItem("lead-ui-selected-client") || "";
  window.localStorage.setItem("lead-ui-selected-client", clientKey);
  if (previous !== clientKey) {
    window.dispatchEvent(new CustomEvent("lead-ui-client-change", { detail: { clientKey } }));
  }
}

function renderBadge(label: string, tone: "ok" | "warn" | "info" = "info") {
  return <span className={`tag ${tone}`}>{label}</span>;
}

function hasProviderValue(detail: ClientDetailPayload | null, key: string) {
  const provider = detail?.client.provider_config;
  const runtime = detail?.provider_runtime as Record<string, unknown> | undefined;
  const value = provider?.[key] ?? runtime?.[`${key}_configured`];
  return typeof value === "string" ? Boolean(value) : Boolean(value);
}

function hasCrmWebhookSecret(detail: ClientDetailPayload | null) {
  return hasProviderValue(detail, "crm_webhook_secret") || hasProviderValue(detail, "zapier_webhook_secret");
}

function statusLabel(value: unknown) {
  return value ? "configured" : "missing";
}

function stringProvider(provider: Record<string, unknown>, key: string) {
  const value = provider[key];
  return typeof value === "string" ? value : "";
}

function compactObject(input: Record<string, string>) {
  const output: Record<string, string> = {};
  for (const [key, value] of Object.entries(input)) {
    if (value.trim()) output[key] = value.trim();
  }
  return output;
}

function defaultWebhookRows(clientKey: string) {
  return {
    zapier_events: `/webhooks/zapier/${clientKey}`,
    website_form: `/webhooks/form/${clientKey}`,
    twilio_sms: `/sms/inbound/${clientKey}`
  };
}

function formatBookingMode(value: string) {
  const labels: Record<string, string> = {
    internal: "internal calendar",
    link: "booking link",
    calendly: "Calendly"
  };
  return labels[value] || titleize(value);
}

function formatEvent(value: string) {
  return titleize(value.replace(/_/g, " "));
}

function formatSource(source: string) {
  const labels: Record<string, string> = {
    meta: "Meta lead ad",
    linkedin: "LinkedIn lead form",
    sms: "SMS intake",
    manual: "Manual entry"
  };
  return labels[source] || titleize(source);
}

function titleize(value: string) {
  return value
    .replace(/[_-]+/g, " ")
    .trim()
    .replace(/\w\S*/g, (word) => word.charAt(0).toUpperCase() + word.slice(1));
}

function shortJson(value: Record<string, unknown>) {
  const text = JSON.stringify(value);
  return text.length > 120 ? `${text.slice(0, 117)}...` : text;
}

function formatDateTime(value: string) {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat(undefined, { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" }).format(date);
}

function dayName(day: number) {
  return ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][day] || `Day ${day + 1}`;
}

function messageFor(error: unknown, fallback: string) {
  return error instanceof Error ? error.message : fallback;
}
