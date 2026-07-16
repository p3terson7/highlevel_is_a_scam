import { useEffect, useMemo, useRef, useState } from "react";
import type { FormEvent, ReactNode } from "react";
import {
  fetchZapierConsole,
  resetDemoData,
  runAiProbe,
  runSmsProbe,
  seedDemoData,
  seedShowcaseData,
  submitIntakeProbe
} from "../../api/adminTools";
import type {
  AiProbeResult,
  DemoMutationResult,
  IntakeProbeKind,
  IntakeProbeResult,
  SmsProbeResult,
  ZapierConsolePayload
} from "../../api/adminTools";
import { fetchAutomationHealth } from "../../api/client";
import type { AutomationHealthPayload, ClientSummary, OwnerWorkspacePayload } from "../../api/types";

type ToolStatus = "idle" | "loading" | "ready" | "error";

export function DemoDataPanel({
  canSeed,
  demoDataPresent,
  clients,
  selectedClientKey,
  onChanged
}: {
  canSeed: boolean;
  demoDataPresent: boolean;
  clients: ClientSummary[];
  selectedClientKey: string;
  onChanged: () => void | Promise<void>;
}) {
  const [busyAction, setBusyAction] = useState("");
  const [message, setMessage] = useState("");
  const [failed, setFailed] = useState(false);
  const [result, setResult] = useState<DemoMutationResult | null>(null);
  const [knownDemoPresent, setKnownDemoPresent] = useState(demoDataPresent);
  const demoClients = clients.filter((client) => /demo|showcase|preciscan|stackleads/i.test(client.client_key));

  useEffect(() => setKnownDemoPresent(demoDataPresent), [demoDataPresent]);

  async function run(label: string, action: () => Promise<DemoMutationResult>, nextPresence?: boolean) {
    setBusyAction(label);
    setMessage(`${label} in progress...`);
    setFailed(false);
    try {
      const payload = await action();
      setResult(payload);
      if (nextPresence !== undefined) setKnownDemoPresent(nextPresence);
      setMessage(`${label} completed.`);
      await onChanged();
    } catch (caught: unknown) {
      setFailed(true);
      setMessage(errorMessage(caught, `${label} failed.`));
    } finally {
      setBusyAction("");
    }
  }

  function resetAllDemoData() {
    if (!window.confirm("Delete all seeded demo clients and their demo conversations? This cannot be undone.")) return;
    void run("Demo reset", resetDemoData, false);
  }

  const disabled = !canSeed || Boolean(busyAction);
  return (
    <section className="surface stack settings-card" aria-labelledby="demo-data-title">
      <div className="surface-title">
        <div>
          <h3 id="demo-data-title">Demo and showcase data</h3>
          <div className="surface-subtitle">Create realistic CRM records for demonstrations without touching production clients.</div>
        </div>
        <span className={`tag ${canSeed ? "ok" : "warn"}`}>{canSeed ? "enabled" : "disabled"}</span>
      </div>

      {!canSeed ? (
        <div className="empty-state">Demo seeding is disabled in this environment.</div>
      ) : (
        <>
          <div className="actions">
            <button className="small" type="button" disabled={disabled} onClick={() => void run("Demo seed", () => seedDemoData(false), true)}>Seed demo clients</button>
            <button className="small ghost" type="button" disabled={disabled} onClick={() => void run("Demo reseed", () => seedDemoData(true), true)}>Reseed demo clients</button>
            <button className="small warn" type="button" disabled={disabled || !knownDemoPresent} onClick={resetAllDemoData}>Reset demo clients</button>
          </div>
          <div className="actions">
            <button className="small" type="button" disabled={disabled || !selectedClientKey} onClick={() => void run("Showcase seed", () => seedShowcaseData(selectedClientKey, false))}>Seed selected client showcase</button>
            <button className="small ghost" type="button" disabled={disabled || !selectedClientKey} onClick={() => void run("Showcase reseed", () => seedShowcaseData(selectedClientKey, true))}>Reseed selected client showcase</button>
          </div>
          <div className="meta-text">{selectedClientKey ? `Selected client: ${selectedClientKey}` : "Select a client before seeding showcase records."}</div>
        </>
      )}

      <div className="compact-list" aria-label="Seeded demo clients">
        {demoClients.length ? demoClients.map((client) => (
          <div className="preview-item" key={client.client_key}>
            <div className="item-title-row">
              <div className="item-title">{client.business_name}</div>
              <span className="tag info">{client.client_key}</span>
            </div>
            <div className="item-snippet">{client.lead_count || 0} records · {client.open_conversations || 0} open conversations</div>
          </div>
        )) : <div className="empty-state">No seeded demo clients detected.</div>}
      </div>

      {message ? <div className="meta-text" role={failed ? "alert" : "status"}>{message}</div> : null}
      {result ? <details><summary>Last operation result</summary><JsonOutput value={result} /></details> : null}
    </section>
  );
}

export function ProviderProbePanel({
  clientKey,
  workspace
}: {
  clientKey: string;
  workspace: OwnerWorkspacePayload | null;
}) {
  const [aiInput, setAiInput] = useState("Can I book a consultation this week?");
  const [leadName, setLeadName] = useState("Test Lead");
  const [leadCity, setLeadCity] = useState("Toronto");
  const [aiStatus, setAiStatus] = useState<ToolStatus>("idle");
  const [aiMessage, setAiMessage] = useState("");
  const [aiResult, setAiResult] = useState<AiProbeResult | null>(null);
  const [smsTo, setSmsTo] = useState("");
  const [smsBody, setSmsBody] = useState("This is a live delivery test from the CRM.");
  const [smsConfirmed, setSmsConfirmed] = useState(false);
  const [smsStatus, setSmsStatus] = useState<ToolStatus>("idle");
  const [smsMessage, setSmsMessage] = useState("");
  const [smsResult, setSmsResult] = useState<SmsProbeResult | null>(null);
  const clientGeneration = useRef(0);
  const twilioConfigured = Boolean(workspace?.runtime.twilio_configured);
  const aiConfigured = Boolean(workspace?.runtime.ai_configured);

  useEffect(() => {
    clientGeneration.current += 1;
    setAiStatus("idle");
    setAiResult(null);
    setAiMessage("");
    setSmsStatus("idle");
    setSmsResult(null);
    setSmsMessage("");
    setSmsConfirmed(false);
  }, [clientKey]);

  async function testAi(event: FormEvent) {
    event.preventDefault();
    if (!clientKey || !aiInput.trim()) return;
    const generation = clientGeneration.current;
    setAiStatus("loading");
    setAiMessage("Running the client-scoped AI decision probe...");
    try {
      const payload = await runAiProbe({
        client_key: clientKey,
        inbound_text: aiInput.trim(),
        lead_name: leadName.trim() || "Test Lead",
        lead_city: leadCity.trim() || "Test City"
      });
      if (generation !== clientGeneration.current) return;
      setAiResult(payload);
      setAiStatus("ready");
      setAiMessage(`AI probe completed${payload.provider ? ` with ${payload.provider}` : ""}.`);
    } catch (caught: unknown) {
      if (generation !== clientGeneration.current) return;
      setAiStatus("error");
      setAiMessage(errorMessage(caught, "AI probe failed."));
    }
  }

  async function testSms(event: FormEvent) {
    event.preventDefault();
    if (!clientKey || !smsTo.trim() || !smsConfirmed || !twilioConfigured) return;
    const generation = clientGeneration.current;
    setSmsStatus("loading");
    setSmsMessage("Sending one live SMS through the selected client's Twilio configuration...");
    try {
      const payload = await runSmsProbe({
        client_key: clientKey,
        to_number: smsTo.trim(),
        body: smsBody.trim() || "This is a live delivery test from the CRM."
      });
      if (generation !== clientGeneration.current) return;
      setSmsResult(payload);
      setSmsStatus("ready");
      setSmsConfirmed(false);
      setSmsMessage(`Twilio accepted the message${payload.provider_sid ? ` (${payload.provider_sid})` : ""}.`);
    } catch (caught: unknown) {
      if (generation !== clientGeneration.current) return;
      setSmsStatus("error");
      setSmsMessage(errorMessage(caught, "SMS probe failed."));
    } finally {
      if (generation === clientGeneration.current) setSmsConfirmed(false);
    }
  }

  return (
    <section className="surface stack test-lab-section" aria-labelledby="provider-probes-title">
      <div className="test-lab-section-header">
        <span className="test-lab-step">6</span>
        <div>
          <h3 className="test-lab-section-title" id="provider-probes-title">Provider probes</h3>
          <div className="surface-subtitle">Test AI decisions safely, or explicitly send one live Twilio message when configured.</div>
        </div>
      </div>

      <form className="stack detail-card" onSubmit={(event) => void testAi(event)}>
        <div className="item-title-row">
          <strong>AI decision</strong>
          <span className={`tag ${aiConfigured ? "ok" : "warn"}`}>{aiConfigured ? "configured" : "offline/mock"}</span>
        </div>
        <div className="form-grid-2">
          <LabeledField label="Probe lead name"><input value={leadName} onChange={(event) => setLeadName(event.currentTarget.value)} /></LabeledField>
          <LabeledField label="Probe lead city"><input value={leadCity} onChange={(event) => setLeadCity(event.currentTarget.value)} /></LabeledField>
        </div>
        <LabeledField label="Inbound message"><textarea value={aiInput} onChange={(event) => setAiInput(event.currentTarget.value)} required /></LabeledField>
        <button className="small" type="submit" disabled={!clientKey || aiStatus === "loading"}>{aiStatus === "loading" ? "Running AI probe..." : "Run AI probe"}</button>
        {aiMessage ? <div className="meta-text" role={aiStatus === "error" ? "alert" : "status"}>{aiMessage}</div> : null}
        {aiResult ? <JsonOutput value={aiResult} /> : null}
      </form>

      <form className="stack detail-card" onSubmit={(event) => void testSms(event)}>
        <div className="item-title-row">
          <strong>Twilio delivery</strong>
          <span className={`tag ${twilioConfigured ? "ok" : "warn"}`}>{twilioConfigured ? "configured" : "needs setup"}</span>
        </div>
        <div className="surface-subtitle">This probe sends a real message and may incur provider charges. It does not create a CRM conversation.</div>
        <LabeledField label="Test SMS recipient"><input className="mono" type="tel" value={smsTo} onChange={(event) => setSmsTo(event.currentTarget.value)} placeholder="+15551234567" disabled={!twilioConfigured} required /></LabeledField>
        <LabeledField label="Test SMS body"><textarea value={smsBody} onChange={(event) => setSmsBody(event.currentTarget.value)} disabled={!twilioConfigured} required /></LabeledField>
        <label className="checkbox-row">
          <input type="checkbox" checked={smsConfirmed} onChange={(event) => setSmsConfirmed(event.currentTarget.checked)} disabled={!twilioConfigured} />
          <span>I understand this sends a live SMS.</span>
        </label>
        <button className="small" type="submit" disabled={!clientKey || !twilioConfigured || !smsConfirmed || smsStatus === "loading"}>{smsStatus === "loading" ? "Sending test SMS..." : "Send test SMS"}</button>
        {smsMessage ? <div className="meta-text" role={smsStatus === "error" ? "alert" : "status"}>{smsMessage}</div> : null}
        {smsResult ? <JsonOutput value={smsResult} /> : null}
      </form>
    </section>
  );
}

export function IntakeProbePanel({
  clientKey,
  webhookSecretConfigured
}: {
  clientKey: string;
  webhookSecretConfigured: boolean;
}) {
  const [kind, setKind] = useState<IntakeProbeKind>("form");
  const [payloadText, setPayloadText] = useState(() => sampleIntakePayload("form"));
  const [webhookSecret, setWebhookSecret] = useState("");
  const [intakeConfirmed, setIntakeConfirmed] = useState(false);
  const [probeStatus, setProbeStatus] = useState<ToolStatus>("idle");
  const [probeMessage, setProbeMessage] = useState("");
  const [probeResult, setProbeResult] = useState<IntakeProbeResult | null>(null);
  const [consoleStatus, setConsoleStatus] = useState<ToolStatus>("idle");
  const [consoleMessage, setConsoleMessage] = useState("");
  const [consolePayload, setConsolePayload] = useState<ZapierConsolePayload | null>(null);
  const consoleRequest = useRef(0);
  const probeGeneration = useRef(0);

  async function refreshConsole() {
    if (!clientKey) return;
    const request = ++consoleRequest.current;
    setConsoleStatus("loading");
    setConsoleMessage("Loading recent intake activity...");
    try {
      const payload = await fetchZapierConsole(clientKey);
      if (request !== consoleRequest.current) return;
      setConsolePayload(payload);
      setConsoleStatus("ready");
      setConsoleMessage(payload.items.length ? `Loaded ${payload.items.length} recent events.` : "No Zapier or form activity has been recorded yet.");
    } catch (caught: unknown) {
      if (request !== consoleRequest.current) return;
      setConsoleStatus("error");
      setConsoleMessage(errorMessage(caught, "Intake console unavailable."));
    }
  }

  useEffect(() => {
    probeGeneration.current += 1;
    consoleRequest.current += 1;
    setProbeStatus("idle");
    setProbeResult(null);
    setProbeMessage("");
    setWebhookSecret("");
    setIntakeConfirmed(false);
    setConsolePayload(null);
    if (clientKey) void refreshConsole();
  }, [clientKey]);

  function chooseKind(nextKind: IntakeProbeKind) {
    setKind(nextKind);
    setPayloadText(sampleIntakePayload(nextKind));
    setProbeResult(null);
    setProbeMessage("");
    setIntakeConfirmed(false);
  }

  async function runProbe(event: FormEvent) {
    event.preventDefault();
    if (!clientKey || !intakeConfirmed) return;
    const generation = probeGeneration.current;
    let payload: Record<string, unknown>;
    try {
      const parsed = JSON.parse(payloadText) as unknown;
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new Error("Payload must be a JSON object.");
      payload = parsed as Record<string, unknown>;
    } catch (caught: unknown) {
      setProbeStatus("error");
      setProbeMessage(errorMessage(caught, "Payload must be valid JSON."));
      return;
    }

    setProbeStatus("loading");
    setProbeMessage(`Submitting a ${kind === "zapier" ? "Zapier" : "website form"} intake event...`);
    try {
      const result = await submitIntakeProbe(clientKey, kind, payload, webhookSecret);
      if (generation !== probeGeneration.current) return;
      setProbeResult(result);
      setProbeStatus("ready");
      setIntakeConfirmed(false);
      setProbeMessage("Intake event accepted and queued for normal server-side processing.");
      await refreshConsole();
    } catch (caught: unknown) {
      if (generation !== probeGeneration.current) return;
      setProbeStatus("error");
      setProbeMessage(errorMessage(caught, "Intake probe failed."));
    } finally {
      setWebhookSecret("");
    }
  }

  const endpoint = clientKey ? `/webhooks/${kind === "zapier" ? "zapier" : "form"}/${clientKey}` : "Select a client";
  return (
    <section className="surface stack test-lab-section" aria-labelledby="intake-probe-title">
      <div className="test-lab-section-header">
        <span className="test-lab-step">7</span>
        <div>
          <h3 className="test-lab-section-title" id="intake-probe-title">Form and Zapier intake</h3>
          <div className="surface-subtitle">Submit realistic JSON through the same public endpoints used by integrations, then inspect the audit console.</div>
        </div>
      </div>

      <form className="stack" onSubmit={(event) => void runProbe(event)}>
        <div className="test-lab-mode-grid">
          <button className={`test-lab-mode-card ${kind === "form" ? "active" : ""}`} type="button" aria-label="Website form" onClick={() => chooseKind("form")}>
            <div className="test-lab-mode-kicker">Intake</div><strong>Website form</strong><span>Tests normalized form submission.</span>
          </button>
          <button className={`test-lab-mode-card ${kind === "zapier" ? "active" : ""}`} type="button" aria-label="Zapier webhook" onClick={() => chooseKind("zapier")}>
            <div className="test-lab-mode-kicker">Intake</div><strong>Zapier webhook</strong><span>Tests the Zapier-compatible payload adapter.</span>
          </button>
        </div>
        <div className="item-subtitle mono">{endpoint}</div>
        <LabeledField label="Intake JSON payload"><textarea className="mono" rows={12} value={payloadText} onChange={(event) => setPayloadText(event.currentTarget.value)} spellCheck={false} /></LabeledField>
        <LabeledField label="Webhook secret (write-only)">
          <input className="mono" type="password" autoComplete="new-password" value={webhookSecret} onChange={(event) => setWebhookSecret(event.currentTarget.value)} placeholder={webhookSecretConfigured ? "Configured; enter it for this request" : "Not configured; leave blank"} />
        </LabeledField>
        <div className="meta-text">The secret is held only for this request and cleared immediately afterward.</div>
        <label className="checkbox-row">
          <input type="checkbox" checked={intakeConfirmed} onChange={(event) => setIntakeConfirmed(event.currentTarget.checked)} />
          <span>I understand this creates a CRM contact and may trigger configured automation or live SMS.</span>
        </label>
        <button className="small" type="submit" disabled={!clientKey || !intakeConfirmed || probeStatus === "loading"}>{probeStatus === "loading" ? "Submitting intake..." : `Submit ${kind === "zapier" ? "Zapier" : "form"} test`}</button>
        {probeMessage ? <div className="meta-text" role={probeStatus === "error" ? "alert" : "status"}>{probeMessage}</div> : null}
        {probeResult ? <JsonOutput value={probeResult} /> : null}
      </form>

      <div className="item-title-row">
        <strong>Intake activity console</strong>
        <button className="small ghost" type="button" disabled={!clientKey || consoleStatus === "loading"} onClick={() => void refreshConsole()}>Refresh console</button>
      </div>
      {consoleMessage ? <div className="meta-text" role={consoleStatus === "error" ? "alert" : "status"}>{consoleMessage}</div> : null}
      {consolePayload ? <ZapierConsole payload={consolePayload} /> : null}
    </section>
  );
}

export function AutomationDiagnosticsPanel({ clientKey }: { clientKey: string }) {
  const [status, setStatus] = useState<ToolStatus>("idle");
  const [message, setMessage] = useState("");
  const [health, setHealth] = useState<AutomationHealthPayload | null>(null);
  const requestGeneration = useRef(0);

  async function load() {
    if (!clientKey) return;
    const request = ++requestGeneration.current;
    setStatus("loading");
    setMessage("Checking client-scoped automations...");
    try {
      const payload = await fetchAutomationHealth(clientKey);
      if (request !== requestGeneration.current) return;
      setHealth(payload);
      setStatus("ready");
      setMessage(payload.status === "healthy" ? "All configured automations report healthy." : `${payload.needs_attention} automations need attention.`);
    } catch (caught: unknown) {
      if (request !== requestGeneration.current) return;
      setStatus("error");
      setMessage(errorMessage(caught, "Automation health unavailable."));
    }
  }

  useEffect(() => {
    requestGeneration.current += 1;
    setStatus("idle");
    setHealth(null);
    if (clientKey) void load();
  }, [clientKey]);

  const rows = useMemo(() => (health?.automations || []).filter((item) => !/meta|linkedin/i.test(`${item.key} ${item.label}`)), [health]);
  return (
    <section className="surface stack test-lab-section" aria-labelledby="automation-diagnostics-title">
      <div className="item-title-row">
        <div>
          <h3 className="test-lab-section-title" id="automation-diagnostics-title">Automation health</h3>
          <div className="surface-subtitle">Configuration, recent execution, and failure signals for the selected client.</div>
        </div>
        <button className="small ghost" type="button" disabled={!clientKey || status === "loading"} onClick={() => void load()}>Refresh health</button>
      </div>
      {message ? <div className="meta-text" role={status === "error" ? "alert" : "status"}>{message}</div> : null}
      <div className="compact-list">
        {rows.length ? rows.map((item) => (
          <div className="preview-item" key={item.key}>
            <div className="item-title-row">
              <div className="item-title">{item.label}</div>
              <span className={`tag ${item.status === "healthy" ? "ok" : "warn"}`}>{item.status.replace(/_/g, " ")}</span>
            </div>
            <div className="item-snippet">{item.detail || "No diagnostic detail."}</div>
            <div className="meta-text">{item.last_run_at ? `Last run ${formatDateTime(item.last_run_at)}` : "No recent run"} · {item.runs_7d || 0} runs / 7d</div>
          </div>
        )) : status === "ready" ? <div className="empty-state">No automation probes are available.</div> : null}
      </div>
    </section>
  );
}

function ZapierConsole({ payload }: { payload: ZapierConsolePayload }) {
  return (
    <div className="stack">
      <div className="item-subtitle mono">Endpoint: {payload.webhook_url}</div>
      <div className="compact-list">
        {payload.items.length ? payload.items.map((item) => (
          <details className="preview-item" key={item.id}>
            <summary>
              <strong>{humanize(item.event_type)}</strong> · {formatDateTime(item.created_at)} · record {item.lead_id ?? "-"}
            </summary>
            <JsonOutput value={item.decision || {}} />
          </details>
        )) : <div className="empty-state">No intake activity yet.</div>}
      </div>
    </div>
  );
}

function JsonOutput({ value }: { value: unknown }) {
  return <pre className="code-output">{JSON.stringify(redactForDisplay(value), null, 2)}</pre>;
}

function LabeledField({ label, children }: { label: string; children: ReactNode }) {
  return <label><span className="react-field-label">{label}</span>{children}</label>;
}

function sampleIntakePayload(kind: IntakeProbeKind) {
  const payload = kind === "zapier"
    ? {
        id: `react-test-${Date.now()}`,
        full_name: "Zapier Test Lead",
        phone: "+15551234567",
        email: "zapier-test@example.com",
        city: "Toronto",
        form_answers: {
          Timeline: "Within 2 weeks",
          "Service interest": "Strategy call"
        }
      }
    : {
        source: "website",
        lead: {
          id: `react-form-test-${Date.now()}`,
          full_name: "Website Form Test Lead",
          phone: "+15551234567",
          email: "form-test@example.com",
          city: "Toronto",
          form_answers: {
            Timeline: "Within 2 weeks",
            "Service interest": "Strategy call"
          }
        }
      };
  return JSON.stringify(payload, null, 2);
}

function formatDateTime(value: string) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat(undefined, { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" }).format(date);
}

function humanize(value: string) {
  return value.replace(/[_-]+/g, " ").replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function errorMessage(error: unknown, fallback: string) {
  return error instanceof Error ? error.message : fallback;
}

function redactForDisplay(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(redactForDisplay);
  if (!value || typeof value !== "object") return value;
  return Object.fromEntries(
    Object.entries(value as Record<string, unknown>).map(([key, item]) => [
      key,
      /(?:password|secret|token|api_key|auth_token)$/i.test(key) ? "[redacted]" : redactForDisplay(item)
    ])
  );
}
