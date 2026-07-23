import { apiJson } from "./client";

export type AiProbeRequest = {
  client_key: string;
  inbound_text: string;
  lead_name: string;
  lead_city: string;
};

export type AiProbeResult = {
  status: string;
  provider?: string;
  provider_error?: string | null;
  reply_text?: string;
  next_state?: string;
  action?: string;
  next_question_key?: string | null;
  collected_fields?: Record<string, unknown>;
  actions?: Array<Record<string, unknown>>;
};

export type SmsProbeRequest = {
  client_key: string;
  to_number: string;
  body: string;
};

export type SmsProbeResult = {
  status: string;
  provider_sid?: string;
};

export type IntakeProbeKind = "form" | "zapier";

export type IntakeProbeResult = {
  status: string;
  source?: string;
  queued_source?: string;
  client_key: string;
};

export type ZapierConsoleItem = {
  id: number;
  event_type: string;
  lead_id?: number | null;
  created_at: string;
  decision?: Record<string, unknown>;
};

export type ZapierConsolePayload = {
  client_key: string;
  webhook_url: string;
  items: ZapierConsoleItem[];
};

export type DemoMutationResult = Record<string, unknown>;

function jsonRequest(method: string, payload: unknown, extraHeaders?: HeadersInit): RequestInit {
  return {
    method,
    headers: {
      "Content-Type": "application/json",
      ...Object.fromEntries(new Headers(extraHeaders).entries())
    },
    body: JSON.stringify(payload)
  };
}

export function runAiProbe(payload: AiProbeRequest): Promise<AiProbeResult> {
  return apiJson<AiProbeResult>("/admin/test/ai", jsonRequest("POST", payload));
}

export function runSmsProbe(payload: SmsProbeRequest): Promise<SmsProbeResult> {
  return apiJson<SmsProbeResult>("/admin/test/sms", jsonRequest("POST", payload));
}

export function submitIntakeProbe(
  clientKey: string,
  kind: IntakeProbeKind,
  payload: Record<string, unknown>,
  webhookSecret = ""
): Promise<IntakeProbeResult> {
  const headers = webhookSecret.trim()
    ? { "X-Zapier-Webhook-Secret": webhookSecret.trim() }
    : undefined;
  return apiJson<IntakeProbeResult>(
    `/webhooks/${kind === "zapier" ? "zapier" : "form"}/${encodeURIComponent(clientKey)}`,
    jsonRequest("POST", payload, headers)
  );
}

export function fetchZapierConsole(clientKey: string, limit = 30): Promise<ZapierConsolePayload> {
  const query = new URLSearchParams({ limit: String(limit) });
  return apiJson<ZapierConsolePayload>(
    `/ui/api/clients/${encodeURIComponent(clientKey)}/zapier-results?${query.toString()}`
  );
}

export function seedDemoData(reset = false): Promise<DemoMutationResult> {
  return apiJson<DemoMutationResult>(`/ui/api/seed-demo${reset ? "?reset=true" : ""}`, {
    method: "POST"
  });
}

export function resetDemoData(): Promise<DemoMutationResult> {
  return apiJson<DemoMutationResult>("/ui/api/seed-demo", { method: "DELETE" });
}

export function seedShowcaseData(clientKey: string, reset = false): Promise<DemoMutationResult> {
  return apiJson<DemoMutationResult>(
    `/ui/api/seed-showcase/${encodeURIComponent(clientKey)}${reset ? "?reset=true" : ""}`,
    { method: "POST" }
  );
}
