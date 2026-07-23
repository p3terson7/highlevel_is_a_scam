import type {
  AgentControl,
  AuditLogItem,
  AutomationHealthPayload,
  CalendarPayload,
  ClientDetailPayload,
  ClientPortalLoginResponse,
  ClientSummary,
  ConversationThreadPayload,
  ConversationsPayload,
  CrmLeadsPayload,
  DashboardPayload,
  KnowledgeJobStatus,
  KnowledgePayload,
  LeadDetailPayload,
  ManualMeetingCreatePayload,
  ManualLeadCreatePayload,
  ManualLeadCreateResponse,
  OwnerCalendarConfig,
  OwnerWorkspacePayload,
  RuntimeConfigStatus,
  SandboxMessageResponse,
  SandboxStartPayload,
  SandboxStartResponse,
  SessionPayload,
  TasksPayload
} from "./types";

export const ADMIN_TOKEN_KEY = "lead-ui-admin-token";
export const PORTAL_TOKEN_KEY = "lead-ui-portal-token";
export const AUTH_MODE_KEY = "lead-ui-auth-mode";
export const AUTH_EXPIRED_EVENT = "lead-ui-auth-expired";
const OUTBOUND_REQUEST_KEY_PREFIX = "lead-ui-outbound-request:";

export class ApiError extends Error {
  readonly status: number;
  readonly detail: unknown;

  constructor(message: string, status: number, detail: unknown) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.detail = detail;
  }
}

export function authHeaders(extra?: HeadersInit): Headers {
  const headers = new Headers(extra);
  // Remove credentials left by pre-cookie releases. Authentication now uses a
  // Secure HttpOnly session cookie, which JavaScript cannot read or exfiltrate.
  headers.delete("X-Admin-Token");
  headers.delete("X-Portal-Token");
  window.localStorage.removeItem(ADMIN_TOKEN_KEY);
  window.localStorage.removeItem(PORTAL_TOKEN_KEY);
  return headers;
}

function csrfHeaders(extra: HeadersInit | undefined, method: string | undefined): Headers {
  const headers = authHeaders(extra);
  if (["POST", "PUT", "PATCH", "DELETE"].includes((method || "GET").toUpperCase())) {
    const csrfToken = readCookie("leadops_csrf");
    if (csrfToken) headers.set("X-CSRF-Token", csrfToken);
  }
  return headers;
}

export function storeAdminAuth() {
  window.localStorage.setItem(AUTH_MODE_KEY, "admin");
  window.localStorage.removeItem(ADMIN_TOKEN_KEY);
  window.localStorage.removeItem(PORTAL_TOKEN_KEY);
}

export function storePortalAuth() {
  window.localStorage.setItem(AUTH_MODE_KEY, "client");
  window.localStorage.removeItem(PORTAL_TOKEN_KEY);
  window.localStorage.removeItem(ADMIN_TOKEN_KEY);
}

export function clearAuthTokens() {
  window.localStorage.removeItem(ADMIN_TOKEN_KEY);
  window.localStorage.removeItem(PORTAL_TOKEN_KEY);
  window.localStorage.removeItem(AUTH_MODE_KEY);
  clearAllOutboundRequestKeys();
}

export function outboundRequestKey(scope: string, fingerprint: string): string {
  const storageKey = `${OUTBOUND_REQUEST_KEY_PREFIX}${scope}`;
  const fingerprintToken = digestFingerprint(fingerprint);
  try {
    const saved = JSON.parse(window.sessionStorage.getItem(storageKey) || "null") as { fingerprint?: string; key?: string } | null;
    if (saved?.fingerprint === fingerprintToken && saved.key) return saved.key;
  } catch {
    // A malformed or unavailable cache should not block an outbound action.
  }
  const randomPart = typeof globalThis.crypto?.randomUUID === "function"
    ? globalThis.crypto.randomUUID()
    : `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
  const key = `react-${scope.replace(/[^A-Za-z0-9._:-]+/g, "-")}-${randomPart}`.slice(0, 128);
  try {
    window.sessionStorage.setItem(storageKey, JSON.stringify({ fingerprint: fingerprintToken, key }));
  } catch {
    // The generated key remains valid for this attempt when storage is unavailable.
  }
  return key;
}

export function clearOutboundRequestKey(scope: string) {
  try {
    window.sessionStorage.removeItem(`${OUTBOUND_REQUEST_KEY_PREFIX}${scope}`);
  } catch {
    // Best-effort cache cleanup.
  }
}

function clearAllOutboundRequestKeys() {
  try {
    const keys = Array.from({ length: window.sessionStorage.length }, (_, index) => window.sessionStorage.key(index));
    keys.forEach((key) => {
      if (key?.startsWith(OUTBOUND_REQUEST_KEY_PREFIX)) window.sessionStorage.removeItem(key);
    });
  } catch {
    // Best-effort cache cleanup.
  }
}

function digestFingerprint(value: string) {
  let first = 0x811c9dc5;
  let second = 0x9e3779b9;
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    first = Math.imul(first ^ code, 0x01000193);
    second = Math.imul(second ^ code, 0x85ebca6b);
  }
  return `${value.length}:${(first >>> 0).toString(16)}:${(second >>> 0).toString(16)}`;
}

export async function apiJson<T>(path: string, options: RequestInit = {}): Promise<T> {
  const response = await fetch(path, {
    ...options,
    credentials: "same-origin",
    headers: csrfHeaders(options.headers, options.method)
  });
  const text = await response.text();
  const payload = text ? parseJson(text) : null;

  if (!response.ok) {
    if (response.status === 401) {
      window.dispatchEvent(new CustomEvent(AUTH_EXPIRED_EVENT));
    }
    throw new ApiError(errorMessage(payload, response.statusText), response.status, payload);
  }

  return payload as T;
}

export function fetchSession(): Promise<SessionPayload> {
  return apiJson<SessionPayload>("/ui/api/session");
}

export function fetchDashboard(clientKey = ""): Promise<DashboardPayload> {
  return apiJson<DashboardPayload>(withQuery("/ui/api/dashboard", { client_key: clientKey || undefined }));
}

export function fetchClients(): Promise<ClientSummary[]> {
  return apiJson<ClientSummary[]>("/ui/api/clients");
}

export function fetchClientDetail(clientKey: string): Promise<ClientDetailPayload> {
  return apiJson<ClientDetailPayload>(`/ui/api/clients/${encodeURIComponent(clientKey)}`);
}

export function createClient(payload: Record<string, unknown>): Promise<ClientDetailPayload["client"] & { webhook_urls?: Record<string, string> }> {
  return apiJson("/admin/clients", jsonRequest("POST", payload));
}

export function updateClient(clientKey: string, payload: Record<string, unknown>): Promise<ClientDetailPayload["client"]> {
  return apiJson(`/admin/clients/${encodeURIComponent(clientKey)}`, jsonRequest("PATCH", payload));
}

export function fetchAutomationHealth(clientKey: string): Promise<AutomationHealthPayload> {
  return apiJson<AutomationHealthPayload>(`/ui/api/clients/${encodeURIComponent(clientKey)}/automation-health`);
}

export function fetchAuditLogs(clientKey: string, limit = 80): Promise<AuditLogItem[]> {
  return apiJson<AuditLogItem[]>(withQuery(`/admin/clients/${encodeURIComponent(clientKey)}/audit-logs`, { limit }));
}

export function fetchOwnerWorkspace(clientKey: string): Promise<OwnerWorkspacePayload> {
  return apiJson<OwnerWorkspacePayload>(`/ui/api/owner/${encodeURIComponent(clientKey)}`);
}

export function updateOwnerAiContext(clientKey: string, payload: { ai_context: string; faq_context?: string }): Promise<{ status: string; ai_context: string; faq_context: string; updated_at: string }> {
  return apiJson(`/ui/api/owner/${encodeURIComponent(clientKey)}/ai-context`, jsonRequest("PATCH", payload));
}

export function fetchOwnerKnowledge(clientKey: string): Promise<KnowledgePayload> {
  return apiJson<KnowledgePayload>(`/ui/api/owner/${encodeURIComponent(clientKey)}/knowledge`);
}

export function clearOwnerKnowledge(clientKey: string): Promise<KnowledgePayload> {
  return apiJson<KnowledgePayload>(
    `/ui/api/owner/${encodeURIComponent(clientKey)}/knowledge`,
    { method: "DELETE" }
  );
}

export function ingestOwnerKnowledge(clientKey: string, payload: { urls: string[]; replace: boolean }): Promise<KnowledgePayload> {
  return apiJson<KnowledgePayload>(`/ui/api/owner/${encodeURIComponent(clientKey)}/knowledge/ingest`, jsonRequest("POST", payload));
}

export function fetchOwnerKnowledgeJobStatus(clientKey: string, jobId: string): Promise<KnowledgeJobStatus> {
  return apiJson<KnowledgeJobStatus>(`/ui/api/owner/${encodeURIComponent(clientKey)}/knowledge/jobs/${encodeURIComponent(jobId)}`);
}

export function updateOwnerCalendar(clientKey: string, payload: OwnerCalendarConfig): Promise<{ status: string; client_key: string; booking_mode: string; internal_calendar: OwnerCalendarConfig; updated_at: string }> {
  return apiJson(`/ui/api/owner/${encodeURIComponent(clientKey)}/calendar`, jsonRequest("PATCH", payload));
}

export function fetchRuntimeStatus(): Promise<RuntimeConfigStatus> {
  return apiJson<RuntimeConfigStatus>("/admin/runtime-config/status");
}

export function updateRuntimeConfig(payload: { openai_api_key?: string; openai_model?: string; ai_provider_mode?: string }): Promise<{ updated_keys: string[]; secret_keys_updated: string[] }> {
  return apiJson("/admin/runtime-config", jsonRequest("PUT", payload));
}

export function startSandbox(clientKey: string, payload: SandboxStartPayload): Promise<SandboxStartResponse> {
  return apiJson<SandboxStartResponse>(`/ui/api/owner/${encodeURIComponent(clientKey)}/sandbox/start`, jsonRequest("POST", payload));
}

export function sendSandboxMessage(leadId: number, body: string): Promise<SandboxMessageResponse> {
  return apiJson<SandboxMessageResponse>(
    `/ui/api/conversations/${leadId}/sandbox/messages`,
    jsonRequest("POST", { body })
  );
}

export function fetchCrmLeads(params: Record<string, string | number | boolean | null | undefined> = {}): Promise<CrmLeadsPayload> {
  return apiJson<CrmLeadsPayload>(withQuery("/ui/api/crm/leads", params));
}

export function createCrmLead(payload: ManualLeadCreatePayload): Promise<ManualLeadCreateResponse> {
  return apiJson<ManualLeadCreateResponse>("/ui/api/crm/leads", jsonRequest("POST", payload));
}

export function fetchLeadDetail(leadId: number): Promise<LeadDetailPayload> {
  return apiJson<LeadDetailPayload>(`/ui/api/crm/leads/${leadId}`);
}

export function updateLeadStage(leadId: number, stage: string): Promise<{ status: string; lead_id: number; crm_stage: string; changed: boolean }> {
  return apiJson(`/ui/api/crm/leads/${leadId}/stage`, jsonRequest("PATCH", { stage }));
}

export function addLeadTag(leadId: number, tag: string): Promise<{ status: string; tags: string[] }> {
  return apiJson(`/ui/api/crm/leads/${leadId}/tags`, jsonRequest("POST", { tag }));
}

export function deleteLeadTag(leadId: number, tag: string): Promise<{ status: string; tags: string[] }> {
  return apiJson(`/ui/api/crm/leads/${leadId}/tags/${encodeURIComponent(tag)}`, { method: "DELETE" });
}

export function addLeadNote(leadId: number, note: string): Promise<{ status: string; note: unknown }> {
  return apiJson(`/ui/api/crm/leads/${leadId}/notes`, jsonRequest("POST", { note }));
}

export function archiveLead(leadId: number, archived: boolean): Promise<{ status: string; lead_id: number; archived: boolean; changed: boolean; tags: string[] }> {
  return apiJson(`/ui/api/conversations/${leadId}/archive`, jsonRequest("PATCH", { archived }));
}

export function fetchTasks(params: Record<string, string | number | boolean | null | undefined> = {}): Promise<TasksPayload> {
  return apiJson<TasksPayload>(withQuery("/ui/api/crm/tasks", params));
}

export function createLeadTask(leadId: number, payload: { title: string; description?: string; due_date?: string }): Promise<{ status: string; task: unknown }> {
  return apiJson(`/ui/api/crm/leads/${leadId}/tasks`, jsonRequest("POST", payload));
}

export function updateTask(taskId: number, payload: { status?: string; title?: string; description?: string; due_date?: string | null }): Promise<{ status: string; task: unknown }> {
  return apiJson(`/ui/api/crm/tasks/${taskId}`, jsonRequest("PATCH", payload));
}

export function fetchCalendar(clientKey: string): Promise<CalendarPayload> {
  return apiJson<CalendarPayload>(`/ui/api/clients/${encodeURIComponent(clientKey)}/calendar`);
}

export function updateMeetingStatus(bookingId: number, status: string): Promise<{ status: string; meeting: unknown }> {
  return apiJson(`/ui/api/calendar/meetings/${bookingId}`, jsonRequest("PATCH", { status }));
}

export function deleteMeeting(bookingId: number): Promise<{ status: string; deleted: boolean; booking_id: number }> {
  return apiJson(`/ui/api/calendar/meetings/${bookingId}`, { method: "DELETE" });
}

export function createMeeting(clientKey: string, payload: ManualMeetingCreatePayload): Promise<{ status: string; meeting: CalendarPayload["items"][number] }> {
  return apiJson(`/ui/api/clients/${encodeURIComponent(clientKey)}/calendar/meetings`, jsonRequest("POST", payload));
}

export function fetchConversations(params: Record<string, string | number | boolean | null | undefined> = {}): Promise<ConversationsPayload> {
  return apiJson<ConversationsPayload>(withQuery("/ui/api/conversations", params));
}

export function fetchConversationThread(leadId: number): Promise<ConversationThreadPayload> {
  return apiJson<ConversationThreadPayload>(`/ui/api/conversations/${leadId}/thread`);
}

export function sendManualMessage(
  leadId: number,
  body: string,
  pauseAgent = true,
  idempotencyKey?: string
): Promise<{ status: string; lead_id: number; provider_sid: string; state: string }> {
  return apiJson(`/ui/api/conversations/${leadId}/messages/manual`, {
    ...jsonRequest("POST", { body, pause_agent: pauseAgent }),
    headers: {
      "Content-Type": "application/json",
      ...(idempotencyKey ? { "Idempotency-Key": idempotencyKey } : {})
    }
  });
}

export function sendManualMediaMessage(
  leadId: number,
  body: string,
  media: File,
  idempotencyKey?: string
): Promise<{ status: string; lead_id: number; provider_sid: string; state: string }> {
  const formData = new FormData();
  formData.append("body", body);
  formData.append("media", media);
  return apiJson(`/ui/api/conversations/${leadId}/messages/manual-media`, {
    method: "POST",
    headers: idempotencyKey ? { "Idempotency-Key": idempotencyKey } : undefined,
    body: formData
  });
}

export function sendBookingLink(
  leadId: number,
  message = "Here is the booking link whenever you are ready.",
  idempotencyKey?: string
): Promise<{ status: string; provider_sid: string; body: string; state: string }> {
  return apiJson(`/ui/api/conversations/${leadId}/actions/booking-link`, {
    ...jsonRequest("POST", { message }),
    headers: {
      "Content-Type": "application/json",
      ...(idempotencyKey ? { "Idempotency-Key": idempotencyKey } : {})
    }
  });
}

export function updateAgentControl(
  leadId: number,
  paused: boolean,
  reason = paused ? "operator_paused" : "operator_resumed",
  note = ""
): Promise<{ status: string; lead_id: number; state: string; agent_control: AgentControl }> {
  return apiJson(`/ui/api/conversations/${leadId}/agent-control`, jsonRequest("PATCH", { paused, reason, note }));
}

export function deleteConversation(leadId: number): Promise<{ status: string; deleted_lead_id: number }> {
  return apiJson(`/ui/api/conversations/${leadId}`, { method: "DELETE" });
}

export function addConversationNote(leadId: number, note: string): Promise<{ status: string; note: unknown }> {
  return apiJson(`/ui/api/conversations/${leadId}/notes`, jsonRequest("POST", { note }));
}

export function markConversationHandoff(leadId: number, note = ""): Promise<{ status: string; state: string }> {
  return apiJson(`/ui/api/conversations/${leadId}/actions/handoff`, jsonRequest("POST", { note }));
}

export function loginClient(email: string, password: string): Promise<ClientPortalLoginResponse> {
  return apiJson<ClientPortalLoginResponse>("/ui/api/login/client", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ email, password })
  });
}

export function loginAdmin(adminToken: string): Promise<{ status: "ok"; session: SessionPayload }> {
  return apiJson("/ui/api/login/admin", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ admin_token: adminToken })
  });
}

export function logoutSession(): Promise<{ status: "ok" }> {
  return apiJson("/ui/api/logout", { method: "POST" });
}

function readCookie(name: string): string {
  const prefix = `${encodeURIComponent(name)}=`;
  const match = document.cookie
    .split(";")
    .map((item) => item.trim())
    .find((item) => item.startsWith(prefix));
  if (!match) return "";
  try {
    return decodeURIComponent(match.slice(prefix.length));
  } catch {
    return "";
  }
}

function parseJson(text: string): unknown {
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

function jsonRequest(method: string, payload: unknown): RequestInit {
  return {
    method,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  };
}

function withQuery(path: string, params: Record<string, string | number | boolean | null | undefined>): string {
  const query = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value === null || value === undefined || value === "") continue;
    query.set(key, String(value));
  }
  const serialized = query.toString();
  return serialized ? `${path}?${serialized}` : path;
}

function errorMessage(payload: unknown, fallback: string): string {
  if (typeof payload === "object" && payload && "detail" in payload) {
    const detail = (payload as { detail: unknown }).detail;
    return typeof detail === "string" ? detail : fallback;
  }
  return fallback || "Request failed";
}
