import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  apiJson,
  authHeaders,
  clearOwnerKnowledge,
  clearOutboundRequestKey,
  fetchCrmLeads,
  fetchDashboard,
  fetchSession,
  outboundRequestKey,
  sendManualMessage,
  sendSandboxMessage,
  updateLeadStage
} from "./client";

describe("api client", () => {
  beforeEach(() => {
    window.localStorage.clear();
    window.sessionStorage.clear();
    document.cookie = "leadops_csrf=; Max-Age=0; Path=/";
    vi.restoreAllMocks();
  });

  it("strips explicit legacy credentials from every HeadersInit shape", () => {
    const tupleHeaders: Array<[string, string]> = [
      ["x-admin-token", "admin-from-tuples"],
      ["x-portal-token", "portal-from-tuples"],
      ["Accept", "application/json"]
    ];
    const headerObject = {
      "X-Admin-Token": "admin-from-object",
      "X-Portal-Token": "portal-from-object",
      Accept: "application/json"
    };
    const headerInstance = new Headers(headerObject);

    for (const initializer of [headerObject, tupleHeaders, headerInstance] satisfies HeadersInit[]) {
      const headers = authHeaders(initializer);

      expect(headers.get("X-Admin-Token")).toBeNull();
      expect(headers.get("X-Portal-Token")).toBeNull();
      expect(headers.get("Accept")).toBe("application/json");
    }

    expect(headerInstance.get("X-Admin-Token")).toBe("admin-from-object");
    expect(headerInstance.get("X-Portal-Token")).toBe("portal-from-object");
  });

  it("uses same-origin cookie auth and removes a legacy admin token", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ status: "ok" }));
    vi.stubGlobal("fetch", fetchMock);
    window.localStorage.setItem("lead-ui-admin-token", "admin-token");

    await fetchSession();

    const headers = fetchMock.mock.calls[0][1].headers as Headers;
    expect(fetchMock).toHaveBeenCalledWith("/ui/api/session", expect.any(Object));
    expect(fetchMock.mock.calls[0][1].credentials).toBe("same-origin");
    expect(headers.get("X-Admin-Token")).toBeNull();
    expect(headers.get("X-Portal-Token")).toBeNull();
    expect(window.localStorage.getItem("lead-ui-admin-token")).toBeNull();
  });

  it("uses same-origin cookie auth and removes a legacy portal token", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ stats: {} }));
    vi.stubGlobal("fetch", fetchMock);
    window.localStorage.setItem("lead-ui-auth-mode", "client");
    window.localStorage.setItem("lead-ui-portal-token", "portal-token");

    await fetchDashboard();

    const headers = fetchMock.mock.calls[0][1].headers as Headers;
    expect(fetchMock).toHaveBeenCalledWith("/ui/api/dashboard", expect.any(Object));
    expect(headers.get("X-Portal-Token")).toBeNull();
    expect(headers.get("X-Admin-Token")).toBeNull();
    expect(window.localStorage.getItem("lead-ui-portal-token")).toBeNull();
  });

  it("raises an ApiError with backend detail text", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(jsonResponse({ detail: "Invalid token" }, 401)));
    const expired = vi.fn();
    window.addEventListener("lead-ui-auth-expired", expired);

    await expect(apiJson("/ui/api/session")).rejects.toMatchObject({
      name: "ApiError",
      status: 401,
      message: "Invalid token"
    });
    expect(expired).toHaveBeenCalledOnce();
    window.removeEventListener("lead-ui-auth-expired", expired);
  });

  it("builds CRM list queries without exposing authentication headers", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ items: [], counts: {}, total: 0, stages: [] }));
    vi.stubGlobal("fetch", fetchMock);
    window.localStorage.setItem("lead-ui-admin-token", "admin-token");

    await fetchCrmLeads({ client_key: "demo", archived: false, stage: "Qualified" });

    const headers = fetchMock.mock.calls[0][1].headers as Headers;
    expect(fetchMock).toHaveBeenCalledWith("/ui/api/crm/leads?client_key=demo&archived=false&stage=Qualified", expect.any(Object));
    expect(headers.get("X-Admin-Token")).toBeNull();
  });

  it("scopes dashboard requests to the selected admin client", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ stats: {} }));
    vi.stubGlobal("fetch", fetchMock);

    await fetchDashboard("demo client");

    expect(fetchMock).toHaveBeenCalledWith("/ui/api/dashboard?client_key=demo+client", expect.any(Object));
  });

  it("reuses outbound retry keys until content changes or the request succeeds", () => {
    const first = outboundRequestKey("manual-42", "same content");
    expect(outboundRequestKey("manual-42", "same content")).toBe(first);
    expect(window.sessionStorage.getItem("lead-ui-outbound-request:manual-42")).not.toContain("same content");

    const changed = outboundRequestKey("manual-42", "changed content");
    expect(changed).not.toBe(first);
    expect(outboundRequestKey("manual-42", "changed content")).toBe(changed);

    clearOutboundRequestKey("manual-42");
    expect(outboundRequestKey("manual-42", "changed content")).not.toBe(changed);
  });

  it("uses existing write endpoints for stage updates and manual replies", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse({ status: "ok", lead_id: 42, crm_stage: "Won", changed: true }))
      .mockResolvedValueOnce(jsonResponse({ status: "ok", lead_id: 42, provider_sid: "SM1", state: "GREETED" }));
    vi.stubGlobal("fetch", fetchMock);
    document.cookie = "leadops_csrf=csrf-token; Path=/";

    await updateLeadStage(42, "Won");
    await sendManualMessage(42, "Following up.", true, "attempt-123");

    expect(fetchMock.mock.calls[0][0]).toBe("/ui/api/crm/leads/42/stage");
    expect(fetchMock.mock.calls[0][1]).toMatchObject({ method: "PATCH" });
    expect((fetchMock.mock.calls[0][1].headers as Headers).get("X-CSRF-Token")).toBe("csrf-token");
    expect(JSON.parse(fetchMock.mock.calls[0][1].body as string)).toEqual({ stage: "Won" });
    expect(fetchMock.mock.calls[1][0]).toBe("/ui/api/conversations/42/messages/manual");
    expect(fetchMock.mock.calls[1][1]).toMatchObject({ method: "POST" });
    expect(JSON.parse(fetchMock.mock.calls[1][1].body as string)).toEqual({ body: "Following up.", pause_agent: true });
    expect((fetchMock.mock.calls[1][1].headers as Headers).get("Idempotency-Key")).toBe("attempt-123");
    expect((fetchMock.mock.calls[1][1].headers as Headers).get("X-CSRF-Token")).toBe("csrf-token");
  });

  it("sends GPT test-lab turns through the inbound sandbox endpoint", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({
      status: "ok",
      lead_id: 42,
      state: "QUALIFYING",
      crm_stage: "Qualified",
      delivery_mode: "sandbox",
      twilio_bypassed: true,
      inbound_message_id: 3,
      reply: { id: 4, body: "Friday works.", provider_message_sid: "MOCK-OUT-4" }
    }));
    vi.stubGlobal("fetch", fetchMock);
    document.cookie = "leadops_csrf=csrf-token; Path=/";

    await sendSandboxMessage(42, "Are you free Friday?");

    expect(fetchMock).toHaveBeenCalledWith(
      "/ui/api/conversations/42/sandbox/messages",
      expect.objectContaining({ method: "POST" })
    );
    expect(JSON.parse(fetchMock.mock.calls[0][1].body as string)).toEqual({ body: "Are you free Friday?" });
    expect((fetchMock.mock.calls[0][1].headers as Headers).get("X-CSRF-Token")).toBe("csrf-token");
    expect((fetchMock.mock.calls[0][1].headers as Headers).get("Idempotency-Key")).toBeNull();
  });

  it("purges website knowledge with CSRF-protected cookie auth", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({
      status: "ok",
      deleted_sources: 2,
      sources: [],
      total_sources: 0,
      total_chunks: 0
    }));
    vi.stubGlobal("fetch", fetchMock);
    document.cookie = "leadops_csrf=csrf-token; Path=/";

    await clearOwnerKnowledge("demo client");

    expect(fetchMock.mock.calls[0][0]).toBe("/ui/api/owner/demo%20client/knowledge");
    expect(fetchMock.mock.calls[0][1]).toMatchObject({ method: "DELETE" });
    expect((fetchMock.mock.calls[0][1].headers as Headers).get("X-CSRF-Token")).toBe("csrf-token");
  });
});

function jsonResponse(payload: unknown, status = 200) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { "Content-Type": "application/json" }
  });
}
