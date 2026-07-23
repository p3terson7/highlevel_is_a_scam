import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { App } from "../../app/App";

describe("Phase 8 admin/config islands", () => {
  beforeEach(() => {
    document.body.innerHTML = "";
    window.localStorage.clear();
    vi.restoreAllMocks();
  });

  it("renders clients, hides the legacy clients layout, and saves an existing client", async () => {
    const legacySurface = document.createElement("div");
    legacySurface.className = "surface";
    const legacyShell = document.createElement("div");
    legacyShell.className = "two-column-shell";
    document.body.appendChild(activeView("view-clients", legacySurface, legacyShell));
    const fetchMock = vi.fn(fetchStub);
    vi.stubGlobal("fetch", fetchMock);

    render(<App mode="clients" />);

    await waitFor(() => expect(screen.getByTestId("react-clients-page")).toBeInTheDocument());
    await waitFor(() => expect(legacySurface).toHaveAttribute("hidden"));
    expect(legacyShell).toHaveAttribute("hidden");
    expect(screen.getByRole("searchbox", { name: /search clients/i })).toBeInTheDocument();
    expect(screen.getByText("Recent conversations")).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Overview" })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Edit" })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Webhooks" })).toBeInTheDocument();

    fireEvent.click(screen.getByRole("tab", { name: "Edit" }));
    expect(screen.getByRole("button", { name: "Business" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Channels" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Agent" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Booking" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Portal" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Review" })).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Channels" }));
    expect(screen.getByText("Provider credentials (client scoped)")).toBeInTheDocument();
    expect(screen.queryByLabelText(/meta/i)).not.toBeInTheDocument();
    expect(screen.queryByLabelText(/linkedin/i)).not.toBeInTheDocument();
    expect(screen.getByLabelText("CRM intake webhook secret")).toHaveValue("");
    expect(screen.getByLabelText("CRM intake webhook secret")).toHaveAttribute("placeholder", "configured");
    expect(screen.getByLabelText("Zapier booking signing secret")).toHaveValue("");
    expect(screen.getByLabelText("Zapier booking signing secret")).toHaveAttribute("placeholder", "configured");
    fireEvent.click(screen.getByLabelText(/remove this client's saved twilio credentials/i));
    fireEvent.click(screen.getByLabelText(/remove this client's saved crm\/zapier webhook credentials/i));
    fireEvent.click(screen.getByRole("button", { name: "Business" }));

    fireEvent.change(screen.getByLabelText("Business name"), {
      target: { value: "Demo Client Updated" }
    });
    fireEvent.click(screen.getByRole("button", { name: /save client/i }));

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        "/admin/clients/demo",
        expect.objectContaining({ method: "PATCH" })
      );
    });
    const updateCall = (fetchMock.mock.calls as Array<[RequestInfo | URL, RequestInit?]>).find((call) => call[0] === "/admin/clients/demo" && call[1]?.method === "PATCH");
    const updatePayload = JSON.parse(updateCall?.[1]?.body as string);
    expect(updatePayload.booking_config).toMatchObject({
      calendly_event_type_uri: "https://api.calendly.test/event-types/1",
      internal_calendar: sampleWorkspace.client.booking_config.internal_calendar
    });
    expect(updatePayload.booking_config).not.toHaveProperty("calendly_personal_access_token");
    expect(updatePayload.booking_config).not.toHaveProperty("calendly_personal_access_token_configured");
    expect(updatePayload.provider_config).toMatchObject({ public_base_url: "https://example.test" });
    expect(updatePayload.provider_config).toHaveProperty("language", "en");
    expect(updatePayload.provider_config).not.toHaveProperty("meta_access_token");
    expect(updatePayload.provider_config).not.toHaveProperty("linkedin_verify_token");
    expect(updatePayload.provider_config).not.toHaveProperty("twilio_from_number");
    expect(updatePayload.provider_config_clear_keys).toEqual([
      "twilio_account_sid",
      "twilio_auth_token",
      "twilio_from_number",
      "crm_webhook_secret",
      "zapier_booking_webhook_secret",
      "zapier_webhook_secret",
      "zapier_booking_webhook_url"
    ]);
  });

  it("writes split webhook secrets and clears the legacy inbound alias", async () => {
    document.body.appendChild(activeView("view-clients", document.createElement("div"), document.createElement("div")));
    const fetchMock = vi.fn(fetchStub);
    vi.stubGlobal("fetch", fetchMock);

    render(<App mode="clients" />);

    await waitFor(() => expect(screen.getByTestId("react-clients-page")).toBeInTheDocument());
    fireEvent.click(screen.getByRole("tab", { name: "Edit" }));
    fireEvent.click(screen.getByRole("button", { name: "Channels" }));
    fireEvent.change(screen.getByLabelText("CRM intake webhook secret"), {
      target: { value: "new-inbound-secret" }
    });
    fireEvent.change(screen.getByLabelText("Zapier booking signing secret"), {
      target: { value: "new-outbound-secret" }
    });
    fireEvent.click(screen.getByRole("button", { name: /save client/i }));

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        "/admin/clients/demo",
        expect.objectContaining({ method: "PATCH" })
      );
    });
    const updateCall = (fetchMock.mock.calls as Array<[RequestInfo | URL, RequestInit?]>).find((call) => call[0] === "/admin/clients/demo" && call[1]?.method === "PATCH");
    const updatePayload = JSON.parse(updateCall?.[1]?.body as string);
    expect(updatePayload.provider_config).toMatchObject({
      crm_webhook_secret: "new-inbound-secret",
      zapier_booking_webhook_secret: "new-outbound-secret"
    });
    expect(updatePayload.provider_config).not.toHaveProperty("zapier_webhook_secret");
    expect(updatePayload.provider_config_clear_keys).toEqual(["zapier_webhook_secret"]);
  });

  it("creates a new client through the existing admin endpoint", async () => {
    document.body.appendChild(activeView("view-clients", document.createElement("div"), document.createElement("div")));
    const fetchMock = vi.fn(fetchStub);
    vi.stubGlobal("fetch", fetchMock);

    render(<App mode="clients" />);

    await waitFor(() => expect(screen.getByTestId("react-clients-page")).toBeInTheDocument());
    fireEvent.click(screen.getByRole("button", { name: /new client/i }));
    fireEvent.change(screen.getByLabelText("Business name"), {
      target: { value: "New Co" }
    });
    fireEvent.change(screen.getByLabelText("Client key"), {
      target: { value: "newco" }
    });
    fireEvent.click(screen.getByRole("button", { name: /save client/i }));

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        "/admin/clients",
        expect.objectContaining({ method: "POST" })
      );
    });
  });

  it("renders settings, hides the legacy settings layout after data loads, and saves AI context", async () => {
    const legacySurface = document.createElement("div");
    legacySurface.className = "surface";
    const legacyLayout = document.createElement("div");
    legacyLayout.id = "settingsLayout";
    document.body.appendChild(activeView("view-settings", legacySurface, legacyLayout));
    const fetchMock = vi.fn(fetchStub);
    vi.stubGlobal("fetch", fetchMock);

    render(<App mode="settings" />);

    await waitFor(() => expect(screen.getByTestId("react-settings-page")).toBeInTheDocument());
    expect(legacySurface).toHaveAttribute("hidden");
    expect(legacyLayout).toHaveAttribute("hidden");
    expect(screen.getByPlaceholderText(/configured; leave blank/i)).toBeInTheDocument();

    fireEvent.change(screen.getByLabelText("AI context"), {
      target: { value: "Use the current business playbook." }
    });
    fireEvent.click(screen.getByRole("button", { name: /save ai context/i }));

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        "/ui/api/owner/demo/ai-context",
        expect.objectContaining({ method: "PATCH" })
      );
    });
  });

  it("saves runtime, website knowledge, and calendar settings through existing endpoints", async () => {
    document.body.appendChild(activeView("view-settings", document.createElement("div"), document.createElement("div")));
    const fetchMock = vi.fn(fetchStub);
    vi.stubGlobal("fetch", fetchMock);

    render(<App mode="settings" />);

    await waitFor(() => expect(screen.getByTestId("react-settings-page")).toBeInTheDocument());
    expect(screen.getByRole("region", { name: /settings readiness/i })).toBeInTheDocument();

    fireEvent.change(screen.getByLabelText("OpenAI model"), {
      target: { value: "gpt-5-mini" }
    });
    fireEvent.click(screen.getByRole("button", { name: /save ai settings/i }));
    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        "/admin/runtime-config",
        expect.objectContaining({ method: "PUT" })
      );
    });
    const runtimeCall = (fetchMock.mock.calls as Array<[RequestInfo | URL, RequestInit?]>).find((call) => call[0] === "/admin/runtime-config" && call[1]?.method === "PUT");
    expect(JSON.parse(runtimeCall?.[1]?.body as string)).toEqual({ openai_model: "gpt-5-mini" });

    fireEvent.change(screen.getByLabelText("Source URLs"), {
      target: { value: "https://example.test/services" }
    });
    fireEvent.click(screen.getByRole("button", { name: /ingest urls/i }));
    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        "/ui/api/owner/demo/knowledge/ingest",
        expect.objectContaining({ method: "POST" })
      );
    });
    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        "/ui/api/owner/demo/knowledge/jobs/knowledge-job-1",
        expect.any(Object)
      );
    });
    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        "/ui/api/owner/demo/knowledge",
        expect.any(Object)
      );
      expect(screen.getByText(/extracted 1 source into 3 chunks/i)).toBeInTheDocument();
    });

    fireEvent.change(screen.getByLabelText("Meeting length"), {
      target: { value: "45" }
    });
    fireEvent.click(screen.getByRole("button", { name: /save calendar availability/i }));
    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        "/ui/api/owner/demo/calendar",
        expect.objectContaining({ method: "PATCH" })
      );
    });
  });

  it("seeds, reseeds, resets, and creates selected-client showcase data", async () => {
    document.body.appendChild(activeView("view-settings", document.createElement("div"), document.createElement("div")));
    const fetchMock = vi.fn(fetchStub);
    vi.stubGlobal("fetch", fetchMock);
    vi.spyOn(window, "confirm").mockReturnValue(true);

    render(<App mode="settings" />);

    await waitFor(() => expect(screen.getByRole("heading", { name: /demo and showcase data/i })).toBeInTheDocument());
    fireEvent.click(screen.getByRole("button", { name: /^seed demo clients$/i }));
    await waitFor(() => expect(fetchMock).toHaveBeenCalledWith("/ui/api/seed-demo", expect.objectContaining({ method: "POST" })));
    expect(await screen.findByText("Demo seed completed.")).toBeInTheDocument();
    expect(document.body).not.toHaveTextContent("demo-password-must-not-render");

    fireEvent.click(screen.getByRole("button", { name: /^reseed demo clients$/i }));
    await waitFor(() => expect(fetchMock).toHaveBeenCalledWith("/ui/api/seed-demo?reset=true", expect.objectContaining({ method: "POST" })));

    fireEvent.click(screen.getByRole("button", { name: /^seed selected client showcase$/i }));
    await waitFor(() => expect(fetchMock).toHaveBeenCalledWith("/ui/api/seed-showcase/demo", expect.objectContaining({ method: "POST" })));

    fireEvent.click(screen.getByRole("button", { name: /^reset demo clients$/i }));
    expect(window.confirm).toHaveBeenCalled();
    await waitFor(() => expect(fetchMock).toHaveBeenCalledWith("/ui/api/seed-demo", expect.objectContaining({ method: "DELETE" })));
  });

  it("runs the GPT + Zapier sandbox and exposes provider, intake, and automation probes", async () => {
    const legacySurface = document.createElement("div");
    legacySurface.className = "surface";
    const legacyShell = document.createElement("div");
    legacyShell.className = "test-lab-shell";
    document.body.appendChild(activeView("view-test-lab", legacySurface, legacyShell));
    const fetchMock = vi.fn(fetchStub);
    vi.stubGlobal("fetch", fetchMock);

    render(<App mode="test-lab" />);

    await waitFor(() => expect(screen.getByTestId("react-test-lab-page")).toBeInTheDocument());
    expect(legacySurface).toHaveAttribute("hidden");
    expect(legacyShell).toHaveAttribute("hidden");
    expect(screen.getByRole("button", { name: /gpt only/i })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /^gpt \+ zapier/i })).toBeEnabled();
    expect(screen.getByRole("heading", { name: /provider probes/i })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: /form and zapier intake/i })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: /automation health/i })).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: /^gpt \+ zapier/i }));
    fireEvent.click(screen.getByRole("button", { name: /^start gpt \+ zapier sandbox$/i }));

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        "/ui/api/owner/demo/sandbox/start",
        expect.objectContaining({ method: "POST" })
      );
    });
    const sandboxCall = (fetchMock.mock.calls as Array<[RequestInfo | URL, RequestInit?]>).find((call) => call[0] === "/ui/api/owner/demo/sandbox/start" && call[1]?.method === "POST");
    const sandboxPayload = JSON.parse(sandboxCall?.[1]?.body as string);
    expect(sandboxPayload).toMatchObject({
      mode: "gpt_zapier",
      full_name: "Martin Gagnon / Fonderie Laurentide",
      phone: "+14185550147",
      email: "martin.gagnon@example.com",
      city: "Québec, QC"
    });
    expect(sandboxPayload.form_answers).toEqual([
      { question: "Secteur d'activité", answer: "Entreprise" },
      { question: "Dimensions de l'objet — Hauteur", answer: "45 mm" },
      { question: "Dimensions de l'objet — Largeur", answer: "280 mm" },
      { question: "Dimensions de l'objet — Longueur", answer: "280 mm" },
      { question: "Dimensions de l'objet — Autres", answer: "Roue dentée en acier, environ 18 kg" },
      { question: "Joindre des fichiers (Images, STL...)", answer: "Deux photos de la pièce brisée et une ancienne fiche fournisseur sont disponibles." },
      { question: "Délai de réalisation souhaité?", answer: "Dans les 5 jours ouvrables" },
      { question: "La demande est-elle urgente?", answer: "Oui — arrêt partiel de production" },
      { question: "Sélectionner les services requis", answer: "Scan 3D, Rétro-ingénierie (3D & 2D), Modélisation" },
      {
        question: "Informations additionnelles",
        answer: "Une roue dentée est brisée et nous n'avons aucun plan CAD exploitable. Nous avons la pièce cassée et une pièce usée de référence; il nous faut un fichier STEP et un dessin technique pour la refaire usiner."
      }
    ]);
    const submittedQuestions = sandboxPayload.form_answers.map((row: { question: string }) => row.question);
    expect(submittedQuestions).not.toContain("Timeline");
    expect(submittedQuestions).not.toContain("Service interest");
    expect(await screen.findByText(/Sandbox started/i)).toBeInTheDocument();
    expect(screen.getByText(/waiting_for_booking/i)).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: /^run ai probe$/i }));
    await waitFor(() => expect(fetchMock).toHaveBeenCalledWith("/admin/test/ai", expect.objectContaining({ method: "POST" })));
    expect(await screen.findByText(/AI probe completed with openai/i)).toBeInTheDocument();

    fireEvent.change(screen.getByLabelText("Test SMS recipient"), { target: { value: "+15555550199" } });
    fireEvent.click(screen.getByRole("checkbox", { name: /understand this sends a live sms/i }));
    fireEvent.click(screen.getByRole("button", { name: /^send test sms$/i }));
    await waitFor(() => expect(fetchMock).toHaveBeenCalledWith("/admin/test/sms", expect.objectContaining({ method: "POST" })));
    expect(await screen.findByText(/Twilio accepted the message/i)).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: /^zapier webhook/i }));
    fireEvent.change(screen.getByLabelText("Webhook secret (write-only)"), { target: { value: "one-request-secret" } });
    fireEvent.click(screen.getByRole("checkbox", { name: /creates a crm contact/i }));
    fireEvent.click(screen.getByRole("button", { name: /^submit zapier test$/i }));

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        "/webhooks/zapier/demo",
        expect.objectContaining({ method: "POST" })
      );
    });
    const intakeCall = (fetchMock.mock.calls as Array<[RequestInfo | URL, RequestInit?]>).find((call) => call[0] === "/webhooks/zapier/demo" && call[1]?.method === "POST");
    expect(new Headers(intakeCall?.[1]?.headers).get("X-Zapier-Webhook-Secret")).toBe("one-request-secret");
    expect(screen.getByLabelText("Webhook secret (write-only)")).toHaveValue("");
    expect(await screen.findByText(/Intake event accepted/i)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /open thread/i })).toHaveAttribute("data-lead-id", "101");
  });
});

function activeView(id: string, ...children: HTMLElement[]) {
  const view = document.createElement("section");
  view.id = id;
  view.className = "view active";
  children.forEach((child) => view.appendChild(child));
  return view;
}

async function fetchStub(input: RequestInfo | URL, init?: RequestInit) {
  const url = String(input);
  if (url === "/ui/api/session") return jsonResponse(sampleSession);
  if (url === "/ui/api/clients") return jsonResponse(sampleClients);
  if (url === "/admin/runtime-config/status") return jsonResponse(sampleRuntime);
  if (url === "/admin/runtime-config" && init?.method === "PUT") {
    return jsonResponse({ updated_keys: ["openai_model"], secret_keys_updated: [] });
  }
  if (url === "/ui/api/owner/demo") return jsonResponse(sampleWorkspace);
  if (url === "/ui/api/clients/demo") return jsonResponse(sampleClientDetail);
  if (url === "/ui/api/clients/demo/automation-health") return jsonResponse(sampleAutomationHealth);
  if (url === "/ui/api/clients/demo/zapier-results?limit=30") return jsonResponse(sampleZapierConsole);
  if (url === "/admin/test/ai" && init?.method === "POST") {
    return jsonResponse({ status: "ok", provider: "openai", reply_text: "I can help with that.", next_state: "QUALIFYING", action: "reply" });
  }
  if (url === "/admin/test/sms" && init?.method === "POST") {
    return jsonResponse({ status: "ok", provider_sid: "SM-TEST-123" });
  }
  if (url === "/webhooks/form/demo" && init?.method === "POST") {
    return jsonResponse({ status: "accepted", source: "website", client_key: "demo" }, 202);
  }
  if (url === "/webhooks/zapier/demo" && init?.method === "POST") {
    return jsonResponse({ status: "accepted", source: "zapier", client_key: "demo" }, 202);
  }
  if (url === "/ui/api/seed-demo" && init?.method === "POST") return jsonResponse({ seeded: true, clients_created: 4, portal_password: "demo-password-must-not-render" });
  if (url === "/ui/api/seed-demo?reset=true" && init?.method === "POST") return jsonResponse({ seeded: true, reset: true });
  if (url === "/ui/api/seed-demo" && init?.method === "DELETE") return jsonResponse({ status: "ok", clients_deleted: 4 });
  if (url === "/ui/api/seed-showcase/demo" && init?.method === "POST") return jsonResponse({ seeded: true, client_key: "demo" });
  if (url === "/ui/api/seed-showcase/demo?reset=true" && init?.method === "POST") return jsonResponse({ seeded: true, reset: true, client_key: "demo" });
  if (url === "/admin/clients/demo" && init?.method === "PATCH") {
    return jsonResponse({ ...sampleClientDetail.client, business_name: "Demo Client Updated" });
  }
  if (url === "/admin/clients" && init?.method === "POST") {
    return jsonResponse({ ...sampleClientDetail.client, id: 2, client_key: "newco", business_name: "New Co" });
  }
  if (url === "/ui/api/clients/newco") {
    return jsonResponse({
      ...sampleClientDetail,
      client: { ...sampleClientDetail.client, id: 2, client_key: "newco", business_name: "New Co" }
    });
  }
  if (url === "/ui/api/clients/newco/automation-health") return jsonResponse(sampleAutomationHealth);
  if (url === "/ui/api/owner/demo/ai-context" && init?.method === "PATCH") {
    return jsonResponse({
      status: "ok",
      client_key: "demo",
      ai_context: "Use the current business playbook.",
      faq_context: "Facts only.",
      updated_at: "2026-06-10T12:30:00Z"
    });
  }
  if (url === "/ui/api/owner/demo/knowledge/ingest" && init?.method === "POST") {
    return jsonResponse({
      ...sampleWorkspace.knowledge,
      status: "queued",
      job_id: "knowledge-job-1",
      total_sources: 1,
      total_chunks: 2
    });
  }
  if (url === "/ui/api/owner/demo/knowledge/jobs/knowledge-job-1") {
    return jsonResponse({
      client_key: "demo",
      job_id: "knowledge-job-1",
      status: "ok",
      terminal: true,
      total_pages: 1,
      failed_pages: 0,
      total_chunks: 3
    });
  }
  if (url === "/ui/api/owner/demo/knowledge") {
    return jsonResponse({
      ...sampleWorkspace.knowledge,
      status: "ok",
      total_sources: 1,
      total_chunks: 3,
      sources: [{ url: "https://example.test/services", title: "Services refreshed", status: "ok", chunk_count: 3 }]
    });
  }
  if (url === "/ui/api/owner/demo/calendar" && init?.method === "PATCH") {
    return jsonResponse({
      status: "ok",
      client_key: "demo",
      booking_mode: "internal",
      internal_calendar: { ...sampleWorkspace.client.booking_config.internal_calendar, slot_minutes: 45 },
      updated_at: "2026-06-10T12:45:00Z"
    });
  }
  if (url === "/ui/api/owner/demo/sandbox/start" && init?.method === "POST") {
    const request = JSON.parse(String(init.body || "{}"));
    return jsonResponse({
      status: "ok",
      lead_id: 101,
      mode: request.mode || "gpt_only",
      state: "GREETED",
      body: "Hi Sam, I’m Hermes, the assistant for Demo Client.",
      booking_debug: null,
      zapier_booking_webhook: request.mode === "gpt_zapier"
        ? { enabled: true, status: "waiting_for_booking" }
        : { enabled: false, status: "disabled" }
    });
  }
  return jsonResponse({ detail: `Unhandled ${url}` }, 404);
}

function jsonResponse(payload: unknown, status = 200) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { "Content-Type": "application/json" }
  });
}

const sampleSession = {
  status: "ok",
  role: "admin",
  app_name: "Lead Ops Console",
  env: "test",
  generated_at: "2026-06-10T12:00:00Z",
  can_seed_demo: true,
  demo_data_present: true,
  client_key: null,
  client_name: null,
  portal_display_name: null
};

const sampleClients = [
  {
    id: 1,
    client_key: "demo",
    business_name: "Demo Client",
    tone: "friendly",
    timezone: "America/Toronto",
    booking_url: "",
    is_active: true,
    portal_enabled: true,
    lead_count: 3,
    open_conversations: 2,
    last_activity_at: "2026-06-10T12:00:00Z",
    last_webhook_received_at: "2026-06-10T11:00:00Z"
  }
];

const sampleRuntime = {
  openai_api_key_configured: true,
  openai_model: "gpt-5-mini",
  ai_provider_mode: "auto"
};

const sampleWorkspace = {
  client: {
    client_key: "demo",
    business_name: "Demo Client",
    booking_url: "",
    booking_mode: "internal",
    booking_config: {
      calendly_personal_access_token: "calendly-secret",
      calendly_event_type_uri: "https://api.calendly.test/event-types/1",
      internal_calendar: {
        slot_minutes: 30,
        notice_minutes: 120,
        horizon_days: 14,
        availability: [{ day: 0, start: "09:00", end: "17:00", enabled: true }]
      }
    },
    provider_config: {},
    fallback_handoff_number: "",
    timezone: "America/Toronto",
    tone: "friendly",
    faq_context: "Facts only.",
    ai_context: "Existing playbook.",
    twilio_inbound_path: "/sms/inbound/demo"
  },
  runtime: {
    twilio_configured: true,
    ai_configured: true,
    zapier_webhook_secret_configured: true,
    zapier_booking_webhook_secret_configured: true,
    zapier_booking_webhook_url_configured: true,
    twilio_from_number: "+15551234567",
    openai_model: "gpt-5-mini",
    ai_provider_mode: "auto",
    public_base_url: "https://example.test",
    source: "global"
  },
  delivery_mode: "twilio",
  knowledge: {
    status: "ok",
    client_key: "demo",
    total_sources: 1,
    total_chunks: 2,
    sources: [{ url: "https://example.test/services", title: "Services", status: "ok", chunk_count: 2 }]
  },
  live_test_checklist: [
    { label: "Twilio configured", done: true, detail: "+15551234567" },
    { label: "AI configured", done: true, detail: "gpt-5-mini" }
  ],
  conversations: []
};

const sampleClientDetail = {
  client: {
    id: 1,
    client_key: "demo",
    business_name: "Demo Client",
    tone: "friendly",
    timezone: "America/Toronto",
    qualification_questions: ["Timeline?"],
    booking_url: "",
    booking_mode: "internal",
    booking_config: sampleWorkspace.client.booking_config,
    provider_config: { language: "en", public_base_url: "https://example.test" },
    fallback_handoff_number: "",
    consent_text: "Reply STOP to opt out.",
    portal_display_name: "Owner",
    portal_email: "owner@example.test",
    portal_enabled: true,
    portal_password_configured: true,
    operating_hours: {},
    faq_context: "Facts only.",
    ai_context: "Existing playbook.",
    template_overrides: {},
    is_active: true,
    created_at: "2026-06-01T12:00:00Z",
    updated_at: "2026-06-10T12:00:00Z"
  },
  webhook_urls: {
    zapier_events: "/webhooks/zapier/demo",
    website_form: "/webhooks/form/demo",
    twilio_sms: "/sms/inbound/demo"
  },
  provider_runtime: sampleWorkspace.runtime,
  onboarding: sampleWorkspace.live_test_checklist,
  recent_conversations: [
    {
      lead_id: 42,
      lead_name: "Jane Prospect",
      phone: "+15551234567",
      source: "zapier",
      client_key: "demo",
      client_name: "Demo Client",
      state: "QUALIFYING",
      crm_stage: "Qualified",
      tags: ["qualified"],
      last_message_snippet: "Can we talk tomorrow?",
      last_activity_at: "2026-06-10T12:00:00Z"
    }
  ],
  recent_logs: [
    {
      id: 5,
      event_type: "crm_task_created",
      lead_id: 42,
      created_at: "2026-06-10T12:00:00Z",
      decision: { title: "Follow up" }
    }
  ],
  counts: { leads: 3, open_conversations: 2 }
};

const sampleAutomationHealth = {
  client_key: "demo",
  generated_at: "2026-06-10T12:00:00Z",
  status: "healthy",
  needs_attention: 0,
  automations: [
    { key: "twilio", label: "Twilio configured", status: "healthy", configured: true, detail: "+15551234567" },
    { key: "ai", label: "AI configured", status: "healthy", configured: true, detail: "gpt-5-mini" }
  ]
};

const sampleZapierConsole = {
  client_key: "demo",
  webhook_url: "/webhooks/zapier/demo",
  items: [
    {
      id: 90,
      event_type: "zapier_webhook_received",
      lead_id: null,
      created_at: "2026-06-10T12:00:00Z",
      decision: { status: "accepted" }
    }
  ]
};
