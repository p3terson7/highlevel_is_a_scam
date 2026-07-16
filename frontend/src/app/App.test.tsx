import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { App } from "./App";

describe("App", () => {
  beforeEach(() => {
    window.localStorage.clear();
    window.history.replaceState({}, "", "/ui");
    document.body.removeAttribute("data-theme");
    document.body.removeAttribute("data-density");
    vi.restoreAllMocks();
  });

  it("renders the isolated React scaffold", () => {
    render(<App />);

    expect(screen.getByRole("heading", { name: /react frontend scaffold/i })).toBeInTheDocument();
    expect(screen.getByText(/before it is mounted into the production fastapi ui/i)).toBeInTheDocument();
  });

  it("renders a non-visual shell island marker", () => {
    const { container } = render(<App mode="island" />);

    expect(container.querySelector("[data-react-island-mounted='true']")).toBeInTheDocument();
    expect(screen.queryByRole("heading", { name: /react frontend scaffold/i })).not.toBeInTheDocument();
  });

  it("renders the React app-shell login when no saved session exists", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(jsonResponse({ detail: "Not authenticated" }, 401)));

    render(<App mode="app-shell" />);

    expect(await screen.findByRole("heading", { name: /sign in to lead ops/i })).toBeInTheDocument();
    expect(screen.getByLabelText(/admin token/i)).toBeInTheDocument();
  });

  it("renders the authenticated React app shell with dashboard navigation", async () => {
    vi.stubGlobal("fetch", vi.fn(fetchStub));

    render(<App mode="app-shell" />);

    expect(await screen.findByTestId("react-app-shell")).toBeInTheDocument();
    await waitFor(() => expect(document.body.dataset.theme).toBe("dark"));
    expect(await screen.findByTestId("react-dashboard-page")).toBeInTheDocument();
    expect(screen.getAllByRole("button", { name: /pipeline/i }).length).toBeGreaterThan(0);
    expect(screen.getByText("Jane Prospect")).toBeInTheDocument();
  });

  it("preserves the saved shell theme in app-shell mode", async () => {
    window.localStorage.setItem("lead-ui-theme", "light");
    vi.stubGlobal("fetch", vi.fn(fetchStub));

    render(<App mode="app-shell" />);

    expect(await screen.findByTestId("react-app-shell")).toBeInTheDocument();
    expect(document.body.dataset.theme).toBe("light");
  });

  it("keeps the authenticated shell active and reports a failed sign out", async () => {
    window.localStorage.setItem("lead-ui-auth-mode", "admin");
    const fetchMock = vi.fn((input: RequestInfo | URL) => {
      if (String(input) === "/ui/api/logout") {
        return Promise.resolve(jsonResponse({ detail: "Logout service unavailable" }, 503));
      }
      return fetchStub(input);
    });
    vi.stubGlobal("fetch", fetchMock);

    render(<App mode="app-shell" />);

    expect(await screen.findByTestId("react-app-shell")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: /sign out/i }));

    expect(await screen.findByRole("alert")).toHaveTextContent(
      /sign out failed\. your session may still be active; please retry\. logout service unavailable/i
    );
    expect(screen.getByTestId("react-app-shell")).toBeInTheDocument();
    expect(window.localStorage.getItem("lead-ui-auth-mode")).toBe("admin");
  });

  it("does not clear the authenticated shell before sign out succeeds", async () => {
    window.localStorage.setItem("lead-ui-auth-mode", "admin");
    let resolveLogout!: (response: Response) => void;
    const pendingLogout = new Promise<Response>((resolve) => {
      resolveLogout = resolve;
    });
    const fetchMock = vi.fn((input: RequestInfo | URL) => {
      if (String(input) === "/ui/api/logout") return pendingLogout;
      return fetchStub(input);
    });
    vi.stubGlobal("fetch", fetchMock);

    render(<App mode="app-shell" />);

    expect(await screen.findByTestId("react-app-shell")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: /sign out/i }));
    await waitFor(() => expect(fetchMock).toHaveBeenCalledWith("/ui/api/logout", expect.any(Object)));
    expect(screen.getByTestId("react-app-shell")).toBeInTheDocument();
    expect(screen.queryByRole("heading", { name: /sign in to lead ops/i })).not.toBeInTheDocument();

    await act(async () => {
      resolveLogout(jsonResponse({ status: "ok" }));
      await pendingLogout;
    });

    expect(await screen.findByRole("heading", { name: /sign in to lead ops/i })).toBeInTheDocument();
    expect(window.localStorage.getItem("lead-ui-auth-mode")).toBeNull();
  });

  it("clears local auth when sign out reports an already-expired session", async () => {
    window.localStorage.setItem("lead-ui-auth-mode", "admin");
    const fetchMock = vi.fn((input: RequestInfo | URL) => {
      if (String(input) === "/ui/api/logout") {
        return Promise.resolve(jsonResponse({ detail: "Not authenticated" }, 401));
      }
      return fetchStub(input);
    });
    vi.stubGlobal("fetch", fetchMock);

    render(<App mode="app-shell" />);

    expect(await screen.findByTestId("react-app-shell")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: /sign out/i }));

    expect(await screen.findByRole("heading", { name: /sign in to lead ops/i })).toBeInTheDocument();
    expect(screen.getByRole("status")).toHaveTextContent("Signed out");
    expect(window.localStorage.getItem("lead-ui-auth-mode")).toBeNull();
  });

  it("honors hashless deep links", async () => {
    window.history.replaceState({}, "", "/crm");
    vi.stubGlobal("fetch", vi.fn(fetchStub));

    render(<App mode="app-shell" />);

    expect(await screen.findByTestId("react-pipeline-page")).toBeInTheDocument();
    fireEvent.click(screen.getAllByRole("button", { name: /^inbox$/i })[0]);
    expect(await screen.findByTestId("react-inbox-page")).toBeInTheDocument();
    expect(window.location.pathname).toBe("/inbox");
    expect(window.location.hash).toBe("");
  });

  it("scopes the dashboard and opens the real add-contact workflow", async () => {
    const fetchMock = vi.fn(fetchStub);
    vi.stubGlobal("fetch", fetchMock);

    render(<App mode="app-shell" />);

    await waitFor(() => expect(fetchMock).toHaveBeenCalledWith("/ui/api/dashboard?client_key=demo", expect.any(Object)));
    fireEvent.click(await screen.findByRole("button", { name: /^add lead$/i }));

    expect(await screen.findByTestId("react-pipeline-page")).toBeInTheDocument();
    expect(screen.getByLabelText("Name")).toBeInTheDocument();
    expect(screen.queryByText(/still needs a React modal/i)).not.toBeInTheDocument();
  });
});

async function fetchStub(input: RequestInfo | URL) {
  const url = String(input);
  if (url === "/ui/api/session") return jsonResponse(sampleSession);
  if (url === "/ui/api/clients") return jsonResponse([{ id: 1, client_key: "demo", business_name: "Demo Client" }]);
  if (url === "/ui/api/dashboard" || url === "/ui/api/dashboard?client_key=demo") return jsonResponse(sampleDashboard);
  if (url === "/ui/api/conversations?client_key=demo") return jsonResponse({ items: [], counts: {}, total: 0 });
  if (url === "/ui/api/crm/leads?archived=false&client_key=demo") return jsonResponse({ items: [], counts: {}, total: 0, stages: ["New Lead", "Qualified", "Won"] });
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

const sampleDashboard = {
  scope: {
    role: "admin",
    client_key: null,
    client_name: null,
    title: "Lead portfolio"
  },
  runtime: {},
  stats: {
    clients_total: 1,
    active_clients: 1,
    conversations_total: 12,
    total_leads: 12,
    attention_needed: 5,
    booked_total: 3,
    handoff_total: 1,
    won_total: 2,
    new_last_24_hours: 1,
    new_last_7_days: 4,
    new_last_30_days: 9,
    open_pipeline_total: 10,
    open_tasks_total: 2,
    overdue_tasks_total: 1,
    due_today_tasks: 1,
    upcoming_meetings_total: 2,
    upcoming_meetings_7d: 1,
    booked_rate: 0.25,
    won_rate: 0.16
  },
  lead_trend: [
    { week_start: "2026-06-01", week_end: "2026-06-07", count: 3 },
    { week_start: "2026-06-08", week_end: "2026-06-14", count: 6 }
  ],
  source_breakdown: [{ key: "meta", count: 12, share: 1 }],
  campaign_performance: {},
  stage_breakdown: [{ key: "Qualified", count: 5, share: 0.42 }],
  onboarding: [],
  top_clients: [],
  upcoming: {
    tasks: [],
    meetings: []
  },
  recent_leads: [
    {
      lead_id: 42,
      lead_name: "Jane Prospect",
      phone: "+15551234567",
      email: "jane@example.com",
      source: "meta",
      client_key: "demo-client",
      client_name: "Demo Client",
      crm_stage: "Qualified",
      conversation_state: "QUALIFYING",
      created_at: "2026-06-10T12:00:00Z",
      last_message_snippet: "Interested",
      last_message_direction: "INBOUND",
      last_message_delivery: null
    }
  ],
  recent_conversations: [],
  latest_activity: {}
};
