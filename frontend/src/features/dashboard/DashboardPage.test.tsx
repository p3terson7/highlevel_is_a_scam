import { render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { App } from "../../app/App";
import type { DashboardPayload, SessionPayload } from "../../api/types";
import { DashboardView } from "./DashboardPage";

describe("DashboardView", () => {
  it("renders dashboard data and preserves legacy drill-through actions", () => {
    render(<DashboardView dashboard={sampleDashboard} />);

    expect(screen.getByRole("heading", { name: /dashboard/i })).toBeInTheDocument();
    expect(screen.getByText("Meta lead ad")).toBeInTheDocument();
    expect(screen.getByText("Jane Prospect")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /open pipeline/i })).toHaveAttribute("data-action", "set-view");
    expect(screen.getByRole("button", { name: /open pipeline/i })).toHaveAttribute("data-view", "crm");
    expect(screen.getByRole("button", { name: /jane prospect/i })).toHaveAttribute("data-action", "open-thread");
    expect(screen.getByRole("button", { name: /jane prospect/i })).toHaveAttribute("data-lead-id", "42");
    expect(screen.getAllByRole("button", { name: /qualified/i })[0]).toHaveAttribute("data-action", "dashboard-open-stage");
  });
});

describe("dashboard island", () => {
  it("hides the legacy dashboard only after React data loads", async () => {
    const dashboardView = document.createElement("section");
    dashboardView.id = "view-dashboard";
    dashboardView.className = "view active";
    const legacyDashboard = document.createElement("div");
    legacyDashboard.id = "legacyDashboardShell";
    dashboardView.appendChild(legacyDashboard);
    document.body.appendChild(dashboardView);
    window.localStorage.clear();
    window.localStorage.setItem("lead-ui-admin-token", "admin-token");
    window.localStorage.setItem("lead-ui-selected-client", "demo");
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse(sampleSession))
      .mockResolvedValueOnce(jsonResponse(sampleDashboard));
    vi.stubGlobal("fetch", fetchMock);

    render(<App mode="dashboard" />);

    expect(legacyDashboard).not.toHaveAttribute("hidden");
    await waitFor(() => expect(legacyDashboard).toHaveAttribute("hidden"));
    expect(screen.getByTestId("react-dashboard-page")).toBeInTheDocument();
    expect(fetchMock).toHaveBeenCalledWith("/ui/api/dashboard?client_key=demo", expect.any(Object));

    dashboardView.remove();
  });
});

const sampleSession: SessionPayload = {
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

const sampleDashboard: DashboardPayload = {
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
  source_breakdown: [
    { key: "meta", count: 7, share: 0.58 },
    { key: "linkedin", count: 5, share: 0.42 }
  ],
  campaign_performance: {},
  stage_breakdown: [
    { key: "Qualified", count: 5, share: 0.42 },
    { key: "Meeting Booked", count: 3, share: 0.25 }
  ],
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

function jsonResponse(payload: unknown) {
  return new Response(JSON.stringify(payload), {
    status: 200,
    headers: { "Content-Type": "application/json" }
  });
}
