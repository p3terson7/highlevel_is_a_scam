import { useEffect, useState } from "react";
import type { CSSProperties } from "react";
import { ApiError, fetchDashboard } from "../../api/client";
import type { DashboardBreakdownRow, DashboardPayload, DashboardRecentLead, DashboardTrendRow } from "../../api/types";
import { useAuth } from "../auth/AuthProvider";

type DashboardPageProps = {
  onReadyChange?: (ready: boolean) => void;
  selectedClientKey?: string;
};

type DashboardState =
  | { status: "idle" | "loading"; dashboard: null; error: null }
  | { status: "ready"; dashboard: DashboardPayload; error: null }
  | { status: "error"; dashboard: null; error: string };

export function DashboardPage({ onReadyChange, selectedClientKey = "" }: DashboardPageProps) {
  const auth = useAuth();
  const [state, setState] = useState<DashboardState>({ status: "idle", dashboard: null, error: null });
  const [retryVersion, setRetryVersion] = useState(0);

  useEffect(() => {
    if (auth.status !== "ready") {
      onReadyChange?.(false);
      return;
    }

    let cancelled = false;
    setState({ status: "loading", dashboard: null, error: null });
    onReadyChange?.(false);

    fetchDashboard(auth.session.role === "admin" ? selectedClientKey : "")
      .then((dashboard) => {
        if (!cancelled) {
          setState({ status: "ready", dashboard, error: null });
          onReadyChange?.(true);
        }
      })
      .catch((error: unknown) => {
        if (!cancelled) {
          const message = error instanceof ApiError || error instanceof Error ? error.message : "Dashboard unavailable";
          setState({ status: "error", dashboard: null, error: message });
          onReadyChange?.(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [auth.status, onReadyChange, retryVersion, selectedClientKey]);

  if (state.status !== "ready") {
    const failed = state.status === "error";
    return (
      <div className="dashboard-shell react-dashboard-page" data-testid="react-dashboard-load-state">
        <section className="surface stack" aria-live="polite">
          <h2>Dashboard</h2>
          <div className="empty-state">
            <div>{failed ? state.error : "Loading dashboard..."}</div>
            {failed ? <button className="small" type="button" onClick={() => setRetryVersion((current) => current + 1)}>Retry</button> : null}
          </div>
        </section>
      </div>
    );
  }

  return <DashboardView dashboard={state.dashboard} />;
}

type DashboardViewProps = {
  dashboard: DashboardPayload;
};

export function DashboardView({ dashboard }: DashboardViewProps) {
  const isAdmin = dashboard.scope.role === "admin";
  const stats = dashboard.stats;
  const latestLeads = dashboard.recent_leads ?? [];
  const newestLead = latestLeads[0] ?? null;

  const kpis = [
    {
      label: "Total records",
      value: stats.total_leads || stats.conversations_total || 0,
      meta: `${stats.new_last_30_days || 0} in the last 30 days`,
      foot: `${stats.new_last_7_days || 0} added this week`,
      view: "leads"
    },
    {
      label: "New last 24h",
      value: stats.new_last_24_hours || 0,
      meta: `${stats.new_last_7_days || 0} in the last 7 days`,
      foot: `${stats.new_last_30_days || 0} in the last 30 days`,
      view: "leads"
    },
    {
      label: "Open tasks",
      value: stats.open_tasks_total || 0,
      meta: `${stats.overdue_tasks_total || 0} overdue`,
      foot: `${stats.upcoming_meetings_7d || 0} meetings in the next 7 days`,
      view: "tasks"
    }
  ];

  return (
    <div className="dashboard-shell react-dashboard-page" data-testid="react-dashboard-page">
      <div className="dashboard-hero">
        <div className="dashboard-hero-grid">
          <div className="dashboard-hero-main">
            <div className="dashboard-hero-head">
              <div className="dashboard-hero-copy">
                <h2>{isAdmin ? "Dashboard" : dashboard.scope.client_name || "Dashboard"}</h2>
                <div className="surface-subtitle">
                  {isAdmin
                    ? "A polished command center for fresh demand, source quality, upcoming work, and pipeline movement."
                    : "See whether new leads came in, where they came from, what needs attention, and what to do next."}
                </div>
                <div className="actions" style={{ marginTop: 8 }}>
                  <button
                    type="button"
                    className="small primary"
                    data-action="crm-open-add-lead"
                    onClick={() => window.localStorage.setItem("lead-ui-react-open-add-contact", "true")}
                  >
                    Add lead
                  </button>
                  <button type="button" className="small ghost" data-action="set-view" data-view="crm">
                    Open pipeline
                  </button>
                  <button type="button" className="small ghost" data-action="set-view" data-view={isAdmin ? "test-lab" : "calendar"}>
                    Create test booking
                  </button>
                  <button type="button" className="small ghost" data-action="set-view" data-view={isAdmin ? "clients" : "settings"}>
                    Connect source
                  </button>
                </div>
              </div>
            </div>
            <div className="dashboard-kpi-grid">
              {kpis.map((item) => (
                <button key={item.label} type="button" className="dashboard-kpi-card dashboard-kpi-action" data-action="set-view" data-view={item.view}>
                  <div className="dashboard-kpi-label">{item.label}</div>
                  <div className="dashboard-kpi-value">{formatNumber(item.value)}</div>
                  <div className="dashboard-kpi-meta">{item.meta}</div>
                  <div className="dashboard-kpi-foot">{item.foot}</div>
                </button>
              ))}
            </div>
          </div>
          <div className="dashboard-trend-card">
            <div className="surface-title">
              <div>
                <h3>Acquisition</h3>
                <div className="surface-subtitle">Last 6 weeks</div>
              </div>
            </div>
            <TrendChart trend={dashboard.lead_trend ?? []} />
          </div>
        </div>
      </div>

      <div className="dashboard-main-grid">
        <div className="stack">
          <section className="surface dashboard-panel dashboard-panel-soft dashboard-source-panel">
            <div className="surface-title">
              <div>
                <h3>Source mix</h3>
                <div className="surface-subtitle">Where demand is coming from right now.</div>
              </div>
            </div>
            <BreakdownChart rows={dashboard.source_breakdown ?? []} kind="source" />
          </section>

          <section className="surface dashboard-panel dashboard-panel-medium">
            <div className="surface-title">
              <div>
                <h3>Pipeline snapshot</h3>
                <div className="surface-subtitle">Compact stage distribution across active opportunities.</div>
              </div>
            </div>
            <StageBreakdown rows={dashboard.stage_breakdown ?? []} />
          </section>
        </div>

        <section className="surface dashboard-panel dashboard-panel-light dashboard-latest-panel">
          <div className="surface-title">
            <div>
              <h3>Latest opportunities</h3>
              <div className="surface-subtitle">
                {stats.new_last_24_hours
                  ? `${stats.new_last_24_hours} new ${stats.new_last_24_hours === 1 ? "opportunity" : "opportunities"} in the last 24 hours.`
                  : "No new opportunities in the last 24 hours."}
              </div>
            </div>
            <button type="button" className="small ghost" data-action="set-view" data-view="leads">
              Open records
            </button>
          </div>
          <div className="dashboard-lead-summary">
            <LeadStat label="Last 24h" value={stats.new_last_24_hours || 0} meta={stats.new_last_24_hours ? "Fresh demand since yesterday." : "No fresh demand overnight."} />
            <LeadStat label="Last 7 days" value={stats.new_last_7_days || 0} meta={`${latestLeads.length} most recent records shown below.`} />
            <LeadStat
              label="Latest arrival"
              value={newestLead ? formatDateTime(newestLead.created_at) : "No records yet"}
              meta={newestLead ? `${newestLead.lead_name || newestLead.phone || "Contact"} · ${formatSource(newestLead.source)}` : "New records will appear here."}
              time
            />
          </div>
          <LatestLeads leads={latestLeads} isClient={!isAdmin} />
        </section>
      </div>
    </div>
  );
}

function TrendChart({ trend }: { trend: DashboardTrendRow[] }) {
  const total = trend.reduce((sum, item) => sum + Number(item.count || 0), 0);
  const max = Math.max(...trend.map((item) => Number(item.count || 0)), 1);

  if (!trend.length) {
    return <EmptyState title="No acquisition trend yet." detail="Once leads start arriving, this panel will show whether demand is accelerating or going quiet." />;
  }

  return (
    <div className="dashboard-trend">
      <div className="dashboard-trend-summary">
        <strong>{formatNumber(total)}</strong>
        <span>opportunities created in the last 6 weeks</span>
      </div>
      <div className="dashboard-trend-bars">
        {trend.map((item) => {
          const count = Number(item.count || 0);
          const height = Math.max(14, Math.round((count / max) * 108));
          return (
            <div className="dashboard-trend-column" key={item.week_start}>
              <div className="dashboard-trend-count">{count}</div>
              <div className="dashboard-trend-bar-wrap">
                <div className="dashboard-trend-bar" style={{ height }} />
              </div>
              <div className="dashboard-trend-label">{formatDateLabel(item.week_start)}</div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function BreakdownChart({ rows, kind }: { rows: DashboardBreakdownRow[]; kind: "source" | "stage" }) {
  const total = rows.reduce((sum, item) => sum + Number(item.count || 0), 0);
  const radius = 50;
  const circumference = 2 * Math.PI * radius;
  let offset = 0;

  if (!rows.length) {
    return (
      <EmptyState
        title={kind === "source" ? "No source data available yet." : "No pipeline data available yet."}
        detail={kind === "source" ? "Connect an acquisition source or add the first lead so source quality becomes visible." : "Add or import leads to turn this into a stage-level operating view."}
      />
    );
  }

  return (
    <div className="dashboard-chart-shell">
      <div className="dashboard-chart-art">
        <div className="dashboard-chart-figure">
          <svg className="dashboard-chart-svg" viewBox="0 0 124 124" aria-hidden="true">
            <circle className="dashboard-chart-track" cx="62" cy="62" r={radius} fill="none" strokeWidth="16" />
            {rows.map((item, index) => {
              const dash = Math.max(Number(item.share || 0) * circumference, 0);
              const segmentOffset = offset;
              offset += dash;
              return (
                <circle
                  key={item.key}
                  className="dashboard-chart-segment"
                  cx="62"
                  cy="62"
                  r={radius}
                  stroke={chartColor(kind, item.key, index)}
                  strokeWidth="16"
                  strokeDasharray={`${dash} ${circumference}`}
                  strokeDashoffset={-segmentOffset}
                  transform="rotate(-90 62 62)"
                />
              );
            })}
          </svg>
          <div className="dashboard-chart-center">
            <div className="dashboard-chart-total">{formatNumber(total)}</div>
            <div className="dashboard-chart-caption">total records</div>
            <div className="dashboard-chart-subcaption">{rows.length} {kind === "source" ? "sources" : "stages"}</div>
          </div>
        </div>
      </div>
      <div className="dashboard-chart-legend">
        {rows.map((item, index) => (
          <div className="dashboard-chart-row" key={item.key}>
            <div className="dashboard-chart-main">
              <span className="dashboard-chart-swatch" style={{ background: chartColor(kind, item.key, index) }} />
              <span className="dashboard-chart-label">{kind === "source" ? formatSource(item.key) : item.key}</span>
            </div>
            <div className="dashboard-chart-stats">
              <span className="dashboard-chart-share">{formatPercent(item.share)}</span>
              <span className="dashboard-chart-count">{item.count} {item.count === 1 ? "opportunity" : "opportunities"}</span>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

function StageBreakdown({ rows }: { rows: DashboardBreakdownRow[] }) {
  const max = Math.max(...rows.map((item) => Number(item.count || 0)), 1);
  if (!rows.length) {
    return <EmptyState title="No pipeline data available yet." detail="Your pipeline snapshot will appear once leads exist and start moving through stages." />;
  }

  return (
    <div className="dashboard-stage-chart">
      <div className="dashboard-stage-rows">
        {rows.map((item, index) => {
          const width = Math.max(6, Math.round((Number(item.count || 0) / max) * 100));
          return (
            <button type="button" className="dashboard-stage-row dashboard-chart-row-action" data-action="dashboard-open-stage" data-stage={item.key} key={item.key}>
              <div className="dashboard-stage-row-top">
                <div className="dashboard-chart-main">
                  <span className="dashboard-chart-swatch" style={{ background: chartColor("stage", item.key, index) }} />
                  <span className="dashboard-chart-label">{item.key}</span>
                </div>
                <div className="dashboard-chart-stats">
                  <span className="dashboard-chart-share">{formatPercent(item.share)}</span>
                  <span className="dashboard-chart-count">{item.count} {item.count === 1 ? "record" : "records"}</span>
                </div>
              </div>
              <div className="dashboard-stage-bar-track">
                <div className="dashboard-stage-bar-fill" style={{ width: `${width}%`, background: chartColor("stage", item.key, index) }} />
              </div>
            </button>
          );
        })}
      </div>
    </div>
  );
}

function LeadStat({ label, value, meta, time = false }: { label: string; value: string | number; meta: string; time?: boolean }) {
  return (
    <div className="dashboard-lead-stat">
      <div className="dashboard-lead-stat-label">{label}</div>
      <div className={`dashboard-lead-stat-value${time ? " time" : ""}`}>{value}</div>
      <div className="dashboard-lead-stat-meta">{meta}</div>
    </div>
  );
}

function LatestLeads({ leads, isClient }: { leads: DashboardRecentLead[]; isClient: boolean }) {
  const visible = leads.slice(0, 5);
  if (!visible.length) {
    return <EmptyState title="No fresh leads yet." detail="When new demand arrives, the latest lead, source, and acquisition time will appear here first." />;
  }

  return (
    <div className="dashboard-lead-list">
      {visible.map((lead) => (
        <button type="button" className="dashboard-lead-row dashboard-lead-row-action" data-action="open-thread" data-lead-id={lead.lead_id} key={lead.lead_id}>
          <div className="dashboard-lead-card" style={{ "--lead-accent": chartColor("source", lead.source, 0) } as CSSProperties}>
            <div className="dashboard-lead-top">
              <div style={{ minWidth: 0 }}>
                <div className="dashboard-lead-context">
                  {lead.crm_stage ? <span className="badge info">{lead.crm_stage}</span> : null}
                  {isRecent(lead.created_at) ? <span className="badge info">new</span> : null}
                </div>
                <div className="dashboard-lead-name">{lead.lead_name || lead.phone || `Contact ${lead.lead_id}`}</div>
                <div className="dashboard-lead-meta">
                  {[formatSource(lead.source), lead.phone, isClient ? "" : lead.client_name].filter(Boolean).join(" · ") || "No contact details"}
                </div>
              </div>
              <div className="dashboard-lead-time">
                Acquired
                <strong>{formatDateTime(lead.created_at)}</strong>
              </div>
            </div>
          </div>
        </button>
      ))}
    </div>
  );
}

function EmptyState({ title, detail }: { title: string; detail: string }) {
  return (
    <div className="dashboard-empty dashboard-empty-guided">
      <div className="dashboard-empty-kicker">Next move</div>
      <div className="dashboard-empty-title">{title}</div>
      <div className="dashboard-empty-detail">{detail}</div>
    </div>
  );
}

function formatNumber(value: number) {
  return new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 }).format(Number(value || 0));
}

function formatPercent(value: number) {
  const percent = Number(value || 0) * 100;
  if (!Number.isFinite(percent)) return "0%";
  return `${percent >= 10 ? Math.round(percent) : Number(percent.toFixed(1))}%`;
}

function formatDateLabel(value: string) {
  return new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric" }).format(new Date(value));
}

function formatDateTime(value: string) {
  return new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" }).format(new Date(value));
}

function formatSource(source: string) {
  const labels: Record<string, string> = {
    meta: "Meta lead ad",
    linkedin: "LinkedIn lead form",
    sms: "SMS intake",
    manual: "Manual entry"
  };
  return labels[source] || source || "Unknown source";
}

function chartColor(kind: "source" | "stage", key: string, index: number) {
  const fallback = ["var(--accent)", "var(--accent-2)", "var(--success)", "var(--accent-3)", "var(--warn)"];
  if (kind === "source") {
    const sourceColors: Record<string, string> = {
      meta: "var(--accent)",
      linkedin: "var(--accent-2)",
      sms: "var(--success)",
      manual: "var(--accent-3)"
    };
    return sourceColors[String(key || "").toLowerCase()] || fallback[index % fallback.length];
  }

  const stageColors: Record<string, string> = {
    "New Lead": "var(--text-dim)",
    Contacted: "var(--accent)",
    Qualified: "var(--accent-2)",
    "Meeting Booked": "var(--success)",
    "Meeting Completed": "var(--accent-3)",
    Won: "var(--success)",
    Lost: "var(--warn)"
  };
  return stageColors[key] || fallback[index % fallback.length];
}

function isRecent(value: string) {
  return new Date(value).getTime() >= Date.now() - 24 * 60 * 60 * 1000;
}
