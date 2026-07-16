import { FormEvent, MouseEvent, useEffect, useMemo, useRef, useState } from "react";
import { fetchClients } from "../api/client";
import type { ClientSummary, SessionPayload } from "../api/types";
import { ClientsPage, LogsPage, SettingsPage, TestLabPage } from "../features/admin/AdminPages";
import { useAuth } from "../features/auth/AuthProvider";
import { DashboardPage } from "../features/dashboard/DashboardPage";
import { ContactActionDialog } from "../features/workflows/WorkflowDialogs";
import { CalendarPage, InboxPage, PipelinePage, RecordsPage, TasksPage } from "../features/workflows/WorkflowPages";
import type { WorkflowQuickAction } from "../features/workflows/WorkflowPages";

type ShellView = "dashboard" | "clients" | "conversations" | "crm" | "leads" | "calendar" | "tasks" | "logs" | "settings" | "test-lab";

type NavItem = {
  view: ShellView;
  label: string;
  meta: string;
  adminOnly?: boolean;
};

const SELECTED_CLIENT_KEY = "lead-ui-selected-client";
const ACTIVE_INBOX_LEAD_KEY = "lead-ui-active-lead";
const ACTIVE_RECORD_LEAD_KEY = "lead-ui-active-crm-lead";
const THEME_KEY = "lead-ui-theme";

const NAV_ITEMS: NavItem[] = [
  { view: "dashboard", label: "Dashboard", meta: "Status and queue" },
  { view: "clients", label: "Clients", meta: "Tenant workspace", adminOnly: true },
  { view: "conversations", label: "Inbox", meta: "Messages and replies" },
  { view: "crm", label: "Pipeline", meta: "Kanban stages" },
  { view: "leads", label: "Records", meta: "Full profiles" },
  { view: "calendar", label: "Calendar", meta: "Meetings and tasks" },
  { view: "tasks", label: "Tasks", meta: "Follow-ups" },
  { view: "logs", label: "Logs", meta: "Audit trail", adminOnly: true },
  { view: "settings", label: "Settings", meta: "Providers and guidance" },
  { view: "test-lab", label: "Test Lab", meta: "Sandbox launcher", adminOnly: true }
];

const VIEW_BY_ROUTE: Record<string, ShellView> = {
  "": "dashboard",
  home: "dashboard",
  dashboard: "dashboard",
  clients: "clients",
  inbox: "conversations",
  conversations: "conversations",
  pipeline: "crm",
  crm: "crm",
  records: "leads",
  leads: "leads",
  calendar: "calendar",
  tasks: "tasks",
  logs: "logs",
  settings: "settings",
  "test-lab": "test-lab",
  test_lab: "test-lab"
};

const PATH_BY_VIEW: Record<ShellView, string> = {
  dashboard: "/ui",
  clients: "/clients",
  conversations: "/inbox",
  crm: "/pipeline",
  leads: "/records",
  calendar: "/calendar",
  tasks: "/tasks",
  logs: "/logs",
  settings: "/settings",
  "test-lab": "/test-lab"
};

const MOBILE_NAV_VIEWS: ShellView[] = ["dashboard", "clients", "conversations", "crm", "calendar", "tasks", "settings"];

export function AppShell() {
  const auth = useAuth();

  if (auth.status === "loading") {
    return (
      <main className="react-auth-shell" aria-label="Loading workspace">
        <section className="react-auth-card">
          <p className="scaffold-eyebrow">Lead Ops</p>
          <h1>Loading workspace</h1>
          <p>Checking your saved session.</p>
        </section>
      </main>
    );
  }

  if (auth.status !== "ready") {
    return <LoginShell error={auth.error} />;
  }

  return <AuthenticatedShell session={auth.session} />;
}

function LoginShell({ error }: { error: string | null }) {
  const auth = useAuth();
  const [mode, setMode] = useState<"admin" | "client">("admin");
  const [adminToken, setAdminToken] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [status, setStatus] = useState(error || "");

  useEffect(() => {
    setStatus(error || "");
  }, [error]);

  async function submit(event: FormEvent) {
    event.preventDefault();
    setStatus("Signing in...");
    try {
      if (mode === "admin") {
        await auth.loginAdmin(adminToken);
      } else {
        await auth.loginClientPortal(email, password);
      }
      setStatus("");
    } catch (caught: unknown) {
      setStatus(caught instanceof Error ? caught.message : "Sign in failed");
    }
  }

  return (
    <main className="react-auth-shell" aria-labelledby="react-login-title">
      <form className="react-auth-card stack" onSubmit={submit}>
        <div className="login-title">
          <div className="brand-mark">LO</div>
          <div>
            <h1 id="react-login-title">Sign in to Lead Ops</h1>
            <p>Use the same admin token or client portal login as the current CRM.</p>
          </div>
        </div>
        <div className="tab-bar" role="tablist" aria-label="Sign in mode">
          <button className={`tab-btn ${mode === "admin" ? "active" : ""}`} type="button" onClick={() => setMode("admin")}>
            Admin
          </button>
          <button className={`tab-btn ${mode === "client" ? "active" : ""}`} type="button" onClick={() => setMode("client")}>
            Client
          </button>
        </div>
        {mode === "admin" ? (
          <label>
            Admin token
            <input value={adminToken} onChange={(event) => setAdminToken(event.currentTarget.value)} type="password" placeholder="Enter ADMIN_TOKEN" autoComplete="current-password" />
          </label>
        ) : (
          <div className="stack">
            <label>
              Client email
              <input value={email} onChange={(event) => setEmail(event.currentTarget.value)} type="email" placeholder="owner@example.com" autoComplete="email" />
            </label>
            <label>
              Password
              <input value={password} onChange={(event) => setPassword(event.currentTarget.value)} type="password" placeholder="Client portal password" autoComplete="current-password" />
            </label>
          </div>
        )}
        <div className="actions">
          <button className="primary" type="submit">
            Sign in
          </button>
          <button className="ghost" type="button" onClick={() => void auth.logout()}>
            Clear saved session
          </button>
        </div>
        {status ? <div className="meta-text" role="status">{status}</div> : null}
      </form>
    </main>
  );
}

function AuthenticatedShell({ session }: { session: SessionPayload }) {
  const auth = useAuth();
  const allowedViews = useMemo(() => allowedViewsFor(session), [session]);
  const [view, setViewState] = useState<ShellView>(() => coerceAllowedView(viewFromLocation(), allowedViews));
  const [clients, setClients] = useState<ClientSummary[]>([]);
  const [selectedClientKey, setSelectedClientKey] = useState(() => session.client_key || window.localStorage.getItem(SELECTED_CLIENT_KEY) || "");
  const selectedClientKeyRef = useRef(selectedClientKey);
  const [notice, setNotice] = useState("");
  const [pageReady, setPageReady] = useState(false);
  const [pageVersion, setPageVersion] = useState(0);
  const [globalSearch, setGlobalSearch] = useState("");
  const [quickAction, setQuickAction] = useState<WorkflowQuickAction | null>(null);
  const [contactActionLeadId, setContactActionLeadId] = useState<number | null>(null);
  const [theme, setTheme] = useState<"dark" | "light">(() => savedTheme());

  const isAdmin = session.role === "admin";
  const selectedClient = clients.find((client) => client.client_key === selectedClientKey);
  const visibleNav = NAV_ITEMS.filter((item) => isAdmin || !item.adminOnly);

  useEffect(() => {
    const update = () => {
      setPageReady(false);
      setQuickAction(null);
      setViewState((current) => coerceAllowedView(viewFromLocation() || current, allowedViews));
    };
    window.addEventListener("hashchange", update);
    window.addEventListener("popstate", update);
    return () => {
      window.removeEventListener("hashchange", update);
      window.removeEventListener("popstate", update);
    };
  }, [allowedViews]);

  useEffect(() => {
    setViewState((current) => coerceAllowedView(current, allowedViews));
  }, [allowedViews]);

  useEffect(() => {
    selectedClientKeyRef.current = selectedClientKey;
  }, [selectedClientKey]);

  useEffect(() => {
    if (!isAdmin) {
      setClients([]);
      if (session.client_key) {
        setSelectedClientKey(session.client_key);
        window.localStorage.setItem(SELECTED_CLIENT_KEY, session.client_key);
      }
      return;
    }

    let cancelled = false;
    fetchClients()
      .then((items) => {
        if (cancelled) return;
        const saved = window.localStorage.getItem(SELECTED_CLIENT_KEY) || "";
        const nextKey = items.some((client) => client.client_key === saved) ? saved : items[0]?.client_key || "";
        setClients(items);
        setSelectedClientKey(nextKey);
        if (nextKey) window.localStorage.setItem(SELECTED_CLIENT_KEY, nextKey);
      })
      .catch((caught: unknown) => {
        if (!cancelled) setNotice(caught instanceof Error ? caught.message : "Client list unavailable");
      });
    return () => {
      cancelled = true;
    };
  }, [isAdmin, session.client_key]);

  useEffect(() => {
    if (!isAdmin) return;
    const syncSelectedClient = (event: Event) => {
      const nextClientKey = (event as CustomEvent<{ clientKey?: string }>).detail?.clientKey;
      if (nextClientKey === undefined || nextClientKey === selectedClientKeyRef.current) return;
      selectedClientKeyRef.current = nextClientKey;
      setSelectedClientKey(nextClientKey);
      window.localStorage.setItem(SELECTED_CLIENT_KEY, nextClientKey);
      setPageReady(false);
      setQuickAction(null);
      setPageVersion((version) => version + 1);
    };
    window.addEventListener("lead-ui-client-change", syncSelectedClient);
    return () => window.removeEventListener("lead-ui-client-change", syncSelectedClient);
  }, [isAdmin]);

  function setView(nextView: ShellView) {
    const allowed = coerceAllowedView(nextView, allowedViews);
    setNotice("");
    setQuickAction(null);
    setPageReady(false);
    setViewState(allowed);
    updateLocationForView(allowed);
  }

  function changeClient(nextClientKey: string) {
    setPageReady(false);
    setQuickAction(null);
    selectedClientKeyRef.current = nextClientKey;
    setSelectedClientKey(nextClientKey);
    window.localStorage.setItem(SELECTED_CLIENT_KEY, nextClientKey);
    window.dispatchEvent(new CustomEvent("lead-ui-client-change", { detail: { clientKey: nextClientKey } }));
    setPageVersion((current) => current + 1);
  }

  function handleWorkspaceClick(event: MouseEvent<HTMLElement>) {
    const actionTarget = (event.target as HTMLElement).closest<HTMLElement>("[data-action]");
    if (!actionTarget) return;

    const action = actionTarget.dataset.action || "";
    if (action === "set-view") {
      const nextView = normalizeView(actionTarget.dataset.view || "");
      if (nextView) {
        event.preventDefault();
        setView(nextView);
      }
      return;
    }

    if (action === "open-thread") {
      const leadId = actionTarget.dataset.leadId;
      if (leadId) {
        event.preventDefault();
        window.localStorage.setItem(ACTIVE_INBOX_LEAD_KEY, leadId);
        setPageReady(false);
        setPageVersion((current) => current + 1);
        setView("conversations");
      }
      return;
    }

    if (action === "open-crm-lead") {
      const leadId = actionTarget.dataset.leadId;
      if (leadId) {
        event.preventDefault();
        window.localStorage.setItem(ACTIVE_RECORD_LEAD_KEY, leadId);
        setPageReady(false);
        setPageVersion((current) => current + 1);
        setView("leads");
      }
      return;
    }

    if (action === "dashboard-open-stage") {
      event.preventDefault();
      const stage = actionTarget.dataset.stage || "all";
      window.localStorage.setItem("lead-ui-crm-stage", stage);
      setView("crm");
      return;
    }

    if (action === "crm-open-add-lead") {
      event.preventDefault();
      setView("crm");
      setQuickAction({ id: Date.now(), kind: "add-contact" });
      return;
    }

    if (action === "open-contact-drawer") {
      const leadId = Number(actionTarget.dataset.leadId || 0);
      if (leadId) {
        event.preventDefault();
        setContactActionLeadId(leadId);
      }
      return;
    }

    if (action === "calendar-add-meeting") {
      event.preventDefault();
      setView("calendar");
      setQuickAction({ id: Date.now(), kind: "add-meeting", leadId: Number(actionTarget.dataset.leadId || 0) || undefined });
    }
  }

  const activeLabel = NAV_ITEMS.find((item) => item.view === view)?.label || "Dashboard";
  const shellKey = `${view}:${selectedClientKey}:${pageVersion}`;

  useEffect(() => {
    document.body.dataset.theme = theme;
    window.localStorage.setItem(THEME_KEY, theme);
  }, [theme]);

  return (
    <div className="window-frame react-app-frame" data-testid="react-app-shell">
      <header className="window-chrome react-shell-chrome">
        <div className="chrome-left">
          <div className="chrome-copy">
            <div className="chrome-title mono">{session.app_name || "Lead Ops Console"}</div>
            <div className="chrome-subtitle">{isAdmin ? "Admin workspace" : session.portal_display_name || session.client_name || "Client workspace"}</div>
          </div>
        </div>
        <nav className="chrome-nav" aria-label="Primary">
          {visibleNav.map((item) => (
            <button id={topNavId(item.view)} className={`top-nav-item nav-item ${view === item.view ? "active" : ""}`} key={item.view} type="button" onClick={() => setView(item.view)}>
              <NavIcon view={item.view} className="top-nav-icon" />
              <span className="top-nav-label">{item.label}</span>
            </button>
          ))}
        </nav>
        <div className="chrome-right">
          <div className="search-wrap react-search-wrap">
            <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.4" aria-hidden="true"><circle cx="7" cy="7" r="4.5" /><path d="m10.4 10.4 3.1 3.1" /></svg>
            <input
              id="globalSearch"
              type="search"
              placeholder="Search workspace"
              aria-label="Search workspace"
              value={globalSearch}
              onChange={(event) => setGlobalSearch(event.currentTarget.value)}
            />
          </div>
          {isAdmin ? (
            <select id="topClientSelector" className="top-client-select" aria-label="Select client workspace" value={selectedClientKey} onChange={(event) => changeClient(event.currentTarget.value)}>
              {clients.length ? clients.map((client) => (
                <option key={client.client_key} value={client.client_key}>
                  {client.business_name}
                </option>
              )) : <option value="">All clients</option>}
            </select>
          ) : null}
          <button id="themeToggle" className="icon-btn ghost" title="Toggle theme" aria-label="Toggle theme" type="button" onClick={() => setTheme((current) => current === "dark" ? "light" : "dark")}>
            <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.4"><path d="M8 2.5v2M8 11.5v2M3.5 8h2M10.5 8h2M4.7 4.7l1.4 1.4M9.9 9.9l1.4 1.4M11.3 4.7l-1.4 1.4M6.1 9.9l-1.4 1.4" /><circle cx="8" cy="8" r="2.4" /></svg>
          </button>
          <button id="refreshButton" className="icon-btn ghost" title="Refresh current view" aria-label="Refresh current view" type="button" onClick={() => void auth.refresh()}>
            <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.4"><path d="M13 8a5 5 0 1 1-1.2-3.2M13 3.5v3h-3" /></svg>
          </button>
          <button id="logoutButton" className="ghost small" type="button" onClick={() => void auth.logout()}>
            Sign out
          </button>
        </div>
      </header>

      <div className="app-shell react-owned-shell">
        <aside id="sidebar" className="sidebar react-sidebar">
          <div className="sidebar-header">
            <div className="brand-mark">LO</div>
            <div className="brand-copy">
              <div className="brand-title">Lead Ops</div>
              <div className="brand-subtitle">{isAdmin ? "Operator workspace" : "Client workspace"}</div>
            </div>
          </div>
          <div className="sidebar-nav">
            <div className="sidebar-section">Workspace</div>
            {visibleNav.map((item) => (
              <button id={sideNavId(item.view)} className={`nav-item ${view === item.view ? "active" : ""}`} key={item.view} type="button" onClick={() => setView(item.view)}>
                <NavIcon view={item.view} className="nav-icon" />
                <div className="nav-copy">
                  <div className="nav-label">{item.label}</div>
                  <div className="nav-meta">{item.meta}</div>
                </div>
              </button>
            ))}
          </div>
          <div className="sidebar-footer">
            <div className="sidebar-footer-copy">
              <div className="mono">{selectedClient?.business_name || session.client_name || "All clients"}</div>
              <div className="chip-row">
                <span className="tag info">{session.role}</span>
                <span className="tag">{session.env}</span>
              </div>
            </div>
          </div>
        </aside>

        <main className="main-shell">
          <div className="workspace react-workspace" onClick={handleWorkspaceClick}>
            {auth.error ? <div className="notice show err react-shell-notice" role="alert">{auth.error}</div> : null}
            {notice ? <div className="notice react-shell-notice" role="status">{notice}</div> : null}
            <div className="react-shell-page" key={shellKey}>
              {renderPage(view, isAdmin, setPageReady, selectedClientKey, globalSearch, quickAction)}
            </div>
          </div>
        </main>
      </div>
      <nav id="mobileTabbar" className="mobile-tabbar" aria-label="Mobile navigation">
        <div className="mobile-tabbar-inner">
          {MOBILE_NAV_VIEWS.map((navView) => {
            const item = NAV_ITEMS.find((candidate) => candidate.view === navView);
            if (!item || (item.adminOnly && !isAdmin)) return null;
            return (
              <button id={mobileNavId(navView)} className={`mobile-tab-item nav-item ${view === navView ? "active" : ""}`} key={navView} type="button" onClick={() => setView(navView)}>
                <NavIcon view={navView} className="mobile-tab-icon" />
                <span className="mobile-tab-label">{mobileNavLabel(navView)}</span>
              </button>
            );
          })}
        </div>
      </nav>
      <ContactActionDialog
        key={contactActionLeadId ?? "closed"}
        leadId={contactActionLeadId}
        onClose={() => setContactActionLeadId(null)}
        onOpenThread={(leadId, clientKey) => {
          if (clientKey && clientKey !== selectedClientKey) changeClient(clientKey);
          window.localStorage.setItem(ACTIVE_INBOX_LEAD_KEY, String(leadId));
          setContactActionLeadId(null);
          setPageVersion((current) => current + 1);
          setView("conversations");
        }}
        onOpenRecord={(leadId, clientKey) => {
          if (clientKey && clientKey !== selectedClientKey) changeClient(clientKey);
          window.localStorage.setItem(ACTIVE_RECORD_LEAD_KEY, String(leadId));
          setContactActionLeadId(null);
          setPageVersion((current) => current + 1);
          setView("leads");
        }}
        onCreateMeeting={(leadId, clientKey) => {
          if (clientKey && clientKey !== selectedClientKey) changeClient(clientKey);
          setContactActionLeadId(null);
          setView("calendar");
          setQuickAction({ id: Date.now(), kind: "add-meeting", leadId });
        }}
      />
    </div>
  );
}

function renderPage(
  view: ShellView,
  isAdmin: boolean,
  onReadyChange: (ready: boolean) => void,
  selectedClientKey: string,
  searchQuery: string,
  quickAction: WorkflowQuickAction | null
) {
  if (view === "dashboard") return <DashboardPage onReadyChange={onReadyChange} selectedClientKey={selectedClientKey} />;
  if (view === "conversations") return <InboxPage onReadyChange={onReadyChange} selectedClientKey={selectedClientKey} searchQuery={searchQuery} />;
  if (view === "crm") return <PipelinePage onReadyChange={onReadyChange} quickAction={quickAction} selectedClientKey={selectedClientKey} searchQuery={searchQuery} />;
  if (view === "leads") return <RecordsPage onReadyChange={onReadyChange} selectedClientKey={selectedClientKey} searchQuery={searchQuery} />;
  if (view === "calendar") return <CalendarPage onReadyChange={onReadyChange} quickAction={quickAction} selectedClientKey={selectedClientKey} />;
  if (view === "tasks") return <TasksPage onReadyChange={onReadyChange} selectedClientKey={selectedClientKey} searchQuery={searchQuery} />;
  if (view === "settings") return <SettingsPage onReadyChange={onReadyChange} />;
  if (isAdmin && view === "clients") return <ClientsPage onReadyChange={onReadyChange} />;
  if (isAdmin && view === "logs") return <LogsPage onReadyChange={onReadyChange} />;
  if (isAdmin && view === "test-lab") return <TestLabPage onReadyChange={onReadyChange} />;
  return <DashboardPage onReadyChange={onReadyChange} />;
}

function savedTheme(): "dark" | "light" {
  try {
    return window.localStorage.getItem(THEME_KEY) === "light" ? "light" : "dark";
  } catch {
    return "dark";
  }
}

function topNavId(view: ShellView) {
  return {
    dashboard: "topNavDashboard",
    clients: "topNavClients",
    conversations: "topNavConversations",
    crm: "topNavCrm",
    leads: "topNavLeads",
    calendar: "topNavCalendar",
    tasks: "topNavTasks",
    logs: "topNavLogs",
    settings: "topNavSettings",
    "test-lab": "topNavTestLab"
  }[view];
}

function sideNavId(view: ShellView) {
  return {
    dashboard: "navDashboard",
    clients: "navClients",
    conversations: "navConversations",
    crm: "navCrm",
    leads: "navLeads",
    calendar: "navCalendar",
    tasks: "navTasks",
    logs: "navLogs",
    settings: "navSettings",
    "test-lab": "navTestLab"
  }[view];
}

function mobileNavId(view: ShellView) {
  return {
    dashboard: "mobileNavDashboard",
    clients: "mobileNavClients",
    conversations: "mobileNavConversations",
    crm: "mobileNavCrm",
    leads: "mobileNavLeads",
    calendar: "mobileNavCalendar",
    tasks: "mobileNavTasks",
    logs: "mobileNavLogs",
    settings: "mobileNavSettings",
    "test-lab": "mobileNavTestLab"
  }[view];
}

function mobileNavLabel(view: ShellView) {
  if (view === "dashboard") return "Home";
  return NAV_ITEMS.find((item) => item.view === view)?.label || "Home";
}

function NavIcon({ view, className }: { view: ShellView; className: string }) {
  const common = { className, viewBox: "0 0 16 16", fill: "none", stroke: "currentColor", strokeWidth: 1.4 };
  if (view === "dashboard") {
    return <svg {...common}><path d="M2.5 2.5h4.5v4.5H2.5zM9 2.5h4.5v7H9zM2.5 9h4.5v4.5H2.5zM9 11.5h4.5V13.5H9z" /></svg>;
  }
  if (view === "clients") {
    return <svg {...common}><path d="M2.5 3.5h11M2.5 7.5h11M2.5 11.5h6" /><circle cx="12.5" cy="11.5" r="1.5" /></svg>;
  }
  if (view === "conversations") {
    return <svg {...common}><path d="M2.5 3.5h11v7h-6l-3 2v-2h-2z" /></svg>;
  }
  if (view === "crm") {
    return <svg {...common}><path d="M2.5 4.5h11M2.5 8h11M2.5 11.5h11" /><circle cx="5" cy="4.5" r="1" /><circle cx="8" cy="8" r="1" /><circle cx="11" cy="11.5" r="1" /></svg>;
  }
  if (view === "leads") {
    return <svg {...common}><path d="M8 8a2.5 2.5 0 1 0 0-5 2.5 2.5 0 0 0 0 5ZM3 13c0-2.2 2.2-3.5 5-3.5s5 1.3 5 3.5" /></svg>;
  }
  if (view === "calendar" || view === "tasks") {
    return <svg {...common}><path d="M3 4h10v9H3zM5.5 2.5v3M10.5 2.5v3M3 7h10" />{view === "tasks" ? <path d="M5.5 9.5h5M5.5 11.5h3" /> : null}</svg>;
  }
  if (view === "logs") {
    return <svg {...common}><path d="M3 2.5h10v11H3zM5.5 5.5h5M5.5 8h5M5.5 10.5h3" /></svg>;
  }
  if (view === "settings") {
    return <svg {...common}><path d="M8 2.5v2M8 11.5v2M3.5 8h-2M14.5 8h-2M4.8 4.8 3.4 3.4M12.6 12.6 11.2 11.2M11.2 4.8l1.4-1.4M3.4 12.6l1.4-1.4" /><circle cx="8" cy="8" r="2.2" /></svg>;
  }
  return <svg {...common}><path d="M4 2.5h8M6 2.5v3l-2.5 5a2 2 0 0 0 1.8 3h5.4a2 2 0 0 0 1.8-3L10 5.5v-3" /></svg>;
}

function allowedViewsFor(session: SessionPayload): ShellView[] {
  if (session.role === "client") return ["dashboard", "conversations", "crm", "leads", "calendar", "tasks", "settings"];
  return NAV_ITEMS.map((item) => item.view);
}

function coerceAllowedView(view: ShellView | null, allowedViews: ShellView[]) {
  return view && allowedViews.includes(view) ? view : "dashboard";
}

function viewFromLocation() {
  const hashView = window.location.hash ? normalizeView(window.location.hash) : null;
  return hashView || normalizeView(window.location.pathname) || "dashboard";
}

function normalizeView(rawValue: string): ShellView | null {
  let normalized = rawValue.trim().replace(/^#/, "").replace(/^\/+|\/+$/g, "");
  if (normalized === "ui") normalized = "";
  if (normalized.startsWith("ui/")) normalized = normalized.slice(3);
  const firstSegment = normalized.split("/")[0] || "";
  return VIEW_BY_ROUTE[firstSegment] || null;
}

function updateLocationForView(view: ShellView) {
  const nextPath = PATH_BY_VIEW[view];
  if (window.location.pathname === nextPath && !window.location.hash) return;
  window.history.pushState({ view }, "", `${nextPath}${window.location.search}`);
  window.dispatchEvent(new PopStateEvent("popstate", { state: { view } }));
}
