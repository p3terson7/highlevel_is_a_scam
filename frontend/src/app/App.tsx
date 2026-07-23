import { useEffect, useState } from "react";
import { ClientsPage, LogsPage, SettingsPage, TestLabPage } from "../features/admin/AdminPages";
import { AuthProvider } from "../features/auth/AuthProvider";
import { DashboardPage } from "../features/dashboard/DashboardPage";
import { CalendarPage, InboxPage, PipelinePage, RecordsPage, TasksPage } from "../features/workflows/WorkflowPages";
import { AppShell } from "./AppShell";

type AppProps = {
  mode?: AppMode;
};

type AppMode = "app-shell" | "calendar" | "clients" | "dashboard" | "inbox" | "island" | "logs" | "pipeline" | "records" | "settings" | "standalone" | "tasks" | "test-lab";

type IslandConfig = {
  viewId: string;
  legacySelectors: string[];
};

const ISLAND_CONFIG: Partial<Record<AppMode, IslandConfig>> = {
  dashboard: {
    viewId: "view-dashboard",
    legacySelectors: ["#legacyDashboardShell"]
  },
  inbox: {
    viewId: "view-conversations",
    legacySelectors: ["#view-conversations > .surface", "#conversationShell"]
  },
  pipeline: {
    viewId: "view-crm",
    legacySelectors: ["#view-crm > .surface", "#crmBoard"]
  },
  records: {
    viewId: "view-leads",
    legacySelectors: ["#view-leads > .surface", "#view-leads > .two-column-shell"]
  },
  calendar: {
    viewId: "view-calendar",
    legacySelectors: ["#view-calendar > .surface", "#view-calendar > .calendar-experience"]
  },
  tasks: {
    viewId: "view-tasks",
    legacySelectors: ["#view-tasks > .surface", "#view-tasks > .surface.stack"]
  },
  clients: {
    viewId: "view-clients",
    legacySelectors: ["#view-clients > .surface", "#view-clients > .two-column-shell"]
  },
  logs: {
    viewId: "view-logs",
    legacySelectors: ["#view-logs > .surface", "#logEventCards", "#view-logs > .surface.stack"]
  },
  settings: {
    viewId: "view-settings",
    legacySelectors: ["#view-settings > .surface", "#settingsLayout"]
  },
  "test-lab": {
    viewId: "view-test-lab",
    legacySelectors: ["#view-test-lab > .surface", "#view-test-lab > .test-lab-shell"]
  }
};

export function App({ mode = "standalone" }: AppProps) {
  if (mode === "app-shell") {
    return (
      <AuthProvider>
        <AppShell />
      </AuthProvider>
    );
  }

  if (
    mode === "dashboard" ||
    mode === "inbox" ||
    mode === "pipeline" ||
    mode === "records" ||
    mode === "calendar" ||
    mode === "tasks" ||
    mode === "clients" ||
    mode === "logs" ||
    mode === "settings" ||
    mode === "test-lab"
  ) {
    return <FeatureIsland mode={mode} />;
  }

  if (mode === "island") {
    return (
      <div className="react-island-status" data-react-island-mounted="true" hidden>
        React island mounted
      </div>
    );
  }

  return (
    <main className="scaffold-shell" aria-labelledby="scaffold-title">
      <section className="scaffold-card">
        <p className="scaffold-eyebrow">Phase 1</p>
        <h1 id="scaffold-title">React frontend scaffold</h1>
        <p>
          This isolated Vite app verifies the React toolchain before it is mounted into the
          production FastAPI UI.
        </p>
      </section>
    </main>
  );
}

function FeatureIsland({ mode }: { mode: Exclude<AppMode, "island" | "standalone"> }) {
  const [ready, setReady] = useState(false);
  const selectedClientKey = useSelectedClientKey();
  const config = ISLAND_CONFIG[mode];
  const active = useViewActive(config?.viewId ?? "");

  useLegacyVisibility(config?.legacySelectors ?? [], active && ready);

  useEffect(() => {
    if (!active) setReady(false);
  }, [active]);

  if (!active) return null;

  return (
    <AuthProvider>
      {mode === "dashboard" ? <DashboardPage onReadyChange={setReady} selectedClientKey={selectedClientKey} /> : null}
      {mode === "inbox" ? <InboxPage onReadyChange={setReady} selectedClientKey={selectedClientKey} /> : null}
      {mode === "pipeline" ? <PipelinePage onReadyChange={setReady} selectedClientKey={selectedClientKey} /> : null}
      {mode === "records" ? <RecordsPage onReadyChange={setReady} selectedClientKey={selectedClientKey} /> : null}
      {mode === "calendar" ? <CalendarPage onReadyChange={setReady} selectedClientKey={selectedClientKey} /> : null}
      {mode === "tasks" ? <TasksPage onReadyChange={setReady} selectedClientKey={selectedClientKey} /> : null}
      {mode === "clients" ? <ClientsPage onReadyChange={setReady} /> : null}
      {mode === "logs" ? <LogsPage onReadyChange={setReady} /> : null}
      {mode === "settings" ? <SettingsPage onReadyChange={setReady} /> : null}
      {mode === "test-lab" ? <TestLabPage onReadyChange={setReady} /> : null}
    </AuthProvider>
  );
}

function useSelectedClientKey() {
  const [clientKey, setClientKey] = useState(() => window.localStorage.getItem("lead-ui-selected-client") || "");

  useEffect(() => {
    const update = (event: Event) => {
      const detailKey = (event as CustomEvent<{ clientKey?: string }>).detail?.clientKey;
      setClientKey(detailKey ?? window.localStorage.getItem("lead-ui-selected-client") ?? "");
    };
    window.addEventListener("lead-ui-client-change", update);
    return () => window.removeEventListener("lead-ui-client-change", update);
  }, []);

  return clientKey;
}

function useViewActive(viewId: string) {
  const [active, setActive] = useState(() => isActiveView(viewId));

  useEffect(() => {
    if (!viewId) return;
    const view = document.getElementById(viewId);
    if (!view) return;

    const update = () => setActive(view.classList.contains("active"));
    update();
    const observer = new MutationObserver(update);
    observer.observe(view, { attributes: true, attributeFilter: ["class"] });
    return () => observer.disconnect();
  }, [viewId]);

  return active;
}

function useLegacyVisibility(selectors: string[], hidden: boolean) {
  useEffect(() => {
    const nodes = selectors.flatMap((selector) => Array.from(document.querySelectorAll<HTMLElement>(selector)));
    nodes.forEach((node) => {
      if (hidden) {
        node.setAttribute("hidden", "");
      } else {
        node.removeAttribute("hidden");
      }
    });

    return () => {
      nodes.forEach((node) => node.removeAttribute("hidden"));
    };
  }, [hidden, selectors.join("\n")]);
}

function isActiveView(viewId: string) {
  return Boolean(viewId && document.getElementById(viewId)?.classList.contains("active"));
}
