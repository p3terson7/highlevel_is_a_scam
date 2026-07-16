import React from "react";
import ReactDOM from "react-dom/client";
import { App } from "./app/App";
import "./styles/app.css";

const islandRoots = Array.from(document.querySelectorAll<HTMLElement>("[data-react-island]"));
const standaloneRoot = document.getElementById("react-root");
const roots = islandRoots.length ? islandRoots : standaloneRoot ? [standaloneRoot] : [];

applySavedShellVisualState();

for (const root of roots) {
  const mode = islandMode(root.dataset.reactIsland);

  ReactDOM.createRoot(root).render(
    <React.StrictMode>
      <App mode={mode} />
    </React.StrictMode>
  );
}

function islandMode(value: string | undefined) {
  if (!value && standaloneRoot?.dataset.reactAppShell === "true") {
    return "app-shell";
  }
  if (
    value === "calendar" ||
    value === "clients" ||
    value === "dashboard" ||
    value === "inbox" ||
    value === "logs" ||
    value === "pipeline" ||
    value === "records" ||
    value === "settings" ||
    value === "tasks" ||
    value === "test-lab"
  ) {
    return value;
  }
  if (value === "phase-3") return "island";
  return "standalone";
}

function applySavedShellVisualState() {
  if (standaloneRoot?.dataset.reactAppShell !== "true") return;
  try {
    const savedTheme = window.localStorage.getItem("lead-ui-theme");
    document.body.dataset.theme = savedTheme === "light" ? "light" : "dark";
  } catch {
    document.body.dataset.theme = "dark";
  }
}
