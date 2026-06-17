      function updateWindowIndicators() {
        const isAdmin = !isClientRole();
        const selectedRuntime = state.clientDetail?.provider_runtime || state.ownerWorkspace?.runtime || state.runtime || {};
        const twilioOk = Boolean(selectedRuntime.twilio_configured);
        const aiOk = Boolean(selectedRuntime.ai_configured ?? state.runtime?.openai_api_key_configured);

        const client = selectedClient();
        setText("sidebarClientSummary", client ? `${client.business_name} (${client.client_key})` : "No client selected");
        document.getElementById("sidebarRuntimePills").innerHTML = isAdmin
          ? [
              renderBadge(twilioOk ? "sms live" : "sms mock", twilioOk ? "ok" : "warn"),
              renderBadge(aiOk ? (selectedRuntime.ai_provider_mode || state.runtime?.ai_provider_mode || "auto") : "AI offline", aiOk ? "ok" : "warn"),
            ].join("")
          : [
              renderBadge("client scope", "info"),
              renderBadge("inbox", ""),
            ].join("");
        updateChromeContext();
      }

      function applyRoleUi() {
        const isAdmin = !isClientRole();
        ["navClients", "navLogs", "navTestLab"].forEach((id) => {
          document.getElementById(id).classList.toggle("hidden", !isAdmin);
        });
        document.querySelectorAll("[data-admin-nav='true']").forEach((node) => {
          node.classList.toggle("hidden", !isAdmin);
        });
        ["view-clients", "view-logs", "view-test-lab"].forEach((id) => {
          document.getElementById(id).classList.toggle("hidden", !isAdmin);
        });
        document.querySelectorAll(".admin-only").forEach((node) => {
          node.classList.toggle("hidden", !isAdmin);
        });
        const settingsLayout = document.getElementById("settingsLayout");
        if (settingsLayout) {
          settingsLayout.classList.toggle("single-column", !isAdmin);
        }
        document.getElementById("topClientSelector").classList.toggle("hidden", !isAdmin);
        document.getElementById("conversationClientFilter").classList.toggle("hidden", !isAdmin);
        document.getElementById("conversationShowArchivedWrap").classList.toggle("hidden", isAdmin);
        document.getElementById("newClientButton").classList.toggle("hidden", !isAdmin);
        document.getElementById("refreshClientsButton").classList.toggle("hidden", !isAdmin);
        document.getElementById("threadDeleteButton").classList.toggle("hidden", !isAdmin);
        document.getElementById("threadArchiveButton").classList.toggle("hidden", isAdmin);
        document.getElementById("crmLeadBackButton").classList.toggle("hidden", isAdmin);
        document.getElementById("crmLeadArchiveButton").classList.toggle("hidden", isAdmin);
        document.getElementById("settingsClientOverviewCard").classList.toggle("hidden", isAdmin);
        document.querySelector("#navDashboard .nav-meta").textContent = isAdmin ? "Portfolio and queue" : "Overview, calendar, tasks";
        document.querySelector("#topNavCrm .top-nav-label").textContent = t("Pipeline");
        document.querySelector("#navCrm .nav-label").textContent = t("Pipeline");
        document.querySelector("#navCrm .nav-meta").textContent = t(isAdmin ? "Kanban stages" : "Pipeline and restores");
        document.querySelector("#mobileNavCrm .mobile-tab-label").textContent = t("Pipeline");
        document.querySelector("#topNavLeads .top-nav-label").textContent = t("Records");
        document.querySelector("#navLeads .nav-label").textContent = t("Records");
        document.querySelector("#navLeads .nav-meta").textContent = t(isAdmin ? "Full profiles" : "Full contact records");
        setText("conversationViewTitle", isAdmin ? "Conversations" : "Inbox");
        setText(
          "conversationViewSubtitle",
          isAdmin
            ? "Inbox-first workflow: resizable three-pane on desktop, tap-to-open thread flow on mobile."
            : "Review messages, reply directly, archive finished conversations, and keep private notes for your team."
        );
        setText("crmViewTitle", "Pipeline");
        setText(
          "crmViewSubtitle",
          isAdmin
            ? "Stage board for daily pipeline operations. Drag cards between stages to update status."
            : "Track active opportunities, restore archived ones, and keep every record in one place."
        );
        setText("leadDetailsViewTitle", isAdmin ? "Records" : "Contact Details");
        setText(
          "leadDetailsViewSubtitle",
          isAdmin
            ? "CRM record with stage, notes, tasks, tags, and timeline."
            : "Open any contact to see the full record, update the stage, and work through notes and follow-ups."
        );
        setText("calendarViewTitle", "Calendar");
        setText(
          "calendarViewSubtitle",
          isAdmin
            ? "Month view of booked meetings with a task sidebar for fast follow-up."
            : "Your monthly calendar and task list in one place, so you can log in and know what is planned."
        );
        setText("settingsViewTitle", isAdmin ? "Settings" : "Business Settings");
        setText(
          "settingsViewSubtitle",
          isAdmin
            ? "Manage provider defaults, client webhooks, AI guidance, booking availability, and live test tools in one workspace."
            : "Update your assistant guidance, booking availability, and send yourself a live test."
        );
        setText("threadNotesSummary", isAdmin ? "Internal notes" : "Private notes");
        setText("threadAuditSummary", isAdmin ? "Activity" : "Technical details");
        setText("threadDetailsTitle", isAdmin ? "Contact" : "Contact details");
        setText("threadHandoffButton", isAdmin ? "Handoff" : "Handoff");
        const threadSendButton = document.getElementById("threadSendManualButton");
        if (threadSendButton) {
          const sendLabel = isAdmin ? "Send message" : "Send reply";
          threadSendButton.setAttribute("aria-label", sendLabel);
          threadSendButton.setAttribute("title", sendLabel);
        }
        if (!isAdmin) {
          const clientAllowedViews = new Set(["dashboard", "conversations", "crm", "leads", "calendar", "tasks", "settings"]);
          if (!clientAllowedViews.has(state.activeView)) {
            state.activeView = "dashboard";
            window.location.hash = "dashboard";
          }
        }
        updateChromeContext();
      }

      function setActiveView(view, pushHash = true) {
        state.activeView = view;
        if (view === "conversations" && isMobileViewport()) {
          state.conversationMobilePanel = "list";
        }
        if (pushHash) window.location.hash = view;
        document.querySelectorAll(".view").forEach((node) => node.classList.toggle("active", node.id === `view-${view}`));
        document.querySelectorAll(".nav-item").forEach((node) => node.classList.toggle("active", node.dataset.view === view));
        if (isMobileViewport()) {
          const workspace = document.querySelector(".workspace");
          if (workspace) workspace.scrollTo({ top: 0, left: 0, behavior: "auto" });
        }
        updateConversationMobileLayout();
        updateConversationFilterUi();
        updateChromeContext();
        saveLocalState();
      }

      function isMobileViewport() {
        return window.matchMedia("(max-width: 920px)").matches;
      }

      function updateConversationMobileLayout() {
        const shell = document.getElementById("conversationShell");
        const backButton = document.getElementById("conversationMobileBackButton");
        if (!shell || !backButton) return;
        const mobile = isMobileViewport();
        shell.classList.remove("mobile-list", "mobile-thread");
        if (!mobile) {
          backButton.classList.add("hidden");
          return;
        }
        const panel = state.conversationMobilePanel === "thread" && state.activeLeadId ? "thread" : "list";
        state.conversationMobilePanel = panel;
        shell.classList.add(panel === "thread" ? "mobile-thread" : "mobile-list");
        backButton.classList.toggle("hidden", panel !== "thread");
      }

      function showConversationListPanel() {
        state.conversationMobilePanel = "list";
        updateConversationMobileLayout();
        saveLocalState();
      }

      function setConversationFiltersExpanded(expanded) {
        state.conversationFiltersExpanded = Boolean(expanded);
        updateConversationFilterUi();
        saveLocalState();
      }

      function updateConversationFilterUi() {
        const view = document.getElementById("view-conversations");
        const toggle = document.getElementById("conversationFiltersToggle");
        if (!view || !toggle) return;
        const mobile = window.matchMedia("(max-width: 680px)").matches;
        if (!mobile) {
          view.classList.remove("mobile-filters-open");
          toggle.classList.add("hidden");
          toggle.setAttribute("aria-expanded", "false");
          return;
        }
        toggle.classList.remove("hidden");
        const isOpen = Boolean(state.conversationFiltersExpanded);
        view.classList.toggle("mobile-filters-open", isOpen);
        toggle.setAttribute("aria-expanded", isOpen ? "true" : "false");
        toggle.textContent = isOpen ? "Hide filters" : "Filters";
      }

      function isNearBottom(node, threshold = 72) {
        if (!node) return true;
        return (node.scrollHeight - node.scrollTop - node.clientHeight) <= threshold;
      }

      function routeFromHash() {
        const view = (window.location.hash || `#${state.activeView}`).replace(/^#/, "");
        const allowed = state.session?.role === "client"
          ? ["dashboard", "conversations", "crm", "leads", "calendar", "tasks", "settings"]
          : ["dashboard", "clients", "conversations", "crm", "leads", "calendar", "tasks", "logs", "settings", "test-lab"];
        const nextView = allowed.includes(view) ? view : "dashboard";
        setActiveView(nextView, false);
        if (view !== nextView) {
          window.location.hash = nextView;
        }
      }

      function renderPreviewConversation(item, includeClient = true) {
        const visibleTags = uniqueStatusTags(item.tags || [], [item.crm_stage, item.state]);
        const summary = item.lead_summary && item.lead_summary !== "No qualification details captured yet."
          ? `<div class="item-summary">${escapeHtml(item.lead_summary)}</div>`
          : "";
        return `
          <div class="preview-item" data-action="open-thread" data-lead-id="${item.lead_id}">
            <div class="item-title-row">
              <div class="item-title">${escapeHtml(item.lead_name || item.phone || `Contact ${item.lead_id}`)}</div>
              <div class="actions">
                ${item.crm_stage ? renderBadge(item.crm_stage, "info") : ""}
                ${maybeRenderConversationState(item.crm_stage, item.state)}
              </div>
            </div>
            <div class="item-subtitle">${escapeHtml(item.phone || "-")}${includeClient ? ` · ${escapeHtml(item.client_name || "")}` : ""}</div>
            <div class="item-snippet">${renderLabeledSnippet(item, "No messages yet.", 120)}</div>
            ${summary}
            <div class="item-meta-row">
              <div class="chip-row">${visibleTags.map((tag) => renderTag(tag, tag.includes("handoff") ? "warn" : "")).join("")}</div>
              <div class="meta-text">${escapeHtml(formatDateTime(item.last_activity_at))}</div>
            </div>
          </div>
        `;
      }

      function renderPreviewLog(log) {
        return `
          <div class="preview-item ${log.lead_id ? "" : ""}">
            <div class="item-title-row">
              <div class="item-title mono">${escapeHtml(log.event_type)}</div>
              <div class="meta-text">${escapeHtml(formatDateTime(log.created_at))}</div>
            </div>
            <div class="item-snippet mono">record ${escapeHtml(log.lead_id ?? "-")}</div>
          </div>
        `;
      }

      function renderChecklist(items) {
        if (!items?.length) return '<div class="empty-state">Nothing to show.</div>';
        return items.map((item) => `
          <div class="check-item">
            <div class="item-title-row">
              <div>
                <div class="item-title">${escapeHtml(item.label)}</div>
                <div class="item-snippet">${escapeHtml(item.detail || "")}</div>
              </div>
              ${renderBadge(item.done ? "done" : "pending", item.done ? "ok" : "warn")}
            </div>
          </div>
        `).join("");
      }

      function updateClientSelectors() {
        const options = state.clients.map((client) => `<option value="${client.client_key}">${escapeHtml(client.business_name)} (${escapeHtml(client.client_key)})</option>`).join("");
        document.getElementById("topClientSelector").innerHTML = state.clients.length ? options : '<option value="">No clients</option>';
        if (state.selectedClientKey) document.getElementById("topClientSelector").value = state.selectedClientKey;

        document.getElementById("conversationClientFilter").innerHTML = `<option value="">All clients</option>${options}`;
        document.getElementById("conversationClientFilter").value = state.conversationFilters.clientKey || "";
        document.getElementById("crmClientFilter").innerHTML = `<option value="">All clients</option>${options}`;
        document.getElementById("crmClientFilter").value = state.crmFilters.clientKey || "";
        document.getElementById("tasksClientFilter").innerHTML = `<option value="">All clients</option>${options}`;
        document.getElementById("tasksClientFilter").value = state.taskFilters.clientKey || "";

        const labClientSelect = document.getElementById("labClientSelect");
        if (labClientSelect) {
          labClientSelect.innerHTML = state.clients.length ? options : '<option value="">No clients</option>';
          labClientSelect.value = state.selectedClientKey || state.clients[0]?.client_key || "";
        }
      }

      async function loadSession() {
        state.session = await apiJson("/ui/api/session");
      }

      async function loadRuntime() {
        if (state.session?.role === "client") {
          state.runtime = null;
          return;
        }
        state.runtime = await apiJson("/admin/runtime-config/status");
      }

      async function loadDashboard() {
        state.dashboard = await apiJson("/ui/api/dashboard");
        renderDashboard();
      }

      async function loadClients() {
        if (state.session?.role === "client") {
          state.clients = state.session?.client_key
            ? [{
                id: 0,
                client_key: state.session.client_key,
                business_name: state.session.client_name || state.session.client_key,
                tone: "",
                timezone: "",
                booking_url: "",
                is_active: true,
                portal_enabled: true,
                lead_count: state.conversations.total || 0,
                open_conversations: (state.conversations.items || []).filter((item) => !["BOOKED", "OPTED_OUT"].includes(item.state)).length,
                last_activity_at: state.conversations.items?.[0]?.last_activity_at || null,
                last_webhook_received_at: null,
              }]
            : [];
          state.selectedClientKey = state.session?.client_key || "";
          state.crmFilters.clientKey = state.selectedClientKey;
          state.taskFilters.clientKey = state.selectedClientKey;
          updateClientSelectors();
          updateWindowIndicators();
          return;
        }
        state.clients = await apiJson("/ui/api/clients");
        if (!state.selectedClientKey && state.clients.length) state.selectedClientKey = state.clients[0].client_key;
        if (state.selectedClientKey && !state.clients.some((client) => client.client_key === state.selectedClientKey)) {
          state.selectedClientKey = state.clients[0]?.client_key || "";
        }
        updateClientSelectors();
        renderClients();
        updateWindowIndicators();
      }

      async function loadClientDetail(clientKey = state.selectedClientKey) {
        if (state.session?.role === "client") {
          state.clientDetail = null;
          return;
        }
        if (!clientKey) {
          state.clientDetail = null;
          renderClientWorkspace();
          renderSettings();
          return;
        }
        state.clientDetail = await apiJson(`/ui/api/clients/${encodeURIComponent(clientKey)}`);
        renderClientWorkspace();
        renderSettings();
      }

      async function loadLogs(clientKey = state.selectedClientKey) {
        if (state.session?.role === "client") {
          state.logEvents = null;
          state.logs = [];
          return;
        }
        if (!clientKey) {
          state.logEvents = null;
          state.logs = [];
          renderLogs();
          return;
        }
        const [events, logs] = await Promise.all([
          apiJson(`/admin/clients/${encodeURIComponent(clientKey)}/events`),
          apiJson(`/admin/clients/${encodeURIComponent(clientKey)}/audit-logs?limit=80`),
        ]);
        state.logEvents = events;
        state.logs = logs;
        renderLogs();
      }

      async function loadOwnerWorkspace(clientKey = state.selectedClientKey) {
        const effectiveClientKey = state.session?.role === "client"
          ? (state.session?.client_key || clientKey)
          : clientKey;
        if (!effectiveClientKey) {
          state.ownerWorkspace = null;
          state.zapierResults = null;
          state.knowledge = null;
          renderTestLab();
          renderZapierResults();
          renderSettings();
          return;
        }
        const ownerWorkspacePromise = apiJson(`/ui/api/owner/${encodeURIComponent(effectiveClientKey)}`);
        const zapierPromise = state.session?.role === "client"
          ? Promise.resolve(null)
          : apiJson(`/ui/api/clients/${encodeURIComponent(effectiveClientKey)}/zapier-results?limit=30`);
        const [ownerWorkspace, zapierResults] = await Promise.all([ownerWorkspacePromise, zapierPromise]);
        state.ownerWorkspace = ownerWorkspace;
        state.zapierResults = zapierResults;
        state.knowledge = ownerWorkspace?.knowledge || null;
        renderTestLab();
        renderZapierResults();
        renderConversationClientGuide();
        renderSettings();
      }

      function applyConversationFilterInputs() {
        state.conversationFilters.clientKey = document.getElementById("conversationClientFilter").value;
        state.conversationFilters.state = document.getElementById("conversationStateFilter").value;
        state.conversationFilters.dateFrom = document.getElementById("conversationDateFrom").value;
        state.conversationFilters.dateTo = document.getElementById("conversationDateTo").value;
        state.conversationFilters.unreadOnly = document.getElementById("conversationUnreadOnly").checked;
        state.conversationFilters.showArchived = document.getElementById("conversationShowArchived").checked;
        saveLocalState();
      }

      function fillConversationFilterInputs() {
        document.getElementById("conversationClientFilter").value = state.conversationFilters.clientKey || "";
        document.getElementById("conversationStateFilter").value = state.conversationFilters.state || "all";
        document.getElementById("conversationDateFrom").value = state.conversationFilters.dateFrom || "";
        document.getElementById("conversationDateTo").value = state.conversationFilters.dateTo || "";
        document.getElementById("conversationUnreadOnly").checked = state.conversationFilters.unreadOnly;
        document.getElementById("conversationShowArchived").checked = state.conversationFilters.showArchived;
      }

      function resetConversationFilters() {
        state.conversationFilters.state = "all";
        state.conversationFilters.dateFrom = "";
        state.conversationFilters.dateTo = "";
        state.conversationFilters.unreadOnly = false;
        state.conversationFilters.showArchived = false;
        if (state.session?.role === "client") {
          state.conversationFilters.clientKey = state.session.client_key || "";
        }
        fillConversationFilterInputs();
        saveLocalState();
      }

      function applyCrmFilterInputs() {
        state.crmFilters.clientKey = document.getElementById("crmClientFilter").value;
        state.crmFilters.stage = document.getElementById("crmStageFilter").value;
        saveLocalState();
      }

      function fillCrmFilterInputs() {
        document.getElementById("crmClientFilter").value = state.crmFilters.clientKey || "";
        document.getElementById("crmStageFilter").value = state.crmFilters.stage || "all";
      }

      function applyTaskFilterInputs() {
        state.taskFilters.clientKey = document.getElementById("tasksClientFilter").value;
        state.taskFilters.status = document.getElementById("tasksStatusFilter").value;
        saveLocalState();
      }

      function fillTaskFilterInputs() {
        document.getElementById("tasksClientFilter").value = state.taskFilters.clientKey || "";
        document.getElementById("tasksStatusFilter").value = state.taskFilters.status || "all";
      }

      async function loadConversations() {
        const url = new URL("/ui/api/conversations", window.location.origin);
        const scopedClientKey = state.session?.role === "client" ? state.session.client_key : state.conversationFilters.clientKey;
        if (scopedClientKey) url.searchParams.set("client_key", scopedClientKey);
        if (state.conversationFilters.state && state.conversationFilters.state !== "all") url.searchParams.set("state", state.conversationFilters.state);
        if (state.conversationFilters.dateFrom) url.searchParams.set("date_from", state.conversationFilters.dateFrom);
        if (state.conversationFilters.dateTo) url.searchParams.set("date_to", state.conversationFilters.dateTo);
        if (state.globalSearch.trim()) url.searchParams.set("q", state.globalSearch.trim());
        state.conversations = await apiJson(`${url.pathname}${url.search}`);
        renderConversationList();
        renderDashboard();
        const visibleItems = filteredConversationItems();
        if (!visibleItems.length) {
          state.activeLeadId = null;
          state.thread = null;
          state.conversationMobilePanel = "list";
          renderThread();
          updateConversationMobileLayout();
          saveLocalState();
          return;
        }
        const stillVisible = visibleItems.some((item) => item.lead_id === state.activeLeadId);
        if (!stillVisible) {
          if (isMobileViewport()) {
            state.activeLeadId = null;
            state.thread = null;
            state.conversationMobilePanel = "list";
            renderThread();
            updateConversationMobileLayout();
            saveLocalState();
          } else {
            await openThread(visibleItems[0].lead_id);
          }
        } else {
          if (!state.thread && state.activeLeadId) {
            await openThread(state.activeLeadId);
          }
        }
      }

      async function loadCrmLeads() {
        const url = new URL("/ui/api/crm/leads", window.location.origin);
        const scopedClientKey = state.session?.role === "client" ? state.session.client_key : state.crmFilters.clientKey;
        if (scopedClientKey) url.searchParams.set("client_key", scopedClientKey);
        if (state.crmFilters.stage && state.crmFilters.stage !== "all") url.searchParams.set("stage", state.crmFilters.stage);
        if (state.globalSearch.trim()) url.searchParams.set("q", state.globalSearch.trim());
        if (state.crmFilters.showArchived) url.searchParams.set("archived", "true");
        state.crmLeads = await apiJson(`${url.pathname}${url.search}`);
        renderCrmBoard();
        renderLeadsView();
        if (!state.crmLeadDetail && state.crmLeads.items?.length) {
          await openCrmLead(state.crmLeads.items[0].lead_id);
        } else if (state.activeCrmLeadId && !state.crmLeads.items.some((item) => item.lead_id === state.activeCrmLeadId)) {
          state.activeCrmLeadId = state.crmLeads.items[0]?.lead_id || null;
          state.crmLeadDetail = null;
          if (state.activeCrmLeadId) {
            await openCrmLead(state.activeCrmLeadId);
          }
        } else {
          renderLeadsView();
        }
      }

      async function loadCrmLeadDetail(leadId = state.activeCrmLeadId) {
        if (!leadId) {
          state.crmLeadDetail = null;
          renderLeadsView();
          return;
        }
        state.crmLeadDetail = await apiJson(`/ui/api/crm/leads/${leadId}`);
        renderLeadsView();
      }

      async function openCrmLead(leadId) {
        state.activeCrmLeadId = Number(leadId);
        saveLocalState();
        await loadCrmLeadDetail(state.activeCrmLeadId);
      }

      async function loadCrmTasks() {
        const url = new URL("/ui/api/crm/tasks", window.location.origin);
        const scopedClientKey = state.session?.role === "client" ? state.session.client_key : state.taskFilters.clientKey;
        if (scopedClientKey) url.searchParams.set("client_key", scopedClientKey);
        if (state.taskFilters.status && state.taskFilters.status !== "all") url.searchParams.set("status", state.taskFilters.status);
        if (state.globalSearch.trim()) url.searchParams.set("q", state.globalSearch.trim());
        state.crmTasks = await apiJson(`${url.pathname}${url.search}`);
        renderTasksView();
        renderCalendarView();
      }

      async function loadCalendar() {
        const clientKey = state.session?.role === "client"
          ? state.session.client_key
          : state.selectedClientKey;
        if (!clientKey) {
          state.calendar = { items: [], total: 0, timezone: "UTC", booking_mode: "link" };
          renderCalendarView();
          return;
        }
        state.calendar = await apiJson(`/ui/api/clients/${encodeURIComponent(clientKey)}/calendar`);
        renderCalendarView();
      }

      function filteredConversationItems() {
        let items = state.conversations.items || [];
        if (isClientRole() && !state.conversationFilters.showArchived) {
          items = items.filter((item) => !leadHasTag(item.tags, "archived"));
        }
        if (state.conversationFilters.unreadOnly) {
          items = items.filter((item) => isUnreadConversation(item));
        }
        return items;
      }

      async function openThread(leadId) {
        state.activeLeadId = Number(leadId);
        saveLocalState();
        state.thread = await apiJson(`/ui/api/conversations/${leadId}/thread`);
        const current = state.conversations.items.find((item) => item.lead_id === Number(leadId));
        if (current?.last_activity_at) {
          state.viewedMap[leadId] = current.last_activity_at;
          saveLocalState();
        }
        if (isMobileViewport()) {
          state.conversationMobilePanel = "thread";
          saveLocalState();
        }
        renderConversationList();
        renderDashboard();
        renderThread();
        updateConversationMobileLayout();
      }

      function isUnreadConversation(item) {
        const viewed = state.viewedMap[item.lead_id];
        if (!viewed) return true;
        return new Date(item.last_activity_at).getTime() > new Date(viewed).getTime();
      }
