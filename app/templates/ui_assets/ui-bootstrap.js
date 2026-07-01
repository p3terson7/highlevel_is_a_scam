      async function bootstrap() {
        await loadSession();
        await loadRuntime();
        if (isClientRole()) {
          state.selectedClientKey = state.session.client_key || "";
          state.globalSearch = "";
          state.activeLeadId = null;
          state.activeCrmLeadId = null;
          state.conversationFilters = {
            clientKey: state.selectedClientKey,
            state: "all",
            dateFrom: "",
            dateTo: "",
            unreadOnly: false,
            showArchived: false,
          };
          state.crmFilters = {
            clientKey: state.selectedClientKey,
            stage: "all",
          };
          state.taskFilters = {
            clientKey: state.selectedClientKey,
            status: "all",
          };
          saveLocalState();
          await Promise.all([loadConversations(), loadCrmLeads(), loadCalendar(), loadCrmTasks(), loadDashboard()]);
          await Promise.all([loadClients(), loadOwnerWorkspace(state.selectedClientKey), loadAutomationHealth(state.selectedClientKey)]);
          if (state.activeCrmLeadId) {
            await loadCrmLeadDetail(state.activeCrmLeadId);
          }
          renderSettings();
        } else {
          await Promise.all([loadDashboard(), loadClients()]);
          if (state.selectedClientKey) {
            await Promise.all([loadClientDetail(state.selectedClientKey), loadLogs(state.selectedClientKey), loadOwnerWorkspace(state.selectedClientKey), loadAutomationHealth(state.selectedClientKey)]);
          } else {
            renderClientWorkspace();
            renderLogs();
            renderSettings();
            renderTestLab();
            renderZapierResults();
          }
          await Promise.all([loadConversations(), loadCrmLeads(), loadCalendar(), loadCrmTasks()]);
          if (state.activeCrmLeadId) {
            await loadCrmLeadDetail(state.activeCrmLeadId);
          }
        }
        fillConversationFilterInputs();
        fillCrmFilterInputs();
        fillTaskFilterInputs();
        document.getElementById("globalSearch").value = state.globalSearch;
        document.getElementById("conversationStateFilter").value = state.conversationFilters.state || "all";
        applyRoleUi();
        routeFromHash();
        updateWindowIndicators();
        setTheme(state.theme);
        applySidebarState();
        applyPaneSizes();
        updateConversationMobileLayout();
        updateConversationFilterUi();
        if (typeof translatePage === "function") translatePage();
      }

      function wireEvents() {
        document.getElementById("loginButton").addEventListener("click", authenticate);
        document.getElementById("clearSessionButton").addEventListener("click", () => {
          clearSavedSession();
          document.getElementById("loginToken").value = "";
          document.getElementById("clientLoginPassword").value = "";
          setText("loginStatus", "Saved session cleared.");
        });
        document.getElementById("loginToken").addEventListener("keydown", (event) => {
          if (event.key === "Enter") authenticate();
        });
        document.getElementById("clientLoginPassword").addEventListener("keydown", (event) => {
          if (event.key === "Enter") authenticate();
        });
        document.getElementById("logoutButton").addEventListener("click", () => {
          clearSavedSession();
          state.session = null;
          lockUi("Saved session cleared.");
        });
        document.getElementById("sidebarToggle").addEventListener("click", toggleSidebar);
        document.getElementById("themeToggle").addEventListener("click", toggleTheme);
        document.getElementById("refreshButton").addEventListener("click", refreshCurrentView);
        document.getElementById("topLanguageToggle").addEventListener("click", () => {
          const current = typeof getUiLanguage === "function" ? getUiLanguage() : "en";
          if (typeof setUiLanguage === "function") setUiLanguage(current === "fr" ? "en" : "fr");
          refreshCurrentView();
        });
        document.getElementById("calendarPrevMonthButton").addEventListener("click", () => shiftCalendarMonth(-1));
        document.getElementById("calendarTodayButton").addEventListener("click", jumpCalendarToToday);
        document.getElementById("calendarNextMonthButton").addEventListener("click", () => shiftCalendarMonth(1));
        document.getElementById("globalSearch").addEventListener("input", scheduleSearchRefresh);
        document.getElementById("topClientSelector").addEventListener("change", async (event) => {
          await selectClient(event.target.value);
        });
        ["conversationClientFilter", "conversationStateFilter", "conversationDateFrom", "conversationDateTo", "conversationUnreadOnly", "conversationShowArchived"].forEach((id) => {
          document.getElementById(id).addEventListener("change", async () => {
            applyConversationFilterInputs();
            await loadConversations();
          });
        });
        document.getElementById("newClientButton").addEventListener("click", beginNewClient);
        document.getElementById("refreshClientsButton").addEventListener("click", async () => {
          await loadClients();
          await loadClientDetail(state.selectedClientKey);
        });
        document.getElementById("saveClientButton").addEventListener("click", saveClient);
        document.getElementById("clientPreviewSlotsButton").addEventListener("click", previewBookingSlots);
        document.getElementById("resetClientFormButton").addEventListener("click", resetClientForm);
        document.getElementById("threadSendManualButton").addEventListener("click", sendManualMessage);
        document.getElementById("threadMediaInput").addEventListener("change", updateThreadMediaPreview);
        document.getElementById("threadClearMediaButton").addEventListener("click", clearThreadMediaSelection);
        document.getElementById("threadAddNoteButton").addEventListener("click", addNote);
        document.getElementById("threadHandoffButton").addEventListener("click", markHandoff);
        document.getElementById("threadArchiveButton").addEventListener("click", () => {
          const archived = leadHasTag(state.thread?.lead?.tags || [], "archived");
          return setConversationArchived(state.activeLeadId, !archived);
        });
        document.getElementById("threadDeleteButton").addEventListener("click", deleteConversation);
        document.getElementById("threadCrmStageSaveButton").addEventListener("click", updateThreadCrmStage);
        document.getElementById("threadTagAddButton").addEventListener("click", () => addCrmTag("thread"));
        document.getElementById("crmAddLeadButton").addEventListener("click", openCrmAddLeadPanel);
        document.getElementById("crmCancelAddLeadButton").addEventListener("click", () => {
          state.crmAddLeadOpen = false;
          renderCrmBoard();
        });
        document.getElementById("manualLeadCreateButton").addEventListener("click", () => createManualLead("crm"));
        document.getElementById("calendarShowMeetingFormButton").addEventListener("click", toggleCalendarMeetingPanel);
        document.getElementById("calendarSelectedAddMeetingButton").addEventListener("click", toggleCalendarMeetingPanel);
        document.getElementById("calendarShowLeadFormButton").addEventListener("click", openCalendarLeadPanel);
        document.getElementById("calendarCreateLeadButton").addEventListener("click", () => createManualLead("calendar"));
        document.getElementById("manualMeetingLeadMode").addEventListener("change", updateManualMeetingLeadMode);
        document.getElementById("manualMeetingCreateButton").addEventListener("click", createManualMeeting);
        ["crmClientFilter", "crmStageFilter"].forEach((id) => {
          document.getElementById(id).addEventListener("change", async () => {
            applyCrmFilterInputs();
            await loadCrmLeads();
          });
        });
        ["tasksClientFilter", "tasksStatusFilter"].forEach((id) => {
          document.getElementById(id).addEventListener("change", async () => {
            applyTaskFilterInputs();
            await loadCrmTasks();
          });
        });
        document.getElementById("crmLeadStageSaveButton").addEventListener("click", updateCrmLeadStage);
        document.getElementById("crmLeadBackButton").addEventListener("click", () => setActiveView("crm"));
        document.getElementById("crmLeadArchiveToggleButton").addEventListener("click", async () => {
          state.crmFilters.showArchived = !state.crmFilters.showArchived;
          state.crmLeadDetail = null;
          state.activeCrmLeadId = null;
          saveLocalState();
          await loadCrmLeads();
        });
        document.getElementById("crmLeadArchiveButton").addEventListener("click", () => {
          const archived = leadHasTag(state.crmLeadDetail?.tags || state.crmLeadDetail?.lead?.tags || [], "archived");
          return setConversationArchived(state.activeCrmLeadId, !archived);
        });
        document.getElementById("crmTagAddButton").addEventListener("click", () => addCrmTag("crm"));
        document.getElementById("crmLeadNoteAddButton").addEventListener("click", addCrmNote);
        document.getElementById("crmTaskCreateButton").addEventListener("click", createCrmTask);
        document.getElementById("saveRuntimeButton").addEventListener("click", saveRuntimeSettings);
        document.getElementById("settingsOpenAiRevealButton").addEventListener("click", toggleOpenAiKeyVisibility);
        document.getElementById("settingsOpenAiKey").addEventListener("input", (event) => {
          const copyButton = document.getElementById("settingsOpenAiCopyButton");
          if (copyButton) copyButton.disabled = !event.target.value.trim();
        });
        document.getElementById("saveAiContextButton").addEventListener("click", saveAiContextSettings);
        document.getElementById("ingestKnowledgeButton").addEventListener("click", ingestKnowledgeUrls);
        document.getElementById("refreshKnowledgeButton").addEventListener("click", refreshKnowledgeSettings);
        document.getElementById("saveSettingsCalendarButton").addEventListener("click", saveSettingsCalendar);
        ["settingsCalendarSlotMinutes", "settingsCalendarNoticeMinutes", "settingsCalendarHorizonDays"].forEach((id) => {
          const el = document.getElementById(id);
          if (!el) return;
          el.addEventListener("input", renderSettingsCalendarVisuals);
          el.addEventListener("change", renderSettingsCalendarVisuals);
        });
        for (let day = 0; day < 7; day += 1) {
          const enabled = document.getElementById(`settingsCalDay${day}Enabled`);
          const start = document.getElementById(`settingsCalDay${day}Start`);
          const end = document.getElementById(`settingsCalDay${day}End`);
          if (enabled) enabled.addEventListener("change", renderSettingsCalendarVisuals);
          if (start) {
            start.addEventListener("input", renderSettingsCalendarVisuals);
            start.addEventListener("change", renderSettingsCalendarVisuals);
          }
          if (end) {
            end.addEventListener("input", renderSettingsCalendarVisuals);
            end.addEventListener("change", renderSettingsCalendarVisuals);
          }
        }
        document.getElementById("seedDemoButton").addEventListener("click", () => seedDemo(false));
        document.getElementById("reseedDemoButton").addEventListener("click", () => seedDemo(true));
        document.getElementById("resetDemoButton").addEventListener("click", resetDemo);
        document.getElementById("seedClientShowcaseButton").addEventListener("click", () => seedClientShowcase(false));
        document.getElementById("reseedClientShowcaseButton").addEventListener("click", () => seedClientShowcase(true));
        document.getElementById("labClientSelect").addEventListener("change", async (event) => {
          await selectClient(event.target.value);
        });
        document.getElementById("labAddFormAnswerButton").addEventListener("click", () => {
          renderTestLabAnswers([...readTestLabAnswers(), { question: "", answer: "" }]);
        });
        document.getElementById("labFormAnswerRows").addEventListener("click", (event) => {
          const button = event.target.closest("[data-action='remove-test-answer']");
          if (!button) return;
          const index = Number(button.dataset.index || 0);
          const rows = readTestLabAnswers();
          rows.splice(index, 1);
          renderTestLabAnswers(rows.length ? rows : defaultTestLabAnswers());
        });
        document.querySelectorAll("[data-action='set-test-mode']").forEach((button) => {
          button.addEventListener("click", () => setTestLabMode(button.dataset.mode || "gpt_only"));
        });
        document.getElementById("labStartButton").addEventListener("click", startTestLabSandbox);
        document.querySelectorAll(".drag-handle").forEach((handle) => {
          handle.addEventListener("pointerdown", (event) => startResizer(handle.dataset.edge, event.clientX));
        });
        let draggingLeadId = null;
        let draggingFromStage = "";
        const crmBoard = document.getElementById("crmBoard");
        crmBoard.addEventListener("dragstart", (event) => {
          const card = event.target.closest(".crm-card[draggable='true']");
          if (!card) return;
          draggingLeadId = Number(card.dataset.leadId || 0) || null;
          draggingFromStage = card.dataset.crmStage || "";
          card.classList.add("dragging");
          event.dataTransfer.effectAllowed = "move";
          event.dataTransfer.setData("text/plain", String(draggingLeadId || ""));
        });
        crmBoard.addEventListener("dragend", () => {
          draggingLeadId = null;
          draggingFromStage = "";
          document.querySelectorAll(".crm-card.dragging").forEach((node) => node.classList.remove("dragging"));
          clearCrmDropTargets();
        });
        crmBoard.addEventListener("dragover", (event) => {
          const zone = event.target.closest(".crm-stage-list[data-stage]");
          if (!zone || !draggingLeadId) return;
          event.preventDefault();
          clearCrmDropTargets();
          zone.classList.add("drop-target");
          event.dataTransfer.dropEffect = "move";
        });
        crmBoard.addEventListener("dragleave", (event) => {
          const zone = event.target.closest(".crm-stage-list[data-stage]");
          if (!zone) return;
          const toNode = event.relatedTarget;
          if (!toNode || !zone.contains(toNode)) {
            zone.classList.remove("drop-target");
          }
        });
        crmBoard.addEventListener("drop", async (event) => {
          const zone = event.target.closest(".crm-stage-list[data-stage]");
          if (!zone || !draggingLeadId) return;
          event.preventDefault();
          const targetStage = zone.dataset.stage || "";
          const leadId = draggingLeadId;
          clearCrmDropTargets();
          if (!targetStage || targetStage === draggingFromStage) return;
          try {
            await moveCrmLeadToStage(leadId, targetStage);
            showNotice(`Lead moved to ${targetStage}.`, "ok");
          } catch (error) {
            showNotice(`Move failed: ${error.message}`, "err");
          }
        });
        document.addEventListener("click", async (event) => {
          const target = event.target.closest("[data-action]");
          if (!target) return;
          const action = target.dataset.action;
          if (action === "set-view") {
            setActiveView(target.dataset.view);
            return;
          }
          if (action === "set-login-mode") {
            setLoginMode(target.dataset.loginMode);
            return;
          }
          if (action === "set-client-tab") {
            state.clientTab = target.dataset.tab;
            saveLocalState();
            renderClientWorkspace();
            return;
          }
          if (action === "set-client-wizard-step") {
            setClientWizardStep(target.dataset.step);
            return;
          }
          if (action === "client-wizard-prev") {
            moveClientWizard(-1);
            return;
          }
          if (action === "client-wizard-next") {
            moveClientWizard(1);
            return;
          }
          if (action === "begin-new-client") {
            beginNewClient();
            return;
          }
          if (action === "select-client") {
            await selectClient(target.dataset.clientKey);
            return;
          }
          if (action === "dashboard-open-client") {
            await selectClient(target.dataset.clientKey);
            setActiveView("clients");
            return;
          }
          if (action === "dashboard-open-stage") {
            state.crmFilters.stage = target.dataset.stage || "all";
            fillCrmFilterInputs();
            await loadCrmLeads();
            setActiveView("crm");
            return;
          }
          if (action === "refresh-automation-health") {
            await loadAutomationHealth(state.selectedClientKey);
            showNotice("Automation health refreshed.", "ok");
            return;
          }
          if (action === "open-automation-details") {
            if (isClientRole()) {
              setActiveView("settings");
            } else {
              setActiveView("logs");
              await loadLogs(state.selectedClientKey);
            }
            return;
          }
          if (action === "open-contact-drawer") {
            openContactActionDrawer(target.dataset.leadId, target.dataset.source || "");
            return;
          }
          if (action === "close-contact-drawer") {
            closeContactActionDrawer();
            return;
          }
          if (action === "contact-drawer-send") {
            await sendContactDrawerMessage();
            return;
          }
          if (action === "contact-drawer-agent-control") {
            await setContactDrawerAgentControl(target.dataset.paused === "true");
            return;
          }
          if (action === "contact-drawer-open-thread") {
            await openThreadFromContactDrawer();
            return;
          }
          if (action === "contact-drawer-create-meeting") {
            createMeetingFromContactDrawer();
            return;
          }
          if (action === "contact-drawer-booking-link") {
            await sendBookingLinkFromContactDrawer();
            return;
          }
          if (action === "calendar-select-day") {
            selectCalendarDate(target.dataset.dateKey);
            return;
          }
          if (action === "calendar-add-meeting") {
            if (!state.calendarMeetingPanelOpen) {
              toggleCalendarMeetingPanel();
            } else {
              syncManualMeetingFormDefaults();
            }
            return;
          }
          if (action === "calendar-meeting-status") {
            await updateManualMeetingStatus(target.dataset.meetingId, target.dataset.status);
            return;
          }
          if (action === "calendar-meeting-delete") {
            await deleteManualMeeting(target.dataset.meetingId);
            return;
          }
          if (action === "open-thread") {
            setActiveView("conversations");
            if (target.dataset.clientKey) {
              state.conversationFilters.clientKey = target.dataset.clientKey;
              fillConversationFilterInputs();
              await loadConversations();
            }
            await openThread(target.dataset.leadId);
            return;
          }
          if (action === "conversation-show-list") {
            showConversationListPanel();
            return;
          }
          if (action === "toggle-conversation-filters") {
            setConversationFiltersExpanded(!state.conversationFiltersExpanded);
            return;
          }
          if (action === "clear-conversation-filters") {
            resetConversationFilters();
            await loadConversations();
            showNotice("Conversation filters cleared.", "ok");
            return;
          }
          if (action === "crm-open-add-lead") {
            openCrmAddLeadPanel();
            return;
          }
          if (action === "open-crm-lead") {
            if (state.contactActionDrawer?.open) closeContactActionDrawer();
            setActiveView("leads");
            await openCrmLead(target.dataset.leadId);
            return;
          }
          if (action === "crm-remove-tag") {
            await removeCrmTag(target.dataset.tag || "");
            return;
          }
          if (action === "crm-task-toggle") {
            await toggleCrmTaskStatus(target.dataset.taskId, target.dataset.nextStatus);
            return;
          }
          if (action === "copy") {
            const copyValue = target.id === "settingsOpenAiCopyButton"
              ? document.getElementById("settingsOpenAiKey")?.value.trim()
              : target.dataset.copy;
            if (!copyValue) {
              showNotice("Nothing to copy.", "warn");
              return;
            }
            await copyToClipboard(copyValue);
          }
        });
        window.addEventListener("hashchange", routeFromHash);
        window.addEventListener("popstate", routeFromHash);
        window.addEventListener("resize", () => {
          applyPaneSizes();
          updateConversationMobileLayout();
          updateConversationFilterUi();
        });
      }

      wireEvents();
      setTheme(state.theme);
      if (typeof installI18nObserver === "function") installI18nObserver();
      if (typeof updateLanguageToggle === "function") updateLanguageToggle();
      applySidebarState();
      applyLoginMode();
      startLivePolling();
      applyPaneSizes();

      const savedAdminToken = adminToken();
      const savedPortalToken = portalToken();
      const savedPortalEmail = localStorage.getItem("lead-ui-portal-email") || "";
      document.getElementById("clientLoginEmail").value = savedPortalEmail;
      if (state.authMode === "client" && savedPortalToken) {
        bootstrap()
          .then(() => {
            document.getElementById("loginOverlay").classList.add("hidden");
            setText("loginStatus", "");
          })
          .catch((error) => {
            clearPortalToken();
            lockUi(`Login failed: ${error.message}`);
          });
      } else if (savedAdminToken) {
        document.getElementById("loginToken").value = savedAdminToken;
        state.authMode = "admin";
        applyLoginMode();
        authenticate();
      } else {
        lockUi();
      }
