      async function saveClient() {
        try {
          const payload = clientFormPayload(state.creatingClient);
          const portalPasswordUpdated = Boolean(payload.portal_password);
          let result;
          if (state.creatingClient) {
            result = await apiJson("/admin/clients", {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify(payload),
            });
            state.creatingClient = false;
            state.selectedClientKey = result.client_key;
            showNotice(`Created ${result.business_name}.`, "ok");
          } else {
            result = await apiJson(`/admin/clients/${encodeURIComponent(state.selectedClientKey)}`, {
              method: "PATCH",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify(payload),
            });
            showNotice(`Saved ${result.business_name}.`, "ok");
          }
          document.getElementById("clientPortalPassword").value = "";
          setText("clientSaveStatus", portalPasswordUpdated ? "Saved. Portal password updated." : "Saved.");
          setText(
            "clientPortalStatus",
            portalPasswordUpdated ? "Password was updated. Use the new value at client login." : "Password is configured. Set a new value to rotate it."
          );
          await Promise.all([loadClients(), loadClientDetail(state.selectedClientKey), loadLogs(state.selectedClientKey), loadOwnerWorkspace(state.selectedClientKey), loadCalendar(), loadDashboard()]);
        } catch (error) {
          setText("clientSaveStatus", `Save failed: ${error.message}`);
          showNotice(`Save failed: ${error.message}`, "err");
        }
      }

      async function previewBookingSlots() {
        if (state.creatingClient) {
          setText("clientBookingPreviewStatus", "Save the client first.");
          return;
        }
        if (!state.selectedClientKey) {
          setText("clientBookingPreviewStatus", "Select a client first.");
          return;
        }
        setText("clientBookingPreviewStatus", "Loading slots...");
        try {
          const payload = await apiJson(`/ui/api/clients/${encodeURIComponent(state.selectedClientKey)}/booking-preview`);
          const lines = (payload.slots || []).map((slot) => `${slot.index}) ${slot.display_time}`);
          setText("clientBookingPreviewStatus", lines.length ? lines.join(" | ") : "No slots returned.");
          showNotice("Fetched live booking slots.", "ok");
        } catch (error) {
          setText("clientBookingPreviewStatus", `Preview failed: ${error.message}`);
          showNotice(`Booking preview failed: ${error.message}`, "err");
        }
      }

      async function selectClient(clientKey) {
        if (state.session?.role === "client") return;
        state.creatingClient = false;
        state.selectedClientKey = clientKey;
        updateClientSelectors();
        updateWindowIndicators();
        await Promise.all([loadClientDetail(clientKey), loadLogs(clientKey), loadOwnerWorkspace(clientKey), loadCalendar()]);
        renderClients();
        renderSettings();
        renderTestLab();
        saveLocalState();
      }

      async function saveRuntimeSettings() {
        const payload = {
          openai_model: document.getElementById("settingsOpenAiModel").value.trim() || "gpt-4.1-mini",
          ai_provider_mode: document.getElementById("settingsAiMode").value,
        };
        const maybeSet = [
          ["openai_api_key", document.getElementById("settingsOpenAiKey").value.trim()],
        ];
        maybeSet.forEach(([key, value]) => {
          if (value) payload[key] = value;
        });
        try {
          const result = await apiJson("/admin/runtime-config", {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
          });
          setText("settingsRuntimeStatus", `Updated ${result.updated_keys.join(", ") || "no keys"}.`);
          await Promise.all([loadRuntime(), loadDashboard(), loadClientDetail(state.selectedClientKey), loadOwnerWorkspace(state.selectedClientKey)]);
          updateWindowIndicators();
          renderSettings();
          renderDashboard();
          renderTestLab();
          showNotice("Runtime settings saved.", "ok");
        } catch (error) {
          setText("settingsRuntimeStatus", `Save failed: ${error.message}`);
          showNotice(`Runtime save failed: ${error.message}`, "err");
        }
      }

      async function saveAiContextSettings() {
        if (!state.selectedClientKey) {
          setText("settingsAiContextStatus", "Select a client first.");
          return;
        }
        const payload = {
          ai_context: document.getElementById("settingsAiContextInput").value.trim(),
          faq_context: document.getElementById("settingsFaqContextInput").value.trim(),
        };
        if (isClientRole()) {
          const confirmMessage = payload.ai_context
            ? "Save these assistant instructions? New AI replies will use them immediately."
            : "Save with blank assistant guidance? New AI replies may become less specific.";
          if (!window.confirm(confirmMessage)) return;
        }
        try {
          const result = await apiJson(`/ui/api/owner/${encodeURIComponent(state.selectedClientKey)}/ai-context`, {
            method: "PATCH",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
          });
          setText("settingsAiContextStatus", `Saved at ${formatDateTime(result.updated_at)}.`);
          if (state.clientDetail?.client?.client_key === state.selectedClientKey) {
            state.clientDetail.client.ai_context = result.ai_context || "";
            state.clientDetail.client.faq_context = result.faq_context || "";
          }
          if (state.ownerWorkspace?.client?.client_key === state.selectedClientKey) {
            state.ownerWorkspace.client.ai_context = result.ai_context || "";
            state.ownerWorkspace.client.faq_context = result.faq_context || "";
          }
          showNotice("AI context saved.", "ok");
        } catch (error) {
          setText("settingsAiContextStatus", `Save failed: ${error.message}`);
          showNotice(`AI context save failed: ${error.message}`, "err");
        }
      }

      async function saveSettingsCalendar() {
        if (!state.selectedClientKey) {
          setText("settingsCalendarStatus", "Select a client first.");
          return;
        }
        const payload = readSettingsCalendarFromForm();
        if (isClientRole()) {
          const currentMode = state.ownerWorkspace?.client?.booking_mode || state.clientDetail?.client?.booking_mode || "link";
          const confirmMessage = currentMode === "internal"
            ? "Save this booking availability? New booking offers will use it right away."
            : "Save this availability and switch new booking offers to the internal calendar?";
          if (!window.confirm(confirmMessage)) return;
        }
        try {
          const result = await apiJson(`/ui/api/owner/${encodeURIComponent(state.selectedClientKey)}/calendar`, {
            method: "PATCH",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
          });
          setText("settingsCalendarStatus", `Saved at ${formatDateTime(result.updated_at)}.`);
          await Promise.all([
            loadOwnerWorkspace(state.selectedClientKey),
            loadCalendar(),
            loadClientDetail(state.selectedClientKey),
          ]);
          renderSettings();
          renderClientWorkspace();
          showNotice("Calendar availability saved.", "ok");
        } catch (error) {
          setText("settingsCalendarStatus", `Save failed: ${error.message}`);
          showNotice(`Calendar save failed: ${error.message}`, "err");
        }
      }

      async function refreshCrmData(keepDetail = true) {
        await Promise.all([loadCrmLeads(), loadCrmTasks()]);
        if (keepDetail && state.activeCrmLeadId) {
          await loadCrmLeadDetail(state.activeCrmLeadId);
        }
      }

      async function updateThreadCrmStage() {
        if (!state.activeLeadId) return;
        const stage = document.getElementById("threadCrmStageSelect").value;
        try {
          await apiJson(`/ui/api/crm/leads/${state.activeLeadId}/stage`, {
            method: "PATCH",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ stage }),
          });
          setText("threadCrmStageStatus", "Updated.");
          await Promise.all([
            loadConversations(),
            openThread(state.activeLeadId),
            refreshCrmData(false),
          ]);
          showNotice("CRM stage updated.", "ok");
        } catch (error) {
          setText("threadCrmStageStatus", `Update failed: ${error.message}`);
          showNotice(`CRM stage update failed: ${error.message}`, "err");
        }
      }

      async function updateCrmLeadStage() {
        if (!state.activeCrmLeadId) return;
        const stage = document.getElementById("crmLeadStageSelect").value;
        try {
          await apiJson(`/ui/api/crm/leads/${state.activeCrmLeadId}/stage`, {
            method: "PATCH",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ stage }),
          });
          setText("crmLeadStatus", "Stage updated.");
          await Promise.all([
            loadCrmLeads(),
            loadConversations(),
            loadCrmLeadDetail(state.activeCrmLeadId),
          ]);
          if (state.activeLeadId === state.activeCrmLeadId) {
            await openThread(state.activeLeadId);
          }
          showNotice("Lead stage updated.", "ok");
        } catch (error) {
          setText("crmLeadStatus", `Stage update failed: ${error.message}`);
          showNotice(`Lead stage update failed: ${error.message}`, "err");
        }
      }

      function clearCrmDropTargets() {
        document.querySelectorAll(".crm-stage-list.drop-target").forEach((node) => node.classList.remove("drop-target"));
      }

      async function moveCrmLeadToStage(leadId, stage) {
        if (!leadId || !stage) return;
        await apiJson(`/ui/api/crm/leads/${leadId}/stage`, {
          method: "PATCH",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ stage }),
        });
        await Promise.all([
          loadCrmLeads(),
          loadConversations(),
          state.activeCrmLeadId ? loadCrmLeadDetail(state.activeCrmLeadId) : Promise.resolve(),
        ]);
        if (state.activeLeadId && (Number(leadId) === state.activeLeadId || state.activeLeadId === state.activeCrmLeadId)) {
          await openThread(state.activeLeadId);
        }
      }

      async function addCrmTag(source = "") {
        const leadId = Number(state.activeCrmLeadId || state.activeLeadId || 0) || 0;
        if (!leadId) return;
        const crmTagInput = document.getElementById("crmTagInput");
        const threadTagInput = document.getElementById("threadTagInput");
        const selectedInput = source === "thread" ? threadTagInput : source === "crm" ? crmTagInput : (crmTagInput?.value ? crmTagInput : threadTagInput);
        const tag = (selectedInput?.value || "").trim();
        if (!tag) {
          setText("crmLeadStatus", "Tag is required.");
          return;
        }
        try {
          await apiJson(`/ui/api/crm/leads/${leadId}/tags`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ tag }),
          });
          if (selectedInput) selectedInput.value = "";
          await Promise.all([
            loadCrmLeads(),
            loadConversations(),
            state.activeCrmLeadId ? loadCrmLeadDetail(state.activeCrmLeadId) : Promise.resolve(),
          ]);
          if (state.activeLeadId) {
            await openThread(state.activeLeadId);
          }
          showNotice("Tag added.", "ok");
        } catch (error) {
          setText("crmLeadStatus", `Tag failed: ${error.message}`);
          showNotice(`Tag failed: ${error.message}`, "err");
        }
      }

      async function removeCrmTag(tag) {
        const leadId = Number(state.activeCrmLeadId || state.activeLeadId || 0) || 0;
        if (!leadId) return;
        try {
          await apiJson(`/ui/api/crm/leads/${leadId}/tags/${encodeURIComponent(tag)}`, {
            method: "DELETE",
          });
          await Promise.all([
            loadCrmLeads(),
            loadConversations(),
            state.activeCrmLeadId ? loadCrmLeadDetail(state.activeCrmLeadId) : Promise.resolve(),
          ]);
          if (state.activeLeadId) {
            await openThread(state.activeLeadId);
          }
          showNotice("Tag removed.", "ok");
        } catch (error) {
          setText("crmLeadStatus", `Tag remove failed: ${error.message}`);
          showNotice(`Tag remove failed: ${error.message}`, "err");
        }
      }

      async function addCrmNote() {
        if (!state.activeCrmLeadId) return;
        const note = document.getElementById("crmLeadNoteInput").value.trim();
        if (!note) {
          setText("crmLeadStatus", "Note is required.");
          return;
        }
        try {
          await apiJson(`/ui/api/crm/leads/${state.activeCrmLeadId}/notes`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ note }),
          });
          document.getElementById("crmLeadNoteInput").value = "";
          await Promise.all([
            loadCrmLeads(),
            loadConversations(),
            loadCrmLeadDetail(state.activeCrmLeadId),
          ]);
          if (state.activeLeadId === state.activeCrmLeadId) {
            await openThread(state.activeLeadId);
          }
          showNotice("CRM note saved.", "ok");
        } catch (error) {
          setText("crmLeadStatus", `Note failed: ${error.message}`);
          showNotice(`CRM note failed: ${error.message}`, "err");
        }
      }

      async function createCrmTask() {
        if (!state.activeCrmLeadId) return;
        const title = document.getElementById("crmTaskTitle").value.trim();
        if (!title) {
          setText("crmLeadStatus", "Task title is required.");
          return;
        }
        const payload = {
          title,
          due_date: document.getElementById("crmTaskDueDate").value || null,
          description: document.getElementById("crmTaskDescription").value.trim() || null,
        };
        try {
          await apiJson(`/ui/api/crm/leads/${state.activeCrmLeadId}/tasks`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
          });
          document.getElementById("crmTaskTitle").value = "";
          document.getElementById("crmTaskDueDate").value = "";
          document.getElementById("crmTaskDescription").value = "";
          await Promise.all([
            loadCrmTasks(),
            loadCrmLeads(),
            loadCrmLeadDetail(state.activeCrmLeadId),
          ]);
          if (state.activeLeadId === state.activeCrmLeadId) {
            await openThread(state.activeLeadId);
          }
          showNotice("Task created.", "ok");
        } catch (error) {
          setText("crmLeadStatus", `Task failed: ${error.message}`);
          showNotice(`Task failed: ${error.message}`, "err");
        }
      }

      async function toggleCrmTaskStatus(taskId, nextStatus) {
        try {
          await apiJson(`/ui/api/crm/tasks/${taskId}`, {
            method: "PATCH",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ status: nextStatus }),
          });
          await Promise.all([
            loadCrmTasks(),
            loadCrmLeads(),
            state.activeCrmLeadId ? loadCrmLeadDetail(state.activeCrmLeadId) : Promise.resolve(),
          ]);
          if (state.activeLeadId && state.activeCrmLeadId === state.activeLeadId) {
            await openThread(state.activeLeadId);
          }
          showNotice(`Task marked ${nextStatus}.`, "ok");
        } catch (error) {
          setText("crmLeadStatus", `Task update failed: ${error.message}`);
          showNotice(`Task update failed: ${error.message}`, "err");
        }
      }

      async function addNote() {
        if (!state.activeLeadId) return;
        const note = document.getElementById("threadNoteInput").value.trim();
        if (!note) {
          setText("threadNoteStatus", "Note is required.");
          return;
        }
        try {
          await apiJson(`/ui/api/conversations/${state.activeLeadId}/notes`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ note }),
          });
          document.getElementById("threadNoteInput").value = "";
          setText("threadNoteStatus", "Saved.");
          if (state.session?.role === "client") {
            await openThread(state.activeLeadId);
            await loadCrmLeads();
          } else {
            await Promise.all([openThread(state.activeLeadId), loadCrmLeads(), loadLogs(state.selectedClientKey)]);
          }
          showNotice("Note saved.", "ok");
        } catch (error) {
          setText("threadNoteStatus", `Note failed: ${error.message}`);
          showNotice(`Note failed: ${error.message}`, "err");
        }
      }

      async function sendManualMessage() {
        if (!state.activeLeadId) return;
        const body = document.getElementById("threadManualMessage").value.trim();
        if (!body) {
          setText("threadManualStatus", "Message body is required.");
          return;
        }
        const sandboxThread = isActiveSandboxThread();
        try {
          const endpoint = sandboxThread
            ? `/ui/api/conversations/${state.activeLeadId}/sandbox/messages`
            : `/ui/api/conversations/${state.activeLeadId}/messages/manual`;
          const result = await apiJson(endpoint, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ body }),
          });
          document.getElementById("threadManualMessage").value = "";
          if (sandboxThread) {
            state.sandboxLeadId = state.activeLeadId;
            saveLocalState();
          }
          setText("threadManualStatus", sandboxThread ? "AI replied in sandbox." : "Sent.");
          if (state.session?.role === "client") {
            await loadConversations();
            await loadCrmLeads();
            await openThread(state.activeLeadId);
          } else {
            await Promise.all([loadConversations(), openThread(state.activeLeadId), loadCrmLeads(), loadLogs(state.selectedClientKey), loadDashboard()]);
          }
          if (sandboxThread) {
            const output = document.getElementById("testLabOutput");
            if (output) output.textContent = JSON.stringify(result, null, 2);
          }
          showNotice(sandboxThread ? "Sandbox turn completed." : "Manual message sent.", "ok");
        } catch (error) {
          setText("threadManualStatus", `Send failed: ${error.message}`);
          showNotice(`${sandboxThread ? "Sandbox turn" : "Manual message"} failed: ${error.message}`, "err");
        }
      }

      async function markHandoff() {
        if (!state.activeLeadId) return;
        try {
          await apiJson(`/ui/api/conversations/${state.activeLeadId}/actions/handoff`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ note: document.getElementById("threadNoteInput").value.trim() || "Marked by operator." }),
          });
          if (state.session?.role === "client") {
            await loadConversations();
            await loadCrmLeads();
            await openThread(state.activeLeadId);
          } else {
            await Promise.all([loadConversations(), openThread(state.activeLeadId), loadCrmLeads(), loadLogs(state.selectedClientKey), loadDashboard()]);
          }
          showNotice("Conversation marked for handoff.", "ok");
        } catch (error) {
          showNotice(`Handoff failed: ${error.message}`, "err");
        }
      }

      async function deleteConversation() {
        if (!state.activeLeadId) return;
        const current = state.thread?.lead?.display_name || `lead ${state.activeLeadId}`;
        if (!window.confirm(`Delete ${current} and the full conversation history? This cannot be undone.`)) {
          return;
        }
        try {
          const deletedLeadId = state.activeLeadId;
          await apiJson(`/ui/api/conversations/${deletedLeadId}`, {
            method: "DELETE",
          });
          state.activeLeadId = null;
          state.thread = null;
          await loadConversations();
          await loadCrmLeads();
          if (state.session?.role !== "client") {
            await Promise.all([loadLogs(state.selectedClientKey), loadDashboard()]);
          }
          showNotice("Conversation deleted.", "ok");
        } catch (error) {
          showNotice(`Delete failed: ${error.message}`, "err");
        }
      }

      async function setConversationArchived(leadId, archived) {
        const numericLeadId = Number(leadId) || 0;
        if (!numericLeadId) return;
        const threadName = state.thread?.lead?.display_name;
        const crmName = state.crmLeadDetail?.lead?.display_name;
        const current = threadName || crmName || `lead ${numericLeadId}`;
        const confirmMessage = archived
          ? `Archive ${current}? It will be removed from the active inbox but kept in Pipeline.`
          : `Restore ${current} to the active inbox?`;
        if (!window.confirm(confirmMessage)) return;
        try {
          await apiJson(`/ui/api/conversations/${numericLeadId}/archive`, {
            method: "PATCH",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ archived }),
          });
          await Promise.all([
            loadConversations(),
            loadCrmLeads(),
            state.activeCrmLeadId === numericLeadId ? loadCrmLeadDetail(numericLeadId) : Promise.resolve(),
          ]);
          if ((state.activeLeadId === numericLeadId) && (!archived || state.conversationFilters.showArchived)) {
            await openThread(numericLeadId);
          }
          showNotice(archived ? "Lead archived from Inbox." : "Lead restored to Inbox.", "ok");
        } catch (error) {
          showNotice(`${archived ? "Archive" : "Restore"} failed: ${error.message}`, "err");
        }
      }

      async function seedDemo(reset = false) {
        try {
          const result = await apiJson(`/ui/api/seed-demo${reset ? "?reset=true" : ""}`, { method: "POST" });
          setText("settingsSeedStatus", JSON.stringify(result));
          await bootstrap();
          showNotice(reset ? "Demo data reseeded." : "Demo data seeded.", "ok");
        } catch (error) {
          setText("settingsSeedStatus", `Seed failed: ${error.message}`);
          showNotice(`Seed failed: ${error.message}`, "err");
        }
      }

      async function resetDemo() {
        if (!window.confirm("Delete demo clients and related demo conversations?")) return;
        try {
          const result = await apiJson("/ui/api/seed-demo", { method: "DELETE" });
          setText("settingsSeedStatus", JSON.stringify(result));
          await bootstrap();
          showNotice("Demo data reset.", "ok");
        } catch (error) {
          setText("settingsSeedStatus", `Reset failed: ${error.message}`);
          showNotice(`Reset failed: ${error.message}`, "err");
        }
      }

      async function seedClientShowcase(reset = false) {
        if (!state.selectedClientKey) {
          setText("settingsSeedStatus", "Select a client first.");
          showNotice("Select a client first.", "warn");
          return;
        }
        try {
          const result = await apiJson(`/ui/api/seed-showcase/${encodeURIComponent(state.selectedClientKey)}${reset ? "?reset=true" : ""}`, { method: "POST" });
          setText("settingsSeedStatus", JSON.stringify(result));
          await bootstrap();
          showNotice(reset ? "Selected client showcase reseeded." : "Selected client showcase seeded.", "ok");
        } catch (error) {
          setText("settingsSeedStatus", `Seed failed: ${error.message}`);
          showNotice(`Seed failed: ${error.message}`, "err");
        }
      }

      async function startTestLabSandbox() {
        const clientKey = document.getElementById("labClientSelect")?.value || state.selectedClientKey;
        if (!clientKey) {
          setText("labStartStatus", "Select a client first.");
          return;
        }
        if (state.testLabMode !== "gpt_only") {
          setText("labStartStatus", "Only GPT only is currently implemented.");
          showNotice("Only GPT only is wired for tomorrow's sandbox.", "info");
          return;
        }

        const formAnswers = readTestLabAnswers().filter((row) => row.question && row.answer);
        if (!formAnswers.length) {
          setText("labStartStatus", "Add at least one form question and answer.");
          return;
        }

        const payload = {
          mode: state.testLabMode,
          full_name: document.getElementById("labLeadName").value.trim() || "Strategy Call Lead",
          phone: document.getElementById("labLeadPhone").value.trim(),
          email: document.getElementById("labLeadEmail").value.trim(),
          city: document.getElementById("labLeadCity").value.trim(),
          form_answers: formAnswers,
        };

        const button = document.getElementById("labStartButton");
        button.disabled = true;
        setText("labStartStatus", "Creating the test lead and asking the agent for the first reply...");
        try {
          if (clientKey !== state.selectedClientKey) {
            state.selectedClientKey = clientKey;
            updateClientSelectors();
            saveLocalState();
          }
          const result = await apiJson(`/ui/api/owner/${encodeURIComponent(clientKey)}/sandbox/start`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
          });
          state.sandboxLeadId = result.lead_id;
          saveLocalState();
          setText("labStartStatus", `Sandbox started. Lead ${result.lead_id} is open in Conversations.`);
          if (state.session?.role === "client") {
            await Promise.all([loadOwnerWorkspace(clientKey), loadConversations(), loadCrmLeads(), loadCalendar(), loadCrmTasks()]);
          } else {
            await Promise.all([loadDashboard(), loadClientDetail(clientKey), loadOwnerWorkspace(clientKey), loadLogs(clientKey), loadConversations(), loadCrmLeads()]);
          }
          setActiveView("conversations");
          await openThread(result.lead_id);
          showNotice("GPT sandbox started. Reply in the thread composer as the lead.", "ok");
        } catch (error) {
          setText("labStartStatus", `Sandbox failed: ${error.message}`);
          showNotice(`Sandbox failed: ${error.message}`, "err");
        } finally {
          button.disabled = false;
        }
      }

      async function copyToClipboard(text) {
        try {
          await navigator.clipboard.writeText(text);
          showNotice("Copied to clipboard.", "ok");
        } catch (error) {
          showNotice(`Copy failed: ${error.message}`, "err");
        }
      }

      function scheduleSearchRefresh() {
        state.globalSearch = document.getElementById("globalSearch").value;
        saveLocalState();
        if (state.searchTimer) window.clearTimeout(state.searchTimer);
        state.searchTimer = window.setTimeout(async () => {
          if (state.activeView === "conversations") {
            await loadConversations();
          } else if (state.activeView === "crm" || state.activeView === "leads") {
            await loadCrmLeads();
            if (state.activeCrmLeadId && state.activeView === "leads") {
              await loadCrmLeadDetail(state.activeCrmLeadId);
            }
          } else if (state.activeView === "calendar") {
            await loadCalendar();
          } else if (state.activeView === "tasks") {
            await loadCrmTasks();
          } else {
            renderDashboard();
            renderClients();
            renderLogs();
          }
        }, 220);
      }

      function isTypingTarget(node) {
        if (!node) return false;
        const tag = String(node.tagName || "").toUpperCase();
        return node.isContentEditable || ["INPUT", "TEXTAREA", "SELECT"].includes(tag);
      }

      function handleGlobalKeydown(event) {
        if (!event) return;
        if (
          event.key === "/"
          && !event.metaKey
          && !event.ctrlKey
          && !event.altKey
          && !isTypingTarget(event.target)
        ) {
          event.preventDefault();
          const search = document.getElementById("globalSearch");
          search.focus();
          search.select();
          return;
        }
        if (
          event.key.toLowerCase() === "r"
          && event.shiftKey
          && !event.metaKey
          && !event.ctrlKey
          && !event.altKey
          && !isTypingTarget(event.target)
        ) {
          event.preventDefault();
          refreshCurrentView();
        }
      }

      async function refreshCurrentView() {
        try {
          if (state.session?.role === "client") {
            await Promise.all([loadConversations(), loadCrmLeads(), loadCalendar(), loadCrmTasks(), loadOwnerWorkspace(state.selectedClientKey)]);
            if (state.activeCrmLeadId) {
              await loadCrmLeadDetail(state.activeCrmLeadId);
            }
            renderSettings();
            updateWindowIndicators();
            return;
          }
          if (state.activeView === "dashboard") {
            await Promise.all([loadRuntime(), loadDashboard()]);
          } else if (state.activeView === "clients") {
            await Promise.all([loadClients(), loadClientDetail(state.selectedClientKey)]);
          } else if (state.activeView === "conversations") {
            await loadConversations();
          } else if (state.activeView === "crm") {
            await loadCrmLeads();
          } else if (state.activeView === "leads") {
            await loadCrmLeads();
            if (state.activeCrmLeadId) {
              await loadCrmLeadDetail(state.activeCrmLeadId);
            }
          } else if (state.activeView === "calendar") {
            await loadCalendar();
          } else if (state.activeView === "tasks") {
            await loadCrmTasks();
          } else if (state.activeView === "logs") {
            await loadLogs(state.selectedClientKey);
          } else if (state.activeView === "settings") {
            await Promise.all([loadRuntime(), loadClientDetail(state.selectedClientKey), loadOwnerWorkspace(state.selectedClientKey)]);
          } else if (state.activeView === "test-lab") {
            await loadOwnerWorkspace(state.selectedClientKey);
          }
          updateWindowIndicators();
        } catch (error) {
          showNotice(`Refresh failed: ${error.message}`, "err");
        }
      }

      function applyPaneSizes() {
        const shell = document.getElementById("conversationShell");
        if (!shell) return;
        if (isMobileViewport()) {
          shell.style.gridTemplateColumns = "minmax(0, 1fr)";
          return;
        }
        shell.style.gridTemplateColumns = `${state.split.conversations.left}px 5px minmax(360px, 1fr) 5px ${state.split.conversations.right}px`;
      }

      function startLivePolling() {
        if (state.pollTimer) return;
        state.pollTimer = window.setInterval(async () => {
          if ((!adminToken() && !portalToken()) || document.visibilityState !== "visible") return;
          if (state.activeView !== "conversations") return;
          try {
            await loadConversations();
            if (state.activeLeadId && (!isMobileViewport() || state.conversationMobilePanel === "thread")) {
              await openThread(state.activeLeadId);
            }
          } catch (_) {
            // Polling is best-effort. Avoid spamming the operator with transient refresh errors.
          }
        }, 4000);
      }

      function startResizer(edge, startX) {
        if (isMobileViewport()) return;
        const shell = document.getElementById("conversationShell");
        const bounds = shell.getBoundingClientRect();
        const startLeft = state.split.conversations.left;
        const startRight = state.split.conversations.right;
        function move(event) {
          const delta = event.clientX - startX;
          if (edge === "left") {
            state.split.conversations.left = Math.min(520, Math.max(240, startLeft + delta));
          } else {
            state.split.conversations.right = Math.min(420, Math.max(240, startRight - delta));
          }
          if (state.split.conversations.left + state.split.conversations.right > bounds.width - 420) {
            if (edge === "left") {
              state.split.conversations.left = bounds.width - 420 - state.split.conversations.right;
            } else {
              state.split.conversations.right = bounds.width - 420 - state.split.conversations.left;
            }
          }
          applyPaneSizes();
        }
        function stop() {
          window.removeEventListener("pointermove", move);
          window.removeEventListener("pointerup", stop);
          saveLocalState();
        }
        window.addEventListener("pointermove", move);
        window.addEventListener("pointerup", stop);
      }
