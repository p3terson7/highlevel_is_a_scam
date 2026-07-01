      function renderClients() {
        const q = state.globalSearch.trim().toLowerCase();
        const items = state.clients.filter((client) => !q || JSON.stringify(client).toLowerCase().includes(q));
        setText("clientsCount", `${items.length}`);
        document.getElementById("clientsList").innerHTML = items.length
          ? items.map((client) => `
              <div class="client-item ${client.client_key === state.selectedClientKey ? "active" : ""}" data-action="select-client" data-client-key="${client.client_key}">
                <div class="item-title-row">
                  <div>
                    <div class="item-title">${escapeHtml(client.business_name)}</div>
                    <div class="item-subtitle mono">${escapeHtml(client.client_key)}</div>
                  </div>
                  ${renderBadge(client.is_active ? "active" : "inactive", client.is_active ? "ok" : "warn")}
                </div>
                <div class="item-meta-row">
                  <div class="chip-row">
                    ${renderBadge(`${client.lead_count} records`)}
                    ${renderBadge(`${client.open_conversations} open`, client.open_conversations ? "info" : "")}
                  </div>
                  <div class="meta-text">${escapeHtml(formatDateTime(client.last_activity_at))}</div>
                </div>
              </div>
            `).join("")
          : renderEmptyState("No clients match the current search.", [
              { label: "New client", attrs: { "data-action": "begin-new-client" } },
            ], { compact: true });
        renderClientWorkspace();
      }

      function renderCrmBoard() {
        const data = state.crmLeads || { items: [], counts: {}, stages: [] };
        const includeClientName = !isClientRole();
        const stages = data.stages?.length
          ? data.stages
          : ["New Lead", "Contacted", "Qualified", "Meeting Booked", "Meeting Completed", "Won", "Lost"];
        document.getElementById("crmCountPills").innerHTML = stages
          .map((stage) => renderBadge(`${formatCrmStageDisplay(stage)} ${data.counts?.[stage] || 0}`, stage.includes("Won") || stage.includes("Booked") ? "ok" : stage === "Lost" ? "warn" : ""))
          .join("");
        document.getElementById("crmAddLeadPanel").classList.toggle("hidden", !state.crmAddLeadOpen);

        document.getElementById("crmBoard").innerHTML = stages.map((stage) => {
          const leads = (data.items || []).filter((item) => item.crm_stage === stage);
          const cards = leads.length
            ? leads.map((item) => {
                const visibleTags = uniqueStatusTags(item.tags || [], [item.crm_stage, item.conversation_state], 2);
                const scoreLabel = formatScoreLabel(item.lead_score);
                const valueLabel = formatCompactCurrency(item.estimated_value);
                const signalBadges = [
                  scoreLabel ? renderBadge(`Score ${scoreLabel}`, Number(item.lead_score) >= 80 ? "ok" : Number(item.lead_score) >= 55 ? "info" : "warn") : "",
                  valueLabel ? renderBadge(valueLabel, "info") : "",
                  renderMessageDeliveryStatus(item.last_message_delivery, { compact: true, onlyWarnings: true }),
                ].filter(Boolean).join("");
                const nextTask = item.next_task_title
                  ? `<div class="crm-card-next"><span>${escapeHtml(t("Next task"))}</span>${escapeHtml(item.next_task_title)}${item.next_task_due_date ? ` · ${escapeHtml(item.next_task_due_date)}` : ""}</div>`
                  : "";
                const campaign = item.campaign_name
                  ? `<div class="crm-card-campaign">${escapeHtml(item.campaign_name)}</div>`
                  : "";
                return `
                  <div class="crm-card" data-action="open-crm-lead" data-lead-id="${item.lead_id}" data-crm-stage="${escapeHtml(item.crm_stage)}" draggable="true">
                    <div class="item-title-row">
                      <div class="item-title">${escapeHtml(item.lead_name || item.phone || `Contact ${item.lead_id}`)}</div>
                      <div class="actions">
                        <button class="small ghost" type="button" data-action="open-contact-drawer" data-lead-id="${item.lead_id}" data-source="pipeline">Message</button>
                        ${maybeRenderConversationState(item.crm_stage, item.conversation_state)}
                      </div>
                    </div>
                    <div class="item-subtitle">${escapeHtml([item.phone || "-", formatLeadSourceLabel(item.source || "-"), includeClientName ? (item.client_name || "") : ""].filter(Boolean).join(" · "))}</div>
                    ${signalBadges ? `<div class="crm-card-metrics">${signalBadges}</div>` : ""}
                    ${campaign}
                    <div class="item-snippet">${renderLabeledSnippet(item, "No messages yet.", 120)}</div>
                    ${nextTask}
                    <div class="item-meta-row">
                      <div class="chip-row">${visibleTags.map((tag) => renderTag(tag)).join("")}</div>
                      <div class="meta-text">${escapeHtml(formatDateTime(item.last_activity_at))}</div>
                    </div>
                  </div>
                `;
              }).join("")
            : renderEmptyState("No records in this stage.", [
                { label: "Add contact", attrs: { "data-action": "crm-open-add-lead" } },
              ], { compact: true });
          return `
            <div class="crm-stage-column">
              <div class="crm-stage-header">
                <div class="item-title">${escapeHtml(formatCrmStageDisplay(stage))}</div>
                ${renderBadge(String(leads.length), leads.length ? "info" : "")}
              </div>
              <div class="crm-stage-list" data-stage="${escapeHtml(stage)}">${cards}</div>
            </div>
          `;
        }).join("");
      }

      function manualLeadOptions() {
        const selectedClientKey = state.session?.role === "client" ? state.session.client_key : state.selectedClientKey;
        const leads = (state.crmLeads?.items || []).filter((lead) => {
          if (!selectedClientKey) return true;
          return !lead.client_key || lead.client_key === selectedClientKey;
        });
        return leads.slice().sort((a, b) => String(a.lead_name || "").localeCompare(String(b.lead_name || "")));
      }

      function fillManualMeetingLeadSelect() {
        const select = document.getElementById("manualMeetingLeadSelect");
        if (!select) return;
        const leads = manualLeadOptions();
        select.innerHTML = leads.length
          ? leads.map((lead) => `<option value="${lead.lead_id}">${escapeHtml(lead.lead_name || lead.phone || `Contact ${lead.lead_id}`)}${lead.phone ? ` · ${escapeHtml(lead.phone)}` : ""}</option>`).join("")
          : '<option value="">No contacts available</option>';
      }

      function selectedDateTimeLocalValue() {
        const dateKey = state.calendarSelectedDate || dateKeyInTimeZone(new Date(), state.calendar?.timezone || undefined);
        const now = new Date();
        const minutes = now.getMinutes();
        const rounded = minutes <= 30 ? 30 : 60;
        let hour = now.getHours();
        let minute = rounded === 60 ? 0 : rounded;
        if (rounded === 60) hour += 1;
        if (dateKey !== dateKeyInTimeZone(now, state.calendar?.timezone || undefined)) {
          hour = 9;
          minute = 0;
        }
        return `${dateKey}T${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}`;
      }

      function syncManualMeetingFormDefaults() {
        const timezoneInput = document.getElementById("manualMeetingTimezone");
        const startInput = document.getElementById("manualMeetingStart");
        if (timezoneInput && !timezoneInput.value) {
          timezoneInput.value = state.calendar?.timezone || state.ownerWorkspace?.client?.timezone || "America/Toronto";
        }
        if (startInput && !startInput.value) {
          startInput.value = selectedDateTimeLocalValue();
        }
        fillManualMeetingLeadSelect();
        updateManualMeetingLeadMode();
        document.getElementById("calendarMeetingPanel")?.classList.toggle("hidden", !state.calendarMeetingPanelOpen);
        document.getElementById("calendarShowMeetingFormButton")?.classList.toggle("active", state.calendarMeetingPanelOpen);
        document.getElementById("calendarSelectedAddMeetingButton")?.classList.toggle("active", state.calendarMeetingPanelOpen);
        document.getElementById("calendarManualLeadPanel")?.classList.toggle("hidden", !state.calendarLeadPanelOpen);
        document.getElementById("calendarShowLeadFormButton")?.classList.toggle("active", state.calendarLeadPanelOpen);
      }

      function toggleCalendarMeetingPanel() {
        state.calendarMeetingPanelOpen = !state.calendarMeetingPanelOpen;
        if (state.calendarMeetingPanelOpen) {
          state.calendarLeadPanelOpen = false;
          const startInput = document.getElementById("manualMeetingStart");
          if (startInput) startInput.value = "";
          setText("manualMeetingStatus", "");
        }
        renderCalendarView();
      }

      function openCalendarLeadPanel() {
        state.calendarLeadPanelOpen = true;
        state.calendarMeetingPanelOpen = false;
        renderCalendarView();
      }

      function updateManualMeetingLeadMode() {
        const mode = document.getElementById("manualMeetingLeadMode")?.value || "existing";
        document.getElementById("manualMeetingExistingLeadWrap")?.classList.toggle("hidden", mode !== "existing");
        document.getElementById("manualMeetingNewLeadWrap")?.classList.toggle("hidden", mode !== "new");
      }

      function renderLeadsView() {
        const leads = state.crmLeads?.items || [];
        const archiveMode = Boolean(state.crmFilters.showArchived);
        const archiveToggle = document.getElementById("crmLeadArchiveToggleButton");
        archiveToggle.textContent = archiveMode ? "Active contacts" : "Archived";
        archiveToggle.classList.toggle("active", archiveMode);
        setText("crmLeadCount", archiveMode ? `${leads.length} archived` : `${leads.length}`);
        document.getElementById("crmLeadList").innerHTML = leads.length
          ? leads.map((item) => {
              const visibleTags = uniqueStatusTags(item.tags || [], [item.crm_stage, item.conversation_state], 2);
              return `
                <div class="lead-list-card ${item.lead_id === state.activeCrmLeadId ? "active" : ""}" data-action="open-crm-lead" data-lead-id="${item.lead_id}">
                  <div class="item-title-row">
                    <div class="item-title">${escapeHtml(item.lead_name || item.phone || `Contact ${item.lead_id}`)}</div>
                    <div class="actions">
                      <button class="small ghost" type="button" data-action="open-contact-drawer" data-lead-id="${item.lead_id}" data-source="records-list">Message</button>
                      ${renderBadge(item.crm_stage || "New Lead", crmStageTone(item.crm_stage))}
                    </div>
                  </div>
                  <div class="lead-list-status">
                    ${maybeRenderConversationState(item.crm_stage, item.conversation_state)}
                    ${visibleTags.map((tag) => renderTag(tag)).join("") || '<span class="lead-list-status-empty">No tags</span>'}
                  </div>
                  <div class="lead-list-meta">
                    <div class="meta-text">Last activity</div>
                    <div class="meta-text">${escapeHtml(formatDateTime(item.last_activity_at))}</div>
                  </div>
                </div>
              `;
            }).join("")
          : renderEmptyState(
              archiveMode ? "No archived contacts yet." : "No active contacts match the current filters.",
              archiveMode ? [] : [{ label: "Add contact", attrs: { "data-action": "crm-open-add-lead" } }],
              { compact: true }
            );

        if (!state.crmLeadDetail) {
          setText("crmLeadTitle", "Contact details");
          document.getElementById("crmLeadHeaderStageBadge").innerHTML = "";
          document.getElementById("crmLeadHeaderLinePrimary").innerHTML = "";
          setText("crmLeadHeaderLineSecondary", "");
          document.getElementById("crmLeadArchiveButton").textContent = "Archive";
          document.getElementById("crmLeadArchiveButton").disabled = true;
          const emptyCrmLeadMessageButton = document.getElementById("crmLeadMessageButton");
          if (emptyCrmLeadMessageButton) {
            emptyCrmLeadMessageButton.disabled = true;
            delete emptyCrmLeadMessageButton.dataset.leadId;
          }
          document.getElementById("crmLeadEmpty").classList.remove("hidden");
          document.getElementById("crmLeadDetail").classList.add("hidden");
          return;
        }

        const payload = state.crmLeadDetail;
        const archived = leadHasTag(payload.tags || payload.lead.tags || [], "archived");
        const leadName = payload.lead.display_name || payload.lead.phone || "Contact";
        const stage = payload.lead.crm_stage || "New Lead";
        const currentConversationState = payload.lead.conversation_state || payload.lead.current_state || "";
        setText("crmLeadTitle", leadName);
        document.getElementById("crmLeadHeaderStageBadge").innerHTML = [
          renderBadge(stage, crmStageTone(stage)),
          !isClientRole() ? maybeRenderConversationState(stage, currentConversationState) : "",
        ].filter(Boolean).join("");
        const phoneDisplay = formatPhoneNumber(payload.lead.phone);
        const emailDisplay = String(payload.lead.email || "").trim();
        const headerPrimaryParts = [];
        if (phoneDisplay && phoneDisplay !== "-" && payload.lead.phone) {
          headerPrimaryParts.push(renderCopyableHeaderValue(phoneDisplay, payload.lead.phone, "phone number"));
        }
        if (emailDisplay) {
          headerPrimaryParts.push(renderCopyableHeaderValue(emailDisplay, emailDisplay, "email"));
        }
        const headerPrimary = headerPrimaryParts.join('<span class="lead-detail-sep">·</span>');
        const headerSecondary = [
          formatLeadSourceLabel(payload.lead.source || "-"),
          formatLongDateTime(payload.lead.last_activity_at),
          !isClientRole() && payload.lead.owner ? payload.lead.owner : "",
          !isClientRole() && payload.client?.business_name ? payload.client.business_name : "",
        ].filter(Boolean).join(" · ");
        document.getElementById("crmLeadHeaderLinePrimary").innerHTML = headerPrimary || "No contact details captured yet.";
        setText("crmLeadHeaderLineSecondary", headerSecondary || "");
        document.getElementById("crmLeadArchiveButton").textContent = archived ? "Restore to inbox" : "Archive";
        document.getElementById("crmLeadArchiveButton").disabled = false;
        const crmLeadMessageButton = document.getElementById("crmLeadMessageButton");
        if (crmLeadMessageButton) {
          crmLeadMessageButton.disabled = false;
          crmLeadMessageButton.dataset.leadId = String(payload.lead.id);
          crmLeadMessageButton.dataset.source = "record";
        }
        document.getElementById("crmLeadEmpty").classList.add("hidden");
        document.getElementById("crmLeadDetail").classList.remove("hidden");
        document.getElementById("crmLeadStageSelect").value = stage;
        const crmLeadStagePills = document.getElementById("crmLeadStagePills");
        crmLeadStagePills.classList.toggle("hidden", isClientRole());
        crmLeadStagePills.innerHTML = isClientRole()
          ? ""
          : [
              renderBadge(stage, crmStageTone(stage)),
              maybeRenderConversationState(stage, currentConversationState),
            ].filter(Boolean).join("");
        setText(
          "crmLeadStageHint",
          isClientRole()
            ? "Update this only when the contact has clearly moved forward or is no longer active."
            : "Keep the pipeline stage aligned with the latest conversation."
        );
        const leadSummaryLines = mergeSummaryRows(payload.lead.summary_lines || [], payload.lead.form_answers || {}, leadName);
        setText("crmLeadNotesTitle", isClientRole() ? "Private notes" : "Internal notes");
        document.getElementById("crmLeadNoteInput").placeholder = isClientRole() ? "Add a private note for your team." : "Add an internal note.";
        const crmSummaryList = document.getElementById("crmLeadSummaryList");
        crmSummaryList.innerHTML = renderSummaryFacts(leadSummaryLines);
        crmSummaryList.classList.toggle("hidden", !leadSummaryLines.length);
        const agentInsights = payload.lead.agent_insights || {};
        document.getElementById("crmLeadAgentIntentPills").innerHTML = [
          agentInsights.intent_level ? renderBadge(agentInsights.intent_level, intentTone(agentInsights.intent_level)) : "",
          agentInsights.qualification_level ? renderBadge(formatFormKey(agentInsights.qualification_level), "") : "",
        ].filter(Boolean).join("");
        document.getElementById("crmLeadAgentInsights").innerHTML = renderAgentInsights(agentInsights);

        const visibleDetailTags = (payload.tags || []).filter((tag) => !isDeprecatedUiTag(tag));
        document.getElementById("crmLeadTags").innerHTML = visibleDetailTags.length
          ? visibleDetailTags.map((tag) => `<span class="tag ${tagTone(tag)}">${escapeHtml(formatTagLabel(tag))}${leadHasTag([tag], "archived") ? "" : ` <button type="button" class="tag-remove-btn" data-action="crm-remove-tag" data-tag="${escapeHtml(tag)}" aria-label="Remove tag">&times;</button>`}</span>`).join("")
          : '<span class="meta-text">No tags yet.</span>';

        document.getElementById("crmLeadNotes").innerHTML = payload.notes?.length
          ? payload.notes.map((note) => `
              <div class="note-item">
                <div class="item-title-row">
                  <div class="item-title">${escapeHtml(note.actor || "operator")}</div>
                  <div class="meta-text">${escapeHtml(formatDateTime(note.created_at))}</div>
                </div>
                <div class="item-snippet">${escapeHtml(note.body || "")}</div>
              </div>
            `).join("")
          : `<div class="empty-state">${isClientRole() ? "No private notes yet." : "No internal notes yet."}</div>`;

        document.getElementById("crmLeadTasks").innerHTML = payload.tasks?.length
          ? payload.tasks.map((task) => `
              <div class="crm-task-item ${task.status === "done" ? "done" : ""}">
                <div class="item-title-row">
                  <div class="item-title">${escapeHtml(task.title)}</div>
                  ${renderBadge(task.status, task.status === "done" ? "ok" : "warn")}
                </div>
                <div class="item-snippet">${escapeHtml(task.description || "-")}</div>
                <div class="item-meta-row">
                  <div class="meta-text">Due: ${escapeHtml(task.due_date || "-")}</div>
                  <div class="actions">
                    <button class="small ghost" data-action="crm-task-toggle" data-task-id="${task.id}" data-next-status="${task.status === "done" ? "open" : "done"}">${task.status === "done" ? "Reopen" : "Done"}</button>
                  </div>
                </div>
              </div>
            `).join("")
          : '<div class="empty-state">No tasks yet.</div>';

        const messages = payload.messages || [];
        document.getElementById("crmLeadMessages").innerHTML = messages.length
          ? messages.slice(-10).map((msg) => {
              const outbound = msg.direction === "OUTBOUND";
              const author = outbound ? (isClientRole() ? "You" : "Outbound") : (isClientRole() ? "Contact" : "Inbound");
              return `
                <div class="crm-message-row ${outbound ? "outbound" : "inbound"}">
                  <div class="crm-message-bubble">
                    <div class="crm-message-header">
                      <span class="crm-message-author">${escapeHtml(author)}</span>
                      <span class="crm-message-time">${escapeHtml(formatDateTime(msg.created_at))}</span>
                    </div>
                    ${msg.body ? `<div class="crm-message-body">${escapeHtml(msg.body || "")}</div>` : ""}
                    ${renderMessageAttachments(msg.attachments || [])}
                    ${outbound ? renderMessageDeliveryStatus(msg.delivery) : ""}
                  </div>
                </div>
              `;
            }).join("")
          : '<div class="empty-state">No messages yet.</div>';

        document.getElementById("crmLeadTimeline").innerHTML = payload.timeline?.length
          ? payload.timeline.slice().reverse().slice(0, 30).map((item) => `
              <div class="preview-item">
                <div class="item-title-row">
                  <div class="item-title mono">${escapeHtml(formatFormKey(item.type || "event"))}</div>
                  <div class="meta-text">${escapeHtml(formatDateTime(item.created_at))}</div>
                </div>
                <div class="item-snippet">${escapeHtml(item.label || item.body || item.reason || "")}</div>
              </div>
            `).join("")
          : '<div class="empty-state">No timeline entries.</div>';
      }

      function renderTasksView() {
        const payload = state.crmTasks || { items: [], counts: {} };
        const timeZone = state.calendar?.timezone || state.ownerWorkspace?.client?.timezone || undefined;
        const todayKey = dateKeyInTimeZone(new Date(), timeZone);
        const bucketForTask = (task) => {
          if (task.status === "done") return "Done";
          if (task.due_date && task.due_date < todayKey) return "Overdue";
          if (task.due_date === todayKey) return "Today";
          if (task.due_date) return "Upcoming";
          return "No due date";
        };
        const bucketTone = {
          Overdue: "err",
          Today: "warn",
          Upcoming: "info",
          "No due date": "",
          Done: "ok",
        };
        const bucketOrder = {
          Overdue: 0,
          Today: 1,
          Upcoming: 2,
          "No due date": 3,
          Done: 4,
        };
        const sortedTasks = (payload.items || []).slice().sort((a, b) => {
          const aBucket = bucketForTask(a);
          const bBucket = bucketForTask(b);
          if (bucketOrder[aBucket] !== bucketOrder[bBucket]) return bucketOrder[aBucket] - bucketOrder[bBucket];
          const aDue = a.due_date || "9999-12-31";
          const bDue = b.due_date || "9999-12-31";
          if (aDue !== bDue) return aDue.localeCompare(bDue);
          return String(a.title || "").localeCompare(String(b.title || ""));
        });
        let lastBucket = "";
        document.getElementById("tasksCountPills").innerHTML = [
          renderBadge(`open ${payload.counts?.open || 0}`, "warn"),
          renderBadge(`done ${payload.counts?.done || 0}`, "ok"),
        ].join("");
        document.getElementById("crmTasksTableBody").innerHTML = payload.items?.length
          ? sortedTasks.map((task) => {
              const bucket = bucketForTask(task);
              const heading = bucket !== lastBucket
                ? `<tr class="task-group-row"><td colspan="7">${renderBadge(bucket, bucketTone[bucket])}</td></tr>`
                : "";
              lastBucket = bucket;
              const dueLabel = task.due_date
                ? formatDateLabel(task.due_date, timeZone, { month: "short", day: "numeric", weekday: "short" })
                : "-";
              return `
                ${heading}
                <tr class="task-row ${bucket.toLowerCase().replaceAll(" ", "-")}">
                  <td data-label="Status">${renderBadge(task.status, task.status === "done" ? "ok" : "warn")}</td>
                  <td data-label="Task"><strong>${escapeHtml(task.title)}</strong>${task.description ? `<div class="meta-text">${escapeHtml(task.description)}</div>` : ""}</td>
                  <td data-label="Contact">${escapeHtml(task.lead_name || "-")}<div class="meta-text mono">${escapeHtml(task.lead_phone || "")}</div></td>
                  <td data-label="Client">${escapeHtml(task.client_name || "-")}</td>
                  <td data-label="Due" class="mono">${escapeHtml(dueLabel)}</td>
                  <td data-label="Stage">${escapeHtml(task.crm_stage || "-")}</td>
                  <td data-label="Actions">
                    <div class="actions">
                      <button class="small ghost" data-action="crm-task-toggle" data-task-id="${task.id}" data-next-status="${task.status === "done" ? "open" : "done"}">${task.status === "done" ? "Reopen" : "Done"}</button>
                      <button class="small ghost" data-action="open-contact-drawer" data-lead-id="${task.lead_id}" data-source="task">Message</button>
                      <button class="small ghost" data-action="open-crm-lead" data-lead-id="${task.lead_id}">Open</button>
                    </div>
                  </td>
                </tr>
              `;
            }).join("")
          : `<tr><td colspan="7">${renderEmptyState("No tasks match the current filters.", [], { compact: true })}</td></tr>`;
      }

      function ensureCalendarFocus(timeZone) {
        const todayKey = dateKeyInTimeZone(new Date(), timeZone || state.calendar?.timezone || undefined);
        let changed = false;
        if (!state.calendarSelectedDate) {
          state.calendarSelectedDate = todayKey;
          changed = true;
        }
        if (!state.calendarMonth) {
          state.calendarMonth = monthKeyForDateKey(state.calendarSelectedDate || todayKey);
          changed = true;
        }
        if (monthKeyForDateKey(state.calendarSelectedDate) !== state.calendarMonth) {
          state.calendarSelectedDate = `${state.calendarMonth}-01`;
          changed = true;
        }
        if (changed) saveLocalState();
      }

      function selectCalendarDate(dateKey) {
        if (!dateKey) return;
        state.calendarSelectedDate = dateKey;
        state.calendarMonth = monthKeyForDateKey(dateKey);
        const startInput = document.getElementById("manualMeetingStart");
        if (state.calendarMeetingPanelOpen && startInput) startInput.value = "";
        saveLocalState();
        renderCalendarView();
      }

      function shiftCalendarMonth(offset) {
        const nextMonth = shiftMonthKey(state.calendarMonth || monthKeyForDateKey(state.calendarSelectedDate), offset);
        state.calendarMonth = nextMonth;
        if (monthKeyForDateKey(state.calendarSelectedDate) !== nextMonth) {
          state.calendarSelectedDate = `${nextMonth}-01`;
        }
        saveLocalState();
        renderCalendarView();
      }

      function jumpCalendarToToday() {
        const todayKey = dateKeyInTimeZone(new Date(), state.calendar?.timezone || undefined);
        state.calendarSelectedDate = todayKey;
        state.calendarMonth = monthKeyForDateKey(todayKey);
        saveLocalState();
        renderCalendarView();
      }

      function buildCalendarMonthCells(monthKey) {
        const parts = parseMonthKey(monthKey);
        if (!parts) return [];
        const first = new Date(Date.UTC(parts.year, parts.month - 1, 1, 12));
        const startOffset = first.getUTCDay();
        const start = new Date(Date.UTC(parts.year, parts.month - 1, 1 - startOffset, 12));
        return Array.from({ length: 42 }, (_, index) => {
          const cellDate = new Date(start.getTime());
          cellDate.setUTCDate(start.getUTCDate() + index);
          const dateKey = `${cellDate.getUTCFullYear()}-${String(cellDate.getUTCMonth() + 1).padStart(2, "0")}-${String(cellDate.getUTCDate()).padStart(2, "0")}`;
          return {
            dateKey,
            dayNumber: cellDate.getUTCDate(),
            inMonth: cellDate.getUTCMonth() === parts.month - 1,
          };
        });
      }

      function agendaMeetingItem(item, timeZone) {
        const statusTone = item.status === "scheduled" ? "info" : item.status === "completed" ? "ok" : item.status === "cancelled" ? "warn" : "err";
        const statusLabel = String(item.status || "scheduled").replaceAll("_", " ");
        return `
          <div class="calendar-agenda-item">
            <div class="item-title-row">
              <div class="item-title">${escapeHtml(item.title || item.lead_name || "Booked meeting")}</div>
              ${renderBadge(statusLabel, statusTone)}
            </div>
            <div class="item-snippet">${escapeHtml(item.lead_name || "No contact")} · ${escapeHtml(`${formatTimeInTimeZone(item.start_at, timeZone)} to ${formatTimeInTimeZone(item.end_at, timeZone)}`)}</div>
            ${item.notes ? `<div class="item-snippet">${escapeHtml(item.notes)}</div>` : ""}
            <div class="item-meta-row">
              <div class="meta-text">${escapeHtml([item.phone || "", item.email || ""].filter(Boolean).join(" · ") || "No contact details")}</div>
              <div class="actions calendar-item-actions">
                ${item.lead_id ? `<button class="small ghost" data-action="open-contact-drawer" data-lead-id="${item.lead_id}" data-source="calendar">Message</button>` : ""}
                ${item.lead_id ? `<button class="small ghost" data-action="open-crm-lead" data-lead-id="${item.lead_id}">Open</button>` : ""}
                <details class="action-menu">
                  <summary class="small ghost">Actions</summary>
                  <div class="action-menu-panel">
                    <button class="small ghost" data-action="calendar-meeting-status" data-meeting-id="${item.id}" data-status="completed">Completed</button>
                    <button class="small ghost" data-action="calendar-meeting-status" data-meeting-id="${item.id}" data-status="no_show">No Show</button>
                    <button class="small ghost" data-action="calendar-meeting-status" data-meeting-id="${item.id}" data-status="cancelled">Cancel</button>
                    <button class="small warn" data-action="calendar-meeting-delete" data-meeting-id="${item.id}">Delete</button>
                  </div>
                </details>
              </div>
            </div>
          </div>
        `;
      }

      function agendaTaskItem(task, timeZone, emphasis = "") {
        const dueText = task.due_date
          ? formatDateLabel(task.due_date, timeZone, { month: "short", day: "numeric", weekday: "short" })
          : "No due date";
        return `
          <div class="calendar-agenda-item">
            <div class="item-title-row">
              <div class="item-title">${escapeHtml(task.title || "Task")}</div>
              ${renderBadge(task.status, task.status === "done" ? "ok" : (emphasis || "warn"))}
            </div>
            <div class="item-snippet">${escapeHtml(task.description || "No extra details.")}</div>
            <div class="item-meta-row">
              <div class="meta-text">${escapeHtml(`${task.lead_name || "Contact"} · ${dueText}`)}</div>
              <div class="actions">
                <button class="small ghost" data-action="crm-task-toggle" data-task-id="${task.id}" data-next-status="${task.status === "done" ? "open" : "done"}">${task.status === "done" ? "Reopen" : "Done"}</button>
                <button class="small ghost" data-action="open-contact-drawer" data-lead-id="${task.lead_id}" data-source="task">Message</button>
                <button class="small ghost" data-action="open-crm-lead" data-lead-id="${task.lead_id}">Open</button>
              </div>
            </div>
          </div>
        `;
      }

      function renderCalendarView() {
        const payload = state.calendar || { items: [], total: 0 };
        const timeZone = payload.timezone || "UTC";
        const todayKey = dateKeyInTimeZone(new Date(), timeZone);
        const openTasks = (state.crmTasks?.items || [])
          .filter((task) => task.status === "open")
          .slice()
          .sort((a, b) => {
            const aDue = a.due_date || "9999-12-31";
            const bDue = b.due_date || "9999-12-31";
            if (aDue !== bDue) return aDue.localeCompare(bDue);
            return String(a.title || "").localeCompare(String(b.title || ""));
          });
        ensureCalendarFocus(timeZone);
        const monthKey = state.calendarMonth;
        const selectedDateKey = state.calendarSelectedDate || todayKey;
        const itemsByDate = new Map();
        (payload.items || []).forEach((item) => {
          const key = dateKeyInTimeZone(item.start_at, timeZone);
          if (!itemsByDate.has(key)) itemsByDate.set(key, []);
          itemsByDate.get(key).push(item);
        });
        itemsByDate.forEach((entries) => {
          entries.sort((a, b) => String(a.start_at).localeCompare(String(b.start_at)));
        });
        const monthItems = (payload.items || []).filter((item) => dateKeyInTimeZone(item.start_at, timeZone).startsWith(monthKey));
        const dueTodayCount = openTasks.filter((task) => task.due_date === todayKey).length;
        const overdueCount = openTasks.filter((task) => task.due_date && task.due_date < todayKey).length;
        const selectedMeetings = itemsByDate.get(selectedDateKey) || [];
        const selectedTasks = openTasks.filter((task) => task.due_date === selectedDateKey);
        const monthCells = buildCalendarMonthCells(monthKey);
        const mode = payload.booking_mode || "link";
        const modeBadge = mode === "internal"
          ? renderBadge("internal calendar", "ok")
          : (mode === "calendly" ? renderBadge("calendly", "warn") : renderBadge("link only", "warn"));

        document.getElementById("calendarSummary").innerHTML = [
          modeBadge,
          renderBadge(`${monthItems.length} this month`, monthItems.length ? "info" : ""),
          renderBadge(`${openTasks.length} open tasks`, openTasks.length ? "warn" : "ok"),
          renderBadge(timeZone, ""),
        ].join("");
        setText("calendarMonthLabel", formatMonthLabel(monthKey, timeZone));
        setText(
          "calendarMonthMeta",
          `${monthItems.length} meeting${monthItems.length === 1 ? "" : "s"} this month · ${timeZone}`
        );
        setText("calendarSelectedTitle", selectedDateKey === todayKey ? "Today" : formatDateLabel(selectedDateKey, timeZone, { month: "long", day: "numeric", weekday: "long" }));
        setText(
          "calendarSelectedSubtitle",
          selectedDateKey === todayKey
            ? "What is booked today, plus the tasks that are due."
            : "Meetings and tasks due on the selected date."
        );
        setText(
          "calendarOpenTasksTitle",
          openTasks.length ? `Tasks to do (${openTasks.length})` : "Tasks to do"
        );
        setText(
          "calendarLandingHint",
          isClientRole()
            ? "This is your default planning view. Click any day to inspect booked meetings, then work through the open task list without leaving the page."
            : "Click a day to inspect bookings, then use the task list to follow up quickly."
        );
        syncManualMeetingFormDefaults();
        document.getElementById("calendarOverviewGrid").innerHTML = [
          ["Meetings today", (itemsByDate.get(todayKey) || []).length],
          ["Tasks due today", dueTodayCount],
          ["Overdue tasks", overdueCount],
          ["Open tasks", openTasks.length],
        ].map(([label, value]) => `
          <div class="calendar-overview-stat">
            <div class="label">${escapeHtml(label)}</div>
            <div class="value">${escapeHtml(value)}</div>
          </div>
        `).join("");
        document.getElementById("calendarWeekdayRow").innerHTML = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
          .map((day) => `<div class="calendar-weekday">${day}</div>`)
          .join("");
        document.getElementById("calendarMonthGrid").innerHTML = monthCells.map((cell) => {
          const entries = itemsByDate.get(cell.dateKey) || [];
          const preview = entries.slice(0, 2).map((item) => `
            <div class="calendar-event">
              <div class="calendar-event-time">${escapeHtml(formatTimeInTimeZone(item.start_at, timeZone))}</div>
              <div class="calendar-event-title">${escapeHtml(item.lead_name || "Meeting")}</div>
            </div>
          `).join("");
          const extras = entries.length > 2 ? `<div class="calendar-event-more">+${entries.length - 2} more</div>` : "";
          return `
            <button class="calendar-day ${cell.inMonth ? "" : "other-month"} ${cell.dateKey === todayKey ? "today" : ""} ${cell.dateKey === selectedDateKey ? "selected" : ""}" data-action="calendar-select-day" data-date-key="${cell.dateKey}">
              <div class="calendar-day-head">
                <div class="calendar-day-number">${cell.dayNumber}</div>
                <div class="calendar-day-summary">${entries.length ? `${entries.length} booked` : ""}</div>
              </div>
              <div class="calendar-day-events">
                ${preview || '<div class="calendar-event-more">Nothing booked</div>'}
                ${extras}
              </div>
            </button>
          `;
        }).join("");
        document.getElementById("calendarSelectedMeetings").innerHTML = selectedMeetings.length
          ? selectedMeetings.map((item) => agendaMeetingItem(item, timeZone)).join("")
          : renderEmptyState("No meetings booked for this day.", [
              { label: "Add meeting", attrs: { "data-action": "calendar-add-meeting" } },
            ], { compact: true });
        document.getElementById("calendarSelectedTasks").innerHTML = selectedTasks.length
          ? selectedTasks.map((task) => agendaTaskItem(task, timeZone, task.due_date && task.due_date < todayKey ? "err" : "warn")).join("")
          : renderEmptyState("No tasks are due on this day.", [], { compact: true });
        document.getElementById("calendarOpenTasks").innerHTML = openTasks.length
          ? openTasks.slice(0, 8).map((task) => {
              const tone = task.due_date && task.due_date < todayKey
                ? "err"
                : (task.due_date === todayKey ? "warn" : "info");
              return agendaTaskItem(task, timeZone, tone);
            }).join("")
          : renderEmptyState("No open tasks right now.", [], { compact: true });
      }

      function readCalendarConfigFromForm(prefix) {
        const availability = [];
        for (let day = 0; day < 7; day += 1) {
          const enabled = document.getElementById(`${prefix}CalDay${day}Enabled`).checked;
          const start = document.getElementById(`${prefix}CalDay${day}Start`).value || "09:00";
          const end = document.getElementById(`${prefix}CalDay${day}End`).value || "17:00";
          availability.push({ day, start, end, enabled });
        }
        return {
          slot_minutes: Number(document.getElementById(`${prefix}CalendarSlotMinutes`).value || 30),
          notice_minutes: Number(document.getElementById(`${prefix}CalendarNoticeMinutes`).value || 120),
          horizon_days: Number(document.getElementById(`${prefix}CalendarHorizonDays`).value || 14),
          availability,
        };
      }

      function applyCalendarConfigToForm(prefix, rawConfig) {
        const config = rawConfig && typeof rawConfig === "object" ? rawConfig : {};
        const internal = config.internal_calendar && typeof config.internal_calendar === "object" ? config.internal_calendar : config;
        document.getElementById(`${prefix}CalendarSlotMinutes`).value = Number(internal.slot_minutes || 30);
        document.getElementById(`${prefix}CalendarNoticeMinutes`).value = Number(internal.notice_minutes || 120);
        document.getElementById(`${prefix}CalendarHorizonDays`).value = Number(internal.horizon_days || 14);
        const availability = Array.isArray(internal.availability) ? internal.availability : [];
        for (let day = 0; day < 7; day += 1) {
          const existing = availability.find((row) => Number(row?.day) === day);
          const weekdayEnabled = day <= 4;
          document.getElementById(`${prefix}CalDay${day}Enabled`).checked = existing?.enabled != null ? Boolean(existing.enabled) : weekdayEnabled;
          document.getElementById(`${prefix}CalDay${day}Start`).value = existing?.start || (day <= 4 ? "09:00" : "10:00");
          document.getElementById(`${prefix}CalDay${day}End`).value = existing?.end || (day <= 4 ? "17:00" : "14:00");
        }
      }

      function readInternalCalendarFromForm() {
        return readCalendarConfigFromForm("client");
      }

      function applyInternalCalendarToForm(rawConfig) {
        applyCalendarConfigToForm("client", rawConfig);
      }

      function readSettingsCalendarFromForm() {
        return readCalendarConfigFromForm("settings");
      }

      function applySettingsCalendarToForm(rawConfig) {
        applyCalendarConfigToForm("settings", rawConfig);
      }

      function clientFormPayload(includeClientKey = false) {
        const templateRaw = document.getElementById("clientTemplateOverrides").value.trim() || "{}";
        let templateOverrides = {};
        try {
          templateOverrides = JSON.parse(templateRaw);
        } catch (error) {
          throw new Error("Template overrides must be valid JSON.");
        }
        const providerConfigRaw = {
          language: document.getElementById("clientLanguage").value || "en",
          twilio_account_sid: document.getElementById("clientProviderTwilioSid").value.trim(),
          twilio_auth_token: document.getElementById("clientProviderTwilioToken").value.trim(),
          twilio_from_number: document.getElementById("clientProviderTwilioFrom").value.trim(),
          meta_verify_token: document.getElementById("clientProviderMetaVerify").value.trim(),
          meta_access_token: document.getElementById("clientProviderMetaAccess").value.trim(),
          meta_graph_api_version: document.getElementById("clientProviderMetaVersion").value.trim(),
          linkedin_verify_token: document.getElementById("clientProviderLinkedinVerify").value.trim(),
          zapier_webhook_secret: document.getElementById("clientProviderZapierSecret").value.trim(),
          zapier_booking_webhook_url: document.getElementById("clientProviderZapierBookingWebhookUrl").value.trim(),
          public_base_url: document.getElementById("clientProviderPublicBaseUrl").value.trim(),
        };
        const providerConfig = {};
        Object.entries(providerConfigRaw).forEach(([key, value]) => {
          if (value) providerConfig[key] = value;
        });
        if (providerConfig.meta_access_token && !providerConfig.meta_graph_api_version) {
          providerConfig.meta_graph_api_version = "v22.0";
        }
        const existingBookingConfig = (
          !state.creatingClient
          && state.clientDetail?.client
          && state.clientDetail.client.booking_config
          && typeof state.clientDetail.client.booking_config === "object"
        )
          ? state.clientDetail.client.booking_config
          : {};
        const payload = {
          business_name: document.getElementById("clientBusinessName").value.trim(),
          tone: document.getElementById("clientTone").value.trim() || "friendly",
          timezone: document.getElementById("clientTimezone").value.trim() || "America/New_York",
          booking_url: document.getElementById("clientBookingUrl").value.trim(),
          booking_mode: document.getElementById("clientBookingMode").value,
          booking_config: {
            ...existingBookingConfig,
            internal_calendar: readInternalCalendarFromForm(),
          },
          fallback_handoff_number: document.getElementById("clientHandoffNumber").value.trim(),
          consent_text: document.getElementById("clientConsentText").value.trim(),
          portal_display_name: document.getElementById("clientPortalDisplayName").value.trim(),
          portal_email: document.getElementById("clientPortalEmail").value.trim(),
          portal_enabled: document.getElementById("clientPortalEnabled").value === "true",
          provider_config: providerConfig,
          qualification_questions: document.getElementById("clientQuestions").value.split("\n").map((item) => item.trim()).filter(Boolean),
          faq_context: document.getElementById("clientFaqContext").value.trim(),
          ai_context: document.getElementById("clientAiContext").value.trim(),
          template_overrides: templateOverrides,
          is_active: document.getElementById("clientIsActive").value === "true",
        };
        const portalPassword = document.getElementById("clientPortalPassword").value;
        if (portalPassword.trim()) payload.portal_password = portalPassword;
        if (!payload.business_name) throw new Error("Business name is required.");
        if (includeClientKey) {
          const clientKey = document.getElementById("clientKeyInput").value.trim();
          if (clientKey) payload.client_key = clientKey;
        }
        return payload;
      }

      function resetClientProviderFields() {
        document.getElementById("clientProviderTwilioSid").value = "";
        document.getElementById("clientProviderTwilioToken").value = "";
        document.getElementById("clientProviderTwilioFrom").value = "";
        document.getElementById("clientProviderMetaVerify").value = "";
        document.getElementById("clientProviderMetaAccess").value = "";
        document.getElementById("clientProviderMetaVersion").value = "v22.0";
        document.getElementById("clientProviderLinkedinVerify").value = "";
        document.getElementById("clientProviderZapierSecret").value = "";
        document.getElementById("clientProviderZapierBookingWebhookUrl").value = "";
        document.getElementById("clientProviderPublicBaseUrl").value = "";
        document.getElementById("clientLanguage").value = "en";
      }

      function resetClientForm() {
        state.creatingClient = false;
        if (state.clientDetail?.client) {
          populateClientFormFromDetail(state.clientDetail);
        } else {
          document.getElementById("clientBusinessName").value = "";
          document.getElementById("clientKeyInput").value = "";
          document.getElementById("clientKeyInput").readOnly = false;
          document.getElementById("clientTone").value = "friendly";
          document.getElementById("clientTimezone").value = "America/New_York";
          document.getElementById("clientLanguage").value = "en";
          document.getElementById("clientIsActive").value = "true";
          document.getElementById("clientBookingUrl").value = "";
          document.getElementById("clientBookingMode").value = "internal";
          applyInternalCalendarToForm({});
          resetClientProviderFields();
          document.getElementById("clientHandoffNumber").value = "";
          document.getElementById("clientConsentText").value = "Reply STOP to opt out. Msg/data rates may apply.";
          document.getElementById("clientPortalDisplayName").value = "";
          document.getElementById("clientPortalEmail").value = "";
          document.getElementById("clientPortalEnabled").value = "false";
          document.getElementById("clientPortalPassword").value = "";
          document.getElementById("clientQuestions").value = "";
          document.getElementById("clientFaqContext").value = "";
          document.getElementById("clientAiContext").value = "";
          document.getElementById("clientTemplateOverrides").value = "{}";
        }
        setText("clientSaveStatus", "");
        setText("clientPortalStatus", "");
        renderClientWorkspace();
      }

      function beginNewClient() {
        state.creatingClient = true;
        state.clientTab = "edit";
        saveLocalState();
        document.getElementById("clientKeyInput").readOnly = false;
        document.getElementById("clientBusinessName").value = "";
        document.getElementById("clientKeyInput").value = "";
        document.getElementById("clientTone").value = "friendly";
        document.getElementById("clientTimezone").value = "America/New_York";
        document.getElementById("clientLanguage").value = "en";
        document.getElementById("clientIsActive").value = "true";
        document.getElementById("clientBookingUrl").value = "";
        document.getElementById("clientBookingMode").value = "internal";
        applyInternalCalendarToForm({});
        resetClientProviderFields();
        document.getElementById("clientHandoffNumber").value = "";
        document.getElementById("clientConsentText").value = "Reply STOP to opt out. Msg/data rates may apply.";
        document.getElementById("clientPortalDisplayName").value = "";
        document.getElementById("clientPortalEmail").value = "";
        document.getElementById("clientPortalEnabled").value = "false";
        document.getElementById("clientPortalPassword").value = "";
        document.getElementById("clientQuestions").value = "What are you hoping to solve?\nWhen do you want to get started?";
        document.getElementById("clientFaqContext").value = "";
        document.getElementById("clientAiContext").value = "";
        document.getElementById("clientTemplateOverrides").value = "{}";
        renderClientWorkspace();
      }

      function populateClientFormFromDetail(data) {
        document.getElementById("clientBusinessName").value = data.client.business_name || "";
        document.getElementById("clientKeyInput").value = data.client.client_key || "";
        document.getElementById("clientKeyInput").readOnly = true;
        document.getElementById("clientTone").value = data.client.tone || "friendly";
        document.getElementById("clientTimezone").value = data.client.timezone || "America/New_York";
        document.getElementById("clientIsActive").value = data.client.is_active ? "true" : "false";
        document.getElementById("clientBookingUrl").value = data.client.booking_url || "";
        const mode = data.client.booking_mode === "calendar" ? "internal" : (data.client.booking_mode || "link");
        document.getElementById("clientBookingMode").value = mode;
        applyInternalCalendarToForm(data.client.booking_config || {});
        const provider = data.client.provider_config || {};
        document.getElementById("clientLanguage").value = provider.language || "en";
        document.getElementById("clientProviderTwilioSid").value = provider.twilio_account_sid || "";
        document.getElementById("clientProviderTwilioToken").value = provider.twilio_auth_token || "";
        document.getElementById("clientProviderTwilioFrom").value = provider.twilio_from_number || "";
        document.getElementById("clientProviderMetaVerify").value = provider.meta_verify_token || "";
        document.getElementById("clientProviderMetaAccess").value = provider.meta_access_token || "";
        document.getElementById("clientProviderMetaVersion").value = provider.meta_graph_api_version || "v22.0";
        document.getElementById("clientProviderLinkedinVerify").value = provider.linkedin_verify_token || "";
        document.getElementById("clientProviderZapierSecret").value = provider.zapier_webhook_secret || "";
        document.getElementById("clientProviderZapierBookingWebhookUrl").value = provider.zapier_booking_webhook_url || "";
        document.getElementById("clientProviderPublicBaseUrl").value = provider.public_base_url || "";
        document.getElementById("clientHandoffNumber").value = data.client.fallback_handoff_number || "";
        document.getElementById("clientConsentText").value = data.client.consent_text || "";
        document.getElementById("clientPortalDisplayName").value = data.client.portal_display_name || "";
        document.getElementById("clientPortalEmail").value = data.client.portal_email || "";
        document.getElementById("clientPortalEnabled").value = data.client.portal_enabled ? "true" : "false";
        document.getElementById("clientPortalPassword").value = "";
        document.getElementById("clientQuestions").value = (data.client.qualification_questions || []).join("\n");
        document.getElementById("clientFaqContext").value = data.client.faq_context || "";
        document.getElementById("clientAiContext").value = data.client.ai_context || "";
        document.getElementById("clientTemplateOverrides").value = JSON.stringify(data.client.template_overrides || {}, null, 2);
        setText(
          "clientPortalStatus",
          data.client.portal_password_configured
            ? "Password is configured. Set a new value to rotate it."
            : "No client portal password configured yet."
        );
      }

      const clientWizardSteps = ["business", "channels", "agent", "booking", "portal", "review"];
      const clientWizardCopy = {
        business: {
          title: "Business setup",
          hint: "Name the workspace, choose language, tone, timezone, and activation state.",
        },
        channels: {
          title: "Channels and handoff",
          hint: "Configure client-scoped Twilio, Meta, LinkedIn, Zapier, public URL, consent, and handoff details.",
        },
        agent: {
          title: "Agent guidance",
          hint: "Shape qualification questions, business context, FAQ, AI playbook, and optional templates.",
        },
        booking: {
          title: "Booking rules",
          hint: "Choose the booking mode and define the internal calendar availability the agent can use.",
        },
        portal: {
          title: "Client portal",
          hint: "Create the client login so the business can manage inbox, pipeline, calendar, and settings.",
        },
        review: {
          title: "Review setup",
          hint: "Confirm the workspace is presentation-ready before saving the client.",
        },
      };

      function validClientWizardStep(step) {
        return clientWizardSteps.includes(step) ? step : "business";
      }

      function setClientWizardStep(step) {
        state.clientWizardStep = validClientWizardStep(step);
        saveLocalState();
        applyClientWizard();
      }

      function moveClientWizard(delta) {
        const current = clientWizardSteps.indexOf(validClientWizardStep(state.clientWizardStep));
        const next = Math.max(0, Math.min(clientWizardSteps.length - 1, current + delta));
        setClientWizardStep(clientWizardSteps[next]);
      }

      function clientWizardFieldValue(id, fallback = "-") {
        const el = document.getElementById(id);
        if (!el) return fallback;
        return String(el.value || "").trim() || fallback;
      }

      function clientWizardStatusLabel(value) {
        return value && value !== "-" ? t("configured") : t("missing");
      }

      function renderClientWizardReview() {
        const enabledDays = Array.from({ length: 7 }).filter((_, index) => document.getElementById(`clientCalDay${index}Enabled`)?.checked).length;
        const aiContext = clientWizardFieldValue("clientAiContext", "");
        const questions = clientWizardFieldValue("clientQuestions", "").split("\n").map((item) => item.trim()).filter(Boolean);
        const cards = [
          {
            title: "Business",
            rows: [
              ["Name", clientWizardFieldValue("clientBusinessName")],
              ["Key", clientWizardFieldValue("clientKeyInput")],
              ["Language", clientWizardFieldValue("clientLanguage")],
              ["Timezone", clientWizardFieldValue("clientTimezone")],
              ["Active", clientWizardFieldValue("clientIsActive")],
            ],
          },
          {
            title: "Channels",
            rows: [
              ["Twilio", clientWizardStatusLabel(clientWizardFieldValue("clientProviderTwilioSid", ""))],
              ["Meta", clientWizardStatusLabel(clientWizardFieldValue("clientProviderMetaAccess", ""))],
              ["LinkedIn", clientWizardStatusLabel(clientWizardFieldValue("clientProviderLinkedinVerify", ""))],
              ["Zapier", clientWizardStatusLabel(clientWizardFieldValue("clientProviderZapierSecret", ""))],
              ["Zapier booking", clientWizardStatusLabel(clientWizardFieldValue("clientProviderZapierBookingWebhookUrl", ""))],
              ["Handoff", clientWizardStatusLabel(clientWizardFieldValue("clientHandoffNumber", ""))],
            ],
          },
          {
            title: "Agent",
            rows: [
              ["Tone", clientWizardFieldValue("clientTone")],
              ["Questions", `${questions.length}`],
              ["Playbook", aiContext ? `${aiContext.length} chars` : "missing"],
              ["FAQ", clientWizardStatusLabel(clientWizardFieldValue("clientFaqContext", ""))],
            ],
          },
          {
            title: "Booking",
            rows: [
              ["Mode", clientWizardFieldValue("clientBookingMode")],
              ["Booking URL", clientWizardStatusLabel(clientWizardFieldValue("clientBookingUrl", ""))],
              ["Meeting length", `${clientWizardFieldValue("clientCalendarSlotMinutes", "30")} min`],
              ["Available days", `${enabledDays}`],
            ],
          },
          {
            title: "Portal",
            rows: [
              ["Enabled", clientWizardFieldValue("clientPortalEnabled")],
              ["Email", clientWizardFieldValue("clientPortalEmail")],
              ["Display name", clientWizardFieldValue("clientPortalDisplayName")],
              ["Password", clientWizardStatusLabel(clientWizardFieldValue("clientPortalPassword", "") || (state.clientDetail?.client?.portal_password_configured ? "saved" : ""))],
            ],
          },
        ];
        return `
          <div class="client-wizard-review-grid">
            ${cards.map((card) => `
              <div class="client-wizard-review-card">
                <div class="client-wizard-review-title">${escapeHtml(t(card.title))}</div>
                ${card.rows.map(([label, value]) => `
                  <div class="client-wizard-review-row">
                    <span>${escapeHtml(t(label))}</span>
                    <strong>${escapeHtml(value)}</strong>
                  </div>
                `).join("")}
              </div>
            `).join("")}
          </div>
        `;
      }

      function applyClientWizard() {
        const wizard = document.getElementById("clientWizard");
        if (!wizard) return;
        const step = validClientWizardStep(state.clientWizardStep);
        const isReview = step === "review";
        const copy = clientWizardCopy[step] || clientWizardCopy.business;
        setText("clientWizardTitle", copy.title);
        setText("clientWizardHint", copy.hint);
        document.querySelectorAll("[data-action='set-client-wizard-step']").forEach((btn) => {
          const active = btn.dataset.step === step;
          btn.classList.toggle("active", active);
          btn.setAttribute("aria-current", active ? "step" : "false");
        });
        document.querySelectorAll("[data-client-wizard-section]").forEach((section) => {
          section.classList.toggle("hidden", isReview || section.dataset.clientWizardSection !== step);
        });
        const review = document.getElementById("clientWizardReview");
        if (review) {
          review.classList.toggle("hidden", !isReview);
          if (isReview) review.innerHTML = renderClientWizardReview();
        }
        const currentIndex = clientWizardSteps.indexOf(step);
        const prev = document.querySelector("[data-action='client-wizard-prev']");
        const next = document.querySelector("[data-action='client-wizard-next']");
        if (prev) prev.disabled = currentIndex <= 0;
        if (next) {
          next.disabled = currentIndex >= clientWizardSteps.length - 1;
          next.textContent = currentIndex >= clientWizardSteps.length - 2 ? t("Review") : t("Next step");
        }
      }

      function renderClientWorkspace() {
        const data = state.clientDetail;
        document.querySelectorAll("[data-action='set-client-tab']").forEach((btn) => btn.classList.toggle("active", btn.dataset.tab === state.clientTab));
        ["overview", "edit", "webhooks"].forEach((tab) => {
          document.getElementById(`clientTab${tab.charAt(0).toUpperCase()}${tab.slice(1)}`).classList.toggle("active", tab === state.clientTab);
        });

        if (state.creatingClient) {
          setText("clientWorkspaceTitle", "New client");
          document.getElementById("clientOverviewCards").innerHTML = '<div class="empty-state">Create the client first, then the overview fills in automatically.</div>';
          document.getElementById("clientRecentConversations").innerHTML = '<div class="empty-state">No conversations yet.</div>';
          document.getElementById("clientRecentLogs").innerHTML = '<div class="empty-state">No logs yet.</div>';
          document.getElementById("clientChecklist").innerHTML = '<div class="empty-state">Save the client to unlock onboarding and webhooks.</div>';
          document.getElementById("clientWebhookRows").innerHTML = '<div class="empty-state">Client-specific webhooks appear after creation.</div>';
          setText("clientBookingPreviewStatus", "");
          applyClientWizard();
          return;
        }

        if (!data) {
          setText("clientWorkspaceTitle", "Client workspace");
          document.getElementById("clientOverviewCards").innerHTML = '<div class="empty-state">Select a client from the left pane.</div>';
          document.getElementById("clientRecentConversations").innerHTML = '<div class="empty-state">No client selected.</div>';
          document.getElementById("clientRecentLogs").innerHTML = '<div class="empty-state">No client selected.</div>';
          document.getElementById("clientChecklist").innerHTML = '<div class="empty-state">No client selected.</div>';
          document.getElementById("clientWebhookRows").innerHTML = '<div class="empty-state">No client selected.</div>';
          setText("clientBookingPreviewStatus", "");
          applyClientWizard();
          return;
        }

        setText("clientWorkspaceTitle", `${data.client.business_name} workspace`);
        populateClientFormFromDetail(data);
        const countEntries = Object.entries(data.counts || {});
        const providerRuntime = data.provider_runtime || {};
        const clientProviderConfig = data.client.provider_config || {};
        const providerOverrideCount = Object.keys(clientProviderConfig).length;
        document.getElementById("clientOverviewCards").innerHTML = [
          ["Current state", data.client.is_active ? "active" : "inactive"],
          ["Timezone", data.client.timezone || "-"],
          ["Conversations", countEntries.reduce((sum, [, count]) => sum + Number(count), 0)],
          ["Booking", data.client.booking_mode || "link"],
          ["SMS provider", providerRuntime.twilio_configured ? (providerRuntime.twilio_from_number || "configured") : "missing"],
          ["AI provider", providerRuntime.ai_configured ? (providerRuntime.openai_model || "configured") : "AI offline"],
          ["Runtime source", providerRuntime.source === "client" ? "client overrides" : "global fallback"],
          ["Overrides", providerOverrideCount ? `${providerOverrideCount} keys` : "none"],
        ].map(([label, value]) => `<div class="surface stat-card"><div class="label">${escapeHtml(label)}</div><div class="value">${escapeHtml(value)}</div></div>`).join("");
        document.getElementById("clientRecentConversations").innerHTML = data.recent_conversations.length
          ? data.recent_conversations.map((item) => renderPreviewConversation(item, false)).join("")
          : '<div class="empty-state">No recent conversations.</div>';
        document.getElementById("clientRecentLogs").innerHTML = data.recent_logs.length
          ? data.recent_logs.map((log) => renderPreviewLog(log)).join("")
          : '<div class="empty-state">No recent logs.</div>';
        document.getElementById("clientChecklist").innerHTML = renderChecklist(data.onboarding);
        const clientBaseUrl = data.client.provider_config?.public_base_url || "";
        const webhookRows = Object.entries(data.webhook_urls || {}).map(([key, value]) => ({ label: key.replaceAll("_", " "), value: absoluteUrl(value, clientBaseUrl) }));
        document.getElementById("clientWebhookRows").innerHTML = renderWebhookRows(webhookRows);
        applyClientWizard();
      }

      function renderConversationList() {
        const items = filteredConversationItems();
        const includeClientName = !isClientRole();
        const counts = items.reduce((result, item) => {
          const key = item.state || "NEW";
          result[key] = (result[key] || 0) + 1;
          return result;
        }, {});
        const archivedHiddenCount = isClientRole() && !state.conversationFilters.showArchived
          ? (state.conversations.items || []).filter((item) => leadHasTag(item.tags, "archived")).length
          : 0;
        setText("conversationTotal", `${items.length}`);
        document.getElementById("conversationCountPills").innerHTML = Object.entries(counts)
          .map(([key, value]) => renderBadge(`${formatConversationStateLabel(key)} ${value}`, stateTone(key)))
          .join("");
        renderConversationClientGuide();
        document.getElementById("conversationList").innerHTML = items.length
          ? items.map((item) => {
              const visibleTags = uniqueStatusTags(item.tags || [], [item.crm_stage, item.state]);
              return `
                <div class="inbox-item ${item.lead_id === state.activeLeadId ? "active" : ""}" data-action="open-thread" data-lead-id="${item.lead_id}">
                  <div class="item-title-row">
                    <div class="item-title">${escapeHtml(item.lead_name)}</div>
                    <div class="actions">
                      ${isUnreadConversation(item) ? renderBadge("new", "info") : ""}
                      ${item.crm_stage ? renderBadge(item.crm_stage, "info") : ""}
                      ${renderMessageDeliveryStatus(item.last_message_delivery, { compact: true, onlyWarnings: true })}
                      ${maybeRenderConversationState(item.crm_stage, item.state)}
                    </div>
                  </div>
                  <div class="item-subtitle">${escapeHtml([item.phone || "-", formatLeadSourceLabel(item.source || "-"), includeClientName ? (item.client_name || "") : ""].filter(Boolean).join(" · "))}</div>
                    <div class="item-snippet">${renderLabeledSnippet(item, "No messages yet.", 140)}</div>
                    <div class="item-meta-row">
                      <div class="chip-row">
                      ${visibleTags.map((tag) => renderTag(tag)).join("")}
                      <button class="small ghost" data-action="open-crm-lead" data-lead-id="${item.lead_id}">${isClientRole() ? "Details" : "Open"}</button>
                    </div>
                    <div class="meta-text">${escapeHtml(formatDateTime(item.last_activity_at))}</div>
                  </div>
                </div>
              `;
            }).join("")
          : `<div class="empty-state"><div>${isClientRole() && archivedHiddenCount ? "Your active inbox is clear. Archived contacts stay in Pipeline until you restore them." : "No conversations match the current filters."}</div><div class="actions" style="margin-top: 8px;"><button class="small ghost" data-action="clear-conversation-filters">Clear filters</button></div></div>`;
        updateConversationMobileLayout();
      }

      function renderThread() {
        if (!state.thread) {
          setText("threadTitle", "Thread");
          document.getElementById("threadManualMessage").placeholder = isClientRole()
            ? t("Send a direct reply to this contact.")
            : t("Type a direct outbound message to this contact.");
          setText("threadComposerHint", isClientRole() ? "Your reply sends as the business's current SMS delivery mode." : "");
          const threadActionHintEl = document.getElementById("threadActionHint");
          threadActionHintEl.textContent = "";
          threadActionHintEl.classList.add("hidden");
          document.getElementById("threadArchiveButton").textContent = "Archive";
          document.getElementById("threadArchiveButton").disabled = true;
          document.getElementById("threadHandoffButton").disabled = true;
          const emptyThreadActionsButton = document.getElementById("threadContactActionsButton");
          if (emptyThreadActionsButton) {
            emptyThreadActionsButton.disabled = true;
            delete emptyThreadActionsButton.dataset.leadId;
          }
          document.getElementById("threadHeaderPills").classList.add("hidden");
          document.getElementById("threadHeaderPills").innerHTML = "";
          document.getElementById("threadPauseAfterSendWrap")?.classList.add("hidden");
          document.getElementById("threadTimeline").innerHTML = '<div class="empty-state">Open a conversation from the left pane.</div>';
          document.getElementById("threadCrmStageSelect").value = "New Lead";
          document.getElementById("threadNoteInput").placeholder = isClientRole() ? "Add a private note for your team." : "Add an internal note.";
          setText("threadCrmStageStatus", "");
          setText("threadLeadOverviewName", "");
          document.getElementById("threadLeadOverviewSubtitle").innerHTML = "";
          setText("threadLeadHeaderMeta", "");
          document.getElementById("threadLeadHeaderStageBadge").innerHTML = "";
          document.getElementById("threadLeadFactGrid").innerHTML = "";
          setText("threadLeadSummary", "");
          const summaryList = document.getElementById("threadLeadSummaryList");
          summaryList.innerHTML = "";
          summaryList.classList.add("hidden");
          document.getElementById("threadTagList").innerHTML = "";
          const emptyThreadTagInput = document.getElementById("threadTagInput");
          if (emptyThreadTagInput) emptyThreadTagInput.value = "";
          document.getElementById("threadFormAnswers").innerHTML = "";
          document.getElementById("threadNotes").innerHTML = "";
          document.getElementById("threadAuditEvents").innerHTML = "";
          state.threadTimelineLeadId = null;
          state.threadTimelineSignature = "";
          document.getElementById("threadEmpty").classList.remove("hidden");
          document.getElementById("threadDetails").classList.add("hidden");
          updateConversationMobileLayout();
          return;
        }

        const payload = state.thread;
        const archived = leadHasTag(payload.lead.tags || [], "archived");
        const sandboxThread = leadHasTag(payload.lead.tags || [], "sandbox");
        setText("threadTitle", payload.lead.display_name || payload.lead.phone || "Thread");
        document.getElementById("threadManualMessage").placeholder = isClientRole()
          ? (sandboxThread ? t("Type the next contact message for the AI sandbox.") : t("Send a direct reply to this contact."))
          : (sandboxThread ? t("Type the next contact message for the AI sandbox.") : t("Type a direct outbound message to this contact."));
        setText(
          "threadComposerHint",
          sandboxThread
            ? "Sandbox mode: this sends as the contact, runs the AI, and stores the reply here. No Twilio."
            : (isClientRole() ? "Your reply sends as the business's current SMS delivery mode." : "")
        );
        const threadActionHintEl = document.getElementById("threadActionHint");
        threadActionHintEl.textContent = "";
        threadActionHintEl.classList.add("hidden");
        document.getElementById("threadArchiveButton").textContent = archived ? "Restore" : "Archive";
        document.getElementById("threadArchiveButton").disabled = false;
        document.getElementById("threadHandoffButton").disabled = false;
        const threadActionsButton = document.getElementById("threadContactActionsButton");
        if (threadActionsButton) {
          threadActionsButton.disabled = false;
          threadActionsButton.dataset.leadId = String(payload.lead.id);
          threadActionsButton.dataset.source = "thread";
        }
        const threadHeaderPills = document.getElementById("threadHeaderPills");
        threadHeaderPills.classList.add("hidden");
        threadHeaderPills.innerHTML = "";
        const timelineEl = document.getElementById("threadTimeline");
        if (state.threadTimelineLeadId !== state.activeLeadId && typeof clearThreadMediaSelection === "function") {
          clearThreadMediaSelection();
        }
        const timelineSignature = threadTimelineSignature(payload.timeline || []);
        const timelineChanged = state.threadTimelineLeadId !== state.activeLeadId || state.threadTimelineSignature !== timelineSignature;
        const shouldStickToLatest = timelineChanged && (state.threadTimelineLeadId !== state.activeLeadId || isNearBottom(timelineEl, 96));
        if (timelineChanged) {
          timelineEl.innerHTML = payload.timeline.length
            ? payload.timeline.map((item) => {
                if (item.type === "message") {
                  const outbound = item.direction === "OUTBOUND";
                  const meta = [
                    outbound ? (isClientRole() ? "You" : "Outbound") : (isClientRole() ? "Contact" : "Inbound"),
                    formatDateTime(item.created_at),
                  ];
                  if (!isClientRole() && item.provider_message_sid) {
                    meta.push(item.provider_message_sid);
                  }
                  return `
                    <div class="bubble-row ${outbound ? "outbound" : "inbound"}">
                      <div class="bubble ${outbound ? "outbound" : ""}">
                        ${item.body ? `<div>${escapeHtml(item.body)}</div>` : ""}
                        ${renderMessageAttachments(item.attachments || [])}
                        ${outbound ? renderMessageDeliveryStatus(item.delivery) : ""}
                        <div class="bubble-meta">
                          ${meta.map((entry) => `<span>${escapeHtml(entry)}</span>`).join("")}
                        </div>
                      </div>
                    </div>
                  `;
                }
                if (item.type === "state") {
                  const detail = `${formatConversationStateLabel(item.previous_state)} -> ${formatConversationStateLabel(item.new_state)}`;
                  const reason = formatConversationTransitionReason(item.reason);
                  return renderThreadTimelineEvent("conversation-state", "Conversation Status", detail);
                }
                if (item.type === "crm_stage") {
                  const detail = `${item.previous_stage || "-"} -> ${item.new_stage || "-"}`;
                  const reason = formatCrmTransitionReason(item.reason);
                  return renderThreadTimelineEvent("crm-stage", "CRM Pipeline Stage", detail);
                }
                if (item.type === "task" || item.type === "task_completed") {
                  const detail = `${item.type === "task_completed" ? "Completed" : "Created"}: ${item.title || ""}`;
                  return renderThreadTimelineEvent("task-event", "Task", detail, "Task tracking update.");
                }
                return `<div class="note-event"><div class="meta-text">${escapeHtml(formatDateTime(item.created_at))}</div><div>${escapeHtml(item.body || "")}</div></div>`;
              }).join("")
            : '<div class="empty-state">No thread activity yet.</div>';
          state.threadTimelineSignature = timelineSignature;
        }
        state.threadTimelineLeadId = state.activeLeadId;
        if (shouldStickToLatest) {
          window.requestAnimationFrame(() => {
            timelineEl.scrollTop = timelineEl.scrollHeight;
          });
        }

        document.getElementById("threadEmpty").classList.add("hidden");
        document.getElementById("threadDetails").classList.remove("hidden");
        const threadLeadName = payload.lead.display_name || payload.lead.phone || "Contact";
        const threadLeadStage = payload.lead.crm_stage || "New Lead";
        const threadState = payload.lead.current_state || "";
        const threadAgentControl = payload.lead.agent_control || {};
        const threadAgentPaused = Boolean(threadAgentControl.paused);
        const threadAgentLabel = threadAgentControl.mode === "handoff"
          ? "Human handoff"
          : (threadAgentPaused ? "AI paused" : "AI active");
        threadHeaderPills.classList.remove("hidden");
        threadHeaderPills.innerHTML = [
          renderBadge(threadAgentLabel, threadAgentPaused ? "warn" : "ok"),
          sandboxThread ? renderBadge("sandbox", "info") : "",
        ].filter(Boolean).join("");
        const pauseAfterSend = document.getElementById("threadPauseAfterSend");
        if (pauseAfterSend && !document.getElementById("threadManualMessage")?.value.trim()) {
          pauseAfterSend.checked = threadAgentPaused;
        }
        document.getElementById("threadPauseAfterSendWrap")?.classList.toggle("hidden", sandboxThread);
        setText("threadLeadOverviewName", threadLeadName);
        const threadPhoneDisplay = formatPhoneNumber(payload.lead.phone);
        const threadEmailDisplay = String(payload.lead.email || "").trim();
        const threadHeaderPrimaryParts = [];
        if (threadPhoneDisplay && threadPhoneDisplay !== "-" && payload.lead.phone) {
          threadHeaderPrimaryParts.push(renderCopyableHeaderValue(threadPhoneDisplay, payload.lead.phone, "phone number"));
        }
        if (threadEmailDisplay) {
          threadHeaderPrimaryParts.push(renderCopyableHeaderValue(threadEmailDisplay, threadEmailDisplay, "email"));
        }
        const threadHeaderPrimary = threadHeaderPrimaryParts.join('<span class="lead-detail-sep">·</span>');
        document.getElementById("threadLeadOverviewSubtitle").innerHTML = threadHeaderPrimary || "No contact details yet.";
        setText(
          "threadLeadHeaderMeta",
          [
            formatLeadSourceLabel(payload.lead.source || "-"),
            payload.lead.last_activity_at ? formatLongDateTime(payload.lead.last_activity_at) : "",
            !isClientRole() ? payload.client.business_name : "",
          ].filter(Boolean).join(" · ")
        );
        document.getElementById("threadLeadHeaderStageBadge").innerHTML = [
          renderBadge(threadLeadStage, crmStageTone(threadLeadStage)),
          !isClientRole() ? maybeRenderConversationState(threadLeadStage, threadState) : "",
        ].filter(Boolean).join("");
        document.getElementById("threadLeadFactGrid").innerHTML = "";
        document.getElementById("threadCrmStageSelect").value = payload.lead.crm_stage || "New Lead";
        document.getElementById("threadNoteInput").placeholder = isClientRole() ? "Add a private note for your team." : "Add an internal note.";
        setText("threadCrmStageStatus", "");
        setText("threadLeadSummary", "");
        const summaryLines = mergeSummaryRows(payload.lead.summary_lines || [], payload.lead.form_answers || {}, threadLeadName);
        const summaryList = document.getElementById("threadLeadSummaryList");
        summaryList.innerHTML = renderSummaryFacts(summaryLines);
        summaryList.classList.toggle("hidden", !summaryLines.length);
        document.getElementById("threadFormAnswers").innerHTML = "";
        const visibleThreadTags = (payload.lead.tags || []).filter((tag) => !isDeprecatedUiTag(tag));
        document.getElementById("threadTagList").innerHTML = visibleThreadTags.length
          ? visibleThreadTags.map((tag) => `<span class="tag ${tagTone(tag)}">${escapeHtml(formatTagLabel(tag))}${leadHasTag([tag], "archived") ? "" : ` <button type="button" class="tag-remove-btn" data-action="crm-remove-tag" data-tag="${escapeHtml(tag)}" aria-label="Remove tag">&times;</button>`}</span>`).join("")
          : '<span class="meta-text">No tags</span>';
        const threadTagInput = document.getElementById("threadTagInput");
        if (threadTagInput) threadTagInput.value = "";
        document.getElementById("threadNotes").innerHTML = payload.notes.length
          ? payload.notes.map((note) => `<div class="note-item"><div class="item-title-row"><div class="item-title mono">${escapeHtml(note.actor || "note")}</div><div class="meta-text">${escapeHtml(formatDateTime(note.created_at))}</div></div><div class="item-snippet">${escapeHtml(note.body)}</div></div>`).join("")
          : `<div class="empty-state">${isClientRole() ? "No private notes." : "No internal notes."}</div>`;
        document.getElementById("threadAuditEvents").innerHTML = payload.audit_events.length
          ? payload.audit_events.slice().reverse().map((event) => {
              const lines = auditDetailLines(event);
              return `
                <div class="audit-item">
                  <div class="item-title-row">
                    <div class="audit-title">${escapeHtml(formatAuditEventLabel(event.event_type))}</div>
                    <div class="meta-text">${escapeHtml(formatDateTime(event.created_at))}</div>
                  </div>
                  <div class="audit-lines">
                    ${lines.length ? lines.map((line) => `<div class="audit-line">${escapeHtml(line)}</div>`).join("") : '<div class="audit-line">No extra details.</div>'}
                  </div>
                </div>
              `;
            }).join("")
          : `<div class="empty-state">${isClientRole() ? "No technical details yet." : "No visible audit events."}</div>`;
        updateConversationMobileLayout();
      }

      function renderLogs() {
        if (!state.selectedClientKey || !state.logEvents) {
          document.getElementById("logEventCards").innerHTML = '<div class="empty-state">Select a client to inspect its event stream.</div>';
          document.getElementById("logsTableBody").innerHTML = '<tr><td colspan="5">No client selected.</td></tr>';
          return;
        }
        document.getElementById("logEventCards").innerHTML = [
          ["last record", state.logEvents.last_lead_received_at],
          ["last inbound", state.logEvents.last_sms_inbound_at],
          ["last outbound", state.logEvents.last_sms_outbound_at],
          ["last AI", state.logEvents.last_ai_decision_at],
        ].map(([label, value]) => `<div class="surface stat-card compact-stat"><div class="label">${escapeHtml(label)}</div><div class="value">${escapeHtml(formatLongDateTime(value))}</div></div>`).join("");

        const q = state.globalSearch.trim().toLowerCase();
        const logs = state.logs.filter((log) => !q || JSON.stringify(log).toLowerCase().includes(q));
        document.getElementById("logsTableBody").innerHTML = logs.length
          ? logs.map((log) => `
              <tr>
                <td data-label="Time" class="mono">${escapeHtml(formatDateTime(log.created_at))}</td>
                <td data-label="Event" class="mono">${escapeHtml(log.event_type)}</td>
                <td data-label="Record">${escapeHtml(log.lead_id ?? "-")}</td>
                <td data-label="Decision" class="mono">${escapeHtml(JSON.stringify(log.decision || {}))}</td>
                <td data-label="Actions">${log.lead_id ? `<button class="small ghost" data-action="open-thread" data-lead-id="${log.lead_id}">Open</button>` : ""}</td>
              </tr>
            `).join("")
          : '<tr><td colspan="5">No matching audit rows.</td></tr>';
      }

      function renderSettings() {
        const isAdmin = !isClientRole();
        const runtime = state.runtime;
        if (isAdmin && runtime) {
          const openAiKey = runtime.openai_api_key || "";
          const openAiKeyInput = document.getElementById("settingsOpenAiKey");
          const openAiCopyButton = document.getElementById("settingsOpenAiCopyButton");
          if (openAiKeyInput) {
            openAiKeyInput.value = openAiKey;
            openAiKeyInput.type = "password";
          }
          if (openAiCopyButton) {
            openAiCopyButton.disabled = !openAiKey;
          }
          setText("settingsOpenAiRevealButton", "Reveal");
          setText("settingsOpenAiKeyStatus", openAiKey ? "Key loaded for admins." : "No OpenAI key configured.");
          document.getElementById("settingsOpenAiModel").value = runtime.openai_model || "gpt-4.1-mini";
          document.getElementById("settingsAiMode").value = "auto";
          document.getElementById("settingsRuntimeSummary").innerHTML = [
            renderBadge(runtime.openai_api_key_configured ? "OpenAI set" : "OpenAI missing", runtime.openai_api_key_configured ? "ok" : "warn"),
            renderBadge(runtime.openai_model || "model missing", runtime.openai_model ? "info" : "warn"),
          ].join("");
        }

        const detail = state.clientDetail;
        const ownerClient = state.ownerWorkspace?.client || null;
        const clientConfig = detail?.client || ownerClient;
        const readinessItems = (state.ownerWorkspace?.live_test_checklist || []).filter((item) => [
          "Twilio configured",
          "AI configured",
          "Automated booking ready",
        ].includes(item.label));
        const webhookData = detail?.webhook_urls || (clientConfig?.client_key ? {
          meta_verify: `/webhooks/meta/${clientConfig.client_key}`,
          meta_events: `/webhooks/meta/${clientConfig.client_key}`,
          zapier_events: `/webhooks/zapier/${clientConfig.client_key}`,
          linkedin_events: `/webhooks/linkedin/${clientConfig.client_key}`,
          twilio_sms: `/sms/inbound/${clientConfig.client_key}`,
        } : null);
        const selectedRuntime = detail?.provider_runtime || state.ownerWorkspace?.runtime || {};
        const setupSteps = isAdmin
          ? [
              {
                label: "Select a business",
                detail: clientConfig ? clientConfig.business_name || clientConfig.client_key : "Choose a client from the top bar or Clients page.",
                done: Boolean(clientConfig),
              },
              {
                label: "Global OpenAI configured",
                detail: runtime?.openai_api_key_configured ? (runtime.openai_model || "Configured") : "Add the centralized OpenAI key once.",
                done: Boolean(runtime?.openai_api_key_configured),
              },
              {
                label: "Client channels configured",
                detail: selectedRuntime.twilio_configured ? "SMS delivery is ready for this business." : "Add Twilio/Zapier/Meta/LinkedIn credentials in Clients > Edit.",
                done: Boolean(selectedRuntime.twilio_configured),
              },
              {
                label: "Business playbook ready",
                detail: clientConfig?.ai_context ? "Custom assistant guidance is present." : "Add offer, qualification, escalation, and tone notes.",
                done: Boolean(clientConfig?.ai_context),
              },
              {
                label: "Booking availability ready",
                detail: formatBookingModeLabel(clientConfig?.booking_mode || "link"),
                done: Boolean(clientConfig && ["internal", "calendar", "calendly"].includes(String(clientConfig.booking_mode || "").toLowerCase())),
              },
            ]
          : [
              {
                label: "Assistant guidance",
                detail: clientConfig?.ai_context ? "The assistant has business-specific instructions." : "Add guidance before relying on new AI replies.",
                done: Boolean(clientConfig?.ai_context),
              },
              {
                label: "Booking availability",
                detail: formatBookingModeLabel(clientConfig?.booking_mode || "link"),
                done: Boolean(clientConfig && ["internal", "calendar", "calendly"].includes(String(clientConfig.booking_mode || "").toLowerCase())),
              },
              {
                label: "SMS delivery",
                detail: formatDeliveryModeLabel(state.ownerWorkspace?.delivery_mode || "mock"),
                done: Boolean(selectedRuntime.twilio_configured),
              },
            ];
        const setupGuide = document.getElementById("settingsSetupGuideSteps");
        if (setupGuide) {
          setupGuide.innerHTML = renderChecklist(setupSteps);
        }

        setText("settingsAiSectionTitle", isAdmin ? "AI Context / Business Playbook" : "AI Assistant Guidance");
        setText(
          "settingsAiSectionSubtitle",
          isAdmin
            ? "Tailor how AI speaks, what to emphasize, and what to avoid for this business. Same field as Clients > Edit."
            : "Guide how your assistant speaks, qualifies new inquiries, and handles sensitive topics. Keep this focused on live customer-facing behavior."
        );
        setText(
          "settingsAiContextHelper",
          isAdmin
            ? "Include differentiators, claims to avoid, offer details, pricing guardrails, process steps, and preferred qualifying approach."
            : "Use short bullets. Include your offer, tone, qualifying approach, pricing guardrails, promises to avoid, and when to escalate to a human. Changes apply to new AI replies right away."
        );
        setText("saveAiContextButton", isAdmin ? "Save AI context" : "Save assistant guidance");
        setText("settingsCalendarSectionTitle", isAdmin ? "Internal calendar availability" : "Booking availability");
        setText(
          "settingsCalendarSectionSubtitle",
          isAdmin
            ? "Business owners can edit this directly. AI uses these windows when offering meeting times."
            : `Set the times new contacts can book.${clientConfig?.timezone ? ` Displayed in ${clientConfig.timezone}.` : ""}`
        );
        setText(
          "settingsCalendarHelper",
          isAdmin
            ? ""
            : ((clientConfig?.booking_mode || "link") !== "internal"
                ? "Saving here switches new booking offers to your internal calendar availability."
                : "Changes apply to new booking offers immediately.")
        );
        setText("saveSettingsCalendarButton", isAdmin ? "Save calendar availability" : "Save booking availability");
        document.getElementById("settingsAiContextInput").placeholder = isAdmin
          ? "Example:\n- We help local service businesses run paid ads and AI follow-up.\n- Prioritize qualified booked calls over raw volume.\n- Never guarantee exact numbers.\n- Ask one focused question at a time.\n- Tone: concise, direct, practical."
          : "Example:\n- Sound warm, confident, and practical.\n- Qualify for budget, timeline, and service area before offering times.\n- Never promise exact results or fixed pricing over text.\n- Escalate to a human if a contact asks for custom pricing or sounds upset.";
        document.getElementById("settingsFaqContextInput").placeholder = isAdmin
          ? "Services, process, constraints, pricing ranges, policies."
          : "Facts the assistant can safely reference: services, neighborhoods served, hours, pricing ranges, warranty notes, and policies.";

        document.getElementById("settingsAiContextInput").value = clientConfig?.ai_context || "";
        document.getElementById("settingsFaqContextInput").value = clientConfig?.faq_context || "";
        renderKnowledgeSettings();
        applySettingsCalendarToForm(clientConfig?.booking_config || {});
        renderSettingsCalendarVisuals();
        if (!clientConfig) {
          setText("settingsCalendarStatus", "Select a client first.");
        } else if (!document.getElementById("settingsCalendarStatus").textContent.startsWith("Saved at")) {
          setText("settingsCalendarStatus", "");
        }

        if (!isAdmin) {
          document.getElementById("settingsClientOverviewGrid").innerHTML = clientConfig
            ? [
                ["Business", clientConfig.business_name || "-"],
                ["Timezone", clientConfig.timezone || "-"],
                ["Messages", formatDeliveryModeLabel(state.ownerWorkspace?.delivery_mode || "mock")],
                ["Booking", formatBookingModeLabel(clientConfig.booking_mode || "link")],
              ].map(([label, value]) => `<div class="detail-card settings-overview-card"><div class="label">${escapeHtml(label)}</div><div class="value">${escapeHtml(value)}</div></div>`).join("")
            : '<div class="empty-state">Workspace details are loading.</div>';
          setText(
            "settingsClientOverviewNote",
            clientConfig
              ? "The controls below affect future AI replies, new booking offers, and test scenarios for this business. They do not rewrite past messages."
              : "Workspace details are loading."
          );
          document.getElementById("settingsClientChecklistSummary").innerHTML = readinessItems.length
            ? renderChecklist(readinessItems)
            : '<div class="empty-state">No readiness details yet.</div>';
        }

        if (!webhookData) {
          document.getElementById("settingsWebhookRows").innerHTML = '<div class="empty-state">Select a client to generate webhook URLs.</div>';
          setText("settingsWebhookWarning", "");
        } else {
          const clientBaseUrl = clientConfig?.provider_config?.public_base_url || "";
          const rows = Object.entries(webhookData || {}).map(([key, value]) => ({ label: key.replaceAll("_", " "), value: absoluteUrl(value, clientBaseUrl) }));
          document.getElementById("settingsWebhookRows").innerHTML = renderWebhookRows(rows);
          const effectiveBase = clientBaseUrl.trim();
          setText("settingsWebhookWarning", effectiveBase && !isLocalLike(effectiveBase) ? "" : "Set this client's public base URL in Clients > Edit for provider-ready webhook copies. Relative or localhost URLs will not work for Twilio or ad platforms.");
        }

        const demoClients = state.clients.filter((client) => client.client_key.startsWith("demo-"));
        document.getElementById("settingsDemoSummary").innerHTML = demoClients.length
          ? demoClients.map((client) => `<div class="preview-item"><div class="item-title-row"><div class="item-title">${escapeHtml(client.business_name)}</div>${renderBadge(client.client_key, "info")}</div><div class="item-snippet">${escapeHtml(`${client.lead_count} records · ${client.open_conversations} open`)}</div></div>`).join("")
          : '<div class="empty-state">No seeded demo clients detected.</div>';
        const showcaseHint = state.selectedClientKey
          ? `Target client: ${state.selectedClientKey}`
          : "Select a client to seed showcase records into that business.";
        setText("settingsClientSeedHint", showcaseHint);
        ["seedDemoButton", "reseedDemoButton", "resetDemoButton"].forEach((id) => {
          document.getElementById(id).disabled = !state.session?.can_seed_demo;
        });
        ["seedClientShowcaseButton", "reseedClientShowcaseButton"].forEach((id) => {
          document.getElementById(id).disabled = !state.session?.can_seed_demo || !state.selectedClientKey;
        });
      }

      function defaultTestLabAnswers() {
        return [
          ["Timeline", "Within 2 weeks"],
          ["Service interest", "I want to understand options and next steps"],
          ["Main goal", "Find the right solution without wasting time"],
          ["Decision role", "Owner"],
        ];
      }

      function readTestLabAnswers() {
        return Array.from(document.querySelectorAll(".test-lab-answer-row")).map((row) => ({
          question: row.querySelector("[data-answer-question]")?.value.trim() || "",
          answer: row.querySelector("[data-answer-value]")?.value.trim() || "",
        })).filter((row) => row.question || row.answer);
      }

      function renderTestLabAnswers(rows = null) {
        const container = document.getElementById("labFormAnswerRows");
        if (!container) return;
        const normalizedRows = (rows && rows.length ? rows : defaultTestLabAnswers()).map((row) => (
          Array.isArray(row) ? { question: row[0], answer: row[1] } : row
        ));
        container.innerHTML = normalizedRows.map((row, index) => `
          <div class="test-lab-answer-row">
            <div>
              <label>Question</label>
              <input data-answer-question value="${escapeHtml(row.question || "")}" placeholder="Timeline" />
            </div>
            <div>
              <label>Answer</label>
              <input data-answer-value value="${escapeHtml(row.answer || "")}" placeholder="Within 2 weeks" />
            </div>
            <button type="button" class="small ghost" data-action="remove-test-answer" data-index="${index}" aria-label="Remove answer">Remove</button>
          </div>
        `).join("");
      }

      function setTestLabMode(mode) {
        if (!["gpt_only", "gpt_zapier"].includes(mode)) {
          showNotice("That path is planned, but not wired yet.", "info");
          return;
        }
        state.testLabMode = mode;
        saveLocalState();
        document.querySelectorAll(".test-lab-mode-card").forEach((node) => {
          node.classList.toggle("active", node.dataset.mode === mode);
        });
        const startButton = document.getElementById("labStartButton");
        if (startButton) {
          startButton.textContent = mode === "gpt_zapier" ? "Start GPT + Zapier sandbox" : "Start GPT sandbox";
        }
        const output = document.getElementById("testLabOutput");
        if (output && !output.textContent.trim()) {
          output.textContent = mode === "gpt_zapier"
            ? "Book a meeting in the sandbox thread to see booking planner debug and the Zapier payload here."
            : "Ask for availability in the sandbox thread to see booking planner debug here. GPT-only mode will not call Zapier.";
        }
      }

      function renderTestLab() {
        const labClientSelect = document.getElementById("labClientSelect");
        if (labClientSelect && state.clients.length) {
          labClientSelect.value = state.selectedClientKey || state.clients[0]?.client_key || "";
        }
        renderTestLabAnswers(readTestLabAnswers());
        setTestLabMode(state.testLabMode || "gpt_only");

        const nameInput = document.getElementById("labLeadName");
        const phoneInput = document.getElementById("labLeadPhone");
        const emailInput = document.getElementById("labLeadEmail");
        const cityInput = document.getElementById("labLeadCity");
        if (nameInput && !nameInput.value) nameInput.value = "Strategy Call Contact";
        if (emailInput && !emailInput.value) emailInput.value = "contact@example.com";
        if (cityInput && !cityInput.value) cityInput.value = "Toronto";
        if (phoneInput && !phoneInput.value) phoneInput.value = "";

        const summary = document.getElementById("testLabOwnerSummary");
        if (!summary) return;
        if (!state.ownerWorkspace) {
          summary.innerHTML = '<div class="empty-state">Select a client to prepare the sandbox.</div>';
          setText("labStartStatus", "");
          return;
        }
        const runtime = state.ownerWorkspace.runtime || {};
        const providerConfig = state.ownerWorkspace.client.provider_config || {};
        summary.innerHTML = [
          ["Client", state.ownerWorkspace.client.business_name || state.ownerWorkspace.client.client_key],
          ["AI", runtime.ai_configured ? "Configured" : "Mock/offline"],
          ["Zapier booking", providerConfig.zapier_booking_webhook_url ? "Configured" : "Missing"],
          ["Training", state.ownerWorkspace.client.ai_context ? "Custom context" : "Default guidance"],
        ].map(([label, value]) => `
          <div class="test-lab-summary-card">
            <div class="label">${escapeHtml(label)}</div>
            <div class="value">${escapeHtml(value)}</div>
          </div>
        `).join("");
        if (!document.getElementById("labStartStatus").textContent) {
          setText(
            "labStartStatus",
            state.sandboxLeadId ? `Last sandbox contact: ${state.sandboxLeadId}. Starting a new one will create a fresh inbox thread.` : ""
          );
        }
      }

      function isActiveSandboxThread() {
        return Boolean(state.thread?.lead && leadHasTag(state.thread.lead.tags || [], "sandbox"));
      }

      function renderZapierResults() {
        const output = document.getElementById("zapierResultOutput");
        if (!output) return;
        if (!state.ownerWorkspace || !state.selectedClientKey) {
          output.textContent = "Select a client to view Zapier ingestion results.";
          return;
        }
        const clientBaseUrl = state.ownerWorkspace?.client?.provider_config?.public_base_url || "";
        const webhookUrl = absoluteUrl(state.zapierResults?.webhook_url || `/webhooks/zapier/${state.selectedClientKey}`, clientBaseUrl);
        const items = state.zapierResults?.items || [];
        if (!items.length) {
          output.textContent = `Endpoint: ${webhookUrl}\n\nNo Zapier activity yet.`;
          return;
        }
        const lines = items.map((item) => {
          const stamp = formatDateTime(item.created_at);
          const leadRef = item.lead_id ?? "-";
          const decision = JSON.stringify(item.decision || {});
          return `[${stamp}] ${item.event_type} · record ${leadRef}\n${decision}`;
        });
        output.textContent = `Endpoint: ${webhookUrl}\n\n${lines.join("\n\n")}`;
      }

      function renderKnowledgeSettings() {
        const urlsInput = document.getElementById("settingsKnowledgeUrlsInput");
        const summary = document.getElementById("settingsKnowledgeSummary");
        const output = document.getElementById("settingsKnowledgeExtraction");
        if (!urlsInput || !summary || !output) return;

        const sources = state.knowledge?.sources || [];
        if (document.activeElement !== urlsInput) {
          urlsInput.value = sources.map((source) => source.url).filter(Boolean).join("\n");
        }
        if (!state.selectedClientKey) {
          summary.innerHTML = '<div class="empty-state">Select a client first.</div>';
          output.textContent = "";
          return;
        }
        if (!sources.length) {
          summary.innerHTML = '<div class="empty-state">No website knowledge sources yet.</div>';
          output.textContent = "Add one URL per line, then ingest to preview extracted text and chunks.";
          return;
        }

        summary.innerHTML = sources.map((source) => {
          const tone = source.status === "ok" ? "ok" : "warn";
          const title = source.title || source.normalized_url || source.url;
          const detail = source.status === "ok"
            ? `${source.chunk_count || 0} chunks · ${formatDateTime(source.last_crawled_at)}`
            : (source.error_message || "Extraction failed");
          return `
            <div class="preview-item">
              <div class="item-title-row">
                <div class="item-title">${escapeHtml(title)}</div>
                ${renderBadge(source.status || "pending", tone)}
              </div>
              <div class="item-subtitle mono">${escapeHtml(source.url || "")}</div>
              <div class="item-snippet">${escapeHtml(detail)}</div>
            </div>
          `;
        }).join("");

        const lines = [];
        if (state.knowledge?.business_profile_context) {
          lines.push("Business profile context used on every AI turn:");
          lines.push(state.knowledge.business_profile_context);
          lines.push("");
        }
        sources.forEach((source, sourceIndex) => {
          lines.push(`[${sourceIndex + 1}] ${source.title || source.url}`);
          lines.push(`URL: ${source.url}`);
          lines.push(`Status: ${source.status}${source.error_message ? ` (${source.error_message})` : ""}`);
          if (source.text_excerpt) {
            lines.push(`Extracted excerpt:\n${source.text_excerpt}`);
          }
          (source.chunks || []).slice(0, 5).forEach((chunk) => {
            lines.push(`Chunk ${Number(chunk.chunk_index || 0) + 1}:\n${chunk.content}`);
          });
          lines.push("");
        });
        output.textContent = lines.join("\n");
      }

      async function refreshKnowledgeSettings() {
        if (!state.selectedClientKey) {
          setText("settingsKnowledgeStatus", "Select a client first.");
          renderKnowledgeSettings();
          return;
        }
        try {
          state.knowledge = await apiJson(`/ui/api/owner/${encodeURIComponent(state.selectedClientKey)}/knowledge`);
          renderKnowledgeSettings();
          setText("settingsKnowledgeStatus", `Loaded ${state.knowledge.total_sources || 0} sources.`);
        } catch (error) {
          setText("settingsKnowledgeStatus", `Refresh failed: ${error.message}`);
          showNotice(`Knowledge refresh failed: ${error.message}`, "err");
        }
      }

      async function ingestKnowledgeUrls() {
        if (!state.selectedClientKey) {
          setText("settingsKnowledgeStatus", "Select a client first.");
          return;
        }
        const urls = document.getElementById("settingsKnowledgeUrlsInput").value
          .split(/\n+/)
          .map((line) => line.trim())
          .filter(Boolean);
        if (!urls.length) {
          setText("settingsKnowledgeStatus", "Add at least one URL.");
          return;
        }
        setText("settingsKnowledgeStatus", "Fetching and extracting website text...");
        try {
          const result = await apiJson(`/ui/api/owner/${encodeURIComponent(state.selectedClientKey)}/knowledge/ingest`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ urls, replace: true }),
          });
          state.knowledge = result;
          if (state.ownerWorkspace?.client?.client_key === state.selectedClientKey) {
            state.ownerWorkspace.knowledge = result;
          }
          renderKnowledgeSettings();
          const okCount = (result.extraction?.pages || []).filter((page) => page.status === "ok").length;
          setText("settingsKnowledgeStatus", `Ingested ${okCount}/${urls.length} URLs · ${result.total_chunks || 0} chunks.`);
          showNotice("Website knowledge ingested.", "ok");
        } catch (error) {
          setText("settingsKnowledgeStatus", `Ingest failed: ${error.message}`);
          showNotice(`Knowledge ingest failed: ${error.message}`, "err");
        }
      }
