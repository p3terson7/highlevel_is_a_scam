      const state = {
        session: null,
        runtime: null,
        dashboard: null,
        clients: [],
        clientDetail: null,
        logEvents: null,
        logs: [],
        ownerWorkspace: null,
        zapierResults: null,
        automationHealth: null,
        knowledge: null,
        conversations: { items: [], counts: {}, total: 0 },
        crmLeads: { items: [], counts: {}, total: 0, stages: [] },
        crmLeadDetail: null,
        crmTasks: { items: [], counts: {}, total: 0 },
        calendar: { items: [], total: 0, timezone: "UTC", booking_mode: "link" },
        calendarMonth: localStorage.getItem("lead-ui-calendar-month") || "",
        calendarSelectedDate: localStorage.getItem("lead-ui-calendar-day") || "",
        crmAddLeadOpen: false,
        calendarMeetingPanelOpen: false,
        calendarLeadPanelOpen: false,
        contactActionDrawer: {
          open: false,
          leadId: null,
          source: "",
        },
        blockedOutboundAttempt: null,
        thread: null,
        selectedClientKey: localStorage.getItem("lead-ui-selected-client") || "",
        activeLeadId: Number(localStorage.getItem("lead-ui-active-lead") || 0) || null,
        activeCrmLeadId: Number(localStorage.getItem("lead-ui-active-crm-lead") || 0) || null,
        sandboxLeadId: Number(localStorage.getItem("lead-ui-sandbox-lead") || 0) || null,
        testLabMode: localStorage.getItem("lead-ui-test-lab-mode") || "gpt_only",
        activeView: localStorage.getItem("lead-ui-view") || "dashboard",
        conversationMobilePanel: localStorage.getItem("lead-ui-conv-mobile-panel") || "list",
        conversationFiltersExpanded: localStorage.getItem("lead-ui-conv-filters-open") === "true",
        threadTimelineLeadId: null,
        threadTimelineSignature: "",
        clientTab: localStorage.getItem("lead-ui-client-tab") || "overview",
        clientWizardStep: localStorage.getItem("lead-ui-client-wizard-step") || "business",
        sidebarCollapsed: localStorage.getItem("lead-ui-sidebar-collapsed") === "true",
        theme: localStorage.getItem("lead-ui-theme") || "dark",
        density: localStorage.getItem("lead-ui-density") || "compact",
        globalSearch: localStorage.getItem("lead-ui-global-search") || "",
        creatingClient: false,
        noticeTimer: null,
        viewedMap: JSON.parse(localStorage.getItem("lead-ui-viewed-map") || "{}"),
        conversationFilters: {
          clientKey: localStorage.getItem("lead-ui-conv-client") || "",
          state: localStorage.getItem("lead-ui-conv-state") || "all",
          dateFrom: localStorage.getItem("lead-ui-conv-date-from") || "",
          dateTo: localStorage.getItem("lead-ui-conv-date-to") || "",
          unreadOnly: localStorage.getItem("lead-ui-conv-unread") === "true",
          showArchived: localStorage.getItem("lead-ui-conv-archived") === "true",
        },
        crmFilters: {
          clientKey: localStorage.getItem("lead-ui-crm-client") || "",
          stage: localStorage.getItem("lead-ui-crm-stage") || "all",
          showArchived: localStorage.getItem("lead-ui-crm-archived") === "true",
        },
        taskFilters: {
          clientKey: localStorage.getItem("lead-ui-task-client") || "",
          status: localStorage.getItem("lead-ui-task-status") || "all",
        },
        authMode: localStorage.getItem("lead-ui-auth-mode") || "admin",
        split: {
          conversations: {
            left: Number(localStorage.getItem("lead-ui-split-conv-left") || 320),
            right: Number(localStorage.getItem("lead-ui-split-conv-right") || 300),
          },
        },
        searchTimer: null,
        pollTimer: null,
      };

      function adminToken() {
        localStorage.removeItem("lead-ui-admin-token");
        return "";
      }

      function portalToken() {
        localStorage.removeItem("lead-ui-portal-token");
        return "";
      }

      function setAdminToken(token) {
        void token;
        localStorage.removeItem("lead-ui-admin-token");
      }

      function setPortalToken(token) {
        void token;
        localStorage.removeItem("lead-ui-portal-token");
      }

      function clearAdminToken() {
        localStorage.removeItem("lead-ui-admin-token");
      }

      function clearPortalToken() {
        localStorage.removeItem("lead-ui-portal-token");
      }

      function clearSavedSession() {
        clearAdminToken();
        clearPortalToken();
        localStorage.removeItem("lead-ui-portal-email");
        try {
          const keys = Array.from({ length: sessionStorage.length }, (_, index) => sessionStorage.key(index));
          keys.forEach((key) => {
            if (key?.startsWith("lead-ui-outbound-request:")) sessionStorage.removeItem(key);
          });
        } catch (_error) {
          // Best-effort cleanup when browser storage is unavailable.
        }
        state.outboundRequestKeys = {};
      }

      function saveLocalState() {
        localStorage.setItem("lead-ui-selected-client", state.selectedClientKey || "");
        localStorage.setItem("lead-ui-active-lead", state.activeLeadId ? String(state.activeLeadId) : "");
        localStorage.setItem("lead-ui-active-crm-lead", state.activeCrmLeadId ? String(state.activeCrmLeadId) : "");
        localStorage.setItem("lead-ui-view", state.activeView);
        localStorage.setItem("lead-ui-conv-mobile-panel", state.conversationMobilePanel || "list");
        localStorage.setItem("lead-ui-conv-filters-open", String(Boolean(state.conversationFiltersExpanded)));
        localStorage.setItem("lead-ui-client-tab", state.clientTab);
        localStorage.setItem("lead-ui-client-wizard-step", state.clientWizardStep || "business");
        localStorage.setItem("lead-ui-sidebar-collapsed", String(state.sidebarCollapsed));
        localStorage.setItem("lead-ui-theme", state.theme);
        localStorage.setItem("lead-ui-density", state.density);
        localStorage.setItem("lead-ui-global-search", state.globalSearch);
        localStorage.setItem("lead-ui-viewed-map", JSON.stringify(state.viewedMap));
        localStorage.setItem("lead-ui-conv-client", state.conversationFilters.clientKey || "");
        localStorage.setItem("lead-ui-conv-state", state.conversationFilters.state || "all");
        localStorage.setItem("lead-ui-conv-date-from", state.conversationFilters.dateFrom || "");
        localStorage.setItem("lead-ui-conv-date-to", state.conversationFilters.dateTo || "");
        localStorage.setItem("lead-ui-conv-unread", String(state.conversationFilters.unreadOnly));
        localStorage.setItem("lead-ui-conv-archived", String(state.conversationFilters.showArchived));
        localStorage.setItem("lead-ui-crm-client", state.crmFilters.clientKey || "");
        localStorage.setItem("lead-ui-crm-stage", state.crmFilters.stage || "all");
        localStorage.setItem("lead-ui-crm-archived", String(state.crmFilters.showArchived));
        localStorage.setItem("lead-ui-task-client", state.taskFilters.clientKey || "");
        localStorage.setItem("lead-ui-task-status", state.taskFilters.status || "all");
        localStorage.setItem("lead-ui-calendar-month", state.calendarMonth || "");
        localStorage.setItem("lead-ui-calendar-day", state.calendarSelectedDate || "");
        localStorage.setItem("lead-ui-sandbox-lead", state.sandboxLeadId || "");
        localStorage.setItem("lead-ui-test-lab-mode", state.testLabMode || "gpt_only");
        localStorage.setItem("lead-ui-split-conv-left", String(state.split.conversations.left));
        localStorage.setItem("lead-ui-split-conv-right", String(state.split.conversations.right));
        localStorage.setItem("lead-ui-auth-mode", state.authMode);
      }

      function csrfToken() {
        const match = document.cookie.split(";").map((value) => value.trim()).find((value) => value.startsWith("leadops_csrf="));
        if (!match) return "";
        try {
          return decodeURIComponent(match.slice("leadops_csrf=".length));
        } catch (_) {
          return "";
        }
      }

      function authHeaders(extra = {}, method = "GET") {
        const headers = { ...extra };
        Object.keys(headers).forEach((name) => {
          const normalized = name.toLowerCase();
          if (normalized === "x-admin-token" || normalized === "x-portal-token") {
            delete headers[name];
          }
        });
        if (["POST", "PUT", "PATCH", "DELETE"].includes(String(method || "GET").toUpperCase())) {
          const token = csrfToken();
          if (token) headers["X-CSRF-Token"] = token;
        }
        return headers;
      }

      async function apiJson(path, options = {}) {
        const response = await fetch(path, {
          ...options,
          credentials: "same-origin",
          headers: authHeaders({ ...(options.headers || {}) }, options.method),
        });
        const text = await response.text();
        let payload = text;
        try {
          payload = JSON.parse(text);
        } catch (_) {}
        if (!response.ok) {
          if (response.status === 401) {
            lockUi("Session expired. Sign in again.");
          }
          const detail = typeof payload === "string" ? payload : payload.detail || JSON.stringify(payload);
          throw new Error(detail);
        }
        return payload;
      }

      function escapeHtml(value) {
        return String(value ?? "")
          .replaceAll("&", "&amp;")
          .replaceAll("<", "&lt;")
          .replaceAll(">", "&gt;")
          .replaceAll('"', "&quot;")
          .replaceAll("'", "&#39;");
      }

      function setText(id, value) {
        const el = document.getElementById(id);
        if (el) el.textContent = typeof translateTextValue === "function" ? translateTextValue(value) : value;
      }

      function formatDateTime(value) {
        if (!value) return "-";
        const date = new Date(value);
        if (Number.isNaN(date.getTime())) return String(value);
        return new Intl.DateTimeFormat(typeof uiLocale === "function" ? uiLocale() : undefined, {
          month: "short",
          day: "numeric",
          hour: "numeric",
          minute: "2-digit",
        }).format(date);
      }

      function isClientRole() {
        return state.session?.role === "client";
      }

      function formatConversationStateLabel(value) {
        const key = String(value || "").trim().toUpperCase();
        const labels = {
          NEW: "New",
          GREETED: "Contacted",
          QUALIFYING: "Qualifying",
          BOOKING_SENT: "Scheduling",
          BOOKED: "Booked",
          HANDOFF: "Needs Handoff",
          OPTED_OUT: "Opted Out",
        };
        return labels[key] || formatFormKey(key || "-");
      }

      function formatLeadSourceLabel(value) {
        const key = String(value || "").trim().toLowerCase();
        const labels = {
          meta: "Meta lead ad",
          linkedin: "LinkedIn lead form",
          sms: "SMS intake",
          zapier: "Zapier intake",
          manual: "Manual entry",
        };
        return labels[key] || formatFormKey(key || "-");
      }

      function datePartsInTimeZone(value, timeZone) {
        const date = value instanceof Date ? value : new Date(value);
        if (Number.isNaN(date.getTime())) return null;
        const formatter = new Intl.DateTimeFormat("en-US", {
          timeZone: timeZone || undefined,
          year: "numeric",
          month: "2-digit",
          day: "2-digit",
        });
        const map = {};
        formatter.formatToParts(date).forEach((part) => {
          if (part.type !== "literal") map[part.type] = part.value;
        });
        if (!map.year || !map.month || !map.day) return null;
        return { year: map.year, month: map.month, day: map.day };
      }

      function dateKeyInTimeZone(value, timeZone) {
        const parts = datePartsInTimeZone(value, timeZone);
        return parts ? `${parts.year}-${parts.month}-${parts.day}` : "";
      }

      function parseMonthKey(monthKey) {
        const match = String(monthKey || "").match(/^(\d{4})-(\d{2})$/);
        if (!match) return null;
        return { year: Number(match[1]), month: Number(match[2]) };
      }

      function monthKeyForDateKey(dateKey) {
        return String(dateKey || "").slice(0, 7);
      }

      function shiftMonthKey(monthKey, offset) {
        const parts = parseMonthKey(monthKey);
        if (!parts) return monthKey;
        const anchor = new Date(Date.UTC(parts.year, parts.month - 1 + Number(offset || 0), 1));
        return `${anchor.getUTCFullYear()}-${String(anchor.getUTCMonth() + 1).padStart(2, "0")}`;
      }

      function calendarDateFromKey(dateKey) {
        const match = String(dateKey || "").match(/^(\d{4})-(\d{2})-(\d{2})$/);
        if (!match) return null;
        return new Date(Date.UTC(Number(match[1]), Number(match[2]) - 1, Number(match[3]), 12));
      }

      function formatMonthLabel(monthKey, timeZone) {
        const parts = parseMonthKey(monthKey);
        if (!parts) return "Month";
        const anchor = new Date(Date.UTC(parts.year, parts.month - 1, 15, 12));
        return new Intl.DateTimeFormat(typeof uiLocale === "function" ? uiLocale() : undefined, {
          timeZone: timeZone || undefined,
          month: "long",
          year: "numeric",
        }).format(anchor);
      }

      function formatDateLabel(dateKey, timeZone, options = {}) {
        const anchor = calendarDateFromKey(dateKey);
        if (!anchor) return dateKey || "-";
        return new Intl.DateTimeFormat(typeof uiLocale === "function" ? uiLocale() : undefined, {
          timeZone: timeZone || undefined,
          month: options.month || "short",
          day: options.day || "numeric",
          weekday: options.weekday,
        }).format(anchor);
      }

      function formatTimeInTimeZone(value, timeZone) {
        if (!value) return "-";
        const date = new Date(value);
        if (Number.isNaN(date.getTime())) return String(value);
        return new Intl.DateTimeFormat(typeof uiLocale === "function" ? uiLocale() : undefined, {
          timeZone: timeZone || undefined,
          hour: "numeric",
          minute: "2-digit",
        }).format(date);
      }

      function minutesFromClockValue(value) {
        const match = String(value || "").match(/^(\d{1,2}):(\d{2})$/);
        if (!match) return null;
        const hour = Number(match[1]);
        const minute = Number(match[2]);
        if (Number.isNaN(hour) || Number.isNaN(minute) || hour < 0 || hour > 23 || minute < 0 || minute > 59) return null;
        return (hour * 60) + minute;
      }

      function formatClockValue(value) {
        const totalMinutes = minutesFromClockValue(value);
        if (totalMinutes == null) return "-";
        const hour24 = Math.floor(totalMinutes / 60);
        const minute = totalMinutes % 60;
        if (typeof getUiLanguage === "function" && getUiLanguage() === "fr") {
          return `${hour24} h ${String(minute).padStart(2, "0")}`;
        }
        const suffix = hour24 >= 12 ? "PM" : "AM";
        const hour12 = hour24 % 12 || 12;
        return `${hour12}:${String(minute).padStart(2, "0")} ${suffix}`;
      }

      function formatCoverageHours(totalMinutes) {
        const minutes = Math.max(0, Number(totalMinutes || 0));
        const hours = Math.floor(minutes / 60);
        const mins = minutes % 60;
        if (!hours && !mins) return "0h";
        if (!mins) return `${hours}h`;
        if (!hours) return `${mins}m`;
        return `${hours}h ${mins}m`;
      }

      function renderSettingsCalendarVisuals() {
        const metricsEl = document.getElementById("settingsCalendarMetrics");
        if (!metricsEl) return;

        const slotMinutes = Number(document.getElementById("settingsCalendarSlotMinutes")?.value || 30);
        const noticeMinutes = Number(document.getElementById("settingsCalendarNoticeMinutes")?.value || 120);
        const horizonDays = Number(document.getElementById("settingsCalendarHorizonDays")?.value || 14);

        let enabledDays = 0;
        let totalCoverageMinutes = 0;

        for (let day = 0; day < 7; day += 1) {
          const enabledInput = document.getElementById(`settingsCalDay${day}Enabled`);
          const startInput = document.getElementById(`settingsCalDay${day}Start`);
          const endInput = document.getElementById(`settingsCalDay${day}End`);
          const rangeEl = document.getElementById(`settingsCalDay${day}Range`);
          const rowEl = document.querySelector(`#settingsCalendarAvailability .settings-calendar-day[data-day="${day}"]`);
          if (!enabledInput || !startInput || !endInput) continue;

          const enabled = Boolean(enabledInput.checked);
          const startMinutes = minutesFromClockValue(startInput.value);
          const endMinutes = minutesFromClockValue(endInput.value);
          const hasValidRange = startMinutes != null && endMinutes != null && endMinutes > startMinutes;

          startInput.disabled = !enabled;
          endInput.disabled = !enabled;
          if (rowEl) {
            rowEl.classList.toggle("disabled", !enabled);
            rowEl.classList.toggle("invalid", enabled && !hasValidRange);
          }

          if (!enabled) {
            if (rangeEl) rangeEl.textContent = "Unavailable";
            continue;
          }

          enabledDays += 1;
          if (hasValidRange) {
            totalCoverageMinutes += (endMinutes - startMinutes);
            if (rangeEl) rangeEl.textContent = `${formatClockValue(startInput.value)} - ${formatClockValue(endInput.value)}`;
          } else if (rangeEl) {
            rangeEl.textContent = "Invalid time window";
          }
        }

        metricsEl.innerHTML = [
          ["Active days", `${enabledDays}/7`],
          ["Weekly coverage", formatCoverageHours(totalCoverageMinutes)],
          ["Booking rules", `${slotMinutes}m slots · ${noticeMinutes}m notice · ${horizonDays}d horizon`],
        ].map(([label, value]) => `
          <div class="settings-calendar-metric">
            <div class="settings-calendar-metric-label">${escapeHtml(label)}</div>
            <div class="settings-calendar-metric-value">${escapeHtml(value)}</div>
          </div>
        `).join("");
      }

      function formatTagLabel(value) {
        const text = String(value || "").trim();
        if (!text) return "-";
        if (/^[a-z0-9 ]+$/.test(text)) {
          return text.charAt(0).toUpperCase() + text.slice(1);
        }
        return text;
      }

      function leadHasTag(tags, expected) {
        const needle = String(expected || "").trim().toLowerCase();
        if (!needle || !Array.isArray(tags)) return false;
        return tags.some((tag) => String(tag || "").trim().toLowerCase() === needle);
      }

      function tagTone(label) {
        const key = String(label || "").trim().toLowerCase();
        if (key.includes("booking")) return "ok";
        if (key.includes("handoff") || key.includes("opted out") || key === "archived") return "warn";
        return "";
      }

      function formatBookingModeLabel(value) {
        const key = String(value || "").trim().toLowerCase();
        const labels = {
          internal: "Internal calendar",
          calendar: "Internal calendar",
          calendly: "Calendly",
          link: "Link only",
        };
        return labels[key] || formatFormKey(key || "-");
      }

      function formatDeliveryModeLabel(value) {
        return String(value || "").trim().toLowerCase() === "twilio" ? "Live SMS" : "Mock mode";
      }

      function formatLongDateTime(value) {
        if (!value) return "-";
        const date = new Date(value);
        if (Number.isNaN(date.getTime())) return String(value);
        return new Intl.DateTimeFormat(typeof uiLocale === "function" ? uiLocale() : undefined, {
          month: "short",
          day: "numeric",
          year: "numeric",
          hour: "numeric",
          minute: "2-digit",
        }).format(date);
      }

      function formatPhoneNumber(value) {
        const raw = String(value || "").trim();
        if (!raw) return "-";
        const digits = raw.replace(/\D/g, "");
        if (digits.length === 11 && digits.startsWith("1")) {
          return `+1 (${digits.slice(1, 4)}) ${digits.slice(4, 7)}-${digits.slice(7)}`;
        }
        if (digits.length === 10) {
          return `(${digits.slice(0, 3)}) ${digits.slice(3, 6)}-${digits.slice(6)}`;
        }
        if (raw.startsWith("+") && digits.length > 10) {
          return `+${digits}`;
        }
        return raw;
      }

      function formatCompactCurrency(value) {
        const number = Number(value || 0);
        if (!Number.isFinite(number) || number <= 0) return "";
        return new Intl.NumberFormat(typeof uiLocale === "function" ? uiLocale() : "en-US", {
          style: "currency",
          currency: "CAD",
          notation: Math.abs(number) >= 10000 ? "compact" : "standard",
          maximumFractionDigits: Math.abs(number) >= 10000 ? 1 : 0,
        }).format(number);
      }

      function formatScoreLabel(value) {
        const score = Number(value || 0);
        if (!Number.isFinite(score) || score <= 0) return "";
        return `${Math.round(score)}/100`;
      }

      function stateTone(value) {
        if (["BOOKED", "BOOKING_SENT"].includes(value)) return "ok";
        if (["HANDOFF", "OPTED_OUT"].includes(value)) return "warn";
        if (["QUALIFYING", "GREETED"].includes(value)) return "info";
        return "";
      }

      function crmStageTone(value) {
        const stage = String(value || "").trim();
        if (!stage) return "";
        if (stage.includes("Won") || stage.includes("Booked")) return "ok";
        if (stage === "Lost") return "warn";
        if (["Contacted", "Qualified", "Meeting Completed"].includes(stage)) return "info";
        return "";
      }

      function formatCrmStageDisplay(value) {
        const stage = String(value || "").trim();
        return stage === "New Lead" ? "New" : stage;
      }

      function isConversationStateRedundant(crmStage, conversationState) {
        const stage = String(crmStage || "").trim().toLowerCase();
        const stateValue = String(conversationState || "").trim().toUpperCase();
        if (!stage || !stateValue) return false;
        if (stage === "new lead" && ["NEW", "GREETED"].includes(stateValue)) return true;
        if (stage === "contacted" && ["GREETED", "QUALIFYING"].includes(stateValue)) return true;
        if (stage === "qualified" && ["QUALIFYING", "BOOKING_SENT"].includes(stateValue)) return true;
        if (stage === "meeting booked" && ["BOOKING_SENT", "BOOKED"].includes(stateValue)) return true;
        if (stage === "meeting completed" && stateValue === "BOOKED") return true;
        if (stage === "won" && stateValue === "BOOKED") return true;
        if (stage === "lost" && ["OPTED_OUT", "HANDOFF"].includes(stateValue)) return true;
        return false;
      }

      function maybeRenderConversationState(crmStage, conversationState) {
        if (!conversationState) return "";
        if (isConversationStateRedundant(crmStage, conversationState)) return "";
        return renderStatePill(conversationState, stateTone(conversationState));
      }

      function canonicalBadgeKey(value) {
        const raw = String(value || "")
          .trim()
          .toLowerCase()
          .replaceAll("_", " ")
          .replaceAll("-", " ")
          .replace(/\s+/g, " ");
        const aliases = {
          "new": "new",
          "new lead": "new",
          "contacted": "contacted",
          "greeted": "contacted",
          "qualified": "qualified",
          "qualifying": "qualified",
          "meeting booked": "booked",
          "booked": "booked",
          "booking sent": "booking sent",
          "booking link sent": "booking sent",
          "meeting completed": "meeting completed",
          "needs handoff": "handoff",
          "needs hand off": "handoff",
          "handoff": "handoff",
          "won": "won",
          "lost": "lost",
          "opted out": "opted out",
          "archived": "archived",
        };
        return aliases[raw] || raw;
      }

      function isDeprecatedUiTag(value) {
        return canonicalBadgeKey(value) === "booking sent";
      }

      function uniqueStatusTags(tags = [], excludedLabels = [], limit = null) {
        const seen = new Set((excludedLabels || []).map((label) => canonicalBadgeKey(label)).filter(Boolean));
        const filtered = [];
        (tags || []).forEach((tag) => {
          const key = canonicalBadgeKey(tag);
          if (!key || isDeprecatedUiTag(tag) || seen.has(key)) return;
          seen.add(key);
          filtered.push(tag);
        });
        return limit == null ? filtered : filtered.slice(0, limit);
      }

      function lastMessageLabel(direction) {
        return String(direction || "").toUpperCase() === "INBOUND" ? "Contact" : "Latest";
      }

      function renderLabeledSnippet(item, fallback = "No messages yet.", maxLen = 170) {
        const label = typeof t === "function" ? t(lastMessageLabel(item?.last_message_direction || "")) : lastMessageLabel(item?.last_message_direction || "");
        const snippet = summarizeText(item?.last_message_snippet || fallback, maxLen) || fallback;
        return `<span class="snippet-label">${escapeHtml(label)}:</span>${escapeHtml(snippet)}`;
      }

      function formatBytes(value) {
        const bytes = Number(value || 0);
        if (!Number.isFinite(bytes) || bytes <= 0) return "";
        if (bytes < 1024 * 1024) return `${Math.max(1, Math.round(bytes / 1024))} KB`;
        return `${(bytes / (1024 * 1024)).toFixed(bytes >= 10 * 1024 * 1024 ? 0 : 1)} MB`;
      }

      function renderMessageAttachments(attachments = []) {
        const items = Array.isArray(attachments) ? attachments : [];
        if (!items.length) return "";
        return `
          <div class="message-attachments">
            ${items.map((attachment) => {
              const kind = String(attachment.media_kind || "").toLowerCase();
              const url = String(attachment.url || "");
              const filename = String(attachment.filename || "media");
              const size = formatBytes(attachment.size_bytes);
              const caption = [filename, size].filter(Boolean).join(" · ");
              if (kind === "image") {
                return `
                  <figure class="message-attachment image">
                    <a href="${escapeHtml(url)}" target="_blank" rel="noopener noreferrer">
                      <img src="${escapeHtml(url)}" alt="${escapeHtml(filename)}" loading="lazy" />
                    </a>
                    <figcaption>${escapeHtml(caption)}</figcaption>
                  </figure>
                `;
              }
              if (kind === "video") {
                return `
                  <figure class="message-attachment video">
                    <video src="${escapeHtml(url)}" controls preload="metadata"></video>
                    <figcaption>${escapeHtml(caption)}</figcaption>
                  </figure>
                `;
              }
              return `
                <a class="message-attachment file" href="${escapeHtml(url)}" target="_blank" rel="noopener noreferrer">
                  ${escapeHtml(caption || "Attachment")}
                </a>
              `;
            }).join("")}
          </div>
        `;
      }

      function messageDeliveryTone(delivery) {
        const severity = String(delivery?.severity || "").toLowerCase();
        if (severity === "warning") return "warn";
        if (severity === "ok") return "ok";
        return "info";
      }

      function messageDeliveryNeedsAttention(delivery) {
        return String(delivery?.severity || "").toLowerCase() === "warning";
      }

      function renderMessageDeliveryStatus(delivery, options = {}) {
        if (!delivery || typeof delivery !== "object") return "";
        if (options.onlyWarnings && !messageDeliveryNeedsAttention(delivery)) return "";
        const tone = messageDeliveryTone(delivery);
        const useFrench = typeof getUiLanguage === "function" && getUiLanguage() === "fr";
        const label = String((useFrench ? delivery.label_fr : "") || delivery.label || "SMS status unknown");
        const description = String((useFrench ? delivery.description_fr : "") || delivery.description || "");
        const providerStatus = String(delivery.provider_status || delivery.status || "").replaceAll("_", " ");
        const providerLabel = useFrench ? "Statut fournisseur" : "Provider status";
        const title = [label, description, providerStatus ? `${providerLabel}: ${providerStatus}` : ""].filter(Boolean).join(" · ");
        return `
          <div class="message-delivery-status ${tone} ${options.compact ? "compact" : ""}" title="${escapeHtml(title)}">
            <span class="delivery-dot"></span>
            <span>${escapeHtml(label)}</span>
            ${options.compact || !description ? "" : `<small>${escapeHtml(description)}</small>`}
          </div>
        `;
      }

      function threadTimelineSignature(items = []) {
        const timelineItems = Array.isArray(items) ? items : [];
        return JSON.stringify(timelineItems.map((item) => {
          if (item?.type === "message") {
            return {
              type: item.type,
              direction: item.direction,
              body: item.body || "",
              provider_message_sid: item.provider_message_sid || "",
              delivery_status: item.delivery?.status || "",
              delivery_severity: item.delivery?.severity || "",
              created_at: item.created_at || "",
              attachments: (item.attachments || []).map((attachment) => ({
                id: attachment.id,
                url: attachment.url || "",
                media_kind: attachment.media_kind || "",
                size_bytes: attachment.size_bytes || 0,
              })),
            };
          }
          return {
            type: item?.type || "",
            body: item?.body || "",
            title: item?.title || "",
            previous_state: item?.previous_state || "",
            new_state: item?.new_state || "",
            previous_stage: item?.previous_stage || "",
            new_stage: item?.new_stage || "",
            reason: item?.reason || "",
            created_at: item?.created_at || "",
          };
        }));
      }

      function renderBadge(label, tone = "") {
        const rawLabel = formatCrmStageDisplay(label);
        const text = typeof translateTextValue === "function" ? translateTextValue(rawLabel) : rawLabel;
        return `<span class="badge ${tone}">${escapeHtml(text)}</span>`;
      }

      function renderTag(label, tone = "") {
        if (isDeprecatedUiTag(label)) return "";
        return `<span class="tag ${tone || tagTone(label)}">${escapeHtml(formatTagLabel(label))}</span>`;
      }

      function renderStatePill(label, tone = "") {
        const text = formatConversationStateLabel(label);
        return `<span class="state-pill ${tone}">${escapeHtml(typeof translateTextValue === "function" ? translateTextValue(text) : text)}</span>`;
      }

      function renderDataAttributes(attrs = {}) {
        return Object.entries(attrs)
          .filter(([, value]) => value !== undefined && value !== null && value !== false)
          .map(([key, value]) => `${escapeHtml(key)}="${escapeHtml(value)}"`)
          .join(" ");
      }

      function renderEmptyState(message, actions = [], options = {}) {
        const title = typeof translateTextValue === "function" ? translateTextValue(message) : message;
        const detail = options.detail
          ? (typeof translateTextValue === "function" ? translateTextValue(options.detail) : options.detail)
          : "";
        const actionMarkup = actions.length
          ? `<div class="empty-state-actions">${actions.map((action) => `
              <button
                type="button"
                class="small ${action.className || "ghost"}"
                ${renderDataAttributes(action.attrs || {})}
              >${escapeHtml(typeof translateTextValue === "function" ? translateTextValue(action.label) : action.label)}</button>
            `).join("")}</div>`
          : "";
        return `
          <div class="empty-state empty-state-rich ${options.compact ? "compact" : ""}">
            <div class="empty-state-title">${escapeHtml(title)}</div>
            ${detail ? `<div class="empty-state-detail">${escapeHtml(detail)}</div>` : ""}
            ${actionMarkup}
          </div>
        `;
      }

      function formatConversationTransitionReason(rawReason) {
        const key = String(rawReason || "").trim().toLowerCase();
        const labels = {
          "agent_transition": "Updated automatically from the latest conversation step.",
          "calendar_booking_created": "Updated automatically after the meeting was booked.",
          "stop keyword": "Contact requested to stop messages.",
          "admin_booking_link_sent": "Updated after a booking link was sent manually.",
          "admin_marked_handoff": "Marked for human follow-up by admin.",
          "portal_marked_handoff": "Marked for human follow-up by client owner.",
          "ui_simulated_initial_ai_sms": "Created by the simulation starter flow.",
          "initial_ai_sms_sent": "Updated when the first AI message was sent.",
          "initial_sms_sent": "Updated when the first message was sent.",
          "after_hours_initial_sms_sent": "Updated when after-hours first message was sent.",
        };
        return labels[key] || formatFormKey(rawReason || "state change");
      }

      function formatCrmTransitionReason(rawReason) {
        const key = String(rawReason || "").trim().toLowerCase();
        const labels = {
          "meaningful_inbound": "Contact sent a meaningful response.",
          "outbound_sms_sent": "Pipeline advanced after outbound response.",
          "booking_confirmed": "Pipeline advanced after booking confirmation.",
          "initial_outbound_sms": "Pipeline advanced after first outreach.",
          "booking_link_sent": "Pipeline advanced after booking link send.",
          "follow_up_sms_sent": "Pipeline advanced after follow-up send.",
          "opt_out_stop_keyword": "Pipeline moved to Lost after STOP.",
          "ui_simulated_initial_outbound": "Pipeline advanced by simulation flow.",
        };
        return labels[key] || formatFormKey(rawReason || "crm stage");
      }

      function renderThreadTimelineEvent(kindClass, label, detail) {
        return `
          <div class="timeline-event ${kindClass}">
            <div class="timeline-event-label">${escapeHtml(label)}</div>
            <div class="timeline-event-detail">${escapeHtml(detail)}</div>
          </div>
        `;
      }

      function showNotice(message = "", tone = "info") {
        const el = document.getElementById("globalNotice");
        if (state.noticeTimer) {
          window.clearTimeout(state.noticeTimer);
          state.noticeTimer = null;
        }
        if (!message) {
          el.className = "notice";
          el.textContent = "";
          return;
        }
        el.className = `notice show ${tone}`;
        const visibleMessage = typeof translateTextValue === "function" ? translateTextValue(message) : message;
        el.textContent = visibleMessage;
        const delay = tone === "err" ? 6500 : tone === "warn" ? 5000 : 3200;
        state.noticeTimer = window.setTimeout(() => {
          if (el.textContent === visibleMessage) {
            el.className = "notice";
            el.textContent = "";
          }
          state.noticeTimer = null;
        }, delay);
      }

      function closeConfirmPopover(result = false) {
        const existing = document.querySelector(".confirm-popover");
        if (existing) existing.remove();
        if (typeof state.pendingConfirmResolve === "function") {
          const resolve = state.pendingConfirmResolve;
          state.pendingConfirmResolve = null;
          resolve(Boolean(result));
        }
      }

      function confirmAction({ title = "Please confirm", message = "", confirmText = "Confirm", cancelText = "Cancel", tone = "warn" } = {}) {
        closeConfirmPopover(false);
        return new Promise((resolve) => {
          state.pendingConfirmResolve = resolve;
          const popover = document.createElement("div");
          popover.className = `confirm-popover ${tone}`;
          popover.setAttribute("role", "alertdialog");
          popover.setAttribute("aria-modal", "false");
          popover.setAttribute("aria-labelledby", "confirmPopoverTitle");
          popover.innerHTML = `
            <div class="confirm-popover-copy">
              <div id="confirmPopoverTitle" class="confirm-popover-title">${escapeHtml(typeof translateTextValue === "function" ? translateTextValue(title) : title)}</div>
              <div class="confirm-popover-message">${escapeHtml(typeof translateTextValue === "function" ? translateTextValue(message) : message)}</div>
            </div>
            <div class="confirm-popover-actions">
              <button type="button" class="small ghost" data-confirm-cancel>${escapeHtml(typeof translateTextValue === "function" ? translateTextValue(cancelText) : cancelText)}</button>
              <button type="button" class="small ${tone === "err" ? "warn" : "primary"}" data-confirm-ok>${escapeHtml(typeof translateTextValue === "function" ? translateTextValue(confirmText) : confirmText)}</button>
            </div>
          `;
          document.body.appendChild(popover);
          const cancelButton = popover.querySelector("[data-confirm-cancel]");
          const confirmButton = popover.querySelector("[data-confirm-ok]");
          const keyHandler = (event) => {
            if (event.key === "Escape") {
              event.preventDefault();
              document.removeEventListener("keydown", keyHandler);
              closeConfirmPopover(false);
            }
          };
          document.addEventListener("keydown", keyHandler);
          cancelButton.addEventListener("click", () => {
            document.removeEventListener("keydown", keyHandler);
            closeConfirmPopover(false);
          });
          confirmButton.addEventListener("click", () => {
            document.removeEventListener("keydown", keyHandler);
            closeConfirmPopover(true);
          });
          window.requestAnimationFrame(() => cancelButton.focus());
        });
      }

      function selectedClient() {
        return state.clients.find((client) => client.client_key === state.selectedClientKey) || null;
      }

      function updateLoginCopy() {
        const isAdmin = state.authMode !== "client";
        setText("loginTitleHeading", isAdmin ? "Lead Ops Console" : "Client Portal");
        setText(
          "loginTitleSubtitle",
          isAdmin
            ? "Clean operations workspace for admin oversight and client lead handling."
            : "Review contacts, manage follow-ups, and update your assistant settings."
        );
      }

      function activeViewLabel() {
        const labels = {
          dashboard: "Dashboard",
          clients: "Clients",
          conversations: "Inbox",
          crm: "Pipeline",
          leads: "Records",
          calendar: "Calendar",
          tasks: "Tasks",
          logs: "Logs",
          settings: "Settings",
          "test-lab": "Test Lab",
        };
        const label = labels[state.activeView] || "Workspace";
        return typeof translateTextValue === "function" ? translateTextValue(label) : label;
      }

      function updateChromeContext() {
        const client = selectedClient();
        const clientName = state.ownerWorkspace?.client?.business_name || state.session?.client_name || client?.business_name || "Client Portal";
        const timezone = state.ownerWorkspace?.client?.timezone || client?.timezone || "";
        const viewLabel = activeViewLabel();
        if (isClientRole()) {
          setText("chromeTitle", clientName);
          setText("chromeSubtitle", timezone ? `${viewLabel} · ${timezone}` : viewLabel);
          document.title = `${clientName} Client Portal`;
          return;
        }
        setText("chromeTitle", "lead-ops-console");
        setText("chromeSubtitle", client ? `${viewLabel} · ${client.business_name}` : `${viewLabel} · admin workspace`);
        document.title = "Lead Ops Console";
      }

      function themeLabel() {
        return state.theme === "dark" ? "dark" : "light";
      }

      function setTheme(theme) {
        state.theme = theme;
        document.body.dataset.theme = theme;
        saveLocalState();
      }

      function toggleTheme() {
        setTheme(state.theme === "dark" ? "light" : "dark");
      }

      function applySidebarState() {
        document.getElementById("sidebar").classList.toggle("collapsed", state.sidebarCollapsed);
      }

      function toggleSidebar() {
        state.sidebarCollapsed = !state.sidebarCollapsed;
        applySidebarState();
        saveLocalState();
      }

      function applyLoginMode() {
        const isAdmin = state.authMode !== "client";
        document.getElementById("loginAdminFields").classList.toggle("hidden", !isAdmin);
        document.getElementById("loginClientFields").classList.toggle("hidden", isAdmin);
        document.getElementById("loginModeAdmin").classList.toggle("active", isAdmin);
        document.getElementById("loginModeClient").classList.toggle("active", !isAdmin);
        updateLoginCopy();
      }

      function setLoginMode(mode) {
        state.authMode = mode === "client" ? "client" : "admin";
        applyLoginMode();
        saveLocalState();
      }

      function lockUi(message = "Sign in to continue.") {
        document.getElementById("loginOverlay").classList.remove("hidden");
        setText("loginStatus", message);
      }

      async function authenticateAdmin() {
        state.authMode = "admin";
        const token = document.getElementById("loginToken").value.trim();
        if (!token) {
          setText("loginStatus", "Admin token is required.");
          return;
        }
        try {
          const result = await fetch("/ui/api/login/admin", {
            method: "POST",
            credentials: "same-origin",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ admin_token: token }),
          });
          const payload = await result.json();
          if (!result.ok) throw new Error(payload.detail || "Login failed");
          clearAdminToken();
          clearPortalToken();
          document.getElementById("loginToken").value = "";
          await bootstrap();
          document.getElementById("loginOverlay").classList.add("hidden");
          setText("loginStatus", "");
          showNotice("Session unlocked.", "ok");
        } catch (error) {
          clearAdminToken();
          document.getElementById("loginOverlay").classList.remove("hidden");
          setText("loginStatus", `Login failed: ${error.message}`);
        }
      }

      async function authenticateClient() {
        state.authMode = "client";
        const email = document.getElementById("clientLoginEmail").value.trim();
        const password = document.getElementById("clientLoginPassword").value;
        if (!email || !password) {
          setText("loginStatus", "Client email and password are required.");
          return;
        }
        clearAdminToken();
        try {
          const result = await fetch("/ui/api/login/client", {
            method: "POST",
            credentials: "same-origin",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ email, password }),
          });
          const payload = await result.json();
          if (!result.ok) {
            throw new Error(payload.detail || "Login failed");
          }
          clearPortalToken();
          localStorage.setItem("lead-ui-portal-email", email);
          await bootstrap();
          document.getElementById("loginOverlay").classList.add("hidden");
          setText("loginStatus", "");
          document.getElementById("clientLoginPassword").value = "";
          showNotice("Client session unlocked.", "ok");
        } catch (error) {
          clearPortalToken();
          document.getElementById("loginOverlay").classList.remove("hidden");
          setText("loginStatus", `Login failed: ${error.message}`);
        }
      }

      async function authenticate() {
        if (state.authMode === "client") {
          await authenticateClient();
          return;
        }
        await authenticateAdmin();
      }

      function isLocalLike(url) {
        if (!url) return true;
        if (url.startsWith("/")) return true;
        try {
          const parsed = new URL(url);
          return ["localhost", "127.0.0.1", "0.0.0.0"].includes(parsed.hostname);
        } catch (_) {
          return true;
        }
      }

      function selectedClientPublicBaseUrl() {
        const detailBase = state.clientDetail?.client?.provider_config?.public_base_url || "";
        const ownerBase = state.ownerWorkspace?.client?.provider_config?.public_base_url || "";
        return detailBase || ownerBase;
      }

      function absoluteUrl(path, preferredBase = "") {
        if (!path) return "";
        if (path.startsWith("http://") || path.startsWith("https://")) return path;
        const base = (preferredBase || selectedClientPublicBaseUrl() || "").trim().replace(/\/$/, "");
        return base ? `${base}${path}` : path;
      }

      function renderWebhookRows(rows) {
        if (!rows.length) return '<div class="empty-state">No webhook URLs available.</div>';
        return rows.map((row) => `
          <div class="webhook-row">
            <div class="item-title-row">
              <div>
                <div class="item-title">${escapeHtml(row.label)}</div>
                <div class="item-snippet mono">${escapeHtml(row.value)}</div>
              </div>
              <div class="actions">
                <button class="small ghost" data-action="copy" data-copy="${escapeHtml(row.value)}">Copy</button>
              </div>
            </div>
          </div>
        `).join("");
      }

      function formatFormKey(rawKey) {
        const cleaned = String(rawKey || "")
          .replaceAll("_", " ")
          .replace(/[?¿]/g, "")
          .trim()
          .replace(/\s+/g, " ");
        const canonicalFrench = {
          "quel service vous interesse": "Quel service vous intéresse",
          "quel service vous int resse": "Quel service vous intéresse",
          "quelle est votre situation actuelle": "Quelle est votre situation actuelle",
          "quel livrable souhaitez vous obtenir": "Quel livrable souhaitez-vous obtenir",
          "quel livrable souhaitez-vous obtenir": "Quel livrable souhaitez-vous obtenir",
          "quelle est l urgence du projet": "Quelle est l’urgence du projet",
          "quelle est lurgence du projet": "Quelle est l’urgence du projet",
        };
        const normalized = cleaned
          .normalize("NFD")
          .replace(/[\u0300-\u036f]/g, "")
          .replace(/[’']/g, " ")
          .replace(/\s+/g, " ")
          .trim()
          .toLowerCase();
        return canonicalFrench[normalized] || cleaned;
      }

      function formatFormValue(value) {
        if (value == null) return "-";
        if (Array.isArray(value)) return value.map((item) => String(item)).join(", ");
        if (typeof value === "object") return JSON.stringify(value);
        return String(value);
      }

      function formatSummaryLabel(value) {
        const text = String(value || "-").trim();
        if (!text) return "-";
        const locale = typeof uiLocale === "function" ? uiLocale() : "fr-CA";
        return text.toLocaleUpperCase(locale);
      }

      function summarizeText(value, maxLen = 180) {
        const text = String(value || "").replace(/\s+/g, " ").trim();
        if (!text) return "";
        return text.length <= maxLen ? text : `${text.slice(0, maxLen - 1).trim()}...`;
      }

      function isDuplicateLeadNameSummaryLine(line, leadName = "") {
        const label = String(line?.label || "").trim().toLowerCase();
        const value = String(line?.value || "").trim().toLowerCase();
        const normalizedLeadName = String(leadName || "").trim().toLowerCase();
        const compactLabel = label.replace(/\s+/g, " ");
        const isNameLabel = compactLabel.includes("name") || compactLabel === "lead" || compactLabel === "contact";
        if (!isNameLabel) return false;
        if (!normalizedLeadName) return true;
        const normalizedValue = value.replace(/\s+/g, " ").trim();
        return normalizedValue === normalizedLeadName || normalizedValue.includes(normalizedLeadName);
      }

      function summaryRowsFromFormAnswers(formAnswers = {}, leadName = "") {
        const excludedKeys = new Set(["created_from", "source"]);
        return Object.entries(formAnswers || {})
          .filter(([key, value]) => {
            if (excludedKeys.has(String(key || "").trim().toLowerCase())) return false;
            if (value == null) return false;
            if (typeof value === "string" && !value.trim()) return false;
            return true;
          })
          .map(([key, value]) => ({ label: formatFormKey(key), value: formatFormValue(value) }))
          .filter((line) => !isDuplicateLeadNameSummaryLine(line, leadName));
      }

      function normalizedSummaryLabelKey(value) {
        return formatFormKey(value)
          .normalize("NFD")
          .replace(/[\u0300-\u036f]/g, "")
          .replace(/[’'`_-]/g, " ")
          .replace(/[^a-z0-9\s]/gi, "")
          .replace(/\s+/g, " ")
          .trim()
          .toLowerCase();
      }

      function uniqueSummaryRows(rows = []) {
        const seen = new Set();
        return rows.filter((row) => {
          const key = normalizedSummaryLabelKey(row?.label || "");
          if (!key || seen.has(key)) return false;
          seen.add(key);
          return true;
        });
      }

      function mergeSummaryRows(summaryLines = [], formAnswers = {}, leadName = "") {
        const normalizedSummaryRows = uniqueSummaryRows((summaryLines || [])
          .filter((line) => !isDuplicateLeadNameSummaryLine(line, leadName))
          .filter((line) => String(line?.value || "").trim() !== ""));
        const normalizedFormRows = uniqueSummaryRows(summaryRowsFromFormAnswers(formAnswers, leadName));

        if (!normalizedFormRows.length) {
          return normalizedSummaryRows;
        }

        const seenLabels = new Set(normalizedFormRows.map((row) => normalizedSummaryLabelKey(row.label)));
        const supplementalSummaryRows = normalizedSummaryRows.filter((row) => {
          const key = normalizedSummaryLabelKey(row.label);
          if (!key || seenLabels.has(key)) return false;
          seenLabels.add(key);
          return true;
        });
        return [...normalizedFormRows, ...supplementalSummaryRows];
      }

      function renderCopyableHeaderValue(displayValue, copyValue, label = "value") {
        const visible = String(displayValue || "").trim();
        const rawCopy = String(copyValue || "").trim();
        if (!visible || !rawCopy) return escapeHtml(visible || "");
        return `<button type="button" class="copy-inline" data-action="copy" data-copy="${escapeHtml(rawCopy)}">${escapeHtml(visible)}<span class="copy-inline-hint">Copy</span></button>`;
      }

      function renderSummaryFacts(lines) {
        return (lines || []).map((line) => `
          <div class="summary-line" data-i18n-skip="true">
            <div class="summary-line-label">${escapeHtml(formatSummaryLabel(line.label || "-"))}</div>
            <div class="summary-line-value">${escapeHtml(line.value || "-")}</div>
          </div>
        `).join("");
      }

      function formatIntentReason(reason) {
        const labels = {
          clear_service_or_request_need: "Clear service or request need",
          specific_form_scope: "Specific scope in form answers",
          some_form_context: "Some form context",
          timeline_provided: "Timeline provided",
          urgent_timeline: "Urgent timeline",
          decision_path_known: "Decision path known",
          decision_maker_signal: "Decision-maker signal",
          location_context: "Location context",
          meaningful_size_or_scope: "Meaningful size or scope",
          pricing_question: "Pricing question",
          scheduling_intent: "Scheduling intent",
          buying_signal: "Buying signal",
          low_intent_language: "Low-intent language",
          generic_question: "Generic question",
        };
        return labels[reason] || formatFormKey(reason || "signal");
      }

      function intentTone(intentLevel) {
        const level = String(intentLevel || "").toUpperCase();
        if (level.includes("HIGH")) return "ok";
        if (level.includes("MEDIUM")) return "info";
        if (level.includes("LOW")) return "warn";
        return "";
      }

      function renderAgentInsights(insights = {}) {
        const reasons = Array.isArray(insights.intent_reasons) ? insights.intent_reasons : [];
        const missing = Array.isArray(insights.important_missing_fields) ? insights.important_missing_fields : [];
        const rows = [];
        if (insights.intent_score != null && insights.intent_score !== "") {
          rows.push({ label: "Intent score", value: String(insights.intent_score) });
        }
        if (reasons.length) {
          rows.push({
            label: "Why this intent",
            value: reasons.map(formatIntentReason).join(", "),
          });
        }
        if (insights.meeting_status) {
          rows.push({ label: "Meeting status", value: formatFormKey(insights.meeting_status) });
        }
        if (insights.meeting_suggested_count) {
          rows.push({ label: "Meeting suggestions", value: String(insights.meeting_suggested_count) });
        }
        if (missing.length) {
          rows.push({
            label: "Useful missing info",
            value: missing.slice(0, 3).map((item) => item.label || formatFormKey(item.key || "")).filter(Boolean).join(", "),
          });
        }
        if (insights.recommended_follow_up) {
          rows.push({ label: "Recommended follow-up", value: insights.recommended_follow_up });
        }
        return rows.length ? renderSummaryFacts(rows) : '<div class="empty-state">No AI reasoning captured yet.</div>';
      }

      function formatAuditEventLabel(eventType) {
        const labels = {
          agent_decision: "AI response",
          crm_stage_changed: "CRM stage updated",
          crm_stage_auto_updated: "CRM stage auto-updated",
          internal_note: "Internal note",
          admin_booking_link_sent: "Booking link sent",
          portal_booking_link_sent: "Booking link sent",
          calendar_booking_offer_sent: "Calendar times offered",
          calendar_booking_created: "Calendar booking created",
          booking_confirmed: "Booking confirmed",
          crm_task_created: "Task created",
          crm_task_completed: "Task completed",
          crm_task_reopened: "Task reopened",
          crm_task_updated: "Task updated",
          manual_outbound_sent: "Manual message sent",
          portal_manual_outbound_sent: "Manual message sent",
          admin_marked_handoff: "Marked for handoff",
          portal_marked_handoff: "Marked for handoff",
          conversation_archived: "Archived from inbox",
          conversation_unarchived: "Restored to inbox",
          rate_limited: "Rate limit applied",
        };
        return labels[eventType] || formatFormKey(eventType);
      }

      function auditDetailLines(event) {
        const decision = event?.decision && typeof event.decision === "object" ? event.decision : {};
        const eventType = event?.event_type || "";
        const lines = [];
        if (eventType === "crm_stage_changed" || eventType === "crm_stage_auto_updated") {
          const from = decision.previous_stage || "-";
          const to = decision.new_stage || "-";
          lines.push(`${t("Stage")}: ${formatCrmStageDisplay(from)} -> ${formatCrmStageDisplay(to)}`);
          if (decision.reason) lines.push(`${t("Reason")}: ${formatFormKey(decision.reason)}`);
          if (decision.inbound) lines.push(`${t("Contact message")}: "${summarizeText(decision.inbound, 120)}"`);
          return lines;
        }
        if (eventType === "agent_decision") {
          if (decision.inbound) lines.push(`${t("Contact message")}: "${summarizeText(decision.inbound, 120)}"`);
          if (decision.outbound) lines.push(`${t("AI reply")}: "${summarizeText(decision.outbound, 140)}"`);
          if (decision.next_state) lines.push(`${t("Conversation state")}: ${t(formatConversationStateLabel(decision.next_state))}`);
          if (decision.provider) lines.push(`${t("Provider")}: ${String(decision.provider).toUpperCase()}`);
          return lines;
        }
        if (eventType === "internal_note") {
          if (decision.note) lines.push(summarizeText(decision.note, 220));
          if (decision.actor_label) lines.push(`By: ${decision.actor_label}`);
          return lines;
        }
        if (eventType === "crm_task_created" || eventType === "crm_task_updated" || eventType === "crm_task_completed" || eventType === "crm_task_reopened") {
          if (decision.title) lines.push(`Task: ${decision.title}`);
          if (decision.new_status) lines.push(`Status: ${decision.new_status}`);
          if (decision.due_date) lines.push(`Due: ${decision.due_date}`);
          return lines;
        }
        if (eventType === "manual_outbound_sent" || eventType === "portal_manual_outbound_sent" || eventType === "admin_booking_link_sent" || eventType === "portal_booking_link_sent") {
          if (decision.body) lines.push(`Message: "${summarizeText(decision.body, 160)}"`);
          if (decision.provider_sid && !isClientRole()) lines.push(`Provider id: ${decision.provider_sid}`);
          return lines;
        }
        if (eventType === "conversation_archived" || eventType === "conversation_unarchived") {
          if (decision.actor_role) lines.push(`By: ${decision.actor_role === "client" ? "Client owner" : "Admin"}`);
          return lines;
        }
        if (eventType === "rate_limited" && decision.inbound) {
          lines.push(`Inbound message: "${summarizeText(decision.inbound, 120)}"`);
          return lines;
        }
        Object.entries(decision).slice(0, 3).forEach(([key, value]) => {
          if (value == null || value === "") return;
          lines.push(`${formatFormKey(key)}: ${summarizeText(formatFormValue(value), 140)}`);
        });
        return lines;
      }
