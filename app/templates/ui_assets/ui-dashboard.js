      function renderConversationClientGuide() {}

      function formatDashboardPercent(value) {
        const percent = Number(value || 0) * 100;
        if (!Number.isFinite(percent)) return "0%";
        const rounded = percent >= 10 ? Math.round(percent) : Number(percent.toFixed(1));
        return `${rounded}%`;
      }

      function formatDashboardNumber(value) {
        const number = Number(value || 0);
        if (!Number.isFinite(number)) return "0";
        return new Intl.NumberFormat(typeof uiLocale === "function" ? uiLocale() : "en-US", { maximumFractionDigits: 0 }).format(number);
      }

      function formatDashboardMoney(value, options = {}) {
        const number = Number(value || 0);
        const maximumFractionDigits = options.compact ? 1 : 2;
        if (!Number.isFinite(number)) return "$0";
        if (options.compact && Math.abs(number) >= 1000) {
          return new Intl.NumberFormat(typeof uiLocale === "function" ? uiLocale() : "en-US", {
            style: "currency",
            currency: "USD",
            notation: "compact",
            maximumFractionDigits,
          }).format(number);
        }
        return new Intl.NumberFormat(typeof uiLocale === "function" ? uiLocale() : "en-US", {
          style: "currency",
          currency: "USD",
          maximumFractionDigits,
        }).format(number);
      }

      function dashboardChartColor(kind, key, index) {
        const sourcePalette = [
          "var(--accent)",
          "var(--success)",
          "var(--warn)",
          "#8b949e",
          "#6e7681",
        ];
        if (kind === "source") {
          const byKey = {
            meta: "var(--accent)",
            linkedin: "#4ac2b7",
            sms: "var(--success)",
            manual: "var(--warn)",
          };
          return byKey[String(key || "").trim().toLowerCase()] || sourcePalette[index % sourcePalette.length];
        }
        const stage = String(key || "").trim();
        const byStage = {
          "New Lead": "#8b949e",
          Contacted: "var(--accent)",
          Qualified: "#4ac2b7",
          "Meeting Booked": "var(--success)",
          "Meeting Completed": "#65a3ff",
          Won: "var(--success)",
          Lost: "var(--warn)",
        };
        return byStage[stage] || sourcePalette[index % sourcePalette.length];
      }

      function renderDashboardTrend(trend) {
        const items = Array.isArray(trend) ? trend : [];
        if (!items.length) return `<div class="dashboard-empty">${escapeHtml(t("No acquisition trend yet."))}</div>`;
        const total = items.reduce((sum, item) => sum + Number(item.count || 0), 0);
        const max = Math.max(...items.map((item) => Number(item.count || 0)), 1);
        return `
          <div class="dashboard-trend-summary">
            <strong>${escapeHtml(total)}</strong>
            <span>${escapeHtml(t("opportunities created in the last 6 weeks"))}</span>
          </div>
          <div class="dashboard-trend-bars">
            ${items.map((item) => {
              const count = Number(item.count || 0);
              const height = Math.max(14, Math.round((count / max) * 108));
              return `
                <div class="dashboard-trend-column">
                  <div class="dashboard-trend-count">${escapeHtml(String(count))}</div>
                  <div class="dashboard-trend-bar-wrap">
                    <div class="dashboard-trend-bar" style="height:${height}px"></div>
                  </div>
                  <div class="dashboard-trend-label">${escapeHtml(formatDateLabel(item.week_start, undefined, { month: "short", day: "numeric" }))}</div>
                </div>
              `;
            }).join("")}
          </div>
        `;
      }

      function renderDashboardGraph(items, kind) {
        const rows = Array.isArray(items) ? items : [];
        if (!rows.length) return `<div class="dashboard-empty">${escapeHtml(t("No source data available yet."))}</div>`;
        const total = rows.reduce((sum, item) => sum + Number(item.count || 0), 0);
        const radius = 50;
        const circumference = 2 * Math.PI * radius;
        let offset = 0;

        const segments = rows.map((item, index) => {
          const share = Number(item.share || 0);
          const dash = share >= 1 ? circumference : Math.max(share * circumference, 0);
          const color = dashboardChartColor(kind, item.key, index);
          const segment = `
            <circle
              class="dashboard-chart-segment"
              cx="62"
              cy="62"
              r="${radius}"
              stroke="${color}"
              stroke-width="16"
              stroke-dasharray="${dash} ${circumference}"
              stroke-dashoffset="${-offset}"
              transform="rotate(-90 62 62)"
            ></circle>
          `;
          offset += dash;
          return segment;
        }).join("");

        const centerLabel = kind === "source" ? t("sources") : t("stages");
        const legend = rows.map((item, index) => {
          const label = kind === "source" ? formatLeadSourceLabel(item.key) : formatCrmStageDisplay(item.key);
          const countLabel = `${item.count} ${item.count === 1 ? t("opportunity") : t("opportunities")}`;
          const share = formatDashboardPercent(item.share);
          const color = dashboardChartColor(kind, item.key, index);
          const content = `
            <div class="dashboard-chart-main">
              <span class="dashboard-chart-swatch" style="background:${color}"></span>
              <span class="dashboard-chart-label">${escapeHtml(label)}</span>
            </div>
            <div class="dashboard-chart-stats">
              <span class="dashboard-chart-share">${escapeHtml(share)}</span>
              <span class="dashboard-chart-count">${escapeHtml(countLabel)}</span>
            </div>
          `;
          return kind === "stage"
            ? `<button type="button" class="dashboard-chart-row dashboard-chart-row-action" data-action="dashboard-open-stage" data-stage="${escapeHtml(item.key)}">${content}</button>`
            : `<div class="dashboard-chart-row">${content}</div>`;
        }).join("");

        return `
          <div class="dashboard-chart-shell">
            <div class="dashboard-chart-art">
              <div class="dashboard-chart-figure">
                <svg class="dashboard-chart-svg" viewBox="0 0 124 124" aria-hidden="true">
                  <circle class="dashboard-chart-track" cx="62" cy="62" r="${radius}" fill="none" stroke-width="16"></circle>
                  ${segments}
                </svg>
                <div class="dashboard-chart-center">
                  <div class="dashboard-chart-total">${escapeHtml(String(total))}</div>
                  <div class="dashboard-chart-caption">${escapeHtml(t("total records"))}</div>
                  <div class="dashboard-chart-subcaption">${escapeHtml(`${rows.length} ${centerLabel}`)}</div>
                </div>
              </div>
            </div>
            <div class="dashboard-chart-legend">${legend}</div>
          </div>
        `;
      }

      function renderDashboardCampaignPerformance(performance) {
        const campaigns = Array.isArray(performance?.campaigns) ? performance.campaigns : [];
        const totals = performance?.totals || {};
        if (!campaigns.length) {
          return `<div class="dashboard-empty">${escapeHtml(t("No ad campaign report imported yet."))}</div>`;
        }
        const maxConversions = Math.max(...campaigns.map((item) => Number(item.conversions || 0)), 1);
        const funnelSteps = [
          {
            label: t("Impressions"),
            value: formatDashboardNumber(totals.impressions),
            fill: 100,
            color: "var(--accent)",
          },
          {
            label: t("Clicks"),
            value: formatDashboardNumber(totals.clicks),
            fill: Math.max(8, Math.round(Number(totals.ctr || 0) * 1000)),
            color: "#4ac2b7",
          },
          {
            label: t("Conversions"),
            value: formatDashboardNumber(totals.conversions),
            fill: Math.max(8, Math.round(Number(totals.conversion_rate || 0) * 100)),
            color: "var(--success)",
          },
        ];
        const kpis = [
          {
            label: t("Spend"),
            value: formatDashboardMoney(totals.spend, { compact: true }),
            meta: `${formatDashboardNumber(totals.campaigns)} ${t("campaigns")}`,
          },
          {
            label: "CPC",
            value: formatDashboardMoney(totals.cpc),
            meta: `${formatDashboardPercent(totals.ctr)} CTR`,
          },
          {
            label: t("Cost / conversion"),
            value: formatDashboardMoney(totals.cost_per_conversion),
            meta: `${formatDashboardNumber(totals.conversions)} ${t("conversions")}`,
          },
          {
            label: t("Reach"),
            value: formatDashboardNumber(totals.reach),
            meta: performance.report_range || t("Last 30 days"),
          },
        ];
        const campaignRows = campaigns.slice(0, 3).map((campaign) => {
          const width = Math.max(5, Math.round((Number(campaign.conversions || 0) / maxConversions) * 100));
          return `
            <div class="dashboard-campaign-row">
              <div class="dashboard-campaign-row-head">
                <div class="dashboard-campaign-name">${escapeHtml(campaign.campaign_name || t("Untitled campaign"))}</div>
                ${renderBadge(campaign.status || "active", campaign.status === "learning" ? "warn" : "ok")}
              </div>
              <div class="dashboard-campaign-metrics">
                <div class="dashboard-campaign-metric"><strong>${escapeHtml(formatDashboardNumber(campaign.conversions))}</strong> ${escapeHtml(t("conversions"))}</div>
                <div class="dashboard-campaign-metric"><strong>${escapeHtml(formatDashboardMoney(campaign.cost_per_conversion))}</strong> ${escapeHtml(t("cost / conversion"))}</div>
                <div class="dashboard-campaign-metric"><strong>${escapeHtml(formatDashboardMoney(campaign.cpc))}</strong> CPC</div>
                <div class="dashboard-campaign-metric"><strong>${escapeHtml(formatDashboardNumber(campaign.clicks))}</strong> ${escapeHtml(t("clicks"))}</div>
              </div>
              <div class="dashboard-campaign-bar" aria-hidden="true">
                <div class="dashboard-campaign-bar-fill" style="width:${width}%"></div>
              </div>
            </div>
          `;
        }).join("");
        return `
          <div class="dashboard-campaign-kpis">
            ${kpis.map((item) => `
              <div class="dashboard-campaign-kpi">
                <div class="dashboard-campaign-kpi-label">${escapeHtml(item.label)}</div>
                <div class="dashboard-campaign-kpi-value">${escapeHtml(item.value)}</div>
                <div class="dashboard-campaign-kpi-meta">${escapeHtml(item.meta)}</div>
              </div>
            `).join("")}
          </div>
          <div class="dashboard-campaign-funnel" aria-label="${escapeHtml(t("Ad campaign funnel"))}">
            ${funnelSteps.map((item) => `
              <div class="dashboard-campaign-step" style="--step-fill:${item.fill}%;--step-color:${item.color}">
                <div class="dashboard-campaign-step-label">${escapeHtml(item.label)}</div>
                <div class="dashboard-campaign-step-value">${escapeHtml(item.value)}</div>
              </div>
            `).join("")}
          </div>
          <div class="dashboard-campaign-list">${campaignRows}</div>
        `;
      }

      function renderDashboardStageChart(items) {
        const rows = Array.isArray(items) ? items : [];
        if (!rows.length) return `<div class="dashboard-empty">${escapeHtml(t("No pipeline data available yet."))}</div>`;
        const total = rows.reduce((sum, item) => sum + Number(item.count || 0), 0);
        const max = Math.max(...rows.map((item) => Number(item.count || 0)), 1);
        const stack = rows.map((item, index) => {
          const width = Math.max(3, Math.round(Number(item.share || 0) * 100));
          const color = dashboardChartColor("stage", item.key, index);
          return `<span class="dashboard-stage-stack-segment" style="width:${width}%;background:${color}"></span>`;
        }).join("");
        const legend = rows.map((item, index) => {
          const color = dashboardChartColor("stage", item.key, index);
          const width = Math.max(6, Math.round((Number(item.count || 0) / max) * 100));
          return `
            <div class="dashboard-stage-row">
              <div class="dashboard-stage-row-top">
                <div class="dashboard-chart-main">
                  <span class="dashboard-chart-swatch" style="background:${color}"></span>
                  <span class="dashboard-chart-label">${escapeHtml(formatCrmStageDisplay(item.key))}</span>
                </div>
                <div class="dashboard-chart-stats">
                  <span class="dashboard-chart-share">${escapeHtml(formatDashboardPercent(item.share))}</span>
                  <span class="dashboard-chart-count">${escapeHtml(`${item.count} ${item.count === 1 ? t("record") : t("records")}`)}</span>
                </div>
              </div>
              <div class="dashboard-stage-bar-track">
                <div class="dashboard-stage-bar-fill" style="width:${width}%;background:${color}"></div>
              </div>
            </div>
          `;
        }).join("");
        return `
          <div class="dashboard-stage-chart">
            <div class="dashboard-stage-summary">
              <div class="dashboard-stage-summary-copy">
                <div class="dashboard-chart-total">${escapeHtml(String(total))}</div>
                <div class="dashboard-chart-caption">${escapeHtml(t("total records in pipeline"))}</div>
              </div>
            </div>
            <div class="dashboard-stage-stack">${stack}</div>
            <div class="dashboard-stage-rows">${legend}</div>
          </div>
        `;
      }

      function renderDashboardLatestLeads(leads, stats) {
        const items = Array.isArray(leads) ? leads : [];
        const newLast24Hours = Number(stats?.new_last_24_hours || 0);
        const newLast7Days = Number(stats?.new_last_7_days || 0);
        const newestLead = items[0] || null;
        setText(
          "dashboardLatestLeadsSubtitle",
          newLast24Hours
            ? `${newLast24Hours} new opportunit${newLast24Hours === 1 ? "y" : "ies"} in the last 24 hours.`
            : "No new opportunities in the last 24 hours."
        );
        document.getElementById("dashboardLatestLeadsSummary").innerHTML = [
          {
            label: "Last 24h",
            value: newLast24Hours,
            meta: newLast24Hours ? t("Fresh demand since yesterday.") : t("No fresh demand overnight."),
            className: "",
          },
          {
            label: t("Last 7 days"),
            value: newLast7Days,
            meta: `${items.length} ${t(items.length === 1 ? "most recent record shown below." : "most recent records shown below.")}`,
            className: "",
          },
          {
            label: t("Latest arrival"),
            value: newestLead ? formatLongDateTime(newestLead.created_at) : t("No records yet"),
            meta: newestLead ? `${newestLead.lead_name || newestLead.phone || t("Contact")} · ${formatLeadSourceLabel(newestLead.source)}` : t("New records will appear here."),
            className: newestLead ? "time" : "",
          },
        ].map((item) => `
          <div class="dashboard-lead-stat">
            <div class="dashboard-lead-stat-label">${escapeHtml(item.label)}</div>
            <div class="dashboard-lead-stat-value ${item.className}">${escapeHtml(String(item.value))}</div>
            <div class="dashboard-lead-stat-meta">${escapeHtml(item.meta)}</div>
          </div>
        `).join("");
        const q = state.globalSearch.trim().toLowerCase();
        const filtered = items
          .filter((item) => !q || JSON.stringify(item).toLowerCase().includes(q))
          .slice(0, 5);
        document.getElementById("dashboardLatestLeads").innerHTML = filtered.length
          ? filtered.map((item) => `
              <button type="button" class="dashboard-lead-row dashboard-lead-row-action" data-action="open-thread" data-lead-id="${item.lead_id}">
                <div class="dashboard-lead-card" style="--lead-accent:${dashboardChartColor("source", item.source, 0)}">
                  <div class="dashboard-lead-top">
                    <div style="min-width:0;">
                      <div class="dashboard-lead-context">
                        ${item.crm_stage ? renderBadge(item.crm_stage, crmStageTone(item.crm_stage)) : ""}
                        ${new Date(item.created_at).getTime() >= (Date.now() - (24 * 60 * 60 * 1000)) ? renderBadge("new", "info") : ""}
                      </div>
                      <div class="dashboard-lead-name">${escapeHtml(item.lead_name || item.phone || `${t("Contact")} ${item.lead_id}`)}</div>
                      <div class="dashboard-lead-meta">
                        ${escapeHtml([formatLeadSourceLabel(item.source), item.phone || "", !isClientRole() ? (item.client_name || "") : ""].filter(Boolean).join(" · ") || t("No contact details"))}
                      </div>
                    </div>
                    <div class="dashboard-lead-time">
                      ${escapeHtml(t("Acquired"))}
                      <strong>${escapeHtml(formatDateTime(item.created_at))}</strong>
                    </div>
                  </div>
                </div>
              </button>
            `).join("")
          : `<div class="dashboard-empty">${escapeHtml(t("No records match the current search."))}</div>`;
      }

      function renderDashboardUpcoming(upcoming) {
        const meetings = upcoming?.meetings || [];
        const tasks = upcoming?.tasks || [];
        if (!meetings.length && !tasks.length) {
          return `<div class="dashboard-empty">${escapeHtml(t("No upcoming meetings or open tasks right now."))}</div>`;
        }
        const meetingRows = meetings.map((item) => `
          <div class="dashboard-agenda-item">
            <div class="dashboard-agenda-head">
              <div class="dashboard-agenda-copy">
                <div class="dashboard-agenda-title">${escapeHtml(item.lead_name || item.title || t("Booked meeting"))}</div>
                <div class="dashboard-agenda-meta">${escapeHtml(`${formatLongDateTime(item.start_at)} · ${formatTimeInTimeZone(item.end_at, item.timezone)}`)}</div>
              </div>
              ${renderBadge("meeting", "info")}
            </div>
            <div class="dashboard-agenda-meta">${escapeHtml([item.phone || "", !isClientRole() ? (item.client_name || "") : ""].filter(Boolean).join(" · ") || t("No contact details"))}</div>
          </div>
        `).join("");
        const taskRows = tasks.map((item) => `
          <div class="dashboard-agenda-item">
            <div class="dashboard-agenda-head">
              <div class="dashboard-agenda-copy">
                <div class="dashboard-agenda-title">${escapeHtml(item.title || t("Task"))}</div>
                <div class="dashboard-agenda-meta">${escapeHtml([item.lead_name || t("Contact"), item.due_date ? formatDateLabel(item.due_date) : t("No due date")].join(" · "))}</div>
              </div>
              ${renderBadge(item.status || "open", item.status === "done" ? "ok" : "warn")}
            </div>
            <div class="dashboard-agenda-meta">${escapeHtml(item.description || t("No extra details."))}</div>
          </div>
        `).join("");
        return `${meetingRows}${taskRows}`;
      }

      function todayTimeZone() {
        return state.calendar?.timezone || state.ownerWorkspace?.client?.timezone || selectedClient()?.timezone || undefined;
      }

      function todayDateKey() {
        return dateKeyInTimeZone(new Date(), todayTimeZone());
      }

      function todayTaskRows(todayKey) {
        return (state.crmTasks?.items || [])
          .filter((task) => task.status !== "done" && (task.due_date === todayKey || (task.due_date && task.due_date < todayKey)))
          .sort((a, b) => {
            const aDue = a.due_date || "9999-12-31";
            const bDue = b.due_date || "9999-12-31";
            if (aDue !== bDue) return aDue.localeCompare(bDue);
            return String(a.title || "").localeCompare(String(b.title || ""));
          })
          .slice(0, 8);
      }

      function todayMeetingRows(todayKey, timeZone) {
        return (state.calendar?.items || [])
          .filter((item) => dateKeyInTimeZone(item.start_at, timeZone) === todayKey)
          .sort((a, b) => String(a.start_at || "").localeCompare(String(b.start_at || "")))
          .slice(0, 8);
      }

      function todayHotRows() {
        return (state.crmLeads?.items || [])
          .filter((item) => !["Won", "Lost"].includes(item.crm_stage || ""))
          .slice()
          .sort((a, b) => {
            const scoreDiff = Number(b.lead_score || 0) - Number(a.lead_score || 0);
            if (scoreDiff) return scoreDiff;
            const valueDiff = Number(b.estimated_value || 0) - Number(a.estimated_value || 0);
            if (valueDiff) return valueDiff;
            return String(b.last_activity_at || "").localeCompare(String(a.last_activity_at || ""));
          })
          .slice(0, 6);
      }

      function todayConversationRows() {
        return (state.conversations?.items || [])
          .slice()
          .sort((a, b) => String(b.last_activity_at || "").localeCompare(String(a.last_activity_at || "")))
          .slice(0, 6);
      }

      function renderTodayTasks(items, todayKey) {
        if (!items.length) {
          return renderEmptyState("No urgent follow-ups for today.", [
            { label: "Open tasks", attrs: { "data-action": "set-view", "data-view": "tasks" } },
          ], { compact: true });
        }
        return items.map((task) => {
          const overdue = task.due_date && task.due_date < todayKey;
          const dueLabel = overdue ? "Overdue" : "Due today";
          return `
            <div class="today-item">
              <div class="today-item-main">
                <div class="today-item-title">${escapeHtml(task.title || t("Task"))}</div>
                <div class="today-item-meta">${escapeHtml([task.lead_name || t("Contact"), task.due_date ? formatDateLabel(task.due_date) : t("No due date")].filter(Boolean).join(" · "))}</div>
                ${task.description ? `<div class="today-item-note">${escapeHtml(task.description)}</div>` : ""}
              </div>
              <div class="today-item-side">
                ${renderBadge(dueLabel, overdue ? "err" : "warn")}
                ${task.lead_id ? `<button class="small ghost" data-action="open-crm-lead" data-lead-id="${task.lead_id}">${escapeHtml(t("Open"))}</button>` : ""}
              </div>
            </div>
          `;
        }).join("");
      }

      function renderTodayMeetings(items, timeZone) {
        if (!items.length) {
          return renderEmptyState("No meetings booked for today.", [
            { label: "Add meeting", attrs: { "data-action": "set-view", "data-view": "calendar" } },
          ], { compact: true });
        }
        return items.map((item) => `
          <div class="today-item">
            <div class="today-item-main">
              <div class="today-item-title">${escapeHtml(item.lead_name || item.title || t("Booked meeting"))}</div>
              <div class="today-item-meta">${escapeHtml(`${formatTimeInTimeZone(item.start_at, item.timezone || timeZone)} · ${formatTimeInTimeZone(item.end_at, item.timezone || timeZone)}`)}</div>
              <div class="today-item-note">${escapeHtml([item.phone || "", !isClientRole() ? (item.client_name || "") : ""].filter(Boolean).join(" · ") || t("No contact details"))}</div>
            </div>
            <div class="today-item-side">
              ${renderBadge(item.status || "booked", stateTone(item.status || "BOOKED"))}
            </div>
          </div>
        `).join("");
      }

      function renderTodayHot(items) {
        if (!items.length) {
          return renderEmptyState("No active opportunities yet.", [
            { label: "Open pipeline", attrs: { "data-action": "set-view", "data-view": "crm" } },
          ], { compact: true });
        }
        return items.map((item) => {
          const scoreLabel = formatScoreLabel(item.lead_score);
          const valueLabel = formatCompactCurrency(item.estimated_value);
          return `
            <button type="button" class="today-item today-item-action" data-action="open-crm-lead" data-lead-id="${item.lead_id}">
              <div class="today-item-main">
                <div class="today-item-title">${escapeHtml(item.lead_name || item.phone || `Contact ${item.lead_id}`)}</div>
                <div class="today-item-meta">${escapeHtml([formatCrmStageDisplay(item.crm_stage), formatLeadSourceLabel(item.source), item.campaign_name || ""].filter(Boolean).join(" · "))}</div>
                <div class="today-item-note">${renderLabeledSnippet(item, "No messages yet.", 100)}</div>
              </div>
              <div class="today-item-side">
                ${scoreLabel ? renderBadge(`Score ${scoreLabel}`, Number(item.lead_score) >= 80 ? "ok" : Number(item.lead_score) >= 55 ? "info" : "warn") : ""}
                ${valueLabel ? renderBadge(valueLabel, "info") : ""}
              </div>
            </button>
          `;
        }).join("");
      }

      function renderTodayConversations(items) {
        if (!items.length) {
          return renderEmptyState("No recent conversations.", [
            { label: "Open inbox", attrs: { "data-action": "set-view", "data-view": "conversations" } },
          ], { compact: true });
        }
        return items.map((item) => `
          <button type="button" class="today-item today-item-action" data-action="open-thread" data-lead-id="${item.lead_id}">
            <div class="today-item-main">
              <div class="today-item-title">${escapeHtml(item.lead_name || item.phone || `Contact ${item.lead_id}`)}</div>
              <div class="today-item-meta">${escapeHtml([formatConversationStateLabel(item.state), item.phone || "", !isClientRole() ? (item.client_name || "") : ""].filter(Boolean).join(" · "))}</div>
              <div class="today-item-note">${renderLabeledSnippet(item, "No messages yet.", 110)}</div>
            </div>
            <div class="today-item-side">
              <div class="meta-text">${escapeHtml(formatDateTime(item.last_activity_at))}</div>
            </div>
          </button>
        `).join("");
      }

      function renderTodayView() {
        const todaySection = document.getElementById("todayStats");
        if (!todaySection) return;
        const stats = state.dashboard?.stats || {};
        const timeZone = todayTimeZone();
        const todayKey = todayDateKey();
        const tasks = todayTaskRows(todayKey);
        const meetings = todayMeetingRows(todayKey, timeZone);
        const hot = todayHotRows();
        const conversations = todayConversationRows();
        const overdueCount = (state.crmTasks?.items || []).filter((task) => task.status !== "done" && task.due_date && task.due_date < todayKey).length;
        const dueTodayCount = (state.crmTasks?.items || []).filter((task) => task.status !== "done" && task.due_date === todayKey).length;

        setText("todayKicker", isClientRole() ? "Business day" : "Daily command center");
        setText("todayTitle", "Today");
        setText(
          "todaySubtitle",
          isClientRole()
            ? "Your meetings, follow-ups, and active opportunities for the day."
            : "A focused operator page for meetings, overdue follow-ups, fresh demand, and high-intent opportunities."
        );
        document.getElementById("todayHeroPills").innerHTML = [
          { label: "Open tasks", value: stats.open_tasks_total || 0, view: "tasks" },
          { label: "New last 24h", value: stats.new_last_24_hours || 0, view: "leads" },
          { label: "Meetings today", value: meetings.length, view: "calendar" },
        ].map((item) => `<button type="button" class="small ghost" data-action="set-view" data-view="${item.view}">${escapeHtml(t(item.label))} ${escapeHtml(String(item.value))}</button>`).join("");
        todaySection.innerHTML = [
          { label: "Due today", value: dueTodayCount, meta: `${overdueCount} ${t("overdue")}`, view: "tasks", tone: dueTodayCount || overdueCount ? "warn" : "ok" },
          { label: "Meetings", value: meetings.length, meta: timeZone || "local time", view: "calendar", tone: meetings.length ? "info" : "" },
          { label: "Fresh demand", value: stats.new_last_24_hours || 0, meta: `${stats.new_last_7_days || 0} ${t("this week")}`, view: "leads", tone: stats.new_last_24_hours ? "ok" : "" },
          { label: "Hot pipeline", value: hot.length, meta: t("ranked by score/value"), view: "crm", tone: hot.length ? "info" : "" },
        ].map((item) => `
          <button type="button" class="today-stat-card" data-action="set-view" data-view="${item.view}">
            <div class="dashboard-kpi-label">${escapeHtml(t(item.label))}</div>
            <div class="dashboard-kpi-value">${escapeHtml(String(item.value))}</div>
            <div class="dashboard-kpi-meta">${escapeHtml(item.meta)}</div>
            <div class="chip-row">${renderBadge(t(item.tone === "ok" ? "clear" : item.tone === "warn" ? "needs action" : item.tone === "info" ? "active" : "quiet"), item.tone)}</div>
          </button>
        `).join("");
        document.getElementById("todayTaskList").innerHTML = renderTodayTasks(tasks, todayKey);
        document.getElementById("todayMeetingList").innerHTML = renderTodayMeetings(meetings, timeZone);
        document.getElementById("todayHotList").innerHTML = renderTodayHot(hot);
        document.getElementById("todayConversationList").innerHTML = renderTodayConversations(conversations);
      }

      function renderDashboard() {
        if (!state.dashboard) return;
        const stats = state.dashboard.stats;
        const isAdmin = !isClientRole();
        setText("dashboardHeroKicker", isAdmin ? "Portfolio overview" : "Business overview");
        setText("dashboardHeroTitle", isAdmin ? "Dashboard" : (state.dashboard.scope?.client_name || "Dashboard"));
        setText(
          "dashboardHeroSubtitle",
          isAdmin
            ? "A clean operating view of new demand first, then immediate actions and pipeline health."
            : "A single place to track demand, source mix, next meetings, and the follow-ups your business needs to handle."
        );

        const kpis = [
          {
            label: "Total records",
            value: stats.total_leads || stats.conversations_total || 0,
            meta: `${stats.new_last_30_days || 0} in the last 30 days`,
            foot: `${stats.new_last_7_days || 0} added this week`,
            view: "leads",
          },
          {
            label: "New last 24h",
            value: stats.new_last_24_hours || 0,
            meta: `${stats.new_last_7_days || 0} in the last 7 days`,
            foot: `${stats.new_last_30_days || 0} in the last 30 days`,
            view: "leads",
          },
          {
            label: "Open tasks",
            value: stats.open_tasks_total || 0,
            meta: `${stats.overdue_tasks_total || 0} overdue`,
            foot: `${stats.upcoming_meetings_7d || 0} meetings in the next 7 days`,
            view: "tasks",
          },
        ];
        document.getElementById("dashboardKpis").innerHTML = kpis.map((item) => `
          <button type="button" class="dashboard-kpi-card dashboard-kpi-action" data-action="set-view" data-view="${escapeHtml(item.view)}">
            <div class="dashboard-kpi-label">${escapeHtml(item.label)}</div>
            <div class="dashboard-kpi-value">${escapeHtml(String(item.value))}</div>
            <div class="dashboard-kpi-meta">${escapeHtml(item.meta)}</div>
            <div class="dashboard-kpi-foot">${escapeHtml(item.foot)}</div>
          </button>
        `).join("");

        document.getElementById("dashboardHeroPills").innerHTML = [
          { label: "Open records", view: "leads" },
          { label: "Pipeline", view: "crm" },
          { label: "Tasks", view: "tasks" },
          { label: "Calendar", view: "calendar" },
        ].map((item) => `<button type="button" class="small ghost" data-action="set-view" data-view="${item.view}">${escapeHtml(t(item.label))}</button>`).join("");
        document.getElementById("dashboardTrend").innerHTML = renderDashboardTrend(state.dashboard.lead_trend || []);
        document.getElementById("dashboardSourceBreakdown").innerHTML = renderDashboardGraph(state.dashboard.source_breakdown || [], "source");
        document.getElementById("dashboardCampaignPerformance").innerHTML = renderDashboardCampaignPerformance(state.dashboard.campaign_performance || {});
        document.getElementById("dashboardStageBreakdown").innerHTML = renderDashboardStageChart(state.dashboard.stage_breakdown || []);
        document.getElementById("dashboardUpcoming").innerHTML = renderDashboardUpcoming(state.dashboard.upcoming || {});
        renderDashboardLatestLeads(state.dashboard.recent_leads || [], stats);
        renderTodayView();
      }
