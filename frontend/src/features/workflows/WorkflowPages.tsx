import { FormEvent, useCallback, useEffect, useRef, useState } from "react";
import {
  ApiError,
  addConversationNote,
  addLeadNote,
  addLeadTag,
  archiveLead,
  clearOutboundRequestKey,
  createCrmLead,
  createLeadTask,
  createMeeting,
  deleteConversation,
  deleteLeadTag,
  deleteMeeting,
  fetchCalendar,
  fetchClients,
  fetchConversationThread,
  fetchConversations,
  fetchCrmLeads,
  fetchLeadDetail,
  fetchTasks,
  markConversationHandoff,
  outboundRequestKey,
  sendBookingLink,
  sendManualMediaMessage,
  sendManualMessage,
  sendSandboxMessage,
  updateAgentControl,
  updateLeadStage,
  updateMeetingStatus,
  updateTask
} from "../../api/client";
import type {
  CalendarBooking,
  ClientSummary,
  ConversationListItem,
  ConversationThreadPayload,
  CrmLeadsPayload,
  LeadDetailPayload,
  LeadListItem,
  LeadTask,
  ManualMeetingCreatePayload,
  ManualLeadCreatePayload,
  SessionPayload,
  TasksPayload,
  ThreadMessage
} from "../../api/types";
import { useAuth } from "../auth/AuthProvider";
import { CalendarMeetingForm, PaneResizer } from "./WorkflowDialogs";

export type WorkflowQuickAction = {
  id: number;
  kind: "add-contact" | "add-meeting";
  leadId?: number;
};

type WorkflowPageProps = {
  onReadyChange?: (ready: boolean) => void;
  quickAction?: WorkflowQuickAction | null;
  selectedClientKey?: string;
  searchQuery?: string;
};

const FALLBACK_STAGES = ["New Lead", "Contacted", "Qualified", "Meeting Booked", "Meeting Completed", "Won", "Lost"];
const INBOX_STATES = ["NEW", "GREETED", "QUALIFYING", "BOOKING_SENT", "BOOKED", "HANDOFF", "OPTED_OUT"];
const INBOX_PANE_SIZES_KEY = "lead-ui-inbox-pane-sizes";
const CRM_CLIENT_FILTER_KEY = "lead-ui-crm-client";
const CRM_STAGE_FILTER_KEY = "lead-ui-crm-stage";
const SELECTED_CLIENT_KEY = "lead-ui-selected-client";
const STALE_REQUEST = Symbol("stale-request");

type PipelineFilters = {
  clientKey: string;
  stage: string;
  search: string;
};

type PipelineLeadForm = ManualLeadCreatePayload & {
  full_name: string;
};

export function PipelinePage({ onReadyChange, quickAction, selectedClientKey = "", searchQuery = "" }: WorkflowPageProps) {
  const auth = useAuth();
  const [state, setState] = useState<{ status: "loading" | "ready" | "error"; data: CrmLeadsPayload | null; error: string }>({
    status: "loading",
    data: null,
    error: ""
  });
  const [clients, setClients] = useState<ClientSummary[]>([]);
  const [filters, setFilters] = useState<PipelineFilters>(() => ({
    ...initialPipelineFilters(),
    clientKey: selectedClientKey || initialPipelineFilters().clientKey,
    search: searchQuery
  }));
  const [addOpen, setAddOpen] = useState(false);
  const [leadForm, setLeadForm] = useState<PipelineLeadForm>(() => emptyPipelineLeadForm());
  const [actionStatus, setActionStatus] = useState("");
  const [movingLeadId, setMovingLeadId] = useState<number | null>(null);
  const [dropStage, setDropStage] = useState("");
  const [retryVersion, setRetryVersion] = useState(0);
  const requestId = useRef(0);
  const debouncedSearch = useDebouncedValue(filters.search, 250);

  const isClientRole = auth.status === "ready" && auth.session.role === "client";

  const load = (nextFilters = filters) => {
    if (auth.status !== "ready") return;
    const currentRequest = ++requestId.current;
    setState((current) => ({ ...current, status: "loading", error: "" }));
    onReadyChange?.(false);

    const scopedClientKey = auth.session.role === "client" ? auth.session.client_key || "" : nextFilters.clientKey;
    const params = {
      archived: false,
      client_key: scopedClientKey || undefined,
      stage: nextFilters.stage !== "all" ? nextFilters.stage : undefined,
      q: nextFilters.search.trim() || undefined
    };
    const clientRequest = auth.session.role === "admin" ? fetchClients() : Promise.resolve<ClientSummary[]>([]);

    Promise.all([clientRequest, fetchCrmLeads(params)])
      .then(([clientItems, data]) => {
        if (currentRequest !== requestId.current) return;
        setClients(clientItems);
        setState({ status: "ready", data, error: "" });
        onReadyChange?.(true);
      })
      .catch((error: unknown) => {
        if (currentRequest !== requestId.current) return;
        setState((current) => ({ ...current, status: "error", error: messageFor(error, "Pipeline unavailable") }));
        onReadyChange?.(false);
      });
  };

  useEffect(() => {
    if (auth.status !== "ready") {
      onReadyChange?.(false);
      return;
    }
    load({ ...filters, search: debouncedSearch });
  }, [auth.status, filters.clientKey, filters.stage, debouncedSearch, retryVersion]);

  useEffect(() => {
    if (!selectedClientKey && !searchQuery) return;
    setFilters((current) => ({
      ...current,
      clientKey: selectedClientKey || current.clientKey,
      search: searchQuery
    }));
  }, [selectedClientKey, searchQuery]);

  useEffect(() => {
    if (auth.status !== "ready" || auth.session.role !== "admin") return;
    const onClientChange = (event: Event) => {
      const clientKey = (event as CustomEvent<{ clientKey?: string }>).detail?.clientKey || "";
      updateFilters({ clientKey });
    };
    window.addEventListener("lead-ui-client-change", onClientChange);
    return () => window.removeEventListener("lead-ui-client-change", onClientChange);
  }, [auth.status]);

  useEffect(() => {
    if (quickAction?.kind === "add-contact") openAddPanel();
  }, [quickAction?.id]);

  useEffect(() => {
    if (!state.data || window.localStorage.getItem("lead-ui-react-open-add-contact") !== "true") return;
    window.localStorage.removeItem("lead-ui-react-open-add-contact");
    openAddPanel();
  }, [state.data]);

  if (!state.data) {
    return (
      <WorkflowLoadState
        title="Pipeline"
        status={state.status}
        error={state.error}
        onRetry={() => setRetryVersion((current) => current + 1)}
      />
    );
  }

  const stages = state.data.stages?.length ? state.data.stages : FALLBACK_STAGES;
  const visibleStages = filters.stage !== "all" ? stages.filter((stage) => stage === filters.stage) : stages;
  const grouped = groupByStage(state.data.items, stages);
  const activeCount = state.data.total;
  const includeClientName = !isClientRole && !filters.clientKey;
  const hasFilters = Boolean(filters.clientKey || filters.search.trim() || filters.stage !== "all");

  function updateFilters(partial: Partial<PipelineFilters>) {
    setFilters((current) => {
      const next = { ...current, ...partial };
      window.localStorage.setItem(CRM_CLIENT_FILTER_KEY, next.clientKey);
      window.localStorage.setItem(CRM_STAGE_FILTER_KEY, next.stage);
      return next;
    });
  }

  function clearFilters() {
    updateFilters({ clientKey: "", search: "", stage: "all" });
  }

  function openAddPanel() {
    setLeadForm(emptyPipelineLeadForm(filters.clientKey || window.localStorage.getItem(SELECTED_CLIENT_KEY) || clients[0]?.client_key || ""));
    setActionStatus("");
    setAddOpen(true);
  }

  function updateLeadForm(partial: Partial<PipelineLeadForm>) {
    setLeadForm((current) => ({ ...current, ...partial }));
  }

  async function moveLead(leadId: number, stage: string) {
    setMovingLeadId(leadId);
    try {
      await updateLeadStage(leadId, stage);
      load();
    } catch (error) {
      setActionStatus(messageFor(error, "Stage could not be updated."));
    } finally {
      setMovingLeadId(null);
    }
  }

  async function submitLead(event: FormEvent) {
    event.preventDefault();
    if (!leadForm.full_name.trim()) {
      setActionStatus("Name is required.");
      return;
    }
    const clientKey = auth.status === "ready" && auth.session.role === "client"
      ? auth.session.client_key || ""
      : leadForm.client_key || filters.clientKey;
    if (!clientKey && !isClientRole) {
      setActionStatus("Choose a client before creating a contact.");
      return;
    }
    setActionStatus("Creating contact...");
    try {
      await createCrmLead(cleanLeadForm({ ...leadForm, client_key: clientKey || undefined }));
      setActionStatus("Contact created.");
      setAddOpen(false);
      setLeadForm(emptyPipelineLeadForm(clientKey));
      load();
    } catch (error) {
      setActionStatus(messageFor(error, "Contact could not be created."));
    }
  }

  return (
    <div className="react-workflow-page react-pipeline-page" data-testid="react-pipeline-page">
      <section className="surface">
        <div className="surface-header">
          <div className="surface-title">
            <div>
              <h2>Pipeline</h2>
              <div className="surface-subtitle">Stage board for daily pipeline operations. Drag cards between stages to update status.</div>
            </div>
          </div>
          <div className="toolbar-right">
            <button className="small" type="button" onClick={openAddPanel}>
              Add contact
            </button>
            <span className="badge">{activeCount} active</span>
          </div>
        </div>
        <div className="toolbar">
          <div className="toolbar-left">
            {!isClientRole ? (
              <select
                aria-label="Filter pipeline by client"
                value={filters.clientKey}
                onChange={(event) => updateFilters({ clientKey: event.currentTarget.value })}
              >
                <option value="">All clients</option>
                {clients.map((client) => (
                  <option key={client.client_key} value={client.client_key}>
                    {client.business_name}
                  </option>
                ))}
              </select>
            ) : null}
            <select
              aria-label="Filter pipeline by stage"
              value={filters.stage}
              onChange={(event) => updateFilters({ stage: event.currentTarget.value })}
            >
              <option value="all">All stages</option>
              {stages.map((stage) => (
                <option key={stage} value={stage}>
                  {formatCrmStageDisplay(stage)}
                </option>
              ))}
            </select>
            <input
              aria-label="Search pipeline"
              placeholder="Search contacts, tags, campaigns..."
              value={filters.search}
              onChange={(event) => updateFilters({ search: event.currentTarget.value })}
            />
          </div>
          <div className="toolbar-right">
            <div className="chip-row">
              {stages.map((stage) => (
                <button
                  className={`tag ${filters.stage === stage ? "info" : ""}`}
                  key={stage}
                  type="button"
                  onClick={() => updateFilters({ stage })}
                >
                  {formatCrmStageDisplay(stage)} {stageCount(grouped, stage)}
                </button>
              ))}
            </div>
            {hasFilters ? <button className="small ghost" type="button" onClick={clearFilters}>Clear</button> : null}
          </div>
        </div>
        {actionStatus ? <div className="meta-text" role="status">{actionStatus}</div> : null}
        {state.status === "error" ? <InlineError message={state.error} onRetry={() => setRetryVersion((current) => current + 1)} /> : null}
        {addOpen ? (
          <PipelineAddLeadPanel
            clients={clients}
            isClientRole={isClientRole}
            stages={stages}
            value={leadForm}
            onCancel={() => setAddOpen(false)}
            onChange={updateLeadForm}
            onSubmit={submitLead}
          />
        ) : null}
      </section>
      <div className="crm-board">
        {visibleStages.map((stage) => (
          <PipelineStageColumn
            dropStage={dropStage}
            includeClientName={includeClientName}
            key={stage}
            leads={grouped[stage] ?? []}
            movingLeadId={movingLeadId}
            stage={stage}
            stages={stages}
            onDropStage={(leadId, nextStage) => void moveLead(leadId, nextStage)}
            onOpenAdd={openAddPanel}
            onSetDropStage={setDropStage}
          />
        ))}
      </div>
    </div>
  );
}

function PipelineAddLeadPanel({
  clients,
  isClientRole,
  stages,
  value,
  onCancel,
  onChange,
  onSubmit
}: {
  clients: ClientSummary[];
  isClientRole: boolean;
  stages: string[];
  value: PipelineLeadForm;
  onCancel: () => void;
  onChange: (partial: Partial<PipelineLeadForm>) => void;
  onSubmit: (event: FormEvent) => void;
}) {
  return (
    <form className="manual-panel" onSubmit={onSubmit}>
      <div className="manual-panel-head">
        <div>
          <div className="title">Add contact</div>
          <div className="meta-text">Create a manual pipeline record without waiting for automation.</div>
        </div>
        <button className="small ghost" type="button" onClick={onCancel}>Close</button>
      </div>
      <div className="form-grid-3">
        {!isClientRole ? (
          <div>
            <label htmlFor="pipelineLeadClient">Client</label>
            <select id="pipelineLeadClient" value={value.client_key || ""} onChange={(event) => onChange({ client_key: event.currentTarget.value })}>
              <option value="">Choose client</option>
              {clients.map((client) => (
                <option key={client.client_key} value={client.client_key}>
                  {client.business_name}
                </option>
              ))}
            </select>
          </div>
        ) : null}
        <div>
          <label htmlFor="pipelineLeadName">Name</label>
          <input id="pipelineLeadName" value={value.full_name} onChange={(event) => onChange({ full_name: event.currentTarget.value })} placeholder="Jane Smith" required />
        </div>
        <div>
          <label htmlFor="pipelineLeadPhone">Phone</label>
          <input id="pipelineLeadPhone" value={value.phone || ""} onChange={(event) => onChange({ phone: event.currentTarget.value })} placeholder="+15551234567" />
        </div>
        <div>
          <label htmlFor="pipelineLeadEmail">Email</label>
          <input id="pipelineLeadEmail" value={value.email || ""} onChange={(event) => onChange({ email: event.currentTarget.value })} placeholder="contact@example.com" />
        </div>
        <div>
          <label htmlFor="pipelineLeadCity">City</label>
          <input id="pipelineLeadCity" value={value.city || ""} onChange={(event) => onChange({ city: event.currentTarget.value })} placeholder="Toronto" />
        </div>
        <div>
          <label htmlFor="pipelineLeadStage">Stage</label>
          <select id="pipelineLeadStage" value={value.crm_stage || "New Lead"} onChange={(event) => onChange({ crm_stage: event.currentTarget.value })}>
            {stages.map((stage) => (
              <option key={stage} value={stage}>
                {formatCrmStageDisplay(stage)}
              </option>
            ))}
          </select>
        </div>
        <div>
          <label htmlFor="pipelineLeadOwner">Owner</label>
          <input id="pipelineLeadOwner" value={value.owner_name || ""} onChange={(event) => onChange({ owner_name: event.currentTarget.value })} placeholder="Team member" />
        </div>
      </div>
      <div>
        <label htmlFor="pipelineLeadNotes">Notes</label>
        <textarea id="pipelineLeadNotes" value={value.notes || ""} onChange={(event) => onChange({ notes: event.currentTarget.value })} placeholder="Optional context for the team." />
      </div>
      <div className="actions">
        <button className="primary" type="submit">Create contact</button>
        <button className="small ghost" type="button" onClick={onCancel}>Cancel</button>
      </div>
    </form>
  );
}

function PipelineStageColumn({
  dropStage,
  includeClientName,
  leads,
  movingLeadId,
  stage,
  stages,
  onDropStage,
  onOpenAdd,
  onSetDropStage
}: {
  dropStage: string;
  includeClientName: boolean;
  leads: LeadListItem[];
  movingLeadId: number | null;
  stage: string;
  stages: string[];
  onDropStage: (leadId: number, stage: string) => void;
  onOpenAdd: () => void;
  onSetDropStage: (stage: string) => void;
}) {
  const isDropTarget = dropStage === stage;
  return (
    <section className="crm-stage-column" aria-label={`${formatCrmStageDisplay(stage)} stage`}>
      <div className="crm-stage-header">
        <div className="item-title">{formatCrmStageDisplay(stage)}</div>
        <span className="badge">{leads.length}</span>
      </div>
      <div
        className={`crm-stage-list ${isDropTarget ? "drop-target" : ""}`}
        data-stage={stage}
        onDragEnter={() => onSetDropStage(stage)}
        onDragLeave={() => onSetDropStage("")}
        onDragOver={(event) => event.preventDefault()}
        onDrop={(event) => {
          event.preventDefault();
          onSetDropStage("");
          const leadId = Number(event.dataTransfer.getData("text/lead-id"));
          if (leadId) onDropStage(leadId, stage);
        }}
      >
        {leads.length ? leads.map((lead) => (
          <PipelineLeadCard
            includeClientName={includeClientName}
            key={lead.lead_id}
            lead={lead}
            moving={movingLeadId === lead.lead_id}
            stages={stages}
          />
        )) : (
          <div className="empty-state compact">
            <div>No records in this stage.</div>
            <button className="small ghost" type="button" onClick={onOpenAdd}>Add contact</button>
          </div>
        )}
      </div>
    </section>
  );
}

function PipelineLeadCard({ includeClientName, lead, moving, stages }: { includeClientName: boolean; lead: LeadListItem; moving: boolean; stages: string[] }) {
  const displayName = lead.lead_name || formatPhone(lead.phone) || `Contact ${lead.lead_id}`;
  const visibleTags = dedupeTags(lead.tags || [], lead.crm_stage).slice(0, 2);
  const conversationBadge = conversationStateBadge(lead.conversation_state, lead.crm_stage);
  const snippet = lead.last_message_snippet || lead.lead_summary || lead.recommended_follow_up || "No recent activity.";
  return (
    <article
      aria-disabled={moving}
      className={`crm-card ${moving ? "dragging" : ""}`}
      data-action="open-crm-lead"
      data-lead-id={lead.lead_id}
      data-crm-stage={lead.crm_stage}
      draggable={!moving}
      role="button"
      tabIndex={0}
      onDragStart={(event) => {
        event.dataTransfer.setData("text/lead-id", String(lead.lead_id));
      }}
      onKeyDown={(event) => {
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          event.currentTarget.click();
        }
      }}
    >
      <div className="item-title-row">
        <div className="item-title">{displayName}</div>
        <div className="actions">
          <button className="small ghost" type="button" data-action="open-contact-drawer" data-lead-id={lead.lead_id} data-source="pipeline">
            Message
          </button>
          {conversationBadge}
        </div>
      </div>
      <div className="item-subtitle">{pipelineMetaLine(lead, includeClientName)}</div>
      <div className="item-snippet">{snippet}</div>
      {lead.campaign_name ? <div className="crm-card-campaign">{lead.campaign_name}</div> : null}
      {lead.next_task_title ? (
        <div className="crm-card-next">
          <span>Next task</span>
          {lead.next_task_title}{lead.next_task_due_date ? ` · ${formatDate(lead.next_task_due_date)}` : ""}
        </div>
      ) : null}
      <div className="item-meta-row">
        <div className="chip-row">
          {visibleTags.map((tag) => renderTag(tag))}
          {lead.intent_level ? renderTag(lead.intent_level) : null}
        </div>
        <div className="meta-text">{formatDateTime(lead.last_activity_at)}</div>
      </div>
      <div className="sr-only">Current stage: {formatCrmStageDisplay(lead.crm_stage)}. Available stages: {stages.map(formatCrmStageDisplay).join(", ")}.</div>
    </article>
  );
}

export function RecordsPage({ onReadyChange, selectedClientKey = "", searchQuery = "" }: WorkflowPageProps) {
  const auth = useAuth();
  const [list, setList] = useState<CrmLeadsPayload | null>(null);
  const [detail, setDetail] = useState<LeadDetailPayload | null>(null);
  const [selectedLeadId, setSelectedLeadId] = useState<number | null>(() => Number(window.localStorage.getItem("lead-ui-active-crm-lead") || 0) || null);
  const [clients, setClients] = useState<ClientSummary[]>([]);
  const [clientKey, setClientKey] = useState(() => selectedClientKey || window.localStorage.getItem(SELECTED_CLIENT_KEY) || window.localStorage.getItem(CRM_CLIENT_FILTER_KEY) || "");
  const [search, setSearch] = useState(searchQuery);
  const [showArchived, setShowArchived] = useState(() => window.localStorage.getItem("lead-ui-crm-archived") === "true");
  const [addOpen, setAddOpen] = useState(false);
  const [leadForm, setLeadForm] = useState<PipelineLeadForm>(() => emptyPipelineLeadForm(window.localStorage.getItem(SELECTED_CLIENT_KEY) || ""));
  const [status, setStatus] = useState("");
  const [tagText, setTagText] = useState("");
  const [noteText, setNoteText] = useState("");
  const [taskTitle, setTaskTitle] = useState("");
  const [taskDueDate, setTaskDueDate] = useState("");
  const [taskDescription, setTaskDescription] = useState("");
  const [loadStatus, setLoadStatus] = useState<"loading" | "ready" | "error">("loading");
  const [busy, setBusy] = useState(false);
  const [retryVersion, setRetryVersion] = useState(0);
  const debouncedSearch = useDebouncedValue(search, 250);
  const listRequestId = useRef(0);
  const detailRequestId = useRef(0);

  const isClientRole = auth.status === "ready" && auth.session.role === "client";
  const stages = detail?.stages?.length ? detail.stages : list?.stages?.length ? list.stages : FALLBACK_STAGES;
  const selectedListItem = list?.items.find((lead) => lead.lead_id === selectedLeadId) || null;
  const selectedArchived = Boolean(selectedListItem?.archived || hasTag(detail?.lead.tags || [], "archived"));
  const recordTags = detail ? dedupeTags(detail.tags || detail.lead.tags || [], detail.lead.crm_stage) : [];

  const loadList = (preferredLeadId = selectedLeadId, archived = showArchived) => {
    if (auth.status !== "ready") return Promise.resolve<number | null>(null);
    const currentRequest = ++listRequestId.current;
    setLoadStatus("loading");
    onReadyChange?.(false);
    const scopedClientKey = isClientRole ? auth.session.client_key || "" : clientKey;
    const clientsRequest = isClientRole ? Promise.resolve<ClientSummary[]>([]) : fetchClients();
    return Promise.all([
      clientsRequest,
      fetchCrmLeads({
        archived,
        client_key: scopedClientKey || undefined,
        q: debouncedSearch.trim() || undefined
      })
    ]).then(([clientItems, data]) => {
      if (currentRequest !== listRequestId.current) throw STALE_REQUEST;
      setClients(clientItems);
      setList(data);
      const nextLeadId = preferredLeadId && data.items.some((lead) => lead.lead_id === preferredLeadId)
        ? preferredLeadId
        : data.items[0]?.lead_id ?? null;
      setSelectedLeadId(nextLeadId);
      return nextLeadId;
    });
  };

  const loadDetail = (leadId: number | null) => {
    const currentRequest = ++detailRequestId.current;
    if (!leadId) {
      setDetail(null);
      window.localStorage.removeItem("lead-ui-active-crm-lead");
      onReadyChange?.(true);
      return Promise.resolve();
    }
    return fetchLeadDetail(leadId)
      .then((payload) => {
        if (currentRequest !== detailRequestId.current) throw STALE_REQUEST;
        setDetail(payload);
        window.localStorage.setItem("lead-ui-active-crm-lead", String(leadId));
        onReadyChange?.(true);
      })
      .catch((error: unknown) => {
        if (error === STALE_REQUEST || currentRequest !== detailRequestId.current) return;
        if (error instanceof ApiError && error.status === 404) {
          window.localStorage.removeItem("lead-ui-active-crm-lead");
          setSelectedLeadId(null);
          setDetail(null);
          onReadyChange?.(true);
          return;
        }
        throw error;
      });
  };

  const refresh = (preferredLeadId = selectedLeadId, archived = showArchived) => {
    setStatus("");
    loadList(preferredLeadId, archived)
      .then((leadId) => loadDetail(leadId))
      .then(() => setLoadStatus("ready"))
      .catch((error: unknown) => {
        if (error === STALE_REQUEST) return;
        setStatus(messageFor(error, "Records unavailable"));
        setLoadStatus("error");
        onReadyChange?.(false);
      });
  };

  useEffect(() => {
    if (auth.status !== "ready") {
      onReadyChange?.(false);
      return;
    }
    refresh();
  }, [auth.status, clientKey, showArchived, debouncedSearch, retryVersion]);

  useEffect(() => {
    if (selectedClientKey) setClientKey(selectedClientKey);
    setSearch(searchQuery);
  }, [selectedClientKey, searchQuery]);

  useEffect(() => {
    if (auth.status === "ready" && list) {
      if (selectedLeadId && !list.items.some((lead) => lead.lead_id === selectedLeadId)) return;
      void loadDetail(selectedLeadId);
    }
  }, [auth.status, list, selectedLeadId]);

  useEffect(() => {
    if (auth.status !== "ready" || auth.session.role !== "admin") return;
    const onClientChange = (event: Event) => {
      const nextClientKey = (event as CustomEvent<{ clientKey?: string }>).detail?.clientKey || "";
      setClientKey(nextClientKey);
      window.localStorage.setItem(CRM_CLIENT_FILTER_KEY, nextClientKey);
      setSelectedLeadId(null);
    };
    window.addEventListener("lead-ui-client-change", onClientChange);
    return () => window.removeEventListener("lead-ui-client-change", onClientChange);
  }, [auth.status]);

  if (!list || (!detail && list.items.length)) {
    return <WorkflowLoadState title="Records" status={loadStatus} error={status} onRetry={() => setRetryVersion((current) => current + 1)} />;
  }

  async function runRecordAction(startMessage: string, successMessage: string, action: () => Promise<void>) {
    if (busy) return;
    setBusy(true);
    setStatus(startMessage);
    try {
      await action();
      setStatus(successMessage);
    } catch (caught) {
      setStatus(messageFor(caught, "Action failed"));
    } finally {
      setBusy(false);
    }
  }

  async function saveStage(stage: string) {
    if (!detail) return;
    const leadId = detail.lead.id;
    await runRecordAction("Updating stage...", "Stage updated.", async () => {
      await updateLeadStage(leadId, stage);
      await loadDetail(leadId);
    });
  }

  function updateLeadForm(partial: Partial<PipelineLeadForm>) {
    setLeadForm((current) => ({ ...current, ...partial }));
  }

  function setArchivedMode(nextArchived: boolean) {
    setShowArchived(nextArchived);
    window.localStorage.setItem("lead-ui-crm-archived", String(nextArchived));
    setSelectedLeadId(null);
    setDetail(null);
  }

  function openAddPanel() {
    setLeadForm(emptyPipelineLeadForm(clientKey || window.localStorage.getItem(SELECTED_CLIENT_KEY) || clients[0]?.client_key || ""));
    setStatus("");
    setAddOpen(true);
  }

  async function submitTag(event: FormEvent) {
    event.preventDefault();
    if (!detail || !tagText.trim()) return;
    const leadId = detail.lead.id;
    const tag = tagText.trim();
    await runRecordAction("Adding tag...", "Tag added.", async () => {
      await addLeadTag(leadId, tag);
      setTagText("");
      await loadDetail(leadId);
    });
  }

  async function removeTag(tag: string) {
    if (!detail) return;
    const leadId = detail.lead.id;
    await runRecordAction("Removing tag...", "Tag removed.", async () => {
      await deleteLeadTag(leadId, tag);
      await loadDetail(leadId);
    });
  }

  async function submitNote(event: FormEvent) {
    event.preventDefault();
    if (!detail || !noteText.trim()) return;
    const leadId = detail.lead.id;
    const body = noteText.trim();
    await runRecordAction("Saving note...", "Note saved.", async () => {
      await addLeadNote(leadId, body);
      setNoteText("");
      await loadDetail(leadId);
    });
  }

  async function submitTask(event: FormEvent) {
    event.preventDefault();
    if (!detail || !taskTitle.trim()) return;
    const leadId = detail.lead.id;
    await runRecordAction("Creating task...", "Task created.", async () => {
      await createLeadTask(leadId, {
        description: taskDescription.trim() || undefined,
        due_date: taskDueDate || undefined,
        title: taskTitle.trim()
      });
      setTaskTitle("");
      setTaskDueDate("");
      setTaskDescription("");
      await loadDetail(leadId);
    });
  }

  async function toggleTask(task: LeadTask) {
    const leadId = detail?.lead.id;
    await runRecordAction("Updating task...", "Task updated.", async () => {
      await updateTask(task.id, { status: task.status === "done" ? "open" : "done" });
      if (leadId) await loadDetail(leadId);
    });
  }

  async function setArchived(nextArchived: boolean) {
    if (!detail) return;
    const leadId = detail.lead.id;
    await runRecordAction(
      nextArchived ? "Archiving contact..." : "Restoring contact...",
      nextArchived ? "Contact archived." : "Contact restored.",
      async () => {
        await archiveLead(leadId, nextArchived);
        refresh(null);
      }
    );
  }

  async function submitLead(event: FormEvent) {
    event.preventDefault();
    if (!leadForm.full_name.trim()) {
      setStatus("Name is required.");
      return;
    }
    const scopedClientKey = isClientRole && auth.status === "ready" ? auth.session.client_key || "" : leadForm.client_key || clientKey;
    if (!scopedClientKey && !isClientRole) {
      setStatus("Choose a client before creating a contact.");
      return;
    }
    setStatus("Creating contact...");
    try {
      const response = await createCrmLead(cleanLeadForm({ ...leadForm, client_key: scopedClientKey || undefined }));
      setShowArchived(false);
      window.localStorage.setItem("lead-ui-crm-archived", "false");
      setAddOpen(false);
      setLeadForm(emptyPipelineLeadForm(scopedClientKey));
      setStatus("Contact created.");
      await refresh(response.lead.lead_id || response.lead.id, false);
    } catch (error) {
      setStatus(messageFor(error, "Contact could not be created."));
    }
  }

  function chooseClient(nextClientKey: string) {
    setClientKey(nextClientKey);
    window.localStorage.setItem(CRM_CLIENT_FILTER_KEY, nextClientKey);
    if (nextClientKey) {
      window.localStorage.setItem(SELECTED_CLIENT_KEY, nextClientKey);
      window.dispatchEvent(new CustomEvent("lead-ui-client-change", { detail: { clientKey: nextClientKey } }));
    }
    setSelectedLeadId(null);
    setDetail(null);
  }

  return (
    <div className="react-workflow-page react-records-page two-column-shell split-shell lead-details-shell" data-testid="react-records-page">
      <aside className="pane">
        <div className="pane-header">
          <div className="pane-title">
            <h3>{showArchived ? "Archived contacts" : "All contacts"}</h3>
            <div className="meta-text">{showArchived ? "Records removed from active daily work." : "Active CRM records and lead details."}</div>
          </div>
          <div className="actions">
            <button className="small" type="button" onClick={openAddPanel}>Add contact</button>
            <button className={`small ghost ${showArchived ? "active" : ""}`} type="button" onClick={() => setArchivedMode(!showArchived)}>
              {showArchived ? "Active" : "Archived"}
            </button>
            <span className="badge">{list.total}</span>
          </div>
        </div>
        <div className="pane-body">
          {loadStatus === "error" ? <InlineError message={status} onRetry={() => setRetryVersion((current) => current + 1)} /> : null}
          <div className="records-toolbar">
            {!isClientRole ? (
              <select
                aria-label="Filter records by client"
                value={clientKey}
                onChange={(event) => chooseClient(event.currentTarget.value)}
              >
                <option value="">All clients</option>
                {clients.map((client) => (
                  <option key={client.client_key} value={client.client_key}>
                    {client.business_name}
                  </option>
                ))}
              </select>
            ) : null}
            <input
              aria-label="Search records"
              placeholder="Search contacts, tags, campaigns..."
              value={search}
              onChange={(event) => setSearch(event.currentTarget.value)}
            />
          </div>
          {addOpen ? (
            <PipelineAddLeadPanel
              clients={clients}
              isClientRole={isClientRole}
              stages={stages}
              value={leadForm}
              onCancel={() => setAddOpen(false)}
              onChange={updateLeadForm}
              onSubmit={submitLead}
            />
          ) : null}
          <div className="compact-list">
            {list.items.length ? list.items.map((lead) => (
              <RecordsLeadListCard
                key={lead.lead_id}
                lead={lead}
                selected={lead.lead_id === selectedLeadId}
                onSelect={setSelectedLeadId}
              />
            )) : (
              <div className="empty-state">{showArchived ? "No archived contacts yet." : "No active contacts match this view."}</div>
            )}
          </div>
        </div>
      </aside>
      <section className="pane focus-surface lead-detail-pane">
        {detail ? (
          <>
            <div className="pane-header">
              <div className="pane-title lead-detail-header">
                <div className="lead-detail-title-row">
                  <h3>{detail.lead.display_name || "Contact record"}</h3>
                  {renderStageBadge(detail.lead.crm_stage)}
                  {conversationStateBadge(detail.lead.conversation_state || detail.lead.current_state || "", detail.lead.crm_stage)}
                </div>
                <div className="lead-detail-subline">
                  <CopyInline value={formatPhone(detail.lead.phone)} copyValue={detail.lead.phone} label="phone number" />
                  {detail.lead.phone && detail.lead.email ? <span className="lead-detail-sep">·</span> : null}
                  <CopyInline value={detail.lead.email} copyValue={detail.lead.email} label="email" />
                </div>
                <div className="lead-detail-meta">
                  {[formatSource(detail.lead.source), formatDateTime(detail.lead.last_activity_at), detail.lead.owner ? `Owner: ${detail.lead.owner}` : "", detail.client.business_name].filter(Boolean).join(" · ")}
                </div>
              </div>
              <div className="actions">
                <button className="small" type="button" data-action="open-contact-drawer" data-lead-id={detail.lead.id}>
                  Message
                </button>
                <button className="small ghost" type="button" data-action="set-view" data-view="crm">
                  Back to pipeline
                </button>
                <button className="small ghost" type="button" onClick={() => void setArchived(!selectedArchived)}>
                  {selectedArchived ? "Restore" : "Archive"}
                </button>
              </div>
            </div>
            <div className="pane-body stack">
              {status ? <div className="meta-text" role="status">{status}</div> : null}
              <div className="lead-record-layout">
                <div className="lead-record-main stack">
                  <section className="detail-card lead-section lead-form-section">
                    <div className="title">Form answers</div>
                    <FormAnswerList rows={formAnswerRows(detail.lead.form_answers, detail.lead.summary_lines)} />
                  </section>
                  <LeadInsightsPanel detail={detail} />
                  <section className="detail-card lead-section lead-stage-card">
                    <div className="lead-section-head">
                      <div>
                        <div className="title">Stage</div>
                        <div className="meta-text">Update only when the contact clearly moves forward or becomes inactive.</div>
                      </div>
                      <div className="chip-row">
                        {renderStageBadge(detail.lead.crm_stage)}
                        {conversationStateBadge(detail.lead.conversation_state || detail.lead.current_state || "", detail.lead.crm_stage)}
                      </div>
                    </div>
                    <StageControl stages={stages} current={detail.lead.crm_stage} onSave={(stage) => void saveStage(stage)} />
                  </section>
                  <section className="detail-card lead-section lead-tags-card">
                    <div className="title">Tags</div>
                    <div className="lead-tags-row">
                      <div className="chip-row">
                        {recordTags.map((tag) => renderTag(tag, () => void removeTag(tag)))}
                        {!recordTags.length ? <span className="meta-text">No tags yet.</span> : null}
                      </div>
                      <form className="lead-combo-control lead-tag-control" onSubmit={(event) => void submitTag(event)}>
                        <input value={tagText} onChange={(event) => setTagText(event.currentTarget.value)} placeholder="add tag" />
                        <button className="combo-action" type="submit">Add</button>
                      </form>
                    </div>
                  </section>
                  <section className="detail-card lead-section lead-tasks-card">
                    <div className="item-title-row">
                      <div className="title">Tasks</div>
                      <span className="badge">{detail.tasks.filter((task) => task.status !== "done").length} open</span>
                    </div>
                    <details className="lead-inline-disclosure">
                      <summary>Add task</summary>
                      <form className="lead-task-form" onSubmit={(event) => void submitTask(event)}>
                        <input value={taskTitle} onChange={(event) => setTaskTitle(event.currentTarget.value)} placeholder="Task title" />
                        <input type="date" value={taskDueDate} onChange={(event) => setTaskDueDate(event.currentTarget.value)} />
                        <textarea value={taskDescription} onChange={(event) => setTaskDescription(event.currentTarget.value)} placeholder="Optional details" />
                        <button className="small" type="submit">Add task</button>
                      </form>
                    </details>
                    <TaskList tasks={detail.tasks} onToggle={toggleTask} />
                  </section>
                  <section className="detail-card lead-section lead-notes-card">
                    <div className="title">{isClientRole ? "Private notes" : "Internal notes"}</div>
                    <NoteList notes={detail.notes} />
                    <form className="lead-combo-control lead-note-control" onSubmit={(event) => void submitNote(event)}>
                      <textarea value={noteText} onChange={(event) => setNoteText(event.currentTarget.value)} placeholder={isClientRole ? "Add a private note." : "Add an internal note."} />
                      <button className="combo-action" type="submit">Add</button>
                    </form>
                  </section>
                  <details className="detail-card detail-disclosure lead-section">
                    <summary>Activity history</summary>
                    <div className="detail-disclosure-body">
                      <TimelineList events={detail.timeline || detail.audit_events || []} />
                    </div>
                  </details>
                </div>
                <div className="lead-record-side stack">
                  <section className="detail-card lead-section lead-messages-card">
                    <div className="title">Recent messages</div>
                    <MessageList messages={detail.messages.slice(-10)} />
                  </section>
                </div>
              </div>
            </div>
          </>
        ) : (
          <div className="empty-state">No contacts available yet.</div>
        )}
      </section>
    </div>
  );
}

function RecordsLeadListCard({
  lead,
  selected,
  onSelect
}: {
  lead: LeadListItem;
  selected: boolean;
  onSelect: (leadId: number) => void;
}) {
  const displayName = lead.lead_name || formatPhone(lead.phone) || `Contact ${lead.lead_id}`;
  const tags = dedupeTags(lead.tags || [], lead.crm_stage).slice(0, 3);
  const stateBadge = conversationStateBadge(lead.conversation_state, lead.crm_stage);
  return (
    <article
      className={`lead-list-card ${selected ? "active" : ""}`}
      data-lead-id={lead.lead_id}
      role="button"
      tabIndex={0}
      onClick={() => onSelect(lead.lead_id)}
      onKeyDown={(event) => {
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          onSelect(lead.lead_id);
        }
      }}
    >
      <div className="item-title-row">
        <div className="item-title">{displayName}</div>
        {renderStageBadge(lead.crm_stage)}
      </div>
      <div className="item-subtitle">{pipelineMetaLine(lead, false)}</div>
      <div className="item-snippet">{lead.last_message_snippet || lead.lead_summary || lead.recommended_follow_up || "No recent activity."}</div>
      <div className="lead-list-status">
        {stateBadge}
        {tags.map((tag) => renderTag(tag))}
      </div>
      <div className="lead-list-meta">
        <span>Last activity</span>
        <span>{formatDateTime(lead.last_activity_at)}</span>
      </div>
    </article>
  );
}

function LeadInsightsPanel({ detail }: { detail: LeadDetailPayload }) {
  const rows = leadInsightRows(detail);
  if (!rows.length) return null;
  return (
    <section className="detail-card lead-section lead-insights-card">
      <div className="title">CRM insight</div>
      <FormAnswerList rows={rows} />
    </section>
  );
}

function leadInsightRows(detail: LeadDetailPayload) {
  const rows: Array<{ label: string; value: string }> = [];
  const lead = detail.lead;
  if (lead.lead_score !== null && lead.lead_score !== undefined && String(lead.lead_score).trim()) {
    rows.push({ label: "Lead score", value: String(lead.lead_score) });
  }
  if (lead.intent_level) rows.push({ label: "Intent", value: lead.intent_level });
  if (lead.estimated_value !== null && lead.estimated_value !== undefined && String(lead.estimated_value).trim()) {
    rows.push({ label: "Estimated value", value: formatMoneyLike(lead.estimated_value) });
  }
  if (lead.campaign_name) rows.push({ label: "Campaign", value: lead.campaign_name });
  if (lead.recommended_follow_up) rows.push({ label: "Recommended follow-up", value: lead.recommended_follow_up });
  return rows;
}

function TimelineList({ events }: { events: Array<Record<string, unknown>> }) {
  if (!events.length) return <div className="empty-state">No visible activity yet.</div>;
  return (
    <div className="compact-list">
      {events.map((event, index) => {
        const title = String(event.event_type || event.type || event.label || "Activity");
        const timestamp = String(event.created_at || event.timestamp || event.at || "");
        const detail = timelineDetail(event);
        return (
          <div className="preview-item" key={String(event.id ?? `${title}-${timestamp}-${index}`)}>
            <div className="item-title">{formatTag(title)}</div>
            {detail ? <div className="item-snippet">{detail}</div> : null}
            <div className="meta-text">{formatDateTime(timestamp)}</div>
          </div>
        );
      })}
    </div>
  );
}

export function TasksPage({ onReadyChange, selectedClientKey = "", searchQuery = "" }: WorkflowPageProps) {
  const auth = useAuth();
  const [tasks, setTasks] = useState<TasksPayload | null>(null);
  const [clients, setClients] = useState<ClientSummary[]>([]);
  const [clientKey, setClientKey] = useState(() => selectedClientKey || window.localStorage.getItem(SELECTED_CLIENT_KEY) || window.localStorage.getItem(CRM_CLIENT_FILTER_KEY) || "");
  const [statusFilter, setStatusFilter] = useState(() => window.localStorage.getItem("lead-ui-tasks-status") || "all");
  const [search, setSearch] = useState(searchQuery);
  const [actionStatus, setActionStatus] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);
  const [retryVersion, setRetryVersion] = useState(0);
  const requestId = useRef(0);
  const debouncedSearch = useDebouncedValue(search, 250);

  const isClientRole = auth.status === "ready" && auth.session.role === "client";

  const load = () => {
    if (auth.status !== "ready") return;
    const currentRequest = ++requestId.current;
    setLoading(true);
    onReadyChange?.(false);
    setError("");
    const scopedClientKey = isClientRole ? auth.session.client_key || "" : clientKey;
    const clientsRequest = isClientRole ? Promise.resolve<ClientSummary[]>([]) : fetchClients();
    Promise.all([
      clientsRequest,
      fetchTasks({
        client_key: scopedClientKey || undefined,
        q: debouncedSearch.trim() || undefined,
        status: statusFilter !== "all" ? statusFilter : undefined
      })
    ])
      .then(([clientItems, payload]) => {
        if (currentRequest !== requestId.current) return;
        setClients(clientItems);
        setTasks(payload);
        setLoading(false);
        onReadyChange?.(true);
      })
      .catch((loadError: unknown) => {
        if (currentRequest !== requestId.current) return;
        setError(messageFor(loadError, "Tasks unavailable"));
        setLoading(false);
        onReadyChange?.(false);
      });
  };

  useEffect(() => {
    if (auth.status === "ready") load();
    else onReadyChange?.(false);
  }, [auth.status, clientKey, statusFilter, debouncedSearch, retryVersion]);

  useEffect(() => {
    if (selectedClientKey) setClientKey(selectedClientKey);
    setSearch(searchQuery);
  }, [selectedClientKey, searchQuery]);

  useEffect(() => {
    if (auth.status !== "ready" || auth.session.role !== "admin") return;
    const onClientChange = (event: Event) => {
      const nextClientKey = (event as CustomEvent<{ clientKey?: string }>).detail?.clientKey || "";
      setClientKey(nextClientKey);
      window.localStorage.setItem(CRM_CLIENT_FILTER_KEY, nextClientKey);
    };
    window.addEventListener("lead-ui-client-change", onClientChange);
    return () => window.removeEventListener("lead-ui-client-change", onClientChange);
  }, [auth.status]);

  if (!tasks) {
    return <WorkflowLoadState title="Tasks" status={loading ? "loading" : "error"} error={error} onRetry={() => setRetryVersion((current) => current + 1)} />;
  }

  async function toggle(task: LeadTask) {
    setActionStatus(task.status === "done" ? "Reopening task..." : "Marking task done...");
    try {
      await updateTask(task.id, { status: task.status === "done" ? "open" : "done" });
      setActionStatus(task.status === "done" ? "Task reopened." : "Task marked done.");
      load();
    } catch (caught) {
      setActionStatus(messageFor(caught, "Task could not be updated."));
    }
  }

  function chooseClient(nextClientKey: string) {
    setClientKey(nextClientKey);
    window.localStorage.setItem(CRM_CLIENT_FILTER_KEY, nextClientKey);
    if (nextClientKey) {
      window.localStorage.setItem(SELECTED_CLIENT_KEY, nextClientKey);
      window.dispatchEvent(new CustomEvent("lead-ui-client-change", { detail: { clientKey: nextClientKey } }));
    }
  }

  function chooseStatus(nextStatus: string) {
    setStatusFilter(nextStatus);
    window.localStorage.setItem("lead-ui-tasks-status", nextStatus);
  }

  function clearFilters() {
    setClientKey("");
    setStatusFilter("all");
    setSearch("");
    window.localStorage.removeItem(CRM_CLIENT_FILTER_KEY);
    window.localStorage.setItem("lead-ui-tasks-status", "all");
  }

  const todayKey = dateKeyInTimeZone(new Date(), "local");
  const groupedTasks = groupTasksByBucket(tasks.items, todayKey);
  const summary = taskSummary(tasks.items, todayKey);
  const hasFilters = Boolean(clientKey || search.trim() || statusFilter !== "all");

  return (
    <div className="react-workflow-page react-tasks-page" data-testid="react-tasks-page">
      <section className="surface">
        <div className="surface-header">
          <div className="surface-title">
            <div>
              <h2>Tasks</h2>
              <div className="surface-subtitle">Follow-up queue across contacts, grouped by what needs attention first.</div>
            </div>
          </div>
          <div className="toolbar-right">
            <span className="badge">{tasks.counts.open || 0} open</span>
            <span className="badge">{tasks.counts.done || 0} done</span>
            <button className="small ghost" type="button" onClick={load}>Refresh</button>
          </div>
        </div>
        <div className="toolbar">
          <div className="toolbar-left">
            {!isClientRole ? (
              <select aria-label="Filter tasks by client" value={clientKey} onChange={(event) => chooseClient(event.currentTarget.value)}>
                <option value="">All clients</option>
                {clients.map((client) => (
                  <option key={client.client_key} value={client.client_key}>
                    {client.business_name}
                  </option>
                ))}
              </select>
            ) : null}
            <select aria-label="Filter tasks by status" value={statusFilter} onChange={(event) => chooseStatus(event.currentTarget.value)}>
              <option value="all">All statuses</option>
              <option value="open">Open</option>
              <option value="done">Done</option>
            </select>
            <input
              aria-label="Search tasks"
              type="search"
              placeholder="Search task, contact, phone, client..."
              value={search}
              onChange={(event) => setSearch(event.currentTarget.value)}
            />
          </div>
          <div className="toolbar-right">
            <div className="chip-row">
              <span className="tag err">Overdue {summary.overdue}</span>
              <span className="tag warn">Today {summary.today}</span>
              <span className="tag info">Upcoming {summary.upcoming}</span>
            </div>
            {hasFilters ? <button className="small ghost" type="button" onClick={clearFilters}>Clear</button> : null}
          </div>
        </div>
        {actionStatus ? <div className="meta-text" role="status">{actionStatus}</div> : null}
        {error ? <InlineError message={error} onRetry={() => setRetryVersion((current) => current + 1)} /> : null}
        {loading ? <div className="meta-text" role="status">Refreshing tasks...</div> : null}
      </section>
      <section className="surface stack focus-surface react-table-surface">
        {tasks.items.length ? <TasksGroupedTable grouped={groupedTasks} todayKey={todayKey} onToggle={toggle} /> : <div className="empty-state">No tasks match the current filters.</div>}
      </section>
    </div>
  );
}

function TasksGroupedTable({
  grouped,
  todayKey,
  onToggle
}: {
  grouped: TaskBucketGroup[];
  todayKey: string;
  onToggle: (task: LeadTask) => Promise<void> | void;
}) {
  return (
    <div className="table-wrap react-tasks-table-wrap">
      <table>
        <thead>
          <tr>
            <th>Status</th>
            <th>Task</th>
            <th>Contact</th>
            <th>Client</th>
            <th>Due</th>
            <th>Stage</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          {grouped.map((group) => (
            <TaskGroupRows group={group} key={group.bucket} todayKey={todayKey} onToggle={onToggle} />
          ))}
        </tbody>
      </table>
    </div>
  );
}

function TaskGroupRows({ group, todayKey, onToggle }: { group: TaskBucketGroup; todayKey: string; onToggle: (task: LeadTask) => Promise<void> | void }) {
  return (
    <>
      <tr className="task-group-row">
        <td colSpan={7}>
          <span className={`tag ${taskBucketTone(group.bucket)}`}>{group.bucket}</span>
          <span className="meta-text">{group.tasks.length} task{group.tasks.length === 1 ? "" : "s"}</span>
        </td>
      </tr>
      {group.tasks.map((task) => (
        <TaskTableRow key={task.id} task={task} todayKey={todayKey} onToggle={onToggle} />
      ))}
    </>
  );
}

function TaskTableRow({ task, todayKey, onToggle }: { task: LeadTask; todayKey: string; onToggle: (task: LeadTask) => Promise<void> | void }) {
  const bucket = taskBucketForTask(task, todayKey);
  return (
    <tr className={`task-row ${taskBucketClass(bucket)}`}>
      <td data-label="Status">{renderTag(task.status)}</td>
      <td data-label="Task">
        <strong>{task.title}</strong>
        {task.description ? <div className="meta-text">{task.description}</div> : null}
      </td>
      <td data-label="Contact">
        {task.lead_name || "-"}
        <div className="meta-text mono">{formatPhone(task.lead_phone || "")}</div>
      </td>
      <td data-label="Client">{task.client_name || "-"}</td>
      <td data-label="Due" className="mono">{formatTaskDue(task, bucket)}</td>
      <td data-label="Stage">{renderStageBadge(task.crm_stage || "") || "-"}</td>
      <td data-label="Actions">
        <div className="actions">
          <button className="small ghost" type="button" onClick={() => void onToggle(task)}>
            {task.status === "done" ? "Reopen" : "Done"}
          </button>
          {task.lead_id ? <button className="small ghost" type="button" data-action="open-contact-drawer" data-lead-id={task.lead_id} data-source="task">Message</button> : null}
          {task.lead_id ? <button className="small ghost" type="button" data-action="open-crm-lead" data-lead-id={task.lead_id}>Open</button> : null}
        </div>
      </td>
    </tr>
  );
}

export function CalendarPage({ onReadyChange, quickAction, selectedClientKey = "" }: WorkflowPageProps) {
  const auth = useAuth();
  const [clients, setClients] = useState<ClientSummary[]>([]);
  const [clientKey, setClientKey] = useState("");
  const [calendar, setCalendar] = useState<{ payload: Awaited<ReturnType<typeof fetchCalendar>>; tasks: TasksPayload; leads: CrmLeadsPayload } | null>(null);
  const [selectedDateKey, setSelectedDateKey] = useState(() => window.localStorage.getItem("lead-ui-calendar-day") || "");
  const [calendarMonth, setCalendarMonth] = useState(() => window.localStorage.getItem("lead-ui-calendar-month") || "");
  const [loadStatus, setLoadStatus] = useState<"loading" | "ready" | "error">("loading");
  const [error, setError] = useState("");
  const [actionStatus, setActionStatus] = useState("");
  const [busy, setBusy] = useState(false);
  const [meetingOpen, setMeetingOpen] = useState(false);
  const [meetingLeadId, setMeetingLeadId] = useState<number | null>(null);
  const [retryVersion, setRetryVersion] = useState(0);
  const requestId = useRef(0);

  useEffect(() => {
    if (auth.status !== "ready") {
      onReadyChange?.(false);
      return;
    }
    let cancelled = false;
    setLoadStatus("loading");
    setError("");
    resolveCalendarClient(auth.session, selectedClientKey)
      .then(({ key, clients: resolvedClients }) => {
        if (cancelled) return;
        setClients(resolvedClients);
        setClientKey(key);
      })
      .catch((caught: unknown) => {
        if (cancelled) return;
        setError(messageFor(caught, "Calendar workspace unavailable"));
        setLoadStatus("error");
        onReadyChange?.(false);
      });
    return () => {
      cancelled = true;
    };
  }, [auth.status, selectedClientKey, retryVersion]);

  useEffect(() => {
    if (quickAction?.kind !== "add-meeting") return;
    setMeetingLeadId(quickAction.leadId || null);
    setMeetingOpen(true);
  }, [quickAction?.id]);

  const load = () => {
    const currentRequest = ++requestId.current;
    if (!clientKey) {
      setCalendar({
        payload: { client_key: "", booking_mode: "none", timezone: "local", items: [], total: 0 },
        tasks: { items: [], counts: {}, total: 0 },
        leads: { items: [], counts: {}, stages: FALLBACK_STAGES, total: 0 }
      });
      setLoadStatus("ready");
      onReadyChange?.(true);
      return;
    }
    setLoadStatus("loading");
    setError("");
    onReadyChange?.(false);
    Promise.all([
      fetchCalendar(clientKey),
      fetchTasks({ client_key: clientKey, status: "open" }),
      fetchCrmLeads({ archived: false, client_key: clientKey })
    ])
      .then(([payload, tasks, leads]) => {
        if (currentRequest !== requestId.current) return;
        setCalendar({ payload, tasks, leads });
        ensureCalendarFocusState(payload.timezone, selectedDateKey, calendarMonth, setSelectedDateKey, setCalendarMonth);
        setLoadStatus("ready");
        onReadyChange?.(true);
      })
      .catch((caught: unknown) => {
        if (currentRequest !== requestId.current) return;
        setError(messageFor(caught, "Calendar unavailable"));
        setLoadStatus("error");
        onReadyChange?.(false);
      });
  };

  useEffect(load, [clientKey, retryVersion]);

  if (!calendar) {
    return <WorkflowLoadState title="Calendar" status={loadStatus} error={error} onRetry={() => setRetryVersion((current) => current + 1)} />;
  }

  async function changeMeetingStatus(meeting: CalendarBooking, nextStatus: string) {
    setBusy(true);
    setActionStatus("Updating meeting...");
    try {
      await updateMeetingStatus(meeting.id, nextStatus);
      setActionStatus("Meeting updated.");
      load();
    } catch (caught) {
      setActionStatus(messageFor(caught, "Meeting could not be updated."));
    } finally {
      setBusy(false);
    }
  }

  async function removeMeeting(meeting: CalendarBooking) {
    const label = meeting.title || meeting.lead_name || "this meeting";
    if (!window.confirm(`Delete ${label}? This cannot be undone.`)) return;
    setBusy(true);
    setActionStatus("Deleting meeting...");
    try {
      await deleteMeeting(meeting.id);
      setActionStatus("Meeting deleted.");
      load();
    } catch (caught) {
      setActionStatus(messageFor(caught, "Meeting could not be deleted."));
    } finally {
      setBusy(false);
    }
  }

  async function addMeeting(payload: ManualMeetingCreatePayload) {
    if (!clientKey) {
      setActionStatus("Choose a client before adding a meeting.");
      return;
    }
    setBusy(true);
    setActionStatus("Adding meeting...");
    try {
      await createMeeting(clientKey, payload);
      setActionStatus("Meeting added.");
      setMeetingOpen(false);
      setMeetingLeadId(null);
      load();
    } catch (caught) {
      setActionStatus(messageFor(caught, "Meeting could not be added."));
    } finally {
      setBusy(false);
    }
  }

  const timeZone = normalizeTimeZone(calendar.payload.timezone);
  const todayKey = dateKeyInTimeZone(new Date(), timeZone);
  const focusedDateKey = selectedDateKey || todayKey;
  const focusedMonth = calendarMonth || monthKeyForDateKey(focusedDateKey);
  const openTasks = sortOpenTasks(calendar.tasks.items);
  const itemsByDate = meetingsByDate(calendar.payload.items, timeZone);
  const monthMeetings = calendar.payload.items.filter((meeting) => dateKeyInTimeZone(meeting.start_at, timeZone).startsWith(focusedMonth));
  const meetingsTodayCount = (itemsByDate.get(todayKey) || []).length;
  const selectedMeetings = itemsByDate.get(focusedDateKey) || [];
  const selectedTasks = openTasks.filter((task) => task.due_date === focusedDateKey);
  const dueTodayCount = openTasks.filter((task) => task.due_date === todayKey).length;
  const overdueCount = openTasks.filter((task) => task.due_date && task.due_date < todayKey).length;

  function selectDate(dateKey: string) {
    setSelectedDateKey(dateKey);
    setCalendarMonth(monthKeyForDateKey(dateKey));
    window.localStorage.setItem("lead-ui-calendar-day", dateKey);
    window.localStorage.setItem("lead-ui-calendar-month", monthKeyForDateKey(dateKey));
  }

  function shiftMonth(offset: number) {
    const nextMonth = shiftMonthKey(focusedMonth, offset);
    const nextSelectedDate = monthKeyForDateKey(focusedDateKey) === nextMonth ? focusedDateKey : `${nextMonth}-01`;
    setCalendarMonth(nextMonth);
    setSelectedDateKey(nextSelectedDate);
    window.localStorage.setItem("lead-ui-calendar-month", nextMonth);
    window.localStorage.setItem("lead-ui-calendar-day", nextSelectedDate);
  }

  function jumpToday() {
    selectDate(todayKey);
  }

  function chooseCalendarClient(nextClientKey: string) {
    setClientKey(nextClientKey);
    if (!nextClientKey) return;
    window.localStorage.setItem(SELECTED_CLIENT_KEY, nextClientKey);
    window.dispatchEvent(new CustomEvent("lead-ui-client-change", { detail: { clientKey: nextClientKey } }));
  }

  return (
    <div className="react-workflow-page react-calendar-page" data-testid="react-calendar-page">
      <section className="surface">
        <div className="surface-header">
          <div className="surface-title">
            <div>
              <h2>Calendar</h2>
              <div className="surface-subtitle">Booked meetings and open follow-ups for the selected client.</div>
            </div>
          </div>
          <div className="toolbar-right">
            {clients.length ? (
              <select aria-label="Select calendar client" value={clientKey} onChange={(event) => chooseCalendarClient(event.currentTarget.value)}>
                {clients.map((client) => (
                  <option key={client.client_key} value={client.client_key}>
                    {client.business_name}
                  </option>
                ))}
              </select>
            ) : null}
            {renderTag(calendar.payload.booking_mode === "internal" ? "internal calendar" : calendar.payload.booking_mode || "link only")}
            <span className="badge">{monthMeetings.length} this month</span>
            <span className="badge">{openTasks.length} open tasks</span>
            <span className="badge">{timeZone}</span>
          </div>
        </div>
        {actionStatus ? <div className="meta-text" role="status">{actionStatus}</div> : null}
        {loadStatus === "error" ? <InlineError message={error} onRetry={() => setRetryVersion((current) => current + 1)} /> : null}
        {meetingOpen ? (
          <CalendarMeetingForm
            busy={busy}
            clientKey={clientKey}
            defaultLeadId={meetingLeadId}
            leads={calendar.leads.items}
            selectedDateKey={focusedDateKey}
            timeZone={timeZone}
            onCancel={() => {
              setMeetingOpen(false);
              setMeetingLeadId(null);
            }}
            onSubmit={addMeeting}
          />
        ) : null}
      </section>
      <div className="calendar-experience">
        <CalendarMonthView
          monthKey={focusedMonth}
          selectedDateKey={focusedDateKey}
          todayKey={todayKey}
          timeZone={timeZone}
          meetingsByDate={itemsByDate}
          monthMeetingCount={monthMeetings.length}
          onSelectDate={selectDate}
          onShiftMonth={shiftMonth}
          onToday={jumpToday}
        />
        <CalendarSidePanel
          dueTodayCount={dueTodayCount}
          meetingsTodayCount={meetingsTodayCount}
          openTasks={openTasks}
          overdueCount={overdueCount}
          selectedDateKey={focusedDateKey}
          selectedMeetings={selectedMeetings}
          selectedTasks={selectedTasks}
          timeZone={timeZone}
          todayKey={todayKey}
          onMeetingDelete={removeMeeting}
          onMeetingStatus={changeMeetingStatus}
          onAddMeeting={() => {
            setMeetingLeadId(null);
            setMeetingOpen(true);
          }}
          busy={busy}
          onTaskToggle={async (task) => {
            setBusy(true);
            setActionStatus("Updating task...");
            try {
              await updateTask(task.id, { status: task.status === "done" ? "open" : "done" });
              setActionStatus("Task updated.");
              load();
            } catch (caught) {
              setActionStatus(messageFor(caught, "Task could not be updated."));
            } finally {
              setBusy(false);
            }
          }}
        />
      </div>
    </div>
  );
}

function CalendarMonthView({
  monthKey,
  selectedDateKey,
  todayKey,
  timeZone,
  meetingsByDate,
  monthMeetingCount,
  onSelectDate,
  onShiftMonth,
  onToday
}: {
  monthKey: string;
  selectedDateKey: string;
  todayKey: string;
  timeZone: string;
  meetingsByDate: Map<string, CalendarBooking[]>;
  monthMeetingCount: number;
  onSelectDate: (dateKey: string) => void;
  onShiftMonth: (offset: number) => void;
  onToday: () => void;
}) {
  return (
    <section className="surface focus-surface calendar-month-card" aria-label="Monthly calendar">
      <div className="calendar-month-header">
        <div>
          <div className="calendar-month-label">{formatMonthLabel(monthKey, timeZone)}</div>
          <div className="calendar-month-meta">{monthMeetingCount} meeting{monthMeetingCount === 1 ? "" : "s"} this month · {timeZone}</div>
        </div>
        <div className="actions">
          <button className="small ghost" type="button" onClick={() => onShiftMonth(-1)}>Previous</button>
          <button className="small" type="button" onClick={onToday}>Today</button>
          <button className="small ghost" type="button" onClick={() => onShiftMonth(1)}>Next</button>
        </div>
      </div>
      <div className="calendar-weekdays" aria-hidden="true">
        {["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"].map((day) => <div className="calendar-weekday" key={day}>{day}</div>)}
      </div>
      <div className="calendar-month-grid">
        {buildCalendarMonthCells(monthKey).map((cell) => (
          <CalendarDayCell
            cell={cell}
            entries={meetingsByDate.get(cell.dateKey) || []}
            key={cell.dateKey}
            selectedDateKey={selectedDateKey}
            timeZone={timeZone}
            todayKey={todayKey}
            onSelectDate={onSelectDate}
          />
        ))}
      </div>
    </section>
  );
}

function CalendarDayCell({
  cell,
  entries,
  selectedDateKey,
  timeZone,
  todayKey,
  onSelectDate
}: {
  cell: CalendarMonthCell;
  entries: CalendarBooking[];
  selectedDateKey: string;
  timeZone: string;
  todayKey: string;
  onSelectDate: (dateKey: string) => void;
}) {
  const classes = [
    "calendar-day",
    cell.inMonth ? "" : "other-month",
    cell.dateKey === todayKey ? "today" : "",
    cell.dateKey === selectedDateKey ? "selected" : ""
  ].filter(Boolean).join(" ");
  const label = `${formatDateLabel(cell.dateKey, timeZone, { month: "long", day: "numeric" })}, ${entries.length} meeting${entries.length === 1 ? "" : "s"}`;
  return (
    <button className={classes} type="button" aria-label={label} onClick={() => onSelectDate(cell.dateKey)}>
      <div className="calendar-day-head">
        <div className="calendar-day-number">{cell.dayNumber}</div>
        <div className="calendar-day-summary">{entries.length ? `${entries.length} booked` : ""}</div>
      </div>
      <div className="calendar-day-events">
        {entries.slice(0, 2).map((meeting) => (
          <div className="calendar-event" key={meeting.id}>
            <div className="calendar-event-time">{formatTimeInTimeZone(meeting.start_at, timeZone)}</div>
            <div className="calendar-event-title">{meeting.lead_name || meeting.title || "Meeting"}</div>
          </div>
        ))}
        {entries.length ? null : <div className="calendar-event-more">Nothing booked</div>}
        {entries.length > 2 ? <div className="calendar-event-more">+{entries.length - 2} more</div> : null}
      </div>
    </button>
  );
}

function CalendarSidePanel({
  busy,
  dueTodayCount,
  meetingsTodayCount,
  openTasks,
  overdueCount,
  selectedDateKey,
  selectedMeetings,
  selectedTasks,
  timeZone,
  todayKey,
  onMeetingDelete,
  onMeetingStatus,
  onAddMeeting,
  onTaskToggle
}: {
  busy: boolean;
  dueTodayCount: number;
  meetingsTodayCount: number;
  openTasks: LeadTask[];
  overdueCount: number;
  selectedDateKey: string;
  selectedMeetings: CalendarBooking[];
  selectedTasks: LeadTask[];
  timeZone: string;
  todayKey: string;
  onMeetingDelete: (meeting: CalendarBooking) => Promise<void>;
  onMeetingStatus: (meeting: CalendarBooking, nextStatus: string) => Promise<void>;
  onAddMeeting: () => void;
  onTaskToggle: (task: LeadTask) => Promise<void>;
}) {
  return (
    <aside className="calendar-side">
      <section className="surface calendar-side-card">
        <div className="surface-title">
          <div>
            <h3>At a glance</h3>
            <div className="surface-subtitle">A quick planning snapshot for today and open follow-ups.</div>
          </div>
        </div>
        <div className="calendar-overview-grid">
          <CalendarOverviewStat label="Meetings today" value={meetingsTodayCount} />
          <CalendarOverviewStat label="Tasks due today" value={dueTodayCount} />
          <CalendarOverviewStat label="Overdue tasks" value={overdueCount} />
          <CalendarOverviewStat label="Open tasks" value={openTasks.length} />
        </div>
        <div className="meta-text">Click any day to inspect booked meetings and due tasks without leaving the page.</div>
      </section>

      <section className="surface calendar-side-card">
        <div className="surface-title">
          <div>
            <h3>{selectedDateKey === todayKey ? "Today" : formatDateLabel(selectedDateKey, timeZone, { month: "long", day: "numeric", weekday: "long" })}</h3>
            <div className="surface-subtitle">{selectedDateKey === todayKey ? "What is booked today, plus tasks that are due." : "Meetings and tasks due on the selected date."}</div>
          </div>
          <button className="small" type="button" onClick={onAddMeeting}>Add meeting</button>
        </div>
        <div>
          <label>Meetings</label>
          <div className="calendar-agenda-list">
            {selectedMeetings.length ? selectedMeetings.map((meeting) => (
              <CalendarMeetingItem busy={busy} meeting={meeting} timeZone={timeZone} key={meeting.id} onDelete={onMeetingDelete} onStatus={onMeetingStatus} />
            )) : <div className="empty-state">No meetings booked for this day.</div>}
          </div>
        </div>
        <div>
          <label>Tasks due</label>
          <div className="calendar-agenda-list">
            {selectedTasks.length ? selectedTasks.map((task) => (
              <CalendarTaskItem busy={busy} task={task} timeZone={timeZone} key={task.id} onToggle={onTaskToggle} />
            )) : <div className="empty-state">No tasks are due on this day.</div>}
          </div>
        </div>
      </section>

      <section className="surface calendar-side-card react-calendar-open-tasks">
        <div className="surface-title">
          <div>
            <h3>{openTasks.length ? `Tasks to do (${openTasks.length})` : "Tasks to do"}</h3>
            <div className="surface-subtitle">Open follow-ups, sorted by what needs attention first.</div>
          </div>
        </div>
        <div className="calendar-agenda-list">
          {openTasks.length ? openTasks.slice(0, 8).map((task) => (
            <CalendarTaskItem busy={busy} task={task} timeZone={timeZone} key={task.id} onToggle={onTaskToggle} />
          )) : <div className="empty-state">No open tasks right now.</div>}
        </div>
      </section>
    </aside>
  );
}

function CalendarOverviewStat({ label, value }: { label: string; value: number }) {
  return (
    <div className="calendar-overview-stat">
      <div className="label">{label}</div>
      <div className="value">{value}</div>
    </div>
  );
}

function CalendarMeetingItem({ busy, meeting, timeZone, onDelete, onStatus }: { busy: boolean; meeting: CalendarBooking; timeZone: string; onDelete: (meeting: CalendarBooking) => Promise<void>; onStatus: (meeting: CalendarBooking, nextStatus: string) => Promise<void> }) {
  return (
    <div className="calendar-agenda-item">
      <div className="item-title-row">
        <div className="item-title">{meeting.title || meeting.lead_name || "Booked meeting"}</div>
        {renderTag(String(meeting.status || "scheduled").replace(/_/g, " "))}
      </div>
      <div className="item-snippet">{meeting.lead_name || "No contact"} · {formatTimeInTimeZone(meeting.start_at, timeZone)} to {formatTimeInTimeZone(meeting.end_at, timeZone)}</div>
      {meeting.notes ? <div className="item-snippet">{meeting.notes}</div> : null}
      <div className="item-meta-row">
        <div className="meta-text">{meeting.lead_phone || "No contact details"}</div>
        <div className="actions calendar-item-actions">
          {meeting.lead_id ? <button className="small ghost" type="button" data-action="open-thread" data-lead-id={meeting.lead_id}>Message</button> : null}
          {meeting.lead_id ? <button className="small ghost" type="button" data-action="open-crm-lead" data-lead-id={meeting.lead_id}>Open</button> : null}
          <details className="action-menu">
            <summary className="small ghost">Actions</summary>
            <div className="action-menu-panel">
              <button className="small ghost" type="button" disabled={busy} onClick={() => void onStatus(meeting, "completed")}>Completed</button>
              <button className="small ghost" type="button" disabled={busy} onClick={() => void onStatus(meeting, "no_show")}>No Show</button>
              <button className="small ghost" type="button" disabled={busy} onClick={() => void onStatus(meeting, "cancelled")}>Cancel</button>
              <button className="small warn" type="button" disabled={busy} onClick={() => void onDelete(meeting)}>Delete</button>
            </div>
          </details>
        </div>
      </div>
    </div>
  );
}

function CalendarTaskItem({ busy, task, timeZone, onToggle }: { busy: boolean; task: LeadTask; timeZone: string; onToggle: (task: LeadTask) => Promise<void> }) {
  const dueText = task.due_date ? formatDateLabel(task.due_date, timeZone, { month: "short", day: "numeric", weekday: "short" }) : "No due date";
  return (
    <div className="calendar-agenda-item">
      <div className="item-title-row">
        <div className="item-title">{task.title || "Task"}</div>
        {renderTag(task.status)}
      </div>
      <div className="item-snippet">{task.description || "No extra details."}</div>
      <div className="item-meta-row">
        <div className="meta-text">{task.lead_name || "Contact"} · {dueText}</div>
        <div className="actions">
          <button className="small ghost" type="button" disabled={busy} onClick={() => void onToggle(task)}>{task.status === "done" ? "Reopen" : "Done"}</button>
          {task.lead_id ? <button className="small ghost" type="button" data-action="open-thread" data-lead-id={task.lead_id}>Message</button> : null}
          {task.lead_id ? <button className="small ghost" type="button" data-action="open-crm-lead" data-lead-id={task.lead_id}>Open</button> : null}
        </div>
      </div>
    </div>
  );
}

export function InboxPage({ onReadyChange, selectedClientKey = "", searchQuery = "" }: WorkflowPageProps) {
  const auth = useAuth();
  const [conversations, setConversations] = useState<ConversationListItem[]>([]);
  const [conversationTotal, setConversationTotal] = useState(0);
  const [thread, setThread] = useState<ConversationThreadPayload | null>(null);
  const [selectedLeadId, setSelectedLeadId] = useState<number | null>(() => Number(window.localStorage.getItem("lead-ui-active-lead") || 0) || null);
  const [message, setMessage] = useState("");
  const [mediaFile, setMediaFile] = useState<File | null>(null);
  const [pauseAfterSend, setPauseAfterSend] = useState(false);
  const [query, setQuery] = useState(searchQuery);
  const [stateFilter, setStateFilter] = useState(() => window.localStorage.getItem("lead-ui-inbox-state") || "all");
  const [dateFrom, setDateFrom] = useState(() => window.localStorage.getItem("lead-ui-inbox-date-from") || "");
  const [dateTo, setDateTo] = useState(() => window.localStorage.getItem("lead-ui-inbox-date-to") || "");
  const [note, setNote] = useState("");
  const [tagText, setTagText] = useState("");
  const [actionStatus, setActionStatus] = useState("");
  const [retryOutboundScope, setRetryOutboundScope] = useState<string | null>(null);
  const [loadStatus, setLoadStatus] = useState<"loading" | "ready" | "error">("loading");
  const [loadError, setLoadError] = useState("");
  const [busy, setBusy] = useState(false);
  const [retryVersion, setRetryVersion] = useState(0);
  const [paneSizes, setPaneSizes] = useState(() => loadInboxPaneSizes());
  const shellRef = useRef<HTMLDivElement>(null);
  const selectedLeadIdRef = useRef(selectedLeadId);
  const listRequestId = useRef(0);
  const threadRequestId = useRef(0);
  const debouncedQuery = useDebouncedValue(query, 250);
  const effectiveClientKey = auth.status === "ready" && auth.session.role === "client"
    ? auth.session.client_key || ""
    : selectedClientKey;
  const sandboxLeadId = thread && hasTag(thread.lead.tags || [], "sandbox") ? thread.lead.id : null;

  useEffect(() => {
    if (!sandboxLeadId) return;
    setMediaFile(null);
    setPauseAfterSend(false);
  }, [sandboxLeadId]);

  useEffect(() => {
    selectedLeadIdRef.current = selectedLeadId;
  }, [selectedLeadId]);

  useEffect(() => setQuery(searchQuery), [searchQuery]);

  const loadThread = useCallback(async (leadId: number | null, background = false) => {
    const currentRequest = ++threadRequestId.current;
    if (!leadId) {
      setThread(null);
      window.localStorage.removeItem("lead-ui-active-lead");
      if (!background) onReadyChange?.(true);
      return;
    }
    try {
      const payload = await fetchConversationThread(leadId);
      if (currentRequest !== threadRequestId.current) return;
      setThread(payload);
      window.localStorage.setItem("lead-ui-active-lead", String(leadId));
      if (!background) onReadyChange?.(true);
    } catch (error: unknown) {
      if (currentRequest !== threadRequestId.current) return;
      if (error instanceof ApiError && error.status === 404) {
        window.localStorage.removeItem("lead-ui-active-lead");
        selectedLeadIdRef.current = null;
        setSelectedLeadId(null);
        setThread(null);
        if (!background) onReadyChange?.(true);
        return;
      }
      throw error;
    }
  }, [onReadyChange]);

  const loadConversations = useCallback(async (background = false) => {
    const currentRequest = ++listRequestId.current;
    if (!background) {
      setLoadStatus("loading");
      setLoadError("");
      onReadyChange?.(false);
    }
    const payload = await fetchConversations({
      client_key: effectiveClientKey || undefined,
      date_from: dateFrom || undefined,
      date_to: dateTo || undefined,
      q: debouncedQuery.trim() || undefined,
      state: stateFilter !== "all" ? stateFilter : undefined
    });
    if (currentRequest !== listRequestId.current) throw STALE_REQUEST;
    setConversations(payload.items);
    setConversationTotal(payload.total);
    const currentLeadId = selectedLeadIdRef.current;
    const nextLeadId = currentLeadId && payload.items.some((item) => item.lead_id === currentLeadId)
      ? currentLeadId
      : shouldAutoSelectConversation()
        ? payload.items[0]?.lead_id ?? null
        : null;
    selectedLeadIdRef.current = nextLeadId;
    setSelectedLeadId(nextLeadId);
    return nextLeadId;
  }, [dateFrom, dateTo, debouncedQuery, effectiveClientKey, onReadyChange, stateFilter]);

  const refresh = useCallback(async (background = false) => {
    try {
      const leadId = await loadConversations(background);
      await loadThread(leadId, background);
      setLoadError("");
      setLoadStatus("ready");
    } catch (caught: unknown) {
      if (caught === STALE_REQUEST) return;
      setLoadError(messageFor(caught, "Inbox unavailable"));
      if (!background) {
        setLoadStatus("error");
        onReadyChange?.(false);
      }
    }
  }, [loadConversations, loadThread, onReadyChange]);

  useEffect(() => {
    if (auth.status === "ready") void refresh(false);
    else onReadyChange?.(false);
  }, [auth.status, effectiveClientKey, debouncedQuery, stateFilter, dateFrom, dateTo, retryVersion]);

  useEffect(() => {
    if (auth.status !== "ready") return;
    const poll = () => {
      if (document.visibilityState === "visible") void refresh(true);
    };
    const timer = window.setInterval(poll, 4_000);
    document.addEventListener("visibilitychange", poll);
    return () => {
      window.clearInterval(timer);
      document.removeEventListener("visibilitychange", poll);
    };
  }, [auth.status, refresh]);

  function chooseConversation(leadId: number) {
    selectedLeadIdRef.current = leadId;
    setSelectedLeadId(leadId);
    setLoadError("");
    setRetryOutboundScope(null);
    void loadThread(leadId).catch((caught: unknown) => setLoadError(messageFor(caught, "Conversation unavailable")));
  }

  function updateInboxFilter(kind: "state" | "dateFrom" | "dateTo", value: string) {
    if (kind === "state") {
      setStateFilter(value);
      window.localStorage.setItem("lead-ui-inbox-state", value);
    } else if (kind === "dateFrom") {
      setDateFrom(value);
      window.localStorage.setItem("lead-ui-inbox-date-from", value);
    } else {
      setDateTo(value);
      window.localStorage.setItem("lead-ui-inbox-date-to", value);
    }
  }

  function clearInboxFilters() {
    setQuery("");
    setStateFilter("all");
    setDateFrom("");
    setDateTo("");
    window.localStorage.removeItem("lead-ui-inbox-state");
    window.localStorage.removeItem("lead-ui-inbox-date-from");
    window.localStorage.removeItem("lead-ui-inbox-date-to");
  }

  async function runAction(startMessage: string, successMessage: string, action: () => Promise<void>) {
    if (busy) return;
    setBusy(true);
    setActionStatus(startMessage);
    setRetryOutboundScope(null);
    try {
      await action();
      setActionStatus(successMessage);
    } catch (caught: unknown) {
      setActionStatus(messageFor(caught, "Action failed"));
    } finally {
      setBusy(false);
    }
  }

  async function submitMessage(event: FormEvent) {
    event.preventDefault();
    if (busy || !thread || (!message.trim() && !mediaFile)) return;
    const leadId = thread.lead.id;
    const body = message.trim();
    const sandboxThread = hasTag(thread.lead.tags || [], "sandbox");
    if (sandboxThread && !body) return;
    const requestScope = `inbox-message-${leadId}`;
    const idempotencyKey = sandboxThread
      ? undefined
      : outboundRequestKey(requestScope, JSON.stringify({
          body,
          pauseAfterSend,
          media: mediaFile ? [mediaFile.name, mediaFile.type, mediaFile.size, mediaFile.lastModified] : null
        }));
    const successMessage = sandboxThread
      ? "AI replied to the test lead in sandbox."
      : mediaFile
        ? pauseAfterSend ? "Media sent and AI paused." : "Media sent."
        : pauseAfterSend ? "Reply sent and AI paused." : "Reply sent.";
    setBusy(true);
    setActionStatus(sandboxThread ? "Sending as the test lead and waiting for GPT..." : "Sending reply...");
    setRetryOutboundScope(null);
    try {
      if (sandboxThread) {
        await sendSandboxMessage(leadId, body);
      } else if (mediaFile) {
        await sendManualMediaMessage(leadId, body, mediaFile, idempotencyKey);
      } else {
        await sendManualMessage(leadId, body, pauseAfterSend, idempotencyKey);
      }

      // Once the provider accepted the message, never present a later refresh/control
      // failure as a send failure that invites a duplicate retry.
      if (!sandboxThread) clearOutboundRequestKey(requestScope);
      setMessage("");
      setMediaFile(null);
      setPauseAfterSend(false);
      if (sandboxThread) window.localStorage.setItem("lead-ui-sandbox-lead", String(leadId));

      let completionMessage = successMessage;
      if (!sandboxThread && mediaFile && pauseAfterSend) {
        try {
          await updateAgentControl(leadId, true, "manual_media_reply_takeover", "Paused automatically after a manual media message.");
        } catch (caught: unknown) {
          completionMessage = `Media sent, but AI could not be paused: ${messageFor(caught, "AI control failed.")}`;
        }
      }

      try {
        await refresh(true);
      } catch (caught: unknown) {
        completionMessage = `${completionMessage} The inbox could not refresh: ${messageFor(caught, "refresh failed.")}`;
      }
      setActionStatus(completionMessage);
    } catch (caught: unknown) {
      setActionStatus(messageFor(caught, sandboxThread ? "The sandbox turn could not be completed." : "Reply could not be sent."));
      if (!sandboxThread) setRetryOutboundScope(requestScope);
    } finally {
      setBusy(false);
    }
  }

  async function submitNote(event: FormEvent) {
    event.preventDefault();
    if (!thread || !note.trim()) return;
    const leadId = thread.lead.id;
    const body = note.trim();
    await runAction("Saving note...", "Note saved.", async () => {
      await addConversationNote(leadId, body);
      setNote("");
      await loadThread(leadId, true);
    });
  }

  async function handoff() {
    if (!thread) return;
    const leadId = thread.lead.id;
    await runAction("Marking handoff...", "Marked for handoff.", async () => {
      await markConversationHandoff(leadId);
      await refresh(true);
    });
  }

  async function archive() {
    if (!thread) return;
    const leadId = thread.lead.id;
    await runAction("Archiving conversation...", "Conversation archived.", async () => {
      await archiveLead(leadId, true);
      await refresh(true);
    });
  }

  async function saveStage(stage: string) {
    if (!thread) return;
    const leadId = thread.lead.id;
    await runAction("Updating stage...", "Stage updated.", async () => {
      await updateLeadStage(leadId, stage);
      await refresh(true);
    });
  }

  async function submitTag(event: FormEvent) {
    event.preventDefault();
    if (!thread || !tagText.trim()) return;
    const leadId = thread.lead.id;
    const tag = tagText.trim();
    await runAction("Adding tag...", "Tag added.", async () => {
      await addLeadTag(leadId, tag);
      setTagText("");
      await loadThread(leadId, true);
    });
  }

  async function removeTag(tag: string) {
    if (!thread) return;
    const leadId = thread.lead.id;
    await runAction("Removing tag...", "Tag removed.", async () => {
      await deleteLeadTag(leadId, tag);
      await loadThread(leadId, true);
    });
  }

  async function sendThreadBookingLink() {
    if (busy || !thread) return;
    const leadId = thread.lead.id;
    const messageText = "Here is the booking link whenever you are ready.";
    const requestScope = `inbox-booking-link-${leadId}`;
    const requestKey = outboundRequestKey(requestScope, messageText);
    setBusy(true);
    setActionStatus("Sending booking link...");
    setRetryOutboundScope(null);
    try {
      await sendBookingLink(leadId, messageText, requestKey);
      clearOutboundRequestKey(requestScope);
      let completionMessage = "Booking link sent.";
      try {
        await refresh(true);
      } catch (caught: unknown) {
        completionMessage = `Booking link sent, but the inbox could not refresh: ${messageFor(caught, "refresh failed.")}`;
      }
      setActionStatus(completionMessage);
    } catch (caught: unknown) {
      setActionStatus(messageFor(caught, "Booking link could not be sent."));
      setRetryOutboundScope(requestScope);
    } finally {
      setBusy(false);
    }
  }

  async function setThreadAgentPaused(paused: boolean) {
    if (!thread) return;
    const leadId = thread.lead.id;
    await runAction(paused ? "Pausing AI..." : "Resuming AI...", paused ? "AI paused for this contact." : "AI resumed for this contact.", async () => {
      await updateAgentControl(
        leadId,
        paused,
        paused ? "operator_paused" : "operator_resumed",
        paused ? "Paused from the inbox." : "Resumed from the inbox."
      );
      await refresh(true);
    });
  }

  async function permanentlyDeleteConversation() {
    if (!thread || auth.status !== "ready" || auth.session.role !== "admin") return;
    const leadId = thread.lead.id;
    const label = thread.lead.display_name || `contact ${leadId}`;
    if (!window.confirm(`Delete ${label} and the full conversation history? This cannot be undone.`)) return;
    await runAction("Deleting conversation...", "Conversation deleted.", async () => {
      await deleteConversation(leadId);
      selectedLeadIdRef.current = null;
      setSelectedLeadId(null);
      setThread(null);
      window.localStorage.removeItem("lead-ui-active-lead");
      await refresh(true);
    });
  }

  function resizePane(edge: "left" | "right", delta: number) {
    setPaneSizes((current) => {
      const max = edge === "left" ? 520 : 420;
      const next = { ...current, [edge]: clamp(current[edge] + delta, 240, max) };
      saveInboxPaneSizes(next);
      return next;
    });
  }

  function startPaneResize(edge: "left" | "right", startX: number) {
    const shellWidth = shellRef.current?.getBoundingClientRect().width || window.innerWidth;
    const start = { ...paneSizes };
    let latest = start;
    const move = (event: PointerEvent) => {
      const pointerDelta = event.clientX - startX;
      const widthDelta = edge === "left" ? pointerDelta : -pointerDelta;
      const max = edge === "left" ? 520 : 420;
      const other = edge === "left" ? start.right : start.left;
      const maxCombined = Math.max(480, shellWidth - 420);
      const width = Math.min(clamp(start[edge] + widthDelta, 240, max), Math.max(240, maxCombined - other));
      latest = { ...start, [edge]: width };
      setPaneSizes(latest);
    };
    const stop = () => {
      window.removeEventListener("pointermove", move);
      window.removeEventListener("pointerup", stop);
      saveInboxPaneSizes(latest);
    };
    window.addEventListener("pointermove", move);
    window.addEventListener("pointerup", stop);
  }

  if (loadStatus !== "ready" && !conversations.length && !thread) {
    return <WorkflowLoadState title="Inbox" status={loadStatus} error={loadError} onRetry={() => setRetryVersion((current) => current + 1)} />;
  }

  return (
    <div className="react-workflow-page react-inbox-page" data-testid="react-inbox-page">
      <section className="surface react-inbox-command">
        <div className="surface-header">
          <div className="surface-title">
            <div>
              <h2>Inbox</h2>
              <div className="surface-subtitle">Conversation list, live thread, and contact context in one workspace.</div>
            </div>
          </div>
          <div className="toolbar-right">
            <button className="small ghost" type="button" disabled={loadStatus === "loading"} onClick={() => void refresh(false)}>Refresh</button>
            <span className="badge">{conversationTotal || conversations.length} conversations</span>
            {thread ? renderStageBadge(thread.lead.crm_stage) : null}
          </div>
        </div>
        <div className="toolbar react-inbox-filters" aria-label="Conversation filters">
          <div className="toolbar-left">
            <input
              type="search"
              aria-label="Search conversations"
              placeholder="Search contacts or messages"
              value={query}
              onChange={(event) => setQuery(event.currentTarget.value)}
            />
            <select aria-label="Filter conversations by state" value={stateFilter} onChange={(event) => updateInboxFilter("state", event.currentTarget.value)}>
              <option value="all">All states</option>
              {INBOX_STATES.map((state) => <option value={state} key={state}>{formatConversationState(state)}</option>)}
            </select>
            <label className="react-compact-field">From<input aria-label="Conversation date from" type="date" value={dateFrom} onChange={(event) => updateInboxFilter("dateFrom", event.currentTarget.value)} /></label>
            <label className="react-compact-field">To<input aria-label="Conversation date to" type="date" min={dateFrom || undefined} value={dateTo} onChange={(event) => updateInboxFilter("dateTo", event.currentTarget.value)} /></label>
          </div>
          <div className="toolbar-right">
            {query || stateFilter !== "all" || dateFrom || dateTo ? <button className="small ghost" type="button" onClick={clearInboxFilters}>Clear filters</button> : null}
          </div>
        </div>
        {loadError ? <InlineError message={loadError} onRetry={() => setRetryVersion((current) => current + 1)} /> : null}
        {loadStatus === "loading" ? <div className="meta-text" role="status">Refreshing conversations...</div> : null}
      </section>

      <div
        className={`conversation-shell react-inbox-shell ${thread ? "mobile-thread" : "mobile-list"}`}
        ref={shellRef}
        style={{ gridTemplateColumns: `${paneSizes.left}px 5px minmax(360px, 1fr) 5px ${paneSizes.right}px` }}
      >
        <ConversationListPane conversations={conversations} selectedLeadId={selectedLeadId} onSelect={chooseConversation} />
        <PaneResizer edge="left" value={paneSizes.left} onKeyboard={resizePane} onPointerDown={startPaneResize} />
        <ThreadPane
          thread={thread}
          message={message}
          mediaFile={mediaFile}
          pauseAfterSend={pauseAfterSend}
          busy={busy}
          setMessage={setMessage}
          setMediaFile={setMediaFile}
          setPauseAfterSend={setPauseAfterSend}
          onBack={() => {
            setSelectedLeadId(null);
            setThread(null);
            window.localStorage.removeItem("lead-ui-active-lead");
          }}
          onSubmitMessage={submitMessage}
        />
        <PaneResizer edge="right" value={paneSizes.right} onKeyboard={resizePane} onPointerDown={startPaneResize} />
        <ContactDetailsPane
          thread={thread}
          busy={busy}
          note={note}
          tagText={tagText}
          actionStatus={actionStatus}
          retryOutboundScope={retryOutboundScope}
          setNote={setNote}
          setTagText={setTagText}
          onArchive={archive}
          onHandoff={handoff}
          onDelete={permanentlyDeleteConversation}
          onSendBookingLink={sendThreadBookingLink}
          onSetAgentPaused={setThreadAgentPaused}
          onRemoveTag={removeTag}
          onSaveStage={saveStage}
          onSubmitNote={submitNote}
          onSubmitTag={submitTag}
          onStartNewOutboundAttempt={() => {
            if (!retryOutboundScope) return;
            const confirmed = window.confirm(
              "Verify the conversation and provider activity first. Start a new outbound attempt only if the previous message was not sent."
            );
            if (!confirmed) return;
            clearOutboundRequestKey(retryOutboundScope);
            setRetryOutboundScope(null);
            setActionStatus("A new outbound attempt is ready. Review the message, then send again.");
          }}
          isAdmin={auth.status === "ready" && auth.session.role === "admin"}
        />
      </div>
    </div>
  );
}

function ConversationListPane({ conversations, selectedLeadId, onSelect }: { conversations: ConversationListItem[]; selectedLeadId: number | null; onSelect: (leadId: number) => void }) {
  return (
    <aside className="pane inbox-pane">
      <div className="pane-header">
        <div className="pane-title"><h3>Inbox</h3></div>
        <span className="badge">{conversations.length}</span>
      </div>
      <div className="pane-body">
        <div className="compact-list">
          {conversations.length ? conversations.map((item) => (
            <button
              key={item.lead_id}
              type="button"
              className={`inbox-item ${item.lead_id === selectedLeadId ? "active" : ""}`}
              onClick={() => onSelect(item.lead_id)}
            >
              <div className="item-title-row">
                <div className="item-title">{item.lead_name || item.phone || `Contact ${item.lead_id}`}</div>
                {renderStageBadge(item.crm_stage || item.state)}
              </div>
              <div className="item-snippet">{item.last_message_snippet || "No messages yet."}</div>
              <div className="lead-list-meta">
                <span>{item.client_name || formatSource(item.source || "")}</span>
                <span>{formatDateTime(item.last_activity_at)}</span>
              </div>
            </button>
          )) : <div className="empty-state">No conversations match the current filters.</div>}
        </div>
      </div>
    </aside>
  );
}

function ThreadPane({
  thread,
  message,
  mediaFile,
  pauseAfterSend,
  busy,
  setMessage,
  setMediaFile,
  setPauseAfterSend,
  onBack,
  onSubmitMessage
}: {
  thread: ConversationThreadPayload | null;
  message: string;
  mediaFile: File | null;
  pauseAfterSend: boolean;
  busy: boolean;
  setMessage: (value: string) => void;
  setMediaFile: (value: File | null) => void;
  setPauseAfterSend: (value: boolean) => void;
  onBack: () => void;
  onSubmitMessage: (event: FormEvent) => void;
}) {
  const mediaInputRef = useRef<HTMLInputElement>(null);
  const sandboxThread = Boolean(thread && hasTag(thread.lead.tags || [], "sandbox"));

  useEffect(() => {
    if (!mediaFile && mediaInputRef.current) mediaInputRef.current.value = "";
  }, [mediaFile]);

  return (
    <section className="pane thread-pane focus-surface">
      {thread ? (
        <>
          <div className="pane-header">
            <div className="pane-title">
              <button className="small ghost conversation-mobile-back hidden" type="button" aria-label="Back to conversations" onClick={onBack}>Back</button>
              <h3>{thread.lead.display_name || "Thread"}</h3>
            </div>
            <div className="chip-row">{renderStageBadge(thread.lead.crm_stage)}{renderTag(thread.lead.current_state)}{sandboxThread ? renderTag("sandbox") : null}</div>
          </div>
          <MessageList messages={thread.messages} timeline />
          <form className="composer" onSubmit={(event) => void onSubmitMessage(event)}>
            <div className="composer-field">
              <div className="composer-combo lead-combo-control">
                <textarea
                  value={message}
                  disabled={busy}
                  aria-label={sandboxThread ? "Test lead message" : "Message"}
                  onChange={(event) => setMessage(event.currentTarget.value)}
                  placeholder={sandboxThread ? "Type the next message as the test lead." : "Type a direct outbound message to this contact."}
                />
                {!sandboxThread ? (
                  <>
                    <input
                      id="react-thread-media-input"
                      ref={mediaInputRef}
                      className="sr-only"
                      type="file"
                      accept="image/*,video/*"
                      aria-label="Attach image or video"
                      disabled={busy}
                      onChange={(event) => setMediaFile(event.currentTarget.files?.[0] || null)}
                    />
                    <label className="combo-action composer-attach-btn icon-only" htmlFor="react-thread-media-input" title="Attach image or video">
                      <svg viewBox="0 0 16 16" fill="none" aria-hidden="true">
                        <path d="M2.5 4.5A2 2 0 0 1 4.5 2.5h7a2 2 0 0 1 2 2v7a2 2 0 0 1-2 2h-7a2 2 0 0 1-2-2v-7Z" stroke="currentColor" strokeWidth="1.35" />
                        <path d="m3.4 11.4 3.1-3.1 2.1 2.1 1.3-1.3 2.7 2.7" stroke="currentColor" strokeWidth="1.35" strokeLinecap="round" strokeLinejoin="round" />
                        <circle cx="10.8" cy="5.3" r="1.1" fill="currentColor" />
                      </svg>
                    </label>
                  </>
                ) : null}
                <button
                  className="combo-action composer-send-btn"
                  type="submit"
                  disabled={busy || (sandboxThread ? !message.trim() : (!message.trim() && !mediaFile))}
                  aria-label={sandboxThread ? "Send as test lead" : "Send message"}
                  title={sandboxThread ? "Send as test lead" : "Send message"}
                >
                  <svg viewBox="0 0 16 16" fill="none" aria-hidden="true">
                    <path d="M2 8 13.5 2.7 11 13.3 7.2 9.5 2 8Z" fill="currentColor" />
                    <path d="M7.2 9.5 13.5 2.7" stroke="rgba(4,8,14,0.26)" strokeWidth="1.1" strokeLinecap="round" />
                  </svg>
                </button>
              </div>
              {!sandboxThread && mediaFile ? (
                <div className="composer-media-preview">
                  <div className="composer-media-card">
                    <div className="composer-media-copy">
                      <div className="composer-media-name">{mediaFile.name}</div>
                      <div className="meta-text">{mediaFile.type || "Media"} · {formatFileSize(mediaFile.size)}</div>
                    </div>
                    <button className="small ghost" type="button" disabled={busy} onClick={() => setMediaFile(null)}>Remove media</button>
                  </div>
                </div>
              ) : null}
              <div className="composer-foot">
                {sandboxThread ? (
                  <span className="meta-text">Sandbox mode: messages send as the test lead, run GPT, and stay inside this thread. Twilio is not used.</span>
                ) : (
                  <label className="checkbox-inline composer-pause-control">
                    <input type="checkbox" checked={pauseAfterSend} disabled={busy} onChange={(event) => setPauseAfterSend(event.currentTarget.checked)} />
                    <span>Pause AI after this manual reply</span>
                  </label>
                )}
                {busy ? <span className="meta-text" role="status">{sandboxThread ? "Waiting for GPT..." : "Sending..."}</span> : null}
              </div>
            </div>
          </form>
        </>
      ) : (
        <div className="empty-state">Select a conversation to inspect the thread and actions.</div>
      )}
    </section>
  );
}

function ContactDetailsPane({
  thread,
  busy,
  isAdmin,
  note,
  tagText,
  actionStatus,
  retryOutboundScope,
  setNote,
  setTagText,
  onArchive,
  onDelete,
  onHandoff,
  onRemoveTag,
  onSaveStage,
  onSendBookingLink,
  onSetAgentPaused,
  onSubmitNote,
  onSubmitTag,
  onStartNewOutboundAttempt
}: {
  thread: ConversationThreadPayload | null;
  busy: boolean;
  isAdmin: boolean;
  note: string;
  tagText: string;
  actionStatus: string;
  retryOutboundScope: string | null;
  setNote: (value: string) => void;
  setTagText: (value: string) => void;
  onArchive: () => Promise<void>;
  onDelete: () => Promise<void>;
  onHandoff: () => Promise<void>;
  onRemoveTag: (tag: string) => Promise<void>;
  onSaveStage: (stage: string) => void;
  onSendBookingLink: () => Promise<void>;
  onSetAgentPaused: (paused: boolean) => Promise<void>;
  onSubmitNote: (event: FormEvent) => void;
  onSubmitTag: (event: FormEvent) => void;
  onStartNewOutboundAttempt: () => void;
}) {
  const agentControl = thread?.lead.agent_control;
  const agentPaused = Boolean(agentControl?.paused);
  const optedOut = agentControl?.mode === "opted_out" || Boolean(thread?.lead.opted_out);
  return (
    <aside className="pane details-pane">
      <div className="pane-header">
        <div className="pane-title"><h3>Contact details</h3></div>
        {thread ? (
          <div className="actions">
            <button className="small" type="button" disabled={busy} onClick={() => void onHandoff()}>Handoff</button>
            <button className="small ghost" type="button" disabled={busy} onClick={() => void onArchive()}>Archive</button>
            {isAdmin ? <button className="small warn" type="button" disabled={busy} onClick={() => void onDelete()}>Delete</button> : null}
          </div>
        ) : null}
      </div>
      <div className="pane-body stack lead-record-main">
        {thread ? (
          <>
            {actionStatus ? <div className="meta-text" role="status">{actionStatus}</div> : null}
            {retryOutboundScope ? (
              <button className="small warn" type="button" disabled={busy} onClick={onStartNewOutboundAttempt}>
                Start a new outbound attempt
              </button>
            ) : null}
            <section className="detail-card lead-section thread-lead-header">
              <div className="lead-detail-title-row">
                <div className="lead-overview-name">{thread.lead.display_name}</div>
                {renderStageBadge(thread.lead.crm_stage)}
              </div>
              <div className="lead-detail-subline">
                <CopyInline value={formatPhone(thread.lead.phone)} copyValue={thread.lead.phone} label="phone number" />
                {thread.lead.phone && thread.lead.email ? <span className="lead-detail-sep">·</span> : null}
                <CopyInline value={thread.lead.email} copyValue={thread.lead.email} label="email" />
              </div>
              <div className="lead-detail-meta">{[formatSource(thread.lead.source), formatDateTime(thread.lead.last_activity_at)].filter(Boolean).join(" · ")}</div>
            </section>
            <section className="detail-card lead-section lead-form-section thread-form-section">
              <div className="title">Form answers</div>
              <FormAnswerList rows={formAnswerRows(thread.lead.form_answers, thread.lead.summary_lines)} />
            </section>
            <section className="detail-card lead-section lead-stage-card thread-stage-card">
              <div className="title">Stage</div>
              <StageControl stages={FALLBACK_STAGES} current={thread.lead.crm_stage || "New Lead"} disabled={busy} onSave={onSaveStage} />
            </section>
            <section className="detail-card lead-section stack thread-agent-control">
              <div className="item-title-row">
                <div>
                  <div className="title">AI control</div>
                  <div className="meta-text">{optedOut ? "This contact opted out." : agentPaused ? "AI is paused; a person owns the next reply." : "AI can answer the next inbound message."}</div>
                </div>
                <span className={`tag ${agentPaused ? "warn" : "ok"}`}>{optedOut ? "Opted out" : agentPaused ? "Paused" : "Active"}</span>
              </div>
              {agentControl?.note ? <div className="meta-text">{agentControl.note}</div> : null}
              <div className="actions">
                <button className="small ghost" type="button" disabled={busy || agentPaused} onClick={() => void onSetAgentPaused(true)}>Pause AI</button>
                <button className="small ghost" type="button" disabled={busy || !agentPaused || optedOut} onClick={() => void onSetAgentPaused(false)}>Resume AI</button>
                <button
                  className="small ghost"
                  type="button"
                  disabled={busy || !thread.client.booking_url || !thread.lead.phone || optedOut}
                  title={thread.client.booking_url ? "Send the configured booking link" : "Configure a booking link first"}
                  onClick={() => void onSendBookingLink()}
                >
                  Send booking link
                </button>
              </div>
            </section>
            <section className="detail-card lead-section lead-tags-card thread-tags-card">
              <div className="title">Tags</div>
              <div className="lead-tags-row">
                <div className="chip-row">
                  {dedupeTags(thread.lead.tags || [], thread.lead.crm_stage || "").map((tag) => renderTag(tag, () => void onRemoveTag(tag)))}
                  {!(thread.lead.tags || []).length ? <span className="meta-text">No tags yet.</span> : null}
                </div>
                <form className="lead-combo-control lead-tag-control" onSubmit={(event) => void onSubmitTag(event)}>
                  <input value={tagText} disabled={busy} onChange={(event) => setTagText(event.currentTarget.value)} placeholder="add tag" />
                  <button className="combo-action" type="submit" disabled={busy || !tagText.trim()}>Add</button>
                </form>
              </div>
            </section>
            <section className="detail-card lead-section lead-notes-card thread-notes-card">
              <div className="title">Internal notes</div>
              <NoteList notes={thread.notes} />
              <form className="lead-combo-control lead-note-control" onSubmit={(event) => void onSubmitNote(event)}>
                <textarea value={note} disabled={busy} onChange={(event) => setNote(event.currentTarget.value)} placeholder="Add an internal note." />
                <button className="combo-action" type="submit" disabled={busy || !note.trim()}>Add</button>
              </form>
            </section>
            <details className="detail-card detail-disclosure lead-section">
              <summary>Activity</summary>
              <div className="detail-disclosure-body">
                <AuditList events={thread.audit_events} />
              </div>
            </details>
          </>
        ) : (
          <div className="empty-state">Select a conversation to inspect the thread and actions.</div>
        )}
      </div>
    </aside>
  );
}

function StageControl({ stages, current, disabled = false, onSave }: { stages: string[]; current: string; disabled?: boolean; onSave: (stage: string) => void }) {
  const [stage, setStage] = useState(current);

  useEffect(() => setStage(current), [current]);

  return (
    <div className="lead-combo-control lead-stage-control">
      <select aria-label="Stage" value={stage} disabled={disabled} onChange={(event) => setStage(event.currentTarget.value)}>
        {(stages.length ? stages : FALLBACK_STAGES).map((item) => (
          <option key={item} value={item}>{item}</option>
        ))}
      </select>
      <button className="combo-action" type="button" aria-label="Update stage" disabled={disabled} onClick={() => onSave(stage)}>Update</button>
    </div>
  );
}

function TaskList({ tasks, onToggle }: { tasks: LeadTask[]; onToggle: (task: LeadTask) => Promise<void> | void }) {
  if (!tasks.length) return <div className="empty-state">No tasks yet.</div>;
  return (
    <div className="compact-list lead-task-list">
      {tasks.map((task) => (
        <div className="preview-item" key={task.id}>
          <div className="item-title-row">
            <div>
              <div className="item-title">{task.title}</div>
              <div className="meta-text">{[task.due_date ? formatDate(task.due_date) : "", task.description || ""].filter(Boolean).join(" · ")}</div>
            </div>
            <button className="small ghost" type="button" onClick={() => void onToggle(task)}>
              {task.status === "done" ? "Reopen" : "Done"}
            </button>
          </div>
        </div>
      ))}
    </div>
  );
}

function MessageList({ messages, timeline = false }: { messages: ThreadMessage[]; timeline?: boolean }) {
  if (!messages.length) return <div className="empty-state">No messages yet.</div>;
  return (
    <div className={timeline ? "timeline" : "crm-message-list"}>
      {messages.map((message, index) => (
        <div key={message.id ?? `${message.created_at}-${index}`} className={`bubble-row ${message.direction === "OUTBOUND" ? "outbound" : "inbound"}`}>
          <div className={`bubble ${message.direction === "OUTBOUND" ? "outbound" : "inbound"}`}>
            <div>{message.body || "(empty message)"}</div>
            <MessageAttachments attachments={message.attachments || []} />
            <div className="bubble-meta">{[message.direction, formatDateTime(message.created_at), deliveryLabel(message.delivery)].filter(Boolean).join(" · ")}</div>
          </div>
        </div>
      ))}
    </div>
  );
}

function MessageAttachments({ attachments }: { attachments: ThreadMessage["attachments"] }) {
  if (!attachments?.length) return null;
  return (
    <div className="message-attachments">
      {attachments.map((attachment, index) => {
        const url = attachment.public_url || attachment.url || "";
        const contentType = attachment.content_type || "";
        const key = `${attachment.id ?? index}-${url || attachment.filename || "attachment"}`;
        if (!url) {
          return <span className="message-attachment file" key={key}>{attachment.filename || "Attachment"}</span>;
        }
        if (contentType.startsWith("image/")) {
          return (
            <figure className="message-attachment" key={key}>
              <img src={url} alt={attachment.filename || "Message attachment"} loading="lazy" />
              {attachment.filename ? <figcaption>{attachment.filename}</figcaption> : null}
            </figure>
          );
        }
        if (contentType.startsWith("video/")) {
          return (
            <figure className="message-attachment" key={key}>
              <video src={url} controls preload="metadata" />
              {attachment.filename ? <figcaption>{attachment.filename}</figcaption> : null}
            </figure>
          );
        }
        return <a className="message-attachment file" href={url} target="_blank" rel="noreferrer" key={key}>{attachment.filename || "Open attachment"}</a>;
      })}
    </div>
  );
}

function deliveryLabel(delivery: ThreadMessage["delivery"]) {
  if (typeof delivery === "string") return delivery;
  if (!delivery || typeof delivery !== "object") return "";
  return String(delivery.label || delivery.status || "").trim();
}

function NoteList({ notes }: { notes: LeadDetailPayload["notes"] }) {
  if (!notes.length) return <div className="empty-state">No internal notes yet.</div>;
  return (
    <div className="compact-list lead-note-list">
      {notes.map((note, index) => (
        <div className="preview-item" key={note.id ?? `${note.created_at}-${index}`}>
          <div className="item-title">{note.note || note.body || ""}</div>
          <div className="meta-text">{formatDateTime(note.created_at)}</div>
        </div>
      ))}
    </div>
  );
}

function AuditList({ events }: { events: Array<Record<string, unknown>> }) {
  if (!events.length) return <div className="empty-state">No visible activity yet.</div>;
  return (
    <div className="compact-list">
      {events.map((event, index) => (
        <div className="preview-item" key={String(event.id ?? index)}>
          <div className="item-title">{String(event.event_type || "Activity")}</div>
          <div className="meta-text">{formatDateTime(String(event.created_at || ""))}</div>
        </div>
      ))}
    </div>
  );
}

function FormAnswerList({ rows }: { rows: Array<{ label: string; value: string }> }) {
  if (!rows.length) return <div className="empty-state">No form answers captured yet.</div>;
  return (
    <div className="form-answer-list">
      {rows.map((row) => (
        <div className="form-answer-item" key={`${row.label}-${row.value}`}>
          <div className="summary-label">{row.label}</div>
          <div className="summary-value">{row.value}</div>
        </div>
      ))}
    </div>
  );
}

function CopyInline({ value, copyValue, label }: { value: string; copyValue: string; label: string }) {
  if (!value || !copyValue) return null;
  async function copy() {
    try {
      await navigator.clipboard?.writeText(copyValue);
    } catch {
      // Copy affordance should never block the CRM workflow.
    }
  }
  return (
    <button className="copy-inline" type="button" onClick={() => void copy()} title={`Copy ${label}`}>
      {value}
      <span className="copy-inline-hint">Copy</span>
    </button>
  );
}

function groupByStage(items: LeadListItem[], stages: string[]) {
  const output: Record<string, LeadListItem[]> = {};
  for (const stage of stages) output[stage] = [];
  for (const item of items) {
    const stage = item.crm_stage || "New Lead";
    if (!output[stage]) output[stage] = [];
    output[stage].push(item);
  }
  return output;
}

function WorkflowLoadState({
  title,
  status,
  error,
  onRetry
}: {
  title: string;
  status: "loading" | "ready" | "error";
  error: string;
  onRetry: () => void;
}) {
  const failed = status === "error";
  return (
    <div className="react-workflow-page">
      <section className="surface stack" aria-live="polite">
        <h2>{title}</h2>
        <div className="empty-state">
          <div>{failed ? error || `${title} unavailable` : `Loading ${title.toLowerCase()}...`}</div>
          {failed ? <button className="small" type="button" onClick={onRetry}>Retry</button> : null}
        </div>
      </section>
    </div>
  );
}

function InlineError({ message, onRetry }: { message: string; onRetry: () => void }) {
  return (
    <div className="empty-state compact" role="alert">
      <span>{message}</span>{" "}
      <button className="small ghost" type="button" onClick={onRetry}>Retry</button>
    </div>
  );
}

function useDebouncedValue<T>(value: T, delayMs: number): T {
  const [debounced, setDebounced] = useState(value);
  useEffect(() => {
    const timer = window.setTimeout(() => setDebounced(value), delayMs);
    return () => window.clearTimeout(timer);
  }, [delayMs, value]);
  return debounced;
}

type InboxPaneSizes = { left: number; right: number };

function loadInboxPaneSizes(): InboxPaneSizes {
  try {
    const value = JSON.parse(window.localStorage.getItem(INBOX_PANE_SIZES_KEY) || "null") as Partial<InboxPaneSizes> | null;
    return {
      left: clamp(Number(value?.left) || 320, 240, 520),
      right: clamp(Number(value?.right) || 284, 240, 420)
    };
  } catch {
    return { left: 320, right: 284 };
  }
}

function saveInboxPaneSizes(value: InboxPaneSizes) {
  try {
    window.localStorage.setItem(INBOX_PANE_SIZES_KEY, JSON.stringify(value));
  } catch {
    // Pane sizing is a non-critical preference.
  }
}

function clamp(value: number, minimum: number, maximum: number) {
  return Math.min(maximum, Math.max(minimum, value));
}

function formatFileSize(bytes: number) {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${Math.round(bytes / 1024)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function initialPipelineFilters(): PipelineFilters {
  const savedClientKey = window.localStorage.getItem(CRM_CLIENT_FILTER_KEY) || window.localStorage.getItem(SELECTED_CLIENT_KEY) || "";
  return {
    clientKey: savedClientKey,
    search: "",
    stage: window.localStorage.getItem(CRM_STAGE_FILTER_KEY) || "all"
  };
}

function emptyPipelineLeadForm(clientKey = ""): PipelineLeadForm {
  return {
    city: "",
    client_key: clientKey,
    crm_stage: "New Lead",
    email: "",
    full_name: "",
    notes: "",
    owner_name: "",
    phone: ""
  };
}

function cleanLeadForm(form: PipelineLeadForm): ManualLeadCreatePayload {
  return {
    city: cleanOptional(form.city),
    client_key: cleanOptional(form.client_key),
    crm_stage: cleanOptional(form.crm_stage),
    email: cleanOptional(form.email),
    full_name: form.full_name.trim(),
    notes: cleanOptional(form.notes),
    owner_name: cleanOptional(form.owner_name),
    phone: cleanOptional(form.phone)
  };
}

function cleanOptional(value: string | undefined) {
  const cleaned = String(value || "").trim();
  return cleaned || undefined;
}

function stageCount(grouped: Record<string, LeadListItem[]>, stage: string) {
  return grouped[stage]?.length ?? 0;
}

function formatCrmStageDisplay(value: string) {
  const stage = String(value || "").trim();
  return stage === "New Lead" ? "New" : stage;
}

function conversationStateBadge(conversationState: string, crmStage: string) {
  if (!conversationState || isConversationStateRedundant(crmStage, conversationState)) return null;
  return renderTag(formatConversationState(conversationState));
}

function isConversationStateRedundant(crmStage: string, conversationState: string) {
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

function formatConversationState(value: string) {
  return titleize(value.toLowerCase().replace(/_/g, " "));
}

function pipelineMetaLine(lead: LeadListItem, includeClientName: boolean) {
  return [
    formatPhone(lead.phone),
    formatSource(lead.source),
    includeClientName ? lead.client_name : ""
  ].filter(Boolean).join(" · ") || "No contact details";
}

function shouldAutoSelectConversation() {
  if (typeof window.matchMedia !== "function") return true;
  return !window.matchMedia("(max-width: 680px)").matches;
}

type CalendarMonthCell = {
  dateKey: string;
  dayNumber: number;
  inMonth: boolean;
};

type TaskBucket = "Overdue" | "Today" | "Upcoming" | "No due date" | "Done";

type TaskBucketGroup = {
  bucket: TaskBucket;
  tasks: LeadTask[];
};

function ensureCalendarFocusState(
  timeZone: string,
  selectedDateKey: string,
  calendarMonth: string,
  setSelectedDateKey: (value: string) => void,
  setCalendarMonth: (value: string) => void
) {
  const todayKey = dateKeyInTimeZone(new Date(), timeZone);
  let nextSelectedDate = selectedDateKey || todayKey;
  const nextMonth = calendarMonth || monthKeyForDateKey(nextSelectedDate);
  if (monthKeyForDateKey(nextSelectedDate) !== nextMonth) {
    nextSelectedDate = `${nextMonth}-01`;
  }
  if (nextSelectedDate !== selectedDateKey) {
    setSelectedDateKey(nextSelectedDate);
    window.localStorage.setItem("lead-ui-calendar-day", nextSelectedDate);
  }
  if (nextMonth !== calendarMonth) {
    setCalendarMonth(nextMonth);
    window.localStorage.setItem("lead-ui-calendar-month", nextMonth);
  }
}

function buildCalendarMonthCells(monthKey: string): CalendarMonthCell[] {
  const parts = parseMonthKey(monthKey);
  if (!parts) return [];
  const first = new Date(Date.UTC(parts.year, parts.month - 1, 1, 12));
  const startOffset = first.getUTCDay();
  const start = new Date(Date.UTC(parts.year, parts.month - 1, 1 - startOffset, 12));
  return Array.from({ length: 42 }, (_, index) => {
    const cellDate = new Date(start.getTime());
    cellDate.setUTCDate(start.getUTCDate() + index);
    const dateKey = dateKeyFromUtcDate(cellDate);
    return {
      dateKey,
      dayNumber: cellDate.getUTCDate(),
      inMonth: cellDate.getUTCMonth() === parts.month - 1
    };
  });
}

function meetingsByDate(items: CalendarBooking[], timeZone: string) {
  const output = new Map<string, CalendarBooking[]>();
  for (const item of items) {
    const key = dateKeyInTimeZone(item.start_at, timeZone);
    if (!output.has(key)) output.set(key, []);
    output.get(key)?.push(item);
  }
  for (const entries of output.values()) {
    entries.sort((a, b) => String(a.start_at).localeCompare(String(b.start_at)));
  }
  return output;
}

function sortOpenTasks(tasks: LeadTask[]) {
  return tasks
    .filter((task) => task.status === "open")
    .slice()
    .sort((a, b) => {
      const aDue = a.due_date || "9999-12-31";
      const bDue = b.due_date || "9999-12-31";
      if (aDue !== bDue) return aDue.localeCompare(bDue);
      return String(a.title || "").localeCompare(String(b.title || ""));
    });
}

function groupTasksByBucket(tasks: LeadTask[], todayKey: string): TaskBucketGroup[] {
  const buckets: TaskBucket[] = ["Overdue", "Today", "Upcoming", "No due date", "Done"];
  const grouped = new Map<TaskBucket, LeadTask[]>();
  for (const bucket of buckets) grouped.set(bucket, []);
  for (const task of tasks) {
    grouped.get(taskBucketForTask(task, todayKey))?.push(task);
  }
  return buckets
    .map((bucket) => ({
      bucket,
      tasks: (grouped.get(bucket) || []).sort(compareTasksForQueue)
    }))
    .filter((group) => group.tasks.length > 0);
}

function compareTasksForQueue(a: LeadTask, b: LeadTask) {
  const aDue = a.due_date || "9999-12-31";
  const bDue = b.due_date || "9999-12-31";
  if (aDue !== bDue) return aDue.localeCompare(bDue);
  return String(a.title || "").localeCompare(String(b.title || ""));
}

function taskBucketForTask(task: LeadTask, todayKey: string): TaskBucket {
  if (task.status === "done") return "Done";
  if (task.due_date && task.due_date < todayKey) return "Overdue";
  if (task.due_date === todayKey) return "Today";
  if (task.due_date) return "Upcoming";
  return "No due date";
}

function taskBucketTone(bucket: TaskBucket) {
  if (bucket === "Overdue") return "err";
  if (bucket === "Today") return "warn";
  if (bucket === "Upcoming") return "info";
  if (bucket === "Done") return "ok";
  return "";
}

function taskBucketClass(bucket: TaskBucket) {
  return bucket.toLowerCase().replace(/\s+/g, "-");
}

function taskSummary(tasks: LeadTask[], todayKey: string) {
  return tasks.reduce(
    (summary, task) => {
      const bucket = taskBucketForTask(task, todayKey);
      if (bucket === "Overdue") summary.overdue += 1;
      if (bucket === "Today") summary.today += 1;
      if (bucket === "Upcoming") summary.upcoming += 1;
      return summary;
    },
    { overdue: 0, today: 0, upcoming: 0 }
  );
}

function formatTaskDue(task: LeadTask, bucket: TaskBucket) {
  if (!task.due_date) return "-";
  if (bucket === "Today") return "Today";
  return formatDate(task.due_date);
}

function normalizeTimeZone(timeZone?: string | null) {
  const fallback = Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC";
  const candidate = timeZone && timeZone !== "local" ? timeZone : fallback;
  try {
    new Intl.DateTimeFormat("en-US", { timeZone: candidate }).format(new Date());
    return candidate;
  } catch {
    return fallback;
  }
}

function dateKeyInTimeZone(value: string | Date, timeZone: string) {
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) return typeof value === "string" ? value.slice(0, 10) : "";
  const normalizedTimeZone = normalizeTimeZone(timeZone);
  const parts = new Intl.DateTimeFormat("en-CA", {
    day: "2-digit",
    month: "2-digit",
    timeZone: normalizedTimeZone,
    year: "numeric"
  }).formatToParts(date);
  const year = parts.find((part) => part.type === "year")?.value || "0000";
  const month = parts.find((part) => part.type === "month")?.value || "01";
  const day = parts.find((part) => part.type === "day")?.value || "01";
  return `${year}-${month}-${day}`;
}

function dateKeyFromUtcDate(date: Date) {
  return `${date.getUTCFullYear()}-${String(date.getUTCMonth() + 1).padStart(2, "0")}-${String(date.getUTCDate()).padStart(2, "0")}`;
}

function monthKeyForDateKey(dateKey: string) {
  return dateKey ? dateKey.slice(0, 7) : dateKeyInTimeZone(new Date(), "UTC").slice(0, 7);
}

function parseMonthKey(monthKey: string) {
  const match = /^(\d{4})-(\d{2})$/.exec(monthKey);
  if (!match) return null;
  const year = Number(match[1]);
  const month = Number(match[2]);
  if (!year || month < 1 || month > 12) return null;
  return { year, month };
}

function shiftMonthKey(monthKey: string, offset: number) {
  const parts = parseMonthKey(monthKey);
  if (!parts) return monthKeyForDateKey(dateKeyInTimeZone(new Date(), "UTC"));
  const date = new Date(Date.UTC(parts.year, parts.month - 1 + offset, 1, 12));
  return `${date.getUTCFullYear()}-${String(date.getUTCMonth() + 1).padStart(2, "0")}`;
}

function formatMonthLabel(monthKey: string, timeZone: string) {
  const parts = parseMonthKey(monthKey);
  if (!parts) return monthKey;
  const date = new Date(Date.UTC(parts.year, parts.month - 1, 1, 12));
  return new Intl.DateTimeFormat(undefined, { month: "long", timeZone: normalizeTimeZone(timeZone), year: "numeric" }).format(date);
}

function formatDateLabel(dateKey: string, timeZone: string, options: Intl.DateTimeFormatOptions = {}) {
  if (!dateKey) return "";
  const date = new Date(`${dateKey}T12:00:00Z`);
  if (Number.isNaN(date.getTime())) return dateKey;
  return new Intl.DateTimeFormat(undefined, { timeZone: normalizeTimeZone(timeZone), ...options }).format(date);
}

function formatTimeInTimeZone(value: string, timeZone: string) {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat(undefined, { hour: "numeric", minute: "2-digit", timeZone: normalizeTimeZone(timeZone) }).format(date);
}

async function resolveCalendarClient(session: SessionPayload, selectedClientKey = ""): Promise<{ key: string; clients: ClientSummary[] }> {
  if (session.role === "client" && session.client_key) {
    return { key: session.client_key, clients: [] };
  }
  const clients = await fetchClients();
  const saved = selectedClientKey || window.localStorage.getItem("lead-ui-selected-client") || "";
  const key = clients.some((client) => client.client_key === saved) ? saved : clients[0]?.client_key ?? "";
  return { key, clients };
}

function formAnswerRows(formAnswers: Record<string, unknown> | undefined, summaryLines: LeadDetailPayload["lead"]["summary_lines"] | undefined) {
  const rows: Array<{ label: string; value: string }> = [];
  if (formAnswers) {
    for (const [key, rawValue] of Object.entries(formAnswers)) {
      const value = stringifyValue(rawValue);
      if (value) rows.push({ label: titleize(key), value });
    }
  }
  if (!rows.length) {
    for (const line of summaryLines ?? []) {
      if (typeof line === "string") {
        const [label, ...valueParts] = line.split(":");
        const value = valueParts.join(":").trim();
        if (label && value) rows.push({ label: label.trim(), value });
      } else {
        const label = stringifyValue(line.label || line.question);
        const value = stringifyValue(line.value || line.answer);
        if (label && value) rows.push({ label, value });
      }
    }
  }
  return rows;
}

function stringifyValue(value: unknown): string {
  if (value === null || value === undefined) return "";
  if (Array.isArray(value)) return value.map(stringifyValue).filter(Boolean).join(", ");
  if (typeof value === "object") return JSON.stringify(value);
  return String(value).trim();
}

function formatMoneyLike(value: number | string) {
  if (typeof value === "number" && Number.isFinite(value)) {
    return new Intl.NumberFormat(undefined, { maximumFractionDigits: 0, style: "currency", currency: "USD" }).format(value);
  }
  const raw = String(value).trim();
  const numeric = Number(raw);
  if (raw && Number.isFinite(numeric) && !/[^\d.,-]/.test(raw)) {
    return new Intl.NumberFormat(undefined, { maximumFractionDigits: 0, style: "currency", currency: "USD" }).format(numeric);
  }
  return raw;
}

function timelineDetail(event: Record<string, unknown>) {
  const direct = stringifyValue(event.detail || event.message || event.note || event.body || event.description);
  if (direct) return direct;
  const decision = event.decision;
  if (decision && typeof decision === "object") {
    const action = stringifyValue((decision as Record<string, unknown>).action);
    const reason = stringifyValue((decision as Record<string, unknown>).reason);
    return [action, reason].filter(Boolean).join(" · ");
  }
  return "";
}

function renderStageBadge(stage: string) {
  if (!stage) return null;
  return <span className="tag info">{stage}</span>;
}

function renderTag(tag: string, onRemove?: () => void) {
  return (
    <span className="tag" key={tag}>
      {formatTag(tag)}
      {onRemove ? <button className="tag-remove-btn" type="button" aria-label={`Remove ${tag}`} onClick={onRemove}>×</button> : null}
    </span>
  );
}

function dedupeTags(tags: string[], stage: string) {
  const seen = new Set<string>();
  return tags.filter((tag) => {
    const key = tag.trim().toLowerCase();
    if (!key || key === stage.trim().toLowerCase() || seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function hasTag(tags: string[], expected: string) {
  return tags.some((tag) => tag.trim().toLowerCase() === expected.trim().toLowerCase());
}

function formatSource(source: string) {
  const labels: Record<string, string> = {
    meta: "Meta lead ad",
    linkedin: "LinkedIn lead form",
    sms: "SMS intake",
    manual: "Manual entry"
  };
  return labels[source] || titleize(source);
}

function formatTag(tag: string) {
  return titleize(tag.replace(/_/g, " "));
}

function titleize(value: string) {
  return value
    .replace(/[_-]+/g, " ")
    .trim()
    .replace(/\w\S*/g, (word) => word.charAt(0).toUpperCase() + word.slice(1));
}

function formatDate(value: string) {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat(undefined, { month: "short", day: "numeric", year: "numeric" }).format(date);
}

function formatDateTime(value: string) {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat(undefined, { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" }).format(date);
}

function formatPhone(value: string) {
  const digits = value.replace(/\D/g, "");
  if (digits.length === 11 && digits.startsWith("1")) {
    return `+1 (${digits.slice(1, 4)}) ${digits.slice(4, 7)}-${digits.slice(7)}`;
  }
  if (digits.length === 10) {
    return `(${digits.slice(0, 3)}) ${digits.slice(3, 6)}-${digits.slice(6)}`;
  }
  return value;
}

function messageFor(error: unknown, fallback: string) {
  return error instanceof Error ? error.message : fallback;
}
