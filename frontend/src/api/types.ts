export type SessionPayload = {
  status: "ok";
  role: "admin" | "client";
  app_name: string;
  env: string;
  generated_at: string;
  can_seed_demo: boolean;
  demo_data_present: boolean;
  client_key: string | null;
  client_name: string | null;
  portal_display_name: string | null;
};

export type DashboardStats = {
  clients_total: number;
  active_clients: number;
  conversations_total: number;
  total_leads: number;
  attention_needed: number;
  booked_total: number;
  handoff_total: number;
  won_total: number;
  new_last_24_hours: number;
  new_last_7_days: number;
  new_last_30_days: number;
  open_pipeline_total: number;
  open_tasks_total: number;
  overdue_tasks_total: number;
  due_today_tasks: number;
  upcoming_meetings_total: number;
  upcoming_meetings_7d: number;
  booked_rate: number;
  won_rate: number;
};

export type DashboardBreakdownRow = {
  key: string;
  label?: string;
  count: number;
  share: number;
};

export type DashboardTrendRow = {
  week_start: string;
  week_end: string;
  count: number;
};

export type MessageDeliveryStatus = {
  status?: string;
  label?: string;
  label_fr?: string;
  severity?: string;
  provider?: string;
  updated_at?: string;
};

export type DashboardRecentLead = {
  lead_id: number;
  lead_name: string;
  phone: string;
  email: string;
  source: string;
  client_key: string;
  client_name: string;
  crm_stage: string;
  conversation_state: string;
  created_at: string;
  last_message_snippet: string;
  last_message_direction: string;
  last_message_delivery: MessageDeliveryStatus | string | null;
};

export type DashboardUpcomingItem = {
  id?: number;
  lead_id?: number;
  lead_name?: string;
  title?: string;
  phone?: string;
  client_name?: string;
  start_at?: string;
  end_at?: string;
  timezone?: string;
  due_date?: string;
  description?: string;
  status?: string;
};

export type CampaignPerformance = {
  report_range?: string;
  campaigns?: Array<{
    campaign_name?: string;
    status?: string;
    conversions?: number;
    cost_per_conversion?: number;
    cpc?: number;
    clicks?: number;
  }>;
  totals?: {
    spend?: number;
    campaigns?: number;
    cpc?: number;
    ctr?: number;
    cost_per_conversion?: number;
    conversions?: number;
    reach?: number;
    impressions?: number;
    clicks?: number;
    conversion_rate?: number;
  };
};

export type DashboardPayload = {
  scope: {
    role: "admin" | "client";
    client_key: string | null;
    client_name: string | null;
    title: string;
  };
  runtime: Record<string, unknown>;
  stats: DashboardStats;
  lead_trend: DashboardTrendRow[];
  source_breakdown: DashboardBreakdownRow[];
  campaign_performance: CampaignPerformance;
  stage_breakdown: DashboardBreakdownRow[];
  onboarding: Array<{ label: string; done: boolean; detail: string }>;
  top_clients: Array<{
    client_key: string;
    business_name: string;
    lead_count: number;
    open_conversations: number;
    booked_total: number;
    last_activity_at: string | null;
    is_active: boolean;
  }>;
  upcoming: {
    tasks: DashboardUpcomingItem[];
    meetings: DashboardUpcomingItem[];
  };
  recent_leads: DashboardRecentLead[];
  recent_conversations: unknown[];
  latest_activity: Record<string, string | null>;
};

export type ClientPortalLoginResponse = {
  status: "ok";
  session: SessionPayload;
};

export type ClientSummary = {
  id: number;
  client_key: string;
  business_name: string;
  tone?: string;
  timezone?: string;
  booking_url?: string;
  is_active?: boolean;
  portal_enabled?: boolean;
  lead_count?: number;
  open_conversations?: number;
  last_activity_at?: string | null;
  last_webhook_received_at?: string | null;
};

export type RuntimeSummary = {
  twilio_configured?: boolean;
  twilio_account_sid_configured?: boolean;
  twilio_auth_token_configured?: boolean;
  crm_webhook_secret_configured?: boolean;
  zapier_booking_webhook_secret_configured?: boolean;
  zapier_webhook_secret_configured?: boolean;
  zapier_booking_webhook_url_configured?: boolean;
  ai_configured?: boolean;
  twilio_from_number?: string;
  openai_model?: string;
  ai_provider_mode?: string;
  public_base_url?: string;
  source?: string;
  has_client_overrides?: boolean;
};

export type ChecklistItem = {
  label: string;
  done: boolean;
  detail?: string;
};

export type ClientProviderReadConfig = {
  public_base_url?: string;
  twilio_from_number?: string;
  [key: string]: unknown;
};

export type AuditLogItem = {
  id: number;
  event_type: string;
  lead_id?: number | null;
  created_at: string;
  decision?: Record<string, unknown>;
};

export type ClientConfig = {
  id?: number;
  client_key: string;
  business_name: string;
  tone?: string;
  timezone?: string;
  qualification_questions?: string[];
  booking_url?: string;
  booking_mode?: string;
  booking_config?: Record<string, unknown>;
  provider_config?: ClientProviderReadConfig;
  fallback_handoff_number?: string;
  consent_text?: string;
  portal_display_name?: string;
  portal_email?: string;
  portal_enabled?: boolean;
  portal_password_configured?: boolean;
  operating_hours?: Record<string, unknown>;
  faq_context?: string;
  ai_context?: string;
  template_overrides?: Record<string, string>;
  is_active?: boolean;
  created_at?: string;
  updated_at?: string;
  twilio_inbound_path?: string;
};

export type ClientDetailPayload = {
  client: ClientConfig;
  webhook_urls: Record<string, string>;
  provider_runtime?: RuntimeSummary;
  onboarding?: ChecklistItem[];
  recent_conversations?: ConversationListItem[];
  recent_logs?: AuditLogItem[];
  counts?: Record<string, number>;
};

export type KnowledgeSource = {
  id?: number;
  url?: string;
  normalized_url?: string;
  final_url?: string;
  title?: string;
  status?: string;
  error_message?: string;
  text_excerpt?: string;
  structured_data?: Record<string, unknown>;
  chunk_count?: number;
  last_crawled_at?: string;
  last_success_at?: string;
  chunks?: Array<{ chunk_index?: number; content?: string }>;
};

export type KnowledgePayload = {
  status?: string;
  job_id?: string;
  deleted_sources?: number;
  cancelled_active_job?: boolean;
  client_key?: string;
  sources?: KnowledgeSource[];
  total_sources?: number;
  total_chunks?: number;
  business_profile_context?: string;
  extraction?: {
    total_pages?: number;
    total_chunks?: number;
    pages?: Array<{ url?: string; status?: string; title?: string }>;
  };
};

export type KnowledgeJobStatus = {
  client_key?: string;
  job_id: string;
  status: "queued" | "running" | "ok" | "partial" | "failed" | "skipped" | "completed" | "cancelled";
  terminal: boolean;
  total_pages: number;
  failed_pages: number;
  total_chunks: number;
};

export type OwnerWorkspacePayload = {
  client: ClientConfig;
  runtime: RuntimeSummary;
  delivery_mode: string;
  knowledge: KnowledgePayload;
  live_test_checklist: ChecklistItem[];
  conversations: ConversationListItem[];
};

export type AutomationHealthPayload = {
  client_key: string;
  generated_at: string;
  status: string;
  needs_attention: number;
  automations: Array<{
    key: string;
    label: string;
    status: string;
    configured: boolean;
    detail?: string;
    last_event_type?: string;
    last_run_at?: string | null;
    runs_7d?: number;
  }>;
};

export type RuntimeConfigStatus = {
  openai_api_key_configured: boolean;
  openai_model: string;
  ai_provider_mode: string;
};

export type OwnerCalendarAvailabilityRow = {
  day: number;
  start: string;
  end: string;
  enabled: boolean;
};

export type OwnerCalendarConfig = {
  slot_minutes: number;
  notice_minutes: number;
  horizon_days: number;
  availability: OwnerCalendarAvailabilityRow[];
};

export type SandboxStartPayload = {
  mode: string;
  full_name?: string;
  phone?: string;
  email?: string;
  city?: string;
  form_answers: Array<{ question: string; answer: string }>;
};

export type SandboxStartResponse = {
  status: string;
  lead_id: number;
  mode?: string;
  state?: string;
  body?: string;
  phone?: string;
  booking_debug?: unknown;
  zapier_booking_webhook?: unknown;
};

export type SandboxMessageResponse = {
  status: string;
  lead_id: number;
  state: string;
  crm_stage: string;
  delivery_mode: "sandbox";
  twilio_bypassed: boolean;
  inbound_message_id: number;
  reply: {
    id: number | null;
    body: string;
    provider_message_sid: string;
  };
  booking_debug?: unknown;
  zapier_booking_webhook?: unknown;
};

export type LeadListItem = {
  lead_id: number;
  lead_name: string;
  phone: string;
  email: string;
  source: string;
  client_key: string;
  client_name: string;
  crm_stage: string;
  conversation_state: string;
  agent_control?: AgentControl;
  last_message_snippet: string;
  last_message_direction: string;
  last_message_delivery: MessageDeliveryStatus | string | null;
  lead_summary: string;
  last_activity_at: string;
  created_at: string;
  tags: string[];
  booked: boolean;
  archived: boolean;
  lead_score?: number | string | null;
  estimated_value?: number | string | null;
  campaign_name?: string;
  intent_level?: string;
  recommended_follow_up?: string;
  next_task_title?: string;
  next_task_due_date?: string;
};

export type CrmLeadsPayload = {
  items: LeadListItem[];
  counts: Record<string, number>;
  total: number;
  stages: string[];
};

export type ManualLeadCreatePayload = {
  client_key?: string;
  full_name: string;
  phone?: string;
  email?: string;
  city?: string;
  owner_name?: string;
  crm_stage?: string;
  notes?: string;
};

export type ManualLeadCreateResponse = {
  status: string;
  lead: {
    id: number;
    lead_id: number;
    display_name: string;
    client_key: string;
    crm_stage: string;
    conversation_state: string;
  agent_control?: AgentControl;
  };
};

export type AgentControl = {
  paused: boolean;
  mode: "active" | "paused" | "handoff" | "opted_out" | string;
  reason?: string;
  note?: string;
  actor_role?: string;
  updated_at?: string;
};

export type ManualMeetingCreatePayload = {
  lead_id?: number;
  new_lead?: {
    full_name: string;
    phone?: string;
    email?: string;
    city?: string;
  };
  start_at: string;
  duration_minutes: number;
  timezone: string;
  title: string;
  notes?: string;
  create_conference_link?: boolean;
  send_email_invite?: boolean;
  include_meeting_link?: boolean;
  send_sms_reminders?: boolean;
};

export type MessageAttachment = {
  id?: number;
  filename?: string;
  content_type?: string;
  public_url?: string;
  url?: string;
};

export type ThreadMessage = {
  id?: number;
  direction: string;
  body: string;
  provider_message_sid?: string;
  attachments?: MessageAttachment[];
  delivery?: MessageDeliveryStatus | string | null;
  created_at: string;
};

export type LeadTask = {
  id: number;
  lead_id: number;
  title: string;
  description?: string;
  due_date?: string | null;
  status: "open" | "done" | string;
  created_at?: string;
  updated_at?: string;
  completed_at?: string | null;
  lead_name?: string;
  lead_phone?: string;
  lead_email?: string;
  crm_stage?: string;
  conversation_state?: string;
  client_key?: string;
  client_name?: string;
};

export type LeadNote = {
  id?: number;
  body?: string;
  note?: string;
  created_at: string;
  actor_role?: string;
};

export type LeadDetailPayload = {
  lead: {
    id: number;
    display_name: string;
    full_name: string;
    phone: string;
    email: string;
    source: string;
    city?: string;
    owner?: string | null;
    crm_stage: string;
    conversation_state?: string;
    current_state?: string;
    agent_control?: AgentControl;
    summary?: string;
    summary_lines?: Array<string | { label?: string; value?: string; question?: string; answer?: string }>;
    form_answers?: Record<string, unknown>;
    lead_score?: number | string | null;
    estimated_value?: number | string | null;
    campaign_name?: string;
    intent_level?: string;
    recommended_follow_up?: string;
    created_at: string;
    last_activity_at: string;
    last_inbound_at?: string | null;
    last_outbound_at?: string | null;
    tags: string[];
  };
  client: {
    client_key: string;
    business_name: string;
    timezone?: string;
    booking_url?: string;
    fallback_handoff_number?: string;
    tone?: string;
  };
  messages: ThreadMessage[];
  notes: LeadNote[];
  tasks: LeadTask[];
  tags?: string[];
  timeline?: Array<Record<string, unknown>>;
  audit_events?: Array<Record<string, unknown>>;
  stages: string[];
};

export type TasksPayload = {
  items: LeadTask[];
  counts: Record<string, number>;
  total: number;
};

export type CalendarBooking = {
  id: number;
  lead_id?: number | null;
  lead_name?: string;
  lead_phone?: string;
  title?: string;
  status: string;
  start_at: string;
  end_at: string;
  timezone?: string;
  notes?: string;
};

export type CalendarPayload = {
  client_key: string;
  booking_mode: string;
  timezone: string;
  total: number;
  items: CalendarBooking[];
};

export type ConversationListItem = {
  lead_id: number;
  lead_name: string;
  phone: string;
  email?: string;
  source?: string;
  client_key?: string;
  client_name?: string;
  state: string;
  crm_stage?: string;
  tags: string[];
  last_message_snippet: string;
  last_message_direction?: string;
  last_message_delivery?: MessageDeliveryStatus | string | null;
  last_activity_at: string;
  archived?: boolean;
};

export type ConversationsPayload = {
  items: ConversationListItem[];
  counts: Record<string, number>;
  total: number;
};

export type ConversationThreadPayload = {
  lead: LeadDetailPayload["lead"] & {
    current_state: string;
    opted_out?: boolean;
  };
  client: LeadDetailPayload["client"];
  messages: ThreadMessage[];
  notes: LeadNote[];
  tasks: LeadTask[];
  audit_events: Array<Record<string, unknown>>;
  timeline: Array<Record<string, unknown>>;
};
