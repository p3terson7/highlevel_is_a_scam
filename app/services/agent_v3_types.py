from __future__ import annotations

import re
from typing import Any, Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field, model_validator

from app.db.models import ConversationStateEnum

QuestionKey = Literal[
    "decision_makers",
    "urgency_driver",
]
IntentLevel = Literal["HIGH_INTENT", "MEDIUM_INTENT", "LOW_INTENT"]
ConversationAct = Literal[
    "answer_question",
    "answer_then_soft_cta",
    "ask_clarifying_question",
    "offer_slots",
    "book_selected_slot",
    "reschedule",
    "handoff",
    "nurture",
]
ActionType = Literal["none", "ask_next_question", "offer_booking", "mark_booked", "handoff_to_human"]
ToolName = Literal["none", "find_slots", "book_slot", "mark_booked", "handoff_to_human"]

_ASSISTANT_NAME = "Hermes"
_ALLOWED_STATES = {
    ConversationStateEnum.QUALIFYING,
    ConversationStateEnum.BOOKING_SENT,
    ConversationStateEnum.BOOKED,
    ConversationStateEnum.HANDOFF,
}
_ALLOWED_CONVERSATION_ACTS = {
    "answer_question",
    "answer_then_soft_cta",
    "ask_clarifying_question",
    "offer_slots",
    "book_selected_slot",
    "reschedule",
    "handoff",
    "nurture",
}
_BOOKED_CONFIRM_PATTERN = re.compile(
    r"\b(i booked|we booked|booked already|already booked|appointment booked|scheduled it|i scheduled|i'm booked|im booked|"
    r"j'ai r[ée]serv[ée]|nous avons r[ée]serv[ée]|d[ée]j[àa] r[ée]serv[ée]|rendez-vous confirm[ée])\b",
    re.IGNORECASE,
)
_HANDOFF_PATTERN = re.compile(
    r"\b(?:"
    r"(?:i (?:need|want|prefer)|get me|connect me|can i|could i|may i) (?:to )?(?:a )?(?:human|person|manager|representative)|"
    r"(?:talk|speak|connect)(?: me)? (?:to|with) (?:a )?(?:human|person|manager|representative|someone)|"
    r"(?:need|want) (?:a )?(?:human|representative)(?: now)?|call me|"
    r"someone from your team (?:can |could |to )?(?:call|contact|reach)|"
    r"(?:je (?:veux|voudrais|souhaite|dois)|puis-je|peut-on) (?:parler|[ée]changer) (?:[àa]|avec) (?:un|une) (?:humain|personne|repr[ée]sentant|g[ée]rant)|"
    r"(?:parler|[ée]changer) (?:[àa]|avec) (?:un|une) (?:humain|personne|repr[ée]sentant|g[ée]rant)|"
    r"mettez-moi en contact|appelez-moi|"
    r"quelqu'un (?:de (?:votre|l['’])[ée]quipe )?(?:peut |pourrait )?(?:m'appeler|me contacter)"
    r")\b",
    re.IGNORECASE,
)
_IDENTITY_QUESTION_PATTERN = re.compile(
    r"\b("
    r"who (?:are you|is this|runs?|owns?|founded|started|is behind)|"
    r"who'?s (?:behind|running)|"
    r"are you (?:the )?(?:founder|owner|human|person|bot|assistant)|"
    r"you(?:'re| re| are) (?:the )?(?:founder|owner|human|person)|"
    r"behind this business|behind the business"
    r")\b",
    re.IGNORECASE,
)
_ASSISTANT_OWNERSHIP_CLAIM_PATTERN = re.compile(
    r"\b("
    r"i(?: am| m|'m) (?:the )?(?:founder|owner|co-?founder)|"
    r"i (?:founded|started|own|run) (?:the|this|our)? ?(?:company|business)?|"
    r"my (?:company|business)|"
    r"we (?:founded|started|own) (?:the|this|our)? ?(?:company|business)?"
    r")\b",
    re.IGNORECASE,
)
_CLOSING_PATTERN = re.compile(r"^(thanks|thank you|ok|okay|cool|great|perfect|sounds good|merci|parfait|d'accord|super)[.! ]*$", re.IGNORECASE)
_TIMELINE_PATTERN = re.compile(
    r"\b(asap|immediately|this week|next week|within \d+\s+(?:day|days|week|weeks|month|months)|"
    r"\d+\s+(?:day|days|week|weeks|month|months)|d[èe]s que possible|imm[ée]diatement|cette semaine|"
    r"semaine prochaine|dans (?:un|une|\d+)\s+(?:jour|jours|semaine|semaines|mois)|"
    r"sous (?:un|une|\d+)\s+(?:jour|jours|semaine|semaines|mois)|au plus tard)\b",
    re.IGNORECASE,
)
_DECISION_MAKER_PATTERN = re.compile(
    r"\b(owner|founder|co-?founder|decision maker|final decision|approv|procurement|partner|stakeholder|team lead|director|"
    r"c['’]est (?:juste )?moi|ce n['’]est que moi|il n['’]y a que moi|juste moi|moi uniquement|"
    r"je suis seul|je (?:d[ée]cide|valide)|moi seul|seul d[ée]cisionnaire|personne d'autre|"
    r"it['’]s (?:just )?me|only me|just me|no one else|nobody else|"
    r"d[ée]cisionnaire|responsable|directeur|propri[ée]taire)\b",
    re.IGNORECASE,
)
_BOOKING_INTENT_PATTERN = re.compile(
    r"\b(yes|yeah|yep|sure|sounds good|works for me|let'?s do (?:it|that)|book(?: (?:it|that))?|"
    r"lock (?:it|that) in|go ahead|schedule|set it up|confirm(?: it| that)?|"
    r"oui|certainement|ça marche|ca marche|allons-y|allez-y|r[ée]server?|r[ée]servez(?:-le)?|"
    r"bloque(?:z|r)?(?:-le)?|planifier|confirmer?|confirmez(?:-le)?|prenez le rendez-vous|"
    r"prendre (?:un )?rendez-vous|je (?:veux|voudrais|souhaite) (?:prendre |r[ée]server )?(?:un )?(?:rendez-vous|appel))\b",
    re.IGNORECASE,
)
_PRICING_PATTERN = re.compile(
    r"\b(?:price|pricing|cost|quote|estimate|how much|rates?|budget|prix|tarif|"
    r"co[ûu]t|soumission|estimation|"
    r"combien\s+(?:[cç]a\s+)?co[ûu]te|combien\s+(?:pour|facturez|chargez)|"
    r"(?:[cç]a|cela)\s+co[ûu]te\s+combien|c(?:['’]|\s+)est\s+combien|"
    r"quel(?:\s+est\s+le)?\s+(?:prix|tarif)|"
    r"(?:revient|facturez|chargez)[^?.!]{0,30}\bcombien)\b",
    re.IGNORECASE,
)
_PRICE_AMOUNT_PATTERN = re.compile(
    r"(\$\s?\d|(?:under|over|around|about|roughly|starts? at|between)\s+\$?\d[\d,]*(?:\.\d+)?\s?(?:k|cad|usd|dollars?)\b|"
    r"\b\d[\d,]*(?:\.\d+)?\s?(?:cad|usd|dollars?)\b)",
    re.IGNORECASE,
)
_BUDGET_TALK_PATTERN = re.compile(r"\b(target (?:budget|range)|range in mind|spend|investment)\b", re.IGNORECASE)
_RESCHEDULE_PATTERN = re.compile(
    r"\b(reschedule|re-schedule|move (?:it|the meeting|the call)|change (?:it|the time|the meeting|the call)|"
    r"different time|another time|new time|instead|can we do|could we do|"
    r"replanifier|d[ée]placer|changer (?:l'heure|le rendez-vous|l'appel)|autre moment|une autre heure)\b",
    re.IGNORECASE,
)
_LOW_INTENT_PATTERN = re.compile(
    r"\b(just looking|just browsing|browsing|researching|early stages?|curious|not ready|"
    r"not sure yet|general idea|learn more|info only|information only|je regarde|curieux|pas pr[êe]t|information seulement|juste des infos)\b",
    re.IGNORECASE,
)
_CALL_REFUSAL_PATTERN = re.compile(
    r"\b(no call|no meeting|don'?t want (?:a )?(?:call|meeting)|do not want (?:a )?(?:call|meeting)|"
    r"not ready to (?:book|schedule)|don'?t schedule|do not schedule|email only|text only|"
    r"prefer email|rather email|stop asking|pas d'appel|pas de rencontre|pas de rendez-vous|courriel seulement|texto seulement|je pr[ée]f[èe]re par courriel)\b",
    re.IGNORECASE,
)
_MEETING_CTA_PATTERN = re.compile(
    r"\b(scoping call|consultation call|strategy call|quick call|short call|short meeting|meeting|appointment|book(?:ing)?|schedule|calendar|"
    r"availability|available times|send (?:over )?times|share (?:live )?times|find (?:a )?time|"
    r"line up (?:a )?(?:call|meeting|appointment|strategy call)|connect with (?:the )?team|coordinate (?:the )?next step|talk (?:with|to) (?:someone|the team)|"
    r"rendez[- ]vous|rencontre avec|appel (?:avec|de consultation|de cadrage|telephonique)|disponibilites?|creneaux?|"
    r"reserver|reservation|planifier|calendrier)\b",
    re.IGNORECASE,
)
_BUYING_SIGNAL_PATTERN = re.compile(
    r"\b(ready to start|ready now|need this soon|asap|urgent|this week|next week|call me|"
    r"someone call|can someone call|can we talk|move forward|next step|send me times|urgent|d[èe]s que possible|cette semaine|semaine prochaine|appelez-moi|prochaine [ée]tape|envoyez-moi des disponibilit[ée]s)\b",
    re.IGNORECASE,
)
_SCOPE_QUANTITY_PATTERN = re.compile(
    r"\b(\d[\d,]*(?:\.\d+)?\s?(?:locations?|units?|rooms?|users?|seats?|employees?|items?|orders?|accounts?))\b",
    re.IGNORECASE,
)
_DAY_NAMES = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday", "lundi", "mardi", "mercredi", "jeudi", "vendredi", "samedi", "dimanche")
_TOOL_JSON_SCHEMA = (
    '{"reply_text":"string","next_state":"QUALIFYING|BOOKING_SENT|BOOKED|HANDOFF",'
    '"conversation_act":"answer_question|answer_then_soft_cta|ask_clarifying_question|offer_slots|book_selected_slot|reschedule|handoff|nurture",'
    '"lead_intent":"string","confidence":0.0,"reasoning_summary":"string","uses_knowledge_context":"boolean",'
    '"collected_fields":{"service_needed":"string|null","timeline":"string|null","locations":"string|null",'
    '"decision_makers":"string|null","urgency_driver":"string|null","booking_intent_locked":"boolean"},'
    '"next_question_key":"decision_makers|urgency_driver|null",'
    '"action":"none|ask_next_question|offer_booking|mark_booked|handoff_to_human",'
    '"tool_call":{"name":"none|find_slots|book_slot|mark_booked|handoff_to_human","args":{}}}'
)


class QuestionSpec(BaseModel):
    key: QuestionKey
    label: str
    question: str
    description: str


_QUESTION_SPECS: tuple[QuestionSpec, ...] = (
    QuestionSpec(
        key="decision_makers",
        label="Decision-makers",
        question="Are you the decision-maker, and should anyone else join the call?",
        description="Identify who needs to attend or approve next steps.",
    ),
    QuestionSpec(
        key="urgency_driver",
        label="Urgency/driver",
        question="Is there a deadline or key date driving this (start date, event, or approval timeline)?",
        description="Identify urgency so scheduling and next steps match the timeline.",
    ),
)
_QUESTION_SPEC_BY_KEY = {spec.key: spec for spec in _QUESTION_SPECS}
_QUESTION_ORDER: tuple[QuestionKey, ...] = tuple(spec.key for spec in _QUESTION_SPECS)
_GENERIC_MISSING_FIELD_SPECS: tuple[dict[str, Any], ...] = (
    {
        "key": "desired_outcome",
        "label": "Desired outcome",
        "question": "What would a successful outcome look like for you?",
        "why": "Clarifies the desired result without assuming a specific industry or service.",
        "key_tokens": (
            "purpose",
            "goal",
            "reason",
            "use_case",
            "use case",
            "why",
            "outcome",
            "success",
            "project_description",
            "additional_information",
            "informations_additionnelles",
            "description_projet",
        ),
        "question_tokens": ("successful outcome", "successful result", "success look", "success looks", "hoping to accomplish", "main goal"),
        "question_tokens_fr": (
            "bon resultat",
            "resultat attendu",
            "resultat recherchez",
            "voulez-vous surtout",
            "souhaitez-vous obtenir",
            "objectif principal",
        ),
    },
    {
        "key": "request_type",
        "label": "Request type",
        "question": "What type of help are you looking for?",
        "why": "Clarifies fit without relying on business-specific assumptions.",
        "key_tokens": ("service", "offering", "product", "request", "need", "goal", "problem", "challenge", "interest", "scope"),
        "question_tokens": ("type of help", "help are you looking for", "what are you looking for"),
        "question_tokens_fr": ("type d'aide", "aide recherchez", "service recherchez", "besoin principal"),
    },
    {
        "key": "timeline",
        "label": "Timeline",
        "question": "When would you ideally like to get started or have this resolved?",
        "why": "Helps prioritize urgency and next steps.",
        "key_tokens": ("timeline", "timeframe", "deadline", "start", "date", "urgency", "when"),
        "question_tokens": ("when would you", "get started", "have this resolved", "timeline"),
        "question_tokens_fr": ("quand aimeriez-vous", "date de livraison", "quel delai", "quelle echeance"),
    },
    {
        "key": "decision_process",
        "label": "Decision process",
        "question": "Are you the best person to coordinate next steps, or should someone else be included?",
        "why": "Clarifies who should be involved before moving forward.",
        "key_tokens": ("decision", "approver", "stakeholder", "owner", "role", "buyer", "contact"),
        "question_tokens": ("best person", "someone else", "coordinate next steps", "decision maker", "decision-maker"),
        "question_tokens_fr": (
            "bonne personne",
            "quelqu'un d'autre",
            "personne qui valide",
            "personne qui prend la decision",
            "coordonner la suite",
        ),
    },
    {
        "key": "follow_up_contact",
        "label": "Follow-up contact",
        "question": "What is the best email or contact method if the team needs to send details?",
        "why": "Keeps follow-up easy without assuming a sales process.",
        "key_tokens": ("email", "contact", "preferred_contact", "preferred contact", "follow_up", "follow up"),
        "question_tokens": ("best email", "contact method", "send details"),
        "question_tokens_fr": ("meilleur courriel", "moyen de contact", "envoyer des details"),
    },
)


class QualificationMemory(BaseModel):
    model_config = ConfigDict(extra="ignore")

    service_needed: str | None = None
    timeline: str | None = None
    locations: str | None = None
    decision_makers: str | None = None
    urgency_driver: str | None = None
    booking_intent_locked: bool = False

    def known_fields(self) -> dict[str, Any]:
        payload = self.model_dump(exclude_none=True)
        if payload.get("booking_intent_locked") is False:
            payload.pop("booking_intent_locked", None)
        return payload


class AgentAction(BaseModel):
    type: Literal["send_booking_link", "offer_calendar_slots", "request_more_info", "handoff_to_human"]
    payload: dict[str, Any] = Field(default_factory=dict)


class ToolCall(BaseModel):
    model_config = ConfigDict(extra="ignore")

    name: ToolName = "none"
    args: dict[str, Any] = Field(default_factory=dict)


class AgentResponse(BaseModel):
    model_config = ConfigDict(extra="ignore")

    reply_text: str = ""
    next_state: ConversationStateEnum = ConversationStateEnum.QUALIFYING
    conversation_act: ConversationAct = "answer_question"
    lead_intent: str = ""
    confidence: float = 0.0
    reasoning_summary: str = ""
    uses_knowledge_context: bool = False
    collected_fields: QualificationMemory = Field(default_factory=QualificationMemory)
    next_question_key: QuestionKey | None = None
    action: ActionType = "none"
    tool_call: ToolCall = Field(default_factory=ToolCall)
    runtime_payload: dict[str, Any] = Field(default_factory=dict)
    provider: Literal["openai", "fallback"] = "openai"
    provider_error: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _coerce_shape(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        raw = dict(data)
        raw.setdefault("collected_fields", {})
        raw.setdefault("tool_call", {"name": "none", "args": {}})
        raw.setdefault("lead_intent", "")
        raw.setdefault("reasoning_summary", "")
        raw.setdefault("confidence", 0.0)
        raw.setdefault("uses_knowledge_context", False)
        tool = raw.get("tool_call")
        if isinstance(tool, dict):
            name = str(tool.get("name", "")).strip().lower()
            if name == "send_booking_link":
                tool["name"] = "find_slots"
        action = str(raw.get("action", "")).strip().lower()
        if action == "send_booking_link":
            raw["action"] = "offer_booking"
        conversation_act = str(raw.get("conversation_act") or "").strip().lower()
        if conversation_act not in _ALLOWED_CONVERSATION_ACTS:
            tool_name = str((raw.get("tool_call") or {}).get("name") if isinstance(raw.get("tool_call"), dict) else "").strip().lower()
            action_name = str(raw.get("action") or "").strip().lower()
            if tool_name == "find_slots":
                conversation_act = "offer_slots"
            elif tool_name == "book_slot":
                conversation_act = "book_selected_slot"
            elif tool_name == "handoff_to_human" or action_name == "handoff_to_human":
                conversation_act = "handoff"
            elif action_name == "ask_next_question":
                conversation_act = "ask_clarifying_question"
            elif action_name == "offer_booking":
                conversation_act = "answer_then_soft_cta"
            else:
                conversation_act = "answer_question"
        raw["conversation_act"] = conversation_act
        return raw

    @property
    def actions(self) -> list[AgentAction]:
        return _action_to_legacy(self.action, self.next_question_key, self.runtime_payload)


class LLMProvider(Protocol):
    name: str

    def generate_json(self, system_prompt: str, user_prompt: str) -> dict[str, Any]: ...



def _action_to_legacy(action: ActionType, next_question_key: QuestionKey | None, runtime_payload: dict[str, Any]) -> list[AgentAction]:
    if runtime_payload.get("booking_offer"):
        return [AgentAction(type="offer_calendar_slots", payload={})]
    if action == "ask_next_question":
        payload = {"question_key": next_question_key} if next_question_key else {}
        return [AgentAction(type="request_more_info", payload=payload)]
    if action == "handoff_to_human":
        return [AgentAction(type="handoff_to_human", payload={})]
    return []


__all__ = [name for name in globals() if not name.startswith("__")]
