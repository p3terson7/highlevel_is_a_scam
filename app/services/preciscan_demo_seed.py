from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models import (
    AuditLog,
    CalendarBooking,
    Client,
    ConversationState,
    ConversationStateEnum,
    Lead,
    LeadSource,
    LeadTag,
    LeadTask,
    Message,
    MessageDirection,
)
from app.db.session import get_session_factory
from app.services.crm import (
    CRM_STAGE_CONTACTED,
    CRM_STAGE_LOST,
    CRM_STAGE_MEETING_BOOKED,
    CRM_STAGE_MEETING_COMPLETED,
    CRM_STAGE_NEW_LEAD,
    CRM_STAGE_QUALIFIED,
    CRM_STAGE_WON,
    TASK_STATUS_DONE,
    TASK_STATUS_OPEN,
)
from app.services.portal_auth import hash_portal_password

CLIENT_KEY = "3d-preciscan"
BUSINESS_NAME = "3D PreciScan"
DEMO_PREFIX = "preciscan-demo"
PORTAL_EMAIL = "demo@3dpreciscan.local"
PORTAL_PASSWORD = "PreciScanDemo2026!"
CLIENT_TIMEZONE = "America/Toronto"

FORM_QUESTIONS = [
    "Quel service vous intéresse?",
    "Quelle est votre situation actuelle?",
    "Quel livrable souhaitez-vous obtenir?",
    "Quelle est l’urgence du projet?",
]


@dataclass(frozen=True)
class PreciScanLeadSpec:
    slug: str
    full_name: str
    company: str
    email: str
    phone: str
    city: str
    source: LeadSource
    campaign: str
    status_fr: str
    crm_stage: str
    conversation_state: ConversationStateEnum
    created_offset: timedelta
    score: int
    estimated_value: int
    service: str
    urgency: str
    form_answers: dict[str, str]
    notes: str
    tags: list[str]
    task: str | None = None
    booking: dict | None = None


def _local_datetime(days_offset: int, hour: int, minute: int) -> datetime:
    tz = ZoneInfo(CLIENT_TIMEZONE)
    local_day = datetime.now(tz).date() + timedelta(days=days_offset)
    return datetime.combine(local_day, time(hour, minute), tzinfo=tz).astimezone(timezone.utc)


def _client_ai_context() -> str:
    return (
        "3D PreciScan est une entreprise B2B au Québec spécialisée en scan 3D, rétro-ingénierie, "
        "inspection 3D/métrologie, conception CAD, fichiers STEP et FEA. Les meilleurs prospects sont "
        "des manufacturiers, ateliers d'usinage, équipes de maintenance et responsables qualité qui ont "
        "une pièce physique, un enjeu industriel concret et un livrable technique à obtenir rapidement. "
        "Qualifier en priorité: urgence, présence de la pièce, livrable souhaité, dimensions, plans existants, "
        "site de scan et impact production."
    )


def _client_faq_context() -> str:
    return (
        "3D PreciScan aide les équipes industrielles à recréer des pièces sans plan CAD, produire des fichiers "
        "STEP, valider des composantes avant production, inspecter des pièces avec rapport dimensionnel et "
        "scanner de grosses pièces sur site. Les demandes urgentes liées à un arrêt de production ou à un "
        "fournisseur disparu doivent être priorisées pour un rappel rapide."
    )


def _booking_config() -> dict:
    return {
        "internal_calendar": {
            "slot_minutes": 30,
            "notice_minutes": 120,
            "horizon_days": 21,
            "availability": [
                {"day": day, "start": "08:30", "end": "12:00", "enabled": True}
                for day in range(5)
            ]
            + [
                {"day": day, "start": "13:00", "end": "16:30", "enabled": True}
                for day in range(4)
            ],
        }
    }


def _answers(
    service: str,
    situation: str,
    livrable: str,
    urgence: str,
    *_extra: str,
) -> dict[str, str]:
    values = [service, situation, livrable, urgence]
    return dict(zip(FORM_QUESTIONS, values, strict=True))


def _lead_specs() -> list[PreciScanLeadSpec]:
    return [
        PreciScanLeadSpec(
            slug="maintenance-piece-brisee",
            full_name="Martin Gagnon",
            company="Fonderie Laurentide",
            email="martin.gagnon@fonderielaurentide.ca",
            phone="+14185550147",
            city="Québec, QC",
            source=LeadSource.LINKEDIN,
            campaign="LinkedIn - Pièce sans plan CAD",
            status_fr="Nouveau",
            crm_stage=CRM_STAGE_NEW_LEAD,
            conversation_state=ConversationStateEnum.NEW,
            created_offset=timedelta(hours=-2, minutes=-10),
            score=94,
            estimated_value=18500,
            service="Rétro-ingénierie + fichier STEP",
            urgency="Très urgent",
            form_answers=_answers(
                "Rétro-ingénierie d'une pièce critique",
                "Une roue dentée est brisée et nous n'avons aucun plan CAD exploitable.",
                "Fichier STEP et dessin technique pour refaire usiner la pièce.",
                "Très urgent, arrêt partiel de production depuis ce matin.",
                "Oui, la pièce brisée et une pièce usée de référence sont disponibles.",
                "Seulement des photos et une ancienne fiche fournisseur incomplète.",
                "Québec, secteur Vanier.",
                "Environ 280 mm de diamètre, 45 mm d'épaisseur.",
                "Directeur maintenance",
                "Aujourd'hui entre 13 h et 15 h.",
            ),
            notes="Lead chaud: arrêt de production, pièce physique disponible, décisionnaire maintenance.",
            tags=["urgent", "piece-sans-plan", "step", "maintenance"],
            task="Appeler Martin pour confirmer la tolérance critique et organiser le dépôt de la pièce.",
        ),
        PreciScanLeadSpec(
            slug="qualite-validation-avant-production",
            full_name="Sophie Tremblay",
            company="Plastiques Beauce",
            email="sophie.tremblay@plastiquesbeauce.ca",
            phone="+14185550218",
            city="Saint-Georges, QC",
            source=LeadSource.LINKEDIN,
            campaign="LinkedIn - Inspection 3D",
            status_fr="Contacté",
            crm_stage=CRM_STAGE_CONTACTED,
            conversation_state=ConversationStateEnum.QUALIFYING,
            created_offset=timedelta(days=-1, hours=-3),
            score=82,
            estimated_value=7200,
            service="Inspection 3D / métrologie",
            urgency="Cette semaine",
            form_answers=_answers(
                "Inspection 3D et rapport dimensionnel",
                "Nous voulons valider les premières pièces avant de lancer la production complète.",
                "Rapport dimensionnel avec écarts versus modèle CAD.",
                "Cette semaine, avant le go/no-go production.",
                "Oui, trois échantillons sont en main.",
                "Oui, fichier STEP et plan PDF avec tolérances principales.",
                "Saint-Georges, Beauce.",
                "Pièces injectées d'environ 180 x 120 x 70 mm.",
                "Responsable qualité",
                "Demain matin avant 10 h.",
            ),
            notes="Besoin clair de validation avant production; comparer 3 échantillons au CAD client.",
            tags=["inspection-3d", "qualite", "avant-production"],
            task="Envoyer la liste des formats CAD acceptés et demander le plan de tolérances.",
        ),
        PreciScanLeadSpec(
            slug="usinage-retro-ingenierie",
            full_name="François Bouchard",
            company="Usinage Bouchard",
            email="fbouchard@usinagebouchard.ca",
            phone="+15145550306",
            city="Longueuil, QC",
            source=LeadSource.META,
            campaign="Meta Retargeting - Rétro-ingénierie",
            status_fr="À qualifier",
            crm_stage=CRM_STAGE_QUALIFIED,
            conversation_state=ConversationStateEnum.QUALIFYING,
            created_offset=timedelta(days=-2, hours=-5),
            score=76,
            estimated_value=9600,
            service="Scan 3D + rétro-ingénierie",
            urgency="1 à 2 semaines",
            form_answers=_answers(
                "Scan 3D et modèle CAD usinable",
                "Un client nous demande de fabriquer une pièce copiée à partir d'un modèle existant.",
                "STEP propre pour programmation CNC et dessin de fabrication.",
                "Idéalement dans 10 jours ouvrables.",
                "Oui, la pièce originale est à l'atelier.",
                "Photos seulement, aucun plan.",
                "Longueuil, atelier d'usinage.",
                "Bloc machiné environ 12 x 8 x 4 pouces.",
                "Propriétaire de l'atelier",
                "Fin de journée, après 16 h.",
            ),
            notes="Bon potentiel atelier partenaire; vérifier si besoin de reverse engineering paramétrique complet.",
            tags=["retro-ingenierie", "usinage", "cnc"],
            task="Qualifier le niveau de précision requis avant soumission.",
        ),
        PreciScanLeadSpec(
            slug="fournisseur-disparu",
            full_name="Karine Lavoie",
            company="Équipements Norbec",
            email="karine.lavoie@equipementsnorbec.ca",
            phone="+14505550441",
            city="Drummondville, QC",
            source=LeadSource.META,
            campaign="Facebook - Fournisseur disparu",
            status_fr="Soumission envoyée",
            crm_stage=CRM_STAGE_MEETING_COMPLETED,
            conversation_state=ConversationStateEnum.HANDOFF,
            created_offset=timedelta(days=-4, hours=-1),
            score=88,
            estimated_value=24000,
            service="Rétro-ingénierie de composante discontinuée",
            urgency="Moins de 2 semaines",
            form_answers=_answers(
                "Rétro-ingénierie et conception CAD",
                "Notre fournisseur ne produit plus une composante utilisée dans notre ligne d'assemblage.",
                "Fichier STEP, plan 2D et recommandation matériau si possible.",
                "Moins de 2 semaines.",
                "Oui, nous avons deux unités neuves en stock.",
                "Ancien plan papier scanné, mais plusieurs cotes sont illisibles.",
                "Drummondville, QC.",
                "Assemblage d'environ 400 x 220 x 180 mm.",
                "Acheteuse technique",
                "Lundi ou mardi entre 9 h et 11 h.",
            ),
            notes="Soumission de démo envoyée; enjeu fournisseur disparu avec budget probable.",
            tags=["fournisseur-disparu", "soumission", "cad"],
            task="Relancer Karine sur la soumission et demander les contraintes matière.",
            booking={"days_offset": 2, "hour": 9, "minute": 30, "duration": 30, "status": "scheduled"},
        ),
        PreciScanLeadSpec(
            slug="ingenieur-inspection-fea",
            full_name="Marc-Antoine Roy",
            company="Hydro-Mécanique Saguenay",
            email="marcantoine.roy@hydromecsaguenay.ca",
            phone="+14185550529",
            city="Saguenay, QC",
            source=LeadSource.LINKEDIN,
            campaign="LinkedIn - Inspection 3D",
            status_fr="Appel réservé",
            crm_stage=CRM_STAGE_MEETING_BOOKED,
            conversation_state=ConversationStateEnum.BOOKED,
            created_offset=timedelta(days=-1, minutes=-40),
            score=90,
            estimated_value=14500,
            service="Inspection 3D + support FEA",
            urgency="Cette semaine",
            form_answers=_answers(
                "Inspection 3D, rapport dimensionnel et préparation FEA",
                "Nous voulons comparer une pièce soudée au modèle théorique avant simulation.",
                "Rapport d'écarts, nuage de points et modèle simplifié pour FEA.",
                "Cette semaine si possible.",
                "Oui, la pièce est au laboratoire.",
                "Oui, STEP et quelques photos de l'assemblage.",
                "Saguenay, arrondissement Chicoutimi.",
                "Environ 900 x 600 x 350 mm.",
                "Ingénieur mécanique",
                "Mercredi après-midi.",
            ),
            notes="Appel réservé; demande technique crédible avec inspection et FEA.",
            tags=["fea", "inspection-3d", "ingenierie"],
            task="Préparer questions sur contraintes de simulation FEA.",
            booking={"days_offset": 1, "hour": 14, "minute": 0, "duration": 45, "status": "scheduled"},
        ),
        PreciScanLeadSpec(
            slug="grosse-piece-sur-site",
            full_name="Nathalie Côté",
            company="Béton Préfab Québec",
            email="n.cote@betonprefabquebec.ca",
            phone="+15815550662",
            city="Trois-Rivières, QC",
            source=LeadSource.LINKEDIN,
            campaign="LinkedIn - Pièce sans plan CAD",
            status_fr="Contacté",
            crm_stage=CRM_STAGE_CONTACTED,
            conversation_state=ConversationStateEnum.QUALIFYING,
            created_offset=timedelta(days=-3, hours=-2),
            score=79,
            estimated_value=16500,
            service="Scan 3D sur site",
            urgency="2 à 3 semaines",
            form_answers=_answers(
                "Scan 3D d'une grosse pièce sur site",
                "Nous devons documenter un moule existant trop lourd pour être déplacé.",
                "Nuage de points, maillage et fichier STEP si faisable.",
                "Dans les 2 à 3 prochaines semaines.",
                "Oui, mais la pièce doit rester à l'usine.",
                "Photos et mesures manuelles partielles.",
                "Trois-Rivières, QC.",
                "Moule d'environ 3,2 m x 1,4 m x 0,8 m.",
                "Directrice opérations",
                "Jeudi matin.",
            ),
            notes="Prévoir logistique scan sur site; valider accès, éclairage et temps machine.",
            tags=["scan-sur-site", "grosse-piece", "operations"],
            task="Demander photos de l'environnement et contraintes d'accès au moule.",
        ),
        PreciScanLeadSpec(
            slug="curieux-moins-qualifie",
            full_name="Julien Morin",
            company="Atelier Métal Nord",
            email="julien.morin@ateliermetalnord.ca",
            phone="+14505550738",
            city="Laval, QC",
            source=LeadSource.META,
            campaign="Meta Retargeting - Rétro-ingénierie",
            status_fr="Perdu",
            crm_stage=CRM_STAGE_LOST,
            conversation_state=ConversationStateEnum.HANDOFF,
            created_offset=timedelta(hours=-8),
            score=38,
            estimated_value=2500,
            service="Information générale scan 3D",
            urgency="Pas urgent",
            form_answers=_answers(
                "Je veux comprendre les prix du scan 3D",
                "Nous sommes curieux pour un projet futur, rien de précis pour l'instant.",
                "Peut-être un STL ou STEP, à déterminer.",
                "Pas urgent, probablement dans quelques mois.",
                "Pas encore, la pièce est chez un client.",
                "Non, seulement une idée approximative.",
                "Laval, QC.",
                "Inconnu.",
                "Technicien méthodes",
                "Vendredi après 15 h.",
            ),
            notes="Lead moins qualifié; bon exemple pour scoring bas et nurturing.",
            tags=["curieux", "faible-intention", "nurture"],
        ),
        PreciScanLeadSpec(
            slug="arret-production-urgent",
            full_name="Isabelle Fortin",
            company="Emballages Richelieu",
            email="isabelle.fortin@emballagesrichelieu.ca",
            phone="+15145550884",
            city="Saint-Hyacinthe, QC",
            source=LeadSource.META,
            campaign="Facebook - Fournisseur disparu",
            status_fr="Appel réservé",
            crm_stage=CRM_STAGE_MEETING_BOOKED,
            conversation_state=ConversationStateEnum.BOOKED,
            created_offset=timedelta(minutes=-55),
            score=97,
            estimated_value=32000,
            service="Rétro-ingénierie urgente",
            urgency="Immédiat",
            form_answers=_answers(
                "Rétro-ingénierie urgente d'une pièce de ligne",
                "Un guide mécanique a cassé; arrêt de production partiel sur la ligne d'emballage.",
                "STEP et dessin pour fabrication rapide chez notre machiniste.",
                "Immédiat, impact direct sur la production aujourd'hui.",
                "Oui, pièce cassée et ancienne pièce de rechange.",
                "Photos, aucune donnée CAD.",
                "Saint-Hyacinthe, QC.",
                "Environ 650 x 90 x 40 mm.",
                "Superviseure production",
                "Le plus vite possible, téléphone direct.",
            ),
            notes="Priorité maximale; proposer scan express et remise STEP accélérée.",
            tags=["urgence-production", "hot", "step"],
            task="Appeler immédiatement et vérifier si collecte de pièce possible aujourd'hui.",
            booking={"days_offset": 0, "hour": 15, "minute": 30, "duration": 30, "status": "scheduled"},
        ),
        PreciScanLeadSpec(
            slug="recurrent-inspection-plusieurs-pieces",
            full_name="Pierre-Luc Simard",
            company="Aérocomposites Montréal",
            email="pl.simard@aerocompositesmtl.ca",
            phone="+14385550921",
            city="Montréal, QC",
            source=LeadSource.LINKEDIN,
            campaign="LinkedIn - Inspection 3D",
            status_fr="Gagné",
            crm_stage=CRM_STAGE_WON,
            conversation_state=ConversationStateEnum.HANDOFF,
            created_offset=timedelta(days=-8, hours=-6),
            score=91,
            estimated_value=45000,
            service="Inspection 3D récurrente",
            urgency="Mensuel",
            form_answers=_answers(
                "Inspection 3D de plusieurs pièces composites",
                "Nous cherchons un partenaire externe pour inspecter des lots récurrents.",
                "Rapports dimensionnels comparatifs et archive des écarts par lot.",
                "Premier lot dans deux semaines, puis récurrent.",
                "Oui, 12 pièces sont disponibles pour le premier lot.",
                "Oui, CAD, plans PDF et critères qualité internes.",
                "Montréal, secteur Saint-Laurent.",
                "Pièces variant de 300 mm à 1,2 m.",
                "Gestionnaire qualité fournisseur",
                "Mardi après 10 h.",
            ),
            notes="Démo gagnée; excellent compte potentiel récurrent avec volume multi-pièces.",
            tags=["gagne", "recurrent", "inspection-3d"],
            task="Créer dossier client récurrent et préparer checklist de réception des lots.",
            booking={"days_offset": -2, "hour": 10, "minute": 30, "duration": 45, "status": "completed"},
        ),
        PreciScanLeadSpec(
            slug="retargeting-no-show",
            full_name="Amélie Pelletier",
            company="Machinerie Vallée",
            email="amelie.pelletier@machinerievallee.ca",
            phone="+18195551036",
            city="Sherbrooke, QC",
            source=LeadSource.META,
            campaign="Meta Retargeting - Rétro-ingénierie",
            status_fr="No-show",
            crm_stage=CRM_STAGE_LOST,
            conversation_state=ConversationStateEnum.BOOKED,
            created_offset=timedelta(days=-5, hours=-4),
            score=58,
            estimated_value=6800,
            service="Conception CAD à partir d'une pièce",
            urgency="À confirmer",
            form_answers=_answers(
                "Scan 3D et conception CAD",
                "Nous avons visité votre site après avoir comparé quelques fournisseurs.",
                "Un fichier STEP modifiable pour une pièce de machine.",
                "À confirmer, pas d'arrêt de production présentement.",
                "Oui, une pièce est disponible.",
                "Quelques photos, pas de plan officiel.",
                "Sherbrooke, QC.",
                "Environ 220 x 160 x 90 mm.",
                "Chargée de projet",
                "Après-midi, mais mon horaire varie beaucoup.",
            ),
            notes="No-show après retargeting; garder en relance douce, intention moyenne.",
            tags=["no-show", "retargeting", "moyenne-intention"],
            task="Relancer avec deux plages horaires courtes et demander photos de la pièce.",
            booking={"days_offset": -1, "hour": 13, "minute": 30, "duration": 30, "status": "no_show"},
        ),
    ]


def _upsert_client(db: Session, *, reset_portal: bool = False) -> tuple[Client, bool]:
    client = db.scalar(select(Client).where(Client.client_key == CLIENT_KEY).limit(1))
    portal_credentials_reset = reset_portal
    if client is None:
        client = Client(client_key=CLIENT_KEY, business_name=BUSINESS_NAME)
        db.add(client)
        db.flush()
        portal_credentials_reset = True

    client.business_name = BUSINESS_NAME
    client.tone = "professionnel, technique et direct"
    client.timezone = CLIENT_TIMEZONE
    client.qualification_questions = FORM_QUESTIONS
    client.booking_url = "https://3dpreciscan.example/consultation-technique"
    client.booking_mode = "internal"
    client.booking_config = _booking_config()
    client.provider_config = {
        "website_url": "https://3dpreciscan.example",
        "demo_industry": "scan 3D industriel et rétro-ingénierie",
    }
    client.fallback_handoff_number = "+15145550113"
    client.consent_text = "Répondez STOP pour vous désabonner. Des frais de messagerie peuvent s'appliquer."
    if portal_credentials_reset or not client.portal_email.strip() or not client.portal_password_hash.strip():
        client.portal_display_name = "3D PreciScan Demo"
        client.portal_email = PORTAL_EMAIL
        client.portal_password_hash = hash_portal_password(PORTAL_PASSWORD)
        portal_credentials_reset = True
    elif not client.portal_display_name.strip():
        client.portal_display_name = "3D PreciScan Demo"
    client.portal_enabled = True
    client.operating_hours = {"days": [0, 1, 2, 3, 4], "start": "08:30", "end": "16:30"}
    client.faq_context = _client_faq_context()
    client.ai_context = _client_ai_context()
    client.template_overrides = {}
    client.is_active = True
    db.flush()
    return client, portal_credentials_reset


def _reset_seeded_demo(db: Session, client: Client) -> int:
    leads = db.scalars(
        select(Lead).where(
            Lead.client_id == client.id,
            Lead.external_lead_id.is_not(None),
            Lead.external_lead_id.like(f"{DEMO_PREFIX}-%"),
        )
    ).all()
    deleted = len(leads)
    for lead in leads:
        db.delete(lead)
    db.flush()
    return deleted


def _seed_lead(db: Session, client: Client, spec: PreciScanLeadSpec, now: datetime) -> None:
    created_at = now + spec.created_offset
    intent_reasons = [
        spec.urgency,
        spec.service,
        f"Valeur estimée: {spec.estimated_value:,} $".replace(",", " "),
    ]
    lead = Lead(
        client_id=client.id,
        external_lead_id=f"{DEMO_PREFIX}-{spec.slug}",
        source=spec.source,
        full_name=spec.full_name,
        phone=spec.phone,
        email=spec.email,
        city=spec.city,
        form_answers=spec.form_answers,
        raw_payload={
            "seeded": True,
            "seed_group": DEMO_PREFIX,
            "company": spec.company,
            "campaign_name": spec.campaign,
            "status": spec.status_fr,
            "lead_score": spec.score,
            "intent_score": spec.score,
            "estimated_value": spec.estimated_value,
            "service": spec.service,
            "urgency": spec.urgency,
            "notes": spec.notes,
            "lead_summary": {
                "intent_level": "Élevé" if spec.score >= 80 else "Moyen" if spec.score >= 55 else "Faible",
                "intent_reasons": intent_reasons,
                "qualification_level": spec.status_fr,
                "recommended_follow_up": spec.task or "Relance de qualification à planifier.",
            },
        },
        consented=True,
        opted_out=False,
        conversation_state=spec.conversation_state,
        crm_stage=spec.crm_stage,
        owner_name="Équipe PreciScan",
        created_at=created_at,
        updated_at=created_at,
    )
    db.add(lead)
    db.flush()

    audit_at = created_at + timedelta(minutes=2)
    db.add(
        AuditLog(
            client_id=client.id,
            lead_id=lead.id,
            event_type="lead_gen_form_received",
            decision={
                "seeded": True,
                "seed_group": DEMO_PREFIX,
                "campaign_name": spec.campaign,
                "status": spec.status_fr,
                "lead_score": spec.score,
                "estimated_value": spec.estimated_value,
            },
            created_at=audit_at,
        )
    )
    db.add(
        ConversationState(
            lead_id=lead.id,
            previous_state=ConversationStateEnum.NEW,
            new_state=spec.conversation_state,
            reason=f"Statut de démo: {spec.status_fr}",
            metadata_json={"seeded": True, "seed_group": DEMO_PREFIX, "campaign_name": spec.campaign},
            created_at=audit_at + timedelta(minutes=1),
        )
    )
    outbound_at = created_at + timedelta(minutes=8)
    if spec.crm_stage != CRM_STAGE_NEW_LEAD:
        db.add(
            Message(
                lead_id=lead.id,
                client_id=client.id,
                direction=MessageDirection.OUTBOUND,
                body=(
                    f"Bonjour {spec.full_name.split()[0]}, merci pour votre demande. "
                    "On peut valider la pièce, le livrable et l'urgence pour vous orienter rapidement."
                ),
                provider_message_sid=f"PRECISCAN-DEMO-{lead.id}-1",
                raw_payload={"seeded": True, "seed_group": DEMO_PREFIX, "agent": {"action": "qualification"}},
                created_at=outbound_at,
            )
        )
        lead.initial_sms_sent_at = outbound_at
        lead.last_outbound_at = outbound_at

    for tag in spec.tags:
        db.add(LeadTag(lead_id=lead.id, client_id=client.id, tag=tag, created_at=created_at + timedelta(minutes=4)))

    if spec.task:
        task_status = TASK_STATUS_DONE if spec.crm_stage in {CRM_STAGE_WON, CRM_STAGE_MEETING_COMPLETED} else TASK_STATUS_OPEN
        completed_at = created_at + timedelta(days=1) if task_status == TASK_STATUS_DONE else None
        db.add(
            LeadTask(
                lead_id=lead.id,
                client_id=client.id,
                title=spec.task,
                description=spec.notes,
                due_date=date.today() if task_status == TASK_STATUS_OPEN else None,
                status=task_status,
                completed_at=completed_at,
                created_by="seed",
                created_at=created_at + timedelta(minutes=12),
                updated_at=completed_at or created_at + timedelta(minutes=12),
            )
        )

    db.add(
        AuditLog(
            client_id=client.id,
            lead_id=lead.id,
            event_type="internal_note",
            decision={"seeded": True, "seed_group": DEMO_PREFIX, "note": spec.notes, "actor_label": "3D PreciScan Demo"},
            created_at=created_at + timedelta(minutes=15),
        )
    )

    if spec.booking:
        start_at = _local_datetime(spec.booking["days_offset"], spec.booking["hour"], spec.booking["minute"])
        db.add(
            CalendarBooking(
                client_id=client.id,
                lead_id=lead.id,
                provider="internal",
                source="manual",
                status=spec.booking["status"],
                start_at=start_at,
                end_at=start_at + timedelta(minutes=spec.booking["duration"]),
                timezone=CLIENT_TIMEZONE,
                title=f"Appel technique - {spec.company}",
                notes=f"Démo PreciScan: {spec.service}. {spec.notes}",
                created_at=created_at + timedelta(minutes=20),
                updated_at=created_at + timedelta(minutes=20),
            )
        )

    lead.updated_at = max(outbound_at if spec.crm_stage != CRM_STAGE_NEW_LEAD else audit_at, created_at + timedelta(minutes=15))


def seed_preciscan_demo_data(db: Session, *, reset: bool = False, reset_portal: bool = False) -> dict:
    client, portal_credentials_reset = _upsert_client(db, reset_portal=reset or reset_portal)
    deleted = _reset_seeded_demo(db, client) if reset else 0
    existing = db.scalar(
        select(Lead.id)
        .where(
            Lead.client_id == client.id,
            Lead.external_lead_id.is_not(None),
            Lead.external_lead_id.like(f"{DEMO_PREFIX}-%"),
        )
        .limit(1)
    )
    if existing is not None:
        return {
            "seeded": False,
            "reason": "preciscan_demo_data_already_present",
            "client_key": client.client_key,
            "business_name": client.business_name,
            "portal_email": client.portal_email,
            "portal_password": PORTAL_PASSWORD if portal_credentials_reset else None,
            "portal_credentials_reset": portal_credentials_reset,
            "deleted_previous_seeded_leads": deleted,
        }

    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    specs = _lead_specs()
    for spec in specs:
        _seed_lead(db, client, spec, now)
    db.flush()
    return {
        "seeded": True,
        "client_key": client.client_key,
        "business_name": client.business_name,
        "seeded_leads": len(specs),
        "portal_email": client.portal_email,
        "portal_password": PORTAL_PASSWORD if portal_credentials_reset else None,
        "portal_credentials_reset": portal_credentials_reset,
        "deleted_previous_seeded_leads": deleted,
        "recommended_showcase_lead": "Isabelle Fortin",
    }


def seed_preciscan_demo(*, reset: bool = False, reset_portal: bool = False) -> dict:
    session_factory = get_session_factory()
    with session_factory() as db:
        result = seed_preciscan_demo_data(db, reset=reset, reset_portal=reset_portal)
        db.commit()
        return result
