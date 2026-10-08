from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from sqlalchemy import Connection, select
from sqlalchemy.exc import SQLAlchemyError

from app.db.models import AuditLog, Client, Lead
from app.db.session import get_engine


_TRACKING_ID_KEYS = {"gclid", "fbclid", "li_fat_id", "msclkid", "ad_id"}
_TRACKING_NAMED_KEYS = {"source"}
_CONSENT_KEYS = {
    "captured_at",
    "form",
    "granted",
    "method",
    "source_fields",
    "status",
    "text",
}


class ReportError(RuntimeError):
    pass


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Print a read-only, PII-minimized attribution report for retained lead rows. "
            "DATABASE_URL is read from the service environment."
        )
    )
    scope = parser.add_mutually_exclusive_group(required=True)
    scope.add_argument("--client-key", help="Report one tenant, for example 3d-preciscan.")
    scope.add_argument(
        "--all-clients",
        action="store_true",
        help="Explicitly report every tenant in the database.",
    )
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument(
        "--latest",
        action="store_true",
        help="Return the newest retained row by created_at and id instead of all rows.",
    )
    selection.add_argument("--lead-id", type=int, help="Return one exact lead ID within the selected scope.")
    return parser


def _mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _iso_utc(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _safe_scalar(value: Any, *, limit: int = 512) -> str | int | float | bool | None:
    if value is None or isinstance(value, (int, float, bool)):
        return value
    return str(value)[:limit]


def _is_tracking_key(key: str) -> bool:
    normalized = key.strip().lower()
    return normalized.startswith("utm_") or normalized in _TRACKING_ID_KEYS | _TRACKING_NAMED_KEYS


def _safe_tracking(value: Any) -> dict[str, str | int | float | bool | None]:
    tracking = _mapping(value)
    return {
        str(key): _safe_scalar(item)
        for key, item in sorted(tracking.items(), key=lambda pair: str(pair[0]))
        if _is_tracking_key(str(key))
    }


def _safe_url(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = urlsplit(text)
        hostname = parsed.hostname or ""
        if parsed.scheme.lower() not in {"http", "https"} or not hostname:
            return None
        port = parsed.port
    except ValueError:
        return None

    netloc = hostname
    if ":" in hostname and not hostname.startswith("["):
        netloc = f"[{hostname}]"
    if port is not None:
        netloc = f"{netloc}:{port}"
    safe_query = [
        (key, str(_safe_scalar(item) or ""))
        for key, item in parse_qsl(parsed.query, keep_blank_values=True)
        if _is_tracking_key(key)
    ]
    return urlunsplit(
        (
            parsed.scheme.lower(),
            netloc,
            parsed.path[:2048],
            urlencode(safe_query, doseq=True),
            "",
        )
    )


def _safe_consent(value: Any) -> dict[str, Any]:
    consent = _mapping(value)
    output: dict[str, Any] = {}
    for key in sorted(_CONSENT_KEYS):
        if key not in consent:
            continue
        item = consent[key]
        if key == "source_fields" and isinstance(item, list):
            output[key] = [str(field)[:128] for field in item[:12]]
        else:
            output[key] = _safe_scalar(item)
    return output


def _source_value(value: Any) -> str:
    return str(getattr(value, "value", value))


def _ingestion_kind(raw_payload: dict[str, Any], normalization_history: list[dict[str, Any]]) -> str:
    if isinstance(raw_payload.get("raw_website_payload"), dict):
        return "website_form"
    payload_source = str(raw_payload.get("source") or "").strip().lower()
    if payload_source in {"ui_manual_lead", "ui_manual_meeting_inline_lead"}:
        return "ui_manual"
    if str(raw_payload.get("created_from") or "").strip().lower() == "ui_ai_sandbox":
        return "test_lab"
    if isinstance(raw_payload.get("first_inbound_payload"), dict):
        return "consumer_initiated_sms"
    if normalization_history:
        return "webhook_normalized"
    return "unknown"


def _normalization_history(connection: Connection, lead_ids: list[int]) -> dict[int, list[dict[str, Any]]]:
    history: dict[int, list[dict[str, Any]]] = defaultdict(list)
    if not lead_ids:
        return history
    rows = connection.execute(
        select(AuditLog.lead_id, AuditLog.created_at, AuditLog.decision)
        .where(
            AuditLog.lead_id.in_(lead_ids),
            AuditLog.event_type == "lead_normalized",
        )
        .order_by(AuditLog.created_at.asc(), AuditLog.id.asc())
    )
    for lead_id, created_at, raw_decision in rows:
        if lead_id is None:
            continue
        decision = _mapping(raw_decision)
        history[lead_id].append(
            {
                "timestamp_utc": _iso_utc(created_at),
                "source": _safe_scalar(decision.get("source")),
                "created": decision.get("created") if isinstance(decision.get("created"), bool) else None,
                "should_send_initial_sms": (
                    decision.get("should_send_initial_sms")
                    if isinstance(decision.get("should_send_initial_sms"), bool)
                    else None
                ),
            }
        )
    return history


def build_report(
    connection: Connection,
    *,
    client_key: str | None,
    all_clients: bool,
    latest: bool = False,
    lead_id: int | None = None,
) -> dict[str, Any]:
    if bool(client_key) == bool(all_clients):
        raise ReportError("Choose exactly one of --client-key or --all-clients")
    if lead_id is not None and lead_id < 1:
        raise ReportError("--lead-id must be greater than zero")

    if client_key:
        client_exists = connection.scalar(select(Client.id).where(Client.client_key == client_key).limit(1))
        if client_exists is None:
            raise ReportError(f"Unknown client key: {client_key}")

    statement = select(
        Lead.id,
        Lead.client_id,
        Client.client_key,
        Client.timezone.label("client_timezone"),
        Lead.created_at,
        Lead.updated_at,
        Lead.source,
        Lead.raw_payload,
        Lead.consented,
        Lead.opted_out,
        Lead.phone,
        Lead.email,
        Lead.initial_sms_sent_at,
    ).join(Client, Client.id == Lead.client_id)
    if client_key:
        statement = statement.where(Client.client_key == client_key)
    if lead_id is not None:
        statement = statement.where(Lead.id == lead_id)
    if latest:
        statement = statement.order_by(Lead.created_at.desc(), Lead.id.desc()).limit(1)
    else:
        statement = statement.order_by(Lead.created_at.asc(), Lead.id.asc())

    rows = list(connection.execute(statement).mappings())
    history_by_lead = _normalization_history(connection, [int(row["id"]) for row in rows])
    leads: list[dict[str, Any]] = []
    for row in rows:
        raw_payload = _mapping(row["raw_payload"])
        submitted_payload = _mapping(raw_payload.get("raw_website_payload"))
        history = history_by_lead.get(int(row["id"]), [])
        leads.append(
            {
                "lead_id": row["id"],
                "client_id": row["client_id"],
                "client_key": row["client_key"],
                "client_timezone": row["client_timezone"],
                "created_at_utc": _iso_utc(row["created_at"]),
                "updated_at_utc": _iso_utc(row["updated_at"]),
                "stored_source": _source_value(row["source"]),
                "ingestion_kind": _ingestion_kind(raw_payload, history),
                "attribution": {
                    "tracking": _safe_tracking(raw_payload.get("tracking")),
                    "source_page_url": _safe_url(raw_payload.get("source_page_url")),
                    "referrer": _safe_url(raw_payload.get("referrer")),
                    "submitted_source": _safe_scalar(submitted_payload.get("source")),
                    "payload_source": _safe_scalar(raw_payload.get("source")),
                },
                "consent": {
                    "stored_consented": bool(row["consented"]),
                    "stored_opted_out": bool(row["opted_out"]),
                    "evidence": _safe_consent(raw_payload.get("consent_evidence")),
                },
                "phone_present": bool(row["phone"]),
                "email_present": bool(row["email"]),
                "initial_sms_sent_at_utc": _iso_utc(row["initial_sms_sent_at"]),
                "normalization_history": history,
            }
        )

    return {
        "generated_at_utc": _iso_utc(datetime.now(timezone.utc)),
        "scope": {
            "client_key": client_key,
            "all_clients": all_clients,
            "latest_only": latest,
            "lead_id": lead_id,
        },
        "reported_lead_count": len(leads),
        "first_created_at_utc": leads[0]["created_at_utc"] if leads else None,
        "last_created_at_utc": leads[-1]["created_at_utc"] if leads else None,
        "history_limitations": [
            "This includes retained lead rows only; hard-deleted leads and data removed by a reset cannot be recovered.",
            "Repeated submissions may update one existing lead; normalization_history shows retained normalization events, while raw attribution can reflect a later submission.",
        ],
        "leads": leads,
    }


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        with get_engine().connect() as connection:
            if connection.dialect.name == "postgresql":
                connection.exec_driver_sql("SET TRANSACTION READ ONLY")
            report = build_report(
                connection,
                client_key=args.client_key,
                all_clients=args.all_clients,
                latest=args.latest,
                lead_id=args.lead_id,
            )
    except ReportError as exc:
        print(f"Lead attribution report error: {exc}", file=sys.stderr)
        return 2
    except SQLAlchemyError as exc:
        print(
            f"Lead attribution report could not read DATABASE_URL ({type(exc).__name__}).",
            file=sys.stderr,
        )
        return 1

    print(json.dumps(report, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
