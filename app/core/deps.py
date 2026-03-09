from functools import lru_cache

from fastapi import Depends
from sqlalchemy.orm import Session

from app.core.config import Settings, get_settings
from app.db.session import get_db
from app.services.booking import BookingService, build_booking_service
from app.services.llm_agent import LLMAgent, build_llm_agent
from app.services.runtime_config import load_runtime_overrides
from app.services.sms_service import SMSService, build_sms_service


@lru_cache
def _sms_singleton() -> SMSService:
    settings = get_settings()
    return build_sms_service(settings)


@lru_cache
def _llm_singleton() -> LLMAgent:
    settings = get_settings()
    return build_llm_agent(settings)


def get_app_settings() -> Settings:
    return get_settings()


def get_sms_service(
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
) -> SMSService:
    overrides = load_runtime_overrides(db)
    if overrides:
        return build_sms_service(settings, runtime_overrides=overrides)
    return _sms_singleton()


def get_llm_agent(
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_app_settings),
) -> LLMAgent:
    overrides = load_runtime_overrides(db)
    if overrides:
        return build_llm_agent(settings, runtime_overrides=overrides)
    return _llm_singleton()


def get_booking_service(
    settings: Settings = Depends(get_app_settings),
) -> BookingService:
    return build_booking_service(timeout_seconds=settings.request_timeout_seconds)


def clear_dependency_caches() -> None:
    _sms_singleton.cache_clear()
    _llm_singleton.cache_clear()
