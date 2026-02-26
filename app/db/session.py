from functools import lru_cache
from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import get_settings


@lru_cache
def get_engine():
    settings = get_settings()
    connect_args = {"check_same_thread": False} if settings.database_url.startswith("sqlite") else {}
    return create_engine(settings.database_url, pool_pre_ping=True, future=True, connect_args=connect_args)


@lru_cache
def get_session_factory() -> sessionmaker[Session]:
    return sessionmaker(
        bind=get_engine(),
        class_=Session,
        autoflush=False,
        autocommit=False,
        expire_on_commit=False,
    )


def get_db() -> Generator[Session, None, None]:
    session = get_session_factory()()
    try:
        yield session
    finally:
        session.close()


def reset_db_caches() -> None:
    get_session_factory.cache_clear()
    get_engine.cache_clear()
