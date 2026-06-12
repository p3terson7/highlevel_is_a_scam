from app.db.session import _normalize_database_url


def test_normalize_render_postgres_url_to_installed_driver():
    assert (
        _normalize_database_url("postgresql://user:pass@host:5432/db")
        == "postgresql+psycopg://user:pass@host:5432/db"
    )


def test_normalize_legacy_postgres_url_to_installed_driver():
    assert (
        _normalize_database_url("postgres://user:pass@host:5432/db")
        == "postgresql+psycopg://user:pass@host:5432/db"
    )


def test_normalize_keeps_explicit_driver_url():
    assert (
        _normalize_database_url("postgresql+psycopg://user:pass@host:5432/db")
        == "postgresql+psycopg://user:pass@host:5432/db"
    )
