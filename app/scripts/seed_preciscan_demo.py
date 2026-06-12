from __future__ import annotations

import argparse
import json
import os

from sqlalchemy.exc import OperationalError

from app.core.config import get_settings
from app.db.session import reset_db_caches
from app.services.preciscan_demo_seed import seed_preciscan_demo


def main() -> int:
    parser = argparse.ArgumentParser(description="Create or refresh the 3D PreciScan fictional showcase client.")
    parser.add_argument("--reset", action="store_true", help="Delete previous seeded 3D PreciScan leads before reseeding.")
    parser.add_argument(
        "--reset-portal",
        action="store_true",
        help="Reset the 3D PreciScan client portal email/password to the known demo credentials.",
    )
    parser.add_argument(
        "--database-url",
        help="Override DATABASE_URL for this seed run, for example sqlite:///./local.db.",
    )
    args = parser.parse_args()

    if args.database_url:
        os.environ["DATABASE_URL"] = args.database_url
        get_settings.cache_clear()
        reset_db_caches()

    try:
        result = seed_preciscan_demo(reset=args.reset, reset_portal=args.reset_portal)
    except OperationalError as exc:
        settings = get_settings()
        if "postgres:5432" in settings.database_url and not args.database_url:
            raise SystemExit(
                "Could not connect to the default Docker Postgres host from this shell.\n"
                "Run this inside the app container instead:\n"
                "  docker compose exec api python -m app.scripts.seed_preciscan_demo --reset\n"
                "Or pass a local database explicitly after running migrations:\n"
                "  DATABASE_URL=sqlite:///./local.db alembic upgrade head\n"
                "  python -m app.scripts.seed_preciscan_demo --reset --database-url sqlite:///./local.db"
            ) from exc
        raise

    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
