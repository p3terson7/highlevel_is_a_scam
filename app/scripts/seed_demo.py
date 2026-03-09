from __future__ import annotations

import argparse
import json

from app.core.config import get_settings
from app.db.session import get_session_factory
from app.services.demo_seed import can_seed_demo, reset_demo_data, seed_demo_data


def main() -> int:
    parser = argparse.ArgumentParser(description="Seed demo clients and conversations for the UI.")
    parser.add_argument("--reset", action="store_true", help="Delete existing demo data before seeding.")
    parser.add_argument("--force", action="store_true", help="Allow seeding outside dev mode.")
    parser.add_argument("--reset-only", action="store_true", help="Delete demo data and exit.")
    args = parser.parse_args()

    settings = get_settings()
    if not args.force and not can_seed_demo(settings):
        print("Demo seed skipped because it is disabled outside dev and --force was not used.")
        return 0

    session_factory = get_session_factory()
    with session_factory() as db:
        if args.reset_only:
            result = reset_demo_data(db)
            db.commit()
        else:
            result = seed_demo_data(db, reset=args.reset)
            db.commit()

    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
