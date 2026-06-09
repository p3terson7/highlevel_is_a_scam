from __future__ import annotations

import argparse
import json

from app.services.stackleads_demo_seed import seed_stackleads_demo


def main() -> int:
    parser = argparse.ArgumentParser(description="Create or refresh the StackLeads fictional showcase client.")
    parser.add_argument("--reset", action="store_true", help="Delete previous seeded StackLeads leads before reseeding.")
    parser.add_argument(
        "--reset-portal",
        action="store_true",
        help="Reset the StackLeads client portal email/password to the known demo credentials.",
    )
    args = parser.parse_args()

    result = seed_stackleads_demo(reset=args.reset, reset_portal=args.reset_portal)
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
