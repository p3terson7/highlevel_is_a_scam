#!/usr/bin/env python3
"""Fail when the Alembic revision graph does not have exactly one head."""

from __future__ import annotations

from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    config = Config(str(REPOSITORY_ROOT / "alembic.ini"))
    revisions = ScriptDirectory.from_config(config)
    heads = revisions.get_heads()

    if len(heads) != 1:
        rendered_heads = ", ".join(heads) if heads else "none"
        raise SystemExit(
            "Expected exactly one Alembic migration head; "
            f"found {len(heads)} ({rendered_heads}). Create a merge revision before merging."
        )

    print(f"Alembic migration head: {heads[0]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
