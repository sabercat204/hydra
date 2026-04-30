"""CLI tool to generate and insert an API key.

Usage:
    python scripts/create_api_key.py --name "analyst-1" --scopes read,search,write

1. Generate random 32-byte key → base64url encode.
2. SHA-256 hash the key.
3. Insert into api_keys table.
4. Print the raw key ONCE (never stored in plaintext).
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


async def create_key(name: str, scopes: list[str]) -> None:
    import asyncpg

    from hydra.config import settings

    # Generate key
    raw_bytes = os.urandom(32)
    raw_key = base64.urlsafe_b64encode(raw_bytes).decode().rstrip("=")
    key_hash = hashlib.sha256(raw_key.encode()).hexdigest()

    # Parse DSN for asyncpg (strip +asyncpg suffix)
    dsn = settings.database.postgres_dsn.replace("+asyncpg", "").replace("postgresql", "postgresql", 1)

    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute(
            """
            INSERT INTO api_keys (key_hash, name, scopes)
            VALUES ($1, $2, $3)
            """,
            key_hash,
            name,
            scopes,
        )
    finally:
        await conn.close()

    print(f"API Key created successfully!")
    print(f"  Name:   {name}")
    print(f"  Scopes: {', '.join(scopes)}")
    print(f"  Key:    {raw_key}")
    print()
    print("⚠️  Save this key now — it will NOT be shown again.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Create a HYDRA API key")
    parser.add_argument("--name", required=True, help="Human-readable key name")
    parser.add_argument(
        "--scopes",
        default="read,search,write",
        help="Comma-separated scopes (default: read,search,write)",
    )
    args = parser.parse_args()
    scopes = [s.strip() for s in args.scopes.split(",")]
    asyncio.run(create_key(args.name, scopes))


if __name__ == "__main__":
    main()
