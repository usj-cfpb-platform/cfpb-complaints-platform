"""MongoDB connection from environment variables (MONGO_URI, MONGO_DB)."""

from __future__ import annotations

from typing import Any

from common.config import env


def get_db(uri: str | None = None, name: str | None = None, timeout_ms: int = 5000) -> Any:
    from pymongo import MongoClient

    client = MongoClient(
        uri or env("MONGO_URI"), serverSelectionTimeoutMS=timeout_ms, tz_aware=True
    )
    client.admin.command("ping")  # fail fast with a clear error if MongoDB is not reachable
    return client[name or env("MONGO_DB", "cfpb")]
