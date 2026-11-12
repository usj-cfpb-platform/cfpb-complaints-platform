"""Configuration loading shared by every layer.

Non-secret settings come from ``config/settings.yaml`` (committed).
Secrets and machine-specific values come from environment variables,
injected by Docker Compose from ``.env`` (never committed).
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SETTINGS_PATH = REPO_ROOT / "config" / "settings.yaml"
UNASSIGNED_DOMAIN = "unassigned"


class ConfigError(ValueError):
    """Raised when configuration is missing or invalid."""


@dataclass(frozen=True)
class DomainConfig:
    """The product domain our group analyses. Never hard-coded in jobs."""

    slug: str
    products: tuple[str, ...]

    @property
    def is_assigned(self) -> bool:
        return self.slug != UNASSIGNED_DOMAIN and len(self.products) > 0


def load_settings(path: Path | str = DEFAULT_SETTINGS_PATH) -> dict[str, Any]:
    """Read the YAML settings file and return it as a dictionary."""
    settings_path = Path(path)
    if not settings_path.is_file():
        raise ConfigError(f"Settings file not found: {settings_path}")
    with settings_path.open(encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ConfigError("Settings root must be a mapping")
    return data


def domain_config(settings: dict[str, Any]) -> DomainConfig:
    """Validate and return the domain section of the settings."""
    raw = settings.get("domain") or {}
    slug = str(raw.get("slug", "")).strip()
    products = raw.get("products", [])
    if not slug:
        raise ConfigError("domain.slug is required")
    if not isinstance(products, list) or not all(
        isinstance(p, str) and p.strip() for p in products
    ):
        raise ConfigError("domain.products must be a list of non-empty strings")
    cleaned = tuple(p.strip() for p in products)
    if len(set(cleaned)) != len(cleaned):
        raise ConfigError("domain.products contains duplicate labels")
    return DomainConfig(slug=slug, products=cleaned)


def require_assigned_domain(settings: dict[str, Any]) -> DomainConfig:
    """Return the domain, or fail loudly if it has not been assigned yet.

    Domain-dependent steps (Spark filter, analysis) call this so they can
    never silently run on an empty or placeholder filter.
    """
    domain = domain_config(settings)
    if not domain.is_assigned:
        raise ConfigError(
            "Domain not assigned yet: set domain.slug and domain.products in config/settings.yaml"
        )
    return domain


def env(name: str, default: str | None = None) -> str:
    """Read an environment variable; fail clearly if it is required and missing."""
    value = os.environ.get(name, default)
    if value is None or value == "":
        raise ConfigError(f"Missing required environment variable: {name}")
    return value
