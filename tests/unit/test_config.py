from pathlib import Path

import pytest

from common.config import (
    DEFAULT_SETTINGS_PATH,
    ConfigError,
    domain_config,
    env,
    load_settings,
    require_assigned_domain,
)


def _write(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "settings.yaml"
    path.write_text(text, encoding="utf-8")
    return path


def test_committed_settings_file_loads() -> None:
    settings = load_settings(DEFAULT_SETTINGS_PATH)
    assert "domain" in settings
    assert settings["privacy"]["min_cell_size"] >= 10


def test_missing_file_raises(tmp_path: Path) -> None:
    with pytest.raises(ConfigError):
        load_settings(tmp_path / "nope.yaml")


def test_unassigned_domain_is_valid_but_not_assigned(tmp_path: Path) -> None:
    settings = load_settings(_write(tmp_path, "domain:\n  slug: unassigned\n  products: []\n"))
    domain = domain_config(settings)
    assert not domain.is_assigned
    with pytest.raises(ConfigError):
        require_assigned_domain(settings)


def test_assigned_domain_is_trimmed(tmp_path: Path) -> None:
    text = "domain:\n  slug: demo\n  products: ['  Product A ', 'Product B']\n"
    domain = require_assigned_domain(load_settings(_write(tmp_path, text)))
    assert domain.products == ("Product A", "Product B")


@pytest.mark.parametrize(
    "products",
    ["'not-a-list'", "['A', '']", "['A', 'A']", "[1, 2]"],
)
def test_invalid_products_rejected(tmp_path: Path, products: str) -> None:
    settings = load_settings(_write(tmp_path, f"domain:\n  slug: demo\n  products: {products}\n"))
    with pytest.raises(ConfigError):
        domain_config(settings)


def test_env_required_and_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CFPB_TEST_VAR", raising=False)
    assert env("CFPB_TEST_VAR", "fallback") == "fallback"
    with pytest.raises(ConfigError):
        env("CFPB_TEST_VAR")
    monkeypatch.setenv("CFPB_TEST_VAR", "x")
    assert env("CFPB_TEST_VAR") == "x"
