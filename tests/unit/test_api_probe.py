from datetime import date
from urllib.parse import parse_qs, urlparse

from scripts.api_probe import build_probe_url, record_fields, total_hits


def test_probe_url_is_date_bounded_and_small() -> None:
    url = build_probe_url("https://example.org/api/", date(2026, 11, 12), window_days=30, size=5)
    query = parse_qs(urlparse(url).query)
    assert query["date_received_min"] == ["2026-10-13"]
    assert query["date_received_max"] == ["2026-11-12"]
    assert query["size"] == ["5"]
    assert query["no_aggs"] == ["true"]


def test_total_hits_accepts_both_shapes() -> None:
    assert total_hits({"hits": {"total": {"value": 42, "relation": "eq"}}}) == 42
    assert total_hits({"hits": {"total": 7}}) == 7
    assert total_hits({}) is None


def test_record_fields_lists_names_only() -> None:
    payload = {"hits": {"hits": [{"_source": {"product": "x", "complaint_id": "1"}}]}}
    assert record_fields(payload) == ["complaint_id", "product"]
    assert record_fields({"hits": {"hits": []}}) == []
