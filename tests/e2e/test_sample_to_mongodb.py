"""End to end on the committed sample: raw -> Spark -> curated -> MongoDB, run twice.

Needs PySpark (Java 17) and a reachable MongoDB (MONGO_URI). CI runs it in the `mongodb` job.
"""

import os
from pathlib import Path

import pytest
import yaml

pytest.importorskip("pyspark")
pymongo = pytest.importorskip("pymongo")


@pytest.fixture()
def mongo_db_name(monkeypatch):
    uri = os.environ.get("MONGO_URI")
    if not uri:
        pytest.skip("MONGO_URI not set")
    client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=2000)
    try:
        client.admin.command("ping")
    except pymongo.errors.PyMongoError:
        pytest.skip("MongoDB not reachable")
    name = "cfpb_e2e_test"
    client.drop_database(name)
    monkeypatch.setenv("MONGO_DB", name)
    yield name
    client.drop_database(name)


def _permissive_rules(tmp_path: Path) -> str:
    """The committed sample contains deliberate defects; on ~40 rows they would trip the real
    thresholds. This test is about the serving layer, so it raises every fail_above."""
    from processing import quality

    rules = yaml.safe_load(Path(quality.DEFAULT_RULES_PATH).read_text(encoding="utf-8"))

    def relax(node):
        if isinstance(node, dict):
            if "fail_above" in node:
                node["fail_above"] = 1e12
                node["warn_above"] = 1e12
            for value in node.values():
                relax(value)

    relax(rules)
    path = tmp_path / "rules.yaml"
    path.write_text(yaml.safe_dump(rules), encoding="utf-8")
    return str(path)


def test_sample_reaches_mongodb_and_rerun_does_not_inflate(mongo_db_name, capsys, tmp_path) -> None:
    from database import load
    from processing import job

    assert job.main(["--sample", "--rules", _permissive_rules(tmp_path)]) == 0
    assert load.main(["--sample"]) == 0
    first = capsys.readouterr().out
    assert "BALANCED: True" in first

    assert load.main(["--sample"]) == 0  # replay of the same curated data
    second = capsys.readouterr().out
    assert "BALANCED: True" in second
    assert "inserted/updated/unchanged: 0 / 0 /" in second

    client = pymongo.MongoClient(os.environ["MONGO_URI"])
    db = client[mongo_db_name]
    run = db.pipeline_runs.find_one(sort=[("finished_at", -1)])
    assert run["balanced"] is True
    assert db.complaints.count_documents({}) == run["curated"]["read_by_loader"]
    assert db.agg_monthly_counts.estimated_document_count() > 0
