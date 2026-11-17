"""Local ledger selection for repeatable repair commands."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from data_pipelines.spark_delta import pending_failed_run


def test_pending_failure_skips_successful_repair_chain(tmp_path: Path) -> None:
    path = tmp_path / "evidence/run_ledger.jsonl"
    path.parent.mkdir()
    entries = [
        {"domain": "saas", "run_id": "first", "status": "FAILED_INJECTED"},
        {"domain": "saas", "run_id": "retry", "status": "FAILED", "repairs_run_id": "first"},
        {"domain": "construction", "run_id": "other", "status": "FAILED"},
    ]
    path.write_text("".join(json.dumps(entry) + "\n" for entry in entries), encoding="utf-8")
    assert pending_failed_run(tmp_path, "saas")["run_id"] == "retry"
    assert pending_failed_run(tmp_path, "construction")["run_id"] == "other"
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps({"domain": "saas", "run_id": "done", "status": "SUCCESS",
                                 "repairs_run_id": "retry"}) + "\n")
    with pytest.raises(ValueError, match="No unrepaired failed run"):
        pending_failed_run(tmp_path, "saas")
