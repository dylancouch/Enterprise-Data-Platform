"""Local ledger selection for repeatable repair commands."""

from __future__ import annotations

import json
import multiprocessing
from pathlib import Path

import pytest

from data_pipelines.construction_pipeline import run_pipeline
from data_pipelines.saas_pipeline import run as run_saas
from data_pipelines.spark_delta import append_ledger_entry, pending_failed_run, single_local_writer


def test_ledger_replacement_preserves_prior_entries(tmp_path: Path) -> None:
    path = tmp_path / "evidence" / "run_ledger.jsonl"
    append_ledger_entry(path, {"run_id": "first", "status": "FAILED"})
    append_ledger_entry(path, {"run_id": "second", "status": "SUCCESS", "repairs_run_id": "first"})
    assert [json.loads(line)["run_id"] for line in path.read_text(encoding="utf-8").splitlines()] == [
        "first", "second"]


def _hold_writer_lock(root: Path, ready: multiprocessing.Queue, release: multiprocessing.Queue) -> None:
    with single_local_writer(root):
        ready.put(True)
        release.get(timeout=10)


def test_second_local_writer_is_refused_until_first_releases(tmp_path: Path) -> None:
    ready = multiprocessing.Queue()
    release = multiprocessing.Queue()
    worker = multiprocessing.Process(target=_hold_writer_lock, args=(tmp_path, ready, release))
    worker.start()
    try:
        assert ready.get(timeout=10) is True
        with pytest.raises(RuntimeError, match="Another pipeline writer"), single_local_writer(tmp_path):
            pass
        with pytest.raises(RuntimeError, match="Another pipeline writer"):
            run_pipeline(tmp_path)
        with pytest.raises(RuntimeError, match="Another pipeline writer"):
            run_saas(tmp_path, None)
    finally:
        release.put(True)
        worker.join(timeout=10)
        if worker.is_alive():
            worker.terminate()
            worker.join()
    assert worker.exitcode == 0
    with single_local_writer(tmp_path):
        pass


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
