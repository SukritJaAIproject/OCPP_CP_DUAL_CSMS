import importlib.util
import json
from pathlib import Path

import pytest
from helpf.dual_csms import DualCSMSManager, DualCSMSError, check_single_csms_fallback


def config():
    return {"enabled": True, "local_csms": "A", "admin_csms": "A",
            "servers": [{"name": "A", "url": "ws://127.0.0.1:1/ocpp/test"}]}


async def test_disabling_dual_cannot_reinterpret_pending_session(tmp_path):
    manager = DualCSMSManager(config(), tmp_path)
    manager.claim_local(1, "tag")
    await manager.close()
    with pytest.raises(DualCSMSError, match="Cannot disable"):
        check_single_csms_fallback(tmp_path)


async def test_idle_single_fallback_does_not_mutate_state(tmp_path):
    manager = DualCSMSManager(config(), tmp_path)
    manager.claim_local(1, "tag")
    assert manager.release_pending(1)
    await manager.close()
    database = tmp_path / "dual_csms_state.sqlite3"
    before = database.read_bytes()
    check_single_csms_fallback(tmp_path)
    assert database.read_bytes() == before


async def test_readonly_diagnostic_reports_unknown_without_payload_or_tag(tmp_path):
    manager = DualCSMSManager(config(), tmp_path)
    manager.claim_local(1, "DO_NOT_DISPLAY_THIS_TAG")
    manager.db.execute("UPDATE sessions SET state='unknown'")
    manager.db.commit()
    await manager.close()
    path = Path(__file__).resolve().parents[1] / "dual_csms_check.py"
    spec = importlib.util.spec_from_file_location("diagnostic", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    report = module.inspect_state(tmp_path)
    assert report["sessions"][0]["state"] == "unknown"
    assert report["attention"]
    assert "DO_NOT_DISPLAY_THIS_TAG" not in json.dumps(report)
