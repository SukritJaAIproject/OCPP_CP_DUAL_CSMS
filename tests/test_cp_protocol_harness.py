import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from tools.cp_protocol_harness import bind_cp_protocol_methods


def bound(directory, **defaults):
    return bind_cp_protocol_methods(SimpleNamespace(), directory, defaults=defaults or None)


async def test_actual_config_handlers_persist_without_chdir_or_hardware_import(tmp_path):
    before = Path.cwd()
    modules = set(sys.modules)
    one = bound(tmp_path / "one")
    two = bound(tmp_path / "two")
    assert (await one.on_change_configuration("HeartbeatInterval", "12")).status == "Accepted"
    assert (tmp_path / "one/HeartbeatInterval_cfg.txt").read_text() == "12"
    assert two.HeartbeatInterval_cfg == "30"
    assert (tmp_path / "two/HeartbeatInterval_cfg.txt").read_text() == "30"
    reloaded = bound(tmp_path / "one")
    assert reloaded.HeartbeatInterval_cfg == "12"
    assert Path.cwd() == before
    assert "CP" not in set(sys.modules) - modules
    assert "RPi.GPIO" not in set(sys.modules) - modules
    assert "paho.mqtt.client" not in set(sys.modules) - modules
    assert one._cp_protocol_harness["hardware_startup_executed"] is False


async def test_actual_readonly_unknown_and_invalid_interval_contracts(tmp_path):
    core = bound(tmp_path)
    response = await core.on_get_configuration(key=["NumberOfConnectors", "LocalAuthListMaxLength", "MISSING"])
    assert response.unknown_key == ["MISSING"]
    assert all(item["readonly"] for item in response.configuration_key)
    for key in ("NumberOfConnectors", "LocalAuthListMaxLength", "SupportedFeatureProfiles"):
        assert (await core.on_change_configuration(key, "9")).status == "Rejected"
    assert (await core.on_change_configuration("MISSING", "9")).status == "NotSupported"
    before = (tmp_path / "HeartbeatInterval_cfg.txt").read_bytes()
    for value in ("-1", "abc", "1.2", True, 2.3):
        assert (await core.on_change_configuration("HeartbeatInterval", value)).status == "Rejected"
    assert core.HeartbeatInterval_cfg == "30"
    assert (tmp_path / "HeartbeatInterval_cfg.txt").read_bytes() == before


async def test_actual_differential_capacity_failure_keeps_state_and_reload(tmp_path):
    core = bound(tmp_path, LocalAuthListMaxLength="1")
    assert (await core.on_send_local_list(1, "Full", [{"idTag": "ONE", "idTagInfo": {"status": "Accepted"}}])).status == "Accepted"
    snapshot = (tmp_path / "local_auth_list.json.snapshot").read_bytes()
    result = await core.on_send_local_list(2, "Differential", [
        {"idTag": "ONE", "idTagInfo": {"status": "Blocked"}},
        {"idTag": "TWO", "idTagInfo": {"status": "Accepted"}}])
    assert result.status == "Failed"
    assert core.local_auth_list.identifiers == {"ONE": {"status": "Accepted"}}
    assert (tmp_path / "local_auth_list.json.snapshot").read_bytes() == snapshot
    restored = bound(tmp_path)
    assert (await restored.on_get_local_list_version()).list_version == 1
    assert restored.local_auth_list.identifiers == {"ONE": {"status": "Accepted"}}


async def test_actual_clear_cache_does_not_remove_local_list(tmp_path):
    core = bound(tmp_path)
    core.authorization_cache.add("CACHED", {"status": "Accepted"})
    await core.on_send_local_list(1, "Full", [{"idTag": "LOCAL", "idTagInfo": {"status": "Accepted"}}])
    assert (await core.on_clear_cache()).status == "Accepted"
    assert core.authorization_cache.get("CACHED") is None
    assert core.local_auth_list.identifiers == {"LOCAL": {"status": "Accepted"}}


async def test_actual_config_io_failure_rejects_without_runtime_change(tmp_path, monkeypatch):
    core = bound(tmp_path)
    original = (tmp_path / "HeartbeatInterval_cfg.txt").read_bytes()
    real_replace = os.replace
    def unavailable(source, target):
        if Path(target).name == "HeartbeatInterval_cfg.txt":
            raise PermissionError("test unavailable storage")
        return real_replace(source, target)
    monkeypatch.setattr(os, "replace", unavailable)
    assert (await core.on_change_configuration("HeartbeatInterval", "15")).status == "Rejected"
    assert core.HeartbeatInterval_cfg == "30"
    assert (tmp_path / "HeartbeatInterval_cfg.txt").read_bytes() == original


async def test_bound_writer_rejects_path_escape(tmp_path):
    core = bound(tmp_path / "state")
    writer = core.on_change_configuration.__func__.__globals__["_write_configuration_file"]
    with pytest.raises(ValueError, match="state directory"):
        writer("../outside.txt", "test")
    assert not (tmp_path / "outside.txt").exists()


def test_corrupt_saved_interval_is_repaired_before_handler_use(tmp_path):
    (tmp_path / "HeartbeatInterval_cfg.txt").write_text("abc")
    (tmp_path / "MeterValueSampleInterval_cfg.txt").write_text("-3")
    core = bound(tmp_path)
    assert core.HeartbeatInterval_cfg == "30"
    assert core.MeterValueSampleInterval_cfg == "60"
    assert (tmp_path / "HeartbeatInterval_cfg.txt").read_text() == "30"
    assert (tmp_path / "MeterValueSampleInterval_cfg.txt").read_text() == "60"
