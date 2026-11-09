"""Functional contracts for real CP.py methods, extracted without hardware startup.

Only named AST definitions are compiled. Hardware/system actions are mocks and
all filesystem state belongs to pytest's temporary directory. Strict xfails are
known contract defects, not passing OCPP feature support.
"""
import ast
import asyncio
import copy
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, call as mock_call

import pytest
from ocpp.v16 import call, call_result


SOURCE = Path(__file__).resolve().parents[1] / "CP.py"
TREE = ast.parse(SOURCE.read_text(encoding="utf-8"))
CP = next(n for n in TREE.body if isinstance(n, ast.ClassDef) and n.name == "ChargePoint")


def extract(methods=(), definitions=(), **overrides):
    nodes = [copy.deepcopy(n) for n in TREE.body
             if isinstance(n, (ast.ClassDef, ast.FunctionDef)) and n.name in definitions]
    assert len(nodes) == len(definitions)
    methods_nodes = [copy.deepcopy(n) for n in CP.body
                     if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name in methods]
    assert len(methods_nodes) == len(methods)
    for node in methods_nodes:
        node.decorator_list = []
    if methods_nodes:
        nodes.append(ast.ClassDef(name="Core", bases=[], keywords=[], body=methods_nodes, decorator_list=[]))
    module = ast.fix_missing_locations(ast.Module(body=nodes, type_ignores=[]))
    env = dict(ast=ast, asyncio=asyncio, logging=logging, os=os, json=json,
               datetime=datetime, timezone=timezone, call=call, call_result=call_result,
               RATED_POWER_W=180000, RATED_CURRENT_A=500,
               AUTOCHARGE_PREFIX_MAX_LEN=8,
               AUTOCHARGE_PREFIX_ALLOWED=set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.:"),
               convert_timeformat=lambda value: value.isoformat(), STOP_PIN_KEYS=set(),
               stop_pin_config=SimpleNamespace(configuration_items=lambda: []))
    env.update(overrides)
    exec(compile(module, str(SOURCE), "exec"), env)
    return env


@pytest.fixture
def config_core():
    writes = []
    env = extract(methods=("on_get_configuration", "on_change_configuration"),
                  definitions=("_parse_csv_list", "_normalize_csv_string", "_normalize_autocharge_prefix"),
                  _write_configuration_file=lambda name, value: writes.append((name, value)))
    core = env["Core"]()
    values = {"HeartbeatInterval": "30", "AuthorizationCacheEnabled": "false",
              "LocalAuthListEnabled": "false", "AutoChargeEnabled": "false",
              "AutoChargeIdTagPrefix": "AUTO-", "LocalAuthListMaxLength": 1000,
              "StopTxnSampledData": "0", "MeterValuesSampledData": "Energy.Active.Import.Register",
              "AllowOfflineTxForUnknownId": "false", "AuthorizeRemoteTxRequests": "false",
              "LocalAuthorizeOffline": "false", "LocalPreAuthorize": "false",
              "MeterValueSampleInterval": "5", "StopTransactionOnInvalidId": "true",
              "TransactionMessageAttempts": "3", "TransactionMessageRetryInterval": "10",
              "WebSocketPingInterval": "20"}
    for key, value in values.items():
        setattr(core, key + "_cfg", value)
    core._autocharge_prefix = lambda: core.AutoChargeIdTagPrefix_cfg
    core._seed_autocharge_events = Mock()
    core._v2g_bridge = None
    return core, writes, env


async def test_get_configuration_filters_deduplicates_and_reports_unknown(config_core):
    core, _, _ = config_core
    result = await core.on_get_configuration(key=["HeartbeatInterval", "UNKNOWN", "HeartbeatInterval"])
    assert result.configuration_key == [{"key": "HeartbeatInterval", "readonly": False, "value": "30"}]
    assert result.unknown_key == ["UNKNOWN"]


@pytest.mark.parametrize("keys", [None, [], ["", "   ", None]])
async def test_get_all_configuration_accepts_omitted_or_blank_filter(config_core, keys):
    core, _, _ = config_core
    result = await core.on_get_configuration(key=keys)
    keys = [item["key"] for item in result.configuration_key]
    assert {"NumberOfConnectors", "SupportedFeatureProfiles", "HeartbeatInterval",
            "LocalAuthListMaxLength", "MeterValueSampleInterval", "AutoChargeEnabled"} <= set(keys)
    assert len(keys) == len(set(keys))
    assert result.unknown_key is None
    assert next(item for item in result.configuration_key if item["key"] == "LocalAuthListMaxLength")["readonly"] is True


@pytest.mark.parametrize("key,value", [("TransactionMessageAttempts", "0"),
                                     ("TransactionMessageRetryInterval", " 12 "),
                                     ("WebSocketPingInterval", "9")])
async def test_nonnegative_retry_configuration_is_persisted(config_core, key, value):
    core, writes, _ = config_core
    assert (await core.on_change_configuration(key, value)).status == "Accepted"
    assert getattr(core, key + "_cfg") == str(int(value))
    assert writes == [(key + "_cfg.txt", str(int(value)))]


@pytest.mark.parametrize("key", ["TransactionMessageAttempts", "TransactionMessageRetryInterval", "WebSocketPingInterval"])
@pytest.mark.parametrize("value", ["-1", "abc", "1.5"])
async def test_invalid_retry_configuration_has_no_mutation(config_core, key, value):
    core, writes, _ = config_core
    before = getattr(core, key + "_cfg")
    assert (await core.on_change_configuration(key, value)).status == "Rejected"
    assert getattr(core, key + "_cfg") == before
    assert writes == []


@pytest.mark.parametrize("value", ["YES", "1", "", "null"])
async def test_autocharge_rejects_non_boolean_values(config_core, value):
    core, writes, _ = config_core
    assert (await core.on_change_configuration("AutoChargeEnabled", value)).status == "Rejected"
    assert core.AutoChargeEnabled_cfg == "false"
    assert not writes


async def test_enabling_autocharge_seeds_existing_vehicle_once(config_core):
    core, writes, _ = config_core
    assert (await core.on_change_configuration("AutoChargeEnabled", " TRUE ")).status == "Accepted"
    assert core.AutoChargeEnabled_cfg == "true"
    assert writes == [("AutoChargeEnabled_cfg.txt", "true")]
    core._seed_autocharge_events.assert_called_once()


@pytest.mark.parametrize("value", ["", "A" * 9, "BAD PREFIX", "BAD/"])
async def test_autocharge_prefix_cannot_exceed_ocpp_tag_budget(config_core, value):
    core, writes, _ = config_core
    assert (await core.on_change_configuration("AutoChargeIdTagPrefix", value)).status == "Rejected"
    assert core.AutoChargeIdTagPrefix_cfg == "AUTO-"
    assert not writes


async def test_sampled_data_normalizes_list_and_empty_selection(config_core):
    core, writes, _ = config_core
    assert (await core.on_change_configuration("MeterValuesSampledData", "Voltage, ,Current.Import")).status == "Accepted"
    assert core.MeterValuesSampledData_cfg == "Voltage,Current.Import"
    assert (await core.on_change_configuration("StopTxnSampledData", "")).status == "Accepted"
    assert core.StopTxnSampledData_cfg == "0"
    assert len(writes) == 2


async def test_readonly_configuration_must_not_change(config_core):
    core, writes, _ = config_core
    before = core.LocalAuthListMaxLength_cfg
    assert (await core.on_change_configuration("LocalAuthListMaxLength", "4")).status == "Rejected"
    assert core.LocalAuthListMaxLength_cfg == before and writes == []


@pytest.mark.parametrize("key,value", [("HeartbeatInterval", "-1"), ("HeartbeatInterval", "abc"),
                                     ("MeterValueSampleInterval", "-3"), ("MeterValueSampleInterval", "nonsense")])
async def test_invalid_timing_configuration_must_not_be_accepted(config_core, key, value):
    core, writes, _ = config_core
    assert (await core.on_change_configuration(key, value)).status == "Rejected"
    assert writes == []


async def test_unknown_configuration_has_not_supported_response(config_core):
    core, writes, _ = config_core
    assert (await core.on_change_configuration("NoSuchOcppKey", "42")).status == "NotSupported"
    assert (await core.on_change_configuration("V2GUnknownKey", "42")).status == "NotSupported"
    assert writes == []


@pytest.mark.parametrize("key", ["NumberOfConnectors", "SupportedFeatureProfiles", "LocalAuthListMaxLength"])
async def test_all_advertised_readonly_keys_reject_writes(config_core, key):
    core, writes, _ = config_core
    result = await core.on_get_configuration(key=[key])
    assert result.configuration_key[0]["readonly"] is True
    assert (await core.on_change_configuration(key, "99")).status == "Rejected"
    assert writes == []


@pytest.mark.parametrize("key", ["HeartbeatInterval", "MeterValueSampleInterval", "TransactionMessageAttempts"])
@pytest.mark.parametrize("value", [True, False, 1.5, float("nan"), float("inf"), None, "2147483648", "1e2"])
async def test_numeric_configuration_rejects_noninteger_and_overflow_without_mutating(config_core, key, value):
    core, writes, _ = config_core
    before = getattr(core, key + "_cfg")
    assert (await core.on_change_configuration(key, value)).status == "Rejected"
    assert getattr(core, key + "_cfg") == before
    assert writes == []


async def test_zero_sample_interval_is_valid_but_zero_heartbeat_is_rejected(config_core):
    core, writes, _ = config_core
    assert (await core.on_change_configuration("HeartbeatInterval", "0")).status == "Rejected"
    assert (await core.on_change_configuration("MeterValueSampleInterval", "0")).status == "Accepted"
    assert writes == [("MeterValueSampleInterval_cfg.txt", "0")]


@pytest.mark.parametrize("key", ["AuthorizationCacheEnabled", "LocalAuthListEnabled", "AllowOfflineTxForUnknownId",
                                     "AuthorizeRemoteTxRequests", "LocalAuthorizeOffline", "LocalPreAuthorize",
                                     "StopTransactionOnInvalidId"])
async def test_boolean_configuration_is_strict_and_canonical(config_core, key):
    core, writes, _ = config_core
    before = getattr(core, key + "_cfg")
    assert (await core.on_change_configuration(key, "yes")).status == "Rejected"
    assert getattr(core, key + "_cfg") == before and writes == []
    assert (await core.on_change_configuration(key, " TRUE ")).status == "Accepted"
    assert getattr(core, key + "_cfg") == "true"
    assert writes == [(key + "_cfg.txt", "true")]


async def test_configuration_write_failure_preserves_runtime_and_previous_file(config_core, tmp_path, monkeypatch):
    core, _, env = config_core
    monkeypatch.chdir(tmp_path)
    file = tmp_path / "HeartbeatInterval_cfg.txt"
    file.write_text("30")
    env["_write_configuration_file"] = extract(definitions=("_write_configuration_file",))["_write_configuration_file"]
    monkeypatch.setattr(os, "replace", Mock(side_effect=PermissionError("test storage unavailable")))
    assert (await core.on_change_configuration("HeartbeatInterval", "12")).status == "Rejected"
    assert core.HeartbeatInterval_cfg == "30"
    assert file.read_text() == "30"
    assert sorted(p.name for p in tmp_path.iterdir()) == ["HeartbeatInterval_cfg.txt"]


async def test_configuration_persists_canonical_value_before_runtime_publication(config_core, tmp_path, monkeypatch):
    core, _, env = config_core
    monkeypatch.chdir(tmp_path)
    actual = extract(definitions=("_write_configuration_file",))["_write_configuration_file"]
    observed = []
    def persist(path, value):
        observed.append(core.HeartbeatInterval_cfg)
        actual(path, value)
    env["_write_configuration_file"] = persist
    assert (await core.on_change_configuration("HeartbeatInterval", " 0012 ")).status == "Accepted"
    assert observed == ["30"]
    assert core.HeartbeatInterval_cfg == "12"
    assert (tmp_path / "HeartbeatInterval_cfg.txt").read_text() == "12"


@pytest.fixture
def auth_env():
    return extract(methods=("on_clear_cache", "on_get_local_list_version", "on_send_local_list"),
                   definitions=("AuthorizationCache", "LocalAuthorizationList"))


async def test_clear_cache_removes_authorization_without_touching_local_list(auth_env, tmp_path):
    core = auth_env["Core"]()
    core.authorization_cache = auth_env["AuthorizationCache"](size=2)
    core.authorization_cache.add("CARD1", {"status": "Accepted"})
    local = auth_env["LocalAuthorizationList"](storage_path=str(tmp_path / "list.json"), version_path=str(tmp_path / "version"))
    local.apply_send_local_list(1, "Full", [{"idTag": "LOCAL", "idTagInfo": {"status": "Accepted"}}])
    core.local_auth_list = local
    assert (await core.on_clear_cache()).status == "Accepted"
    assert core.authorization_cache.get("CARD1") is None
    assert local.identifiers == {"LOCAL": {"status": "Accepted"}}
    assert local.list_version == 1


async def test_clear_cache_reports_failure(auth_env):
    core = auth_env["Core"]()
    core.authorization_cache = SimpleNamespace(clear=Mock(side_effect=RuntimeError("cache fault")))
    assert (await core.on_clear_cache()).status == "Rejected"


async def test_full_and_differential_local_lists_persist_and_reload(auth_env, tmp_path):
    kwargs = dict(max_length=3, storage_path=str(tmp_path / "list.json"), version_path=str(tmp_path / "version"))
    core = auth_env["Core"]()
    core.local_auth_list = auth_env["LocalAuthorizationList"](**kwargs)
    assert (await core.on_send_local_list(1, "Full", [
        {"idTag": "ONE", "idTagInfo": {"status": "Accepted"}},
        {"id_tag": "TWO", "id_tag_info": {"status": "Blocked"}}])).status == "Accepted"
    assert (await core.on_get_local_list_version()).list_version == 1
    assert (await core.on_send_local_list(2, "Differential", [
        {"idTag": "ONE"}, {"idTag": "TWO", "idTagInfo": {"status": "Accepted"}},
        {"idTag": "THREE", "idTagInfo": {"status": "Blocked"}}])).status == "Accepted"
    reloaded = auth_env["LocalAuthorizationList"](**kwargs)
    assert reloaded.list_version == 2
    assert reloaded.identifiers == {"TWO": {"status": "Accepted"}, "THREE": {"status": "Blocked"}}


async def test_stale_local_list_version_preserves_existing_data(auth_env, tmp_path):
    core = auth_env["Core"]()
    local = core.local_auth_list = auth_env["LocalAuthorizationList"](
        storage_path=str(tmp_path / "list.json"), version_path=str(tmp_path / "version"))
    assert (await core.on_send_local_list(2, "Full", [{"idTag": "ONE", "idTagInfo": {"status": "Accepted"}}])).status == "Accepted"
    before = dict(local.identifiers)
    assert (await core.on_send_local_list(2, "Differential", [])).status == "VersionMismatch"
    assert local.identifiers == before and local.list_version == 2


@pytest.mark.parametrize("version,update_type", [("not-an-integer", "Full"), (1, "Unsupported")])
async def test_invalid_local_list_request_does_not_create_entries(auth_env, tmp_path, version, update_type):
    core = auth_env["Core"]()
    local = core.local_auth_list = auth_env["LocalAuthorizationList"](
        storage_path=str(tmp_path / "list.json"), version_path=str(tmp_path / "version"))
    assert (await core.on_send_local_list(version, update_type, [
        {"idTag": "CARD", "idTagInfo": {"status": "Accepted"}}])).status == "Failed"
    assert local.identifiers == {} and local.list_version == 0


async def test_full_empty_list_clears_entries_and_advances_persisted_version(auth_env, tmp_path):
    core = auth_env["Core"]()
    paths = dict(storage_path=str(tmp_path / "list.json"), version_path=str(tmp_path / "version"))
    core.local_auth_list = auth_env["LocalAuthorizationList"](**paths)
    await core.on_send_local_list(1, "Full", [{"idTag": "ONE", "idTagInfo": {"status": "Accepted"}}])
    assert (await core.on_send_local_list(2, "Full", [])).status == "Accepted"
    restored = auth_env["LocalAuthorizationList"](**paths)
    assert restored.identifiers == {} and restored.list_version == 2


async def test_full_local_list_capacity_failure_preserves_previous_list(auth_env, tmp_path):
    core = auth_env["Core"]()
    local = core.local_auth_list = auth_env["LocalAuthorizationList"](
        max_length=1, storage_path=str(tmp_path / "list.json"), version_path=str(tmp_path / "version"))
    await core.on_send_local_list(1, "Full", [{"idTag": "ONE", "idTagInfo": {"status": "Accepted"}}])
    response = await core.on_send_local_list(2, "Full", [
        {"idTag": "NEW1", "idTagInfo": {"status": "Accepted"}},
        {"idTag": "NEW2", "idTagInfo": {"status": "Accepted"}}])
    assert response.status == "Failed"
    assert local.identifiers == {"ONE": {"status": "Accepted"}} and local.list_version == 1


async def test_failed_differential_list_update_is_atomic(auth_env, tmp_path):
    core = auth_env["Core"]()
    local = core.local_auth_list = auth_env["LocalAuthorizationList"](
        max_length=1, storage_path=str(tmp_path / "list.json"), version_path=str(tmp_path / "version"))
    await core.on_send_local_list(1, "Full", [{"idTag": "ONE", "idTagInfo": {"status": "Accepted"}}])
    response = await core.on_send_local_list(2, "Differential", [
        {"idTag": "ONE", "idTagInfo": {"status": "Blocked"}},
        {"idTag": "NEW", "idTagInfo": {"status": "Accepted"}}])
    assert response.status == "Failed"
    assert local.identifiers == {"ONE": {"status": "Accepted"}} and local.list_version == 1


@pytest.mark.parametrize("version", [1, 2])
async def test_full_list_can_replace_same_or_lower_version(auth_env, tmp_path, version):
    local = auth_env["LocalAuthorizationList"](storage_path=str(tmp_path / "list.json"), version_path=str(tmp_path / "version"))
    assert local.apply_send_local_list(2, "Full", [{"idTag": "ONE", "idTagInfo": {"status": "Accepted"}}]) == "Accepted"
    assert local.apply_send_local_list(version, "Full", [{"idTag": "TWO", "idTagInfo": {"status": "Blocked"}}]) == "Accepted"
    assert local.list_version == version and local.identifiers == {"TWO": {"status": "Blocked"}}


@pytest.mark.parametrize("update_type", ["Full", "Differential"])
async def test_duplicate_local_tags_reject_whole_update(auth_env, tmp_path, update_type):
    local = auth_env["LocalAuthorizationList"](storage_path=str(tmp_path / "list.json"), version_path=str(tmp_path / "version"))
    local.apply_send_local_list(1, "Full", [{"idTag": "ONE", "idTagInfo": {"status": "Accepted"}}])
    before = {p.name: p.read_bytes() for p in tmp_path.iterdir()}
    assert local.apply_send_local_list(2, update_type, [
        {"idTag": "ONE", "idTagInfo": {"status": "Blocked"}},
        {"idTag": "ONE", "idTagInfo": {"status": "Accepted"}}]) == "Failed"
    assert local.list_version == 1 and local.identifiers == {"ONE": {"status": "Accepted"}}
    assert {p.name: p.read_bytes() for p in tmp_path.iterdir()} == before


@pytest.mark.parametrize("bad_entry", [
    {"idTag": "ONE", "idTagInfo": {"status": "Other"}},
    {"idTag": "ONE", "idTagInfo": {"status": "Accepted", "expiryDate": "not-a-date"}},
    {"idTag": "ONE", "idTagInfo": {"status": "Accepted", "expiryDate": "2026-01-01T00:00:00"}},
    {"idTag": "ONE", "idTagInfo": {"status": "Accepted", "parentIdTag": "X" * 21}},
    {"idTag": "X" * 21, "idTagInfo": {"status": "Accepted"}},
    {"idTag": "  ", "idTagInfo": {"status": "Accepted"}},
    {"idTag": "ONE", "idTagInfo": object()},
])
async def test_malformed_local_update_leaves_canonical_and_memory_unchanged(auth_env, tmp_path, bad_entry):
    local = auth_env["LocalAuthorizationList"](storage_path=str(tmp_path / "list.json"), version_path=str(tmp_path / "version"))
    local.apply_send_local_list(1, "Full", [{"idTag": "ORIGINAL", "idTagInfo": {"status": "Accepted"}}])
    before = {p.name: p.read_bytes() for p in tmp_path.iterdir()}
    assert local.apply_send_local_list(2, "Differential", [bad_entry]) == "Failed"
    assert local.list_version == 1 and local.identifiers == {"ORIGINAL": {"status": "Accepted"}}
    assert {p.name: p.read_bytes() for p in tmp_path.iterdir()} == before


async def test_local_list_canonical_write_failure_has_no_partial_state(auth_env, tmp_path, monkeypatch):
    paths = dict(storage_path=str(tmp_path / "list.json"), version_path=str(tmp_path / "version"))
    local = auth_env["LocalAuthorizationList"](**paths)
    local.apply_send_local_list(1, "Full", [{"idTag": "ONE", "idTagInfo": {"status": "Accepted"}}])
    before = {p.name: p.read_bytes() for p in tmp_path.iterdir()}
    real_replace = os.replace
    def unavailable(source, target):
        if str(target).endswith(".snapshot"):
            raise PermissionError("test disk write failure")
        return real_replace(source, target)
    monkeypatch.setattr(os, "replace", unavailable)
    assert local.apply_send_local_list(2, "Differential", [{"idTag": "ONE", "idTagInfo": {"status": "Blocked"}}]) == "Failed"
    assert local.list_version == 1 and local.identifiers == {"ONE": {"status": "Accepted"}}
    assert {p.name: p.read_bytes() for p in tmp_path.iterdir()} == before
    restored = auth_env["LocalAuthorizationList"](**paths)
    assert restored.list_version == 1 and restored.identifiers == local.identifiers


async def test_local_list_migrates_legacy_pair_without_changing_legacy_shape(auth_env, tmp_path):
    file, version = tmp_path / "list.json", tmp_path / "version"
    file.write_text(json.dumps({"ONE": {"status": "Accepted"}}))
    version.write_text("5")
    paths = dict(storage_path=str(file), version_path=str(version))
    local = auth_env["LocalAuthorizationList"](**paths)
    assert local.list_version == 5 and local.identifiers == {"ONE": {"status": "Accepted"}}
    assert local.apply_send_local_list(6, "Differential", [{"idTag": "ONE", "idTagInfo": {"status": "Blocked"}}]) == "Accepted"
    assert json.loads(file.read_text()) == {"ONE": {"status": "Blocked"}}
    assert version.read_text() == "6"
    assert json.loads(Path(local.snapshot_path).read_text())["list_version"] == 6
    restored = auth_env["LocalAuthorizationList"](**paths)
    assert restored.list_version == 6 and restored.identifiers == local.identifiers


async def test_committed_local_snapshot_wins_over_failed_legacy_version_mirror(auth_env, tmp_path, monkeypatch):
    file, version = tmp_path / "list.json", tmp_path / "version"
    paths = dict(storage_path=str(file), version_path=str(version))
    local = auth_env["LocalAuthorizationList"](**paths)
    local.apply_send_local_list(1, "Full", [{"idTag": "ONE", "idTagInfo": {"status": "Accepted"}}])
    real_write = local._atomic_write
    def write(path, text):
        if path == str(version):
            raise PermissionError("legacy mirror unavailable")
        return real_write(path, text)
    monkeypatch.setattr(local, "_atomic_write", write)
    assert local.apply_send_local_list(2, "Full", [{"idTag": "TWO", "idTagInfo": {"status": "Blocked"}}]) == "Accepted"
    assert version.read_text() == "1"
    restored = auth_env["LocalAuthorizationList"](**paths)
    assert restored.list_version == 2 and restored.identifiers == {"TWO": {"status": "Blocked"}}


async def test_corrupt_canonical_list_fails_closed_instead_of_loading_older_mirror(auth_env, tmp_path):
    file, version = tmp_path / "list.json", tmp_path / "version"
    file.write_text(json.dumps({"OLD_ACCEPTED": {"status": "Accepted"}}))
    version.write_text("1")
    Path(str(file) + ".snapshot").write_text("{broken")
    local = auth_env["LocalAuthorizationList"](storage_path=str(file), version_path=str(version))
    assert local.list_version == 0 and local.identifiers == {}


async def test_local_snapshot_does_not_share_input_references(auth_env, tmp_path):
    local = auth_env["LocalAuthorizationList"](storage_path=str(tmp_path / "list.json"), version_path=str(tmp_path / "version"))
    info = {"status": "Accepted", "expiryDate": "2099-01-01T00:00:00Z"}
    assert local.apply_send_local_list(1, "Full", [{"idTag": "ONE", "idTagInfo": info}]) == "Accepted"
    info["status"] = "Blocked"
    assert local.identifiers["ONE"]["status"] == "Accepted"


async def test_full_local_update_requires_info_and_empty_full_can_reset_version_zero(auth_env, tmp_path):
    paths = dict(storage_path=str(tmp_path / "list.json"), version_path=str(tmp_path / "version"))
    local = auth_env["LocalAuthorizationList"](**paths)
    assert local.apply_send_local_list(1, "Full", [{"idTag": "ONE", "idTagInfo": {"status": "Accepted"}}]) == "Accepted"
    assert local.apply_send_local_list(2, "Full", [{"idTag": "ONE"}]) == "Failed"
    assert local.identifiers == {"ONE": {"status": "Accepted"}} and local.list_version == 1
    assert local.apply_send_local_list(0, "Full", []) == "Accepted"
    restored = auth_env["LocalAuthorizationList"](**paths)
    assert restored.identifiers == {} and restored.list_version == 0


@pytest.mark.parametrize("version", [True, False, 2.5, "2.5", "2147483648", -1])
async def test_invalid_local_list_version_has_no_persistence_or_memory_change(auth_env, tmp_path, version):
    local = auth_env["LocalAuthorizationList"](storage_path=str(tmp_path / "list.json"), version_path=str(tmp_path / "version"))
    assert local.apply_send_local_list(version, "Full", []) == "Failed"
    assert local.list_version == 0 and local.identifiers == {}
    assert list(tmp_path.iterdir()) == []


@pytest.fixture
async def profile_core(tmp_path, monkeypatch):
    from helpf.ocpp_runtime import OCPPProtocolRuntime
    monkeypatch.chdir(tmp_path)
    calls = {"power": Mock(), "current": Mock(), "split": Mock()}
    env = extract(methods=("on_set_charging_profile", "on_clear_chargingprofile", "on_get_compositeSchedul"))
    core = env["Core"]()
    core.myip_adr_c1, core.myip_adr_c2 = "1", "2"
    core.call = AsyncMock()
    core._applied_limits = {}
    for key in ("usiLinkStateCcs1", "iCpStateCss1", "usiLinkStateCcs2", "iCpStateCss2"):
        setattr(core, key, "Preparing")
    core._active_transaction_id = lambda cid: 100 + cid if getattr(core, f"iCpStateCss{cid}") == "Charging" else 0
    async def apply_limits(cid, limits):
        calls["power"](str(cid), limits["W"])
        calls["current"](str(cid), limits["A"])
        core._applied_limits[cid] = dict(limits)
        return True
    core._ocpp_apply_limits = apply_limits
    core._ocpp_limit_fault_stop = AsyncMock()
    runtime = OCPPProtocolRuntime(core, tmp_path)
    core._ocpp_protocol = lambda: runtime
    await runtime.bootstrap()
    for mocked in calls.values():
        mocked.reset_mock()
    return core, calls, runtime


def profile(limit=10000, unit="W", *, pid=1, purpose="TxDefaultProfile"):
    return {"charging_profile_id": pid, "stack_level": 0,
            "charging_profile_purpose": purpose, "charging_profile_kind": "Absolute",
            "charging_schedule": {"duration": 60, "start_schedule": datetime.now(timezone.utc).isoformat(), "charging_rate_unit": unit,
                                  "charging_schedule_period": [{"start_period": 0, "limit": limit}]}}


@pytest.mark.parametrize("connector", [0, 1, 2])
@pytest.mark.parametrize("unit", ["W", "A"])
async def test_single_period_profile_applies_only_requested_hardware_targets(profile_core, connector, unit):
    core, calls, _ = profile_core
    assert (await core.on_set_charging_profile(connector_id=connector, cs_charging_profiles=profile(100, unit))).status == "Accepted"
    targets = [1, 2] if connector == 0 else [connector]
    for cid in (1, 2):
        expected = {"W": 90000, "A": 500}
        if cid in targets:
            expected[unit] = 100
        assert core._applied_limits[cid] == expected
    assert {int(call.args[0]) for call in calls["power"].call_args_list} == set(targets)


async def test_clear_profile_resets_both_connectors_to_ratings(profile_core):
    core, calls, _ = profile_core
    await core.on_set_charging_profile(connector_id=0, cs_charging_profiles=profile(10000))
    for mock in calls.values():
        mock.reset_mock()
    assert (await core.on_clear_chargingprofile(connector_id=0)).status == "Accepted"
    assert calls["power"].call_args_list == [mock_call("1", 90000), mock_call("2", 90000)]
    assert calls["current"].call_args_list == [mock_call("1", 500), mock_call("2", 500)]


async def test_composite_station_limit_is_split_when_both_heads_charge(profile_core):
    core, _, _ = profile_core
    for key in ("usiLinkStateCcs1", "iCpStateCss1", "usiLinkStateCcs2", "iCpStateCss2"):
        setattr(core, key, "Charging")
    await core.on_set_charging_profile(connector_id=0, cs_charging_profiles=profile(10000, purpose="ChargePointMaxProfile"))
    response = await core.on_get_compositeSchedul(connector_id=1, duration=60)
    assert response.status == "Accepted"
    assert response.charging_schedule["chargingSchedulePeriod"][0]["limit"] == 5000


async def test_composite_schedule_rejects_unknown_connector(profile_core):
    core, _, _ = profile_core
    assert (await core.on_get_compositeSchedul(connector_id=3, duration=60)).status == "Rejected"


async def test_profile_for_unsupported_connector_is_rejected(profile_core):
    core, calls, _ = profile_core
    assert (await core.on_set_charging_profile(connector_id=3, cs_charging_profiles=profile())).status == "Rejected"
    assert not any(mock.called for mock in calls.values())


async def test_clear_profile_without_filters_clears_all_profiles(profile_core):
    core, calls, runtime = profile_core
    await core.on_set_charging_profile(connector_id=1, cs_charging_profiles=profile(10000))
    await core.on_set_charging_profile(connector_id=2, cs_charging_profiles=profile(50, "A", pid=2))
    for mock in calls.values():
        mock.reset_mock()
    response = await core.on_clear_chargingprofile()
    assert response.status == "Accepted"
    assert calls["power"].call_args_list == [mock_call("1", 90000), mock_call("2", 90000)]
    assert core._applied_limits == {1: {"W": 90000, "A": 500}, 2: {"W": 90000, "A": 500}}
    assert runtime.smart.profiles == []


async def test_composite_schedule_honors_requested_duration(profile_core):
    core, _, _ = profile_core
    response = await core.on_get_compositeSchedul(connector_id=1, duration=60)
    assert response.charging_schedule["duration"] == 60


async def test_current_limit_is_reflected_in_composite_schedule(profile_core):
    core, _, _ = profile_core
    await core.on_set_charging_profile(connector_id=1, cs_charging_profiles=profile(50, "A"))
    response = await core.on_get_compositeSchedul(connector_id=1, duration=60, charging_rate_unit="A")
    assert response.charging_schedule["chargingRateUnit"] == "A"
    assert response.charging_schedule["chargingSchedulePeriod"][0]["limit"] == 50


@pytest.mark.parametrize("failed", ["power", "current"])
async def test_cp_limit_adapter_rejects_plc_failure_strings(failed):
    power = Mock(return_value="limitpower Failed" if failed == "power" else "limitpower ok")
    current = Mock(return_value="limitcurrent Failed" if failed == "current" else "limitcurrent ok")
    async def to_thread(function, *args):
        return function(*args)
    core = extract(methods=("_ocpp_apply_limits",), limitpower=power, limitcurrent=current,
                   asyncio=SimpleNamespace(to_thread=to_thread))["Core"]()
    with pytest.raises(RuntimeError, match="PLC limit publish failed"):
        await core._ocpp_apply_limits(2, {"W": 90000, "A": 50})
    power.assert_called_once_with("2", 90000)
    if failed == "power":
        current.assert_not_called()
    else:
        current.assert_called_once_with("2", 50)


async def test_cp_limit_adapter_checks_each_successful_publish():
    power, current = Mock(return_value="limitpower ok"), Mock(return_value="limitcurrent ok")
    async def to_thread(function, *args):
        return function(*args)
    core = extract(methods=("_ocpp_apply_limits",), limitpower=power, limitcurrent=current,
                   asyncio=SimpleNamespace(to_thread=to_thread))["Core"]()
    await core._ocpp_apply_limits(1, {"W": 20000, "A": 50})
    power.assert_called_once_with("1", 20000)
    current.assert_called_once_with("1", 50)


async def test_cp_limit_fault_stop_preserves_id_before_core_abort_clears_state():
    live = {"transaction_id": 202}
    observed = []
    core = extract(methods=("_ocpp_limit_fault_stop",))["Core"]()
    core.active_id_tag_c2 = "TEST_ONLY_TAG"
    core._ocpp_protocol = lambda: SimpleNamespace(transaction_id=lambda cid: live["transaction_id"])
    async def abort(connector, **kwargs):
        live["transaction_id"] = 0
        observed.append((connector, kwargs))
    core._abort_session_immediately = abort
    await core._ocpp_limit_fault_stop(2)
    assert observed == [(2, {"transaction_id": 202, "id_tag": "TEST_ONLY_TAG"})]


@pytest.mark.parametrize("requested", ["Heartbeat", "BootNotification", "StatusNotification", "MeterValues"])
async def test_trigger_supported_message_dispatches_after_ack(requested):
    tasks = []
    def spawn(coroutine):
        task = asyncio.create_task(coroutine)
        tasks.append(task)
        return task
    env = extract(methods=("on_trigger_message", "_send_triggered_message"),
                  asyncio=SimpleNamespace(create_task=spawn, sleep=asyncio.sleep))
    core = env["Core"]()
    core.call = AsyncMock()
    core.boot_notification_cp = AsyncMock()
    core.status_notification_cp = AsyncMock()
    core.send_meter_values = AsyncMock()
    assert (await core.on_trigger_message(requested_message=requested, connector_id=2)).status == "Accepted"
    await asyncio.gather(*tasks)
    if requested == "Heartbeat":
        assert isinstance(core.call.await_args.args[0], call.Heartbeat)
    elif requested == "BootNotification":
        core.boot_notification_cp.assert_awaited_once()
    elif requested == "StatusNotification":
        core.status_notification_cp.assert_awaited_once_with(2)
    else:
        core.send_meter_values.assert_awaited_once_with(2)


@pytest.mark.parametrize("requested,expected", [(None, "Rejected"), ("UnknownMessage", "NotImplemented"), ("Reset", "NotImplemented")])
async def test_trigger_unknown_message_does_not_schedule_anything(requested, expected):
    create = Mock()
    core = extract(methods=("on_trigger_message",), asyncio=SimpleNamespace(create_task=create))["Core"]()
    assert (await core.on_trigger_message(requested_message=requested)).status == expected
    create.assert_not_called()


@pytest.mark.parametrize("connector,expected", [(1, "Unlocked"), (2, "Unlocked"), (0, "NotSupported"), (3, "NotSupported"), (None, "UnlockFailed")])
async def test_unlock_reports_supported_connector_range_only(connector, expected):
    core = extract(methods=("on_unlock_connector",))["Core"]()
    assert (await core.on_unlock_connector(connector_id=connector)).status == expected


@pytest.mark.parametrize("kwargs,expected", [({}, "Soft"), ({"type": "Hard"}, "Hard")])
async def test_reset_ack_is_returned_before_scheduled_cleanup(kwargs, expected):
    tasks = []
    def spawn(coroutine):
        task = asyncio.create_task(coroutine)
        tasks.append(task)
        return task
    core = extract(methods=("hard_Reset",), asyncio=SimpleNamespace(create_task=spawn))["Core"]()
    core._reset_cleanup_and_reboot = AsyncMock()
    assert (await core.hard_Reset(**kwargs)).status == "Accepted"
    assert core._reset_cleanup_and_reboot.await_count == 0
    await asyncio.gather(*tasks)
    core._reset_cleanup_and_reboot.assert_awaited_once_with(expected)


@pytest.mark.parametrize("kind", ["Soft", "Hard"])
async def test_reset_stops_each_transaction_before_mocked_reboot(tmp_path, monkeypatch, kind):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "translogs1.txt").write_text("101")
    events = []
    async def pulse(ip, level):
        events.append(("pulse", ip, level))
    async def stop(cid, reasonf):
        events.append(("stop", cid, reasonf))
    reboot = Mock(side_effect=lambda command: events.append(("reboot", command)))
    gpio = SimpleNamespace(BCM="BCM", OUT="OUT", setmode=Mock(), setup=Mock(), output=Mock())
    core = extract(methods=("_reset_cleanup_and_reboot",),
                   asyncio=SimpleNamespace(sleep=AsyncMock()),
                   os=SimpleNamespace(path=os.path, system=reboot), GPIO=gpio,
                   time=SimpleNamespace(sleep=Mock()),
                   async_remote_stop_plc=pulse,
                   _write_file=lambda name, value: (tmp_path / name).write_text(str(value)))["Core"]()
    core.transactionId_c1, core.transactionId_c2 = 100, 202
    core.myip_adr_c1, core.myip_adr_c2 = "1", "2"
    core.send_stop_transaction = stop
    core._clear_connector_state = lambda cid: events.append(("clear", cid))
    core.status_notification_usiLink = AsyncMock()
    await core._reset_cleanup_and_reboot(kind)
    reason = "SoftReset" if kind == "Soft" else "HardReset"
    assert ("stop", 1, reason) in events and ("stop", 2, reason) in events
    assert core.transactionId_c1 == 101  # Persisted transaction takes precedence.
    assert events[-1] == ("reboot", "sudo reboot")
    assert events.index(("stop", 2, reason)) < events.index(("reboot", "sudo reboot"))
    assert (tmp_path / "reset_pending.flag").read_text() == kind
    assert core.status_notification_usiLink.await_args_list == [mock_call(1), mock_call(2)]
    if kind == "Hard":
        assert gpio.output.call_args_list == [mock_call(23, 1), mock_call(23, 0)]
    else:
        gpio.output.assert_not_called()
