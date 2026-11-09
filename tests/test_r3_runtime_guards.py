"""Upgrade and final start-barrier contracts, without physical hardware."""
import ast
import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
from unittest.mock import AsyncMock

import pytest

from helpf.dual_csms import DualCSMSManager
from live_dual_simulator import SimCore, prepare_state
from test_cp_ocpp_functions import extract
from test_dual_csms_integration import eventually
from test_r3_protocol_integration import running_full, start


def offline_config():
    return {"enabled": True, "local_csms": "A", "admin_csms": "A",
            "servers": [{"name": "A", "url": "ws://127.0.0.1:1/ocpp/TEST_A"},
                        {"name": "B", "url": "ws://127.0.0.1:2/ocpp/TEST_B"}]}


async def test_legacy_shared_amp_limit_is_imported_as_one_conservative_snapshot(tmp_path):
    directory = prepare_state(tmp_path)
    (directory / "charging_rate_unit.txt").write_text("A")
    (directory / "limit.txt").write_text("50")
    manager = DualCSMSManager(offline_config(), directory)
    try:
        core = SimCore(manager, directory, 1, full_protocol=True)
        manager.bind_core(core)
        await core._ocpp_protocol().bootstrap()
        commands = [json.loads(line) for line in (directory / "simulator-events.jsonl").read_text().splitlines()
                    if json.loads(line)["event"] == "simulated_limit_command"]
        assert commands and all(row["limits"]["A"] <= 50 for row in commands)
        assert core._sim_limits[1]["A"] == core._sim_limits[2]["A"] == 50
        assert len(core._ocpp_protocol().smart.profiles) == 2
        await core._ocpp_protocol().close()
    finally:
        await manager.close()


def test_corrupt_legacy_limit_does_not_become_an_unlimited_profile(tmp_path):
    directory = prepare_state(tmp_path)
    (directory / "charging_rate_unit.txt").write_text("A")
    (directory / "limit.txt").write_text("not-a-limit")
    manager = DualCSMSManager(offline_config(), directory)
    try:
        with pytest.raises(ValueError):
            SimCore(manager, directory, 1, full_protocol=True)
        assert not (directory / "ocpp_charging_profiles.json").exists()
    finally:
        manager.db.close()


async def test_inoperative_cancels_an_already_accepted_but_unarmed_start(tmp_path):
    async with running_full(tmp_path) as env:
        release = asyncio.Event()
        original = env.core.start_transaction

        async def hold_before_hardware(connector):
            await release.wait()
            await original(connector)

        env.core.start_transaction = hold_before_hardware
        try:
            assert await env.b.request("RemoteStartTransaction", {"connectorId": 2, "idTag": "B_CARD"}) == {"status": "Accepted"}
            assert env.core.heads["2"]["pending"]
            assert await env.a.request("ChangeAvailability", {"connectorId": 2, "type": "Inoperative"}) == {"status": "Scheduled"}
            await eventually(lambda: not env.core.heads["2"]["pending"])
            release.set()
            await eventually(lambda: env.runtime.services.state["availability"]["2"] == "Inoperative")
            assert not env.b.received("StartTransaction")
            assert env.manager._live(2) is None
        finally:
            release.set()


async def test_triggered_meter_has_trigger_context_without_changing_periodic_samples(tmp_path):
    async with running_full(tmp_path) as env:
        await start(env.b, env.core, 2, "B_CARD")
        assert await env.b.request("TriggerMessage", {"requestedMessage": "MeterValues", "connectorId": 2}) == {"status": "Accepted"}
        await eventually(lambda: any(m["meterValue"][0]["sampledValue"][0].get("context") == "Trigger" for m in env.b.received("MeterValues")))
        assert not env.a.received("MeterValues")


def test_start_pulse_barrier_blocks_one_but_still_allows_zero_reset():
    source = Path(__file__).resolve().parents[1] / "helpf/plc_function.py"
    tree = ast.parse(source.read_text(encoding="utf-8"))
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "remote_start_plc_W")
    plc = SimpleNamespace(remote_start=Mock(return_value=True))
    env = {"_cp_instance": SimpleNamespace(_ocpp_hardware_start_allowed=lambda cid: False), "_get_plc": lambda: plc}
    exec(compile(ast.fix_missing_locations(ast.Module(body=[node], type_ignores=[])), str(source), "exec"), env)
    assert "Failed" in env["remote_start_plc_W"]("1", 1)
    plc.remote_start.assert_not_called()
    assert "Successful" in env["remote_start_plc_W"]("1", 0)
    plc.remote_start.assert_called_once_with("1", 0)


def test_actual_startup_integer_reader_repairs_old_invalid_files(tmp_path):
    env = extract(definitions=("_read_cfg_text", "_read_safe_integer_cfg", "_write_configuration_file"))
    path = tmp_path / "HeartbeatInterval_cfg.txt"
    path.write_text("abc")
    assert env["_read_safe_integer_cfg"](path, 30, 1) == "30"
    assert path.read_text() == "30"


def test_actual_meter_current_offered_respects_a_profile_even_with_voltage_known():
    env = extract(methods=("_build_meter_item_list",),
                  _normalize_csv_string=lambda value, default="0": value,
                  _parse_csv_list=lambda value: value.split(","))
    core = env["Core"]()
    core.MeterValuesSampledData_cfg = "Current.Offered"
    core._corrected_power_kw = lambda value: value
    rows = core._build_meter_item_list({"voltage": 400}, None, 90000, 400, 5, current_limit=50)
    assert next(row for row in rows if row["measurand"] == "Current.Offered")["value"] == "50"


@pytest.mark.parametrize("message", ["DiagnosticsStatusNotification", "FirmwareStatusNotification"])
async def test_single_CSMS_CP_trigger_routes_new_status_after_ack(message):
    tasks = []
    def spawn(coroutine):
        task = asyncio.create_task(coroutine)
        tasks.append(task)
        return task
    env = extract(methods=("on_trigger_message", "_send_triggered_message"),
                  asyncio=SimpleNamespace(create_task=spawn, sleep=asyncio.sleep))
    core = env["Core"]()
    core._ocpp_trigger_service_status = AsyncMock()
    assert (await core.on_trigger_message(requested_message=message)).status == "Accepted"
    await asyncio.gather(*tasks)
    core._ocpp_trigger_service_status.assert_awaited_once_with("A", message)


async def test_every_advertised_readonly_configuration_rejects_writes(tmp_path):
    async with running_full(tmp_path) as env:
        values = await env.a.request("GetConfiguration", {})
        rows = values["configurationKey"]
        required = {"ChargeProfileMaxStackLevel", "ChargingScheduleAllowedChargingRateUnit",
                    "ChargingScheduleMaxPeriods", "MaxChargingProfilesInstalled",
                    "ReserveConnectorZeroSupported", "FirmwareUpdateMode"}
        assert required <= {row["key"] for row in rows}
        for row in rows:
            if row["readonly"]:
                assert await env.a.request("ChangeConfiguration", {"key": row["key"], "value": "4"}) == {"status": "Rejected"}
        after = await env.a.request("GetConfiguration", {})
        assert after == values


async def test_limit_failure_after_upstream_start_is_stopped_without_false_charging(tmp_path):
    async with running_full(tmp_path) as env:
        original = env.core._ocpp_apply_limits
        async def failing_adapter(connector, limits):
            if connector == 2 and limits["W"] > 90000:
                return False
            return await original(connector, limits)
        env.core._ocpp_apply_limits = failing_adapter
        assert await env.b.request("RemoteStartTransaction", {"connectorId": 2, "idTag": "B_CARD"}) == {"status": "Accepted"}
        await eventually(lambda: bool(env.b.received("StopTransaction")))
        await eventually(lambda: env.manager.db.execute("SELECT state FROM sessions ORDER BY local_id DESC LIMIT 1").fetchone()[0] == "closed")
        assert len(env.b.received("StartTransaction")) == len(env.b.received("StopTransaction")) == 1
        assert env.core._active_transaction_id(2) == 0
        assert 2 in env.runtime.faulted
        assert not any(row["connectorId"] == 2 and row["status"] == "Charging" for row in env.b.received("StatusNotification"))


async def test_production_adapter_clears_legacy_sharing_only_after_both_heads_zero():
    writes = []
    def writer(kind):
        def record(cid, value):
            writes.append((kind, cid, value))
            return kind + " ok"
        return record
    env = extract(methods=("_ocpp_apply_limits",), limitpower=writer("power"),
                  limitcurrent=writer("current"), limit2connector=writer("sharing"))
    core = env["Core"]()
    await core._ocpp_apply_limits(1, {"W": 0, "A": 0})
    assert not any(row[0] == "sharing" for row in writes)
    await core._ocpp_apply_limits(2, {"W": 0, "A": 0})
    assert writes == [("power", "1", 0), ("current", "1", 0), ("power", "2", 0),
                      ("current", "2", 0), ("sharing", "1", 0), ("sharing", "2", 0)]
    await core._ocpp_apply_limits(1, {"W": 90000, "A": 500})
    assert [row for row in writes if row[0] == "sharing"] == [("sharing", "1", 0), ("sharing", "2", 0)]
