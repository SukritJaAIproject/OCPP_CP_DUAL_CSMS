"""R3.1 CP Max contracts through the real runtime and loopback OCPP sockets.

The profile below is the command produced by the verified CSMS set_profile
HTTP action. No real CSMS endpoint or hardware-facing CP.py is contacted.
"""
import asyncio
import copy
import json
from unittest.mock import Mock

import pytest

from helpf.dual_csms import DualCSMSManager
from live_dual_simulator import SimCore
from tools.csms_api_client import CSMSAPI, SERVERS
from test_dual_csms_integration import eventually
from test_r3_protocol_integration import running_full, start, stop


# Verified HTTP parameter names are different from the OCPP wire names. The
# server creates a Relative, constant, indefinite profile for this action.
API_PARAMS = {"connector_id": 0, "limit": 100000,
              "purpose": "ChargePointMaxProfile", "stack_level": 0, "rate_unit": "W"}
WIRE_PAYLOAD = {"connectorId": 0, "csChargingProfiles": {
    "chargingProfileId": 1794140000, "stackLevel": 0,
    "chargingProfilePurpose": "ChargePointMaxProfile", "chargingProfileKind": "Relative",
    "chargingSchedule": {"chargingRateUnit": "W",
        "chargingSchedulePeriod": [{"startPeriod": 0, "limit": 100000}]}}}


def adapter_events(directory):
    return [record for line in (directory / "simulator-events.jsonl").read_text().splitlines()
            if (record := json.loads(line))["event"] == "simulated_limit_command"]


def assert_cap_for_every_write(initial, events):
    """Replay the real adapter log to inspect intermediate two-head budgets."""
    limits = copy.deepcopy(initial)
    for row in events:
        limits[row["connector"]] = row["limits"]
        assert sum(limits[cid]["W"] for cid in (1, 2)) <= 100000, row


@pytest.mark.parametrize("server", ["A", "B"])
def test_http_set_profile_client_preserves_verified_params_and_test_identity(monkeypatch, server):
    # _request is replaced before any API operation; even authentication is not
    # attempted. This checks the actual client serialization boundary only.
    api = CSMSAPI(server)
    request = Mock(return_value={"status": "queued"})
    monkeypatch.setattr(api, "_request", request)
    assert api.command("set_profile", API_PARAMS) == {"status": "queued"}
    request.assert_called_once_with("/api/command", {
        "cp_id": SERVERS[server]["charger_id"], "command": "set_profile", "params": API_PARAMS})


@pytest.mark.parametrize("first_stop", [1, 2])
async def test_runtime_cpmax_caps_A1_B2_then_independent_stops(tmp_path, first_stop):
    async with running_full(tmp_path) as env:
        assert await env.a.request("SetChargingProfile", WIRE_PAYLOAD) == {"status": "Accepted"}
        assert env.core._sim_limits[1]["W"] == env.core._sim_limits[2]["W"] == 50000
        # Initial installation lowers the old 180 kW rating before acknowledging
        # the new cap. Audit all subsequent writes from the confirmed 100 kW
        # state, including transient changes while sessions start and stop.
        initial = copy.deepcopy(env.core._sim_limits)
        event_start = len(adapter_events(env.directory))
        assert await env.b.request("SetChargingProfile", WIRE_PAYLOAD) == {"status": "Rejected"}
        await asyncio.gather(start(env.a, env.core, 1, "A_CARD"),
                             start(env.b, env.core, 2, "B_CARD"))
        await eventually(lambda: all(env.core.heads[str(cid)]["status"] == "Charging" for cid in (1, 2)))
        assert env.core._sim_limits[1]["W"] == env.core._sim_limits[2]["W"] == 50000
        assert [p["connectorId"] for p in env.a.received("StartTransaction")] == [1]
        assert [p["connectorId"] for p in env.b.received("StartTransaction")] == [2]
        for peer in (env.a, env.b):
            result = await peer.request("GetCompositeSchedule", {"connectorId": 0, "duration": 60,
                                                                  "chargingRateUnit": "W"})
            assert result["status"] == "Accepted"
            assert result["chargingSchedule"]["chargingSchedulePeriod"] == [{"startPeriod": 0, "limit": 100000}]

        peer_for = {1: env.a, 2: env.b}
        remaining = 3 - first_stop
        await stop(peer_for[first_stop], env.core, first_stop)
        await eventually(lambda: env.core._sim_limits[first_stop]["W"] == 0 and
                                  env.core._sim_limits[remaining]["W"] == 100000)
        assert env.core.heads[str(remaining)]["status"] == "Charging"
        assert env.core._active_transaction_id(remaining) > 0
        assert not peer_for[remaining].received("StopTransaction")
        await stop(peer_for[remaining], env.core, remaining)
        await eventually(lambda: env.core._sim_limits[1]["W"] == env.core._sim_limits[2]["W"] == 50000)
        assert len(env.a.received("StopTransaction")) == len(env.b.received("StopTransaction")) == 1
        assert_cap_for_every_write(initial, adapter_events(env.directory)[event_start:])
        assert not env.runtime.smart.inhibited
        assert not env.runtime.faulted


async def test_simcore_full_protocol_restart_reestablishes_persisted_cpmax(tmp_path):
    async with running_full(tmp_path) as env:
        assert await env.a.request("SetChargingProfile", WIRE_PAYLOAD) == {"status": "Accepted"}
        await asyncio.gather(start(env.a, env.core, 1, "A_CARD"),
                             start(env.b, env.core, 2, "B_CARD"))
        await stop(env.a, env.core, 1)
        await stop(env.b, env.core, 2)
        await eventually(lambda: env.core._sim_limits[1]["W"] == env.core._sim_limits[2]["W"] == 50000)
        profile_file = env.directory / "ocpp_charging_profiles.json"
        before = profile_file.read_bytes()
        directory = env.directory

    # The closed original manager releases its DB. A fresh SimCore and runtime
    # then load the same state; no manager.start() or external socket is used.
    config = {"enabled": True, "local_csms": "A", "admin_csms": "A", "servers": [
        {"name": "A", "url": "ws://127.0.0.1:1/ocpp/TEST_A"},
        {"name": "B", "url": "ws://127.0.0.1:2/ocpp/TEST_B"}]}
    manager = DualCSMSManager(config, directory)
    runtime = None
    try:
        core = SimCore(manager, directory, .2, full_protocol=True)
        manager.bind_core(core)
        runtime = core._ocpp_protocol()
        event_start = len(adapter_events(directory))
        await runtime.bootstrap()
        assert runtime.ready and not runtime.smart.inhibited
        assert core._sim_limits[1]["W"] == core._sim_limits[2]["W"] == 50000
        assert profile_file.read_bytes() == before
        assert len(runtime.smart.profiles) == 1
        result = await runtime.composite(connector_id=0, duration=60, charging_rate_unit="W")
        assert result.status == "Accepted"
        assert result.charging_schedule["chargingSchedulePeriod"] == [{"startPeriod": 0, "limit": 100000}]
        assert_cap_for_every_write({1: {"W": 0}, 2: {"W": 0}}, adapter_events(directory)[event_start:])
    finally:
        if runtime:
            await runtime.close()
        await manager.close()
