import asyncio
import json
from pathlib import Path

import pytest
from ocpp.v16 import call
from live_dual_simulator import SimCore, prepare_state, simulator_config
from helpf.dual_csms import DualCSMSManager
from test_dual_csms_integration import FakeCSMS, eventually, now


def make_config(a, b):
    return {"simulator": True, "enabled": True, "local_csms": "A", "admin_csms": "A",
            "request_timeout": .5, "reconnect_seconds": .1,
            "servers": [{"name": "A", "url": a.url + "_A"},
                        {"name": "B", "url": b.url + "_B"}]}


async def test_simulator_routes_actual_remote_start_meter_and_stop(tmp_path):
    async with FakeCSMS(transaction_id=123) as a, FakeCSMS(transaction_id=123) as b:
        directory = prepare_state(tmp_path / "state")
        manager = DualCSMSManager(make_config(a, b), directory)
        core = SimCore(manager, directory, meter_period=.2)
        manager.bind_core(core)
        task = asyncio.create_task(manager.start())
        meter = None
        try:
            await manager.call(call.BootNotification(charge_point_vendor="LocalTest", charge_point_model="Simulator"))
            await asyncio.wait_for(asyncio.gather(*(e.wait() for e in manager.ready.values())), 5)
            core._dual_ready = True
            assert not a.received("StartTransaction") and not b.received("StartTransaction")
            assert (await a.request("RemoteStartTransaction", {"connectorId": 1, "idTag": "A-TEST"}))["status"] == "Accepted"
            await eventually(lambda: core._active_transaction_id(1) > 0)
            assert (await b.request("RemoteStartTransaction", {"connectorId": 1, "idTag": "B-TEST"}))["status"] == "Rejected"
            assert (await b.request("RemoteStartTransaction", {"connectorId": 2, "idTag": "B-TEST"}))["status"] == "Accepted"
            await eventually(lambda: core._active_transaction_id(2) > 0)
            meter = asyncio.create_task(core.meter_loop())
            await eventually(lambda: bool(a.received("MeterValues")) and bool(b.received("MeterValues")))
            assert a.received("MeterValues")[0]["connectorId"] == 1
            assert b.received("MeterValues")[0]["connectorId"] == 2
            assert a.received("MeterValues")[0]["transactionId"] == 123
            assert b.received("MeterValues")[0]["transactionId"] == 123
            assert (await a.request("RemoteStopTransaction", {"transactionId": 123}))["status"] == "Accepted"
            await eventually(lambda: core._active_transaction_id(1) == 0)
            assert core._active_transaction_id(2) > 0
            assert (await b.request("RemoteStopTransaction", {"transactionId": 123}))["status"] == "Accepted"
            await eventually(lambda: core._active_transaction_id(2) == 0)
            await eventually(lambda: bool(a.received("StopTransaction")) and bool(b.received("StopTransaction")))
            assert a.received("StopTransaction")[0]["transactionId"] == 123
            assert b.received("StopTransaction")[0]["transactionId"] == 123
        finally:
            if meter:
                meter.cancel()
                await asyncio.gather(meter, return_exceptions=True)
            await asyncio.gather(*list(core.tasks), return_exceptions=True)
            await manager.close()
            await task


def test_simulator_rejects_production_identity(tmp_path):
    config = {"simulator": True, "enabled": True, "local_csms": "A", "admin_csms": "A",
              "servers": [{"name": "A", "url": "ws://localhost:1/ocpp/CP-A-0001"},
                          {"name": "B", "url": "ws://localhost:2/ocpp/OTHER-TEST"}]}
    path = tmp_path / "config.json"
    path.write_text(json.dumps(config))
    with pytest.raises(ValueError, match="Production"):
        simulator_config(path)


def test_simulator_refuses_nonempty_charger_directory(tmp_path):
    (tmp_path / "translogs1.txt").write_text("0")
    with pytest.raises(ValueError, match="empty"):
        prepare_state(tmp_path)
