import asyncio
import json

from ocpp.v16 import call
from helpf.dual_csms import DualCSMSManager
from live_dual_simulator import SimCore, prepare_state
from tools.simulator_test_controls import execute_control, protocol_trace, redact
from test_dual_csms_integration import FakeCSMS, eventually


def test_trace_removes_upload_credentials_and_nested_vendor_tags():
    result = redact({"location": "https://test.invalid/upload/t/PRIVATE_UPLOAD_KEY/",
                     "data": json.dumps({"idTag": "PRIVATE_CARD", "status": "Accepted"})})
    serialized = json.dumps(result)
    assert "PRIVATE_UPLOAD_KEY" not in serialized
    assert "PRIVATE_CARD" not in serialized
    assert json.loads(result["data"])["status"] == "Accepted"
    assert redact({"key": "AutoChargeStopPin", "value": "123456"})["value"] == "[redacted]"


async def test_wire_journal_and_local_controls_use_real_test_sockets(tmp_path):
    async with FakeCSMS() as a, FakeCSMS(auth_status="Blocked") as b:
        config = {"enabled": True, "local_csms": "A", "admin_csms": "A", "request_timeout": .5,
                  "reconnect_seconds": .1, "servers": [{"name": "A", "url": a.url}, {"name": "B", "url": b.url}]}
        trace = tmp_path / "wire.jsonl"
        with protocol_trace(trace):
            directory = prepare_state(tmp_path / "state")
            manager = DualCSMSManager(config, directory)
            core = SimCore(manager, directory, .2)
            manager.bind_core(core)
            task = asyncio.create_task(manager.start())
            try:
                await manager.call(call.BootNotification(charge_point_vendor="TEST", charge_point_model="TEST"))
                await asyncio.wait_for(asyncio.gather(*(event.wait() for event in manager.ready.values())), 3)
                accepted = await execute_control(core, {"action": "authorize", "server": "A", "id_tag": "PRIVATE_TEST_TAG"})
                rejected = await execute_control(core, {"action": "authorize", "server": "B", "id_tag": "PRIVATE_TEST_TAG"})
                assert accepted["response"]["id_tag_info"]["status"] == "Accepted"
                assert rejected["response"]["id_tag_info"]["status"] == "Blocked"
                await execute_control(core, {"action": "status", "connector": 1, "status": "Faulted", "error_code": "OtherError"})
                await eventually(lambda: any(item.get("status") == "Faulted" for item in a.received("StatusNotification")))
                assert (await execute_control(core, {"action": "query"}))["heads"]["1"]["status"] == "Faulted"
                await execute_control(core, {"action": "status", "connector": 1, "status": "Available"})
            finally:
                await manager.close()
                await task
        records = [json.loads(line) for line in trace.read_text().splitlines()]
        assert "PRIVATE_TEST_TAG" not in trace.read_text()
        assert any(r["action"] == "Authorize" and r["message_type"] == 3
                   and r["payload"]["idTagInfo"]["status"] == "Accepted" for r in records)
        assert any(r["action"] == "Authorize" and r["message_type"] == 3
                   and r["payload"]["idTagInfo"]["status"] == "Blocked" for r in records)
