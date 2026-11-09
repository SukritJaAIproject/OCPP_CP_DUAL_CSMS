"""R3's actual protocol composition on loopback sockets and HTTP.

SimCore(full_protocol=True) binds the reviewed actual CP config/auth methods;
the production runtime, service and profile engines are used unchanged. No
hardware CP constructor or hardware/OS integration is imported or executed.
"""
import asyncio
import contextlib
import hashlib
import io
import json
import zipfile
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from ocpp.v16 import call

from helpf.dual_csms import DualCSMSManager
from live_dual_simulator import SimCore, prepare_state
from test_dual_csms_integration import FakeCSMS, configuration, eventually, now
from test_ocpp_services import HTTPFixture


class ProtocolCSMS(FakeCSMS):
    """Plain JSON CSMS; captures all seven new actions through real messages."""
    def __init__(self, *args, parent=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.parent = parent

    def _result(self, action, payload):
        if action == "Authorize":
            info = {"status": self.auth_status}
            if self.parent:
                info["parentIdTag"] = self.parent
            return {"idTagInfo": info}
        return super()._result(action, payload)


@asynccontextmanager
async def running_full(tmp_path, *, a_parent=None, b_parent=None):
    async with ProtocolCSMS(transaction_id=1101, parent=a_parent) as a, ProtocolCSMS(transaction_id=2202, parent=b_parent) as b, HTTPFixture() as http:
        directory = prepare_state(tmp_path / "simulator")
        configuration_file = {"transfer": {"allowed_hosts": ["127.0.0.1"], "allow_http_hosts": ["127.0.0.1"], "timeout": 2},
                              "diagnostic_sources": ["ocpp_runtime_events.jsonl"],
                              "trusted_firmware_sha256": {http.base + "/firmware.bin": hashlib.sha256(http.data).hexdigest()}}
        (directory / "ocpp_services_config.json").write_text(json.dumps(configuration_file), encoding="utf-8")
        config = configuration(a, b)
        config["request_timeout"] = 2
        manager = DualCSMSManager(config, directory)
        core = SimCore(manager, directory, .2, full_protocol=True)
        manager.bind_core(core)
        runtime = core._ocpp_protocol()
        await runtime.bootstrap()
        manager_task = asyncio.create_task(manager.start())
        protocol_task = asyncio.create_task(runtime.run())
        meter_task = asyncio.create_task(core.meter_loop())
        try:
            await core.call(call.BootNotification(charge_point_vendor="R3_TEST", charge_point_model="SIMULATED"))
            await asyncio.wait_for(asyncio.gather(*(event.wait() for event in manager.ready.values())), 5)
            core._dual_ready = True
            for cid in (0, 1, 2):
                await core.status(cid, "Available")
            await eventually(lambda: len(a.received("StatusNotification")) >= 3 and len(b.received("StatusNotification")) >= 3)
            yield SimpleNamespace(core=core, manager=manager, runtime=runtime, a=a, b=b, http=http, directory=directory)
        finally:
            for cid in (1, 2):
                if core._active_transaction_id(cid) > 0:
                    with contextlib.suppress(Exception):
                        await core.stop_transaction(cid, "Local")
            with contextlib.suppress(Exception):
                await eventually(lambda: manager.db.execute("SELECT count(*) FROM outbox WHERE state!='delivered'").fetchone()[0] == 0, 3)
            await runtime.close()
            for task in (protocol_task, meter_task):
                task.cancel()
            await asyncio.gather(protocol_task, meter_task, return_exceptions=True)
            if core.tasks:
                await asyncio.gather(*tuple(core.tasks), return_exceptions=True)
            await manager.close()
            await asyncio.gather(manager_task, return_exceptions=True)


async def start(peer, core, connector, tag):
    assert await peer.request("RemoteStartTransaction", {"connectorId": connector, "idTag": tag}) == {"status": "Accepted"}
    await eventually(lambda: core._active_transaction_id(connector) > 0)


async def stop(peer, core, connector):
    assert await peer.request("RemoteStopTransaction", {"transactionId": peer.transaction_id}) == {"status": "Accepted"}
    await eventually(lambda: core._active_transaction_id(connector) == 0 and connector not in core.stopping)


def amp_profile(connector=1, limit=50):
    return {"connectorId": connector, "csChargingProfiles": {
        "chargingProfileId": 17, "stackLevel": 0,
        "chargingProfilePurpose": "TxDefaultProfile", "chargingProfileKind": "Absolute",
        "chargingSchedule": {"startSchedule": now(), "duration": 3600, "chargingRateUnit": "A",
            "chargingSchedulePeriod": [{"startPeriod": 0, "limit": limit}]}}}


async def test_full_stack_availability_admin_and_scheduled_transaction_boundary(tmp_path):
    async with running_full(tmp_path) as env:
        assert await env.b.request("ChangeAvailability", {"connectorId": 1, "type": "Inoperative"}) == {"status": "Rejected"}
        assert env.runtime.services.can_start(1, "A", "CARD")
        await start(env.b, env.core, 2, "B_CARD")
        assert await env.a.request("ChangeAvailability", {"connectorId": 2, "type": "Inoperative"}) == {"status": "Scheduled"}
        assert env.core.heads["2"]["status"] == "Charging"
        await stop(env.b, env.core, 2)
        await eventually(lambda: env.runtime.services.state["availability"]["2"] == "Inoperative")
        await eventually(lambda: any(p["connectorId"] == 2 and p["status"] == "Unavailable" for p in env.a.received("StatusNotification")))
        assert await env.b.request("RemoteStartTransaction", {"connectorId": 2, "idTag": "B_CARD"}) == {"status": "Rejected"}
        assert await env.a.request("ChangeAvailability", {"connectorId": 2, "type": "Operative"}) == {"status": "Accepted"}
        await eventually(lambda: env.runtime.services.can_start(2, "B", "B_CARD"))


async def test_reservation_ids_are_owner_scoped_and_reach_start_transaction(tmp_path):
    async with running_full(tmp_path) as env:
        expiry = (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()
        request_a = {"connectorId": 1, "idTag": "A_CARD", "reservationId": 71, "expiryDate": expiry}
        request_b = {"connectorId": 2, "idTag": "B_CARD", "reservationId": 71, "expiryDate": expiry}
        assert await env.a.request("ReserveNow", request_a) == {"status": "Accepted"}
        assert await env.b.request("ReserveNow", request_b) == {"status": "Accepted"}
        assert await env.b.request("CancelReservation", {"reservationId": 71}) == {"status": "Accepted"}
        assert await env.b.request("CancelReservation", {"reservationId": 71}) == {"status": "Rejected"}
        assert env.runtime.services.reservation_id_for(1, "A", "A_CARD") == 71
        assert await env.b.request("ReserveNow", request_b) == {"status": "Accepted"}
        assert await env.a.request("RemoteStartTransaction", {"connectorId": 1, "idTag": "WRONG"}) == {"status": "Rejected"}
        assert await env.a.request("RemoteStartTransaction", {"connectorId": 2, "idTag": "B_CARD"}) == {"status": "Rejected"}
        await start(env.a, env.core, 1, "A_CARD")
        await start(env.b, env.core, 2, "B_CARD")
        assert env.a.received("StartTransaction")[-1]["reservationId"] == 71
        assert env.b.received("StartTransaction")[-1]["reservationId"] == 71
        assert env.runtime.services.state["reservations"] == []


async def test_diagnostics_real_http_upload_origin_statuses_and_B_denial(tmp_path):
    async with running_full(tmp_path) as env:
        destination = env.http.base + "/upload/t/private-token/"
        assert await env.b.request("GetDiagnostics", {"location": destination}) == {}
        assert not env.http.uploads
        result = await env.a.request("GetDiagnostics", {"location": destination})
        assert result["fileName"].endswith(".zip")
        await eventually(lambda: any(p["status"] == "Uploaded" for p in env.a.received("DiagnosticsStatusNotification")))
        assert [p["status"] for p in env.a.received("DiagnosticsStatusNotification")] == ["Uploading", "Uploaded"]
        assert env.b.received("DiagnosticsStatusNotification") == []
        archive = zipfile.ZipFile(io.BytesIO(env.http.uploads[0][2]))
        assert archive.testzip() is None
        assert json.loads(archive.read("summary.json"))["redacted"] is True
        assert await env.b.request("TriggerMessage", {"requestedMessage": "DiagnosticsStatusNotification"}) == {"status": "Accepted"}
        await eventually(lambda: env.b.received("DiagnosticsStatusNotification") == [{"status": "Idle"}])


async def test_firmware_real_http_staging_and_progress_never_installation(tmp_path):
    async with running_full(tmp_path) as env:
        request = {"location": env.http.base + "/firmware.bin", "retrieveDate": now()}
        assert (await env.b.request("UpdateFirmware", request))["callError"] == "SecurityError"
        assert not env.runtime.services.state["jobs"]
        assert await env.a.request("UpdateFirmware", request) == {}
        await eventually(lambda: any(p["status"] == "Downloaded" for p in env.a.received("FirmwareStatusNotification")))
        assert env.b.received("FirmwareStatusNotification") == []
        statuses = [p["status"] for p in env.a.received("FirmwareStatusNotification")]
        assert statuses == ["Downloading", "Downloaded"]
        row = next(j for j in env.runtime.services.state["jobs"].values() if j["kind"] == "firmware")
        assert row["installed"] is False and row["trusted_integrity_verified"] is True
        assert (env.directory / "firmware_staging" / row["staged_name"]).read_bytes() == env.http.data
        assert await env.b.request("TriggerMessage", {"requestedMessage": "FirmwareStatusNotification"}) == {"status": "Accepted"}
        await eventually(lambda: env.b.received("FirmwareStatusNotification") == [{"status": "Idle"}])


async def test_service_progress_waits_for_origin_reconnect_without_cross_fanout(tmp_path):
    async with running_full(tmp_path) as env:
        await env.b.go_offline()
        await eventually(lambda: not env.manager.ready["B"].is_set())
        await env.runtime.services.send_current_status("B", "DiagnosticsStatusNotification")
        await env.runtime.services.send_current_status("B", "FirmwareStatusNotification")
        await env.runtime.services.flush_notifications()
        assert len(env.runtime.services.state["notifications"]) == 2
        assert env.a.received("DiagnosticsStatusNotification") == []
        assert env.a.received("FirmwareStatusNotification") == []
        assert await env.a.request("GetLocalListVersion", {}) == {"listVersion": 0}
        env.b.online = True
        await eventually(lambda: env.manager.ready["B"].is_set())
        await eventually(lambda: not env.runtime.services.state["notifications"])
        assert env.b.received("DiagnosticsStatusNotification") == [{"status": "Idle"}]
        assert env.b.received("FirmwareStatusNotification") == [{"status": "Idle"}]


async def test_smart_50A_schedule_and_clear_use_actual_runtime_adapter(tmp_path):
    async with running_full(tmp_path) as env:
        assert await env.b.request("SetChargingProfile", amp_profile()) == {"status": "Rejected"}
        assert await env.a.request("SetChargingProfile", amp_profile(3)) == {"status": "Rejected"}
        assert await env.a.request("SetChargingProfile", amp_profile()) == {"status": "Accepted"}
        result = await env.b.request("GetCompositeSchedule", {"connectorId": 1, "duration": 60, "chargingRateUnit": "A"})
        assert result["status"] == "Accepted"
        assert result["chargingSchedule"]["duration"] == 60
        assert result["chargingSchedule"]["chargingSchedulePeriod"] == [{"startPeriod": 0, "limit": 50}]
        assert env.core._sim_limits[1]["A"] == 50
        assert await env.a.request("ClearChargingProfile", {}) == {"status": "Accepted"}
        assert env.runtime.smart.profiles == []
        assert env.core._sim_limits[1]["A"] == 500


async def test_actual_CP_configuration_and_list_contracts_over_two_sockets(tmp_path):
    async with running_full(tmp_path) as env:
        for peer in (env.a, env.b):
            result = await peer.request("GetConfiguration", {"key": ["LocalAuthListMaxLength", "NumberOfConnectors"]})
            assert {item["key"] for item in result["configurationKey"]} == {"LocalAuthListMaxLength", "NumberOfConnectors"}
        original = env.core.LocalAuthListMaxLength_cfg
        assert await env.a.request("ChangeConfiguration", {"key": "LocalAuthListMaxLength", "value": "1"}) == {"status": "Rejected"}
        assert env.core.LocalAuthListMaxLength_cfg == original
        for key, value in [("HeartbeatInterval", "abc"), ("HeartbeatInterval", "-1"), ("MeterValueSampleInterval", "-1")]:
            assert await env.a.request("ChangeConfiguration", {"key": key, "value": value}) == {"status": "Rejected"}
        assert await env.a.request("ChangeConfiguration", {"key": "UnknownTestKey", "value": "1"}) == {"status": "NotSupported"}
        assert await env.b.request("ChangeConfiguration", {"key": "HeartbeatInterval", "value": "5"}) == {"status": "Rejected"}
        full = {"listVersion": 1, "updateType": "Full", "localAuthorizationList": [{"idTag": "ONE", "idTagInfo": {"status": "Accepted"}}]}
        assert await env.b.request("SendLocalList", full) == {"status": "Failed"}
        assert await env.a.request("SendLocalList", full) == {"status": "Accepted"}
        env.core.local_auth_list.max_length = 1
        failed = {"listVersion": 2, "updateType": "Differential", "localAuthorizationList": [
            {"idTag": "ONE", "idTagInfo": {"status": "Blocked"}}, {"idTag": "TWO", "idTagInfo": {"status": "Accepted"}}]}
        before = (env.directory / "local_auth_list.json").read_bytes()
        assert await env.a.request("SendLocalList", failed) == {"status": "Failed"}
        assert env.core.local_auth_list.identifiers == {"ONE": {"status": "Accepted"}}
        assert (env.directory / "local_auth_list.json").read_bytes() == before
        assert await env.b.request("GetLocalListVersion", {}) == {"listVersion": 1}


async def test_uncached_group_reservation_authorization_does_not_deadlock_reader(tmp_path):
    async with running_full(tmp_path, b_parent="FAMILY_B") as env:
        expiry = (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()
        assert await env.b.request("ReserveNow", {"connectorId": 2, "idTag": "RESERVED", "parentIdTag": "FAMILY_B", "reservationId": 75, "expiryDate": expiry}) == {"status": "Accepted"}
        await start(env.b, env.core, 2, "CHILD")
        assert env.b.received("Authorize")[-1]["idTag"] == "CHILD"
        assert env.b.received("StartTransaction")[-1]["reservationId"] == 75


async def test_same_child_tag_parent_resolution_is_scoped_to_owner(tmp_path):
    async with running_full(tmp_path, a_parent="FAMILY_A", b_parent="FAMILY_B") as env:
        expiry = (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()
        for peer, cid, family, rid in [(env.a, 1, "FAMILY_A", 81), (env.b, 2, "FAMILY_B", 82)]:
            assert await peer.request("ReserveNow", {"connectorId": cid, "idTag": "RESERVED", "parentIdTag": family, "reservationId": rid, "expiryDate": expiry}) == {"status": "Accepted"}
        # Hold A after its parent lookup so its cached resolution remains live
        # while B resolves the identical child tag against B's distinct parent.
        env.a.drop_start_response = True
        assert await env.a.request("RemoteStartTransaction", {"connectorId": 1, "idTag": "CHILD"}) == {"status": "Accepted"}
        await eventually(lambda: bool(env.a.received("StartTransaction")))
        await start(env.b, env.core, 2, "CHILD")
        assert env.b.received("Authorize") == [{"idTag": "CHILD"}]
        assert env.b.received("StartTransaction")[-1]["reservationId"] == 82
        uid = next(uid for action, _, uid in env.a.calls if action == "StartTransaction")
        await next(iter(env.a.connections)).send(json.dumps([3, uid, {"transactionId": env.a.transaction_id, "idTagInfo": {"status": "Accepted"}}]))
        await eventually(lambda: env.core._active_transaction_id(1) > 0)
        assert env.a.received("StartTransaction")[-1]["reservationId"] == 81
