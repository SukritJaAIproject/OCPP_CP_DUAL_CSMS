"""Protocol contracts for the remaining dual-router OCPP action surfaces.

Every endpoint is a loopback-only JSON CSMS. No charger module is imported.
"""
import asyncio

import pytest
from ocpp.v16 import call

from test_dual_csms_integration import (
    FakeCSMS, FakeCore, configuration, running_manager,
    start_session, stop_request, eventually, now,
)


def profile(remote_id=None):
    value = {"chargingProfileId": 7, "stackLevel": 0,
             "chargingProfilePurpose": "TxProfile" if remote_id is not None else "TxDefaultProfile",
             "chargingProfileKind": "Absolute",
             "chargingSchedule": {"chargingRateUnit": "W", "chargingSchedulePeriod": [
                 {"startPeriod": 0, "limit": 10000}]}}
    if remote_id is not None:
        value["transactionId"] = remote_id
    return value


@pytest.mark.parametrize("server", ["A", "B"])
async def test_read_only_configuration_and_local_list_queries_from_each_server(tmp_path, server):
    async with FakeCSMS() as a, FakeCSMS() as b:
        async with running_manager(configuration(a, b), tmp_path) as (_, core):
            peer = {"A": a, "B": b}[server]
            assert await peer.request("GetConfiguration", {}) == {"configurationKey": []}
            assert await peer.request("GetLocalListVersion", {}) == {"listVersion": 0}
            assert await peer.request("GetCompositeSchedule", {"connectorId": 1, "duration": 60}) == {"status": "Rejected"}
            assert core.mutations == [] and core.starts == [] and core.stops == []


ADMIN_CASES = [
    ("Reset", {"type": "Soft"}, "Rejected"),
    ("ChangeConfiguration", {"key": "MeterValueSampleInterval", "value": "5"}, "Rejected"),
    ("SetChargingProfile", {"connectorId": 1, "csChargingProfiles": profile()}, "Rejected"),
    ("ClearChargingProfile", {}, "Unknown"),
    ("ClearCache", {}, "Rejected"),
    ("SendLocalList", {"listVersion": 1, "updateType": "Full", "localAuthorizationList": []}, "Failed"),
    ("UnlockConnector", {"connectorId": 1}, "UnlockFailed"),
]


@pytest.mark.parametrize("action,payload,denial", ADMIN_CASES)
async def test_admin_commands_have_a_positive_A_path_and_no_B_side_effect(tmp_path, action, payload, denial):
    async with FakeCSMS() as a, FakeCSMS() as b:
        async with running_manager(configuration(a, b), tmp_path) as (_, core):
            assert await b.request(action, payload) == {"status": denial}
            assert core.mutations == []
            result = await a.request(action, payload)
            assert result["status"] in ("Accepted", "Unlocked")
            assert len(core.mutations) == 1
            assert core.mutations[0][0] == action


async def test_tx_profile_scopes_id_to_sender_and_matching_connector(tmp_path):
    async with FakeCSMS(transaction_id=9021) as a, FakeCSMS(transaction_id=9021) as b:
        async with running_manager(configuration(a, b), tmp_path) as (manager, core):
            first = await start_session(manager, core, a, 1, "A-card")
            await start_session(manager, core, b, 2, "B-card")
            assert await a.request("SetChargingProfile", {"connectorId": 1, "csChargingProfiles": profile(9021)}) == {"status": "Accepted"}
            delegated = core.mutations[-1][1]["cs_charging_profiles"]
            assert delegated["transaction_id"] == first.transaction_id
            assert delegated["transaction_id"] != 9021
            mutations = len(core.mutations)
            assert await a.request("SetChargingProfile", {"connectorId": 2, "csChargingProfiles": profile(9021)}) == {"status": "Rejected"}
            assert await a.request("SetChargingProfile", {"connectorId": 1, "csChargingProfiles": profile(999999)}) == {"status": "Rejected"}
            assert await b.request("SetChargingProfile", {"connectorId": 2, "csChargingProfiles": profile(9021)}) == {"status": "Rejected"}
            assert len(core.mutations) == mutations


@pytest.mark.parametrize("requested", ["BootNotification", "Heartbeat", "StatusNotification"])
async def test_trigger_message_sends_the_requested_message_only_to_requester(tmp_path, requested):
    async with FakeCSMS() as a, FakeCSMS() as b:
        async with running_manager(configuration(a, b), tmp_path) as (manager, _):
            if requested == "StatusNotification":
                await manager.call(call.StatusNotification(connector_id=1, error_code="NoError", status="Available"))
                await eventually(lambda: len(a.received(requested)) == 1 and len(b.received(requested)) == 1)
            before_a, before_b = len(a.received(requested)), len(b.received(requested))
            request = {"requestedMessage": requested}
            if requested == "StatusNotification":
                request["connectorId"] = 1
            assert await b.request("TriggerMessage", request) == {"status": "Accepted"}
            await eventually(lambda: len(b.received(requested)) == before_b + 1)
            assert len(a.received(requested)) == before_a


async def test_trigger_meter_values_requires_owner_and_reaches_that_owner(tmp_path):
    async with FakeCSMS() as a, FakeCSMS(transaction_id=9022) as b:
        async with running_manager(configuration(a, b), tmp_path) as (manager, core):
            transaction = await start_session(manager, core, b, 2)

            async def send_meter(connector):
                assert connector == 2
                await manager.call(call.MeterValues(connector_id=2, transaction_id=transaction.transaction_id,
                    meter_value=[{"timestamp": now(), "sampled_value": [{"value": "1020"}]}]))

            core.send_meter_values = send_meter
            request = {"requestedMessage": "MeterValues", "connectorId": 2}
            assert await a.request("TriggerMessage", request) == {"status": "Rejected"}
            assert await b.request("TriggerMessage", request) == {"status": "Accepted"}
            await eventually(lambda: bool(b.received("MeterValues")))
            assert not a.received("MeterValues")
            assert b.received("MeterValues")[0]["transactionId"] == 9022


@pytest.mark.parametrize("requested", ["DiagnosticsStatusNotification", "FirmwareStatusNotification"])
async def test_unsupported_trigger_messages_are_explicitly_not_implemented(tmp_path, requested):
    async with FakeCSMS() as a, FakeCSMS() as b:
        async with running_manager(configuration(a, b), tmp_path) as (_, core):
            assert await a.request("TriggerMessage", {"requestedMessage": requested}) == {"status": "NotImplemented"}
            assert core.mutations == []


async def test_pending_peer_cannot_trigger_or_mutate_hardware(tmp_path):
    async with FakeCSMS() as a, FakeCSMS(boot_status="Pending") as b:
        async with running_manager(configuration(a, b), tmp_path, both_ready=False) as (_, core):
            assert await b.request("TriggerMessage", {"requestedMessage": "Heartbeat"}) == {"status": "Rejected"}
            assert await b.request("RemoteStartTransaction", {"connectorId": 1, "idTag": "pending-card"}) == {"status": "Rejected"}
            assert await b.request("GetConfiguration", {}) == {"configurationKey": []}
            assert core.starts == [] and core.mutations == []


UNSUPPORTED = [
    ("DataTransfer", {"vendorId": "TestVendor", "data": "{}"}),
]


@pytest.mark.parametrize("action,payload", UNSUPPORTED)
async def test_unknown_vendor_data_transfer_has_standard_result_without_side_effect(tmp_path, action, payload):
    async with FakeCSMS() as a, FakeCSMS() as b:
        async with running_manager(configuration(a, b), tmp_path) as (_, core):
            assert (await a.request(action, payload)) == {"status": "UnknownVendorId"}
            assert core.starts == [] and core.stops == [] and core.mutations == []


@pytest.mark.parametrize("status", ["Accepted", "Invalid", "Blocked", "Expired", "ConcurrentTx"])
async def test_local_authorize_asks_B_only_when_A_does_not_accept(tmp_path, status):
    # [R3.1.4] Until R3.1.3 the answer of A was final. Chosen 2026-11-09: A first, then B;
    # the charger-side session belongs to the CSMS that accepted the tag.
    async with FakeCSMS(auth_status=status) as a, FakeCSMS(auth_status="Accepted") as b:
        async with running_manager(configuration(a, b), tmp_path) as (manager, _):
            result = await manager.call(call.Authorize(id_tag="LOCAL-TEST"))
            assert result.id_tag_info["status"] == "Accepted"
            assert len(a.received("Authorize")) == 1
            assert len(b.received("Authorize")) == (0 if status == "Accepted" else 1)
            assert manager.local_owner_for("LOCAL-TEST") == ("A" if status == "Accepted" else "B")


@pytest.mark.parametrize("connector", [0, 3, -1])
async def test_invalid_remote_start_connectors_are_rejected_before_core(tmp_path, connector):
    async with FakeCSMS() as a, FakeCSMS() as b:
        async with running_manager(configuration(a, b), tmp_path) as (manager, core):
            assert await a.request("RemoteStartTransaction", {"connectorId": connector, "idTag": "test"}) == {"status": "Rejected"}
            assert core.starts == []
            assert not manager.snapshot()["sessions"]


async def test_missing_remote_start_connector_is_rejected_and_not_auto_selected(tmp_path):
    async with FakeCSMS() as a, FakeCSMS() as b:
        async with running_manager(configuration(a, b), tmp_path) as (_, core):
            assert await a.request("RemoteStartTransaction", {"idTag": "test"}) == {"status": "Rejected"}
            assert core.starts == []


async def test_unknown_cross_owner_and_closed_transaction_stops_are_rejected(tmp_path):
    async with FakeCSMS(transaction_id=9023) as a, FakeCSMS() as b:
        async with running_manager(configuration(a, b), tmp_path) as (manager, core):
            transaction = await start_session(manager, core, a, 1)
            assert await b.request("RemoteStopTransaction", {"transactionId": 9023}) == {"status": "Rejected"}
            assert await a.request("RemoteStopTransaction", {"transactionId": 999999}) == {"status": "Rejected"}
            assert core.stops == []
            await manager.call(stop_request(transaction.transaction_id))
            await eventually(lambda: manager.snapshot()["sessions"][0]["state"] == "closed")
            assert await a.request("RemoteStopTransaction", {"transactionId": 9023}) == {"status": "Rejected"}
            assert core.stops == []


async def test_trigger_status_for_nonexistent_connector_is_rejected(tmp_path):
    async with FakeCSMS() as a, FakeCSMS() as b:
        async with running_manager(configuration(a, b), tmp_path) as (manager, _):
            await manager.call(call.StatusNotification(connector_id=1, error_code="NoError", status="Available"))
            await eventually(lambda: bool(b.received("StatusNotification")))
            before = len(b.received("StatusNotification"))
            assert await b.request("TriggerMessage", {"requestedMessage": "StatusNotification", "connectorId": 999}) == {"status": "Rejected"}
            assert len(b.received("StatusNotification")) == before


class VariableBootIntervalCSMS(FakeCSMS):
    def __init__(self):
        super().__init__()
        self.boot_interval = 60

    def _result(self, action, payload):
        result = super()._result(action, payload)
        if action == "BootNotification":
            result["interval"] = self.boot_interval
        return result


async def test_triggered_boot_accepted_interval_updates_heartbeat_schedule(tmp_path):
    async with VariableBootIntervalCSMS() as a, VariableBootIntervalCSMS() as b:
        async with running_manager(configuration(a, b), tmp_path) as (_, _core):
            assert not b.received("Heartbeat")
            b.boot_interval = 1
            assert await b.request("TriggerMessage", {"requestedMessage": "BootNotification"}) == {"status": "Accepted"}
            await eventually(lambda: len(b.received("BootNotification")) == 2)
            await eventually(lambda: bool(b.received("Heartbeat")), timeout=2)
            assert not a.received("Heartbeat")
