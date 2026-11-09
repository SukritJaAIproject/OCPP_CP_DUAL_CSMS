"""OCPP integration checks against two real, loopback-only WebSocket servers.

The small JSON CSMS below deliberately doesn't use the routing implementation's
classes.  Assertions inspect frames observed by each remote server as well as
the calls received by a fake hardware controller.  CP.py is never imported.
"""
import asyncio
import contextlib
import copy
import inspect
import json
import sqlite3
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import pytest
from ocpp.v16 import call, call_result
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

from helpf.dual_csms import (
    DualCSMSError,
    DualCSMSManager,
    StartOutcomeUnknown,
    validate_config,
)


def now():
    return datetime.now(timezone.utc).isoformat()


async def eventually(predicate, timeout=5):
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("Expected condition was not observed before timeout")
        await asyncio.sleep(0.02)


class FakeCSMS:
    """Independent CSMS endpoint that records the actual OCPP wire messages."""

    def __init__(self, *, transaction_id=123, boot_status="Accepted", auth_status="Accepted"):
        self.transaction_id = transaction_id
        self.boot_status = boot_status
        self.auth_status = auth_status
        self.drop_start_response = False
        self.call_errors = {}
        self.online = True
        self.connections = set()
        self.pending = {}
        self.calls = []
        self.server = None
        self.url = None

    async def __aenter__(self):
        self.server = await serve(self._handler, "127.0.0.1", 0, subprotocols=["ocpp1.6"])
        port = self.server.sockets[0].getsockname()[1]
        self.url = f"ws://127.0.0.1:{port}/ocpp/TEST_CP"
        return self

    async def __aexit__(self, *_):
        self.server.close()
        await self.server.wait_closed()
        for future in self.pending.values():
            if not future.done():
                future.cancel()

    async def _handler(self, ws):
        if not self.online:
            await ws.close(code=1013, reason="offline test")
            return
        self.connections.add(ws)
        try:
            async for raw in ws:
                message = json.loads(raw)
                if message[0] == 2:
                    _, uid, action, payload = message
                    self.calls.append((action, copy.deepcopy(payload), uid))
                    if action in self.call_errors:
                        await ws.send(json.dumps([4, uid, self.call_errors[action], "test rejection", {}]))
                        continue
                    result = self._result(action, payload)
                    if result is not None:
                        await ws.send(json.dumps([3, uid, result]))
                elif message[0] in (3, 4):
                    future = self.pending.get(message[1])
                    if future is not None and not future.done():
                        future.set_result(message)
        except ConnectionClosed:
            pass
        finally:
            self.connections.discard(ws)

    def _result(self, action, payload):
        if action == "BootNotification":
            return {"status": self.boot_status, "currentTime": now(), "interval": 1}
        if action == "Heartbeat":
            return {"currentTime": now()}
        if action == "Authorize":
            return {"idTagInfo": {"status": self.auth_status}}
        if action == "StartTransaction":
            if self.drop_start_response:
                return None
            return {"transactionId": self.transaction_id, "idTagInfo": {"status": self.auth_status}}
        if action == "DataTransfer":
            return {"status": "Accepted"}
        return {}

    def received(self, action):
        return [payload for actual, payload, _ in self.calls if actual == action]

    async def request(self, action, payload, *, uid=None, timeout=3):
        await eventually(lambda: bool(self.connections))
        uid = uid or uuid.uuid4().hex
        future = asyncio.get_running_loop().create_future()
        self.pending[uid] = future
        try:
            ws = next(iter(self.connections))
            await ws.send(json.dumps([2, uid, action, payload]))
            response = await asyncio.wait_for(future, timeout)
            if response[0] == 4:
                return {"callError": response[2], "description": response[3]}
            return response[2]
        finally:
            self.pending.pop(uid, None)

    async def go_offline(self):
        self.online = False
        await asyncio.gather(*(ws.close(code=1012, reason="test outage") for ws in list(self.connections)))
        await eventually(lambda: not self.connections)


class FakeCore:
    """Records control requests, and performs no hardware or network actions."""

    def __init__(self):
        self.starts = []
        self.stops = []
        self.mutations = []
        self.pending = set()
        self.active = {}
        self.start_delay = 0
        self.AuthorizeRemoteTxRequests_cfg = "false"
        self._dual_ready = True

    def _dual_connector_busy(self, connector_id):
        return connector_id in self.pending or bool(self.active.get(connector_id))

    def _active_transaction_id(self, connector_id):
        return self.active.get(connector_id, 0)

    async def remote_start_transaction(self, id_tag, connector_id, **kwargs):
        if self.start_delay:
            await asyncio.sleep(self.start_delay)
        self.starts.append((connector_id, id_tag))
        self.pending.add(connector_id)
        return call_result.RemoteStartTransaction(status="Accepted")

    async def remote_stop_transaction(self, transaction_id, **kwargs):
        self.stops.append(transaction_id)
        return call_result.RemoteStopTransaction(status="Accepted")

    async def hard_Reset(self, **kwargs):
        self.mutations.append(("Reset", kwargs))
        return call_result.Reset(status="Accepted")

    async def on_change_configuration(self, **kwargs):
        self.mutations.append(("ChangeConfiguration", kwargs))
        return call_result.ChangeConfiguration(status="Accepted")

    async def on_set_charging_profile(self, **kwargs):
        self.mutations.append(("SetChargingProfile", kwargs))
        return call_result.SetChargingProfile(status="Accepted")

    async def on_clear_chargingprofile(self, **kwargs):
        self.mutations.append(("ClearChargingProfile", kwargs))
        return call_result.ClearChargingProfile(status="Accepted")

    async def on_clear_cache(self, **kwargs):
        self.mutations.append(("ClearCache", kwargs))
        return call_result.ClearCache(status="Accepted")

    async def on_send_local_list(self, **kwargs):
        self.mutations.append(("SendLocalList", kwargs))
        return call_result.SendLocalList(status="Accepted")

    async def on_get_configuration(self, **kwargs):
        return call_result.GetConfiguration(configuration_key=[])

    async def on_get_local_list_version(self, **kwargs):
        return call_result.GetLocalListVersion(list_version=0)

    async def on_get_compositeSchedul(self, **kwargs):
        return call_result.GetCompositeSchedule(status="Rejected")

    async def on_unlock_connector(self, **kwargs):
        self.mutations.append(("UnlockConnector", kwargs))
        return call_result.UnlockConnector(status="Unlocked")

    async def on_trigger_message(self, **kwargs):
        return call_result.TriggerMessage(status="Accepted")


def configuration(a, b):
    return {
        "enabled": True,
        "local_csms": "A",
        "admin_csms": "A",
        "request_timeout": 0.4,
        "reconnect_seconds": 0.1,
        "servers": [
            {"name": "A", "url": a.url, "enabled": True},
            {"name": "B", "url": b.url, "enabled": True},
        ],
    }


@asynccontextmanager
async def running_manager(config, state_dir, *, core=None, both_ready=True, wait_any_ready=True):
    core = core or FakeCore()
    manager = DualCSMSManager(copy.deepcopy(config), str(state_dir))
    manager.bind_core(core)
    task = asyncio.create_task(manager.start())
    try:
        boot = await asyncio.wait_for(manager.call(call.BootNotification(
            charge_point_vendor="TEST", charge_point_model="DUAL_TEST")), timeout=5)
        assert boot.status == "Accepted"
        if both_ready:
            await asyncio.wait_for(asyncio.gather(*(manager.ready[name].wait() for name in ("A", "B"))), timeout=5)
        elif wait_any_ready:
            await eventually(lambda: any(event.is_set() for event in manager.ready.values()))
        yield manager, core
    finally:
        await manager.close()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def start_session(manager, core, csms, connector, tag="test-tag"):
    response = await csms.request("RemoteStartTransaction", {"connectorId": connector, "idTag": tag})
    assert response == {"status": "Accepted"}
    started = await manager.call(call.StartTransaction(
        connector_id=connector, id_tag=tag, meter_start=1000, timestamp=now()))
    core.active[connector] = started.transaction_id
    core.pending.discard(connector)
    return started


def meter_request(connector, transaction):
    return call.MeterValues(connector_id=connector, transaction_id=transaction,
                            meter_value=[{"timestamp": now(), "sampled_value": [{"value": "1010"}]}])


def stop_request(transaction):
    return call.StopTransaction(transaction_id=transaction, meter_stop=1010,
                                timestamp=now(), reason="Local")


@pytest.mark.asyncio
async def test_two_real_connections_boot_and_status_broadcast(tmp_path):
    async with FakeCSMS() as a, FakeCSMS() as b:
        async with running_manager(configuration(a, b), tmp_path) as (manager, _):
            await manager.call(call.StatusNotification(connector_id=1, error_code="NoError", status="Available"))
            await eventually(lambda: a.received("StatusNotification") and b.received("StatusNotification"))
            assert a.received("BootNotification")[0]["chargePointModel"] == "DUAL_TEST"
            assert b.received("BootNotification")[0]["chargePointModel"] == "DUAL_TEST"
            assert a.received("StatusNotification")[-1] == b.received("StatusNotification")[-1]


@pytest.mark.asyncio
async def test_simultaneous_remote_start_claims_before_hardware_await(tmp_path):
    async with FakeCSMS() as a, FakeCSMS() as b:
        core = FakeCore()
        core.start_delay = 0.1
        async with running_manager(configuration(a, b), tmp_path, core=core) as (manager, _):
            results = await asyncio.gather(
                a.request("RemoteStartTransaction", {"connectorId": 1, "idTag": "A-tag"}),
                b.request("RemoteStartTransaction", {"connectorId": 1, "idTag": "B-tag"}))
            assert sorted(item["status"] for item in results) == ["Accepted", "Rejected"]
            assert len(core.starts) == 1


@pytest.mark.asyncio
async def test_duplicate_remote_start_message_is_not_executed_twice(tmp_path):
    async with FakeCSMS() as a, FakeCSMS() as b:
        async with running_manager(configuration(a, b), tmp_path) as (manager, core):
            payload = {"connectorId": 1, "idTag": "A-tag"}
            first = await a.request("RemoteStartTransaction", payload, uid="same-request")
            second = await a.request("RemoteStartTransaction", payload, uid="same-request")
            assert first == second == {"status": "Accepted"}
            assert core.starts == [(1, "A-tag")]


@pytest.mark.asyncio
async def test_same_remote_id_is_scoped_and_transactions_are_not_broadcast(tmp_path):
    async with FakeCSMS(transaction_id=123) as a, FakeCSMS(transaction_id=123) as b:
        async with running_manager(configuration(a, b), tmp_path) as (manager, core):
            first = await start_session(manager, core, a, 1, "A-tag")
            wrong_stop = await b.request("RemoteStopTransaction", {"transactionId": 123})
            assert wrong_stop == {"status": "Rejected"}
            assert core.stops == []
            second = await start_session(manager, core, b, 2, "B-tag")
            assert first.transaction_id != second.transaction_id
            assert first.transaction_id > 0 and second.transaction_id > 0
            await manager.call(meter_request(1, first.transaction_id))
            await manager.call(meter_request(2, second.transaction_id))
            await eventually(lambda: len(a.received("MeterValues")) == 1 and len(b.received("MeterValues")) == 1)
            assert a.received("MeterValues")[0]["transactionId"] == 123
            assert a.received("MeterValues")[0]["connectorId"] == 1
            assert b.received("MeterValues")[0]["transactionId"] == 123
            assert b.received("MeterValues")[0]["connectorId"] == 2
            assert len(a.received("StartTransaction")) == len(b.received("StartTransaction")) == 1
            assert await a.request("RemoteStopTransaction", {"transactionId": 123}) == {"status": "Accepted"}
            assert await b.request("RemoteStopTransaction", {"transactionId": 123}) == {"status": "Accepted"}
            assert core.stops == [first.transaction_id, second.transaction_id]
            await manager.call(stop_request(first.transaction_id))
            await manager.call(stop_request(second.transaction_id))
            await eventually(lambda: len(a.received("StopTransaction")) == 1 and len(b.received("StopTransaction")) == 1)
            assert a.received("StopTransaction")[0]["transactionId"] == 123
            assert b.received("StopTransaction")[0]["transactionId"] == 123


@pytest.mark.asyncio
async def test_non_admin_cannot_reset_configure_or_clear_limits(tmp_path):
    async with FakeCSMS() as a, FakeCSMS() as b:
        async with running_manager(configuration(a, b), tmp_path) as (manager, core):
            for action, payload in [
                ("Reset", {"type": "Soft"}),
                ("ChangeConfiguration", {"key": "HeartbeatInterval", "value": "20"}),
                ("ClearChargingProfile", {}),
                ("ClearCache", {}),
            ]:
                response = await b.request(action, payload)
                assert response.get("status") in ("Rejected", "Unknown") or "callError" in response
            assert core.mutations == []
            assert await a.request("Reset", {"type": "Soft"}) == {"status": "Accepted"}
            assert [action for action, _ in core.mutations] == ["Reset"]


@pytest.mark.asyncio
async def test_remote_start_is_blocked_until_core_initialization_finishes(tmp_path):
    async with FakeCSMS() as a, FakeCSMS() as b:
        core = FakeCore()
        core._dual_ready = False
        async with running_manager(configuration(a, b), tmp_path, core=core) as (manager, _):
            payload = {"connectorId": 1, "idTag": "A-tag"}
            assert await a.request("RemoteStartTransaction", payload) == {"status": "Rejected"}
            assert core.starts == []
            core._dual_ready = True
            assert await a.request("RemoteStartTransaction", payload) == {"status": "Accepted"}


@pytest.mark.asyncio
async def test_secondary_boot_rejection_does_not_reboot_or_block_primary(tmp_path):
    async with FakeCSMS() as a, FakeCSMS(boot_status="Rejected") as b:
        async with running_manager(configuration(a, b), tmp_path, both_ready=False) as (manager, core):
            await eventually(lambda: bool(b.received("BootNotification")))
            assert not manager.ready["B"].is_set()
            result = await start_session(manager, core, a, 1)
            assert result.transaction_id > 0
            assert core.mutations == []
            assert not b.received("StartTransaction")


@pytest.mark.asyncio
async def test_secondary_outage_does_not_block_primary_and_status_replays(tmp_path):
    async with FakeCSMS() as a, FakeCSMS() as b:
        async with running_manager(configuration(a, b), tmp_path) as (manager, core):
            await b.go_offline()
            await eventually(lambda: not manager.ready["B"].is_set())
            await asyncio.wait_for(manager.call(call.StatusNotification(
                connector_id=1, error_code="NoError", status="Preparing")), timeout=0.3)
            started = await start_session(manager, core, a, 1)
            assert started.transaction_id > 0
            b.online = True
            await asyncio.wait_for(manager.ready["B"].wait(), timeout=5)
            await eventually(lambda: any(item["status"] == "Preparing" for item in b.received("StatusNotification")))
            assert b.received("StartTransaction") == []


@pytest.mark.asyncio
async def test_offline_stop_is_durable_across_manager_restart(tmp_path):
    async with FakeCSMS(transaction_id=982) as a, FakeCSMS() as b:
        config = configuration(a, b)
        async with running_manager(config, tmp_path) as (manager, core):
            started = await start_session(manager, core, a, 1)
            await a.go_offline()
            await eventually(lambda: not manager.ready["A"].is_set())
            await asyncio.wait_for(manager.call(stop_request(started.transaction_id)), timeout=0.3)
            assert a.received("StopTransaction") == []
        async with running_manager(config, tmp_path, both_ready=False) as (manager, core):
            a.online = True
            await asyncio.wait_for(manager.ready["A"].wait(), timeout=5)
            await eventually(lambda: len(a.received("StopTransaction")) == 1)
            assert a.received("StopTransaction")[0]["transactionId"] == 982
            assert b.received("StopTransaction") == []


@pytest.mark.asyncio
async def test_permanently_rejected_meter_does_not_block_stop_delivery(tmp_path):
    async with FakeCSMS(transaction_id=99) as a, FakeCSMS() as b:
        a.call_errors["MeterValues"] = "PropertyConstraintViolation"
        async with running_manager(configuration(a, b), tmp_path) as (manager, core):
            started = await start_session(manager, core, a, 1)
            await manager.call(meter_request(1, started.transaction_id))
            await eventually(lambda: bool(a.received("MeterValues")))
            await manager.call(stop_request(started.transaction_id))
            await eventually(lambda: len(a.received("StopTransaction")) == 1, timeout=3)
            assert a.received("StopTransaction")[0]["transactionId"] == 99
            failed = [item for item in manager.snapshot()["outbox"] if item["action"] == "MeterValues"]
            assert len(failed) == 1
            assert failed[0]["state"] == "failed"
            assert failed[0]["last_error"]
            assert failed[0]["attempts"] == 1


@pytest.mark.asyncio
async def test_active_transaction_owner_and_local_id_survive_restart(tmp_path):
    async with FakeCSMS(transaction_id=81) as a, FakeCSMS(transaction_id=81) as b:
        config = configuration(a, b)
        async with running_manager(config, tmp_path) as (manager, core):
            first = await start_session(manager, core, a, 1)
            second = await start_session(manager, core, b, 2)
            active = dict(core.active)
        restored_core = FakeCore()
        restored_core.active = active
        async with running_manager(config, tmp_path, core=restored_core) as (manager, core):
            assert await a.request("RemoteStopTransaction", {"transactionId": 81}) == {"status": "Accepted"}
            assert await b.request("RemoteStopTransaction", {"transactionId": 81}) == {"status": "Accepted"}
            assert core.stops == [first.transaction_id, second.transaction_id]
            assert len(a.received("StartTransaction")) == len(b.received("StartTransaction")) == 1


@pytest.mark.asyncio
async def test_only_clean_pending_reservation_can_be_released(tmp_path):
    async with FakeCSMS() as a, FakeCSMS() as b:
        async with running_manager(configuration(a, b), tmp_path) as (manager, core):
            payload = {"connectorId": 1, "idTag": "A-tag"}
            assert await a.request("RemoteStartTransaction", payload) == {"status": "Accepted"}
            assert await b.request("RemoteStartTransaction", payload) == {"status": "Rejected"}
            # Models an explicit local abort before a StartTransaction was sent.
            core.pending.discard(1)
            released = manager.release_pending(1)
            if inspect.isawaitable(released):
                await released
            assert await b.request("RemoteStartTransaction", payload) == {"status": "Accepted"}


@pytest.mark.asyncio
async def test_lost_start_response_is_not_replayed_even_after_restart(tmp_path):
    async with FakeCSMS() as a, FakeCSMS() as b:
        a.drop_start_response = True
        config = configuration(a, b)
        async with running_manager(config, tmp_path) as (manager, core):
            assert await a.request("RemoteStartTransaction", {"connectorId": 1, "idTag": "tag"}) == {"status": "Accepted"}
            for _ in range(2):
                with pytest.raises(StartOutcomeUnknown):
                    await manager.call(call.StartTransaction(connector_id=1, id_tag="tag", meter_start=1000, timestamp=now()))
            assert len(a.received("StartTransaction")) == 1
            result = manager.release_pending(1)
            if inspect.isawaitable(result):
                await result
            assert await b.request("RemoteStartTransaction", {"connectorId": 1, "idTag": "new"}) == {"status": "Rejected"}
        async with running_manager(config, tmp_path) as (manager, core):
            with pytest.raises(StartOutcomeUnknown):
                await manager.call(call.StartTransaction(connector_id=1, id_tag="tag", meter_start=1000, timestamp=now()))
            assert len(a.received("StartTransaction")) == 1
            assert await b.request("RemoteStartTransaction", {"connectorId": 1, "idTag": "new"}) == {"status": "Rejected"}


@pytest.mark.asyncio
async def test_rejected_start_retains_id_for_compensating_stop(tmp_path):
    async with FakeCSMS(transaction_id=432, auth_status="Invalid") as a, FakeCSMS() as b:
        async with running_manager(configuration(a, b), tmp_path) as (manager, core):
            rejected = await start_session(manager, core, a, 1)
            assert rejected.id_tag_info["status"] == "Invalid"
            await manager.call(stop_request(rejected.transaction_id))
            await eventually(lambda: len(a.received("StopTransaction")) == 1)
            assert a.received("StopTransaction")[0]["transactionId"] == 432
            assert b.received("StopTransaction") == []


@pytest.mark.asyncio
async def test_local_authorize_and_unclaimed_start_use_local_csms(tmp_path):
    async with FakeCSMS() as a, FakeCSMS(auth_status="Invalid") as b:
        async with running_manager(configuration(a, b), tmp_path) as (manager, core):
            result = await manager.call(call.Authorize(id_tag="local-card"))
            assert result.id_tag_info["status"] == "Accepted"
            started = await manager.call(call.StartTransaction(
                connector_id=1, id_tag="local-card", meter_start=100, timestamp=now()))
            assert started.transaction_id > 0
            assert len(a.received("Authorize")) == 1
            assert len(a.received("StartTransaction")) == 1
            assert b.received("Authorize") == []
            assert b.received("StartTransaction") == []


def test_duplicate_server_urls_are_rejected():
    config = {
        "enabled": True, "local_csms": "A", "admin_csms": "A",
        "servers": [{"name": name, "url": "wss://example.test/ocpp/SAME", "enabled": True} for name in ("A", "B")],
    }
    with pytest.raises(ValueError):
        validate_config(config)


@pytest.mark.asyncio
@pytest.mark.parametrize("remote_id", [-1, 0])
async def test_rejected_nonpositive_id_remains_no_transaction(tmp_path, remote_id):
    async with FakeCSMS(transaction_id=remote_id, auth_status="Invalid") as a, FakeCSMS() as b:
        async with running_manager(configuration(a, b), tmp_path) as (manager, core):
            assert await a.request("RemoteStartTransaction", {"connectorId": 1, "idTag": "bad"}) == {"status": "Accepted"}
            result = await manager.call(call.StartTransaction(
                connector_id=1, id_tag="bad", meter_start=0, timestamp=now()))
            assert result.transaction_id == remote_id
            assert result.id_tag_info["status"] == "Invalid"
            assert not manager.connector_reserved(1)
            assert a.received("StopTransaction") == b.received("StopTransaction") == []
            session = manager.snapshot()["sessions"][0]
            assert session["state"] == "cancelled"


@pytest.mark.asyncio
@pytest.mark.parametrize("remote_id", [-1, 0])
async def test_accepted_nonpositive_id_is_blocked_without_replay(tmp_path, remote_id):
    async with FakeCSMS(transaction_id=remote_id) as a, FakeCSMS() as b:
        async with running_manager(configuration(a, b), tmp_path) as (manager, core):
            assert await a.request("RemoteStartTransaction", {"connectorId": 1, "idTag": "tag"}) == {"status": "Accepted"}
            for _ in range(2):
                with pytest.raises(StartOutcomeUnknown):
                    await manager.call(call.StartTransaction(
                        connector_id=1, id_tag="tag", meter_start=0, timestamp=now()))
            assert len(a.received("StartTransaction")) == 1
            assert manager.connector_reserved(1)
            assert manager.snapshot()["sessions"][0]["state"] == "unknown"
            assert not manager.release_pending(1)


@pytest.mark.asyncio
async def test_local_claim_cannot_reuse_existing_local_or_remote_claim(tmp_path):
    async with FakeCSMS() as a, FakeCSMS() as b:
        async with running_manager(configuration(a, b), tmp_path) as (manager, core):
            assert manager.claim_local(1, "same-tag")
            assert not manager.claim_local(1, "same-tag")
            assert not manager.claim_local(1, "different-tag")
            assert await b.request("RemoteStartTransaction", {"connectorId": 1, "idTag": "same-tag"}) == {"status": "Rejected"}
            assert await a.request("RemoteStartTransaction", {"connectorId": 2, "idTag": "same-tag"}) == {"status": "Accepted"}
            assert not manager.claim_local(2, "same-tag")
            assert core.starts == [(2, "same-tag")]
            assert len(manager.snapshot()["sessions"]) == 2


@pytest.mark.asyncio
async def test_eds_nested_ids_route_to_owner_even_after_stop(tmp_path):
    async with FakeCSMS(transaction_id=401) as a, FakeCSMS(transaction_id=902) as b:
        async with running_manager(configuration(a, b), tmp_path) as (manager, core):
            started = await start_session(manager, core, b, 2, "B-tag")
            await manager.call(stop_request(started.transaction_id))
            await eventually(lambda: any(row["state"] == "closed" for row in manager.snapshot()["sessions"]))
            body = {"connectorId": 2, "events": [{"transactionId": started.transaction_id, "detail": "complete"}]}
            payload = call.DataTransfer(vendor_id="EDS", message_id="V2G", data=json.dumps(body))
            result = await manager.call(payload)
            assert result.status == "Accepted"
            assert a.received("DataTransfer") == []
            remote_body = json.loads(b.received("DataTransfer")[0]["data"])
            assert remote_body == {"connectorId": 2, "events": [{"transactionId": 902, "detail": "complete"}]}
            # The controller's immutable payload is still in its local namespace.
            assert json.loads(payload.data) == body


@pytest.mark.asyncio
async def test_eds_mixed_owner_payload_is_not_sent_to_either_csms(tmp_path):
    async with FakeCSMS(transaction_id=41) as a, FakeCSMS(transaction_id=82) as b:
        async with running_manager(configuration(a, b), tmp_path) as (manager, core):
            first = await start_session(manager, core, a, 1, "A-tag")
            second = await start_session(manager, core, b, 2, "B-tag")
            with pytest.raises(DualCSMSError, match="different CSMS"):
                await manager.call(call.DataTransfer(vendor_id="EDS", data=json.dumps([
                    {"transactionId": first.transaction_id}, {"transactionId": second.transaction_id}])))
            assert a.received("DataTransfer") == b.received("DataTransfer") == []


@pytest.mark.asyncio
async def test_other_vendor_payload_is_opaque_and_routes_to_local_csms(tmp_path):
    async with FakeCSMS() as a, FakeCSMS() as b:
        async with running_manager(configuration(a, b), tmp_path) as (manager, core):
            data = '{"transactionId":"vendor-defined-id"}'
            await manager.call(call.DataTransfer(vendor_id="OtherVendor", data=data))
            assert a.received("DataTransfer")[0]["data"] == data
            assert b.received("DataTransfer") == []


@pytest.mark.asyncio
async def test_shutdown_cancels_shielded_start_before_closing_state(tmp_path):
    async with FakeCSMS() as a, FakeCSMS() as b:
        a.drop_start_response = True
        config = configuration(a, b)
        config["request_timeout"] = 10
        async with running_manager(config, tmp_path) as (manager, core):
            assert await a.request("RemoteStartTransaction", {"connectorId": 1, "idTag": "tag"}) == {"status": "Accepted"}
            caller = asyncio.create_task(manager.call(call.StartTransaction(
                connector_id=1, id_tag="tag", meter_start=0, timestamp=now())))
            await eventually(lambda: bool(a.received("StartTransaction")))
            start_workers = list(manager._start_tasks.values())
            assert len(start_workers) == 1
            caller.cancel()
            with pytest.raises(asyncio.CancelledError):
                await caller
            assert not start_workers[0].done()
            await asyncio.wait_for(manager.close(), timeout=2)
            assert all(worker.done() for worker in start_workers)
        with sqlite3.connect(tmp_path / "dual_csms_state.sqlite3") as db:
            assert db.execute("SELECT state FROM sessions").fetchall() == [("unknown",)]
        assert len(a.received("StartTransaction")) == 1


@pytest.mark.asyncio
async def test_active_mapping_without_core_transaction_fails_startup(tmp_path):
    async with FakeCSMS() as a, FakeCSMS() as b:
        config = configuration(a, b)
        async with running_manager(config, tmp_path) as (manager, core):
            await start_session(manager, core, a, 1)
        restarted = DualCSMSManager(config, str(tmp_path))
        try:
            with pytest.raises(DualCSMSError, match="reconcile before startup"):
                restarted.bind_core(FakeCore())
        finally:
            await restarted.close()
        assert len(a.received("StartTransaction")) == 1


@pytest.mark.asyncio
async def test_both_endpoints_offline_do_not_block_local_boot_and_status_storage(tmp_path):
    async with FakeCSMS() as a, FakeCSMS() as b:
        a.online = b.online = False
        config = configuration(a, b)
        async with running_manager(config, tmp_path, both_ready=False, wait_any_ready=False) as (manager, core):
            assert not any(event.is_set() for event in manager.ready.values())
            await asyncio.wait_for(manager.call(call.StatusNotification(
                connector_id=1, error_code="NoError", status="Faulted")), timeout=0.3)
            assert a.received("StatusNotification") == b.received("StatusNotification") == []
        async with running_manager(config, tmp_path, both_ready=False, wait_any_ready=False) as (manager, core):
            b.online = True
            await asyncio.wait_for(manager.ready["B"].wait(), timeout=5)
            await eventually(lambda: bool(b.received("StatusNotification")))
            assert b.received("StatusNotification")[-1]["status"] == "Faulted"
            assert not manager.ready["A"].is_set()


@pytest.mark.asyncio
async def test_completed_command_id_can_be_reused_in_new_connection(tmp_path):
    async with FakeCSMS() as a, FakeCSMS() as b:
        async with running_manager(configuration(a, b), tmp_path) as (manager, core):
            assert await a.request("RemoteStartTransaction", {
                "connectorId": 1, "idTag": "old-tag"}, uid="counter-1") == {"status": "Accepted"}
            assert manager.release_pending(1)
            core.pending.clear()
            await a.go_offline()
            await eventually(lambda: not manager.ready["A"].is_set())
            a.online = True
            await asyncio.wait_for(manager.ready["A"].wait(), timeout=5)
            assert await a.request("RemoteStartTransaction", {
                "connectorId": 1, "idTag": "new-tag"}, uid="counter-1") == {"status": "Accepted"}
            assert core.starts == [(1, "old-tag"), (1, "new-tag")]


@pytest.mark.asyncio
async def test_rejected_stop_remains_failed_evidence_not_remote_success(tmp_path):
    async with FakeCSMS() as a, FakeCSMS() as b:
        a.call_errors["StopTransaction"] = "PropertyConstraintViolation"
        async with running_manager(configuration(a, b), tmp_path) as (manager, core):
            started = await start_session(manager, core, a, 1)
            await manager.call(stop_request(started.transaction_id))
            await eventually(lambda: any(row["state"] == "failed" for row in manager.snapshot()["outbox"]))
            record = manager.snapshot()["outbox"][0]
            assert record["action"] == "StopTransaction"
            assert record["state"] == "failed"
            assert record["attempts"] == 1
            assert record["last_error"]
            assert manager.snapshot()["sessions"][0]["state"] != "closed"
            assert b.received("StopTransaction") == []
