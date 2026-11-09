"""R3.1.4: a charger-side idTag is authorized by A first, then by B (chosen 2026-11-09).

The session belongs to the CSMS that accepted the tag, so StartTransaction, MeterValues
and StopTransaction go there. The tag itself is never logged or stored by this mapping.
"""
import logging

import pytest
from ocpp.v16 import call

from helpf.dual_csms import BackendUnavailable, DualCSMSManager
from test_dual_csms_integration import FakeCSMS, configuration, now, running_manager

TAG = "CARD-OF-B-77"


def owner_rows(manager):
    return [tuple(r) for r in manager.db.execute("SELECT owner FROM auth_owners").fetchall()]


@pytest.mark.asyncio
async def test_card_known_only_to_b_charges_with_b(tmp_path, caplog):
    caplog.set_level(logging.INFO)
    async with FakeCSMS(auth_status="Invalid") as a, FakeCSMS(transaction_id=555) as b:
        async with running_manager(configuration(a, b), tmp_path) as (manager, core):
            result = await manager.call(call.Authorize(id_tag=TAG))
            assert result.id_tag_info["status"] == "Accepted"
            assert len(a.received("Authorize")) == 1 and len(b.received("Authorize")) == 1
            assert manager.local_owner_for(TAG) == "B"
            assert manager.claim_local(1, TAG)
            started = await manager.call(call.StartTransaction(connector_id=1, id_tag=TAG, meter_start=10, timestamp=now()))
            assert started.id_tag_info["status"] == "Accepted"
            assert a.received("StartTransaction") == [] and len(b.received("StartTransaction")) == 1
            await manager.call(call.MeterValues(connector_id=1, transaction_id=started.transaction_id,
                                                meter_value=[{"timestamp": now(), "sampledValue": [{"value": "11"}]}]))
            await manager.call(call.StopTransaction(transaction_id=started.transaction_id, id_tag=TAG,
                                                    meter_stop=20, timestamp=now(), reason="Local"))
            await b_receives(b, "StopTransaction")
            assert a.received("MeterValues") == [] and a.received("StopTransaction") == []
            assert b.received("StopTransaction")[0]["transactionId"] == 555
            # B owns it: A cannot stop it remotely, B could (checked by the existing owner rules)
    assert all(TAG not in record.getMessage() for record in caplog.records if record.name == "dual_csms")
    assert any("[B] Authorize Accepted; the charger-side session belongs to B" in r.getMessage() for r in caplog.records)


async def b_receives(csms, action):
    from test_dual_csms_integration import eventually
    await eventually(lambda: bool(csms.received(action)))


@pytest.mark.asyncio
async def test_card_accepted_by_a_never_asks_b(tmp_path, caplog):
    caplog.set_level(logging.INFO)
    async with FakeCSMS() as a, FakeCSMS() as b:
        async with running_manager(configuration(a, b), tmp_path) as (manager, core):
            assert (await manager.call(call.Authorize(id_tag=TAG))).id_tag_info["status"] == "Accepted"
            assert b.received("Authorize") == []
            assert manager.local_owner_for(TAG) == "A"
    # every tap tells which CSMS accepted it, without the tag
    assert any("[A] Authorize Accepted; the charger-side session belongs to A" in r.getMessage() for r in caplog.records)
    assert all(TAG not in record.getMessage() for record in caplog.records if record.name == "dual_csms")


@pytest.mark.asyncio
async def test_card_rejected_by_both_returns_the_answer_of_a(tmp_path):
    async with FakeCSMS(auth_status="Blocked") as a, FakeCSMS(auth_status="Invalid") as b:
        async with running_manager(configuration(a, b), tmp_path) as (manager, core):
            result = await manager.call(call.Authorize(id_tag=TAG))
            assert result.id_tag_info["status"] == "Blocked"
            assert len(b.received("Authorize")) == 1
            assert owner_rows(manager) == []
            assert manager.local_owner_for(TAG) == "A"


@pytest.mark.asyncio
async def test_a_offline_falls_through_to_b(tmp_path):
    async with FakeCSMS() as a, FakeCSMS(transaction_id=9) as b:
        async with running_manager(configuration(a, b), tmp_path) as (manager, core):
            await a.go_offline()
            from test_dual_csms_integration import eventually
            await eventually(lambda: not manager.ready["A"].is_set())
            result = await manager.call(call.Authorize(id_tag=TAG))
            assert result.id_tag_info["status"] == "Accepted"
            assert manager.local_owner_for(TAG) == "B"


@pytest.mark.asyncio
async def test_claim_made_before_authorize_follows_the_accepting_csms(tmp_path):
    async with FakeCSMS(auth_status="Invalid") as a, FakeCSMS(transaction_id=77) as b:
        async with running_manager(configuration(a, b), tmp_path) as (manager, core):
            assert manager.claim_local(2, TAG)                 # the RFID command path claims first
            assert manager._live(2)["owner"] == "A"
            await manager.call(call.Authorize(id_tag=TAG))
            assert manager._live(2)["owner"] == "B"
            started = await manager.call(call.StartTransaction(connector_id=2, id_tag=TAG, meter_start=1, timestamp=now()))
            assert started.id_tag_info["status"] == "Accepted"
            assert len(b.received("StartTransaction")) == 1 and a.received("StartTransaction") == []


@pytest.mark.asyncio
async def test_cached_card_without_authorize_keeps_its_owner_after_restart(tmp_path):
    async with FakeCSMS(auth_status="Invalid") as a, FakeCSMS() as b:
        async with running_manager(configuration(a, b), tmp_path) as (manager, core):
            await manager.call(call.Authorize(id_tag=TAG))
        # a new process: the controller's cache skips Authorize, the owner comes from the state file
        async with running_manager(configuration(a, b), tmp_path) as (manager, core):
            assert manager.local_owner_for(TAG) == "B"
            started = await manager.call(call.StartTransaction(connector_id=1, id_tag=TAG, meter_start=1, timestamp=now()))
            assert started.id_tag_info["status"] == "Accepted"
            assert len(b.received("StartTransaction")) == 1 and a.received("StartTransaction") == []


@pytest.mark.asyncio
async def test_both_offline_raises_like_before(tmp_path):
    manager = DualCSMSManager({"enabled": True, "local_csms": "A", "admin_csms": "A", "servers": [
        {"name": "A", "url": "ws://127.0.0.1:1/ocpp/x"}, {"name": "B", "url": "ws://127.0.0.1:2/ocpp/y"}]}, tmp_path)
    try:
        with pytest.raises(BackendUnavailable):
            await manager._authorize(call.Authorize(id_tag=TAG))
    finally:
        await manager.close()


@pytest.mark.asyncio
async def test_cp_rfid_command_start_with_a_card_of_b(tmp_path):
    """Real CP.py _dual_rfid_command_start + authorize against the real router."""
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    from test_cp_dual_hooks import extracted

    pulses, gates = [], []

    async def pulse(cid, level):
        pulses.append((cid, level))

    async def no_sleep(seconds):
        return None

    async def gate(cid, owner, tag):
        gates.append((cid, owner))
        return True

    async with FakeCSMS(auth_status="Invalid") as a, FakeCSMS(transaction_id=4321) as b:
        async with running_manager(configuration(a, b), tmp_path) as (manager, _):
            core = extracted("_dual_rfid_command_start", "authorize",
                             asyncio=SimpleNamespace(sleep=no_sleep),
                             plc=SimpleNamespace(get_connector_status=lambda cid: ("Preparing", "Available")),
                             async_remote_start_plc=pulse, _write_file=lambda *args: None)
            core._dual_manager = manager
            core.call = manager.call
            core.allow_rfid_command_start = True
            core._valid_id_tag = lambda tag: bool(tag)
            core._dual_local_claims = set()
            core._dual_connector_busy = lambda cid: False
            core._active_transaction_id = lambda cid: 0
            core._clear_stale_stop_latch_for_new_start = lambda *args: None
            core._abort_session_immediately = AsyncMock()
            core._ocpp_start_allowed = gate
            core.auth_timestamps = {}
            core.AuthorizationCacheEnabled_cfg = "false"

            assert await core._dual_rfid_command_start(1, TAG) is True
            assert gates == [(1, "A")]                   # first time: the tag is not known yet
            assert manager._live(1)["owner"] == "B"      # B accepted, the claim follows B
            assert pulses == [("1", 1), ("1", 0)]
            started = await manager.call(call.StartTransaction(connector_id=1, id_tag=TAG, meter_start=5, timestamp=now()))
            assert started.id_tag_info["status"] == "Accepted"
            assert len(b.received("StartTransaction")) == 1 and a.received("StartTransaction") == []
            assert manager._live(1)["remote_id"] == 4321

            # the next tap of the same card on head 2 is gated for B straight away
            assert await core._dual_rfid_command_start(2, TAG) is True
            assert gates[-1] == (2, "B") and manager._live(2)["owner"] == "B"


@pytest.mark.asyncio
async def test_cp_rfid_command_start_refused_by_both_releases_the_claim(tmp_path):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    from test_cp_dual_hooks import extracted

    pulses = []

    async def pulse(cid, level):
        pulses.append((cid, level))

    async with FakeCSMS(auth_status="Invalid") as a, FakeCSMS(auth_status="Invalid") as b:
        async with running_manager(configuration(a, b), tmp_path) as (manager, _):
            core = extracted("_dual_rfid_command_start", "authorize",
                             plc=SimpleNamespace(get_connector_status=lambda cid: ("Preparing", "Available")),
                             async_remote_start_plc=pulse, _write_file=lambda *args: None)
            core._dual_manager = manager
            core.call = manager.call
            core.allow_rfid_command_start = True
            core._valid_id_tag = lambda tag: bool(tag)
            core._dual_local_claims = set()
            core._dual_connector_busy = lambda cid: False
            core._active_transaction_id = lambda cid: 0
            core._abort_session_immediately = AsyncMock()
            core.auth_timestamps = {}
            core.AuthorizationCacheEnabled_cfg = "false"
            assert await core._dual_rfid_command_start(1, TAG) is False
            assert manager._live(1) is None and pulses == []
            assert len(a.received("Authorize")) == 1 and len(b.received("Authorize")) == 1


def test_owner_map_stores_a_hash_and_is_bounded(tmp_path):
    manager = DualCSMSManager({"enabled": True, "local_csms": "A", "admin_csms": "A", "servers": [
        {"name": "A", "url": "ws://127.0.0.1:1/ocpp/x"}, {"name": "B", "url": "ws://127.0.0.1:2/ocpp/y"}]}, tmp_path)
    manager._AUTH_OWNER_LIMIT = 3
    for number in range(5):
        manager._remember_auth_owner("tag-%d" % number, "B")
    rows = manager.db.execute("SELECT tag, owner FROM auth_owners").fetchall()
    assert len(rows) == 3
    assert all(len(row["tag"]) == 64 and not row["tag"].startswith("tag-") for row in rows)
    assert manager.local_owner_for("tag-4") == "B" and manager.local_owner_for("tag-0") == "A"
    manager.db.close()
