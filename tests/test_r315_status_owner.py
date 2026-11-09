"""R3.1.5: connector statuses name the CSMS that owns the session (chosen 2026-11-09, option 1).

Both CSMS already receive every StatusNotification. While a connector charges, `info`
says "Session of CSMS B (tx 555)" so either CSMS's log, and the charger log, tell whose
session it was. No second transaction, no MeterValues to the other CSMS, no idTag.
"""
import asyncio

import pytest
from ocpp.v16 import call

from test_dual_csms_integration import (FakeCSMS, configuration, eventually, now, running_manager,
                                        start_session, stop_request)


def status(connector, state, **extra):
    return call.StatusNotification(connector_id=connector, error_code="NoError", status=state,
                                   timestamp=now(), **extra)


async def last_status(csms, connector):
    await eventually(lambda: any(item["connectorId"] == connector for item in csms.received("StatusNotification")))
    return [item for item in csms.received("StatusNotification") if item["connectorId"] == connector][-1]


async def both_have(a, b, connector, state):
    await eventually(lambda: all(any(item["connectorId"] == connector and item["status"] == state
                                     for item in csms.received("StatusNotification")) for csms in (a, b)))


@pytest.mark.asyncio
async def test_charging_status_names_the_owner_on_both_csms(tmp_path):
    async with FakeCSMS(transaction_id=101) as a, FakeCSMS(transaction_id=555) as b:
        async with running_manager(configuration(a, b), tmp_path) as (manager, core):
            await start_session(manager, core, b, 1, tag="CARD-OF-B")
            await manager.call(status(1, "Charging"))
            await both_have(a, b, 1, "Charging")
            for csms in (a, b):
                item = await last_status(csms, 1)
                assert item["status"] == "Charging"
                assert item["info"] == "Session of CSMS B (tx 555)"
                assert "CARD" not in str(item)
            # the other head is idle: nothing is added
            await manager.call(status(2, "Available"))
            await both_have(a, b, 2, "Available")
            assert "info" not in await last_status(a, 2)


@pytest.mark.asyncio
async def test_session_of_a_is_named_on_b_as_well(tmp_path):
    async with FakeCSMS(transaction_id=77) as a, FakeCSMS() as b:
        async with running_manager(configuration(a, b), tmp_path) as (manager, core):
            await start_session(manager, core, a, 2)
            await manager.call(status(2, "SuspendedEV"))
            await both_have(a, b, 2, "SuspendedEV")
            assert (await last_status(b, 2))["info"] == "Session of CSMS A (tx 77)"
            assert b.received("StartTransaction") == [] and b.received("MeterValues") == []


@pytest.mark.asyncio
async def test_preparing_available_and_faulted_are_untouched(tmp_path):
    async with FakeCSMS() as a, FakeCSMS() as b:
        async with running_manager(configuration(a, b), tmp_path) as (manager, core):
            await start_session(manager, core, a, 1)
            for state in ("Preparing", "Faulted", "Available"):
                await manager.call(status(1, state))
                await both_have(a, b, 1, state)
                assert "info" not in await last_status(b, 1)


@pytest.mark.asyncio
async def test_charger_info_text_is_kept(tmp_path):
    async with FakeCSMS() as a, FakeCSMS() as b:
        async with running_manager(configuration(a, b), tmp_path) as (manager, core):
            await start_session(manager, core, a, 1)
            await manager.call(status(1, "Charging", info="Issue F71202 cleared."))
            await both_have(a, b, 1, "Charging")
            assert (await last_status(b, 1))["info"] == "Issue F71202 cleared."


@pytest.mark.asyncio
async def test_charging_without_any_csms_session_has_no_label(tmp_path):
    # an HMI status-only session: the CSMS see Charging but no transaction exists
    async with FakeCSMS() as a, FakeCSMS() as b:
        async with running_manager(configuration(a, b), tmp_path) as (manager, core):
            await manager.call(status(1, "Charging"))
            await both_have(a, b, 1, "Charging")
            assert "info" not in await last_status(a, 1)


@pytest.mark.asyncio
async def test_finishing_right_after_the_stop_names_the_ended_session(tmp_path):
    async with FakeCSMS() as a, FakeCSMS(transaction_id=909) as b:
        async with running_manager(configuration(a, b), tmp_path) as (manager, core):
            started = await start_session(manager, core, b, 1)
            await manager.call(stop_request(started.transaction_id))
            await eventually(lambda: bool(b.received("StopTransaction")))
            core.active.pop(1, None)
            await manager.call(status(1, "Finishing"))
            await both_have(a, b, 1, "Finishing")
            assert (await last_status(a, 1))["info"] == "Session of CSMS B (tx 909)"
            # the cable is pulled: Available carries nothing
            await manager.call(status(1, "Available"))
            await both_have(a, b, 1, "Available")
            assert "info" not in await last_status(a, 1)


@pytest.mark.asyncio
async def test_finishing_long_after_the_stop_is_not_labelled(tmp_path):
    async with FakeCSMS() as a, FakeCSMS(transaction_id=909) as b:
        async with running_manager(configuration(a, b), tmp_path) as (manager, core):
            started = await start_session(manager, core, b, 1)
            await manager.call(stop_request(started.transaction_id))
            await eventually(lambda: bool(b.received("StopTransaction")))
            core.active.pop(1, None)
            manager._FINISHING_LABEL_SECONDS = 0      # the stop was "long ago"
            await asyncio.sleep(0.01)
            await manager.call(status(1, "Finishing"))
            await both_have(a, b, 1, "Finishing")
            assert "info" not in await last_status(a, 1)


@pytest.mark.asyncio
async def test_finishing_after_restart_with_only_old_sessions_is_not_labelled(tmp_path):
    async with FakeCSMS() as a, FakeCSMS(transaction_id=909) as b:
        async with running_manager(configuration(a, b), tmp_path) as (manager, core):
            started = await start_session(manager, core, b, 1)
            await manager.call(stop_request(started.transaction_id))
            await eventually(lambda: bool(b.received("StopTransaction")))
    async with FakeCSMS() as a, FakeCSMS() as b:
        async with running_manager(configuration(a, b), tmp_path) as (manager, core):
            # an RFID attempt that never reached StartTransaction ends with Finishing
            await manager.call(status(1, "Finishing"))
            await both_have(a, b, 1, "Finishing")
            assert "info" not in await last_status(a, 1)


@pytest.mark.asyncio
async def test_label_survives_reconnect_and_trigger_message_replay(tmp_path):
    async with FakeCSMS(transaction_id=31) as a, FakeCSMS() as b:
        async with running_manager(configuration(a, b), tmp_path) as (manager, core):
            await start_session(manager, core, a, 1)
            await manager.call(status(1, "Charging"))
            await both_have(a, b, 1, "Charging")
            b.calls.clear()
            assert await b.request("TriggerMessage", {"requestedMessage": "StatusNotification", "connectorId": 1}) == {"status": "Accepted"}
            assert (await last_status(b, 1))["info"] == "Session of CSMS A (tx 31)"


@pytest.mark.asyncio
async def test_remote_start_reserved_head_is_labelled_without_transaction_id(tmp_path):
    async with FakeCSMS() as a, FakeCSMS() as b:
        async with running_manager(configuration(a, b), tmp_path) as (manager, core):
            assert await b.request("RemoteStartTransaction", {"connectorId": 2, "idTag": "card-b"}) == {"status": "Accepted"}
            # the legacy flow can report Charging before StartTransaction is answered
            await manager.call(status(2, "Charging"))
            await both_have(a, b, 2, "Charging")
            assert (await last_status(a, 2))["info"] == "Session of CSMS B"


def test_label_fits_the_ocpp_info_field_for_long_csms_names(tmp_path):
    from helpf.dual_csms import DualCSMSManager
    name = "LONG-operator-name-that-does-not-fit-the-info-field"
    manager = DualCSMSManager({"enabled": True, "local_csms": "A", "admin_csms": "A", "servers": [
        {"name": "A", "url": "ws://127.0.0.1:1/ocpp/x"}, {"name": name, "url": "ws://127.0.0.1:2/ocpp/y"}]}, tmp_path)
    try:
        row = manager._reserve(1, name, "card")
        manager.db.execute("UPDATE sessions SET state='active', remote_id=1234567 WHERE local_id=?", (row["local_id"],))
        label = manager.session_label(1, "Charging")
        assert len(label) <= 50 and label.startswith("Session of CSMS LONG-")
    finally:
        manager.db.close()
