"""R3.1.7: a card that its CSMS accepted starts the session while the EV waits in SuspendedEV.

Found on Edgebox 192.168.3.15 on 2026-11-10: the EV was plugged in at 14:17:49 and reached CP
state C (CableCheck, which the PLC reports as SuspendedEV) at 14:17:51. The card was tapped at
14:17:54, CSMS A answered Accepted - and nothing started. The EV gave up at 14:18:54. The same
happened at 14:19:18. The card path only started from Available or Preparing and returned
without a log line; RemoteStart had been fixed for this state in R3.1.2.

Also pinned here: a card tapped with no vehicle connected does not pulse the PLC (at 14:10:19 the
PLC kept such a pulse as a charging permission for 5 minutes with no session behind it), and every
card that does not start names its reason in the log, without the idTag.
"""
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from ocpp.v16 import call

from test_dual_csms_integration import FakeCSMS, configuration, now, running_manager
from test_r316_start_latch import GUARD, Files, cp_class, idle_flags

TAG = "SECRET-CARD-317"     # <= 20 chars (CiString20)


class Claims:
    def __init__(self):
        self.claimed, self.released, self.free = [], [], True

    def local_owner_for(self, tag):
        return "A"

    def claim_local(self, cid, tag):
        if not self.free:
            return False
        self.claimed.append(cid)
        return True

    def release_pending(self, cid):
        self.released.append(cid)
        return True


def card_core(states, auth="Accepted", names=("_dual_rfid_command_start",), manager=None, **env):
    """states: the PLC states read one after another (the last one repeats)."""
    seen, pulses, files = list(states), [], Files()

    def get_connector_status(cid):
        state = seen.pop(0) if len(seen) > 1 else seen[0]
        return state, state

    async def pulse(cid, level):
        pulses.append((cid, level))

    core = cp_class(names, _write_file=files, async_remote_start_plc=pulse,
                    asyncio=SimpleNamespace(sleep=AsyncMock()),
                    plc=SimpleNamespace(get_connector_status=get_connector_status), **env)()
    idle_flags(core)
    core.files, core.pulses = files, pulses
    core._dual_manager = manager if manager is not None else Claims()
    core.allow_rfid_command_start = True
    core._dual_local_claims = set()
    core.busy = ""
    core._dual_connector_busy = lambda cid: bool(core.busy)
    core._dual_busy_reason = lambda cid: core.busy
    core._valid_id_tag = lambda tag: bool(tag)
    core._active_transaction_id = lambda cid: core.transactionId_c1
    core._clear_stale_stop_latch_for_new_start = lambda *args: None
    core._abort_session_immediately = AsyncMock()
    core.authorize = AsyncMock(return_value=SimpleNamespace(id_tag_info={"status": auth}))
    return core


def card_logs(caplog):
    return [m for m in caplog.messages if "[CARD-START]" in m]


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["Preparing", "SuspendedEV", "SuspendedEVSE"])
async def test_accepted_card_starts_while_the_ev_waits(state, caplog):
    caplog.set_level(logging.INFO)
    core = card_core([state])
    assert await core._dual_rfid_command_start(1, TAG) is True
    assert core.pulses == [("1", 1), ("1", 0)]
    assert core.rfid_start_received_c1 == 1 and core.start_transaction_SW1 == 1 and core.accepted_tag1 == TAG
    assert core._dual_manager.claimed == [1] and core._dual_manager.released == []
    assert card_logs(caplog) == ["[CARD-START] C1 card accepted, start sent to the PLC (PLC %s)" % state]
    assert all(TAG not in m for m in caplog.messages)


@pytest.mark.asyncio
async def test_card_without_a_vehicle_does_nothing(caplog):
    core = card_core(["Available"])
    assert await core._dual_rfid_command_start(2, TAG) is False
    core.authorize.assert_not_awaited()
    assert core.pulses == [] and core.files == {} and core._dual_manager.claimed == []
    assert card_logs(caplog) == ["[CARD-START] C2 card not started: no vehicle connected - plug in, then tap the card"]


@pytest.mark.asyncio
@pytest.mark.parametrize("after_authorize", ["Charging", "Finishing", "Faulted", "Available"])
async def test_accepted_card_is_not_started_when_the_plc_moved_on(after_authorize, caplog):
    core = card_core(["SuspendedEV", after_authorize])
    assert await core._dual_rfid_command_start(1, TAG) is False
    assert core.pulses == [] and core.files == {}
    assert core._dual_manager.claimed == [1] and core._dual_manager.released == [1]
    assert card_logs(caplog) == ["[CARD-START] C1 card accepted but not started: PLC reports %s" % after_authorize]
    assert all(TAG not in m for m in caplog.messages)


@pytest.mark.asyncio
async def test_every_other_refusal_names_its_reason(caplog):
    caplog.set_level(logging.INFO)
    core = card_core(["SuspendedEV"])
    core.busy = "previous session finishing - unplug the cable first"
    assert await core._dual_rfid_command_start(1, TAG) is False

    core = card_core(["SuspendedEV"])
    core._dual_manager.free = False
    assert await core._dual_rfid_command_start(1, TAG) is False

    core = card_core(["SuspendedEV"], auth="Invalid")
    assert await core._dual_rfid_command_start(1, TAG) is False
    assert core._dual_manager.released == [1] and core.pulses == []

    core = card_core(["SuspendedEV"])
    core.transactionId_c1 = 77
    assert await core._dual_rfid_command_start(1, TAG) is False

    core = card_core(["SuspendedEV"])
    core._ocpp_start_allowed = AsyncMock(return_value=False)
    core._ocpp_start_refusal = lambda cid: "connector Inoperative"
    assert await core._dual_rfid_command_start(1, TAG) is False
    core.authorize.assert_not_awaited()

    assert card_logs(caplog) == [
        "[CARD-START] C1 card not started: previous session finishing - unplug the cable first",
        "[CARD-START] C1 card not started: connector claimed by another start",
        "[CARD-START] C1 card not started: not accepted by a CSMS",
        "[CARD-START] C1 card accepted but not started: a transaction is open",
        "[CARD-START] C1 card not started: connector Inoperative",
    ]
    assert all(TAG not in m for m in caplog.messages)


@pytest.mark.asyncio
async def test_field_replay_2026_11_10_card_tapped_during_cable_check(tmp_path, caplog):
    """CP.py's card start, its real start guard and Authorize against the real router."""
    caplog.set_level(logging.INFO)
    async with FakeCSMS(transaction_id=4321) as a, FakeCSMS(auth_status="Invalid") as b:
        async with running_manager(configuration(a, b), tmp_path) as (manager, _):
            core = card_core(["SuspendedEV"],                       # 14:17:51 CP state C, CableCheck
                             names=("_dual_rfid_command_start", "authorize") + GUARD,
                             manager=manager, take_local_request=lambda kind, cid: 0.0, call=call)
            del core._dual_connector_busy, core._dual_busy_reason, core._valid_id_tag, core.authorize
            core.call = manager.call
            core._dual_ready, core._protocol_runtime = True, None
            core._dual_aborting = set()
            core._remote_stop_worker_running = lambda cid: False
            core._session_finishing_active = lambda cid: False
            core._local_status_session_active = lambda cid: False
            core.auth_timestamps = {}
            core.AuthorizationCacheEnabled_cfg = "false"
            assert await core._dual_rfid_command_start(1, TAG) is True     # 14:17:54 R3.1.6: False
            assert core.pulses == [("1", 1), ("1", 0)]
            assert manager._live(1)["owner"] == "A" and manager._live(1)["state"] == "reserved"
            started = await manager.call(call.StartTransaction(connector_id=1, id_tag=TAG, meter_start=5, timestamp=now()))
            assert started.id_tag_info["status"] == "Accepted"
            assert len(a.received("StartTransaction")) == 1 and b.received("StartTransaction") == []
    assert card_logs(caplog) == ["[CARD-START] C1 card accepted, start sent to the PLC (PLC SuspendedEV)"]
    assert all(TAG not in r.getMessage() for r in caplog.records if r.name in ("root", "dual_csms"))
