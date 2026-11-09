"""Real loopback OCPP Reset/Unlock checks; hardware CP.py is never imported."""
import asyncio
from contextlib import asynccontextmanager
import json

import pytest
from ocpp.v16 import call

from helpf.dual_csms import DualCSMSManager
from live_dual_simulator import SimCore, prepare_state
from test_dual_csms_integration import FakeCSMS, eventually
from test_live_simulator import make_config


@asynccontextmanager
async def running_core(tmp_path, *, full=True):
    async with FakeCSMS(transaction_id=101) as a, FakeCSMS(transaction_id=202) as b:
        directory = prepare_state(tmp_path / 'state')
        manager = DualCSMSManager(make_config(a, b), directory)
        core = SimCore(manager, directory, .2, full_protocol=full)
        core._reset_reply_delay = .03
        core._reset_wait_seconds = 2
        manager.bind_core(core)
        if full:
            await core._ocpp_protocol().bootstrap()
        task = asyncio.create_task(manager.start())
        try:
            await manager.call(call.BootNotification(charge_point_vendor='LocalTest',
                charge_point_model='Simulator', firmware_version='SIM-R3.1-2026-11-08'))
            await asyncio.wait_for(asyncio.gather(*(event.wait() for event in manager.ready.values())), 3)
            core._dual_ready = True
            for cid in (0, 1, 2):
                await core.status(cid, 'Available')
            yield core, manager, a, b
        finally:
            core._dual_ready = False
            for child in list(core.tasks):
                child.cancel()
            await asyncio.gather(*list(core.tasks), return_exceptions=True)
            await manager.close()
            await task


@pytest.mark.parametrize('reset_type', ['Soft', 'Hard'])
async def test_reset_stops_both_owners_before_reboot_and_never_resumes(tmp_path, reset_type):
    async with running_core(tmp_path) as (core, manager, a, b):
        assert (await a.request('RemoteStartTransaction', {'connectorId': 1, 'idTag': 'TEST-A'}))['status'] == 'Accepted'
        assert (await b.request('RemoteStartTransaction', {'connectorId': 2, 'idTag': 'TEST-B'}))['status'] == 'Accepted'
        await eventually(lambda: all(core._active_transaction_id(cid) > 0 for cid in (1, 2)))
        assert (await a.request('Reset', {'type': reset_type}))['status'] == 'Accepted'
        assert not core._dual_ready
        reset_task = core._reset_task
        assert (await core.hard_Reset(type=reset_type)).status == 'Accepted'
        assert core._reset_task is reset_task
        # Dispatch remains closed to new starts and to repeated admin commands.
        assert (await b.request('RemoteStartTransaction', {'connectorId': 1, 'idTag': 'TEST-RACE'}))['status'] == 'Rejected'
        await asyncio.wait_for(asyncio.shield(reset_task), 5)
        assert core._dual_ready and not core._reset_blocked
        assert all(core._active_transaction_id(cid) == 0 for cid in (1, 2))
        for cid in (1, 2):
            assert (core.directory / f'translogs{cid}.txt').read_text() == '0'
        for endpoint, remote_id in ((a, 101), (b, 202)):
            assert len(endpoint.received('StartTransaction')) == 1
            assert len(endpoint.received('StopTransaction')) == 1
            assert endpoint.received('StopTransaction')[0]['transactionId'] == remote_id
            assert endpoint.received('StopTransaction')[0]['reason'] == reset_type + 'Reset'
            assert len(endpoint.received('BootNotification')) == 2
            actions = [action for action, _, _ in endpoint.calls]
            assert actions.index('StopTransaction') < len(actions) - 1 - actions[::-1].index('BootNotification')
        assert manager.db.execute("SELECT count(*) FROM sessions WHERE state != 'closed'").fetchone()[0] == 0
        assert manager.db.execute("SELECT count(*) FROM outbox WHERE state != 'delivered'").fetchone()[0] == 0
        events = [json.loads(line) for line in (core.directory / 'simulator-events.jsonl').read_text().splitlines()]
        assert sum(row['event'] == 'simulated_reset_completed' for row in events) == 1
        assert next(row for row in events if row['event'] == 'simulated_reset_completed')['physical_reboot'] is False


async def test_reset_registration_wait_covers_reconnect_delay(tmp_path):
    """R3.1 live finding: a reconnect delay equal to the fixed reset wait blocked the simulator."""
    async with running_core(tmp_path) as (core, manager, a, b):
        manager.config["reconnect_seconds"] = .6
        core._reset_wait_seconds = .4   # shorter than reconnect delay + Boot round trip
        assert (await a.request('Reset', {'type': 'Soft'}))['status'] == 'Accepted'
        await asyncio.wait_for(asyncio.shield(core._reset_task), 8)
        assert core._dual_ready and not core._reset_blocked
        events = [json.loads(line) for line in (core.directory / 'simulator-events.jsonl').read_text().splitlines()]
        assert sum(row['event'] == 'simulated_reset_completed' for row in events) == 1
        assert not any(row['event'] == 'simulated_reset_blocked' for row in events)
        assert len(a.received('BootNotification')) == len(b.received('BootNotification')) == 2


async def test_idle_reset_reboots_without_creating_or_resuming_transactions(tmp_path):
    async with running_core(tmp_path) as (core, manager, a, b):
        assert (await a.request('Reset', {'type': 'Soft'}))['status'] == 'Accepted'
        await asyncio.wait_for(asyncio.shield(core._reset_task), 4)
        assert core._dual_ready
        assert not a.received('StartTransaction') and not b.received('StartTransaction')
        assert not a.received('StopTransaction') and not b.received('StopTransaction')
        assert len(a.received('BootNotification')) == len(b.received('BootNotification')) == 2


@pytest.mark.parametrize('state', ['reserved', 'starting', 'unknown'])
async def test_reset_preserves_unresolved_start_claim_and_rejects(tmp_path, state):
    async with running_core(tmp_path) as (core, manager, a, b):
        claim = manager._reserve(1, 'A', 'TEST-PENDING')
        manager.db.execute('UPDATE sessions SET state=? WHERE local_id=?', (state, claim['local_id']))
        manager.db.commit()
        core.heads['1']['pending'] = True
        core.persist()
        assert (await a.request('Reset', {'type': 'Soft'}))['status'] == 'Rejected'
        assert core._reset_task is None and core._dual_ready
        assert manager._live(1)['state'] == state
        assert core.heads['1']['pending'] is True
        assert len(a.received('BootNotification')) == len(b.received('BootNotification')) == 1


async def test_reset_failure_keeps_durable_stop_and_start_gate_closed(tmp_path):
    async with running_core(tmp_path) as (core, manager, a, b):
        core._reset_wait_seconds = .15
        assert (await b.request('RemoteStartTransaction', {'connectorId': 2, 'idTag': 'TEST-B'}))['status'] == 'Accepted'
        await eventually(lambda: core._active_transaction_id(2) > 0)
        b.call_errors['StopTransaction'] = 'GenericError'
        assert (await a.request('Reset', {'type': 'Soft'}))['status'] == 'Accepted'
        await asyncio.wait_for(asyncio.shield(core._reset_task), 3)
        assert not core._dual_ready and core._reset_blocked
        assert core._active_transaction_id(2) == 0
        stop = manager.db.execute("SELECT * FROM outbox WHERE action='StopTransaction'").fetchone()
        assert stop is not None and stop['state'] == 'failed'
        assert len(a.received('BootNotification')) == len(b.received('BootNotification')) == 1
        assert (await b.request('RemoteStartTransaction', {'connectorId': 1, 'idTag': 'TEST-BLOCKED'}))['status'] == 'Rejected'


async def test_only_admin_can_reset_or_unlock_and_simulated_unlock_has_no_os_action(tmp_path):
    async with running_core(tmp_path) as (core, manager, a, b):
        assert (await b.request('Reset', {'type': 'Soft'}))['status'] == 'Rejected'
        assert core._reset_task is None
        assert (await b.request('UnlockConnector', {'connectorId': 1}))['status'] == 'UnlockFailed'
        for cid in (1, 2):
            assert (await a.request('UnlockConnector', {'connectorId': cid}))['status'] == 'Unlocked'
        assert (await a.request('UnlockConnector', {'connectorId': 0}))['status'] == 'NotSupported'
        assert (await core.on_unlock_connector(connector_id='invalid')).status == 'UnlockFailed'
        events = [json.loads(line) for line in (core.directory / 'simulator-events.jsonl').read_text().splitlines()]
        unlocks = [row for row in events if row['event'] == 'simulated_unlock_result']
        assert len(unlocks) == 3 and all(row['physical_unlock'] is False for row in unlocks)


async def test_basic_simulator_keeps_reset_and_unlock_unsupported(tmp_path):
    async with running_core(tmp_path, full=False) as (core, manager, a, b):
        assert (await a.request('Reset', {'type': 'Soft'}))['status'] == 'Rejected'
        assert (await a.request('UnlockConnector', {'connectorId': 1}))['status'] == 'NotSupported'


async def test_invalid_reset_type_and_in_progress_stop_do_not_schedule_reset(tmp_path):
    async with running_core(tmp_path) as (core, manager, a, b):
        assert (await core.hard_Reset(type='Unsupported')).status == 'Rejected'
        core.stopping.add(1)
        assert (await a.request('Reset', {'type': 'Soft'}))['status'] == 'Rejected'
        assert core._reset_task is None and core._dual_ready
