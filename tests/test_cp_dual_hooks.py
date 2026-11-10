"""Execute selected real CP methods without importing its hardware startup code."""
import ast
import asyncio
import logging
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from ocpp.v16 import call, call_result
from helpf.dual_csms import DualCSMSError, StartOutcomeUnknown


SOURCE = Path(__file__).resolve().parents[1] / "CP.py"


def extracted(*names, **overrides):
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
    source = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "ChargePoint")
    nodes = [n for n in source.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name in names]
    assert len(nodes) == len(names)
    for node in nodes:
        node.decorator_list = []
    cls = ast.ClassDef(name="Core", bases=[], keywords=[], body=nodes, decorator_list=[])
    module = ast.fix_missing_locations(ast.Module(body=[cls], type_ignores=[]))
    env = dict(asyncio=asyncio, logging=logging, time=time, SimpleNamespace=SimpleNamespace,
               call=call, call_result=call_result, DualCSMSError=DualCSMSError,
               StartOutcomeUnknown=StartOutcomeUnknown)
    env.update(overrides)
    exec(compile(module, str(SOURCE), "exec"), env)
    return env["Core"]()


async def test_core_call_uses_router_and_keeps_observer():
    expected = call_result.MeterValues()
    router = SimpleNamespace(call=AsyncMock(return_value=expected))
    observed = []
    core = extracted("call")
    core._dual_manager = router
    core._v2g_bridge = SimpleNamespace(observe_ocpp=lambda req, res: observed.append((req, res)))
    request = call.MeterValues(connector_id=1, meter_value=[])
    assert await core.call(request) is expected
    router.call.assert_awaited_once_with(request)
    assert observed == [(request, expected)]


async def test_terminal_start_error_bypasses_legacy_retries_and_aborts_once():
    core = extracted("_call_txn_message_with_retry", "_dual_start_transaction")
    core._dual_manager = object()
    core.TransactionMessageAttempts_cfg = "3"
    core.TransactionMessageRetryInterval_cfg = "10"
    core.call = AsyncMock(side_effect=StartOutcomeUnknown("lost response"))
    core._abort_session_immediately = AsyncMock()
    request = call.StartTransaction(connector_id=2, id_tag="card", meter_start=10,
                                    timestamp="2026-11-07T00:00:00Z")
    assert await core._dual_start_transaction(request) is None
    assert core.call.await_count == 1
    core._abort_session_immediately.assert_awaited_once_with(2, id_tag="card")


@pytest.mark.parametrize("head", [1, 2])
async def test_physical_reader_cannot_overwrite_tag_of_pending_session(head):
    writes = []
    core = extracted(f"_handle_rfid_auth_c{head}",
                     async_get_rfid=AsyncMock(return_value="NEWCARD"), plc=object(),
                     _write_file=lambda *args: writes.append(args))
    core._dual_manager = SimpleNamespace(connector_reserved=lambda cid: True)
    core._dual_ready = True
    core._active_transaction_id = lambda cid: 0
    setattr(core, f"myip_adr_c{head}", str(head))
    setattr(core, f"myid_tag_ath_c{head}", "ORIGINAL")
    await getattr(core, f"_handle_rfid_auth_c{head}")("Preparing", "Available")
    assert getattr(core, f"myid_tag_ath_c{head}") == "ORIGINAL"
    assert writes == []


async def test_mqtt_rfid_claim_is_held_during_authorization_and_pulses_once():
    writes, pulses, claims = [], [], set()
    auth_started, auth_release = asyncio.Event(), asyncio.Event()

    def claim(cid, tag):
        if cid in claims:
            return False
        claims.add(cid)
        return True

    async def authorize(tag):
        auth_started.set()
        await auth_release.wait()
        return call_result.Authorize(id_tag_info={"status": "Accepted"})

    async def pulse(cid, level):
        pulses.append((cid, level))

    async def no_sleep(seconds):
        return None

    core = extracted("_dual_rfid_command_start",
                     asyncio=SimpleNamespace(sleep=no_sleep),
                     plc=SimpleNamespace(get_connector_status=lambda cid: ("Preparing", "Available")),
                     async_remote_start_plc=pulse, _write_file=lambda *args: writes.append(args))
    core._dual_manager = SimpleNamespace(claim_local=claim, release_pending=lambda cid: claims.discard(cid))
    core.allow_rfid_command_start = True
    core._valid_id_tag = lambda tag: bool(tag)
    core._dual_local_claims = set()
    core._dual_connector_busy = lambda cid: cid in core._dual_local_claims
    core._dual_busy_reason = lambda cid: "local RFID start in progress"   # R3.1.7 logs the reason
    core._active_transaction_id = lambda cid: 0
    core.authorize = authorize
    core._clear_stale_stop_latch_for_new_start = lambda *args: None
    core._abort_session_immediately = AsyncMock()
    task = asyncio.create_task(core._dual_rfid_command_start(1, "OWNER"))
    await auth_started.wait()
    assert 1 in claims
    assert await core._dual_rfid_command_start(1, "OTHER") is False
    assert writes == [] and pulses == []
    auth_release.set()
    assert await task is True
    assert core.myid_tag_c1 == "OWNER"
    assert pulses == [("1", 1), ("1", 0)]
    assert 1 in claims  # Held until transaction termination, not just auth completion.


async def test_authorize_rejection_is_not_cached_as_success_timestamp():
    core = extracted("authorize")
    core._dual_manager = object()
    core.auth_timestamps = {"card": 123.0}
    core.AuthorizationCacheEnabled_cfg = "false"
    core.call = AsyncMock(return_value=call_result.Authorize(id_tag_info={"status": "Invalid"}))
    assert (await core.authorize("card")).id_tag_info["status"] == "Invalid"
    assert "card" not in core.auth_timestamps


async def test_dual_stop_uses_standard_payload_without_connector_extension():
    core = extracted("_send_stop_transaction")
    core._dual_manager = SimpleNamespace(call=AsyncMock(return_value=call_result.StopTransaction()))
    request = call.StopTransaction(transaction_id=1, meter_stop=20, timestamp="2026-11-07T00:00:00Z")
    await core._send_stop_transaction(1, request)
    core._dual_manager.call.assert_awaited_once_with(request)
