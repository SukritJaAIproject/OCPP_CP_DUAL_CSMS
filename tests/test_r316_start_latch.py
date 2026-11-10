"""R3.1.6: a RemoteStart that ends before StartTransaction no longer locks its head.

Found on Edgebox 192.168.3.15 on 2026-11-10: a RemoteStart from A was accepted at 10:37:49
and pulsed, the EV ended its DIN session at 10:38:07 with no transaction, the cable was
removed at 10:38:22 - and start_transaction_SW2 stayed 3. Every later RemoteStart on head 2,
from A and from B, was refused with "start already sent to the PLC (start_transaction_SW2=3)"
until CP.py was restarted.

R3.1.6 releases the start latch when its session ends (Finishing before StartTransaction,
cable removed, aborted start, boot with the cable out), and the start guard releases a latch
that was left behind anyway instead of refusing. A start that may still be running keeps
the head. No idTag is ever logged.
"""
import ast
import asyncio
import logging
import os
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from ocpp.v16 import call_result
from ocpp.v16.enums import RemoteStartStopStatus

from test_dual_csms_integration import FakeCSMS, FakeCore, configuration, running_manager

SOURCE = Path(__file__).resolve().parents[1] / "CP.py"
TEXT = SOURCE.read_text(encoding="utf-8")
SECRET_TAG = "SECRET-TAG-316"   # <= 20 chars (CiString20)
TIMEOUT = 180.0                 # REMOTE_START_LATCH_TIMEOUT_SECONDS default
LATCH = ("_heal_stale_start_latch", "_stale_start_latch_reason", "_clear_remote_start_latch",
         "_remote_start_latch_age", "_valid_id_tag")
GUARD = ("_dual_connector_busy", "_dual_busy_reason") + LATCH


def cp_class(names, base=None, **env_extra):
    """A class made of the named ChargePoint methods, taken unchanged from CP.py."""
    tree = ast.parse(TEXT)
    source = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "ChargePoint")
    nodes = [n for n in source.body
             if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name in names]
    assert sorted(n.name for n in nodes) == sorted(set(names))
    for node in nodes:   # keep @staticmethod, drop the ocpp @on(...) routing decorators
        node.decorator_list = [d for d in node.decorator_list
                               if isinstance(d, ast.Name) and d.id == "staticmethod"]
    bases = [ast.Name(id="Base", ctx=ast.Load())] if base is not None else []
    module = ast.fix_missing_locations(ast.Module(body=[ast.ClassDef(
        name="Core", bases=bases, keywords=[], body=nodes, decorator_list=[])], type_ignores=[]))
    env = {"Base": base, "staticmethod": staticmethod, "time": time, "os": os, "logging": logging,
           "asyncio": asyncio, "REMOTE_START_LATCH_TIMEOUT_SECONDS": TIMEOUT,
           "call_result": call_result, "RemoteStartStopStatus": RemoteStartStopStatus}
    env.update(env_extra)
    exec(compile(module, str(SOURCE), "exec"), env)
    return env["Core"]


class Files(dict):
    """_write_file stand-in: the last value written to each state file."""

    def __call__(self, name, value):
        self[name] = value


class Claims:
    """The dual-CSMS router as the latch helpers see it: one pending claim per head."""

    def __init__(self, age=None):
        self.age, self.released = age, []

    def reserved_age(self, connector):
        return self.age

    def release_pending(self, connector):
        if self.age is None:
            return False
        self.age = None
        self.released.append(connector)
        return True


def blocks(n):
    return ("block_sw1_C1", "block_sw2_C1", "block_sw3_C1") if n == 1 else ("block_sw1", "block_sw2", "block_sw3")


def idle_flags(core):
    for cid in (1, 2):
        for name in ("remote_start_received", "myremotestart", "rfid_start_received", "rfid_recovery_pending"):
            setattr(core, "%s_c%d" % (name, cid), 0)
        setattr(core, "start_transaction_SW%d" % cid, 1)
        setattr(core, "start_trans_SW_c%d" % cid, 1)
        setattr(core, "accepted_tag%d" % cid, "0")
        setattr(core, "active_id_tag_c%d" % cid, "Default")
        setattr(core, "transactionId_c%d" % cid, 0)


def accepted(core, cid):
    """State remote_start_transaction() leaves: the PLC pulse is still to come."""
    setattr(core, "myremotestart_c%d" % cid, 1)
    setattr(core, "remote_start_received_c%d" % cid, 1)
    setattr(core, "_remote_start_latch_mono_c%d" % cid, time.monotonic())


def pulse(core, cid, age=0.0):
    """State main_run_program() leaves after the PLC start pulse of an accepted RemoteStart."""
    setattr(core, "remote_start_received_c%d" % cid, 1)
    setattr(core, "start_transaction_SW%d" % cid, 3)
    setattr(core, "myremotestart_c%d" % cid, 0)
    setattr(core, "start_trans_SW_c%d" % cid, 2)
    setattr(core, "_remote_start_latch_mono_c%d" % cid, time.monotonic() - age)


def latch(core, cid):
    return tuple(int(getattr(core, name % cid)) for name in
                 ("myremotestart_c%d", "remote_start_received_c%d", "start_transaction_SW%d"))


def latch_logs(caplog):
    return [m for m in caplog.messages if "[START-LATCH]" in m]


# ---------------------------------------------------------------------------- the start guard
def guard_core(plc_state="Preparing", claims=None):
    files, calls = Files(), []

    def take_local_request(kind, cid):
        calls.append(("take_local_request", kind, cid))
        return 0.0

    def get_connector_status(cid):
        calls.append(("plc", cid))
        if isinstance(plc_state, Exception):
            raise plc_state
        return plc_state, plc_state

    core = cp_class(GUARD, _write_file=files, take_local_request=take_local_request,
                    plc=SimpleNamespace(get_connector_status=get_connector_status))()
    core.files, core.calls = files, calls
    core._dual_ready, core._dual_manager, core._protocol_runtime = True, claims, None
    core._dual_aborting, core._dual_local_claims = set(), set()
    core.txn, core.finishing, core.local_session, core.stop_worker = 0, False, False, False
    core._active_transaction_id = lambda cid: core.txn
    core._remote_stop_worker_running = lambda cid: core.stop_worker
    core._session_finishing_active = lambda cid: core.finishing
    core._local_status_session_active = lambda cid: core.local_session
    idle_flags(core)
    return core


@pytest.mark.parametrize("cid", [1, 2])
def test_field_state_sw3_left_after_its_session_is_released_and_the_start_admitted(cid, caplog):
    caplog.set_level(logging.INFO)
    core = guard_core("Preparing")
    setattr(core, "start_transaction_SW%d" % cid, 3)        # what R3.1.5 left on 2026-11-10
    assert core._dual_busy_reason(cid) == ""
    assert core._dual_connector_busy(cid) is False
    assert latch(core, cid) == (0, 0, 1)
    assert core.files == {"start_transaction_SW%d.txt" % cid: 1}
    assert latch_logs(caplog) == [
        "[START-LATCH] C%d released start_transaction_SW%d=3: stale, start_transaction_SW%d=3 "
        "left after its RemoteStart session ended" % (cid, cid, cid)]
    # the release itself reads no hardware; the guard then runs its usual last checks
    assert core.calls[0] == ("take_local_request", "start", cid)


def test_remote_start_flag_without_pulse_is_released(caplog):
    caplog.set_level(logging.INFO)
    core = guard_core()
    core.remote_start_received_c1 = 1     # R3.1.5's boot reset cleared SW1 but not this flag
    assert core._dual_busy_reason(1) == ""
    assert latch(core, 1) == (0, 0, 1) and core.files == {"remote_start_received_c1.txt": 0}
    assert latch_logs(caplog) == ["[START-LATCH] C1 released remote_start_received_c1=1: stale, "
                                  "remote_start_received_c1=1 without a PLC start pulse"]


@pytest.mark.parametrize("setup", [
    pytest.param(lambda c: accepted(c, 1), id="pulse-still-to-come"),
    pytest.param(lambda c: pulse(c, 1, age=5), id="pulsed-5s-ago"),
    pytest.param(lambda c: pulse(c, 1, age=TIMEOUT - 1), id="pulsed-179s-ago"),
    pytest.param(lambda c: (pulse(c, 1), setattr(c, "_remote_start_latch_mono_c1", None)), id="age-unknown"),
])
def test_a_start_that_may_still_run_keeps_the_head(setup, tmp_path, monkeypatch, caplog):
    monkeypatch.chdir(tmp_path)           # no state file: the age of a restored latch is unknown
    core = guard_core()
    setup(core)
    before = latch(core, 1)
    assert core._dual_busy_reason(1) == "remote_start_received_c1=1"
    assert latch(core, 1) == before and core.files == {}
    assert core.calls == []               # like R3.1.1: nothing after the failed check runs
    assert latch_logs(caplog) == []


@pytest.mark.parametrize("state", ["Available", "Preparing"])
def test_old_latch_is_released_when_the_plc_shows_no_ev_session(state, caplog):
    caplog.set_level(logging.INFO)
    core = guard_core(state)
    pulse(core, 1, age=TIMEOUT + 20)
    assert core._dual_busy_reason(1) == ""
    assert latch(core, 1) == (0, 0, 1)
    assert core.files == {"remote_start_received_c1.txt": 0, "start_transaction_SW1.txt": 1}
    assert latch_logs(caplog) == [
        "[START-LATCH] C1 released remote_start_received_c1=1, start_transaction_SW1=3: stale, "
        "RemoteStart sent to the PLC 200 s ago and no StartTransaction (PLC %s)" % state]


@pytest.mark.parametrize("state", ["SuspendedEV", "SuspendedEVSE", "Charging", "Finishing", "Faulted",
                                   TimeoutError("no PLC")])
def test_old_latch_is_kept_while_an_ev_session_may_run(state, caplog):
    core = guard_core(state)
    pulse(core, 1, age=TIMEOUT + 20)
    assert core._dual_busy_reason(1) == "remote_start_received_c1=1"
    assert latch(core, 1) == (0, 1, 3) and core.files == {}
    assert core.calls == [("plc", "1")]   # the one read that decides it
    assert latch_logs(caplog) == []


@pytest.mark.parametrize("setup, expected", [
    (lambda c: setattr(c, "txn", 41), "transaction 41 is active"),
    (lambda c: setattr(c, "rfid_start_received_c1", 1), "rfid_start_received_c1=1"),
    (lambda c: setattr(c, "rfid_recovery_pending_c1", 1), "rfid_recovery_pending_c1=1"),
    (lambda c: setattr(c, "accepted_tag1", "CARD"), "start already sent to the PLC (start_transaction_SW1=3)"),
    (lambda c: c._dual_local_claims.add(1), "local RFID start in progress"),
    (lambda c: c._dual_aborting.add(1), "previous start is being aborted"),
    (lambda c: setattr(c, "finishing", True), "previous session finishing - unplug the cable first"),
    (lambda c: setattr(c, "local_session", True), "session started from the charger screen"),
    (lambda c: setattr(c, "stop_worker", True), "remote stop still running"),
    (lambda c: setattr(c, "_dual_ready", False), "charger startup not finished"),
])
def test_a_head_owned_by_something_else_is_never_released(setup, expected, caplog):
    core = guard_core(claims=Claims(age=TIMEOUT + 20))
    core.start_transaction_SW1 = 3
    setup(core)
    assert core._dual_busy_reason(1) == expected
    assert core._heal_stale_start_latch(1) == ""
    assert core.start_transaction_SW1 == 3 and core.files == {}
    assert core._dual_manager.released == []
    assert latch_logs(caplog) == []


def test_open_transaction_is_never_touched_by_the_release_itself():
    core = guard_core(claims=Claims(age=TIMEOUT + 20))
    pulse(core, 1)
    core.txn = 41
    assert core._clear_remote_start_latch(1, "test") is False
    assert latch(core, 1) == (0, 1, 3) and core.files == {} and core._dual_manager.released == []


def test_latch_age_after_a_restart_comes_from_its_state_file(tmp_path, monkeypatch, caplog):
    monkeypatch.chdir(tmp_path)
    caplog.set_level(logging.INFO)
    core = guard_core("Preparing")
    pulse(core, 1)
    core._remote_start_latch_mono_c1 = None          # a new process: only the files are left
    state_file = tmp_path / "start_transaction_SW1.txt"
    state_file.write_text("3")
    os.utime(state_file, (time.time() + 3600, time.time() + 3600))
    assert core._remote_start_latch_age(1) is None   # clock set back: the file proves nothing
    assert core._dual_busy_reason(1) == "remote_start_received_c1=1"
    os.utime(state_file, (time.time() - 300, time.time() - 300))
    assert 299 <= core._remote_start_latch_age(1) <= 310
    assert core._dual_busy_reason(1) == ""
    assert latch(core, 1) == (0, 0, 1)
    assert "RemoteStart sent to the PLC 300 s ago" in latch_logs(caplog)[0]


@pytest.mark.parametrize("setup, claim_age, released_flags, claim_released, reason", [
    # the claim of a start being admitted right now must survive
    (lambda c: setattr(c, "start_transaction_SW1", 3), 5, True, False,
     "start_transaction_SW1=3 left after its RemoteStart session ended"),
    (lambda c: setattr(c, "start_transaction_SW1", 3), TIMEOUT + 20, True, True,
     "start_transaction_SW1=3 left after its RemoteStart session ended"),
    (lambda c: pulse(c, 1, age=TIMEOUT + 20), TIMEOUT + 25, True, True,
     "RemoteStart sent to the PLC 200 s ago and no StartTransaction (PLC Preparing)"),
    (lambda c: None, TIMEOUT + 20, False, True, "CSMS claim pending 200 s without a start"),
    (lambda c: None, 5, False, False, ""),
    (lambda c: pulse(c, 1, age=5), 6, False, False, ""),
])
def test_pending_csms_claim(setup, claim_age, released_flags, claim_released, reason, caplog):
    caplog.set_level(logging.INFO)
    core = guard_core("Preparing", claims=Claims(age=claim_age))
    setup(core)
    assert core._heal_stale_start_latch(1) == reason
    assert bool(core.files) is released_flags
    if released_flags:
        assert latch(core, 1) == (0, 0, 1)
    assert core._dual_manager.released == ([1] if claim_released else [])
    if reason:
        assert latch_logs(caplog)[0].endswith(": stale, " + reason)
        assert ("CSMS claim" in latch_logs(caplog)[0]) is claim_released
    else:
        assert latch_logs(caplog) == []


def test_release_check_never_raises(caplog):
    core = guard_core()
    core.start_transaction_SW1 = 3

    def broken(cid):
        raise RuntimeError("translogs1.txt unreadable")
    core._active_transaction_id = broken
    assert core._heal_stale_start_latch(1) == ""
    assert latch_logs(caplog) == ["[START-LATCH] C1 stale start check failed (RuntimeError)"]


def module_constant(value):
    tree = ast.parse(TEXT)
    node = next(n for n in tree.body if isinstance(n, ast.Try) and any(
        isinstance(t, ast.Name) and t.id == "REMOTE_START_LATCH_TIMEOUT_SECONDS"
        for s in n.body if isinstance(s, ast.Assign) for t in s.targets))
    env = {"os": SimpleNamespace(getenv=lambda key, default: default if value is None else value)}
    exec(compile(ast.fix_missing_locations(ast.Module(body=[node], type_ignores=[])), str(SOURCE), "exec"), env)
    return env["REMOTE_START_LATCH_TIMEOUT_SECONDS"]


@pytest.mark.parametrize("value, expected", [(None, 180.0), ("600", 600.0), ("5", 30.0), ("three minutes", 180.0)])
def test_timeout_setting(value, expected):
    assert module_constant(value) == expected


# ------------------------------------------------------- the session ends before StartTransaction
def handler_core(n, names, tmp_path, monkeypatch, claims, plc_finishing=0):
    monkeypatch.chdir(tmp_path)           # no translogsN.txt: no transaction on the head
    files = Files()

    async def async_get_finishing_status(plc, ip):
        return plc_finishing

    core = cp_class(tuple(names) + LATCH, _write_file=files, clear_local_request=lambda kind, cid: None,
                    async_get_finishing_status=async_get_finishing_status, plc=SimpleNamespace())()
    core.files = files
    idle_flags(core)
    core._dual_manager = claims
    core._active_transaction_id = lambda cid: 0
    core._local_status_session_active = lambda cid: False
    core._rfid_session_authorized = lambda cid: False
    for name in ("_clear_stale_stop_latch_for_new_start", "_repair_authorized_start_switches",
                 "_set_local_status_session", "_set_session_finishing", "_clear_pending_local_stop"):
        setattr(core, name, MagicMock())
    for name in ("status_notification_with_status", "status_notification_usiLink", "send_stop_transaction",
                 "status_notification_preparing_timeout", "_stop_orphaned_transaction_c1",
                 "_stop_orphaned_transaction_c2"):
        setattr(core, name, AsyncMock())
    core._finishing_stop_reason = lambda cid: "EVDisconnected"
    core._autocharge_task = {}
    core.remote_start_only = True
    for cid in (1, 2):
        setattr(core, "remote_stop_finishing_c%d" % cid, 0)
        setattr(core, "finshing_sent_sw_c%d" % cid, 1)
        setattr(core, "finshing_sent_c%d" % cid, 0)
        setattr(core, "block%d" % cid, 0)
        setattr(core, "myip_adr_c%d" % cid, str(cid))
    sw1, sw2, sw3 = blocks(n)
    setattr(core, sw1, 1)                 # Preparing was reported for this plug-in
    setattr(core, sw2, 0)
    setattr(core, sw3, 1)
    return core


@pytest.mark.asyncio
@pytest.mark.parametrize("n", [1, 2])
async def test_finishing_before_start_transaction_reports_finishing_and_releases(n, tmp_path, monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    core = handler_core(n, ["_handle_finishing_c%d" % n], tmp_path, monkeypatch, Claims(age=20))
    pulse(core, n, age=18)                # 2026-11-10: SessionStop 18 s after the pulse
    setattr(core, "finshing_sent_c%d" % n, 1)
    await getattr(core, "_handle_finishing_c%d" % n)()
    core.status_notification_usiLink.assert_awaited_once_with(n)
    core.send_stop_transaction.assert_not_awaited()
    core.status_notification_preparing_timeout.assert_not_awaited()
    assert latch(core, n) == (0, 0, 1) and core._dual_manager.released == [n]
    assert getattr(core, blocks(n)[0]) == 1 and getattr(core, "finshing_sent_sw_c%d" % n) == 2
    assert latch_logs(caplog) == [
        "[START-LATCH] C%d RemoteStart session ended before StartTransaction; "
        "sending Finishing without StopTransaction" % n,
        "[START-LATCH] C%d released remote_start_received_c%d=1, start_transaction_SW%d=3, "
        "CSMS claim: session ended before StartTransaction" % (n, n, n)]


@pytest.mark.asyncio
@pytest.mark.parametrize("n", [1, 2])
async def test_finishing_keeps_the_card_and_preparing_timeout_branches(n, tmp_path, monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    core = handler_core(n, ["_handle_finishing_c%d" % n], tmp_path, monkeypatch, Claims())
    setattr(core, "rfid_start_received_c%d" % n, 1)
    setattr(core, "finshing_sent_c%d" % n, 1)
    await getattr(core, "_handle_finishing_c%d" % n)()
    assert any("[RFID-OCPP-V7] C%d session ended before StartTransaction" % n in m for m in caplog.messages)

    core = handler_core(n, ["_handle_finishing_c%d" % n], tmp_path, monkeypatch, Claims())
    setattr(core, "finshing_sent_c%d" % n, 1)  # plugged in, no RemoteStart: PREPARING TIMEOUT as before
    await getattr(core, "_handle_finishing_c%d" % n)()
    core.status_notification_preparing_timeout.assert_awaited_once_with(n)
    core.status_notification_usiLink.assert_not_awaited()
    assert latch_logs(caplog) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("n", [1, 2])
async def test_cable_removed_before_start_transaction_releases(n, tmp_path, monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    core = handler_core(n, ["_handle_status_notification_c%d" % n, "_release_session_identity"],
                        tmp_path, monkeypatch, Claims(age=33))
    pulse(core, n, age=33)                # 2026-11-10: cable removed 33 s after the pulse
    await getattr(core, "_handle_status_notification_c%d" % n)("Available", "Available")
    core.status_notification_with_status.assert_awaited_once_with(n, "Available")
    assert latch(core, n) == (0, 0, 1) and core._dual_manager.released == [n]
    assert latch_logs(caplog) == ["[START-LATCH] C%d released start_transaction_SW%d=3, CSMS claim: "
                                  "cable removed before StartTransaction" % (n, n)]


@pytest.mark.asyncio
@pytest.mark.parametrize("n", [1, 2])
async def test_cable_removed_after_a_finished_transaction_logs_nothing(n, tmp_path, monkeypatch, caplog):
    core = handler_core(n, ["_handle_status_notification_c%d" % n, "_release_session_identity"],
                        tmp_path, monkeypatch, Claims())
    setattr(core, "start_transaction_SW%d" % n, 4)   # RemoteStart transaction, stopped already
    await getattr(core, "_handle_status_notification_c%d" % n)("Available", "Available")
    assert getattr(core, "start_transaction_SW%d" % n) == 4
    assert latch_logs(caplog) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("n, setup, released", [
    (1, lambda c: pulse(c, 1), "start_transaction_SW1=3"),     # StartTransaction rejected
    (2, lambda c: pulse(c, 2), "start_transaction_SW2=3"),
    (2, lambda c: accepted(c, 2), "myremotestart_c2=1"),        # Inoperative before the pulse
])
async def test_aborted_start_releases_the_latch_before_its_awaits(n, setup, released, caplog):
    caplog.set_level(logging.INFO)
    seen, files = [], Files()

    async def async_remote_stop_plc(ip, value):
        seen.append((value, latch(core, n)))

    core = cp_class(("_abort_session_immediately",) + LATCH, _write_file=files,
                    async_remote_stop_plc=async_remote_stop_plc, asyncio=SimpleNamespace(sleep=AsyncMock()))()
    idle_flags(core)
    core._dual_manager, core._dual_aborting = Claims(age=40), set()
    core._active_transaction_id = lambda cid: int(getattr(core, "transactionId_c%d" % cid) or 0)
    core._set_session_finishing = MagicMock()
    core._send_orphan_stop_transaction = AsyncMock()
    core.status_notification_with_status = AsyncMock()
    core.myip_adr_c1, core.myip_adr_c2 = "1", "2"
    setup(core)
    await core._abort_session_immediately(n)
    assert latch(core, n) == (0, 0, 1)
    assert seen[0] == (1, (0, 0, 1))      # released before the first await, like the flags beside it
    assert core._dual_manager.released == [n]   # the claim still goes after the orphan stop
    assert latch_logs(caplog) == ["[START-LATCH] C%d released %s: start aborted" % (n, released)]


def test_boot_reset_with_the_cable_out_clears_the_remote_start_flags():
    tree = ast.parse(TEXT)
    node = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == "_reset_connector")
    files = Files()
    env = {"_write_file": files}
    exec(compile(ast.fix_missing_locations(ast.Module(body=[node], type_ignores=[])), str(SOURCE), "exec"), env)
    env["_reset_connector"](2)
    assert files["myremotestart_c2.txt"] == "0" and files["remote_start_received_c2.txt"] == "0"
    assert files["start_transaction_SW2.txt"] == "1"
    for n in (1, 2):
        assert ("cp_cls.myremotestart_c%d = cp_cls.remote_start_received_c%d = 0" % (n, n)) in TEXT


def test_the_pulse_and_the_acceptance_record_when_the_latch_was_set():
    text = TEXT.replace("\r\n", "\n")
    for n in (1, 2):
        assert ("self.remote_start_received_c%d = 1\n"
                "            self._remote_start_latch_mono_c%d = time.monotonic()" % (n, n)) in text
        assert ("self.start_transaction_SW%d = 3\n"
                "                    self.myremotestart_c%d = 0\n"
                "                    self.start_trans_SW_c%d = 2\n"
                "                    self._remote_start_latch_mono_c%d = time.monotonic()" % (n, n, n, n)
                ) in text


# --------------------------------------------- field replay through the dual-CSMS router (head 2)
def replay_core():
    """CP.py's guard, latch helpers, RemoteStart handler and head-2 handlers on a FakeCore."""
    plc = {"state": "Available", "finishing": 0}
    files = Files()

    async def async_get_finishing_status(plc_object, ip):
        return plc["finishing"]

    names = GUARD + ("remote_start_transaction", "_handle_status_notification_c2", "_handle_finishing_c2",
                     "_release_session_identity")
    core = cp_class(names, base=FakeCore, _write_file=files,
                    take_local_request=lambda kind, cid: 0.0, clear_local_request=lambda kind, cid: None,
                    async_get_finishing_status=async_get_finishing_status, command_origin=lambda name: name,
                    plc=SimpleNamespace(get_connector_status=lambda cid: (plc["state"], plc["state"])))()
    core.files = files
    idle_flags(core)
    core._protocol_runtime = None
    core._dual_aborting, core._dual_local_claims = set(), set()
    core.finishing_latch = False
    core._remote_stop_worker_running = lambda cid: False
    core._session_finishing_active = lambda cid: core.finishing_latch
    core._local_status_session_active = lambda cid: False
    core._rfid_session_authorized = lambda cid: False
    core.statuses = []

    async def status(cid, value):
        core.statuses.append(value)

    async def finishing(cid):
        core.finishing_latch = True
        core.statuses.append("Finishing")

    def set_finishing(cid, value):
        core.finishing_latch = bool(value)

    core.status_notification_with_status = status
    core.status_notification_usiLink = finishing
    core._set_session_finishing = set_finishing
    for name in ("_clear_stale_stop_latch_for_new_start", "_repair_authorized_start_switches",
                 "_set_local_status_session", "_clear_pending_local_stop"):
        setattr(core, name, MagicMock())
    for name in ("send_stop_transaction", "status_notification_preparing_timeout", "_stop_orphaned_transaction_c2"):
        setattr(core, name, AsyncMock())
    core._finishing_stop_reason = lambda cid: "EVDisconnected"
    core._autocharge_task = {}
    core.remote_start_only = True
    core.remote_stop_finishing_c2, core.finshing_sent_sw_c2, core.finshing_sent_c2, core.block2 = 0, 1, 0, 0
    core.myip_adr_c2 = "2"
    core.block_sw1, core.block_sw2, core.block_sw3 = 0, 1, 1      # idle after boot
    return core, plc


async def remote_start(csms):
    return (await csms.request("RemoteStartTransaction", {"connectorId": 2, "idTag": SECRET_TAG}))["status"]


def no_tag_logged(caplog):
    """CP.py and the router never log an idTag. (The ocpp library's own wire log, which the
    charger writes as before, is not part of this check.)"""
    return all(SECRET_TAG not in r.getMessage() for r in caplog.records if r.name in ("root", "dual_csms"))


def refusals(caplog):
    return [m for m in caplog.messages if "RemoteStart C2 rejected" in m]


@pytest.mark.asyncio
async def test_field_replay_2026_11_10_cable_removed_after_the_ev_ended_the_session(tmp_path, monkeypatch, caplog):
    monkeypatch.chdir(tmp_path)
    caplog.set_level(logging.INFO)
    async with FakeCSMS() as a, FakeCSMS() as b:
        core, plc = replay_core()
        async with running_manager(configuration(a, b), tmp_path / "dual", core=core) as (manager, _):
            core._dual_manager = manager
            plc["state"] = "Preparing"                                   # 10:37:37 plugged in
            await core._handle_status_notification_c2("Preparing", "Preparing")
            assert await remote_start(a) == "Accepted"                   # 10:37:49
            assert manager.reserved_age(2) is not None and manager.reserved_age(1) is None
            pulse(core, 2)                                               # 10:37:50 PLC start pulse
            # 10:38:07 the EV ends its DIN session; no transaction, no Finishing flag
            plc["state"] = "Available"                                   # 10:38:22 cable removed
            await core._handle_status_notification_c2("Available", "Available")
            assert latch(core, 2) == (0, 0, 1) and manager.reserved_age(2) is None
            plc["state"] = "Preparing"                                   # 10:40:04 plugged in again
            await core._handle_status_notification_c2("Preparing", "Preparing")
            assert await remote_start(a) == "Accepted"                   # 10:40:36 R3.1.5: Rejected
            assert latch(core, 2) == (1, 1, 1) and manager._live(2)["owner"] == "A"
            assert await remote_start(b) == "Rejected"                   # head 2 is A's session now
    assert core.statuses == ["Preparing", "Available", "Preparing"]
    assert "[START-LATCH] C2 released start_transaction_SW2=3, CSMS claim: cable removed before " \
           "StartTransaction" in caplog.messages
    assert refusals(caplog) == ["[B] RemoteStart C2 rejected: already claimed by A session 2 (reserved)"]
    assert no_tag_logged(caplog)


@pytest.mark.asyncio
async def test_field_replay_with_the_finishing_flag(tmp_path, monkeypatch, caplog):
    monkeypatch.chdir(tmp_path)
    caplog.set_level(logging.INFO)
    async with FakeCSMS() as a, FakeCSMS() as b:
        core, plc = replay_core()
        async with running_manager(configuration(a, b), tmp_path / "dual", core=core) as (manager, _):
            core._dual_manager = manager
            plc["state"] = "Preparing"
            await core._handle_status_notification_c2("Preparing", "Preparing")
            assert await remote_start(a) == "Accepted"
            pulse(core, 2)
            core.finshing_sent_c2 = plc["finishing"] = 1                 # the PLC ends the session
            await core._handle_finishing_c2()
            assert latch(core, 2) == (0, 0, 1) and manager.reserved_age(2) is None
            assert await remote_start(b) == "Rejected"                   # OCPP: unplug first
            plc["state"], core.finshing_sent_c2, plc["finishing"] = "Available", 0, 0
            await core._handle_status_notification_c2("Available", "Available")
            await core._handle_finishing_c2()
            plc["state"] = "Preparing"
            await core._handle_status_notification_c2("Preparing", "Preparing")
            assert await remote_start(b) == "Accepted"
            assert manager._live(2)["owner"] == "B"
    assert core.statuses == ["Preparing", "Finishing", "Available", "Preparing"]
    assert refusals(caplog) == ["[B] RemoteStart C2 rejected: previous session finishing - unplug the cable first"]
    assert no_tag_logged(caplog)


@pytest.mark.asyncio
async def test_latch_left_by_r315_on_disk_is_released_by_the_next_remote_start(tmp_path, monkeypatch, caplog):
    monkeypatch.chdir(tmp_path)
    caplog.set_level(logging.INFO)
    async with FakeCSMS() as a, FakeCSMS() as b:
        core, plc = replay_core()
        core.start_transaction_SW2 = 3          # R3.1.5 left it; the upgrade restarted CP.py
        plc["state"] = "Preparing"              # with the cable in, so boot reset kept it
        async with running_manager(configuration(a, b), tmp_path / "dual", core=core) as (manager, _):
            core._dual_manager = manager
            assert await remote_start(b) == "Accepted"
            assert manager._live(2)["owner"] == "B"
    assert refusals(caplog) == []
    assert "[START-LATCH] C2 released start_transaction_SW2=3: stale, start_transaction_SW2=3 left " \
           "after its RemoteStart session ended" in caplog.messages


@pytest.mark.asyncio
@pytest.mark.parametrize("state, after_timeout", [("Preparing", "Accepted"), ("Available", "Accepted"),
                                                  ("SuspendedEV", "Rejected"), ("Charging", "Rejected")])
async def test_start_that_never_reached_start_transaction_holds_the_head_only_for_the_timeout(
        state, after_timeout, tmp_path, monkeypatch, caplog):
    monkeypatch.chdir(tmp_path)
    caplog.set_level(logging.INFO)
    async with FakeCSMS() as a, FakeCSMS() as b:
        core, plc = replay_core()
        async with running_manager(configuration(a, b), tmp_path / "dual", core=core) as (manager, _):
            core._dual_manager = manager
            plc["state"] = "Preparing"
            await core._handle_status_notification_c2("Preparing", "Preparing")
            assert await remote_start(a) == "Accepted"
            pulse(core, 2)
            plc["state"] = state                # no StartTransaction, no Finishing, cable stays in
            assert await remote_start(b) == "Rejected"
            core._remote_start_latch_mono_c2 -= TIMEOUT + 20
            manager.db.execute("UPDATE sessions SET created = created - ? WHERE state = 'reserved'", (TIMEOUT + 20,))
            manager.db.commit()
            assert await remote_start(b) == after_timeout
            assert manager._live(2)["owner"] == ("B" if after_timeout == "Accepted" else "A")
    assert refusals(caplog)[0] == "[B] RemoteStart C2 rejected: already claimed by A session 1 (reserved)"
    released = [m for m in latch_logs(caplog) if "released" in m]
    if after_timeout == "Accepted":
        assert released == ["[START-LATCH] C2 released remote_start_received_c2=1, start_transaction_SW2=3, "
                            "CSMS claim: stale, RemoteStart sent to the PLC 200 s ago and no StartTransaction "
                            "(PLC %s)" % state]
    else:
        assert released == [] and len(refusals(caplog)) == 2
    assert no_tag_logged(caplog)


class BrokenHealCore(FakeCore):
    def _heal_stale_start_latch(self, connector_id):
        raise RuntimeError("state file unreadable")


@pytest.mark.asyncio
async def test_router_keeps_working_when_the_release_check_fails(tmp_path, caplog):
    caplog.set_level(logging.WARNING, logger="dual_csms")
    async with FakeCSMS() as a, FakeCSMS() as b:
        async with running_manager(configuration(a, b), tmp_path, core=BrokenHealCore()) as (manager, core):
            assert await a.request("RemoteStartTransaction", {"connectorId": 1, "idTag": "x"}) == {"status": "Accepted"}
    assert "[A] RemoteStart C1: stale start check failed (RuntimeError)" in caplog.messages
