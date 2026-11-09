"""R3.1.1: every refused RemoteStart names its reason in the log; behaviour is unchanged.

Found on the factory Edgebox on 2026-11-09: a RemoteStart from A was answered
Rejected and nothing in the log said why. These checks pin the reason texts, that
no idTag is logged, and that CP.py's guard keeps R3.1's conditions and order.
"""
import ast
import logging
from pathlib import Path
from types import SimpleNamespace

import pytest

from helpf.ocpp_runtime import OCPPProtocolRuntime
from test_dual_csms_integration import FakeCSMS, FakeCore, configuration, running_manager

SOURCE = Path(__file__).resolve().parents[1] / "CP.py"
SECRET_TAG = "SECRET-TAG-77"  # <= 20 chars (CiString20)


def refusals(caplog):
    return [r.getMessage() for r in caplog.records if "RemoteStart C" in r.getMessage()]


class ExplainingCore(FakeCore):
    def __init__(self):
        super().__init__()
        self.reason = ""

    def _dual_connector_busy(self, connector_id):
        return bool(self.reason) or super()._dual_connector_busy(connector_id)

    def _dual_busy_reason(self, connector_id):
        return self.reason


@pytest.mark.asyncio
async def test_router_names_claim_busy_and_startup_reasons_without_tags(tmp_path, caplog):
    caplog.set_level(logging.WARNING, logger="dual_csms")
    async with FakeCSMS() as a, FakeCSMS() as b:
        core = ExplainingCore()
        async with running_manager(configuration(a, b), tmp_path, core=core) as (manager, _):
            payload = {"connectorId": 1, "idTag": SECRET_TAG}
            assert await a.request("RemoteStartTransaction", payload) == {"status": "Accepted"}
            assert await b.request("RemoteStartTransaction", payload) == {"status": "Rejected"}
            assert any(m.startswith("[B] RemoteStart C1 rejected: already claimed by A session")
                       and "(reserved)" in m for m in refusals(caplog))

            core.reason = "previous session finishing - unplug the cable first"
            assert await a.request("RemoteStartTransaction", {"connectorId": 2, "idTag": SECRET_TAG}) == {"status": "Rejected"}
            assert "[A] RemoteStart C2 rejected: previous session finishing - unplug the cable first" in refusals(caplog)

            core.reason = ""
            core._dual_ready = False
            assert await a.request("RemoteStartTransaction", {"connectorId": 2, "idTag": SECRET_TAG}) == {"status": "Rejected"}
            assert "[A] RemoteStart C2 rejected: charger startup not finished" in refusals(caplog)
    assert all(SECRET_TAG not in r.getMessage() for r in caplog.records)


@pytest.mark.asyncio
async def test_router_reports_plain_busy_for_a_core_without_reasons(tmp_path, caplog):
    caplog.set_level(logging.WARNING, logger="dual_csms")
    async with FakeCSMS() as a, FakeCSMS() as b:
        core = FakeCore()
        core.pending.add(1)   # FakeCore treats a pending start as busy
        async with running_manager(configuration(a, b), tmp_path, core=core) as (manager, _):
            assert await a.request("RemoteStartTransaction", {"connectorId": 1, "idTag": "x"}) == {"status": "Rejected"}
    assert "[A] RemoteStart C1 rejected: connector busy" in refusals(caplog)


def runtime(**overrides):
    services = SimpleNamespace(_operative=lambda cid: True, physical_status=lambda cid: "Preparing",
                               _reservations=lambda: [])
    fake = SimpleNamespace(ready=True, faulted=set(), smart=SimpleNamespace(inhibited=False, last_error=None),
                           services=services)
    for key, value in overrides.items():
        setattr(fake, key, value)
    return fake


@pytest.mark.parametrize("fake, expected", [
    (runtime(ready=False), "protocol runtime not ready"),
    (runtime(faulted={1}), "connector faulted after a PLC limit failure"),
    (runtime(smart=SimpleNamespace(inhibited=True, last_error="RuntimeError")),
     "smart charging inhibited (RuntimeError)"),
    (runtime(services=SimpleNamespace(_operative=lambda cid: False, physical_status=lambda cid: "Available",
                                      _reservations=lambda: [])), "connector Inoperative"),
    (runtime(services=SimpleNamespace(_operative=lambda cid: True, physical_status=lambda cid: "Faulted",
                                      _reservations=lambda: [])), "connector status Faulted"),
    (runtime(services=SimpleNamespace(_operative=lambda cid: True, physical_status=lambda cid: "Available",
                                      _reservations=lambda: [{"connector_id": 0}])),
     "reserved for another idTag or CSMS"),
    (runtime(), "start not allowed"),
])
def test_runtime_start_refusal_texts(fake, expected):
    assert OCPPProtocolRuntime.start_refusal(fake, 1) == expected


def cp_guard(plc_state="Preparing", local_request=0.0):
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "ChargePoint")
    names = ("_dual_connector_busy", "_dual_busy_reason", "_valid_id_tag")
    nodes = [n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name in names]
    assert len(nodes) == len(names)
    calls = []

    def take_local_request(kind, cid):
        calls.append(("take_local_request", kind, cid))
        return local_request

    def get_connector_status(cid):
        calls.append(("plc", cid))
        if plc_state is None:
            raise TimeoutError("no PLC")
        return plc_state, plc_state

    module = ast.fix_missing_locations(ast.Module(body=[ast.ClassDef(
        name="Core", bases=[], keywords=[], body=nodes, decorator_list=[])], type_ignores=[]))
    env = {"take_local_request": take_local_request, "staticmethod": staticmethod,
           "plc": SimpleNamespace(get_connector_status=get_connector_status)}
    exec(compile(module, str(SOURCE), "exec"), env)
    core = env["Core"]()
    core._dual_ready = True
    core._dual_aborting, core._dual_local_claims = set(), set()
    core._protocol_runtime = SimpleNamespace(faulted=set(), smart=SimpleNamespace(inhibited=False, last_error=None),
                                             services=SimpleNamespace(_operative=lambda cid: True))
    core.txn = 0
    core.finishing = core.local_session = core.stop_worker = False
    core._active_transaction_id = lambda cid: core.txn
    core._remote_stop_worker_running = lambda cid: core.stop_worker
    core._session_finishing_active = lambda cid: core.finishing
    core._local_status_session_active = lambda cid: core.local_session
    for cid in (1, 2):
        for name in ("remote_start_received", "myremotestart", "rfid_start_received", "rfid_recovery_pending"):
            setattr(core, f"{name}_c{cid}", 0)
        setattr(core, f"start_transaction_SW{cid}", 1)
        setattr(core, f"accepted_tag{cid}", "0")
    return core, calls


@pytest.mark.parametrize("setup, expected", [
    (lambda c: setattr(c, "_dual_ready", False), "charger startup not finished"),
    (lambda c: c._dual_local_claims.add(1), "local RFID start in progress"),
    (lambda c: setattr(c, "txn", 41), "transaction 41 is active"),
    (lambda c: setattr(c, "stop_worker", True), "remote stop still running"),
    (lambda c: setattr(c, "finishing", True), "previous session finishing - unplug the cable first"),
    (lambda c: setattr(c, "local_session", True), "session started from the charger screen"),
    (lambda c: setattr(c, "remote_start_received_c1", 1), "remote_start_received_c1=1"),
    (lambda c: setattr(c, "start_transaction_SW1", 3), "start already sent to the PLC (start_transaction_SW1=3)"),
    (lambda c: setattr(c, "accepted_tag1", "CARD"), "an accepted RFID tag is waiting for its start"),
    (lambda c: c._protocol_runtime.faulted.add(1), "connector faulted after a PLC limit failure"),
])
def test_cp_guard_reasons_stop_before_the_hardware_read(setup, expected):
    core, calls = cp_guard()
    setup(core)
    assert core._dual_busy_reason(1) == expected
    assert core._dual_connector_busy(1) is True
    assert calls == []          # like R3.1: nothing after the first failed check runs
    assert "CARD" not in expected


@pytest.mark.parametrize("plc_state, local_request, expected, busy", [
    ("Available", 0.0, "", False),
    ("Preparing", 0.0, "", False),
    ("Finishing", 0.0, "PLC reports Finishing", True),
    # R3.1.2: an EV waiting for authorisation (state C, no CurrentDemand) is not busy
    ("SuspendedEV", 0.0, "", False),
    ("SuspendedEVSE", 0.0, "", False),
    ("Charging", 0.0, "PLC reports Charging", True),
    ("Faulted", 0.0, "PLC reports Faulted", True),
    (None, 0.0, "PLC state unreadable (TimeoutError)", True),
    ("Preparing", 12.5, "start request from the charger screen pending", True),
])
def test_cp_guard_plc_and_screen_reasons(plc_state, local_request, expected, busy):
    core, calls = cp_guard(plc_state, local_request)
    assert core._dual_busy_reason(1) == expected
    assert core._dual_connector_busy(1) is busy
    assert calls[0] == ("take_local_request", "start", 1)
    assert (("plc", "1") in calls) is (local_request <= 0)
