"""R3.1.2: R3's service layer must not hide a Vector fault, and start admission must see it.

Found on the factory Edgebox on 2026-11-09 15:15: the Vector bridge sent Faulted for
connectors 1 and 2 (active PM faults) and, a moment later, the R3 startup announcement sent
Available for both, so A and B showed free connectors while the faults were still active.
"""
import ast
import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest
from ocpp.v16 import call, call_result

from helpf import ocpp_runtime
from helpf.ocpp_runtime import OCPPProtocolRuntime, service_status_active

SOURCE = Path(__file__).resolve().parents[1] / "CP.py"
NAMES = ("_ocpp_vector_fault_active", "_ocpp_hides_service_status", "_ocpp_physical_status",
         "_vector_fault_blocks_normal_status")


def core(active=(), enabled=True, raw="Available", transaction=0):
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "ChargePoint")
    nodes = [n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name in NAMES]
    assert sorted(n.name for n in nodes) == sorted(NAMES)
    module = ast.fix_missing_locations(ast.Module(body=[ast.ClassDef(
        name="Core", bases=[], keywords=[], body=nodes, decorator_list=[])], type_ignores=[]))
    env = {"VECTOR_FAULT_BRIDGE_ENABLED": enabled, "service_status_active": service_status_active}
    exec(compile(module, str(SOURCE), "exec"), env)
    instance = env["Core"]()
    instance.vector_fault_bridge = SimpleNamespace(has_active=lambda cid: cid in active)
    instance.suppressed = []
    instance._log_vector_fault_suppression = lambda cid, status: instance.suppressed.append((cid, status))
    instance.iCpStateCss1 = instance.iCpStateCss2 = raw
    instance._active_transaction_id = lambda cid: transaction
    return instance


def status(connector, value="Available"):
    return call.StatusNotification(connector_id=connector, error_code="NoError", status=value)


def test_physical_status_is_faulted_while_vector_fault_is_active():
    assert core(active={1})._ocpp_physical_status(1) == "Faulted"
    assert core(active={0})._ocpp_physical_status(0) == "Faulted"
    assert core(active={1})._ocpp_physical_status(2) == "Available"
    # unchanged R3 mapping when no Vector fault is active
    assert core(raw="SuspendedEV")._ocpp_physical_status(1) == "Preparing"
    assert core(raw="SuspendedEV", transaction=7)._ocpp_physical_status(1) == "SuspendedEV"
    assert core(active={1}, enabled=False)._ocpp_physical_status(1) == "Available"


def test_physical_status_falls_back_when_the_bridge_is_not_ready():
    instance = core()
    del instance.vector_fault_bridge
    assert instance._ocpp_physical_status(1) == "Available"


def test_service_status_is_hidden_only_for_a_vector_faulted_connector():
    instance = core(active={1})
    token = ocpp_runtime._SERVICE_STATUS.set(True)
    try:
        assert instance._ocpp_hides_service_status(status(1)) is True
        assert instance._ocpp_hides_service_status(status(2)) is False
        assert instance._ocpp_hides_service_status(call.Heartbeat()) is False
    finally:
        ocpp_runtime._SERVICE_STATUS.reset(token)
    # statuses from the normal CP.py path keep their own Vector gate
    assert instance._ocpp_hides_service_status(status(1)) is False
    assert instance.suppressed == [(1, "service Available")]


def test_publish_status_marks_its_calls_as_service_statuses():
    seen = []

    class Core:
        async def call(self, payload):
            seen.append((type(payload).__name__, payload.status, service_status_active()))
            return call_result.StatusNotification()

    fake = SimpleNamespace(core=Core(), faulted=set())
    asyncio.run(OCPPProtocolRuntime.publish_status(fake, 1, "Available"))
    assert seen == [("StatusNotification", "Available", True)]
    assert service_status_active() is False


@pytest.mark.parametrize("physical, expected", [("Faulted", "connector status Faulted"),
                                                ("Preparing", "start not allowed")])
def test_start_refusal_names_a_vector_fault(physical, expected):
    services = SimpleNamespace(_operative=lambda cid: True, physical_status=lambda cid: physical,
                               _reservations=lambda: [])
    fake = SimpleNamespace(ready=True, faulted=set(), smart=SimpleNamespace(inhibited=False, last_error=None),
                           services=services)
    assert OCPPProtocolRuntime.start_refusal(fake, 1) == expected


def test_chargepoint_call_drops_hidden_service_status_before_the_router():
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "ChargePoint")
    method = next(n for n in cls.body if isinstance(n, ast.AsyncFunctionDef) and n.name == "call")
    first = method.body[1]
    assert isinstance(first, ast.If)                       # if runtime is not None:
    guard = first.body[0]
    assert isinstance(guard, ast.If) and "_ocpp_hides_service_status" in ast.unparse(guard.test)
    assert "call_result.StatusNotification()" in ast.unparse(guard.body[0])
    assert "filter_status" in ast.unparse(first.body[1])  # the filter still runs for kept statuses
