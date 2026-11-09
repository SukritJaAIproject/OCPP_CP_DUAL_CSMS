"""The real final PLC pulse gate crosses a worker and real SQLite safely.

Only the PLC publishing backend is mocked. No CP.py startup or network starts.
"""
import ast
import asyncio
import threading
import time
from pathlib import Path
from unittest.mock import AsyncMock, Mock
from types import SimpleNamespace

import pytest

from helpf.dual_csms import DualCSMSManager
from helpf.ocpp_runtime import OCPPProtocolMixin


SOURCE = Path(__file__).resolve().parents[1] / "helpf/plc_function.py"


def pulse_environment(controller, loop, plc):
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "remote_start_plc_W")
    environment = {"asyncio": asyncio, "threading": threading, "_cp_instance": controller, "_main_loop": loop,
                   "_get_plc": lambda: plc}
    exec(compile(ast.fix_missing_locations(ast.Module(body=[node], type_ignores=[])), str(SOURCE), "exec"), environment)
    return environment


@pytest.fixture
async def real_gate(tmp_path):
    config = {"enabled": True, "local_csms": "A", "admin_csms": "A",
              "servers": [{"name": "A", "url": "ws://127.0.0.1:1/TEST-NO-CONNECTION"}]}
    manager = DualCSMSManager(config, tmp_path)
    class Core(OCPPProtocolMixin):
        def _active_transaction_id(self, connector):
            return 0
        async def _ocpp_apply_limits(self, connector, limits):
            return True
    core = Core()
    core._dual_manager = manager
    core._ocpp_state_dir = tmp_path
    core.call = AsyncMock()
    runtime = core._ocpp_protocol()
    await runtime.bootstrap()
    threads = []
    original = core._ocpp_hardware_start_allowed
    def observed(connector):
        threads.append(threading.get_ident())
        return original(connector)
    core._ocpp_hardware_start_allowed = observed
    publications = []
    plc = SimpleNamespace(remote_start=Mock(side_effect=lambda *args: publications.append(threading.get_ident()) or True))
    loop = asyncio.get_running_loop()
    env = pulse_environment(core, loop, plc)
    try:
        yield SimpleNamespace(core=core, manager=manager, runtime=runtime, threads=threads,
                              publications=publications, plc=plc, loop=loop, env=env)
    finally:
        await runtime.close()
        manager.db.close()


async def test_worker_publish_uses_main_loop_admission_with_actual_sqlite(real_gate):
    main_thread = threading.get_ident()
    result = await asyncio.to_thread(real_gate.env["remote_start_plc_W"], "1", 1)
    assert result == "Remote Start Successful"
    assert real_gate.threads == [main_thread]
    assert len(real_gate.publications) == 1 and real_gate.publications[0] != main_thread
    real_gate.plc.remote_start.assert_called_once_with("1", 1)


async def test_worker_gate_denies_inoperative_connector_before_publish(real_gate):
    await real_gate.runtime.services.change_availability("A", 2, "Inoperative")
    assert "Failed" in await asyncio.to_thread(real_gate.env["remote_start_plc_W"], "2", 1)
    assert real_gate.threads == [threading.get_ident()]
    real_gate.plc.remote_start.assert_not_called()


async def test_same_loop_gate_does_not_wait_on_itself(real_gate, monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("Same-loop admission must not enqueue and wait on itself")
    monkeypatch.setattr(asyncio, "run_coroutine_threadsafe", forbidden)
    assert real_gate.env["remote_start_plc_W"]("1", 1) == "Remote Start Successful"
    assert real_gate.threads == [threading.get_ident()]


@pytest.mark.parametrize("binding", ["missing", "closed"])
async def test_missing_or_closed_loop_is_fail_closed_but_zero_reset_is_allowed(real_gate, binding):
    closed = asyncio.new_event_loop()
    closed.close()
    real_gate.env["_main_loop"] = None if binding == "missing" else closed
    assert "Failed" in await asyncio.to_thread(real_gate.env["remote_start_plc_W"], "1", 1)
    real_gate.plc.remote_start.assert_not_called()
    assert await asyncio.to_thread(real_gate.env["remote_start_plc_W"], "1", 0) == "Remote Start Successful"
    real_gate.plc.remote_start.assert_called_once_with("1", 0)
    assert real_gate.threads == []


async def test_absent_controller_cannot_publish_a_high_start_pulse(real_gate):
    real_gate.env["_cp_instance"] = None
    assert "Failed" in await asyncio.to_thread(real_gate.env["remote_start_plc_W"], "1", 1)
    real_gate.plc.remote_start.assert_not_called()


async def test_gate_exception_fails_closed_without_publishing(real_gate):
    real_gate.core._ocpp_hardware_start_allowed = Mock(side_effect=RuntimeError("admission fault"))
    assert "Failed" in await asyncio.to_thread(real_gate.env["remote_start_plc_W"], "1", 1)
    real_gate.core._ocpp_hardware_start_allowed.assert_called_once_with(1)
    real_gate.plc.remote_start.assert_not_called()


async def test_rebound_controller_cannot_use_old_gate_approval(real_gate, monkeypatch):
    original = asyncio.run_coroutine_threadsafe
    def rebind(coroutine, loop):
        real_gate.env["_cp_instance"] = SimpleNamespace(_ocpp_hardware_start_allowed=lambda cid: True)
        return original(coroutine, loop)
    monkeypatch.setattr(asyncio, "run_coroutine_threadsafe", rebind)
    assert "Failed" in await asyncio.to_thread(real_gate.env["remote_start_plc_W"], "1", 1)
    real_gate.plc.remote_start.assert_not_called()
    assert real_gate.threads == []


async def test_stalled_loop_times_out_and_never_publishes_late(real_gate, monkeypatch):
    # Deliberately stall this loop after the worker enqueues admission. The real
    # two-second timeout must deny the pulse and cancel the queued decision.
    queued = threading.Event()
    completed = []
    original = asyncio.run_coroutine_threadsafe
    def observed(coroutine, loop):
        future = original(coroutine, loop)
        queued.set()
        return future
    monkeypatch.setattr(asyncio, "run_coroutine_threadsafe", observed)
    worker = threading.Thread(target=lambda: completed.append(real_gate.env["remote_start_plc_W"]("1", 1)))
    worker.start()
    assert queued.wait(1)
    time.sleep(2.1)
    worker.join(timeout=1)
    assert not worker.is_alive()
    assert completed and "Failed" in completed[0]
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    real_gate.plc.remote_start.assert_not_called()
    assert real_gate.threads == []
