"""Exercise only the MQTT callback AST; never import hardware-facing modules."""

import ast
import asyncio
import datetime
import inspect
import json
import logging
from pathlib import Path
from types import SimpleNamespace


SOURCE = Path(__file__).resolve().parents[1] / "helpf" / "plc_function.py"


def load_callback(core, **overrides):
    tree = ast.parse(SOURCE.read_text(encoding="utf-8-sig"))
    function = next(
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "handle_rfid_command"
    )

    def forbidden(*args, **kwargs):
        raise AssertionError("Unexpected legacy file, worker, or hardware operation")

    namespace = {
        "datetime": datetime.datetime,
        "pytz": SimpleNamespace(timezone=lambda _: datetime.timezone.utc),
        "json": json,
        "logger": logging.getLogger("rfid-bridge-test"),
        "asyncio": asyncio,
        "_cp_instance": core,
        "_main_loop": None,
        "_write_file": forbidden,
        "threading": SimpleNamespace(Thread=forbidden),
        "remote_start_plc": forbidden,
        "_send_remote_start": forbidden,
        "_send_remote_stop": forbidden,
    }
    namespace.update(overrides)
    exec(compile(ast.Module(body=[function], type_ignores=[]), str(SOURCE), "exec"), namespace)
    return namespace


def start_payload(head=1):
    return json.dumps({"head": head, "idTag": "new-tag", "action": "start"})


def test_dual_callback_schedules_once_without_overwriting_remote_identity():
    events = []

    async def start(head, tag):
        events.append((head, tag))

    core = SimpleNamespace(
        _dual_manager=object(), _dual_rfid_command_start=start,
        myid_tag_c1="remote-owner-tag", myid_tag_ath_c1="local-original-tag",
    )
    ns = load_callback(core)

    async def run():
        ns["_main_loop"] = asyncio.get_running_loop()
        ns["handle_rfid_command"](start_payload())
        assert events == []  # The MQTT callback returns before the coroutine runs.
        assert core.myid_tag_c1 == "remote-owner-tag"
        assert core.myid_tag_ath_c1 == "local-original-tag"
        for _ in range(3):
            await asyncio.sleep(0)
        assert events == [(1, "new-tag")]

    asyncio.run(run())


def test_delegated_failure_is_logged_without_legacy_fallback(caplog):
    async def start(head, tag):
        raise RuntimeError("connector claim refused")

    core = SimpleNamespace(_dual_manager=object(), _dual_rfid_command_start=start)
    ns = load_callback(core)

    async def run():
        ns["_main_loop"] = asyncio.get_running_loop()
        ns["handle_rfid_command"](start_payload(2))
        for _ in range(4):
            await asyncio.sleep(0)

    with caplog.at_level(logging.ERROR, logger="rfid-bridge-test"):
        asyncio.run(run())
    assert "Dual-CSMS start failed head=2" in caplog.text
    assert "connector claim refused" in caplog.text


def test_scheduling_failure_closes_unscheduled_coroutine(caplog):
    captured = []

    async def start(head, tag):
        raise AssertionError("Unscheduled coroutine must never execute")

    def schedule(command, loop):
        captured.append(command)
        raise RuntimeError("event loop is closed")

    core = SimpleNamespace(_dual_manager=object(), _dual_rfid_command_start=start)
    ns = load_callback(
        core, _main_loop=object(),
        asyncio=SimpleNamespace(run_coroutine_threadsafe=schedule),
    )
    with caplog.at_level(logging.ERROR, logger="rfid-bridge-test"):
        ns["handle_rfid_command"](start_payload())
    assert len(captured) == 1
    assert inspect.getcoroutinestate(captured[0]) == inspect.CORO_CLOSED
    assert "Cannot schedule dual-CSMS start head=1" in caplog.text


def test_single_csms_keeps_legacy_prestore_and_worker_path():
    writes, workers = [], []

    class Worker:
        def __init__(self, *, target, daemon):
            assert callable(target)
            assert daemon is True

        def start(self):
            workers.append("started")  # Do not execute hardware-facing worker.

    core = SimpleNamespace(_dual_manager=None)
    ns = load_callback(
        core, _main_loop=object(),
        _write_file=lambda name, value: writes.append((name, value)),
        threading=SimpleNamespace(Thread=Worker),
    )
    ns["handle_rfid_command"](start_payload(2))
    assert core.myid_tag_c2 == core.myid_tag_ath_c2 == "new-tag"
    assert writes == [("myid_tag_c2.txt", "new-tag"), ("myid_tag_ath_c2.txt", "new-tag")]
    assert workers == ["started"]


def test_dual_mode_does_not_route_stop_through_start_coroutine():
    workers, writes = [], []

    def forbidden_start(*args):
        raise AssertionError("Stop must not call the new start bridge")

    class Worker:
        def __init__(self, *, target, daemon):
            assert callable(target)

        def start(self):
            workers.append("stop worker")

    core = SimpleNamespace(
        _dual_manager=object(), _dual_rfid_command_start=forbidden_start,
        accepted_tag1="owner-tag",
    )
    ns = load_callback(
        core, _main_loop=object(),
        _write_file=lambda name, value: writes.append((name, value)),
        threading=SimpleNamespace(Thread=Worker),
    )
    ns["handle_rfid_command"](json.dumps({"head": 1, "idTag": "owner-tag", "action": "stop"}))
    assert core.my_remote_stop_c1 == 1
    assert workers == ["stop worker"]
    assert ("my_remote_stop_c1.txt", 1) in writes
