import ast
import asyncio
import logging
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from helpf.dual_csms import BackendUnavailable


async def test_unsent_offline_v2g_record_does_not_spend_attempts():
    path = Path(__file__).resolve().parents[1] / "helpf" / "v2g_datatransfer.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    method = next(node for node in ast.walk(tree)
                  if isinstance(node, ast.AsyncFunctionDef) and node.name == "send_loop")
    cls = ast.ClassDef(name="Bridge", bases=[], keywords=[], body=[method], decorator_list=[])
    module = ast.fix_missing_locations(ast.Module(body=[cls], type_ignores=[]))

    async def stop_on_defer(seconds):
        assert seconds == 5
        raise asyncio.CancelledError

    async def inline(func, *args):
        return func(*args)

    shim = SimpleNamespace(wait_for=asyncio.wait_for, to_thread=inline, sleep=stop_on_defer,
                           TimeoutError=asyncio.TimeoutError, CancelledError=asyncio.CancelledError)
    env = {"asyncio": shim, "LOG": logging.getLogger("test-v2g")}
    exec(compile(module, str(path), "exec"), env)
    bridge = env["Bridge"]()
    bridge.next_record = lambda: dict(vendor_id="EDS", message_id="V2G", data="{}",
                                     send_timeout=1, send_interval=0)
    bridge.finish = Mock()
    core = SimpleNamespace(_v2g_boot_accepted=True, _dual_manager=object(),
                           call=AsyncMock(side_effect=BackendUnavailable("B offline")))
    with pytest.raises(asyncio.CancelledError):
        await bridge.send_loop(core)
    core.call.assert_awaited_once()
    bridge.finish.assert_not_called()
