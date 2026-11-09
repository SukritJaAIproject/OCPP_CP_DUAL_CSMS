"""Test-only protocol journal and simulated local events. Never loaded by CP.py."""
import asyncio
import contextlib
import dataclasses
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from ocpp.v16 import call
from helpf import dual_csms


def now():
    return datetime.now(timezone.utc).isoformat()


def redact(value):
    if isinstance(value, dict):
        field = str(value.get("key", "")).lower()
        if (field.endswith("pin") or "pincode" in field
                or any(word in field for word in ("password", "token", "secret", "authorizationkey"))):
            value = {**value, **({"value": "[redacted]"} if "value" in value else {})}
        return {key: ("[redacted]" if key.lower().replace("_", "").endswith("idtag")
                or any(part in key.lower() for part in ("password", "authorization", "token", "certificate")) else redact(item))
                for key, item in value.items()}
    if isinstance(value, list):
        return [redact(item) for item in value]
    if isinstance(value, str) and "://" in value:
        # Diagnostics/firmware destinations can contain temporary bearer tokens
        # in either the path, query or user-info. The journal needs no URL body.
        return "[redacted URL]"
    if isinstance(value, str) and value.lstrip().startswith(("{", "[")):
        try:
            return json.dumps(redact(json.loads(value)), ensure_ascii=False)
        except (ValueError, TypeError):
            pass
    if isinstance(value, str) and len(value) > 2048:
        return value[:2048] + "[truncated]"
    return value


@contextlib.contextmanager
def protocol_trace(path):
    if path is None:
        yield
        return
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    pending = {}
    original = dual_csms._Peer

    def record(server, direction, raw):
        try:
            frame = json.loads(raw)
            kind, uid = frame[:2]
            if kind == 2:
                action, body = frame[2:4]
                pending[(server, direction, uid)] = action
            else:
                opposite = "CP_TO_CSMS" if direction == "CSMS_TO_CP" else "CSMS_TO_CP"
                action = pending.get((server, opposite, uid), "uncorrelated")
                body = frame[2] if kind == 3 else {"errorCode": frame[2], "description": frame[3], "details": frame[4]}
            event = {"timestamp": now(), "server": server, "direction": direction,
                     "message_type": kind, "message_id": uid, "action": action, "payload": redact(body)}
            with path.open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(event, ensure_ascii=False) + "\n")
        except Exception:
            # A failed observation must not turn a successful protocol exchange into a retry.
            import logging
            logging.getLogger("simulator_trace").exception("Could not journal protocol frame")

    class ObservedPeer(original):
        async def route_message(self, raw_msg):
            record(self.name, "CSMS_TO_CP", raw_msg)
            return await super().route_message(raw_msg)

        async def _send(self, message):
            record(self.name, "CP_TO_CSMS", message)
            return await super()._send(message)

    dual_csms._Peer = ObservedPeer
    try:
        yield
    finally:
        dual_csms._Peer = original


async def execute_control(core, request):
    """Only local simulator events/probes; RemoteStart still comes from a CSMS."""
    action = request["action"]
    manager = core.manager
    if action == "authorize":
        server = request.get("server", manager.config["local_csms"])
        if server not in manager.servers:
            raise ValueError("Unknown test CSMS")
        payload = call.Authorize(id_tag=request["id_tag"])
        response = (await manager.call(payload) if server == manager.config["local_csms"]
                    else await manager._request(server, payload))
        return {"server": server, "scope": "local_route" if server == manager.config["local_csms"] else "direct_peer_protocol_probe",
                "response": dataclasses.asdict(response)}
    if action == "data_transfer":
        response = await manager.call(call.DataTransfer(vendor_id=request.get("vendor_id", "LocalTest"),
            message_id=request.get("message_id", "OCPP_FUNCTION_TEST"), data=json.dumps(request.get("data", {}))))
        return {"response": dataclasses.asdict(response)}
    if action == "status":
        connector = int(request["connector"])
        if connector not in (1, 2):
            raise ValueError("Use simulator connector 1 or 2")
        from ocpp.v16.enums import ChargePointStatus, ChargePointErrorCode
        ChargePointStatus(request["status"])
        error = request.get("error_code", "NoError")
        ChargePointErrorCode(error)
        await core.status(connector, request["status"], error_code=error)
        return {"connector": connector, "status": request["status"]}
    if action == "local_stop":
        connector = int(request["connector"])
        from ocpp.v16.enums import Reason
        reason = request.get("reason", "Local")
        Reason(reason)
        if connector not in (1, 2) or core._active_transaction_id(connector) <= 0:
            raise ValueError("No simulator transaction to stop")
        core.stopping.add(connector)
        await core.stop_transaction(connector, reason)
        return {"connector": connector, "reason": reason, "stop_saved": True}
    if action == "disconnect":
        server = request["server"]
        peer = manager.peers.get(server)
        if peer is None:
            raise ValueError("Test peer is already disconnected")
        await peer._connection.close(code=1000, reason="Controlled simulator reconnect test")
        for _ in range(100):
            if not manager.ready[server].is_set():
                break
            await asyncio.sleep(.01)
        return {"server": server, "ready": manager.ready[server].is_set()}
    if action == "query":
        return {"ready": {name: event.is_set() for name, event in manager.ready.items()},
                "heads": {cid: {key: value for key, value in row.items() if key != "id_tag"}
                          for cid, row in core.heads.items()}}
    raise ValueError("Unsupported simulator control; remote commands must be sent from CSMS")


async def control_loop(core, directory):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    while True:
        for source in sorted(directory.glob("*.request.json")):
            claimed = source.with_name(source.name.replace(".request.json", ".claimed.json"))
            os.replace(source, claimed)
            destination = source.with_name(source.name.replace(".request.json", ".result.json"))
            try:
                request = json.loads(claimed.read_text(encoding="utf-8-sig"))
                result = {"timestamp": now(), "action": request.get("action"), "ok": True,
                          "result": redact(await execute_control(core, request))}
            except asyncio.CancelledError:
                raise  # Claimed file remains evidence; never automatically replay it.
            except Exception as error:
                result = {"timestamp": now(), "ok": False, "error_type": type(error).__name__, "error": str(error)[:300]}
            temporary = destination.with_suffix(".tmp")
            temporary.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
            os.replace(temporary, destination)
        await asyncio.sleep(.15)
