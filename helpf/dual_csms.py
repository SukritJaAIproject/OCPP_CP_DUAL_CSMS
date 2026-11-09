"""Two independent OCPP 1.6 connections sharing one physical charge controller.

The hardware controller uses local transaction IDs.  Only this module knows the
remote IDs and their owning CSMS.  Stop/Meter acknowledgements to the controller
mean "saved in the durable outbox", not "acknowledged by the remote CSMS".
"""
from __future__ import annotations

import asyncio
import base64
import copy
import dataclasses
import hashlib
import inspect
import json
import logging
import os
import sqlite3
import ssl
import time
import uuid
from datetime import datetime, timezone
from contextvars import ContextVar
from pathlib import Path
from urllib.parse import urlparse

import websockets
from ocpp.routing import on
from ocpp.exceptions import OCPPError, SecurityError
from ocpp.v16 import ChargePoint as OcppChargePoint
from ocpp.v16 import call, call_result
from ocpp.v16.enums import Action

LOG = logging.getLogger("dual_csms")
_COMMAND_ORIGIN = ContextVar("ocpp_command_origin", default=None)


def command_origin(default=None):
    """The requesting peer, scoped to this async command and its child tasks."""
    return _COMMAND_ORIGIN.get() or default


class DualCSMSError(RuntimeError):
    retryable = False


class StartOutcomeUnknown(DualCSMSError):
    """A Start may have reached its server. Never blindly send another Start."""


class BackendUnavailable(DualCSMSError):
    # A message wasn't transmitted: an existing durable producer may defer it
    # without spending its server-response retry budget.
    defer_without_attempt = True


class StartPolicyRejected(DualCSMSError):
    """A start was rejected locally before any StartTransaction was sent."""


def _now():
    return datetime.now(timezone.utc).isoformat()


def _json(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _status(value):
    return getattr(value, "value", value)


def check_single_csms_fallback(state_dir):
    """Do not reinterpret persisted local IDs as remote IDs after disabling dual."""
    root = Path(state_dir)
    database = root / "dual_csms_state.sqlite3"
    if not database.exists():
        return
    with sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True) as db:
        pending = db.execute(
            "SELECT 1 FROM sessions WHERE state NOT IN ('closed','cancelled') "
            "UNION SELECT 1 FROM outbox WHERE state!='delivered' LIMIT 1").fetchone()
    if pending:
        raise DualCSMSError("Cannot disable dual CSMS while sessions or outbound evidence are pending")
    for cid in (1, 2):
        file = root / f"translogs{cid}.txt"
        if file.exists() and int(file.read_text().strip() or "0") > 0:
            raise DualCSMSError("Cannot enter single-CSMS mode with a persisted dual local ID")


def validate_config(config):
    """Validate without connecting or importing any hardware modules."""
    result = copy.deepcopy(config)
    if not isinstance(result, dict):
        raise ValueError("Dual CSMS configuration must be a JSON object")
    servers = result.get("servers", [])
    if not isinstance(servers, list):
        raise ValueError("servers must be a list")
    active = []
    seen = set()
    seen_urls = set()
    for item in servers:
        if not isinstance(item, dict) or not isinstance(item.get("name"), str):
            raise ValueError("Every server needs a string name")
        name = item["name"]
        if not name or name in seen:
            raise ValueError("Server names must be nonempty and unique")
        seen.add(name)
        if not item.get("enabled", True):
            continue
        url = urlparse(item.get("url", ""))
        if url.scheme not in ("ws", "wss") or not url.hostname:
            raise ValueError("Enabled server %s needs a ws:// or wss:// URL" % name)
        if url.username or url.password or url.fragment:
            raise ValueError("Put credentials in environment variables, not the URL")
        normalized_url = url._replace(scheme=url.scheme.lower(), netloc=url.netloc.lower()).geturl()
        if normalized_url in seen_urls:
            raise ValueError("CSMS URLs must differ; duplicate station connections are not allowed")
        seen_urls.add(normalized_url)
        if bool(item.get("username_env")) != bool(item.get("password_env")):
            raise ValueError("username_env and password_env must be configured together")
        if bool(item.get("client_cert")) != bool(item.get("client_key")):
            raise ValueError("client_cert and client_key must be configured together")
        if url.scheme == "ws" and any(item.get(k) for k in
                ("username_env", "password_env", "client_cert", "client_key")):
            raise ValueError("Credentials require wss://")
        active.append(name)
    # [R3.1.3] admin_csms may name one CSMS ("A") or several (["A", "B"]); every admin
    # may send the administrative commands. The first admin receives notifications that
    # have no requesting CSMS (for example after a restart).
    admins = result.get("admin_csms", "A")
    admins = [admins] if isinstance(admins, str) else admins
    if (not isinstance(admins, list) or not admins
            or not all(isinstance(name, str) and name for name in admins)
            or len(set(admins)) != len(admins)):
        raise ValueError("admin_csms must be a CSMS name or a list of different CSMS names")
    if result.get("enabled", True):
        if not 1 <= len(active) <= 2:
            raise ValueError("Enable one or two CSMS connections")
        if result.get("local_csms") not in active:
            raise ValueError("local_csms must name an enabled CSMS")
        if any(name not in active for name in admins):
            raise ValueError("admin_csms must name an enabled CSMS")
    result["admins"] = list(admins)
    result["primary_admin"] = admins[0]
    for key, default, minimum in (("request_timeout", 15, .2),
                                   ("reconnect_seconds", 5, .05)):
        number = float(result.get(key, default))
        if not minimum <= number <= 300:
            raise ValueError("Invalid %s" % key)
        result[key] = number
    result["meter_queue_limit"] = int(result.get("meter_queue_limit", 2000))
    if not 1 <= result["meter_queue_limit"] <= 100000:
        raise ValueError("Invalid meter_queue_limit")
    return result


class _Peer(OcppChargePoint):
    def __init__(self, name, ws, manager):
        super().__init__(name, ws, response_timeout=manager.config["request_timeout"])
        self.manager = manager
        self.name = name

    async def route_message(self, raw_msg):
        # Persist command response caching across reconnect/process restart. Never
        # repeat a side effect whose previous result was not durably recorded.
        try:
            msg = json.loads(raw_msg)
        except (TypeError, ValueError):
            return await super().route_message(raw_msg)
        if isinstance(msg, list) and len(msg) == 4 and msg[0] == 2:
            cached = self.manager._command_begin(self.name, msg)
            if cached is not None:
                await self._connection.send(cached)
                return
        await super().route_message(raw_msg)

    async def _send(self, message):
        parsed = json.loads(message)
        if parsed[0] in (3, 4):
            self.manager._command_finish(self.name, parsed[1], message)
        await super()._send(message)

    @on(Action.remote_start_transaction)
    async def remote_start(self, **kwargs):
        return await self.manager.dispatch(self.name, "RemoteStartTransaction", **kwargs)

    @on(Action.remote_stop_transaction)
    async def remote_stop(self, **kwargs):
        return await self.manager.dispatch(self.name, "RemoteStopTransaction", **kwargs)

    @on(Action.reset)
    async def reset(self, **kwargs):
        return await self.manager.dispatch(self.name, "Reset", **kwargs)

    @on(Action.change_configuration)
    async def change_configuration(self, **kwargs):
        return await self.manager.dispatch(self.name, "ChangeConfiguration", **kwargs)

    @on(Action.get_configuration)
    async def get_configuration(self, **kwargs):
        return await self.manager.dispatch(self.name, "GetConfiguration", **kwargs)

    @on(Action.set_charging_profile)
    async def set_charging_profile(self, **kwargs):
        return await self.manager.dispatch(self.name, "SetChargingProfile", **kwargs)

    @on(Action.clear_charging_profile)
    async def clear_charging_profile(self, **kwargs):
        return await self.manager.dispatch(self.name, "ClearChargingProfile", **kwargs)

    @on(Action.get_composite_schedule)
    async def get_composite_schedule(self, **kwargs):
        return await self.manager.dispatch(self.name, "GetCompositeSchedule", **kwargs)

    @on(Action.clear_cache)
    async def clear_cache(self, **kwargs):
        return await self.manager.dispatch(self.name, "ClearCache", **kwargs)

    @on(Action.send_local_list)
    async def send_local_list(self, **kwargs):
        return await self.manager.dispatch(self.name, "SendLocalList", **kwargs)

    @on(Action.get_local_list_version)
    async def get_local_list_version(self, **kwargs):
        return await self.manager.dispatch(self.name, "GetLocalListVersion", **kwargs)

    @on(Action.trigger_message)
    async def trigger_message(self, **kwargs):
        return await self.manager.dispatch(self.name, "TriggerMessage", **kwargs)

    @on(Action.unlock_connector)
    async def unlock_connector(self, **kwargs):
        return await self.manager.dispatch(self.name, "UnlockConnector", **kwargs)

    @on(Action.change_availability)
    async def change_availability(self, **kwargs):
        return await self.manager.dispatch(self.name, "ChangeAvailability", **kwargs)

    @on(Action.reserve_now)
    async def reserve_now(self, **kwargs):
        return await self.manager.dispatch(self.name, "ReserveNow", **kwargs)

    @on(Action.cancel_reservation)
    async def cancel_reservation(self, **kwargs):
        return await self.manager.dispatch(self.name, "CancelReservation", **kwargs)

    @on(Action.get_diagnostics)
    async def get_diagnostics(self, **kwargs):
        return await self.manager.dispatch(self.name, "GetDiagnostics", **kwargs)

    @on(Action.update_firmware)
    async def update_firmware(self, **kwargs):
        return await self.manager.dispatch(self.name, "UpdateFirmware", **kwargs)

    @on(Action.data_transfer)
    async def data_transfer(self, **kwargs):
        # No inbound vendor extension is configured. Respond with the standard
        # application result, without executing arbitrary vendor payloads.
        return call_result.DataTransfer(status="UnknownVendorId")


class DualCSMSManager:
    is_dual = True
    _LIVE = ("reserved", "starting", "unknown", "active", "rejected")
    _METHODS = {
        "RemoteStartTransaction": "remote_start_transaction",
        "RemoteStopTransaction": "remote_stop_transaction",
        "Reset": "hard_Reset",
        "ChangeConfiguration": "on_change_configuration",
        "GetConfiguration": "on_get_configuration",
        "SetChargingProfile": "on_set_charging_profile",
        "ClearChargingProfile": "on_clear_chargingprofile",
        "GetCompositeSchedule": "on_get_compositeSchedul",
        "ClearCache": "on_clear_cache",
        "SendLocalList": "on_send_local_list",
        "GetLocalListVersion": "on_get_local_list_version",
        "UnlockConnector": "on_unlock_connector",
        "ChangeAvailability": "on_change_availability",
        "ReserveNow": "on_reserve_now",
        "CancelReservation": "on_cancel_reservation",
        "GetDiagnostics": "on_get_diagnostics",
        "UpdateFirmware": "on_update_firmware",
    }
    _ADMIN = {"Reset", "ChangeConfiguration", "SetChargingProfile",
              "ClearChargingProfile", "ClearCache", "SendLocalList",
              "UnlockConnector", "ChangeAvailability", "GetDiagnostics", "UpdateFirmware"}

    def __init__(self, config, state_dir):
        self.config = validate_config(config)
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(str(self.state_dir / "dual_csms_state.sqlite3"))
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS sessions (
                local_id INTEGER PRIMARY KEY AUTOINCREMENT,
                connector INTEGER NOT NULL, owner TEXT NOT NULL,
                id_tag TEXT NOT NULL, state TEXT NOT NULL,
                remote_id INTEGER, start_payload TEXT, start_result TEXT,
                created REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS outbox (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                server TEXT NOT NULL, action TEXT NOT NULL,
                payload TEXT NOT NULL, session_id INTEGER,
                unique_key TEXT NOT NULL UNIQUE,
                message_id TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT);
            CREATE TABLE IF NOT EXISTS statuses (
                connector INTEGER PRIMARY KEY, payload TEXT NOT NULL,
                version INTEGER NOT NULL DEFAULT 1);
            CREATE TABLE IF NOT EXISTS commands (
                server TEXT NOT NULL, message_id TEXT NOT NULL,
                digest TEXT NOT NULL, response TEXT, created REAL NOT NULL,
                PRIMARY KEY(server, message_id));
            CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT);
            CREATE TABLE IF NOT EXISTS auth_owners (
                tag TEXT PRIMARY KEY, owner TEXT NOT NULL, updated REAL NOT NULL);
        """)
        # A crash after sending Start but before persisting its response is
        # indistinguishable from a lost response. It is never replayed.
        self.db.execute("UPDATE sessions SET state='unknown' WHERE state='starting'")
        self.db.execute("UPDATE outbox SET state='pending' WHERE state='sending'")
        self.db.commit()
        self.servers = {s["name"]: s for s in self.config["servers"]
                        if s.get("enabled", True)}
        self.ready = {name: asyncio.Event() for name in self.servers}
        self._heartbeat_intervals = {name: 30 for name in self.servers}
        self._heartbeat_changed = {name: asyncio.Event() for name in self.servers}
        self.peers = {}
        self.core = None
        self._boot = None
        self._boot_event = asyncio.Event()
        self._any_ready = asyncio.Event()
        self._closed = asyncio.Event()
        self._wake = {name: asyncio.Event() for name in self.servers}
        self._status_wake = {name: asyncio.Event() for name in self.servers}
        self._tasks = set()
        self._local_claims = set()    # local_id of charger-side claims, owner fixed by Authorize
        self._session_ends = {}       # connector -> (local_id, monotonic) of the last stop queued
        self._start_tasks = {}
        self._running = False
        self._db_closed = False
        try:
            self._check_state_configuration()
            self._check_legacy_transactions()
        except BaseException:
            self.db.rollback()
            self.db.close()
            self._db_closed = True
            raise

    def _check_state_configuration(self):
        # An owner name may not silently point to a different backend while
        # transaction evidence still refers to the previous one.
        for name, server in self.servers.items():
            digest = hashlib.sha256(server["url"].encode()).hexdigest()
            key = "endpoint:" + name
            old = self.db.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()
            pending = self.db.execute(
                "SELECT 1 FROM sessions WHERE owner=? AND state NOT IN ('closed','cancelled') "
                "UNION SELECT 1 FROM outbox WHERE server=? AND state!='delivered' LIMIT 1",
                (name, name)).fetchone()
            if old and old[0] != digest and pending:
                raise DualCSMSError("Cannot change endpoint for %s while transactions are pending" % name)
            self.db.execute("INSERT OR REPLACE INTO metadata VALUES (?,?)", (key, digest))
        pending_owners = self.db.execute(
            "SELECT DISTINCT owner FROM sessions WHERE state NOT IN ('closed','cancelled') "
            "UNION SELECT DISTINCT server FROM outbox WHERE state!='delivered'").fetchall()
        if any(row[0] not in self.servers for row in pending_owners):
            raise DualCSMSError("Cannot disable a CSMS with pending transaction evidence")
        self.db.commit()

    def _check_legacy_transactions(self):
        for connector in (1, 2):
            file = self.state_dir / ("translogs%d.txt" % connector)
            if not file.exists():
                continue
            try:
                txn = int(file.read_text(encoding="utf-8").strip() or "0")
            except (ValueError, OSError) as exc:
                raise DualCSMSError("Cannot read persisted transaction for connector %d" % connector) from exc
            if txn > 0 and not self.db.execute(
                    "SELECT 1 FROM sessions WHERE local_id=? AND connector=?",
                    (txn, connector)).fetchone():
                raise DualCSMSError("First dual-CSMS installation requires idle connectors; "
                                    "unmapped translogs%d.txt transaction" % connector)

    def bind_core(self, core):
        if self.core is not None and self.core is not core:
            raise DualCSMSError("Only one physical controller may be bound")
        active_reader = getattr(core, "_active_transaction_id", None)
        if active_reader:
            for connector in (1, 2):
                local_id = int(active_reader(connector) or 0)
                if local_id <= 0:
                    continue
                mapped = self.db.execute(
                    "SELECT state FROM sessions WHERE local_id=? AND connector=?",
                    (local_id, connector)).fetchone()
                if mapped is None or mapped["state"] != "active":
                    raise DualCSMSError(
                        "Controller transaction on connector %d is not an active mapped session; "
                        "reconcile before startup" % connector)
            for row in self.db.execute("SELECT * FROM sessions WHERE state='active'").fetchall():
                if int(active_reader(row["connector"]) or 0) != row["local_id"]:
                    raise DualCSMSError(
                        "Persisted CSMS session and controller transaction disagree on connector %d; "
                        "reconcile before startup" % row["connector"])
        self.core = core

    def _live(self, connector):
        return self.db.execute(
            "SELECT * FROM sessions WHERE connector=? AND state IN "
            "('reserved','starting','unknown','active','rejected') ORDER BY local_id DESC LIMIT 1",
            (int(connector),)).fetchone()

    def connector_reserved(self, connector):
        return self._live(connector) is not None

    def claim_local(self, connector, id_tag):
        connector = int(connector)
        if connector not in (1, 2):
            return False
        row = self._live(connector)
        if row:
            # Same tag/owner does not prove this is the same command. In
            # particular a local RFID event must not take over a pending
            # RemoteStart from the default server, even with an identical tag.
            return False
        row = self._reserve(connector, self.local_owner_for(id_tag), str(id_tag))
        self._local_claims.add(row["local_id"])
        return True

    # [R3.1.4] A charger-side idTag (RFID, MQTT RFID command, AutoCharge) is authorized
    # by local_csms first and then by the other CSMS; the session belongs to the CSMS
    # that accepted it. The mapping is kept as a hash of the tag, never the tag itself.
    _AUTH_OWNER_LIMIT = 5000

    @staticmethod
    def _tag_key(id_tag):
        return hashlib.sha256(("dual-csms-auth:%s" % id_tag).encode("utf-8")).hexdigest()

    def local_owner_for(self, id_tag):
        """CSMS that last accepted this idTag at the charger; local_csms when unknown."""
        row = self.db.execute("SELECT owner FROM auth_owners WHERE tag=?", (self._tag_key(id_tag),)).fetchone()
        if row is not None and row["owner"] in self.servers:
            return row["owner"]
        return self.config["local_csms"]

    def _remember_auth_owner(self, id_tag, owner):
        self.db.execute("INSERT INTO auth_owners(tag,owner,updated) VALUES (?,?,?) "
                        "ON CONFLICT(tag) DO UPDATE SET owner=excluded.owner,updated=excluded.updated",
                        (self._tag_key(id_tag), owner, time.time()))
        self.db.execute("DELETE FROM auth_owners WHERE tag NOT IN "
                        "(SELECT tag FROM auth_owners ORDER BY updated DESC LIMIT ?)", (self._AUTH_OWNER_LIMIT,))
        # A local claim made before Authorize follows the CSMS that accepted the tag.
        for local_id in list(self._local_claims):
            row = self.db.execute("SELECT * FROM sessions WHERE local_id=?", (local_id,)).fetchone()
            if row is None or row["state"] != "reserved":
                self._local_claims.discard(local_id)
            elif row["id_tag"] == str(id_tag) and row["owner"] != owner:
                self.db.execute("UPDATE sessions SET owner=? WHERE local_id=?", (owner, local_id))
                LOG.info("[DUAL] local start on connector %s now belongs to %s", row["connector"], owner)
        self.db.commit()

    def _authorize_order(self):
        first = self.config["local_csms"]
        return [first] + [name for name in self.servers if name != first]

    async def _authorize(self, payload):
        origin = command_origin()
        if origin in self.servers:
            # Authorize made while handling one CSMS's command stays with that CSMS.
            return await self._request(origin, payload)
        first_refusal = error = None
        order = self._authorize_order()
        for position, name in enumerate(order):
            following = order[position + 1] if position + 1 < len(order) else None
            try:
                response = await self._request(name, payload)
            except Exception as exc:     # not registered, timeout, transport error, CALLERROR
                error = exc
                LOG.warning("[%s] Authorize unavailable (%s)%s", name, type(exc).__name__,
                            "; asking %s" % following if following else "")
                continue
            info = getattr(response, "id_tag_info", None)
            status = _status(info.get("status") if isinstance(info, dict) else getattr(info, "status", None))
            if status == "Accepted":
                self._remember_auth_owner(payload.id_tag, name)
                LOG.info("[%s] Authorize Accepted; the charger-side session belongs to %s", name, name)
                return response
            LOG.info("[%s] Authorize %s%s", name, status, "; asking %s" % following if following else "")
            first_refusal = first_refusal or response
        if first_refusal is not None:
            return first_refusal
        raise error if error is not None else BackendUnavailable("No CSMS answered Authorize")

    def _reserve(self, connector, owner, id_tag):
        cursor = self.db.execute(
            "INSERT INTO sessions(connector,owner,id_tag,state,created) VALUES (?,?,?,'reserved',?)",
            (connector, owner, id_tag, time.time()))
        self.db.commit()
        return self.db.execute("SELECT * FROM sessions WHERE local_id=?", (cursor.lastrowid,)).fetchone()

    def release_pending(self, connector):
        row = self._live(connector)
        if row and row["state"] == "reserved":
            self.db.execute("UPDATE sessions SET state='cancelled' WHERE local_id=?", (row["local_id"],))
            self.db.commit()
            return True
        return False

    def snapshot(self):
        return {
            "ready": {name: event.is_set() for name, event in self.ready.items()},
            "sessions": [dict(r) for r in self.db.execute("SELECT * FROM sessions ORDER BY local_id")],
            "outbox": [dict(r) for r in self.db.execute("SELECT * FROM outbox ORDER BY seq")],
        }

    def _spawn(self, coroutine):
        task = asyncio.create_task(coroutine)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    async def start(self):
        if self._running:
            raise DualCSMSError("Manager is already running")
        self._running = True
        try:
            for name in self.servers:
                self._spawn(self._peer_loop(name))
                self._spawn(self._outbox_loop(name))
            await self._closed.wait()
        finally:
            self._closed.set()
            tasks = list(self._tasks) + list(self._start_tasks.values())
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self._running = False

    async def close(self):
        if self._db_closed:
            return
        self._closed.set()
        tasks = list(self._tasks) + list(self._start_tasks.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if not self._db_closed:
            self.db.close()
            self._db_closed = True

    async def ping(self):
        # Real connections use websockets' own independent ping/pong loop.
        return None

    def _connection_options(self, server):
        options = dict(subprotocols=["ocpp1.6"], open_timeout=20,
                       ping_interval=30, ping_timeout=60, close_timeout=5)
        if server["url"].startswith("wss://"):
            context = ssl.create_default_context(cafile=server.get("ca_file") or None)
            if server.get("client_cert"):
                context.load_cert_chain(server["client_cert"], server["client_key"])
            options["ssl"] = context
        if server.get("username_env"):
            username = os.environ.get(server["username_env"])
            password = os.environ.get(server["password_env"])
            if username is None or password is None:
                raise DualCSMSError("Missing credential environment variables")
            encoded = base64.b64encode((username + ":" + password).encode()).decode()
            header_key = ("additional_headers" if "additional_headers" in
                          inspect.signature(websockets.connect).parameters else "extra_headers")
            options[header_key] = {"Authorization": "Basic " + encoded}
        return options

    async def _peer_loop(self, name):
        server = self.servers[name]
        while not self._closed.is_set():
            await self._boot_event.wait()
            children = []
            try:
                async with websockets.connect(server["url"], **self._connection_options(server)) as ws:
                    if ws.subprotocol != "ocpp1.6":
                        raise DualCSMSError("Server did not negotiate ocpp1.6")
                    peer = _Peer(name, ws, self)
                    # OCPP message IDs are scoped to a connection, not a durable
                    # global command identifier. A server may restart its ID
                    # counter on reconnect. Keep unresolved side-effect evidence
                    # fail-closed, but do not answer new commands with completed
                    # responses from a previous connection. Session ownership is
                    # the durable guard against a second physical Start.
                    self.db.execute("DELETE FROM commands WHERE server=? AND response IS NOT NULL", (name,))
                    self.db.commit()
                    self.peers[name] = peer
                    reader = asyncio.create_task(peer.start())
                    children.append(reader)
                    while not self._closed.is_set():
                        response = await self._request(name, self._boot, allow_unready=True)
                        if _status(response.status) == "Accepted":
                            break
                        LOG.warning("[%s] BootNotification %s; connection remains isolated",
                                    name, _status(response.status))
                        await asyncio.sleep(max(1, int(response.interval or 30)))
                    self.ready[name].set()
                    self._any_ready.set()
                    self._wake[name].set()
                    self._status_wake[name].set()
                    LOG.info("[%s] OCPP registration accepted", name)
                    self._heartbeat_intervals[name] = max(1, int(response.interval or 30))
                    children.append(asyncio.create_task(self._heartbeat_loop(name)))
                    children.append(asyncio.create_task(self._status_loop(name)))
                    done, _ = await asyncio.wait(children, return_when=asyncio.FIRST_COMPLETED)
                    for task in done:
                        task.result()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # Do not log URLs, auth headers, tags, or arbitrary server text.
                LOG.warning("[%s] connection unavailable (%s); retrying", name, type(exc).__name__)
            finally:
                self.ready[name].clear()
                self.peers.pop(name, None)
                if not any(e.is_set() for e in self.ready.values()):
                    self._any_ready.clear()
                for task in children:
                    task.cancel()
                await asyncio.gather(*children, return_exceptions=True)
            await asyncio.sleep(self.config["reconnect_seconds"])

    async def _request(self, name, payload, allow_unready=False, message_id=None):
        peer = self.peers.get(name)
        if peer is None or (not allow_unready and not self.ready[name].is_set()):
            raise BackendUnavailable("CSMS %s is not registered" % name)
        # Every peer has its own ocpp call lock; no global lock blocks the other CSMS.
        kwargs = {"suppress": False}
        if message_id is not None:
            kwargs["unique_id"] = message_id
        return await peer.call(payload, **kwargs)

    async def call_service(self, server, payload):
        """Send origin-scoped service messages without guessing the local CSMS.

        OCPPServices owns the durable notification queue. A transport exception
        propagates so that queue retains the same notification for retry.
        """
        if server not in self.servers or type(payload).__name__ not in {
                "DiagnosticsStatusNotification", "FirmwareStatusNotification", "Authorize"}:
            raise DualCSMSError("Unsupported service route")
        return await self._request(server, payload)

    async def _heartbeat_loop(self, name):
        while True:
            changed = self._heartbeat_changed[name]
            changed.clear()
            try:
                await asyncio.wait_for(changed.wait(), self._heartbeat_intervals[name])
                continue
            except asyncio.TimeoutError:
                pass
            await self._request(name, call.Heartbeat())

    async def _status_loop(self, name):
        sent = {}
        while True:
            self._status_wake[name].clear()
            rows = self.db.execute("SELECT * FROM statuses ORDER BY connector").fetchall()
            for row in rows:
                if sent.get(row["connector"]) == row["version"]:
                    continue
                await self._request(name, call.StatusNotification(**json.loads(row["payload"])))
                sent[row["connector"]] = row["version"]
            await self._status_wake[name].wait()

    async def call(self, payload, *args, **kwargs):
        name = type(payload).__name__
        if name == "BootNotification":
            self._boot = payload
            self._boot_event.set()
            # This is a LOCAL bridge acknowledgement, not proof that either
            # CSMS accepted registration. Let the physical safety/control loop
            # initialize even during an outage. Real peers register separately;
            # their ready gates still protect all upstream operations/commands.
            return call_result.BootNotification(current_time=_now(), interval=30, status="Accepted")
        if name == "Heartbeat":
            return call_result.Heartbeat(current_time=_now())
        if name == "StatusNotification":
            data = dataclasses.asdict(self._label_status(payload))
            connector = int(data["connector_id"])
            self.db.execute("INSERT INTO statuses(connector,payload,version) VALUES (?,?,1) "
                            "ON CONFLICT(connector) DO UPDATE SET payload=excluded.payload,version=version+1",
                            (connector, _json(data)))
            self.db.commit()
            for event in self._status_wake.values():
                event.set()
            return call_result.StatusNotification()
        if name == "Authorize":
            return await self._authorize(payload)
        if name == "StartTransaction":
            return await self._start_transaction(payload)
        if name == "MeterValues":
            return self._queue_meter(payload)
        if name == "StopTransaction":
            return self._queue_stop(payload)
        if name == "DataTransfer":
            return await self._data_transfer(payload)
        if name in ("DiagnosticsStatusNotification", "FirmwareStatusNotification"):
            return await self.call_service(command_origin(self.config["primary_admin"]), payload)
        return await self._request(self.config["local_csms"], payload)

    async def _start_transaction(self, payload):
        connector = int(payload.connector_id)
        if connector not in (1, 2):
            raise DualCSMSError("Unsupported connector")
        row = self._live(connector)
        if row is None:
            row = self._reserve(connector, self.local_owner_for(payload.id_tag), str(payload.id_tag))
        if row["id_tag"] != str(payload.id_tag):
            raise DualCSMSError("Start idTag differs from reserved physical session")
        guard = getattr(self.core, "_ocpp_start_allowed", None)
        if guard is not None and row["state"] == "reserved":
            allowed = guard(connector, row["owner"], str(payload.id_tag))
            if inspect.isawaitable(allowed):
                allowed = await allowed
            if not allowed:
                self.release_pending(connector)
                raise StartPolicyRejected("Availability or reservation denies this start")
            reservation = getattr(self.core, "_ocpp_reservation_id", lambda *args: None)(
                connector, row["owner"], str(payload.id_tag))
            if reservation is not None:
                payload = dataclasses.replace(payload, reservation_id=reservation)
        if row["state"] == "unknown":
            raise StartOutcomeUnknown("Start outcome requires reconciliation for connector %d" % connector)
        if row["start_result"]:
            return call_result.StartTransaction(**json.loads(row["start_result"]))
        existing = self._start_tasks.get(row["local_id"])
        if existing:
            return await asyncio.shield(existing)
        if not self.ready[row["owner"]].is_set():
            raise BackendUnavailable("Session owner %s is not registered" % row["owner"])
        task = asyncio.create_task(self._do_start(row, payload))
        self._start_tasks[row["local_id"]] = task
        task.add_done_callback(lambda done: self._start_tasks.pop(row["local_id"], None))
        try:
            return await asyncio.shield(task)
        finally:
            if task.done():
                self._start_tasks.pop(row["local_id"], None)

    async def _do_start(self, row, payload):
        local_id = row["local_id"]
        self.db.execute("UPDATE sessions SET state='starting',start_payload=? WHERE local_id=?",
                        (_json(dataclasses.asdict(payload)), local_id))
        self.db.commit()
        try:
            response = await self._request(row["owner"], payload)
            remote_id = response.transaction_id
            if isinstance(remote_id, bool) or not isinstance(remote_id, int):
                raise DualCSMSError("Invalid StartTransaction response ID")
            result = dataclasses.asdict(response)
            info = result.get("id_tag_info", {})
            accepted = isinstance(info, dict) and _status(info.get("status")) == "Accepted"
            # This hardware controller treats nonpositive IDs as "no
            # transaction". Preserve that deployed contract instead of turning
            # a refusal sentinel into a positive local transaction ID.
            if remote_id <= 0:
                if accepted:
                    raise DualCSMSError("Accepted StartTransaction has a nonpositive ID")
                self.db.execute("UPDATE sessions SET state='cancelled',start_result=? WHERE local_id=?",
                                (_json(result), local_id))
                self.db.commit()
                return call_result.StartTransaction(**result)
            duplicate = self.db.execute(
                "SELECT local_id FROM sessions WHERE owner=? AND remote_id=? AND local_id!=? "
                "AND state NOT IN ('closed','cancelled')", (row["owner"], remote_id, local_id)).fetchone()
            if duplicate:
                raise DualCSMSError("CSMS reused an active transaction ID")
            result["transaction_id"] = local_id
            self.db.execute("UPDATE sessions SET state=?,remote_id=?,start_result=? WHERE local_id=?",
                            ("active" if accepted else "rejected", remote_id, _json(result), local_id))
            self.db.commit()
            LOG.info("[%s] mapped connector %d to local session %d", row["owner"], row["connector"], local_id)
            if accepted:
                hook = getattr(self.core, "_ocpp_transaction_started", None)
                if hook is not None:
                    try:
                        result_hook = hook(row["connector"], row["owner"], str(payload.id_tag), local_id)
                        if inspect.isawaitable(result_hook):
                            await result_hook
                    except Exception:
                        # A confirmed Start is never relabelled outcome-unknown
                        # because a local profile/reservation observer failed.
                        LOG.exception("Confirmed start service hook failed")
                operational = getattr(self.core, "_ocpp_start_operational", None)
                if operational is not None and not operational(row["connector"]):
                    raise StartPolicyRejected("Confirmed start was stopped by a local limit fault")
            return call_result.StartTransaction(**result)
        except StartPolicyRejected:
            # The upstream result is already durably known. Do not relabel a
            # deliberate local abort as an ambiguous network outcome/replay.
            raise
        except BaseException as exc:
            self.db.execute("UPDATE sessions SET state='unknown' WHERE local_id=? AND state='starting'", (local_id,))
            self.db.commit()
            if isinstance(exc, asyncio.CancelledError):
                raise
            LOG.error("[%s] Start outcome unknown for connector %d; automatic replay blocked",
                      row["owner"], row["connector"])
            raise StartOutcomeUnknown("Start outcome unknown for connector %d" % row["connector"]) from exc

    def _transaction(self, local_id):
        row = self.db.execute("SELECT * FROM sessions WHERE local_id=?", (int(local_id),)).fetchone()
        if row is None or row["remote_id"] is None:
            raise DualCSMSError("Unknown local transaction; remote ID cannot be guessed")
        return row

    def _enqueue(self, server, action, payload, session_id, unique_key):
        self.db.execute("INSERT OR IGNORE INTO outbox(server,action,payload,session_id,unique_key,message_id) "
                        "VALUES (?,?,?,?,?,?)", (server, action, _json(payload), session_id, unique_key, uuid.uuid4().hex))
        self.db.commit()
        self._wake[server].set()

    def _queue_meter(self, payload):
        data = dataclasses.asdict(payload)
        local_id = data.get("transaction_id")
        if local_id is None:
            owner = self.config["local_csms"]
        else:
            row = self._transaction(local_id)
            if row["state"] in ("stopping", "closed"):
                return call_result.MeterValues()
            if int(data["connector_id"]) != row["connector"]:
                raise DualCSMSError("Meter connector does not match transaction")
            owner = row["owner"]
            data["transaction_id"] = row["remote_id"]
        key = "meter:" + hashlib.sha256(_json(data).encode()).hexdigest() + ":" + owner
        self._enqueue(owner, "MeterValues", data, local_id, key)
        limit = self.config["meter_queue_limit"]
        count = self.db.execute("SELECT count(*) FROM outbox WHERE server=? AND action='MeterValues' AND state='pending'", (owner,)).fetchone()[0]
        if count > limit:
            self.db.execute("DELETE FROM outbox WHERE seq IN (SELECT seq FROM outbox WHERE server=? "
                            "AND action='MeterValues' AND state='pending' ORDER BY seq LIMIT ?)", (owner, count - limit))
            self.db.commit()
            LOG.warning("[%s] meter outbox capacity reached; discarded %d oldest samples", owner, count - limit)
        return call_result.MeterValues()

    def _queue_stop(self, payload):
        row = self._transaction(payload.transaction_id)
        data = dataclasses.asdict(payload)
        data.pop("connector_id", None)  # use standard OCPP 1.6 at both backends
        data["transaction_id"] = row["remote_id"]
        # One committed transaction: durable stop evidence and ownership release.
        self.db.execute("INSERT OR IGNORE INTO outbox(server,action,payload,session_id,unique_key,message_id) "
                        "VALUES (?,'StopTransaction',?,?,?,?)",
                        (row["owner"], _json(data), row["local_id"], "stop:%s" % row["local_id"], uuid.uuid4().hex))
        self.db.execute("UPDATE sessions SET state='stopping' WHERE local_id=? AND state!='closed'", (row["local_id"],))
        self.db.commit()
        self._session_ends[int(row["connector"])] = (row["local_id"], time.monotonic())
        self._wake[row["owner"]].set()
        return call_result.StopTransaction()

    # [R3.1.5] Both CSMS receive every StatusNotification. While a connector has a
    # session, the status carries the owning CSMS in `info`, so either CSMS's log
    # and the charger log say whose session it was without a second transaction
    # and without the idTag. The charger's own `info` text is never replaced.
    _SESSION_STATUSES = ("Charging", "SuspendedEV", "SuspendedEVSE", "Finishing")
    _FINISHING_LABEL_SECONDS = 600

    def _session_for_status(self, connector, status):
        if status != "Finishing":
            return self._live(connector)
        row = self.db.execute(
            "SELECT * FROM sessions WHERE connector=? AND state NOT IN ('reserved','cancelled') "
            "ORDER BY local_id DESC LIMIT 1", (connector,)).fetchone()
        if row is None:
            return None
        if row["state"] != "closed":
            return row
        # Finishing is also sent for sessions that never reached StartTransaction,
        # so a closed transaction labels it only right after its own stop.
        ended = self._session_ends.get(connector)
        if ended and ended[0] == row["local_id"] and time.monotonic() - ended[1] < self._FINISHING_LABEL_SECONDS:
            return row
        return None

    def session_label(self, connector, status):
        """`info` text for a connector status, or None when no CSMS session is involved."""
        row = self._session_for_status(int(connector), status)
        if row is None:
            return None
        label = "Session of CSMS %s" % row["owner"]
        if row["remote_id"] is not None:
            label += " (tx %s)" % row["remote_id"]
        return label[:50]

    def _label_status(self, payload):
        status = _status(getattr(payload, "status", None))
        if status not in self._SESSION_STATUSES or getattr(payload, "info", None):
            return payload
        label = self.session_label(payload.connector_id, status)
        return dataclasses.replace(payload, info=label) if label else payload

    async def _outbox_loop(self, name):
        while not self._closed.is_set():
            await self.ready[name].wait()
            self._wake[name].clear()
            row = self.db.execute("SELECT * FROM outbox WHERE server=? AND state='pending' ORDER BY seq LIMIT 1", (name,)).fetchone()
            if row is None:
                await self._wake[name].wait()
                continue
            try:
                self.db.execute("UPDATE outbox SET state='sending',attempts=attempts+1 WHERE seq=?", (row["seq"],))
                self.db.commit()
                payload = getattr(call, row["action"])(**json.loads(row["payload"]))
                await self._request(name, payload, message_id=row["message_id"])
                self.db.execute("UPDATE outbox SET state='delivered',last_error=NULL WHERE seq=?", (row["seq"],))
                if row["action"] == "StopTransaction":
                    self.db.execute("UPDATE sessions SET state='closed' WHERE local_id=?", (row["session_id"],))
                self.db.commit()
            except asyncio.CancelledError:
                self.db.execute("UPDATE outbox SET state='pending' WHERE seq=? AND state='sending'", (row["seq"],))
                self.db.commit()
                raise
            except Exception as exc:
                # Schema/CALLERROR failures need inspection, not a retry loop
                # that blocks a later StopTransaction behind a poison sample.
                state = "failed" if isinstance(exc, OCPPError) else "pending"
                self.db.execute("UPDATE outbox SET state=?,last_error=? WHERE seq=?", (state, type(exc).__name__, row["seq"]))
                self.db.commit()
                LOG.warning("[%s] %s retained in outbox state=%s (%s)", name, row["action"], state, type(exc).__name__)
                await asyncio.sleep(self.config["reconnect_seconds"])

    async def _data_transfer(self, payload):
        # The existing EDS V2G bridge observes the controller's local IDs. Only
        # this known vendor contract is translated; other vendors remain opaque.
        if getattr(payload, "vendor_id", None) != "EDS":
            return await self._request(self.config["local_csms"], payload)
        data = getattr(payload, "data", None)
        if data is None:
            return await self._request(self.config["local_csms"], payload)
        try:
            body = json.loads(data)
        except (TypeError, ValueError) as exc:
            raise DualCSMSError("EDS DataTransfer requires valid JSON data") from exc
        owners = set()

        def translate(value):
            if isinstance(value, list):
                return [translate(item) for item in value]
            if not isinstance(value, dict):
                return value
            result = {}
            for key, item in value.items():
                if key == "transactionId" and item is not None:
                    if isinstance(item, bool) or not isinstance(item, int):
                        raise DualCSMSError("EDS transactionId must be a local integer ID")
                    row = self._transaction(item)
                    owners.add(row["owner"])
                    result[key] = row["remote_id"]
                else:
                    result[key] = translate(item)
            return result

        translated = translate(body)
        if len(owners) > 1:
            raise DualCSMSError("EDS DataTransfer contains transactions belonging to different CSMS")
        owner = next(iter(owners), self.config["local_csms"])
        return await self._request(owner, dataclasses.replace(payload, data=_json(translated)))

    def _command_begin(self, server, msg):
        unique_id = str(msg[1])
        digest = hashlib.sha256(_json(msg[2:]).encode()).hexdigest()
        row = self.db.execute("SELECT * FROM commands WHERE server=? AND message_id=?", (server, unique_id)).fetchone()
        if row:
            if row["digest"] != digest:
                return _json([4, unique_id, "ProtocolError", "Request ID reused with different content", {}])
            if row["response"]:
                return row["response"]
            return _json([4, unique_id, "InternalError", "Previous command outcome unresolved; not replayed", {}])
        self.db.execute("INSERT INTO commands VALUES (?,?,?,?,?)", (server, unique_id, digest, None, time.time()))
        self.db.commit()
        return None

    def _command_finish(self, server, unique_id, message):
        self.db.execute("UPDATE commands SET response=? WHERE server=? AND message_id=?", (message, server, unique_id))
        # Bound completed records while keeping unresolved commands for audit.
        self.db.execute("DELETE FROM commands WHERE server=? AND response IS NOT NULL AND message_id NOT IN "
                        "(SELECT message_id FROM commands WHERE server=? AND response IS NOT NULL ORDER BY created DESC LIMIT 2048)", (server, server))
        self.db.commit()

    def _refuse_start(self, server, connector, reason):
        # Field diagnosis: every refusal names its reason. Never log the idTag.
        LOG.warning("[%s] RemoteStart C%s rejected: %s", server, connector, reason)
        return self._rejected("RemoteStartTransaction")

    def _start_refusal(self, connector):
        explain = getattr(self.core, "_ocpp_start_refusal", None)
        try:
            return explain(connector) if explain else "start not allowed"
        except Exception as error:
            return "start not allowed (%s)" % type(error).__name__

    def _busy_reason(self, connector):
        explain = getattr(self.core, "_dual_busy_reason", None)
        try:
            return (explain(connector) or "connector busy") if explain else "connector busy"
        except Exception as error:
            return "connector busy (%s)" % type(error).__name__

    @staticmethod
    def _rejected(action):
        if action == "GetDiagnostics":
            return call_result.GetDiagnostics()
        if action == "UpdateFirmware":
            raise SecurityError(description="Firmware staging is restricted to the administrator")
        statuses = {"UnlockConnector": "UnlockFailed", "SendLocalList": "Failed",
                    "ClearChargingProfile": "Unknown"}
        return getattr(call_result, action)(status=statuses.get(action, "Rejected"))

    async def dispatch(self, server, action, **kwargs):
        if server not in self.servers or self.core is None:
            raise DualCSMSError("Unknown CSMS or no physical controller bound")
        if not self.ready[server].is_set():
            if action == "RemoteStartTransaction":
                return self._refuse_start(server, kwargs.get("connector_id"), "this CSMS has not accepted the BootNotification yet")
            return self._rejected(action) if action not in ("GetConfiguration", "GetLocalListVersion") else await self._invoke(action, kwargs, server)
        if action == "RemoteStartTransaction":
            connector = kwargs.get("connector_id")
            if isinstance(connector, bool) or connector not in (1, 2):
                return self._refuse_start(server, connector, "invalid connector")
            if not getattr(self.core, "_dual_ready", False):
                return self._refuse_start(server, connector, "charger startup not finished")
            live = self._live(connector)
            if live is not None:
                return self._refuse_start(server, connector, "already claimed by %s session %s (%s)"
                                          % (live["owner"], live["local_id"], live["state"]))
            busy = getattr(self.core, "_dual_connector_busy", None)
            if busy and busy(connector):
                return self._refuse_start(server, connector, self._busy_reason(connector))
            row = self._reserve(connector, server, str(kwargs.get("id_tag", "")))
            try:
                admission = getattr(self.core, "_ocpp_start_admission", None)
                if admission is not None:
                    decision = admission(connector, server, str(kwargs.get("id_tag", "")))
                    if decision is False:
                        self.release_pending(connector)
                        return self._refuse_start(server, connector, self._start_refusal(connector))
                    if decision is None:
                        # This peer's reader cannot await Authorize.conf while
                        # it is still handling RemoteStart.req. Keep the claim,
                        # answer Accepted, then verify before arming hardware.
                        self._spawn(self._deferred_remote_start(server, dict(kwargs)))
                        return call_result.RemoteStartTransaction(status="Accepted")
                guard = getattr(self.core, "_ocpp_start_allowed", None)
                allowed = guard(connector, server, str(kwargs.get("id_tag", ""))) if guard else True
                if inspect.isawaitable(allowed):
                    allowed = await allowed
                if not allowed:
                    self.release_pending(connector)
                    return self._refuse_start(server, connector, self._start_refusal(connector))
                result = await self._invoke(action, kwargs, server)
            except Exception:
                # Dispatch may have changed hardware intent before raising. Keep
                # the claim until the core explicitly clears it.
                raise
            if _status(result.status) != "Accepted":
                self.db.execute("UPDATE sessions SET state='cancelled' WHERE local_id=? AND state='reserved'", (row["local_id"],))
                self.db.commit()
            return result
        if action == "RemoteStopTransaction":
            if not getattr(self.core, "_dual_ready", False):
                return self._rejected(action)
            remote_id = kwargs.get("transaction_id")
            row = self.db.execute("SELECT * FROM sessions WHERE owner=? AND remote_id=? AND state IN ('active','rejected','stopping') ORDER BY local_id DESC LIMIT 1", (server, remote_id)).fetchone()
            if row is None:
                return self._rejected(action)
            if row["state"] == "stopping":
                return call_result.RemoteStopTransaction(status="Accepted")
            kwargs["transaction_id"] = row["local_id"]
            return await self._invoke(action, kwargs, server)
        if action == "TriggerMessage":
            return await self._trigger(server, kwargs)
        if action in self._ADMIN:
            if server not in self.config["admins"] or not getattr(self.core, "_dual_ready", False):
                return self._rejected(action)
        if action == "SetChargingProfile":
            profile = copy.deepcopy(kwargs.get("cs_charging_profiles", {}))
            remote_id = profile.get("transaction_id")
            if remote_id is not None:
                row = self.db.execute("SELECT * FROM sessions WHERE owner=? AND remote_id=? AND state='active'", (server, remote_id)).fetchone()
                if row is None or kwargs.get("connector_id") != row["connector"]:
                    return self._rejected(action)
                profile["transaction_id"] = row["local_id"]
                kwargs["cs_charging_profiles"] = profile
        result = await self._invoke(action, kwargs, server)
        if (action == "ChangeAvailability" and kwargs.get("type") == "Inoperative"
                and _status(result.status) in ("Accepted", "Scheduled")):
            targets = (1, 2) if kwargs.get("connector_id") == 0 else (kwargs.get("connector_id"),)
            for cid in targets:
                row = self._live(cid)
                if row and row["state"] == "reserved":
                    self.release_pending(cid)
                    cancel = getattr(self.core, "_ocpp_cancel_pending_start", None)
                    if cancel is not None:
                        await cancel(cid)
        if (action == "ChangeConfiguration" and kwargs.get("key") == "HeartbeatInterval"
                and _status(result.status) == "Accepted"):
            self._heartbeat_intervals[server] = max(1, int(kwargs["value"]))
            self._heartbeat_changed[server].set()
        return result

    async def _deferred_remote_start(self, server, kwargs):
        await asyncio.sleep(0)
        connector = kwargs["connector_id"]
        invoked = False
        try:
            if not await self.core._ocpp_start_allowed(connector, server, str(kwargs["id_tag"])):
                self.release_pending(connector)
                return
            invoked = True
            result = await self._invoke("RemoteStartTransaction", kwargs, server)
            if _status(result.status) != "Accepted":
                self.release_pending(connector)
        except Exception:
            if not invoked:
                self.release_pending(connector)
            LOG.exception("Deferred reservation authorization/start failed")

    async def _invoke(self, action, kwargs, server=None):
        method_name = self._METHODS.get(action)
        if method_name is None:
            raise DualCSMSError("Unsupported delegated OCPP action")
        token = _COMMAND_ORIGIN.set(server)
        try:
            return await getattr(self.core, method_name)(**kwargs)
        finally:
            _COMMAND_ORIGIN.reset(token)

    async def _trigger(self, server, kwargs):
        requested = _status(kwargs.get("requested_message"))
        connector = kwargs.get("connector_id")
        if connector is not None and (isinstance(connector, bool) or connector not in (0, 1, 2)):
            return call_result.TriggerMessage(status="Rejected")
        if requested == "MeterValues":
            connector = connector or 1
            row = self._live(connector)
            if row is None or row["owner"] != server or row["state"] != "active":
                return call_result.TriggerMessage(status="Rejected")
            sender = getattr(self.core, "send_triggered_meter_values", self.core.send_meter_values)
            self._spawn(sender(connector))
        elif requested == "StatusNotification":
            # Core snapshots use the normal shared status flow; this does not
            # expose the other server's transaction IDs or duplicate transactions.
            self._status_wake[server].set()
            self._spawn(self._trigger_status(server, connector))
        elif requested in ("BootNotification", "Heartbeat"):
            self._spawn(self._trigger_simple(server, requested))
        elif requested in ("DiagnosticsStatusNotification", "FirmwareStatusNotification"):
            reporter = getattr(self.core, "_ocpp_trigger_service_status", None)
            if reporter is None:
                return call_result.TriggerMessage(status="NotImplemented")
            self._spawn(reporter(server, requested))
        else:
            return call_result.TriggerMessage(status="NotImplemented")
        return call_result.TriggerMessage(status="Accepted")

    async def _trigger_status(self, server, connector):
        await asyncio.sleep(0)
        rows = self.db.execute("SELECT * FROM statuses WHERE (? IS NULL OR connector=?) ORDER BY connector", (connector, connector)).fetchall()
        try:
            for row in rows:
                await self._request(server, call.StatusNotification(**json.loads(row["payload"])))
        except Exception as exc:
            LOG.warning("[%s] triggered status unavailable (%s)", server, type(exc).__name__)

    async def _trigger_simple(self, server, requested):
        await asyncio.sleep(0)
        try:
            payload = self._boot if requested == "BootNotification" else call.Heartbeat()
            response = await self._request(server, payload)
            if requested == "BootNotification" and _status(response.status) == "Accepted":
                self._heartbeat_intervals[server] = max(1, int(response.interval or 30))
                self._heartbeat_changed[server].set()
            if requested == "BootNotification" and _status(response.status) != "Accepted":
                self.ready[server].clear()
                peer = self.peers.get(server)
                if peer:
                    await peer._connection.close()
        except Exception as exc:
            LOG.warning("[%s] triggered %s unavailable (%s)", server, requested, type(exc).__name__)
