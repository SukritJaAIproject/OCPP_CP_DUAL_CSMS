"""Windows/Linux OCPP test station: real CSMS sockets, entirely simulated hardware.

No CP.py, GPIO, serial, PLC, MQTT, or notification integrations are imported.
Transactions begin only after RemoteStartTransaction arrives from a test CSMS.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import os
import signal
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote, urlparse

from ocpp.v16 import call, call_result
from helpf.dual_csms import DualCSMSManager, DualCSMSError, StartOutcomeUnknown, StartPolicyRejected, validate_config
from helpf.ocpp_runtime import OCPPProtocolMixin


LOG = logging.getLogger("local_simulator")
MARKER = ".dual_csms_simulator_only"


def now():
    return datetime.now(timezone.utc).isoformat()


def atomic_json(path, value):
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8") as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def simulator_config(path):
    config = validate_config(json.loads(path.read_text(encoding="utf-8-sig")))
    if config.get("simulator") is not True:
        raise ValueError("Dedicated simulator config must explicitly contain simulator: true")
    active = [s for s in config["servers"] if s.get("enabled", True)]
    if len(active) != 2:
        raise ValueError("This live test requires exactly two enabled test endpoints")
    identities = []
    for server in active:
        identity = unquote(urlparse(server["url"]).path.rstrip("/").rsplit("/", 1)[-1])
        upper = identity.upper()
        if upper in {"CP-A-0001", "CP-B-0001"}:
            raise ValueError("Production charger identity is forbidden in this simulator")
        if not identity or not any(word in upper for word in ("TEST", "SIM")):
            raise ValueError("Each charger identity must explicitly contain TEST or SIM")
        identities.append(identity)
    if len(set(identities)) != 2:
        raise ValueError("Use distinct test charger identities on A and B")
    config["_simulator_identities"] = identities
    return config


def prepare_state(path):
    path = path.resolve()
    path.mkdir(parents=True, exist_ok=True)
    marker = path / MARKER
    if not marker.exists():
        if any(path.iterdir()):
            raise ValueError("New simulator state directory must be empty; never use charger runtime directory")
        marker.write_text("SIMULATED HARDWARE ONLY\n", encoding="ascii")
    return path


@contextmanager
def simulator_lock(directory):
    """Prevent two processes from controlling the same simulator state."""
    stream = (directory / ".simulator.lock").open("a+b")
    stream.seek(0, os.SEEK_END)
    if stream.tell() == 0:
        stream.write(b"0")
        stream.flush()
    stream.seek(0)
    try:
        if os.name == "nt":
            import msvcrt
            msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as error:
        stream.close()
        raise RuntimeError("Another simulator process is using this state directory") from error
    try:
        yield
    finally:
        stream.close()


class SimCore(OCPPProtocolMixin):
    # Reset here exercises protocol ordering only. It never restarts Windows,
    # imports CP.py, or calls hardware/OS reboot adapters.
    _reset_reply_delay = .5
    _reset_wait_seconds = 15.0

    def __init__(self, manager, directory, meter_period, full_protocol=False):
        self.manager = manager
        self._dual_manager = manager
        self.directory = directory
        self._ocpp_state_dir = directory
        self._full_protocol = full_protocol
        self._smart_suspended = set()
        self._sim_limits = {}
        self.meter_period = meter_period
        self._dual_ready = False
        self.tasks = set()
        self.stopping = set()
        self._reset_task = None
        self._reset_blocked = False
        state_file = directory / "sim_state.json"
        if state_file.exists():
            state = json.loads(state_file.read_text(encoding="utf-8"))
            if state.get("simulated_hardware") is not True:
                raise ValueError("Refusing non-simulator core state")
            self.heads = state["heads"]
        else:
            self.heads = {str(cid): {"local_id": 0, "id_tag": "", "meter_wh": 0,
                                    "pending": False, "status": "Available"} for cid in (1, 2)}
            self.persist()
        if full_protocol:
            from tools.cp_protocol_harness import bind_cp_protocol_methods
            bind_cp_protocol_methods(self, directory, defaults={"MeterValueSampleInterval": str(max(1, int(meter_period)))})
            service_config = directory / "ocpp_services_config.json"
            if not service_config.exists():
                # This test-only mode explicitly permits its local HTTP fixtures.
                allowed = {urlparse(s["url"]).hostname for s in manager.servers.values()}
                plain = {urlparse(s["url"]).hostname for s in manager.servers.values() if s["url"].startswith("ws://")}
                atomic_json(service_config, {"transfer": {"allowed_hosts": sorted(allowed | {"127.0.0.1"}),
                    "allow_http_hosts": sorted(plain | {"127.0.0.1"})}, "diagnostic_sources": ["ocpp_runtime_events.jsonl"]})
            self._ocpp_protocol()

    async def call(self, payload, *args, **kwargs):
        if self._full_protocol:
            payload = self._ocpp_protocol().filter_status(payload)
        return await self.manager.call(payload, *args, **kwargs)

    async def _ocpp_start_allowed(self, connector, owner, tag):
        return await super()._ocpp_start_allowed(connector, owner, tag) if self._full_protocol else True

    def _ocpp_start_admission(self, connector, owner, tag):
        return super()._ocpp_start_admission(connector, owner, tag) if self._full_protocol else True

    def _ocpp_reservation_id(self, connector, owner, tag):
        return super()._ocpp_reservation_id(connector, owner, tag) if self._full_protocol else None

    async def _ocpp_transaction_started(self, connector, owner, tag, local_id):
        if self._full_protocol:
            await super()._ocpp_transaction_started(connector, owner, tag, local_id)

    def _ocpp_pending_start(self, connector):
        return self.heads[str(connector)]["pending"]

    def _ocpp_physical_status(self, connector):
        return self.heads[str(connector)]["status"] if connector in (1, 2) else "Available"

    async def _ocpp_apply_limits(self, connector, limits):
        self._sim_limits[connector] = limits
        self.event("simulated_limit_command", connector=connector, limits=limits)
        row = self.heads[str(connector)]
        if row["local_id"] > 0:
            if min(limits.values()) <= 0 and row["status"] == "Charging":
                self._smart_suspended.add(connector)
                await self.status(connector, "SuspendedEVSE")
            elif min(limits.values()) > 0 and connector in self._smart_suspended:
                self._smart_suspended.discard(connector)
                await self.status(connector, "Charging")

    async def _ocpp_limit_fault_stop(self, connector):
        transaction_id = self._ocpp_protocol().transaction_id(connector)
        if transaction_id > 0:
            if self._active_transaction_id(connector) <= 0:
                self.heads[str(connector)].update(local_id=transaction_id, pending=False)
            await self.stop_transaction(connector, "Other")

    def _ocpp_start_operational(self, connector):
        return super()._ocpp_start_operational(connector) if self._full_protocol else True

    async def _ocpp_cancel_pending_start(self, connector):
        row = self.heads[str(connector)]
        if row["local_id"] == 0:
            row.update(pending=False, id_tag="")
            self.persist()
            await self.status(connector, "Available")

    def persist(self):
        atomic_json(self.directory / "sim_state.json", {"simulated_hardware": True, "heads": self.heads})
        for cid in (1, 2):
            path = self.directory / f"translogs{cid}.txt"
            temporary = path.with_suffix(".txt.tmp")
            temporary.write_text(str(self.heads[str(cid)]["local_id"]), encoding="ascii")
            os.replace(temporary, path)

    def event(self, action, **details):
        record = {"timestamp": now(), "simulated_hardware": True, "event": action, **details}
        with (self.directory / "simulator-events.jsonl").open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(record, ensure_ascii=False) + "\n")
        LOG.info("SIM %s %s", action, json.dumps(details, ensure_ascii=False))

    def spawn(self, coroutine):
        task = asyncio.create_task(coroutine)
        self.tasks.add(task)
        def completed(done):
            self.tasks.discard(done)
            if not done.cancelled() and done.exception():
                self.event("task_failed", error_type=type(done.exception()).__name__)
        task.add_done_callback(completed)
        return task

    def _active_transaction_id(self, connector_id):
        return int(self.heads[str(connector_id)]["local_id"])

    def _dual_connector_busy(self, connector_id):
        row = self.heads[str(connector_id)]
        return not self._dual_ready or row["pending"] or row["local_id"] > 0 or connector_id in self.stopping

    async def status(self, cid, status, error_code="NoError"):
        if cid in (1, 2):
            self.heads[str(cid)]["status"] = status
            self.persist()
        await self.call(call.StatusNotification(connector_id=cid, error_code=error_code,
                                                        status=status, timestamp=now()))
        self.event("status_queued", connector=cid, status=status)

    async def remote_start_transaction(self, id_tag, connector_id, **kwargs):
        if connector_id not in (1, 2) or self._dual_connector_busy(connector_id):
            return call_result.RemoteStartTransaction(status="Rejected")
        row = self.heads[str(connector_id)]
        row["pending"] = True
        row["id_tag"] = id_tag
        self.persist()
        self.event("remote_start_accepted", connector=connector_id)
        self.spawn(self.start_transaction(connector_id))
        return call_result.RemoteStartTransaction(status="Accepted")

    async def start_transaction(self, cid):
        # Yield so the receiving OCPP handler sends its response before this
        # outbound call waits for a response on the same peer.
        await asyncio.sleep(0)
        row = self.heads[str(cid)]
        if self._full_protocol and not row["pending"]:
            return
        try:
            await self.status(cid, "Preparing")
            result = await self.manager.call(call.StartTransaction(
                connector_id=cid, id_tag=row["id_tag"], meter_start=row["meter_wh"], timestamp=now()))
            accepted = result.id_tag_info.get("status") == "Accepted"
            if accepted and result.transaction_id > 0:
                row["local_id"] = result.transaction_id
                row["pending"] = False
                self.persist()
                self.event("transaction_started", connector=cid, local_id=result.transaction_id)
                await self.status(cid, "Charging")
            else:
                if result.transaction_id > 0:
                    await self.manager.call(call.StopTransaction(transaction_id=result.transaction_id,
                        meter_stop=row["meter_wh"], timestamp=now(), id_tag=row["id_tag"], reason="DeAuthorized"))
                self.manager.release_pending(cid)
                row.update(local_id=0, id_tag="", pending=False)
                self.persist()
                self.event("start_rejected", connector=cid)
                await self.status(cid, "Available")
        except StartOutcomeUnknown:
            row["pending"] = True
            self.persist()
            self.event("start_outcome_unknown", connector=cid, automatic_replay=False)
            await self.status(cid, "Faulted")
        except StartPolicyRejected:
            row.update(local_id=0, id_tag="", pending=False)
            self.persist()
            self.event("start_policy_rejected", connector=cid)
            await self.status(cid, "Available")
        except DualCSMSError as error:
            if self.manager.release_pending(cid):
                row.update(local_id=0, id_tag="", pending=False)
                self.persist()
            self.event("start_failed", connector=cid, error_type=type(error).__name__)
            await self.status(cid, "Faulted")

    async def send_meter_values(self, connector_id):
        row = self.heads[str(connector_id)]
        if row["local_id"] <= 0:
            return
        await self.call(call.MeterValues(connector_id=connector_id,
            transaction_id=row["local_id"], meter_value=[{"timestamp": now(), "sampledValue": [
                {"value": str(row["meter_wh"]), "measurand": "Energy.Active.Import.Register", "unit": "Wh"}]}]))
        self.event("simulated_meter_queued", connector=connector_id, local_id=row["local_id"], meter_wh=row["meter_wh"])

    async def meter_loop(self):
        sent = {1: time.monotonic(), 2: time.monotonic()}
        while True:
            await asyncio.sleep(.1 if self._full_protocol else self.meter_period)
            for cid in (1, 2):
                row = self.heads[str(cid)]
                if self._full_protocol:
                    period = int(self.MeterValueSampleInterval_cfg)
                    if period == 0 or time.monotonic() - sent[cid] < period:
                        continue
                if row["local_id"] > 0 and cid not in self.stopping and row["status"] == "Charging":
                    sent[cid] = time.monotonic()
                    row["meter_wh"] += 5  # Synthetic energy: never a physical measurement.
                    self.persist()
                    await self.send_meter_values(cid)

    async def remote_stop_transaction(self, transaction_id, **kwargs):
        for cid in (1, 2):
            if self.heads[str(cid)]["local_id"] == transaction_id:
                if cid not in self.stopping:
                    self.stopping.add(cid)
                    self.spawn(self.stop_transaction(cid, "Remote"))
                self.event("remote_stop_accepted", connector=cid, local_id=transaction_id)
                return call_result.RemoteStopTransaction(status="Accepted")
        return call_result.RemoteStopTransaction(status="Rejected")

    async def stop_transaction(self, cid, reason):
        await asyncio.sleep(0)
        row = self.heads[str(cid)]
        local_id = row["local_id"]
        try:
            if local_id <= 0:
                return
            await self.send_meter_values(cid)
            await self.manager.call(call.StopTransaction(transaction_id=local_id,
                meter_stop=row["meter_wh"], timestamp=now(), id_tag=row["id_tag"], reason=reason))
            row.update(local_id=0, id_tag="", pending=False)
            self.persist()
            self.event("stop_saved_to_outbox", connector=cid, local_id=local_id)
            await self.status(cid, "Finishing")
            await asyncio.sleep(.2)
            await self.status(cid, "Available")
        finally:
            self.stopping.discard(cid)

    async def on_get_configuration(self, **kwargs):
        return call_result.GetConfiguration(configuration_key=[
            {"key": "NumberOfConnectors", "readonly": True, "value": "2"},
            {"key": "MeterValueSampleInterval", "readonly": True, "value": str(int(self.meter_period))},
            {"key": "SupportedFeatureProfiles", "readonly": True, "value": "Core"}])

    async def on_get_local_list_version(self, **kwargs):
        return call_result.GetLocalListVersion(list_version=0)

    async def hard_Reset(self, **kwargs):
        if not self._full_protocol or kwargs.get("type", "Soft") not in ("Soft", "Hard"):
            return call_result.Reset(status="Rejected")
        if self._reset_task is not None and not self._reset_task.done():
            return call_result.Reset(status="Accepted")
        if self._reset_blocked or self.manager._closed.is_set() or self.stopping:
            return call_result.Reset(status="Rejected")
        # A start with an unknown remote outcome cannot be erased or replayed.
        # Reject even a reserved/in-flight start so no deferred task can arm a
        # head after the reset has checked for active sessions.
        if (any(row["pending"] for row in self.heads.values())
                or self.manager.db.execute("SELECT 1 FROM sessions WHERE state IN "
                    "('reserved','starting','unknown') LIMIT 1").fetchone() is not None):
            self.event("reset_rejected_pending_start")
            return call_result.Reset(status="Rejected")
        for cid in (1, 2):
            saved = self.manager._live(cid)
            if saved is not None and saved["local_id"] != self._active_transaction_id(cid):
                self.event("reset_rejected_inconsistent_session", connector=cid)
                return call_result.Reset(status="Rejected")
        self._dual_ready = False
        reset_type = kwargs.get("type", "Soft")
        self.event("simulated_reset_accepted", reset_type=reset_type)
        self._reset_task = self.spawn(self._simulated_reset(reset_type))
        return call_result.Reset(status="Accepted")

    async def _simulated_reset(self, reset_type):
        try:
            # As in CP.py, return Reset.conf before sending outbound Stop calls
            # on the same OCPP connection.
            await asyncio.sleep(self._reset_reply_delay)
            reason = "SoftReset" if reset_type == "Soft" else "HardReset"
            for cid in (1, 2):
                if self._active_transaction_id(cid) > 0:
                    self.stopping.add(cid)
                    await self.stop_transaction(cid, reason)
            deadline = time.monotonic() + self._reset_wait_seconds
            while self.manager.db.execute("SELECT 1 FROM outbox WHERE state != 'delivered' LIMIT 1").fetchone():
                if self.manager._closed.is_set():
                    return
                if time.monotonic() >= deadline:
                    raise TimeoutError("Reset outbox delivery did not finish")
                await asyncio.sleep(.05)
            # Closing only the simulated CP sockets makes each manager loop
            # register again using the saved Boot payload. The durable manager
            # and its database remain alive; no old transaction is resumed.
            previous = dict(self.manager.peers)
            await asyncio.wait_for(asyncio.gather(*(peer._connection.close()
                for peer in previous.values())), timeout=self._reset_wait_seconds)
            # The manager sleeps reconnect_seconds before reconnecting, then
            # needs one connect + BootNotification round trip. A fixed wait equal
            # to the reconnect delay (15 s on the live A/B config) always expired
            # a few hundred ms early, so derive the bound from the config.
            register_wait = max(self._reset_wait_seconds,
                                float(self.manager.config["reconnect_seconds"])
                                + float(self.manager.config["request_timeout"]) + 5.0)
            deadline = time.monotonic() + register_wait
            while not all(self.manager.ready[name].is_set()
                    and self.manager.peers.get(name) is not previous.get(name)
                    for name in self.manager.servers):
                if self.manager._closed.is_set():
                    return
                if time.monotonic() >= deadline:
                    raise TimeoutError("Reset registration did not finish")
                await asyncio.sleep(.05)
            if not self.manager._closed.is_set():
                self._smart_suspended.clear()
                await self.status(0, "Available")
                for cid in (1, 2):
                    await self.status(cid, "Available")
                self._dual_ready = True
                self.event("simulated_reset_completed", reset_type=reset_type,
                           physical_reboot=False, transactions_resumed=False)
        except asyncio.CancelledError:
            self.event("simulated_reset_cancelled", start_gate_open=False)
            raise
        except Exception as error:
            self._reset_blocked = True
            self.event("simulated_reset_blocked", error_type=type(error).__name__, start_gate_open=False)

    async def on_change_configuration(self, **kwargs):
        return call_result.ChangeConfiguration(status="Rejected")

    async def on_set_charging_profile(self, **kwargs):
        if self._full_protocol:
            return await self._ocpp_protocol().set_profile(**kwargs)
        return call_result.SetChargingProfile(status="Rejected")

    async def on_clear_chargingprofile(self, **kwargs):
        if self._full_protocol:
            return await self._ocpp_protocol().clear_profiles(**kwargs)
        return call_result.ClearChargingProfile(status="Unknown")

    async def on_get_compositeSchedul(self, **kwargs):
        if self._full_protocol:
            return await self._ocpp_protocol().composite(**kwargs)
        return call_result.GetCompositeSchedule(status="Rejected")

    async def _service(self, method, **kwargs):
        if not self._full_protocol:
            raise DualCSMSError("Enable --full-protocol for the new services")
        from helpf.dual_csms import command_origin
        return await getattr(self._ocpp_protocol().services, method)(command_origin("A"), **kwargs)

    async def on_change_availability(self, **kwargs):
        return await self._service("change_availability", **kwargs)

    async def on_reserve_now(self, **kwargs):
        return await self._service("reserve_now", **kwargs)

    async def on_cancel_reservation(self, **kwargs):
        return await self._service("cancel_reservation", **kwargs)

    async def on_get_diagnostics(self, **kwargs):
        return await self._service("get_diagnostics", **kwargs)

    async def on_update_firmware(self, **kwargs):
        return await self._service("update_firmware", **kwargs)

    async def on_clear_cache(self, **kwargs):
        return call_result.ClearCache(status="Rejected")

    async def on_send_local_list(self, **kwargs):
        return call_result.SendLocalList(status="Failed")

    async def on_unlock_connector(self, connector_id=None, **kwargs):
        if not self._full_protocol:
            return call_result.UnlockConnector(status="NotSupported")
        # Match CP.py's pure handler response; no physical lock adapter is
        # invoked by this simulator (and no claim of physical unlock is made).
        try:
            cid = int(connector_id)
        except (TypeError, ValueError):
            return call_result.UnlockConnector(status="UnlockFailed")
        result = "Unlocked" if cid in (1, 2) else "NotSupported"
        self.event("simulated_unlock_result", connector=cid, status=result, physical_unlock=False)
        return call_result.UnlockConnector(status=result)


async def run(args, config, directory):
    manager = DualCSMSManager(config, str(directory))
    core = SimCore(manager, directory, args.meter_period, getattr(args, "full_protocol", False))
    manager_task = meter_task = control_task = protocol_task = None
    stopping = asyncio.Event()
    loop = asyncio.get_running_loop()
    previous_handlers = {}
    for name in ("SIGINT", "SIGTERM"):
        signum = getattr(signal, name, None)
        if signum is not None:
            previous_handlers[signum] = signal.getsignal(signum)
            signal.signal(signum, lambda *_: loop.call_soon_threadsafe(stopping.set))
    try:
        manager.bind_core(core)
        if core._full_protocol:
            await core._ocpp_protocol().bootstrap()
            protocol_task = asyncio.create_task(core._ocpp_protocol().run())
        manager_task = asyncio.create_task(manager.start())
        serial = "SIM-" + hashlib.sha256("|".join(config["_simulator_identities"]).encode()).hexdigest()[:12]
        await manager.call(call.BootNotification(charge_point_vendor="LocalTest",
            charge_point_model="Dual-CSMS Simulator", charge_point_serial_number=serial,
            firmware_version="SIM-R3.1-2026-11-08" if core._full_protocol else "SIM-2026-11-07"))
        core._dual_ready = True
        core.event("simulator_started", test_identities=config["_simulator_identities"],
                   automatic_transactions=False, hardware="simulated", meter_increment_wh=5)
        await core.status(0, "Available")
        for cid in (1, 2):
            row = core.heads[str(cid)]
            await core.status(cid, "Charging" if row["local_id"] else "Faulted" if row["pending"] else "Available")
        meter_task = asyncio.create_task(core.meter_loop())
        if getattr(args, "control_dir", None):
            from tools.simulator_test_controls import control_loop
            control_task = asyncio.create_task(control_loop(core, args.control_dir))
        deadline = time.monotonic() + args.duration if args.duration else None
        last_ready = None
        while not stopping.is_set():
            ready = {name: event.is_set() for name, event in manager.ready.items()}
            if ready != last_ready:
                core.event("registration_state", ready=ready, both_registered=all(ready.values()))
                last_ready = ready
            if manager_task.done():
                manager_task.result()
                raise RuntimeError("Manager stopped unexpectedly")
            if meter_task.done():
                meter_task.result()
                raise RuntimeError("Meter loop stopped unexpectedly")
            if protocol_task is not None and protocol_task.done():
                protocol_task.result()
                raise RuntimeError("Protocol service loop stopped unexpectedly")
            if deadline and time.monotonic() >= deadline:
                break
            if getattr(args, "stop_file", None) and args.stop_file.exists():
                core.event("stop_file_received")
                break
            try:
                await asyncio.wait_for(stopping.wait(), .25)
            except asyncio.TimeoutError:
                pass
    finally:
        core._dual_ready = False
        if protocol_task is not None:
            protocol_task.cancel()
            await asyncio.gather(protocol_task, return_exceptions=True)
        if control_task is not None:
            control_task.cancel()
            await asyncio.gather(control_task, return_exceptions=True)
        if meter_task is not None:
            meter_task.cancel()
            await asyncio.gather(meter_task, return_exceptions=True)
        if core.tasks:
            _, pending = await asyncio.wait(list(core.tasks), timeout=config["request_timeout"] + 2)
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
        for cid in (1, 2):
            if core._active_transaction_id(cid) > 0:
                try:
                    await core.stop_transaction(cid, "Local")
                except Exception as error:
                    core.event("shutdown_stop_failed", connector=cid, error_type=type(error).__name__)
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            remaining = manager.db.execute("SELECT count(*) FROM outbox WHERE state IN ('pending','sending')").fetchone()[0]
            if remaining == 0:
                break
            await asyncio.sleep(.25)
        core.event("simulator_stopped", outbox=[dict(row) for row in manager.db.execute(
            "SELECT server,action,state,count(*) AS count FROM outbox GROUP BY server,action,state")],
            unresolved_sessions=[{"connector": row["connector"], "owner": row["owner"], "state": row["state"]}
                for row in manager.db.execute("SELECT connector,owner,state FROM sessions WHERE state NOT IN ('closed','cancelled')")])
        await manager.close()
        if manager_task is not None:
            await asyncio.gather(manager_task, return_exceptions=True)
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True, help="Dedicated config containing simulator:true and TEST/SIM identities")
    parser.add_argument("--state-dir", type=Path, required=True, help="Dedicated empty simulator state directory")
    parser.add_argument("--duration", type=float, default=0, help="Seconds before graceful shutdown; 0 waits for Ctrl+C")
    parser.add_argument("--meter-period", type=float, default=5, help="Seconds per simulated 5 Wh increment")
    parser.add_argument("--stop-file", type=Path, help="Optional file whose creation requests graceful shutdown")
    parser.add_argument("--protocol-log", type=Path, help="Optional redacted OCPP wire journal for testing")
    parser.add_argument("--control-dir", type=Path, help="Optional local simulator test-event queue")
    parser.add_argument("--full-protocol", action="store_true", help="Use actual CP configuration/auth methods and the production R3 protocol services with simulated hardware")
    args = parser.parse_args(argv)
    if args.duration < 0 or args.meter_period < .2:
        parser.error("duration must be nonnegative and meter-period at least 0.2 seconds")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    # The OCPP library's INFO wire log includes idTags. Keep structured redacted
    # simulator events while still exposing per-server manager registration logs.
    logging.getLogger("ocpp").setLevel(logging.WARNING)
    config = simulator_config(args.config)
    directory = prepare_state(args.state_dir)
    print("SIMULATED HARDWARE ONLY. Transactions start only on incoming test CSMS commands.", flush=True)
    print("State directory:", directory, flush=True)
    from tools.simulator_test_controls import protocol_trace
    with simulator_lock(directory), protocol_trace(args.protocol_log):
        asyncio.run(run(args, config, directory))


if __name__ == "__main__":
    main()
