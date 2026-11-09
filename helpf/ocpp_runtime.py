"""Shared OCPP service/scheduling integration, with injected hardware adapters.

No GPIO, PLC, serial or operating-system commands are imported by this module.
The production CP and the Windows protocol harness use these same services.
"""
from __future__ import annotations

import asyncio
import dataclasses
import inspect
import json
import logging
import math
import time
from contextvars import ContextVar
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from ocpp.v16 import call, call_result
from helpf.dual_csms import command_origin
from helpf.ocpp_services import OCPPServices, HTTPTransferPolicy
from helpf.ocpp_smart_charging import SmartChargingController

LOG = logging.getLogger("ocpp_runtime")
_SERVICE_STATUS = ContextVar("ocpp_service_status", default=False)
_TRIGGER_METER = ContextVar("ocpp_trigger_meter", default=False)


def service_status_active():
    """True while publish_status() sends a status from this service layer
    (availability, reservation, startup announcement or limit fault)."""
    return _SERVICE_STATUS.get()


class OCPPProtocolRuntime:
    def __init__(self, core, directory, config=None):
        self.core = core
        self.directory = Path(directory).resolve()
        self.directory.mkdir(parents=True, exist_ok=True)
        self.config = config or {}
        self.manager = getattr(core, "_dual_manager", None)
        self.local_owner = self.manager.config["local_csms"] if self.manager else "A"
        # [R3.1.3] the first admin: the default origin when a command carries none
        self.admin_owner = self.manager.config["primary_admin"] if self.manager else "A"
        self.physical = {0: "Available", 1: "Available", 2: "Available"}
        self.faulted = set()
        self.started_at = {}
        self._closed = False
        self._bootstrap_lock = asyncio.Lock()
        self.ready = False
        self._resolved_parents = {}
        transfer_cfg = self.config.get("transfer", {})
        transfer = HTTPTransferPolicy(
            transfer_cfg.get("allowed_hosts", []),
            allow_http_hosts=transfer_cfg.get("allow_http_hosts", []),
            timeout=transfer_cfg.get("timeout", 20),
            max_download_bytes=transfer_cfg.get("max_download_bytes", 64 * 1024 * 1024),
            max_upload_bytes=transfer_cfg.get("max_upload_bytes", 4 * 1024 * 1024))
        sources = [self.directory / name for name in self.config.get("diagnostic_sources", ["ocpp_runtime_events.jsonl"])]
        for source in sources:
            source.resolve().relative_to(self.directory)
        self.services = OCPPServices(
            self.directory, is_active=self.is_active, physical_status=self.base_status,
            publish_status=self.publish_status, publish_message=self.publish_message,
            transfer=transfer, diagnostic_sources=sources,
            allow_connector_zero_reservation=False,
            trusted_firmware_sha256=self.config.get("trusted_firmware_sha256", {}))
        self.smart = SmartChargingController(
            self.directory / "ocpp_charging_profiles.json", self.apply_limits,
            active_transaction=self.transaction_id, transaction_started_at=self.transaction_time,
            rated_power_w=self.config.get("rated_power_w", 180000),
            rated_current_a=self.config.get("rated_current_a", 500),
            station_rated_power_w=self.config.get("station_rated_power_w", 180000),
            fault_callback=self.limit_fault)
        self._legacy_profiles = []
        self.core.SupportedFeatureProfiles_cfg = "Core,LocalAuthListManagement,RemoteTrigger,SmartCharging,Reservation"
        self.journal("runtime_initialized", firmware_mode="download_and_stage")
        unit_file, limit_file = self.directory / "charging_rate_unit.txt", self.directory / "limit.txt"
        if not self.smart.path.exists() and (unit_file.exists() or limit_file.exists()):
            unit = unit_file.read_text(encoding="utf-8").strip()
            limit = float(limit_file.read_text(encoding="utf-8").strip())
            if unit not in ("W", "A") or not math.isfinite(limit) or limit < 0:
                raise ValueError("Legacy charging limit requires inspection before migration")
            limit = min(limit, self.smart.rated[unit])
            marked = [cid for cid in (0, 1, 2) if (self.directory / f"status_limit{cid}.txt").exists()
                      and (self.directory / f"status_limit{cid}.txt").read_text().strip() == "1"]
            # R2's A branch did not set flags. Applying its shared cap to both
            # heads is conservative when the original target cannot be known.
            targets = marked or [1, 2]
            for cid in targets:
                self._legacy_profiles.append((cid, {
                    "charging_profile_id": 900000000 + cid, "stack_level": 0,
                    "charging_profile_purpose": "ChargePointMaxProfile" if cid == 0 else "TxDefaultProfile",
                    "charging_profile_kind": "Absolute", "charging_schedule": {
                        "start_schedule": datetime.now(timezone.utc).isoformat(), "charging_rate_unit": unit,
                        "charging_schedule_period": [{"start_period": 0, "limit": limit}]}}))

    async def bootstrap(self):
        async with self._bootstrap_lock:
            if self.ready:
                return
            if self._legacy_profiles and not await self.smart.import_legacy_profiles(self._legacy_profiles):
                raise RuntimeError("Could not migrate the persisted legacy limit")
            self._legacy_profiles = []
            if not await self.smart.tick():
                raise RuntimeError("Smart charging limits could not be established")
            self.ready = True

    def journal(self, event, **values):
        # Only call sites with selected non-secret scalar fields enter this log.
        record = {"timestamp": datetime.now(timezone.utc).isoformat(), "event": event, **values}
        try:
            with (self.directory / "ocpp_runtime_events.jsonl").open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(record, ensure_ascii=False) + "\n")
        except OSError:
            LOG.warning("Could not append the protocol service journal")

    def transaction_id(self, connector):
        if self.manager:
            row = self.manager._live(connector)
            if row and row["state"] == "active":
                return row["local_id"]
        return int(self.core._active_transaction_id(connector) or 0)

    def is_active(self, connector):
        if connector not in (1, 2):
            return False
        if self.transaction_id(connector) > 0:
            return True
        if self.manager and self.manager.connector_reserved(connector):
            return True
        pending = getattr(self.core, "_ocpp_pending_start", None)
        return bool(pending(connector)) if pending else False

    def transaction_time(self, connector):
        if self.manager:
            row = self.manager._live(connector)
            if row and row["start_payload"]:
                return json.loads(row["start_payload"]).get("timestamp")
        return self.started_at.get(connector)

    def base_status(self, connector):
        if connector in self.faulted:
            return "Faulted"
        provider = getattr(self.core, "_ocpp_physical_status", None)
        if provider:
            return str(provider(connector))
        return self.physical.get(connector, "Available")

    def filter_status(self, payload):
        if type(payload).__name__ == "MeterValues" and _TRIGGER_METER.get():
            meters = json.loads(json.dumps(payload.meter_value))
            for meter in meters:
                for sample in meter.get("sampled_value", meter.get("sampledValue", [])):
                    sample["context"] = "Trigger"
            return dataclasses.replace(payload, meter_value=meters)
        if type(payload).__name__ != "StatusNotification":
            return payload
        cid = payload.connector_id
        if not _SERVICE_STATUS.get():
            self.physical[cid] = str(payload.status)
            self.journal("physical_status", connector=cid, status=str(payload.status))
        status = self.services.effective_status(cid, str(payload.status))
        if cid in self.faulted:
            status = "Faulted"
        if status != str(payload.status):
            return dataclasses.replace(payload, status=status,
                                       error_code="OtherError" if cid in self.faulted else payload.error_code)
        return payload

    async def publish_status(self, connector, status):
        token = _SERVICE_STATUS.set(True)
        try:
            await self.core.call(call.StatusNotification(
                connector_id=connector, status=status, error_code="OtherError" if connector in self.faulted else "NoError",
                timestamp=datetime.now(timezone.utc).isoformat()))
        finally:
            _SERVICE_STATUS.reset(token)

    async def publish_message(self, owner, payload):
        if self.manager:
            return await self.manager.call_service(owner, payload)
        return await self.core.call(payload)

    async def apply_limits(self, connector, limits):
        result = self.core._ocpp_apply_limits(connector, dict(limits))
        if inspect.isawaitable(result):
            result = await result
        if result is False:
            raise RuntimeError("Hardware limit adapter rejected the update")
        self.journal("limit_command_published", connector=connector, power_w=limits["W"], current_a=limits["A"])

    async def limit_fault(self, connector, reason):
        self.faulted.add(connector)
        await self.publish_status(connector, "Faulted")
        stopper = getattr(self.core, "_ocpp_limit_fault_stop", None)
        if stopper:
            result = stopper(connector)
            if inspect.isawaitable(result):
                await result

    def parent_tag(self, tag, owner=None):
        owner = owner or self.local_owner
        cache_key = (owner, tag)
        if cache_key in self._resolved_parents:
            value, expires = self._resolved_parents[cache_key]
            if expires > time.monotonic():
                return value
            self._resolved_parents.pop(cache_key, None)
        if owner != self.local_owner:
            return None
        for name in ("auth_cache", "authorization_cache", "local_auth_list"):
            cache = getattr(self.core, name, None)
            if cache:
                getter = getattr(cache, "get", None)
                info = getter(tag) if getter else getattr(cache, "identifiers", {}).get(tag)
                if isinstance(info, dict) and info.get("status") == "Accepted":
                    expiry = info.get("expiry_date", info.get("expiryDate"))
                    if expiry:
                        try:
                            if datetime.fromisoformat(expiry.replace("Z", "+00:00")) <= datetime.now(timezone.utc):
                                continue
                        except (TypeError, ValueError):
                            continue
                    return info.get("parentIdTag", info.get("parent_id_tag"))
        return None

    def can_start(self, connector, owner, tag):
        return (self.ready and connector not in self.faulted and not self.smart.inhibited
                and self.services.can_start(connector, owner, tag, self.parent_tag(tag, owner)))

    def start_refusal(self, connector):
        """Why a new start is refused, for the log only. Never contains an idTag."""
        if not self.ready:
            return "protocol runtime not ready"
        if connector in self.faulted:
            return "connector faulted after a PLC limit failure"
        if self.smart.inhibited:
            return "smart charging inhibited (%s)" % (self.smart.last_error or "unknown error")
        if not self.services._operative(connector):
            return "connector Inoperative"
        status = str(self.services.physical_status(connector))
        if status not in ("Available", "Preparing", "Reserved"):
            return "connector status %s" % status
        if any(row["connector_id"] in (0, connector) for row in self.services._reservations()):
            return "reserved for another idTag or CSMS"
        return "start not allowed"

    def start_admission(self, connector, owner, tag):
        """None requests async parent verification after the CALLRESULT is sent."""
        if connector in self.faulted or self.smart.inhibited:
            return False
        if self.services.can_start(connector, owner, tag, self.parent_tag(tag, owner)):
            return True
        if (self.services._operative(connector) and self.base_status(connector) in ("Available", "Preparing", "Reserved")
                and any(r["owner"] == owner and r["connector_id"] in (0, connector) and r.get("parent_id_tag")
                        for r in self.services._reservations())):
            return None
        return False

    async def start_allowed(self, connector, owner, tag):
        await self.bootstrap()
        if self.can_start(connector, owner, tag):
            return True
        if connector in self.faulted or self.smart.inhibited:
            return False
        # Only a reservation held by the same CSMS can require parent-tag
        # resolution. Never authorize through the other CSMS or log the tag.
        reservations = self.services._reservations()
        needs_parent = any(r["owner"] == owner and r["connector_id"] in (0, connector)
                           and r.get("parent_id_tag") for r in reservations)
        if not needs_parent:
            return False
        try:
            result = await self.publish_message(owner, call.Authorize(id_tag=tag))
            info = result.id_tag_info
            if info.get("status") != "Accepted":
                return False
            parent = info.get("parent_id_tag", info.get("parentIdTag"))
            self._resolved_parents[(owner, tag)] = (parent, time.monotonic() + 60)
            return self.services.can_start(connector, owner, tag, parent)
        except Exception:
            return False

    def reservation_id(self, connector, owner, tag):
        return self.services.reservation_id_for(connector, owner, tag, self.parent_tag(tag, owner))

    async def transaction_started(self, connector, owner, tag, local_id, *, started_at=None):
        self.started_at[connector] = started_at or datetime.now(timezone.utc)
        await self.services.transaction_started(connector, owner, tag, self.parent_tag(tag, owner))
        self._resolved_parents.pop((owner, tag), None)
        self.journal("transaction_confirmed", connector=connector, owner=owner, local_transaction=local_id)
        await self.smart.tick()

    async def set_profile(self, **kwargs):
        await self.bootstrap()
        return call_result.SetChargingProfile(status=await self.smart.set_profile(
            kwargs.get("connector_id"), kwargs.get("cs_charging_profiles")))

    async def clear_profiles(self, **kwargs):
        await self.bootstrap()
        return call_result.ClearChargingProfile(status=await self.smart.clear_profiles(
            profile_id=kwargs.get("id"), connector_id=kwargs.get("connector_id"),
            charging_profile_purpose=kwargs.get("charging_profile_purpose"), stack_level=kwargs.get("stack_level")))

    async def composite(self, **kwargs):
        await self.bootstrap()
        return call_result.GetCompositeSchedule(**await self.smart.composite_schedule(
            kwargs.get("connector_id"), kwargs.get("duration"), kwargs.get("charging_rate_unit")))

    async def run(self):
        await self.bootstrap()
        await self.services.start()
        try:
            while not self._closed:
                await self.services.tick()
                await self.smart.tick()
                await asyncio.sleep(.25)
        finally:
            await self.services.close()

    async def close(self):
        self._closed = True
        await self.services.close()


class OCPPProtocolMixin:
    """Thin shared hooks; concrete CP owns its GPIO/PLC adapter methods."""
    def _ocpp_protocol(self):
        runtime = getattr(self, "_protocol_runtime", None)
        if runtime is None:
            directory = Path(getattr(self, "_ocpp_state_dir", Path.cwd()))
            config_path = directory / "ocpp_services_config.json"
            config = json.loads(config_path.read_text(encoding="utf-8-sig")) if config_path.exists() else {}
            runtime = OCPPProtocolRuntime(self, directory, config)
            self._protocol_runtime = runtime
        return runtime

    async def _ocpp_start_allowed(self, connector, owner, tag):
        return await self._ocpp_protocol().start_allowed(connector, owner, tag)

    def _ocpp_start_refusal(self, connector):
        return self._ocpp_protocol().start_refusal(connector)

    def _ocpp_start_admission(self, connector, owner, tag):
        return self._ocpp_protocol().start_admission(connector, owner, tag)

    def _ocpp_reservation_id(self, connector, owner, tag):
        return self._ocpp_protocol().reservation_id(connector, owner, tag)

    def _ocpp_start_operational(self, connector):
        runtime = self._ocpp_protocol()
        return connector not in runtime.faulted and not runtime.smart.inhibited

    async def _ocpp_transaction_started(self, connector, owner, tag, local_id):
        await self._ocpp_protocol().transaction_started(connector, owner, tag, local_id)

    async def _ocpp_trigger_service_status(self, owner, requested):
        await self._ocpp_protocol().services.send_current_status(owner, requested)

    async def send_triggered_meter_values(self, connector):
        token = _TRIGGER_METER.set(True)
        try:
            await self.send_meter_values(connector)
        finally:
            _TRIGGER_METER.reset(token)

    def _ocpp_hardware_start_allowed(self, connector):
        runtime = self._ocpp_protocol()
        owner = runtime.local_owner
        if runtime.manager:
            row = runtime.manager._live(connector)
            if row:
                owner = row["owner"]
        tag = str(getattr(self, f"myid_tag_c{connector}", ""))
        return runtime.can_start(connector, owner, tag)
