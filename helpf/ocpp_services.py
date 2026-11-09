"""OCPP 1.6 availability, reservations and managed file-transfer services.

This module has no hardware imports and executes no shell commands. The owner
argument is the originating CSMS name supplied by the dual-CSMS dispatcher.
Call publish_message(owner, request) to route progress back to that peer.

UpdateFirmware currently implements the explicitly selected *staging* workflow:
download, optional trusted digest validation, atomic storage and Downloaded.
It never reports Installing/Installed or executes downloaded content.
"""
from __future__ import annotations

import asyncio
import copy
import hashlib
import inspect
import json
import os
import re
import ssl
import time
import uuid
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote, urlsplit, urlunsplit
from urllib.request import HTTPRedirectHandler, HTTPSHandler, ProxyHandler, Request, build_opener

from ocpp.v16 import call, call_result


def utc_now():
    return datetime.now(timezone.utc)


def date_time(value):
    if isinstance(value, datetime):
        result = value
    elif isinstance(value, str):
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        raise ValueError("An ISO date/time with timezone is required")
    if result.tzinfo is None:
        raise ValueError("A timezone is required")
    return result.astimezone(timezone.utc)


async def invoke(callback, *args):
    result = callback(*args)
    return await result if inspect.isawaitable(result) else result


def _connector(value, allow_zero=False):
    return type(value) is int and value in ((0, 1, 2) if allow_zero else (1, 2))


def _tag(value):
    return isinstance(value, str) and 0 < len(value) <= 20 and value.strip() == value


def _owner(value):
    return isinstance(value, str) and bool(value) and len(value) <= 64


class TransferRejected(ValueError):
    """A bounded transfer policy refused this destination or file."""


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, msg, headers, newurl):
        raise TransferRejected("File-transfer redirects are not allowed")


class HTTPTransferPolicy:
    """Bounded HTTP(S) transfers to explicitly configured hostnames only.

    HTTP is accepted only for names also listed in allow_http_hosts. Certificate
    verification remains enabled for HTTPS. FTP, URL credentials and redirects
    are unsupported. URL values are deliberately absent from exception text.
    """

    def __init__(self, allowed_hosts=(), *, allow_http_hosts=(), timeout=20,
                 max_download_bytes=64 * 1024 * 1024, max_upload_bytes=4 * 1024 * 1024,
                 upload_method="PUT", append_upload_filename=True, max_download_seconds=120):
        self.allowed_hosts = {str(name).lower() for name in allowed_hosts}
        self.allow_http_hosts = {str(name).lower() for name in allow_http_hosts}
        self.timeout = float(timeout)
        self.max_download_bytes = int(max_download_bytes)
        self.max_upload_bytes = int(max_upload_bytes)
        self.max_download_seconds = float(max_download_seconds)
        if not 0 < self.timeout <= 120 or min(self.max_download_bytes, self.max_upload_bytes) <= 0:
            raise ValueError("Invalid transfer resource bounds")
        if not 0 < self.max_download_seconds <= 3600:
            raise ValueError("Invalid transfer duration bound")
        if upload_method not in ("PUT", "POST"):
            raise ValueError("Use PUT or POST for diagnostics")
        self.upload_method = upload_method
        self.append_upload_filename = bool(append_upload_filename)
        self.opener = build_opener(ProxyHandler({}), _NoRedirect(),
                                   HTTPSHandler(context=ssl.create_default_context()))

    def validate_url(self, location):
        if not isinstance(location, str) or len(location) > 4096:
            raise TransferRejected("Invalid transfer destination")
        try:
            url = urlsplit(location)
            host = (url.hostname or "").lower()
            port = url.port
        except ValueError:
            raise TransferRejected("Invalid transfer destination") from None
        if (url.scheme not in ("http", "https") or not host or
                host not in self.allowed_hosts or url.username or url.password or url.fragment or
                any(c in location for c in ("\r", "\n", "\x00")) or port == 0):
            raise TransferRejected("Transfer destination is outside the configured policy")
        if url.scheme == "http" and host not in self.allow_http_hosts:
            raise TransferRejected("Unencrypted transfers are not enabled for this host")
        return location

    async def upload(self, location, path):
        self.validate_url(location)
        path = Path(path)
        if not path.is_file() or path.stat().st_size > self.max_upload_bytes:
            raise TransferRejected("Diagnostics archive exceeds the transfer limit")
        url = urlsplit(location)
        if self.append_upload_filename:
            location = urlunsplit((url.scheme, url.netloc,
                                  url.path.rstrip("/") + "/" + quote(path.name), url.query, ""))
        return await asyncio.to_thread(self._upload, location, path)

    def _upload(self, location, path):
        data = path.read_bytes()
        if len(data) > self.max_upload_bytes:
            raise TransferRejected("Diagnostics archive exceeds the transfer limit")
        request = Request(location, data=data, method=self.upload_method,
                          headers={"Content-Type": "application/zip",
                                   "Content-Disposition": 'attachment; filename="' + path.name + '"'})
        with self.opener.open(request, timeout=self.timeout) as response:
            if not 200 <= response.status < 300:
                raise TransferRejected("Diagnostics upload was not acknowledged")
        return True

    async def download(self, location, destination):
        self.validate_url(location)
        return await asyncio.to_thread(self._download, location, Path(destination))

    def _download(self, location, destination):
        destination.parent.mkdir(parents=True, exist_ok=True)
        partial = destination.with_name(destination.name + "." + uuid.uuid4().hex + ".part")
        digest, total = hashlib.sha256(), 0
        deadline = time.monotonic() + self.max_download_seconds
        try:
            with self.opener.open(Request(location, method="GET"), timeout=self.timeout) as response:
                if not 200 <= response.status < 300:
                    raise TransferRejected("Firmware download was not acknowledged")
                length = response.headers.get("Content-Length")
                if length is not None and (not length.isdigit() or int(length) > self.max_download_bytes):
                    raise TransferRejected("Firmware exceeds the configured download limit")
                with partial.open("xb") as stream:
                    while True:
                        if time.monotonic() > deadline:
                            raise TransferRejected("Firmware download exceeded the configured duration")
                        block = response.read1(65536)
                        if not block:
                            break
                        total += len(block)
                        if total > self.max_download_bytes:
                            raise TransferRejected("Firmware exceeds the configured download limit")
                        digest.update(block)
                        stream.write(block)
                    stream.flush()
                    os.fsync(stream.fileno())
            if total == 0:
                raise TransferRejected("Empty firmware download")
            if length is not None and total != int(length):
                raise TransferRejected("Firmware download ended before its declared length")
            os.replace(partial, destination)
            return {"path": str(destination), "sha256": digest.hexdigest(), "bytes": total}
        finally:
            if partial.exists():
                partial.unlink()


class OCPPServices:
    """Persistent logical services; the caller owns physical actuation.

    is_active and physical_status are synchronous, cached state callbacks. They
    must not perform blocking PLC reads. physical_status returns the physical
    base status before effective_status adds administrative overrides.
    """

    def __init__(self, state_dir, *, is_active, physical_status, publish_status,
                 publish_message, transfer=None, clock=utc_now, diagnostic_sources=(),
                 allow_connector_zero_reservation=False, reservations_enabled=True,
                 trusted_firmware_sha256=None, verify_firmware=None, installer=None,
                 max_staging_bytes=256 * 1024 * 1024):
        self.directory = Path(state_dir).resolve()
        self.directory.mkdir(parents=True, exist_ok=True)
        self.path = self.directory / "ocpp_services_state.json"
        self.is_active, self.physical_status = is_active, physical_status
        self.publish_status, self.publish_message = publish_status, publish_message
        self.transfer = transfer or HTTPTransferPolicy()
        self.clock = clock
        self.diagnostic_sources = tuple(Path(p) for p in diagnostic_sources)
        if len(self.diagnostic_sources) > 16:
            raise ValueError("At most sixteen explicitly selected diagnostics sources are allowed")
        self.allow_connector_zero_reservation = bool(allow_connector_zero_reservation)
        self.reservations_enabled = bool(reservations_enabled)
        self.trusted_firmware_sha256 = dict(trusted_firmware_sha256 or {})
        if any(not isinstance(location, str) or not isinstance(value, str)
               or not re.fullmatch(r"[0-9a-fA-F]{64}", value)
               for location, value in self.trusted_firmware_sha256.items()):
            raise ValueError("Trusted firmware digests must be SHA-256 hexadecimal strings")
        self.verify_firmware = verify_firmware
        # Deliberately retained as a future adapter hook, not invoked in staging mode.
        self.installer = installer
        self.firmware_mode = "download_and_stage"
        self.max_staging_bytes = int(max_staging_bytes)
        if self.max_staging_bytes <= 0:
            raise ValueError("Firmware staging capacity must be positive")
        self.tasks = set()
        self.background_errors = []
        self._running_jobs = set()
        self._draining = False
        self._diagnostic_building = False
        self._closed = False
        self._dirty = {0, 1, 2}
        default = {"schema": 1, "availability": {str(cid): "Operative" for cid in (0, 1, 2)},
                   "pending_availability": {}, "reservations": [], "notifications": [],
                   "last_status": {}, "jobs": {}}
        if self.path.exists():
            if self.path.stat().st_size > 2 * 1024 * 1024:
                raise ValueError("OCPP services state is too large; preserve it for inspection")
            loaded = json.loads(self.path.read_text(encoding="utf-8"))
            self._validate_state(loaded)
            default.update(loaded)
        self.state = default

    @staticmethod
    def _validate_state(state):
        if not isinstance(state, dict) or state.get("schema") != 1:
            raise ValueError("Unsupported OCPP services state; preserve it for inspection")
        availability = state.get("availability")
        pending = state.get("pending_availability")
        if (not isinstance(availability, dict) or set(availability) != {"0", "1", "2"}
                or any(v not in ("Operative", "Inoperative") for v in availability.values())
                or not isinstance(pending, dict) or not set(pending) <= {"0", "1", "2"}
                or any(v not in ("Operative", "Inoperative") for v in pending.values())):
            raise ValueError("Invalid persisted availability; preserve it for inspection")
        reservations = state.get("reservations")
        notifications = state.get("notifications")
        jobs = state.get("jobs")
        last = state.get("last_status")
        if (not isinstance(reservations, list) or len(reservations) > 2
                or not isinstance(notifications, list) or not isinstance(jobs, dict)
                or not isinstance(last, dict)):
            raise ValueError("Invalid OCPP services state collections")
        for row in reservations:
            if (not isinstance(row, dict) or not _owner(row.get("owner"))
                    or not _connector(row.get("connector_id"), True)
                    or type(row.get("reservation_id")) is not int or row["reservation_id"] < 0
                    or not _tag(row.get("id_tag"))
                    or (row.get("parent_id_tag") is not None and not _tag(row["parent_id_tag"]))):
                raise ValueError("Invalid persisted reservation")
            date_time(row.get("expiry_date"))
        for row in notifications:
            if (not isinstance(row, dict) or not _owner(row.get("owner"))
                    or row.get("action") not in ("DiagnosticsStatusNotification", "FirmwareStatusNotification")
                    or not re.fullmatch(r"[0-9a-f]{32}", str(row.get("id", "")))):
                raise ValueError("Invalid persisted notification")
            allowed = ({"Idle", "Uploading", "Uploaded", "UploadFailed"} if row["action"] == "DiagnosticsStatusNotification"
                       else {"Idle", "Downloading", "Downloaded", "DownloadFailed", "Installing", "Installed", "InstallationFailed"})
            if row.get("status") not in allowed:
                raise ValueError("Invalid persisted progress status")
        for job_id, row in jobs.items():
            if (not re.fullmatch(r"[0-9a-f]{32}", job_id) or not isinstance(row, dict)
                    or not _owner(row.get("owner")) or row.get("kind") not in ("firmware", "diagnostics")
                    or row.get("phase") not in ("waiting", "downloading", "uploading", "uploaded", "downloaded", "failed")):
                raise ValueError("Invalid persisted transfer job")
            if row["kind"] == "diagnostics" and not re.fullmatch(r"diagnostics-[0-9a-f]{32}\.zip", str(row.get("archive_name", ""))):
                raise ValueError("Invalid persisted diagnostics filename")
            if row["kind"] == "firmware":
                date_time(row.get("retrieve_date"))
            OCPPServices._retry_settings(row.get("retries"), row.get("retry_interval"))

    def _now(self):
        return date_time(self.clock())

    def _commit(self, state):
        temporary = self.path.with_name(self.path.name + "." + uuid.uuid4().hex + ".tmp")
        try:
            with temporary.open("x", encoding="utf-8") as stream:
                json.dump(state, stream, ensure_ascii=False, separators=(",", ":"))
                stream.flush()
                os.fsync(stream.fileno())
            try:
                os.chmod(temporary, 0o600)
            except OSError:
                pass
            os.replace(temporary, self.path)
            self.state = state
        finally:
            if temporary.exists():
                temporary.unlink()

    def _spawn(self, coroutine):
        task = asyncio.create_task(coroutine)
        self.tasks.add(task)
        def completed(done):
            self.tasks.discard(done)
            if not done.cancelled() and done.exception() is not None:
                self.background_errors.append(type(done.exception()).__name__)
                self.background_errors[:] = self.background_errors[-32:]
        task.add_done_callback(completed)
        return task

    def _reservations(self):
        now = self._now()
        return [row for row in self.state["reservations"] if date_time(row["expiry_date"]) > now]

    @staticmethod
    def _matches(row, owner, id_tag, parent_id_tag):
        incoming = {value for value in (id_tag, parent_id_tag) if value}
        reserved = {value for value in (row["id_tag"], row.get("parent_id_tag")) if value}
        return row["owner"] == owner and bool(incoming & reserved)

    def _operative(self, cid):
        return (self.state["availability"].get("0") != "Inoperative" and
                self.state["availability"].get(str(cid)) != "Inoperative" and
                self.state["pending_availability"].get("0") != "Inoperative" and
                self.state["pending_availability"].get(str(cid)) != "Inoperative")

    def can_start(self, connector_id, owner, id_tag, parent_id_tag=None):
        if not _connector(connector_id) or not self._operative(connector_id):
            return False
        if str(self.physical_status(connector_id)) not in ("Available", "Preparing", "Reserved"):
            return False
        reservations = self._reservations()
        specific = [r for r in reservations if r["connector_id"] == connector_id]
        if specific:
            return self._matches(specific[0], owner, id_tag, parent_id_tag)
        floating = [r for r in reservations if r["connector_id"] == 0]
        if not floating:
            # A dispatcher may already hold this connector's pending claim.
            # Busy arbitration belongs to that dispatcher, not this tag gate.
            return True
        if any(self._matches(row, owner, id_tag, parent_id_tag) for row in floating):
            return True
        # An unreserved start may not consume capacity held by floating reservations.
        free = sum(not self.is_active(cid) and self._operative(cid)
                   and str(self.physical_status(cid)) in ("Available", "Preparing", "Reserved")
                   and not any(r["connector_id"] == cid for r in reservations) for cid in (1, 2))
        return free > len(floating)

    def reservation_id_for(self, connector_id, owner, id_tag, parent_id_tag=None):
        for row in sorted(self._reservations(), key=lambda r: r["connector_id"] == 0):
            if row["connector_id"] in (0, connector_id) and self._matches(row, owner, id_tag, parent_id_tag):
                return row["reservation_id"]
        return None

    def effective_status(self, connector_id, physical_status):
        if physical_status == "Faulted":
            return physical_status
        if connector_id in (1, 2) and self.is_active(connector_id):
            return physical_status
        if connector_id == 0 and any(self.is_active(cid) for cid in (1, 2)):
            return physical_status
        if connector_id in (0, 1, 2) and not self._operative(connector_id):
            return "Unavailable"
        if connector_id in (1, 2) and physical_status in ("Available", "Reserved"):
            if any(r["connector_id"] == connector_id for r in self._reservations()):
                return "Reserved"
        return physical_status

    async def change_availability(self, owner, connector_id, type, **kwargs):
        if not _owner(owner) or not _connector(connector_id, True) or type not in ("Operative", "Inoperative"):
            return call_result.ChangeAvailability(status="Rejected")
        targets = (0, 1, 2) if connector_id == 0 else (connector_id,)
        state, scheduled = copy.deepcopy(self.state), False
        for cid in targets:
            active = any(self.is_active(i) for i in (1, 2)) if cid == 0 else self.is_active(cid)
            if state["availability"][str(cid)] == type:
                state["pending_availability"].pop(str(cid), None)
            elif active:
                state["pending_availability"][str(cid)] = type
                scheduled = True
            else:
                state["availability"][str(cid)] = type
                state["pending_availability"].pop(str(cid), None)
        if type == "Inoperative":
            state["reservations"] = [r for r in state["reservations"]
                                     if r["connector_id"] not in targets]
        self._prune_reservations(state)
        self._commit(state)
        self._dirty.update(targets)
        self._spawn(self._announce())
        return call_result.ChangeAvailability(status="Scheduled" if scheduled else "Accepted")

    async def reserve_now(self, owner, connector_id, expiry_date, id_tag, reservation_id,
                          parent_id_tag=None, **kwargs):
        if (not self.reservations_enabled or not _owner(owner) or not _connector(connector_id, True)
                or (connector_id == 0 and not self.allow_connector_zero_reservation)
                or type(reservation_id) is not int or reservation_id < 0 or not _tag(id_tag)
                or (parent_id_tag is not None and not _tag(parent_id_tag))):
            return call_result.ReserveNow(status="Rejected")
        try:
            expiry = date_time(expiry_date)
        except (ValueError, TypeError):
            return call_result.ReserveNow(status="Rejected")
        if expiry <= self._now():
            return call_result.ReserveNow(status="Rejected")
        reservations = self._reservations()
        # Same origin/id replaces its previous reservation only after validating the new request.
        others = [r for r in reservations if (r["owner"], r["reservation_id"]) != (owner, reservation_id)]
        candidates = (1, 2) if connector_id == 0 else (connector_id,)
        available = []
        failures = []
        for cid in candidates:
            physical = str(self.physical_status(cid))
            if physical == "Faulted":
                failures.append("Faulted")
            elif not self._operative(cid) or physical == "Unavailable":
                failures.append("Unavailable")
            elif self.is_active(cid) or physical not in ("Available", "Reserved") or any(r["connector_id"] == cid for r in others):
                failures.append("Occupied")
            else:
                available.append(cid)
        floating = sum(row["connector_id"] == 0 for row in others)
        if not available or (connector_id == 0 and len(available) <= floating):
            status = "Occupied" if "Occupied" in failures or available else failures[0] if failures else "Occupied"
            return call_result.ReserveNow(status=status)
        if connector_id != 0:
            total_free = sum(not self.is_active(cid) and self._operative(cid)
                             and str(self.physical_status(cid)) in ("Available", "Reserved")
                             and not any(r["connector_id"] == cid for r in others) for cid in (1, 2))
            if total_free <= floating:
                return call_result.ReserveNow(status="Occupied")
        state = copy.deepcopy(self.state)
        state["reservations"] = others + [{"owner": owner, "connector_id": connector_id,
            "expiry_date": expiry.isoformat(), "id_tag": id_tag, "parent_id_tag": parent_id_tag,
            "reservation_id": reservation_id}]
        self._commit(state)
        self._dirty.update((1, 2) if connector_id == 0 else (connector_id,))
        self._spawn(self._announce())
        return call_result.ReserveNow(status="Accepted")

    async def cancel_reservation(self, owner, reservation_id, **kwargs):
        matches = [r for r in self._reservations() if r["owner"] == owner and r["reservation_id"] == reservation_id]
        if not matches or type(reservation_id) is not int:
            return call_result.CancelReservation(status="Rejected")
        state = copy.deepcopy(self.state)
        state["reservations"] = [r for r in state["reservations"] if r not in matches]
        self._commit(state)
        self._dirty.update((1, 2))
        self._spawn(self._announce())
        return call_result.CancelReservation(status="Accepted")

    async def transaction_started(self, connector_id, owner, id_tag, parent_id_tag=None):
        state = copy.deepcopy(self.state)
        reservation_id = self.reservation_id_for(connector_id, owner, id_tag, parent_id_tag)
        state["reservations"] = [r for r in state["reservations"]
            if not (r["owner"] == owner and r["reservation_id"] == reservation_id)]
        if state != self.state:
            self._commit(state)
        self._dirty.add(connector_id)

    async def _announce(self):
        for cid in tuple(sorted(self._dirty)):
            try:
                physical = "Available" if cid == 0 else str(self.physical_status(cid))
                await invoke(self.publish_status, cid, self.effective_status(cid, physical))
                self._dirty.discard(cid)
            except Exception:
                pass  # Logical state remains enforced; the periodic task retries its announcement.

    def _queue_status(self, owner, action, status, *, job_id=None, phase=None, fields=None):
        state = copy.deepcopy(self.state)
        if job_id:
            row = state["jobs"][job_id]
            if phase:
                row["phase"] = phase
            row.update(fields or {})
        state["last_status"].setdefault(owner, {})[action] = status
        state["notifications"].append({"id": uuid.uuid4().hex, "owner": owner,
                                       "action": action, "status": status})
        if len(state["notifications"]) > 128:
            # Progress is current-state information, not transaction history.
            # Keep the newest bounded snapshots if an origin remains offline.
            state["notifications"] = state["notifications"][-128:]
        self._commit(state)
        if not self._closed:
            self._spawn(self.flush_notifications())

    async def flush_notifications(self):
        if self._draining or self._closed:
            return
        self._draining = True
        try:
            # A disconnected origin cannot block progress reports to the other peer.
            blocked = set()
            for item in list(self.state["notifications"]):
                if item["owner"] in blocked:
                    continue
                try:
                    request = getattr(call, item["action"])(status=item["status"])
                    await invoke(self.publish_message, item["owner"], request)
                except Exception:
                    blocked.add(item["owner"])
                    continue
                state = copy.deepcopy(self.state)
                state["notifications"] = [n for n in state["notifications"] if n["id"] != item["id"]]
                self._commit(state)
        finally:
            self._draining = False

    async def send_current_status(self, owner, requested_message):
        allowed = {"DiagnosticsStatusNotification", "FirmwareStatusNotification"}
        if requested_message not in allowed or not _owner(owner):
            return False
        kind = "diagnostics" if requested_message == "DiagnosticsStatusNotification" else "firmware"
        active = [row for row in self.state["jobs"].values() if row["owner"] == owner
                  and row["kind"] == kind and row["phase"] in ("uploading", "downloading")]
        status = ("Uploading" if kind == "diagnostics" else "Downloading") if active else "Idle"
        self._queue_status(owner, requested_message, status)
        return True

    @staticmethod
    def _retry_settings(retries, retry_interval):
        retries = 0 if retries is None else retries
        retry_interval = 30 if retry_interval is None else retry_interval
        if type(retries) is not int or not 0 <= retries <= 10:
            raise ValueError("retries must be an integer from zero to ten")
        if type(retry_interval) is not int or not 0 <= retry_interval <= 86400:
            raise ValueError("retryInterval must be an integer from zero to 86400")
        return retries, retry_interval

    def _new_job(self, row):
        state = copy.deepcopy(self.state)
        unfinished = {"waiting", "downloading", "uploading"}
        if any(j["kind"] == row["kind"] and j["phase"] in unfinished for j in state["jobs"].values()):
            raise ValueError("A transfer of this kind is already in progress")
        # Retain bounded, completed staging/audit metadata. Active jobs are never evicted.
        completed = [key for key, job in state["jobs"].items() if job["phase"] not in unfinished]
        pruned_archives = []
        for key in completed[:-15]:
            old = state["jobs"][key]
            if old["kind"] == "diagnostics":
                pruned_archives.append(self.directory / "diagnostics" / old["archive_name"])
            del state["jobs"][key]
        job_id = uuid.uuid4().hex
        state["jobs"][job_id] = row
        self._commit(state)
        for archive in pruned_archives:
            try:
                archive.unlink(missing_ok=True)
            except OSError:
                pass
        return job_id

    async def get_diagnostics(self, owner, location, retries=None, retry_interval=None,
                              start_time=None, stop_time=None, **kwargs):
        if not _owner(owner):
            return call_result.GetDiagnostics()
        building, archive, job_id = False, None, None
        try:
            self.transfer.validate_url(location)
            retry, interval = self._retry_settings(retries, retry_interval)
            start = date_time(start_time) if start_time is not None else None
            stop = date_time(stop_time) if stop_time is not None else None
            if start and stop and start > stop:
                raise ValueError("Diagnostics start time exceeds stop time")
            if self._diagnostic_building or any(j["kind"] == "diagnostics" and j["phase"] in {"waiting", "uploading"}
                   for j in self.state["jobs"].values()):
                return call_result.GetDiagnostics()
            self._diagnostic_building = building = True
            archive_name = "diagnostics-" + uuid.uuid4().hex + ".zip"
            archive = self.directory / "diagnostics" / archive_name
            summary = {"created_at": self._now().isoformat(), "hardware_scope": "controller diagnostic snapshot",
                       "availability": dict(self.state["availability"]),
                       "pending_availability": dict(self.state["pending_availability"]),
                       "reservation_count": len(self._reservations()),
                       "firmware_mode": self.firmware_mode,
                       "start_time": start.isoformat() if start else None,
                       "stop_time": stop.isoformat() if stop else None}
            await asyncio.to_thread(self._build_archive, archive, summary, start, stop)
            job_id = self._new_job({"owner": owner, "kind": "diagnostics", "phase": "waiting",
                "location": location, "archive_name": archive_name, "retries": retry,
                "retry_interval": interval, "created_at": self._now().isoformat()})
            self._launch_job(job_id)
            return call_result.GetDiagnostics(file_name=archive_name)
        except (ValueError, OSError, TransferRejected):
            if archive is not None and job_id is None and archive.exists():
                archive.unlink()
            return call_result.GetDiagnostics()
        finally:
            if building:
                self._diagnostic_building = False

    async def update_firmware(self, owner, location, retrieve_date, retries=None,
                               retry_interval=None, **kwargs):
        # UpdateFirmware.conf has no status field. Invalid requests get an explicit
        # progress failure notification; no content is fetched or executed.
        if not _owner(owner):
            return call_result.UpdateFirmware()
        try:
            self.transfer.validate_url(location)
            retry, interval = self._retry_settings(retries, retry_interval)
            retrieve = date_time(retrieve_date)
            job_id = self._new_job({"owner": owner, "kind": "firmware", "phase": "waiting",
                "location": location, "retrieve_date": retrieve.isoformat(), "retries": retry,
                "retry_interval": interval, "created_at": self._now().isoformat(),
                "mode": self.firmware_mode})
            self._launch_job(job_id)
        except (ValueError, OSError, TransferRejected):
            self._queue_status(owner, "FirmwareStatusNotification", "DownloadFailed")
        return call_result.UpdateFirmware()

    def _launch_job(self, job_id):
        if self._closed or job_id in self._running_jobs:
            return
        self._running_jobs.add(job_id)
        self._spawn(self._run_job(job_id))

    async def _run_job(self, job_id):
        await asyncio.sleep(0)  # Return the inbound CALLRESULT before any progress CALL.
        row = self.state["jobs"][job_id]
        try:
            if row["kind"] == "diagnostics":
                await self._diagnostics_job(job_id)
            else:
                await self._firmware_job(job_id)
        except asyncio.CancelledError:
            raise  # A waiting/downloading/uploading job is resumable after restart.
        except Exception as error:
            action = "DiagnosticsStatusNotification" if row["kind"] == "diagnostics" else "FirmwareStatusNotification"
            status = "UploadFailed" if row["kind"] == "diagnostics" else "DownloadFailed"
            self._queue_status(row["owner"], action, status, job_id=job_id, phase="failed",
                               fields={"error_type": type(error).__name__})
        finally:
            self._running_jobs.discard(job_id)

    async def _diagnostics_job(self, job_id):
        row = self.state["jobs"][job_id]
        archive = self.directory / "diagnostics" / row["archive_name"]
        for attempt in range(row["retries"] + 1):
            self._queue_status(row["owner"], "DiagnosticsStatusNotification", "Uploading",
                               job_id=job_id, phase="uploading", fields={"attempt": attempt + 1})
            try:
                success = await invoke(self.transfer.upload, row["location"], archive)
                if success is not True:
                    raise TransferRejected("Diagnostics upload was not acknowledged")
                self._queue_status(row["owner"], "DiagnosticsStatusNotification", "Uploaded",
                                   job_id=job_id, phase="uploaded")
                return
            except asyncio.CancelledError:
                raise
            except Exception:
                if attempt >= row["retries"]:
                    raise
                await asyncio.sleep(row["retry_interval"])

    async def _firmware_job(self, job_id):
        row = self.state["jobs"][job_id]
        while self._now() < date_time(row["retrieve_date"]):
            await asyncio.sleep(min(1, (date_time(row["retrieve_date"]) - self._now()).total_seconds()))
        destination = self.directory / "firmware_staging" / ("firmware-" + job_id + ".bin")
        for attempt in range(row["retries"] + 1):
            self._queue_status(row["owner"], "FirmwareStatusNotification", "Downloading",
                               job_id=job_id, phase="downloading", fields={"attempt": attempt + 1})
            try:
                if self._staging_size() >= self.max_staging_bytes:
                    raise TransferRejected("Firmware staging storage is full")
                result = await invoke(self.transfer.download, row["location"], destination)
                if self._staging_size() > self.max_staging_bytes:
                    raise TransferRejected("Firmware staging storage capacity exceeded")
                actual, size = await asyncio.to_thread(self._staged_digest, destination)
                if size == 0 or actual != result.get("sha256") or size != result.get("bytes"):
                    raise TransferRejected("Staged firmware integrity mismatch")
                trusted = self.trusted_firmware_sha256.get(row["location"])
                if trusted is not None and actual.lower() != str(trusted).lower():
                    raise TransferRejected("Firmware does not match the configured trusted digest")
                verified = bool(trusted)
                if self.verify_firmware is not None:
                    if await invoke(self.verify_firmware, destination, actual) is not True:
                        raise TransferRejected("Firmware verifier refused the staged image")
                    verified = True
                self._queue_status(row["owner"], "FirmwareStatusNotification", "Downloaded",
                    job_id=job_id, phase="downloaded", fields={"staged_name": destination.name,
                    "sha256": actual, "bytes": size, "trusted_integrity_verified": verified,
                    "installed": False, "mode": self.firmware_mode})
                return
            except asyncio.CancelledError:
                raise
            except Exception:
                if destination.exists():
                    destination.unlink()
                if attempt >= row["retries"]:
                    raise
                await asyncio.sleep(row["retry_interval"])

    @staticmethod
    def _staged_digest(path):
        digest, size = hashlib.sha256(), 0
        with path.open("rb") as stream:
            for block in iter(lambda: stream.read(65536), b""):
                size += len(block)
                digest.update(block)
        return digest.hexdigest(), size

    def _staging_size(self):
        directory = self.directory / "firmware_staging"
        return sum(path.stat().st_size for path in directory.glob("firmware-*")
                   if path.is_file() and not path.is_symlink())

    def _prune_reservations(self, state):
        now = self._now()

        def operative(cid):
            return (state["availability"].get("0") != "Inoperative"
                    and state["availability"].get(str(cid)) != "Inoperative"
                    and state["pending_availability"].get("0") != "Inoperative"
                    and state["pending_availability"].get(str(cid)) != "Inoperative")

        rows = [r for r in state["reservations"] if date_time(r["expiry_date"]) > now]
        specific = [r for r in rows if r["connector_id"] != 0 and operative(r["connector_id"])
                    and str(self.physical_status(r["connector_id"])) not in ("Faulted", "Unavailable")]
        capacity = sum(operative(cid) and not self.is_active(cid)
                       and str(self.physical_status(cid)) in ("Available", "Reserved")
                       and not any(r["connector_id"] == cid for r in specific) for cid in (1, 2))
        floating = [r for r in rows if r["connector_id"] == 0][:capacity]
        state["reservations"] = specific + floating

    @staticmethod
    def _redact_line(line):
        def safe(value):
            if isinstance(value, dict):
                return {key: ("[redacted]" if re.search(r"id_?tag|password|token|secret|authorization|certificate|private.?key|rfid", key, re.I)
                               else safe(item)) for key, item in value.items()}
            if isinstance(value, list):
                return [safe(item) for item in value]
            if isinstance(value, str):
                return scrub(value)
            return value

        def scrub(text):
            text = re.sub(r"https?://[^\s\"'<>]+", "[redacted URL]", text, flags=re.I)
            text = re.sub(r"(?im)((?:proxy-)?authorization\s*[:=]\s*)[^\r\n]*", r"\1[redacted]", text)
            text = re.sub(r"(?i)([\"']?(?:id_?tag|password|token|secret|private_?key|bot_token|rfid)[\"']?\s*[:=]\s*)(\"[^\"]*\"|'[^']*'|[^\s,;}\]]+)",
                          r"\1[redacted]", text)
            return text

        try:
            return json.dumps(safe(json.loads(line)), ensure_ascii=False)
        except (ValueError, TypeError):
            return scrub(line)

    @staticmethod
    def _line_timestamp(line):
        try:
            obj = json.loads(line)
            value = obj.get("timestamp", obj.get("time", obj.get("created_at")))
            if value:
                return date_time(value)
        except (ValueError, TypeError, AttributeError):
            pass
        match = re.match(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:[.,]\d+)?(?:Z|[+-]\d{2}:?\d{2})?", line)
        if match:
            try:
                stamp = match[0].replace(",", ".")
                value = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
                # A naive wall-clock log line has no reliable UTC position.
                # Omit it from time-filtered exports rather than guessing a zone.
                return None if value.tzinfo is None else value.astimezone(timezone.utc)
            except ValueError:
                pass
        return None

    def _build_archive(self, destination, summary, start, stop):
        destination.parent.mkdir(parents=True, exist_ok=True)
        partial = destination.with_name(destination.name + ".part")
        total = 0
        sources = []
        try:
            with zipfile.ZipFile(partial, "x", compression=zipfile.ZIP_DEFLATED) as archive:
                for index, source in enumerate(self.diagnostic_sources):
                    if (source.is_symlink() or not source.is_file() or source.suffix.lower() not in (".log", ".jsonl")
                            or re.search(r"secret|password|token|private.?key|authorization|config|rfid", source.name, re.I)):
                        continue
                    remaining = 2 * 1024 * 1024 - total
                    if remaining <= 0:
                        break
                    with source.open("rb") as stream:
                        content = stream.read(min(remaining, 1024 * 1024))
                    total += len(content)
                    kept = []
                    for raw in content.decode("utf-8", errors="replace").splitlines():
                        if len(raw) > 65536:
                            continue
                        stamp = self._line_timestamp(raw)
                        if (start or stop) and (stamp is None or (start and stamp < start) or (stop and stamp > stop)):
                            continue
                        kept.append(self._redact_line(raw))
                    name = f"logs/{index:02d}-" + re.sub(r"[^A-Za-z0-9._-]", "_", source.name)
                    archive.writestr(name, "\n".join(kept) + ("\n" if kept else ""))
                    sources.append({"name": name, "lines": len(kept), "source_bytes_considered": len(content)})
                summary["sources"] = sources
                summary["redacted"] = True
                archive.writestr("summary.json", json.dumps(summary, indent=2))
            os.replace(partial, destination)
        finally:
            if partial.exists():
                partial.unlink()

    async def tick(self):
        state = copy.deepcopy(self.state)
        for cid, target in tuple(state["pending_availability"].items()):
            active = any(self.is_active(i) for i in (1, 2)) if cid == "0" else self.is_active(int(cid))
            if not active:
                state["availability"][cid] = target
                del state["pending_availability"][cid]
        self._prune_reservations(state)
        if state != self.state:
            self._commit(state)
            self._dirty.update((0, 1, 2))
        await self._announce()
        await self.flush_notifications()

    async def start(self):
        self._closed = False
        await self.tick()
        for job_id, row in tuple(self.state["jobs"].items()):
            if row["phase"] in {"waiting", "downloading", "uploading"}:
                self._launch_job(job_id)

    async def run(self, interval=1):
        await self.start()
        while not self._closed:
            await self.tick()
            await asyncio.sleep(interval)

    async def wait_jobs(self):
        while self._running_jobs:
            await asyncio.sleep(.01)
        await self.flush_notifications()

    async def close(self):
        self._closed = True
        pending = tuple(self.tasks)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
