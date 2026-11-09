"""Protocol services with loopback HTTP and injected physical state only."""
import asyncio
import copy
import hashlib
import io
import json
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock

import pytest

from helpf.ocpp_services import HTTPTransferPolicy, OCPPServices, TransferRejected


NOW = datetime(2026, 11, 8, tzinfo=timezone.utc)


class Transfer:
    def __init__(self, data=b"test firmware", fail_upload=0):
        self.data = data
        self.uploads = []
        self.downloads = []
        self.fail_upload = fail_upload

    def validate_url(self, location):
        if not location.startswith("https://allowed.test/"):
            raise TransferRejected("destination refused")

    async def upload(self, location, path):
        self.uploads.append((location, Path(path).read_bytes()))
        if len(self.uploads) <= self.fail_upload:
            raise OSError("test unavailable")
        return True

    async def download(self, location, path):
        self.downloads.append(location)
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_bytes(self.data)
        return {"path": str(path), "bytes": len(self.data),
                "sha256": hashlib.sha256(self.data).hexdigest()}


@pytest.fixture
def services(tmp_path):
    active = {1: False, 2: False}
    physical = {1: "Available", 2: "Available"}
    clock = [NOW]
    sent, statuses = [], []
    transfer = Transfer()

    async def publish(owner, message):
        sent.append((owner, type(message).__name__, message.status))

    async def status(cid, value):
        statuses.append((cid, value))

    def factory(**kwargs):
        options = dict(is_active=lambda cid: active[cid], physical_status=lambda cid: physical[cid],
                       publish_status=status, publish_message=publish, transfer=transfer,
                       clock=lambda: clock[0])
        options.update(kwargs)
        return OCPPServices(tmp_path / "state", **options)

    core = factory()
    return core, active, physical, clock, sent, statuses, transfer, factory


async def test_availability_persists_and_blocks_starts_until_restored(services):
    core, _, _, _, _, statuses, _, factory = services
    assert (await core.change_availability("A", 1, "Inoperative")).status == "Accepted"
    await core.tick()
    assert not core.can_start(1, "B", "CARD")
    assert core.can_start(2, "B", "CARD")
    assert (1, "Unavailable") in statuses
    restored = factory()
    assert not restored.can_start(1, "A", "CARD")
    assert (await restored.change_availability("A", 1, "Operative")).status == "Accepted"
    assert restored.can_start(1, "A", "CARD")
    await core.close()
    await restored.close()


async def test_scheduled_availability_waits_for_active_transaction(services):
    core, active, _, _, _, _, _, _ = services
    active[1] = True
    assert (await core.change_availability("A", 1, "Inoperative")).status == "Scheduled"
    assert core.state["availability"]["1"] == "Operative"
    assert core.effective_status(1, "Charging") == "Charging"
    assert not core.can_start(1, "A", "CARD")
    active[1] = False
    await core.tick()
    assert core.state["availability"]["1"] == "Inoperative"
    assert core.effective_status(1, "Available") == "Unavailable"
    await core.close()


async def test_already_operative_cancels_pending_inoperative(services):
    core, active, _, _, _, _, _, _ = services
    active[1] = True
    await core.change_availability("A", 1, "Inoperative")
    assert (await core.change_availability("A", 1, "Operative")).status == "Accepted"
    assert "1" not in core.state["pending_availability"]
    # Start policy must not reject the dispatcher's own already-claimed head.
    active[2] = True
    assert core.can_start(1, "A", "CARD")
    await core.close()


async def test_all_connectors_schedules_active_and_applies_idle_head(services):
    core, active, _, _, _, _, _, _ = services
    active[1] = True
    assert (await core.change_availability("A", 0, "Inoperative")).status == "Scheduled"
    assert core.effective_status(0, "Available") == "Available"
    assert core.effective_status(1, "Charging") == "Charging"
    assert core.effective_status(2, "Available") == "Unavailable"
    active[1] = False
    await core.tick()
    assert all(core.state["availability"][str(cid)] == "Inoperative" for cid in (0, 1, 2))
    assert core.effective_status(1, "Faulted") == "Faulted"
    await core.close()


@pytest.mark.parametrize("connector,value", [(True, "Inoperative"), (3, "Operative"), (-1, "Operative"), ("1", "Operative"), (1, "invalid")])
async def test_invalid_availability_does_not_mutate(services, connector, value):
    core = services[0]
    before = copy.deepcopy(core.state)
    assert (await core.change_availability("A", connector, value)).status == "Rejected"
    assert core.state == before


async def test_failed_availability_persistence_preserves_previous_memory_and_disk(services, monkeypatch):
    core = services[0]
    await core.change_availability("A", 1, "Inoperative")
    before, data = copy.deepcopy(core.state), core.path.read_bytes()
    monkeypatch.setattr("helpf.ocpp_services.os.replace", Mock(side_effect=OSError("test disk failure")))
    with pytest.raises(OSError):
        await core.change_availability("A", 1, "Operative")
    assert core.state == before and core.path.read_bytes() == data
    assert not core.can_start(1, "A", "CARD")
    await core.close()


async def test_owner_scoped_reservation_ids_and_parent_authorization(services):
    core = services[0]
    expiry = (NOW + timedelta(hours=1)).isoformat()
    assert (await core.reserve_now("A", 1, expiry, "A_CARD", 7, "FAMILY")).status == "Accepted"
    assert (await core.reserve_now("B", 2, expiry, "B_CARD", 7)).status == "Accepted"
    assert core.effective_status(1, "Available") == "Reserved"
    assert not core.can_start(1, "B", "A_CARD")
    assert core.can_start(1, "A", "CHILD", "FAMILY")
    assert core.reservation_id_for(1, "A", "CHILD", "FAMILY") == 7
    assert (await core.cancel_reservation("C", 7)).status == "Rejected"
    assert (await core.cancel_reservation("A", 7)).status == "Accepted"
    assert core.reservation_id_for(2, "B", "B_CARD") == 7
    assert not core.can_start(2, "A", "B_CARD")
    await core.close()


async def test_reservation_consumed_only_on_matching_confirmed_start(services):
    core = services[0]
    await core.reserve_now("B", 2, (NOW + timedelta(hours=1)).isoformat(), "B_CARD", 12)
    await core.transaction_started(2, "A", "B_CARD")
    assert core.reservation_id_for(2, "B", "B_CARD") == 12
    await core.transaction_started(2, "B", "B_CARD")
    assert core.reservation_id_for(2, "B", "B_CARD") is None
    await core.close()


async def test_one_start_does_not_consume_two_matching_reservations(services):
    _, _, _, _, _, _, _, factory = services
    core = factory(allow_connector_zero_reservation=True)
    expiry = (NOW + timedelta(hours=1)).isoformat()
    await core.reserve_now("A", 0, expiry, "CARD", 1)
    await core.reserve_now("A", 1, expiry, "CARD", 2)
    assert core.reservation_id_for(1, "A", "CARD") == 2
    await core.transaction_started(1, "A", "CARD")
    assert [r["reservation_id"] for r in core.state["reservations"]] == [1]
    await core.close()


async def test_reservation_expiry_releases_and_announces_available(services):
    core, _, _, clock, _, statuses, _, _ = services
    await core.reserve_now("A", 1, (NOW + timedelta(seconds=10)).isoformat(), "CARD", 1)
    clock[0] += timedelta(seconds=11)
    await core.tick()
    assert core.state["reservations"] == []
    assert core.can_start(1, "B", "OTHER")
    assert (1, "Available") in statuses
    await core.close()


@pytest.mark.parametrize("physical,active,expected", [("Available", True, "Occupied"), ("Preparing", False, "Occupied"), ("Faulted", False, "Faulted"), ("Unavailable", False, "Unavailable")])
async def test_reservation_rejects_unavailable_physical_head(services, physical, active, expected):
    core, busy, states, *_ = services
    busy[1], states[1] = active, physical
    assert (await core.reserve_now("A", 1, (NOW + timedelta(hours=1)).isoformat(), "CARD", 1)).status == expected
    assert core.state["reservations"] == []


async def test_same_owner_reservation_update_is_atomic_and_fault_cancels(services):
    core, _, physical, *_ = services
    expiry = (NOW + timedelta(hours=1)).isoformat()
    await core.reserve_now("A", 1, expiry, "CARD", 5)
    assert (await core.reserve_now("A", 1, expiry, "NEW", 5)).status == "Accepted"
    assert len(core.state["reservations"]) == 1
    assert core.can_start(1, "A", "NEW")
    before = copy.deepcopy(core.state)
    assert (await core.reserve_now("A", 2, NOW.isoformat(), "NEW", 5)).status == "Rejected"
    assert core.state == before
    physical[1] = "Faulted"
    await core.tick()
    assert not core.state["reservations"]
    await core.close()


async def test_default_rejects_connector_zero_and_optional_floating_preserves_capacity(services):
    core, active, _, _, _, _, _, factory = services
    expiry = (NOW + timedelta(hours=1)).isoformat()
    assert (await core.reserve_now("A", 0, expiry, "CARD", 1)).status == "Rejected"
    floating = factory(allow_connector_zero_reservation=True)
    assert (await floating.reserve_now("B", 0, expiry, "CARD", 1)).status == "Accepted"
    assert floating.can_start(1, "A", "OTHER")
    active[1] = True
    assert not floating.can_start(2, "A", "OTHER")
    assert floating.can_start(2, "B", "CARD")
    await floating.transaction_started(2, "B", "CARD")
    assert not floating.state["reservations"]
    await floating.close()


async def test_floating_reservation_survives_one_head_unavailable_but_not_loss_of_all_capacity(services):
    _, _, physical, _, _, _, _, factory = services
    core = factory(allow_connector_zero_reservation=True)
    await core.reserve_now("B", 0, (NOW + timedelta(hours=1)).isoformat(), "CARD", 1)
    await core.change_availability("A", 1, "Inoperative")
    assert core.reservation_id_for(2, "B", "CARD") == 1
    assert core.can_start(2, "B", "CARD")
    physical[2] = "Faulted"
    await core.tick()
    assert core.state["reservations"] == []
    await core.close()


async def test_diagnostics_redacts_and_filters_explicit_sources(services, tmp_path):
    _, _, _, _, sent, _, transfer, factory = services
    selected = tmp_path / "charger.jsonl"
    selected.write_text('\n'.join(json.dumps({"timestamp": stamp, "idTag": "PRIVATE_TAG", "url": "https://allowed.test/t/PRIVATE_TOKEN/", "data": "Authorization: Basic PRIVATE_AUTH"}) for stamp in ["2026-11-07T23:00:00Z", "2026-11-08T00:00:00Z"]), encoding="utf-8")
    excluded = tmp_path / "secret-config.log"
    excluded.write_text("DO_NOT_SEND")
    core = factory(diagnostic_sources=[selected, excluded])
    result = await core.get_diagnostics("B", "https://allowed.test/upload/t/opaque/", start_time="2026-11-07T23:30:00Z", stop_time="2026-11-08T00:30:00Z")
    assert result.file_name.endswith(".zip")
    await core.wait_jobs()
    assert sent == [("B", "DiagnosticsStatusNotification", "Uploading"), ("B", "DiagnosticsStatusNotification", "Uploaded")]
    archive = zipfile.ZipFile(io.BytesIO(transfer.uploads[0][1]))
    logs = archive.read("logs/00-charger.jsonl").decode()
    assert logs.count("timestamp") == 1
    assert all(secret not in logs for secret in ("PRIVATE_TAG", "PRIVATE_TOKEN", "PRIVATE_AUTH", "DO_NOT_SEND"))
    assert "2026-11-08T00:00:00Z" in logs
    assert not any("secret" in name for name in archive.namelist())
    assert json.loads(archive.read("summary.json"))["redacted"] is True
    await core.close()


async def test_diagnostics_retry_and_final_failure_notification(services):
    _, _, _, _, sent, _, _, factory = services
    flaky = Transfer(fail_upload=1)
    core = factory(transfer=flaky)
    await core.get_diagnostics("A", "https://allowed.test/upload/", retries=1, retry_interval=0)
    await core.wait_jobs()
    assert len(flaky.uploads) == 2
    assert sent[-1] == ("A", "DiagnosticsStatusNotification", "Uploaded")
    flaky.fail_upload = 100
    await core.get_diagnostics("B", "https://allowed.test/upload/", retries=1, retry_interval=0)
    await core.wait_jobs()
    assert sent[-1] == ("B", "DiagnosticsStatusNotification", "UploadFailed")
    await core.close()


async def test_concurrent_diagnostics_requests_return_one_owned_archive(services):
    core = services[0]
    results = await asyncio.gather(*(core.get_diagnostics(owner, "https://allowed.test/upload/") for owner in ("A", "B")))
    await core.wait_jobs()
    assert sum(result.file_name is not None for result in results) == 1
    assert len(list((core.directory / "diagnostics").glob("*.zip"))) == 1
    assert len(core.state["jobs"]) == 1
    await core.close()


def test_plaintext_log_redaction_removes_credentials_and_tag_values():
    line = "2026-11-08T00:00:00Z idTag='PRIVATE_CARD' bot_token=PRIVATE_BOT location=https://safe.test/t/PRIVATE_UPLOAD/\nAuthorization: Basic PRIVATE_BASIC"
    clean = OCPPServices._redact_line(line)
    assert not any(value in clean for value in ("PRIVATE_CARD", "PRIVATE_BOT", "PRIVATE_UPLOAD", "PRIVATE_BASIC"))


def test_time_filtered_diagnostics_never_guess_naive_log_timezone():
    assert OCPPServices._line_timestamp("2026-11-08 07:00:00,000 INFO example") is None
    assert OCPPServices._line_timestamp("2026-11-08T07:00:00+07:00 INFO example") == NOW


@pytest.mark.parametrize("retries,interval", [(True, 0), (-1, 0), (11, 0), (0, -1), (0, "5")])
async def test_invalid_retry_parameters_never_start_transfer(services, retries, interval):
    core, _, _, _, sent, _, transfer, _ = services
    result = await core.get_diagnostics("A", "https://allowed.test/upload/", retries=retries, retry_interval=interval)
    assert result.file_name is None and not transfer.uploads
    await core.update_firmware("A", "https://allowed.test/fw.bin", NOW.isoformat(), retries=retries, retry_interval=interval)
    await core.flush_notifications()
    assert sent[-1] == ("A", "FirmwareStatusNotification", "DownloadFailed")
    assert not transfer.downloads
    await core.close()


async def test_firmware_stages_verified_image_without_ever_running_installer(services):
    _, _, _, _, sent, _, transfer, factory = services
    location = "https://allowed.test/fw.bin"
    digest = hashlib.sha256(transfer.data).hexdigest()
    installer = Mock(side_effect=AssertionError("staging cannot invoke installer"))
    core = factory(trusted_firmware_sha256={location: digest}, installer=installer)
    assert (await core.update_firmware("B", location, NOW.isoformat())).__class__.__name__ == "UpdateFirmware"
    await core.wait_jobs()
    assert sent == [("B", "FirmwareStatusNotification", "Downloading"), ("B", "FirmwareStatusNotification", "Downloaded")]
    row = next(iter(core.state["jobs"].values()))
    assert row["phase"] == "downloaded" and row["sha256"] == digest
    assert row["trusted_integrity_verified"] is True and row["installed"] is False
    assert (core.directory / "firmware_staging" / row["staged_name"]).read_bytes() == transfer.data
    installer.assert_not_called()
    await core.close()


async def test_firmware_computed_hash_is_not_claimed_trusted_and_restart_does_not_redownload(services):
    _, _, _, _, sent, _, transfer, factory = services
    core = factory()
    await core.update_firmware("A", "https://allowed.test/fw.bin", NOW.isoformat())
    await core.wait_jobs()
    row = next(iter(core.state["jobs"].values()))
    assert row["trusted_integrity_verified"] is False and row["installed"] is False
    await core.close()
    restored = factory()
    await restored.start()
    await restored.wait_jobs()
    assert len(transfer.downloads) == 1
    assert not any(status in ("Installed", "Installing") for _, _, status in sent)
    await restored.close()


async def test_trigger_reports_idle_after_completed_transfers_without_faking_installed(services):
    core, _, _, _, sent, *_ = services
    await core.get_diagnostics("A", "https://allowed.test/upload/")
    await core.wait_jobs()
    await core.send_current_status("A", "DiagnosticsStatusNotification")
    await core.flush_notifications()
    assert sent[-1] == ("A", "DiagnosticsStatusNotification", "Idle")
    await core.update_firmware("A", "https://allowed.test/fw.bin", NOW.isoformat())
    await core.wait_jobs()
    await core.send_current_status("A", "FirmwareStatusNotification")
    await core.flush_notifications()
    assert sent[-1] == ("A", "FirmwareStatusNotification", "Idle")
    assert not any(status == "Installed" for _, _, status in sent)
    await core.close()


async def test_firmware_wrong_trusted_digest_fails_and_removes_staged_file(services):
    _, _, _, _, sent, _, _, factory = services
    core = factory(trusted_firmware_sha256={"https://allowed.test/fw.bin": "0" * 64})
    await core.update_firmware("A", "https://allowed.test/fw.bin", NOW.isoformat())
    await core.wait_jobs()
    assert sent[-1] == ("A", "FirmwareStatusNotification", "DownloadFailed")
    assert not list((core.directory / "firmware_staging").glob("*.bin"))
    await core.close()


async def test_staging_capacity_refuses_new_download_without_deleting_prior_image(services):
    _, _, _, _, sent, _, transfer, factory = services
    core = factory(max_staging_bytes=len(transfer.data))
    await core.update_firmware("A", "https://allowed.test/first.bin", NOW.isoformat())
    await core.wait_jobs()
    first = list((core.directory / "firmware_staging").glob("*.bin"))
    assert len(first) == 1
    await core.update_firmware("A", "https://allowed.test/second.bin", NOW.isoformat())
    await core.wait_jobs()
    assert sent[-1] == ("A", "FirmwareStatusNotification", "DownloadFailed")
    assert len(transfer.downloads) == 1
    assert first[0].read_bytes() == transfer.data
    await core.close()


async def test_firmware_honors_future_retrieve_date(services):
    _, _, _, clock, _, _, transfer, factory = services
    core = factory()
    await core.update_firmware("A", "https://allowed.test/fw.bin", (NOW + timedelta(seconds=1)).isoformat())
    await asyncio.sleep(.03)
    assert not transfer.downloads
    clock[0] += timedelta(seconds=2)
    await asyncio.wait_for(core.wait_jobs(), 2)
    assert transfer.downloads == ["https://allowed.test/fw.bin"]
    await core.close()


async def test_future_firmware_job_resumes_staging_after_restart(services):
    _, _, _, clock, sent, _, transfer, factory = services
    core = factory()
    await core.update_firmware("B", "https://allowed.test/fw.bin", (NOW + timedelta(hours=1)).isoformat())
    await core.close()
    assert not transfer.downloads
    clock[0] += timedelta(hours=2)
    restored = factory()
    await restored.start()
    await restored.wait_jobs()
    assert sent[-1] == ("B", "FirmwareStatusNotification", "Downloaded")
    assert len(transfer.downloads) == 1
    await restored.close()


async def test_external_verifier_must_explicitly_accept_staged_image(services):
    _, _, _, _, sent, _, _, factory = services
    core = factory(verify_firmware=lambda path, sha: False)
    await core.update_firmware("A", "https://allowed.test/fw.bin", NOW.isoformat())
    await core.wait_jobs()
    assert sent[-1] == ("A", "FirmwareStatusNotification", "DownloadFailed")
    assert not list((core.directory / "firmware_staging").glob("*.bin"))
    await core.close()


@pytest.mark.parametrize("damage", ["availability", "archive_path", "notification_status"])
async def test_corrupted_state_fails_closed_without_overwriting_evidence(services, damage):
    core, _, _, _, _, _, _, factory = services
    state = copy.deepcopy(core.state)
    if damage == "availability":
        del state["availability"]["1"]
    elif damage == "archive_path":
        state["jobs"]["a" * 32] = {"kind": "diagnostics", "owner": "A", "phase": "waiting",
            "archive_name": "../../secret.txt", "location": "https://allowed.test/upload/",
            "retries": 0, "retry_interval": 0}
    else:
        state["notifications"] = [{"id": "a" * 32, "owner": "A", "action": "FirmwareStatusNotification", "status": "PretendInstalled"}]
    original = json.dumps(state)
    core.path.write_text(original, encoding="utf-8")
    with pytest.raises(ValueError):
        factory()
    assert core.path.read_text(encoding="utf-8") == original


async def test_disconnected_origin_notifications_do_not_block_other_peer_and_persist(services):
    _, _, _, _, _, _, _, factory = services
    blocked = {"A"}
    sent = []

    async def publish(owner, message):
        if owner in blocked:
            raise OSError("offline")
        sent.append((owner, message.status))

    core = factory(publish_message=publish)
    await core.send_current_status("A", "FirmwareStatusNotification")
    await core.send_current_status("B", "DiagnosticsStatusNotification")
    await core.flush_notifications()
    assert sent == [("B", "Idle")]
    assert len(core.state["notifications"]) == 1
    await core.close()
    blocked.clear()
    restored = factory(publish_message=publish)
    await restored.start()
    assert sent[-1] == ("A", "Idle")
    assert restored.state["notifications"] == []
    await restored.close()


class HTTPFixture:
    data = b"firmware fixture bytes"

    async def __aenter__(self):
        self.uploads = []
        self.server = await asyncio.start_server(self.handle, "127.0.0.1", 0)
        self.base = "http://127.0.0.1:" + str(self.server.sockets[0].getsockname()[1])
        return self

    async def __aexit__(self, *args):
        self.server.close()
        await self.server.wait_closed()

    async def handle(self, reader, writer):
        try:
            head = (await reader.readuntil(b"\r\n\r\n")).decode("ascii")
            method, path, _ = head.split("\r\n")[0].split()
            headers = dict(line.split(": ", 1) for line in head.split("\r\n")[1:] if ": " in line)
            if method in ("PUT", "POST"):
                body = await reader.readexactly(int(headers["Content-Length"]))
                self.uploads.append((method, path, body))
                response = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK"
            elif path == "/redirect":
                response = b"HTTP/1.1 302 Found\r\nLocation: http://untrusted.invalid/image\r\nContent-Length: 0\r\n\r\n"
            elif path == "/truncated":
                response = b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\nshort"
            else:
                response = b"HTTP/1.1 200 OK\r\nContent-Length: " + str(len(self.data)).encode() + b"\r\n\r\n" + self.data
            writer.write(response)
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()


async def test_real_loopback_http_upload_and_firmware_download(services):
    _, _, _, _, sent, _, _, factory = services
    async with HTTPFixture() as server:
        transport = HTTPTransferPolicy(["127.0.0.1"], allow_http_hosts=["127.0.0.1"])
        core = factory(transfer=transport)
        result = await core.get_diagnostics("B", server.base + "/upload/t/opaque/?keep=token")
        await core.wait_jobs()
        assert server.uploads[0][0] == "PUT"
        assert server.uploads[0][1] == "/upload/t/opaque/" + result.file_name + "?keep=token"
        assert zipfile.ZipFile(io.BytesIO(server.uploads[0][2])).testzip() is None
        await core.update_firmware("A", server.base + "/firmware.bin", NOW.isoformat())
        await core.wait_jobs()
        row = next(row for row in core.state["jobs"].values() if row["kind"] == "firmware")
        assert row["bytes"] == len(server.data)
        assert ("A", "FirmwareStatusNotification", "Downloaded") in sent
        assert ("B", "DiagnosticsStatusNotification", "Uploaded") in sent
        await core.close()


@pytest.mark.parametrize("url", ["file:///tmp/fw", "ftp://allowed.test/fw", "https://other.test/fw", "https://name:pass@allowed.test/fw", "http://allowed.test/fw", "https://allowed.test/fw#fragment"])
def test_transfer_destination_policy_rejects_unsafe_or_unconfigured_urls(url):
    policy = HTTPTransferPolicy(["allowed.test"])
    with pytest.raises(TransferRejected):
        policy.validate_url(url)


async def test_download_rejects_redirect_and_oversize_without_leaving_artifact(tmp_path):
    async with HTTPFixture() as server:
        policy = HTTPTransferPolicy(["127.0.0.1"], allow_http_hosts=["127.0.0.1"], max_download_bytes=3)
        target = tmp_path / "fw.bin"
        with pytest.raises(TransferRejected):
            await policy.download(server.base + "/firmware.bin", target)
        with pytest.raises(TransferRejected):
            await policy.download(server.base + "/redirect", target)
        assert not target.exists() and not list(tmp_path.glob("*.part"))


async def test_download_rejects_truncated_body_even_without_trusted_checksum(tmp_path):
    async with HTTPFixture() as server:
        policy = HTTPTransferPolicy(["127.0.0.1"], allow_http_hosts=["127.0.0.1"])
        target = tmp_path / "fw.bin"
        with pytest.raises(TransferRejected):
            await policy.download(server.base + "/truncated", target)
        assert not target.exists() and not list(tmp_path.glob("*.part"))
