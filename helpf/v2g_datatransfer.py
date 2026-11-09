"""R5.5.0 MQTT evidence -> OCPP 1.6 DataTransfer (EDS bridge 1.2, live stream).

No charger writes and no EXI decoding. SQLite owns config/outbox; the shared PLC MQTT
client supplies observations across OCPP WebSocket reconnects. See README_V2G.md for the wire contract.
"""
import asyncio
import hashlib
import json
import logging
import math
import os
import queue
import sqlite3
import threading
import time
from collections import deque
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

LOG = logging.getLogger("V2G")
TYPES = (
    "SECCDiscoveryProtocoldata", "SupportedAppProtocol", "SessionSetup",
    "ServiceDiscovery", "ServicePaymentSelection", "ContractAuthentication",
    "ChargeParameterDiscovery", "CableCheck", "Precharge", "PowerDelivery",
    "CurrentDemand", "TimeToFull", "WeldingDetection", "SessionStop",
)
# The CSMS contract lists 13 families; CurrentDemand is not one of them. It stays
# supported and can be switched on with ChangeConfiguration, but it is off by default:
# one charging session produced 4,939 CurrentDemand records on 2026-09-22, which is
# 85% of an outbox backlog that the OCPP uplink could never drain.
CSMS_TYPES = tuple(x for x in TYPES if x != "CurrentDemand")
DEFAULTS = {
    "V2GMessageEnable": "true", "V2GMessageData": ",".join(CSMS_TYPES),
    "TimeToFullSampledInterval": "120", "CurrentDemandSampledInterval": "60",
    "V2GDataTransferVendorId": "EDS", "V2GDataTransferMessageId": "",
    # 3 s cut off real CSMS replies measured at up to 7.9 s; the abandoned record was
    # then re-sent, so the CSMS saw frames 886/887 twice and out of order.
    "V2GSendTimeoutSeconds": "10", "V2GSendIntervalMs": "50", "V2GMaxAttempts": "10",
    "V2GRequireCaptureComplete": "false",
}
ALIASES = {"TimeToFullSamapIedInterval": "TimeToFullSampledInterval"}
READONLY = {"V2GMessageSupportedData": ",".join(TYPES),
            "V2GDataTransferSchema": "eds.v2g.ocpp/1.1",
            "V2GSequenceSemantics": "pcap_exact_occurrence_order",
            "V2GSequenceDelivery": "strict_head_of_line_no_reorder_on_retry",
            "V2GOccurrenceCollapse": "vector_addon_r5_5_0_head_tail_state_change",
            "V2GCaptureMode": "live_stream_during_session_plus_post_session_gap_fill",
            "V2GDeduplicationKey": "eventId=wire_identity(connectorId,sessionId,observedAt,direction,wireName)"}
MAX_INPUT = 262144
MAX_DATA = 65536
MAX_ROWS = 10000
MAX_BYTES = 64 * 1024 * 1024
RETENTION = 7 * 86400
# A pcap_message is one frame on the wire. The live stream (during the session) and the
# post-session parser both see the same frame with the same PCAP timestamp, so the frame's
# identity is (connector, V2G SessionID, observedAt, direction, wire name). Collapse
# counters, sequence numbers and capture metadata differ between the two paths and are
# therefore NOT part of the identity: whichever path reports the frame first is sent, the
# later copy is dropped as a duplicate.
WIRE_RECORD_TYPES = ("pcap_message", "pcap_derived_field")
CLOCK_WINDOW_SECONDS = 120
LIVE_TX_LOOKBACK_SECONDS = 300
LIVE_TX_CLOCK_TOLERANCE_SECONDS = 2


def dumps(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"),
                      sort_keys=True, allow_nan=False)


def epoch(value):
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return dt.timestamp() if dt.tzinfo is not None else None
    except (ValueError, TypeError, OverflowError):
        return None


def family(value):
    aliases = {"sdp": TYPES[0], "seccdiscoveryprotocoldata": TYPES[0],
               "precharge": "Precharge"}
    text = str(value).strip()
    return aliases.get(text.lower(), next((x for x in TYPES if x.lower() == text.lower()), None))


def time_seconds(value):
    """Normalize decoded PhysicalValue time, preserving the untouched source field."""
    try:
        raw = value.get("raw", value)
        factor = {"s": 1, "m": 60, "h": 3600}.get(raw["Unit"])
        exponent = int(raw["Multiplier"])
        if factor is None or not -9 <= exponent <= 9:
            return None
        seconds = Decimal(str(raw["Value"])) * (Decimal(10) ** exponent) * factor
        if not seconds.is_finite() or seconds < 0:
            return None
        return format(seconds.normalize(), "f")
    except (AttributeError, KeyError, TypeError, ValueError, InvalidOperation):
        return None


def find_key(value, wanted):
    """Return the first exact-key match in a decoded EXI JSON tree."""
    if isinstance(value, dict):
        if wanted in value:
            return value[wanted]
        for child in value.values():
            found = find_key(child, wanted)
            if found is not None:
                return found
    elif isinstance(value, list):
        for child in value:
            found = find_key(child, wanted)
            if found is not None:
                return found
    return None


class Bridge:
    def __init__(self, state_path, station_id, now=time.time):
        self.station_id = station_id
        self.now = now
        self.lock = threading.RLock()
        path = Path(state_path)
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.db = sqlite3.connect(str(path), check_same_thread=False, timeout=2)
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS config(key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS outbox(
                seq INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT UNIQUE NOT NULL,
                family TEXT NOT NULL, data TEXT NOT NULL, size INTEGER NOT NULL,
                state TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0,
                next_try REAL NOT NULL DEFAULT 0, created REAL NOT NULL,
                result TEXT, frozen INTEGER NOT NULL DEFAULT 0);
            CREATE INDEX IF NOT EXISTS outbox_due ON outbox(state,next_try,seq);
            CREATE TABLE IF NOT EXISTS transactions(
                tx INTEGER PRIMARY KEY, connector INTEGER NOT NULL, start REAL NOT NULL,
                stop REAL, updated REAL NOT NULL);
            CREATE INDEX IF NOT EXISTS tx_window ON transactions(connector,start,stop);
            CREATE TABLE IF NOT EXISTS cadence(
                key TEXT PRIMARY KEY, observed REAL NOT NULL, updated REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS session_tx(
                connector INTEGER NOT NULL, session_id TEXT NOT NULL, tx INTEGER NOT NULL,
                updated REAL NOT NULL, PRIMARY KEY(connector, session_id));
        """)
        for key, value in DEFAULTS.items():
            self.db.execute("INSERT OR IGNORE INTO config VALUES (?,?)", (key, value))
        self.db.commit()
        os.chmod(path, 0o600)
        # Live stream + post-session bursts share V2G/TestEgat; a small inbox dropped records.
        self.inbox = queue.Queue(maxsize=4096)
        # (edgebox receive time, receive - add-on clock) samples -> vSECC clock offset
        self.clock_samples = deque(maxlen=600)
        self.shared_mqtt = None
        self.mqtt_callback = None
        self.worker = None
        self.stopping = threading.Event()
        self.last_housekeep = 0
        self.dropped = 0
        # Seconds the head-of-line record still owes its retry backoff, so the sender
        # waits for it instead of sending a later frame ahead of it.
        self.retry_wait = 0.0

    def config(self):
        with self.lock:
            return dict(self.db.execute("SELECT key,value FROM config"))

    def configuration_items(self):
        cfg = self.config()
        items = [{"key": k, "readonly": False, "value": v} for k, v in cfg.items()]
        items.extend({"key": k, "readonly": False, "value": cfg[v]} for k, v in ALIASES.items())
        items.extend({"key": k, "readonly": True, "value": v} for k, v in READONLY.items())
        return items

    def change_configuration(self, key, value):
        key = ALIASES.get(key, key)
        if key in READONLY:
            return "Rejected"
        if key not in DEFAULTS:
            return "NotSupported"
        if not isinstance(value, str):
            return "Rejected"
        value = value.strip()
        if key == "V2GMessageEnable":
            value = value.lower()
            if value not in ("true", "false"):
                return "Rejected"
        elif key == "V2GMessageData":
            selected = [family(x) for x in value.split(",")] if value else []
            if any(x is None for x in selected):
                return "Rejected"
            value = ",".join(dict.fromkeys(selected))
        elif key.endswith("Interval"):
            if not value.isascii() or not value.isdigit() or not 1 <= int(value) <= 86400:
                return "Rejected"
            value = str(int(value))
        elif key in ("V2GSendTimeoutSeconds", "V2GSendIntervalMs", "V2GMaxAttempts"):
            low, high = {"V2GSendTimeoutSeconds": (1, 300), "V2GSendIntervalMs": (0, 60000),
                         "V2GMaxAttempts": (1, 100)}[key]
            if not value.isascii() or not value.isdigit() or not low <= int(value) <= high:
                return "Rejected"
            value = str(int(value))
        elif key == "V2GRequireCaptureComplete":
            value = value.lower()
            if value not in ("true", "false"):
                return "Rejected"
        elif key == "V2GDataTransferVendorId":
            if not value or len(value) > 255:
                return "Rejected"
        elif key == "V2GDataTransferMessageId" and len(value) > 50:
            return "Rejected"
        try:
            with self.lock, self.db:
                self.db.execute("UPDATE config SET value=? WHERE key=?", (value, key))
            LOG.info("[V2G] configuration %s updated", key)
            return "Accepted"
        except sqlite3.Error:
            LOG.exception("[V2G] configuration persistence failed")
            return "Rejected"

    def submit(self, payload, retained=False):
        if retained:
            return  # Never replay an obsolete retained session as a new observation.
        if not isinstance(payload, (bytes, str, dict)) or (not isinstance(payload, dict) and len(payload) > MAX_INPUT):
            self._drop("invalid_or_oversized_mqtt")
            return
        try:
            self.inbox.put_nowait(("mqtt", (payload, self.now())))
        except queue.Full:
            self._drop("mqtt_inbox_full")

    def observe_ocpp(self, request, response):
        """Non-blocking observation only, after the original OCPP call succeeds."""
        action = type(request).__name__
        if action not in ("StartTransaction", "StopTransaction") or response is None:
            return
        if action == "StartTransaction":
            info = getattr(response, "id_tag_info", {}) or {}
            status = info.get("status") if isinstance(info, dict) else getattr(info, "status", None)
            if status != "Accepted":
                return
            event = ("start", getattr(request, "connector_id", None),
                     getattr(response, "transaction_id", None), getattr(request, "timestamp", None))
        else:
            event = ("stop", None, getattr(request, "transaction_id", None),
                     getattr(request, "timestamp", None))
        try:
            self.inbox.put_nowait(("ocpp", event))
        except queue.Full:
            self._drop("transaction_observation_queue_full")

    def record_transaction(self, action, connector, tx, timestamp):
        at = epoch(timestamp)
        if type(tx) is not int or tx <= 0 or at is None:
            return
        with self.lock, self.db:
            if action == "start" and connector in (1, 2):
                self.db.execute("INSERT OR IGNORE INTO transactions VALUES (?,?,?,?,?)",
                                (tx, connector, at, None, self.now()))
            elif action == "stop":
                self.db.execute("UPDATE transactions SET stop=?,updated=? WHERE tx=? AND start<=?",
                                (at, self.now(), tx, at))

    def _drop(self, reason):
        self.dropped += 1
        if self.dropped <= 5 or self.dropped % 100 == 0:
            LOG.error("[V2G] dropped=%d reason=%s", self.dropped, reason)

    def _base(self, p, msg_type, record_type):
        session = p.get("session") or {}
        association = p.get("association") or {}
        return {
            "schemaVersion": "eds.v2g.ocpp/1.1", "chargePointId": self.station_id,
            "sourceStationId": p.get("stationId"), "connectorId": p["connectorId"],
            "messageType": msg_type, "recordType": record_type,
            "sessionId": p.get("sessionId") or session.get("v2gSessionId") or association.get("selectedSessionId"),
            "observationId": session.get("observationId"),
            "sessionStartedAt": association.get("preferredSessionStartedAt") or session.get("firstObservedAt"),
            "sessionEndedAt": association.get("preferredSessionEndedAt"),
            "observedAt": p.get("observedAt"),
            "sourceKind": p.get("kind"), "sourceAddonVersion": p.get("addonVersion"),
            "rawCaptureId": p.get("rawCaptureId"),
            "captureComplete": p.get("captureComplete"),
            "protocolBoundaryComplete": p.get("protocolBoundaryComplete"),
            "protocolTerminationStatus": p.get("protocolTerminationStatus"),
            "transactionId": None, "transactionAssociation": "unresolved",
        }

    def observe_clock(self, p, received):
        """Add-on status carries the vSECC/Node-RED wall clock (`time`, 1 Hz). The smallest
        (receive - time) over the last minutes is the clock offset plus the minimum local
        MQTT latency (a few ms). PCAP timestamps come from the same vSECC clock."""
        if received is None or p.get("kind") != "v2g_automation_status":
            return
        sent = epoch(p.get("time"))
        if sent is None:
            return
        with self.lock:
            self.clock_samples.append((received, received - sent))

    def clock_offset(self):
        with self.lock:
            now = self.now()
            recent = [d for (r, d) in self.clock_samples if now - r <= CLOCK_WINDOW_SECONDS]
        if len(recent) < 3:
            return None
        return min(recent)

    def ingest(self, payload, received=None):
        """Accept only documented evidence kinds; diagnostics are not V2G messages."""
        p = json.loads(payload) if isinstance(payload, (str, bytes)) else payload
        if isinstance(p, dict):
            self.observe_clock(p, received)
        if not isinstance(p, dict) or type(p.get("connectorId")) is not int or p["connectorId"] not in (1, 2):
            return
        cfg = self.config()
        if cfg["V2GMessageEnable"] != "true":
            return
        selected = set(cfg["V2GMessageData"].split(","))
        kind = p.get("kind")
        records = []
        if kind == "v2g_raw_message_occurrence":
            msg = family(p.get("messageId"))
            direction = p.get("direction")
            decoded = p.get("decoded")
            frame_seq = p.get("frameSeq")
            if (msg and direction in ("request", "response") and
                    isinstance(decoded, dict) and type(frame_seq) is int and
                    epoch(p.get("observedAt")) is not None):
                out = self._base(p, msg, "pcap_message")
                out.update(
                    direction=direction,
                    observedAt=p.get("observedAt"),
                    pcapFrameSeq=frame_seq,
                    pcapSequenceIndex=p.get("sequenceIndex"),
                    wireName=p.get("wireName"),
                    decoded=decoded,
                    rawV2G=True,
                    sequenceMode="pcap_exact_occurrence_order",
                    sequenceSource=p.get("sequenceSource") or "post_session",
                    captureMode=p.get("captureMode") or "post_session_finalized_pcap",
                )
                # Only present when the Vector add-on collapsed a polling loop.
                # Absent keys keep R5.4.7 payloads hash-identical, so an outbox
                # written by the previous add-on is not duplicated after upgrade.
                for out_key, src_key in (("pcapOccurrenceIndex", "occurrenceIndex"),
                                         ("occurrenceCount", "occurrenceCount"),
                                         ("suppressedBefore", "suppressedBefore"),
                                         ("suppressedCount", "suppressedCount"),
                                         ("collapsed", "collapsed"),
                                         ("collapseReason", "collapseReason"),
                                         ("collapseRule", "collapseRule")):
                    if p.get(src_key) is not None:
                        out[out_key] = p[src_key]
                records.append(out)

                # TimeToFull is a CSMS logical item carried inside a
                # CurrentDemandReq, not a separate V2G wire message. Place the
                # derived DataTransfer directly after its parent PCAP frame so
                # it cannot jump ahead of the protocol sequence.
                cadence_key = dumps([p.get("stationId"), p["connectorId"],
                                     p.get("sessionId"), "TimeToFull", "pcap"])
                at = epoch(p.get("observedAt"))
                if (msg == "CurrentDemand" and direction == "request" and
                        "TimeToFull" in selected and at is not None and
                        self._cadence_due(cadence_key, at,
                                          int(cfg["TimeToFullSampledInterval"]))):
                    physical = find_key(decoded, "RemainingTimeToFullSoC")
                    seconds = time_seconds(physical)
                    if seconds is not None:
                        ttf = self._base(p, "TimeToFull", "pcap_derived_field")
                        ttf.update(
                            direction=direction,
                            observedAt=p.get("observedAt"),
                            pcapFrameSeq=frame_seq,
                            pcapSequenceIndex=p.get("sequenceIndex"),
                            derivedAfterParent=True,
                            derivedFrom="CurrentDemandReq.RemainingTimeToFullSoC",
                            values={"EstimateChargingTime": seconds},
                            unit="s",
                            rawV2G=True,
                            sequenceMode="pcap_exact_occurrence_order",
                            sequenceSource=p.get("sequenceSource") or "post_session",
                            captureMode=p.get("captureMode") or "post_session_finalized_pcap",
                        )
                        records.append(ttf)
        # The old checklist, first/last samples, stop audit and live telemetry
        # are deliberately not enqueued. They are useful diagnostics, but they
        # are summaries/derived observations and therefore cannot reproduce
        # PCAP packet order. The Vector flow still publishes them for diagnosis.
        require_complete = cfg["V2GRequireCaptureComplete"] == "true"
        for record in records:
            if record["messageType"] not in selected:
                continue
            # A capture whose protocol boundary was never proven is still honest
            # evidence, but the CSMS can ask not to be given partial sessions.
            if require_complete and record.get("captureComplete") is not True:
                self._drop("capture_incomplete_suppressed_by_config")
                continue
            self.enqueue(record)

    def _cadence_due(self, key, observed, interval):
        """Atomically reserve a sampled derived-field position in wire order."""
        with self.lock, self.db:
            last = self.db.execute("SELECT observed FROM cadence WHERE key=?", (key,)).fetchone()
            if last and observed - last[0] < interval:
                return False
            self.db.execute("INSERT OR REPLACE INTO cadence VALUES (?,?,?)",
                            (key, observed, self.now()))
            return True

    def _telemetry(self, p, cfg, selected):
        session = p.get("session") or {}
        if session.get("observationOpen") is not True or not session.get("observationId"):
            return
        at = epoch(p.get("observedAt") or p.get("exportedAt"))
        if at is None:
            return
        values = p.get("values") or {}
        quality = p.get("quality") or {}
        for msg, names, interval_key in (
            ("TimeToFull", ("EstimateChargingTime",), "TimeToFullSampledInterval"),
            ("CurrentDemand", ("EVTargetCurrent", "EVTargetVoltage"), "CurrentDemandSampledInterval"),
        ):
            if msg not in selected:
                continue
            if msg == "CurrentDemand" and "CurrentDemand" not in str(values.get("ChargingSessionState", "")):
                continue
            data, quality_out = {}, {}
            for name in names:
                value = p.get(name) if name in p else values.get(name)
                q = quality if p.get("kind") == "time_to_full_telemetry" else quality.get(name, {})
                if not isinstance(q, dict) or q.get("status") != "observed_in_session" or value is None:
                    continue
                try:
                    if not math.isfinite(float(value)) or float(value) < 0 or isinstance(value, bool):
                        continue
                    if float(q.get("ageMs") or 0) > 120000:
                        continue
                except (TypeError, ValueError):
                    continue
                data[name] = str(value) if msg == "TimeToFull" else value
                quality_out[name] = q
            if len(data) != len(names):
                continue
            cadence_key = dumps([p.get("stationId"), p["connectorId"], session["observationId"], msg])
            with self.lock, self.db:
                last = self.db.execute("SELECT observed FROM cadence WHERE key=?", (cadence_key,)).fetchone()
                if last and at - last[0] < int(cfg[interval_key]):
                    continue
                out = self._base(p, msg, "mqtt_telemetry")
                out.update(values=data, quality=quality_out, rawV2G=False,
                           vectorTransactionId=values.get("OCPPTransactionID"),
                           unit="s" if msg == "TimeToFull" else None)
                if self.enqueue(out):
                    self.db.execute("INSERT OR REPLACE INTO cadence VALUES (?,?,?)", (cadence_key, at, self.now()))

    @staticmethod
    def wire_identity(record):
        return {"identity": "eds.v2g.wire/1", "recordType": record.get("recordType"),
                "connectorId": record.get("connectorId"),
                "sessionId": str(record.get("sessionId") or "").lower(),
                "observedAt": record.get("observedAt"), "direction": record.get("direction"),
                "wireName": record.get("wireName") or record.get("messageType"),
                "messageType": record.get("messageType")}

    def enqueue(self, record):
        if record.get("recordType") in WIRE_RECORD_TYPES:
            identity = self.wire_identity(record)
        else:
            # Ignore reparse build IDs, while retaining original capture metadata in data.
            identity = {k: v for k, v in record.items() if k not in (
                "rawCaptureId", "sourceAddonVersion", "transactionId", "transactionAssociation")}
        event_id = hashlib.sha256(dumps(identity).encode()).hexdigest()
        record = dict(record, eventId=event_id)
        body = dumps(record)
        size = len(body.encode())
        if size > MAX_DATA:
            self._drop("datatransfer_over_64KiB_not_truncated")
            return False
        with self.lock, self.db:
            if self.db.execute("SELECT 1 FROM outbox WHERE event_id=?", (event_id,)).fetchone():
                return True
            count, total = self.db.execute("SELECT COUNT(*),COALESCE(SUM(size),0) FROM outbox WHERE state!='accepted'").fetchone()
            if count >= MAX_ROWS or total + size > MAX_BYTES:
                self._drop("persistent_outbox_full")
                return False
            self.db.execute("INSERT INTO outbox(event_id,family,data,size,created) VALUES (?,?,?,?,?)",
                            (event_id, record["messageType"], body, size, self.now()))
        LOG.info("[V2G] queued %s C%s %s", record["messageType"], record["connectorId"], event_id[:12])
        return True

    def next_record(self):
        cfg = self.config()
        if cfg["V2GMessageEnable"] != "true":
            return None
        selected = [x for x in cfg["V2GMessageData"].split(",") if x]
        if not selected:
            return None
        with self.lock, self.db:
            self.housekeep()
            # Strict head-of-line delivery: the oldest pending record of a selected
            # family is the ONLY candidate. R5.4.7 filtered on next_try inside the
            # query, so a record waiting out its backoff was skipped and the next
            # frame went first - that is how frame 887 reached the CSMS before
            # frame 886 was re-sent. A record of a de-selected family never blocks.
            sql = ("SELECT * FROM outbox WHERE state='pending' AND family IN ("
                   + ",".join("?" for _ in selected) + ") ORDER BY seq LIMIT 1")
            row = self.db.execute(sql, selected).fetchone()
            if row is None:
                self.retry_wait = 0.0
                return None
            wait = row["next_try"] - self.now()
            if wait > 0:
                self.retry_wait = wait
                return None
            self.retry_wait = 0.0
            if not row["frozen"]:
                body = json.loads(row["data"])
                offset = self.clock_offset()
                observed = epoch(body.get("observedAt"))
                if offset is not None and observed is not None:
                    # vSECC PCAP clock -> this charge point's (OCPP) clock
                    body["clockOffsetMs"] = int(round(offset * 1000))
                    body["observedAtChargePointClock"] = datetime.fromtimestamp(
                        observed + offset, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
                self._associate(body, offset)
                frozen = dumps(body)
                self.db.execute("UPDATE outbox SET data=?,size=?,frozen=1 WHERE seq=?",
                                (frozen, len(frozen.encode()), row["seq"]))
                row = self.db.execute("SELECT * FROM outbox WHERE seq=?", (row["seq"],)).fetchone()
            return dict(row, vendor_id=cfg["V2GDataTransferVendorId"],
                        message_id=cfg["V2GDataTransferMessageId"] or row["family"],
                        send_timeout=float(cfg["V2GSendTimeoutSeconds"]),
                        send_interval=int(cfg["V2GSendIntervalMs"]) / 1000.0)

    def _associate(self, body, offset):
        """Attach the OCPP transactionId. Called with self.lock held."""
        connector = body["connectorId"]
        sid = str(body.get("sessionId") or "").lower()
        if sid:
            memo = self.db.execute("SELECT tx FROM session_tx WHERE connector=? AND session_id=?",
                                   (connector, sid)).fetchone()
            if memo:
                body["transactionId"] = memo[0]
                body["transactionAssociation"] = "same_v2g_session_as_earlier_record"
                return
        start = epoch(body.get("sessionStartedAt"))
        end = epoch(body.get("sessionEndedAt"))
        live = body.get("captureMode") == "live_stream" and body["recordType"] in WIRE_RECORD_TYPES
        if live:
            # The session is still running: the only candidate is the transaction that is
            # open on this connector right now and began after this V2G session started
            # (a stale open transaction from an earlier plug-in never matches).
            if start is None:
                return
            lo = start + (offset or 0.0) - LIVE_TX_LOOKBACK_SECONDS
            observed = epoch(body.get("observedAt"))
            txs = self.db.execute(
                "SELECT tx FROM transactions WHERE connector=? AND stop IS NULL AND start>=? AND start<=?",
                (connector, lo, self.now())).fetchall()
            if not txs and observed is not None:
                # Field 2026-09-23 tx1656: the whole handshake ran before RemoteStart, so no
                # record was sent while the transaction was open and the session->tx memo
                # was empty when the flushed tail (PowerDelivery stop / SessionStop) arrived
                # after StopTransaction. Match the frame's own time (vSECC clock corrected
                # to this charge point) against a transaction of this plug-in that was
                # open at that moment.
                seen = observed + (offset or 0.0)
                txs = self.db.execute(
                    "SELECT tx FROM transactions WHERE connector=? AND start>=? AND start<=? "
                    "AND stop IS NOT NULL AND stop>=?",
                    (connector, lo, seen + LIVE_TX_CLOCK_TOLERANCE_SECONDS,
                     seen - LIVE_TX_CLOCK_TOLERANCE_SECONDS)).fetchall()
                if len(txs) == 1:
                    body["transactionId"] = txs[0][0]
                    body["transactionAssociation"] = "frame_time_inside_stopped_ocpp_transaction"
                    txs = None
            if txs is None:
                pass
            elif len(txs) == 1:
                body["transactionId"] = txs[0][0]
                body["transactionAssociation"] = "open_ocpp_transaction_during_live_v2g_session"
            elif len(txs) > 1:
                body["transactionAssociation"] = "ambiguous_open_transactions"
            else:
                body["transactionAssociation"] = "no_ocpp_transaction_yet_at_send_time"
        else:
            # Live telemetry correlates at its own observed time. Decoded evidence
            # uses the frozen whole-session window; never the current CP transaction.
            if body["recordType"] == "mqtt_telemetry":
                start = end = epoch(body.get("observedAt"))
            if start is not None and end is not None and end >= start:
                txs = self.db.execute(
                    "SELECT tx FROM transactions WHERE connector=? AND start<=? AND COALESCE(stop,?)>=?",
                    (connector, end, self.now(), start)).fetchall()
                if len(txs) == 1:
                    body["transactionId"] = txs[0][0]
                    body["transactionAssociation"] = "unique_time_overlap_with_observed_ocpp_transaction"
                elif len(txs) > 1:
                    body["transactionAssociation"] = "ambiguous_time_overlap"
        if sid and body.get("transactionId") is not None and body["recordType"] in WIRE_RECORD_TYPES:
            self.db.execute("INSERT OR REPLACE INTO session_tx VALUES (?,?,?,?)",
                            (connector, sid, body["transactionId"], self.now()))

    def finish(self, row, status):
        status = str(getattr(status, "value", status))
        attempts = row["attempts"] + 1
        max_attempts = int(self.config()["V2GMaxAttempts"])
        # Unknown vendor/message or explicit rejection needs contract/config review;
        # quarantine it instead of retrying forever and delaying unrelated messages.
        terminal = status in ("Rejected", "UnknownVendorId", "UnknownMessageId")
        if status == "Accepted":
            state = "accepted"
        elif terminal:
            state = "rejected"
        elif attempts >= max_attempts:
            # Head-of-line order means one poison record would stall every later
            # frame, so give up on it explicitly rather than block the outbox.
            state, status = "rejected", "attempts_exhausted:" + status
        else:
            state = "pending"
        with self.lock, self.db:
            self.db.execute("UPDATE outbox SET state=?,attempts=?,next_try=?,result=? WHERE seq=?",
                            (state, attempts, self.now() + min(300, 2 ** min(attempts, 8)), status[:200], row["seq"]))
            if state == "accepted":
                # Keep the event hash for dedup, not a second copy of every payload.
                self.db.execute("UPDATE outbox SET data='{}',size=2 WHERE seq=?", (row["seq"],))
        LOG.log(logging.INFO if state == "accepted" else logging.WARNING,
                "[V2G] %s %s event=%s attempt=%s", status, row["family"], row["event_id"][:12], attempts)

    def housekeep(self):
        now = self.now()
        if now - self.last_housekeep < 3600:
            return
        cutoff = now - RETENTION
        expired = self.db.execute("SELECT COUNT(*) FROM outbox WHERE state='pending' AND created<?", (cutoff,)).fetchone()[0]
        if expired:
            LOG.error("[V2G] expiring %d undelivered records older than 7 days", expired)
        self.db.execute("DELETE FROM outbox WHERE created<?", (cutoff,))
        self.db.execute("DELETE FROM outbox WHERE state='accepted' AND seq NOT IN "
                        "(SELECT seq FROM outbox WHERE state='accepted' ORDER BY seq DESC LIMIT 50000)")
        self.db.execute("DELETE FROM cadence WHERE updated<?", (cutoff,))
        self.db.execute("DELETE FROM transactions WHERE updated<?", (cutoff,))
        self.db.execute("DELETE FROM session_tx WHERE updated<?", (cutoff,))
        self.last_housekeep = now

    async def send_loop(self, cp_instance):
        from ocpp.v16 import call
        factory = getattr(call, "DataTransfer", None) or getattr(call, "DataTransferPayload", None)
        if factory is None:
            LOG.error("[V2G] ocpp package has no DataTransfer payload class")
            return
        while True:
            try:
                if not getattr(cp_instance, "_v2g_boot_accepted", False):
                    await asyncio.sleep(1)
                    continue
                row = await asyncio.to_thread(self.next_record)
                if row is None:
                    # Nothing due: either the outbox is empty or the head record is
                    # still in backoff. Never send a later frame to fill the gap.
                    await asyncio.sleep(min(0.5, self.retry_wait) if self.retry_wait > 0 else 0.5)
                    continue
                req = factory(vendor_id=row["vendor_id"], message_id=row["message_id"], data=row["data"])
                try:
                    # Covers waiting for the shared ocpp call lock AND the response.
                    res = await asyncio.wait_for(cp_instance.call(req), timeout=row["send_timeout"])
                    status = getattr(res, "status", "NoResponse")
                except asyncio.TimeoutError:
                    status = "Timeout"
                except Exception as exc:
                    if (getattr(cp_instance, "_dual_manager", None) is not None
                            and getattr(exc, "defer_without_attempt", False)):
                        # The bridge stays alive while one upstream is offline.
                        # No message was sent, so retain this record without
                        # exhausting its existing V2G delivery retry budget.
                        await asyncio.sleep(5)
                        continue
                    status = type(exc).__name__
                await asyncio.to_thread(self.finish, row, status)
                if row["send_interval"]:
                    await asyncio.sleep(row["send_interval"])
            except asyncio.CancelledError:
                raise
            except Exception:
                LOG.exception("[V2G] sender error; charging tasks continue")
                await asyncio.sleep(5)

    def start_mqtt(self, shared_mqtt):
        """Attach once to helpf.plc_function's existing client. Never own a socket."""
        if self.shared_mqtt is not None:
            if self.shared_mqtt is not shared_mqtt:
                raise RuntimeError("V2G already attached to a different PLC client")
            return
        def incoming(topic, payload, meta):
            if topic == "V2G/TestEgat":
                self.submit(payload, bool(meta.get("retained", False)))
        incoming.wants_meta = True
        self.shared_mqtt = shared_mqtt
        self.mqtt_callback = incoming
        self.worker = threading.Thread(target=self._consume, name="v2g-persist", daemon=True)
        self.worker.start()
        # Callback must exist before subscribe; the PLC client owns reconnects.
        shared_mqtt.register_topic_callback("V2G/TestEgat", incoming)
        shared_mqtt.subscribe_topic("V2G/TestEgat", qos=1)
        LOG.info("[V2G] attached to shared PLC MQTT %s:%s V2G/TestEgat",
                 shared_mqtt.config.broker, shared_mqtt.config.port)

    def _consume(self):
        while not self.stopping.is_set():
            try:
                kind, item = self.inbox.get(timeout=0.5)
            except queue.Empty:
                continue
            try:
                if kind == "mqtt":
                    item, received = item if isinstance(item, tuple) else (item, None)
                    if isinstance(item, dict) and len(dumps(item).encode()) > MAX_INPUT:
                        self._drop("oversized_shared_mqtt_payload")
                    else:
                        self.ingest(item, received=received)
                else:
                    self.record_transaction(*item)
            except Exception:
                LOG.exception("[V2G] invalid input/persistence failure; discarded one item")
            finally:
                self.inbox.task_done()

    def close(self):
        self.stopping.set()
        if self.shared_mqtt is not None and self.mqtt_callback is not None:
            self.shared_mqtt.unregister_topic_callback("V2G/TestEgat", self.mqtt_callback)
        # The PLC owns the connection: never disconnect or unsubscribe it here.
        if self.worker:
            self.worker.join(timeout=3)
        with self.lock:
            self.db.close()


_singleton = None


def get_bridge(shared_mqtt, station_id):
    global _singleton
    if _singleton is None:
        bridge = None
        try:
            bridge = Bridge(os.getenv("V2G_STATE_DB", ".v2g_bridge/state.sqlite3"), station_id)
            bridge.start_mqtt(shared_mqtt)
            _singleton = bridge
        except Exception:
            LOG.exception("[V2G] bridge unavailable; original charger remains operational")
            if bridge:
                bridge.close()
    return _singleton
