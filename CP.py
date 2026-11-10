# -*- coding: utf-8 -*-
# CP RFID/OCPP V10 - physical Charging/Suspended status + correct zero-power offset
# Dual-CSMS test build 2026-11-07: one hardware core, owner-scoped OCPP sessions.
import os
import time
import json
import ast
import pytz
import queue
import asyncio
import logging
import calendar
import inspect
import websockets
import functools
import threading
import dataclasses
from contextlib import asynccontextmanager
from pathlib import Path
import RPi.GPIO as GPIO
import psutil
from datetime import timezone
from datetime import datetime
from types import SimpleNamespace
from sdnotify import SystemdNotifier
from ocpp.routing import on
from ocpp.v16 import ChargePoint as cp
from ocpp.v16 import call, call_result
from ocpp.v16.enums import RemoteStartStopStatus, Action, AuthorizationStatus, RegistrationStatus
import paho.mqtt.client as mqtt
from helpf.pin_stop_bridge import PinStopBridge, valid_pin
from helpf.stop_pin_config import StopPinConfig, KEYS as STOP_PIN_KEYS
from helpf.meter_energy import MeterEnergy, energy_wh
from helpf.v2g_datatransfer import get_bridge
from helpf.vector_fault_ocpp import (
    VectorFaultEventBridge,
    VectorFaultInbox,
    VectorFaultRecord,
)
from helpf.dual_csms import DualCSMSManager, DualCSMSError, validate_config, check_single_csms_fallback, command_origin
from helpf.ocpp_runtime import OCPPProtocolMixin, service_status_active
from helpf.len_meter_test import *
from helpf.plc_function import (
    initialize_plc_runtime,
    remote_start_plc,
    remote_stop_plc,
    remote_start_plc_W,
    remote_stop_plc_W,
    limitpower,
    limitcurrent,
    limit2connector,
    cmd_low,
    set_cp_instance,
    handle_rfid_command,
    _write_file,
    last_sent,
    take_local_request,
    clear_local_request,
)

# A missing/disabled config preserves the original single-CSMS connection.
# Validate before starting the PLC runtime; never open two sockets to the same URL.
_DUAL_CONFIG_PATH = Path(os.environ.get(
    "OCPP_DUAL_CONFIG", str(Path(__file__).with_name("dual_csms_config.json"))))
DUAL_CSMS_CONFIG = json.loads(_DUAL_CONFIG_PATH.read_text(encoding="utf-8-sig")) if _DUAL_CONFIG_PATH.exists() else {"enabled": False}
if DUAL_CSMS_CONFIG.get("enabled", False):
    validate_config(DUAL_CSMS_CONFIG)
else:
    check_single_csms_fallback(os.getcwd())

plc = initialize_plc_runtime()
pin_stop_bridge = PinStopBridge(plc)
pin_stop_bridge.start()
stop_pin_config = StopPinConfig(os.path.dirname(os.path.abspath(__file__)))
meter_energy = MeterEnergy(os.path.dirname(os.path.abspath(__file__)))

notifier = SystemdNotifier()

# ============================================================
# OCPP broker (publish heartbeat / meter data)
# ============================================================

# ============================================================
# StopTransaction carrying connectorId
# ------------------------------------------------------------
# OCPP 1.6 StopTransaction.req has no connectorId - the CSMS is expected to map
# the transactionId it issued at StartTransaction back to a connector - and the
# bundled JSON schema sets additionalProperties=false. Sending it is therefore a
# deliberate extension, requested for this deployment:
#   * the payload is sent with local schema validation skipped, and
#   * if the CSMS answers CALLERROR the plain, spec-conformant StopTransaction is
#     resent immediately, so a stop can never be lost because of the extra field.
# The ocpp library derives the OCPP action from the class name, so __name__ must
# read StopTransaction even though the Python symbol is more descriptive.
# ============================================================
@dataclasses.dataclass
class _StopTransactionWithConnectorId:
    meter_stop: int
    timestamp: str
    transaction_id: int
    connector_id: int
    reason: object = None
    id_tag: object = None
    transaction_data: object = None


_StopTransactionWithConnectorId.__name__ = "StopTransaction"

# Older ocpp releases have no skip_schema_validation kwarg; without it the extra
# field cannot leave this process at all, so fall back to the standard payload.
STOP_TXN_CONNECTOR_ID_SUPPORTED = (
    "skip_schema_validation" in inspect.signature(cp.call).parameters
)

OCPP_BROKER   = "mqtt.example.com"
OCPP_PORT     = 1883
OCPPSTATION   = "Station1"
RFID_COMMAND_TOPIC = "topic/RFIDCommand"

# Latest station-level temperature sampled by edgebox_temp().  MeterValues for
# both connectors use this shared reading; None means that no valid reading is
# currently available and Temperature must be omitted from the payload.
LATEST_EDGEBOX_TEMP_C = None

# AutoCharge stays in CP.py: plc_function already exposes a non-blocking topic
# callback API, so its MQTT thread only enqueues EVCCID events.  The OCPP event
# loop drains this queue and performs Authorize without blocking the MQTT thread.
AUTOCHARGE_ID_TAG_PREFIX = "AUTO-"          # default; OCPP key AutoChargeIdTagPrefix
AUTOCHARGE_PREFIX_MAX_LEN = 8                # 8 + 12-hex EVCCID = 20 (OCPP 1.6 CiString20)
AUTOCHARGE_PREFIX_ALLOWED = set(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.:"
)


def _normalize_autocharge_prefix(value):
    """Return a valid AutoCharge idTag prefix, or None if the value is not allowed."""
    text = str(value if value is not None else "").strip()
    if not text or len(text) > AUTOCHARGE_PREFIX_MAX_LEN:
        return None
    if any(ch not in AUTOCHARGE_PREFIX_ALLOWED for ch in text):
        return None
    return text
AUTOCHARGE_EVCCID_TOPIC = "vsecc/connector/{connector}/ev/evccid"
AUTOCHARGE_EVCCID_EVENTS = queue.Queue(maxsize=64)
AUTOCHARGE_EVCCID_LATEST = {"1": "", "2": ""}
AUTOCHARGE_EVCCID_LOCK = threading.RLock()

# EV-initiated shutdown is observed on the existing plant MQTT connection.  The
# callbacks below run on paho's network thread, so they only validate and enqueue;
# the OCPP asyncio loop remains the sole owner of transaction state and messages.
EV_STOP_V2G_TOPIC = "V2G/TestEgat"
EV_STOP_STATE_TOPIC = "vsecc/connector/{connector}/status/charging_session_state"
EV_STOP_EVENTS = queue.Queue(maxsize=64)
EV_STOP_EVENT_MAX_AGE_SECONDS = 30.0

# A local stop command should raise the PLC Finishing flag within a few seconds.
# Keep its origin only long enough to distinguish it from an EV-initiated finish;
# an abandoned command must not mislabel a much later EV stop as Local.
LOCAL_STOP_REASON_TTL_SECONDS = 30.0


def _autocharge_evccid_callback(topic, payload_value):
    """Capture one EVCCID publication without doing OCPP work on the MQTT thread."""
    try:
        parts = str(topic or "").split("/")
        if len(parts) != 5 or parts[:2] != ["vsecc", "connector"] or parts[3:] != ["ev", "evccid"]:
            return
        connector = parts[2]
        if connector not in ("1", "2"):
            return
        evccid = str(payload_value if payload_value is not None else "").strip()
        with AUTOCHARGE_EVCCID_LOCK:
            AUTOCHARGE_EVCCID_LATEST[connector] = evccid
        try:
            AUTOCHARGE_EVCCID_EVENTS.put_nowait((connector, evccid))
        except queue.Full:
            # Preserve the newest observation. The asyncio side also coalesces
            # multiple publications per connector before dispatching work.
            try:
                AUTOCHARGE_EVCCID_EVENTS.get_nowait()
            except queue.Empty:
                pass
            try:
                AUTOCHARGE_EVCCID_EVENTS.put_nowait((connector, evccid))
            except queue.Full:
                pass
    except Exception as e:
        logging.warning(f"[AUTOCHARGE] EVCCID callback failed: {e}")


def _find_nested_value(value, wanted):
    """Return the first exact-key match in a decoded V2G JSON tree."""
    if isinstance(value, dict):
        if wanted in value:
            return value[wanted]
        for child in value.values():
            found = _find_nested_value(child, wanted)
            if found is not None:
                return found
    elif isinstance(value, list):
        for child in value:
            found = _find_nested_value(child, wanted)
            if found is not None:
                return found
    return None


def _enqueue_ev_stop_event(connector_id, source, detail, session_id=None):
    """Queue one fresh shutdown observation without blocking the MQTT thread."""
    event = {
        "connector_id": int(connector_id),
        "source": str(source),
        "detail": str(detail),
        "session_id": str(session_id or ""),
        "received_at": time.monotonic(),
    }
    try:
        EV_STOP_EVENTS.put_nowait(event)
        return
    except queue.Full:
        pass

    # Preserve the newest physical observation if the consumer was briefly busy.
    try:
        EV_STOP_EVENTS.get_nowait()
    except queue.Empty:
        pass
    try:
        EV_STOP_EVENTS.put_nowait(event)
    except queue.Full:
        logging.warning("[EV-STOP] event queue full; dropped newest C%s %s",
                        connector_id, detail)


def _v2g_ev_stop_callback(topic, payload_value, meta=None):
    """Capture live EV->EVSE PowerDelivery/SessionStop requests only."""
    try:
        if (meta or {}).get("retained"):
            return
        data = payload_value if isinstance(payload_value, dict) else json.loads(
            payload_value.decode() if isinstance(payload_value, (bytes, bytearray))
            else str(payload_value)
        )
        if not isinstance(data, dict):
            return
        if data.get("kind") != "v2g_raw_message_occurrence":
            return
        if str(data.get("captureMode") or "") != "live_stream":
            return                    # never act on delayed post-session replay
        if str(data.get("direction") or "").lower() != "request":
            return

        connector_id = int(data.get("connectorId") or 0)
        if connector_id not in (1, 2):
            return
        message_id = str(data.get("messageId") or "").strip().lower()
        wire_name = str(data.get("wireName") or "").strip().lower()

        if message_id == "sessionstop" or wire_name == "sessionstopreq":
            _enqueue_ev_stop_event(
                connector_id, "v2g_live", "SessionStopReq", data.get("sessionId")
            )
            return

        if message_id != "powerdelivery" and wire_name != "powerdeliveryreq":
            return
        ready = _find_nested_value(data.get("decoded"), "ReadyToChargeState")
        is_stop = (
            ready is False or
            (not isinstance(ready, bool) and ready == 0) or
            str(ready).strip().lower() in ("false", "0")
        )
        if is_stop:
            _enqueue_ev_stop_event(
                connector_id, "v2g_live", "PowerDeliveryReq.ReadyToChargeState=false",
                data.get("sessionId"),
            )
    except Exception as error:
        logging.warning("[EV-STOP] invalid V2G event ignored: %s: %s",
                        type(error).__name__, error)


_v2g_ev_stop_callback.wants_meta = True


def _charging_state_ev_stop_callback(topic, payload_value, meta=None):
    """Use fresh vSECC shutdown-stage transitions as the protocol fallback."""
    try:
        if (meta or {}).get("retained"):
            return
        parts = str(topic or "").split("/")
        if (len(parts) != 5 or parts[:2] != ["vsecc", "connector"] or
                parts[3:] != ["status", "charging_session_state"]):
            return
        connector_id = int(parts[2])
        if connector_id not in (1, 2):
            return
        state = str(payload_value or "").strip()
        normalized = "".join(ch for ch in state.lower() if ch.isalnum())
        if "weldingdetection" in normalized or "sessionstop" in normalized:
            _enqueue_ev_stop_event(connector_id, "charging_state", state)
    except Exception as error:
        logging.warning("[EV-STOP] invalid charging-state event ignored: %s: %s",
                        type(error).__name__, error)


_charging_state_ev_stop_callback.wants_meta = True

# ============================================================
# Vector fault source created in the Node-RED Vector flow.
# This is intentionally NOT the Phoenix 45-position fault vector.
# ============================================================
def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return bool(default)
    return str(value).strip().lower() in ("1", "true", "yes", "on")

VECTOR_FAULT_BRIDGE_ENABLED = _env_bool("VECTOR_FAULT_BRIDGE_ENABLED", True)
# The Vector/Node-RED fault collector publishes on the local plant broker, which is a
# different host from the OCPP broker above - and is the very host helpf/plc_function.py
# already holds a connection to. These topics therefore ride on ITS client (see the
# registration block further down), so there is no host/port/credential pair here:
# MQTTConfig owns all four (MQTT_HOST / MQTT_PORT / MQTT_USER / MQTT_PASS), and moving
# the plant broker now moves the fault path with it instead of silently splitting it.
VECTOR_FAULT_EVENT_TOPIC = os.getenv(
    "VECTOR_FAULT_EVENT_TOPIC", "BPC_200kW/fault/event"
).strip()
VECTOR_FAULT_STATUS_TOPIC = os.getenv(
    "VECTOR_FAULT_STATUS_TOPIC", "BPC_200kW/fault/status"
).strip()
VECTOR_FAULT_SEND_OPTIONAL_ALARMS = _env_bool(
    "VECTOR_FAULT_SEND_OPTIONAL_ALARMS", False
)
VECTOR_FAULT_STATUS_MAX_AGE_SECONDS = float(
    os.getenv("VECTOR_FAULT_STATUS_MAX_AGE_SECONDS", "120")
)
# The collector's code for the emergency-stop button. Seeing it raised is what
# triggers "close the transactions before announcing Faulted".
EMERGENCY_STOP_FAULT_CODE = os.getenv(
    "EMERGENCY_STOP_FAULT_CODE", "SYS_EMERGENCY_STOP"
).strip()
# [ABORT-STOP] How long _abort_session_immediately() waits for the orphan
# StopTransaction before giving up. It runs inside main_run_program, and
# _call_txn_message_with_retry can burn attempts x the response timeout plus the
# retry sleeps (minutes, doubled when the connectorId payload draws a CALLERROR).
# Blocking the loop that long would stall the OTHER connector's status polling,
# so the send is cancelled at this cap - which also releases ocpp's _call_lock.
# The StopTransaction is already on the wire by then; only the .conf is given up.
try:
    ABORT_STOP_WAIT_SECONDS = max(
        0.0, float(os.getenv("ABORT_STOP_WAIT_SECONDS", "10"))
    )
except Exception:
    ABORT_STOP_WAIT_SECONDS = 10.0

# [RFID-STOP-INVALID] Only the idTag that opened the transaction may stop it.
# A different card, or a session whose owner cannot be identified, is rejected
# locally as Invalid and never reaches the stop sequence.
# The RFID value is a LEVEL, not an edge: plc.get_rfid() returns the retained topic
# and topic/RFIDCommand can be republished, so one physical tap is seen many times.
# Re-processing every one of those would spam Invalid logs, so a tag is only
# evaluated again after this cooldown.
try:
    RFID_STOP_AUTH_COOLDOWN_SECONDS = max(
        1.0, float(os.getenv("RFID_STOP_AUTH_COOLDOWN_SECONDS", "15"))
    )
except Exception:
    RFID_STOP_AUTH_COOLDOWN_SECONDS = 15.0

# [RFID-STOP-INVALID] Watching the panel directly, so a foreign card never has to get
# past the HMI's own same-card filter.
#   HMI_TOUCH_TOPIC  every DWIN touch, as {"vp":"0x8100","vp_raw":33024,"value":N}
#   RFID_UID_TOPIC   the card reader's bare UID, retained, cleared to "0" on release
# Both live on the PLC/Vector broker, so both are read over plc_function's client.
# The operator presses the head's Stop button and then taps: the button says WHICH
# head and that a stop is meant, the reader says which card. That is everything
# topic/RFIDCommand would have carried.
HMI_TOUCH_TOPIC = os.getenv("HMI_TOUCH_TOPIC", "RFID").strip()
RFID_UID_TOPIC = os.getenv("RFID_UID_TOPIC", "topic/RFID0").strip()
# DWIN VP address -> connector, for the two Stop buttons and the two Start buttons.
HMI_VP_STOP = {0x8000: 1, 0x8100: 2}
HMI_VP_START = {0x6100: 1, 0x6300: 2}
# How long a Stop press stays armed waiting for the card. The HMI itself works to a
# 30s window, so match it rather than inventing a different one.
try:
    RFID_STOP_SELECT_TTL_SECONDS = max(
        1.0, float(os.getenv("RFID_STOP_SELECT_TTL_SECONDS", "30"))
    )
except Exception:
    RFID_STOP_SELECT_TTL_SECONDS = 30.0

# [START-LATCH] R3.1.6. A RemoteStart sent to the PLC that never reached StartTransaction
# keeps its head for at most this long while the PLC shows no EV session (Available or
# Preparing); the next start request then releases it instead of being refused. Flags
# whose session has provably ended are released at once, without waiting.
try:
    REMOTE_START_LATCH_TIMEOUT_SECONDS = max(
        30.0, float(os.getenv("REMOTE_START_LATCH_TIMEOUT_SECONDS", "180"))
    )
except Exception:
    REMOTE_START_LATCH_TIMEOUT_SECONDS = 180.0

# [PREPARING-TIMEOUT] Plugged in, Preparing sent, but no RemoteStart arrived
# before the controller's own timeout ended the session.  Reported as Faulted
# (not Finishing) until the cable is removed.  The timeout itself is set on the
# controller; CP.py only reacts to the controller ending the session.
PREPARING_TIMEOUT_ERROR_CODE = "OtherError"
PREPARING_TIMEOUT_INFO = "PREPARING TIMEOUT"
PREPARING_TIMEOUT_VENDOR_ERROR_CODE = "PREPARING_TIMEOUT"

VECTOR_FAULT_VENDOR_ID = os.getenv(
    "VECTOR_FAULT_VENDOR_ID", "EgatDimomdService"
).strip() or "EgatDimomdService"


class OrderedVectorFaultInbox(VectorFaultInbox):
    """Serve Vector fault events to the bridge in connectorId order 0, 1, 2.

    One press of the emergency stop makes the fault collector publish several
    events back to back, and MQTT arrival order is not connector order (the log
    showed C0, C2, C1).  The bridge sends each event the moment it takes it out of
    the inbox, so the ordering has to happen here, on the way out.

    Only a run of consecutive events of the SAME kind is reordered: a raised run is
    sorted among itself and a cleared run among itself, and anything else (a status
    snapshot, an unparsable payload, a change of kind) ends the run.  So a clear can
    never overtake a raise and the sequence the collector produced is preserved.

    A cleared event carries only the fault code, not a connectorId, so the codes seen
    on raised events and in status snapshots are remembered here and used to place the
    matching clear.  An unknown code is simply served in place, never reordered.

    Everything else - put(), the retry re-queue, latest() - behaves as before.
    """

    def __init__(self, *args, event_topic="", status_topic="",
                 settle_seconds=0.4, max_hold_seconds=2.0, **kwargs):
        super().__init__(*args, **kwargs)
        self._event_topic = str(event_topic)
        self._status_topic = str(status_topic)
        self._ordered_buffer = []
        self._pending = []
        self._code_connector = {}
        # The collector does not publish one press as a single burst - on this unit
        # the events trickle in 70..270 ms apart, while the bridge polls every 200 ms
        # and sends whatever it finds.  Sorting only what happens to share a poll is
        # not enough, so hold arrivals until the group goes quiet for settle_seconds
        # (max_hold_seconds is a backstop so a continuous trickle still drains).
        self._settle_seconds = max(0.0, float(settle_seconds))
        self._max_hold_seconds = max(self._settle_seconds, float(max_hold_seconds))

    # -- VectorFaultInbox interface -----------------------------------------
    def get_nowait(self):
        if not self._ordered_buffer:
            self._collect_pending()
            if not self._pending or not self._pending_settled():
                # Nothing ready yet.  drain_once() just ends this pass and run()
                # comes back in 200 ms, so the event loop is never blocked.
                raise queue.Empty
            self._fill_ordered_buffer()
        return self._ordered_buffer.pop(0)

    def empty(self):
        if self._ordered_buffer or self._pending:
            return False
        return super().empty()

    def qsize(self):
        return len(self._ordered_buffer) + len(self._pending) + super().qsize()

    # -- hold until the group is complete -------------------------------------
    def _collect_pending(self):
        while True:
            try:
                self._pending.append(super().get_nowait())
            except queue.Empty:
                return

    def _pending_settled(self):
        if self._settle_seconds <= 0.0:
            return True
        newest = 0.0
        oldest = None
        for item in self._pending:
            stamp = float(getattr(item, "received_at", 0.0) or 0.0)
            if stamp > newest:
                newest = stamp
            if oldest is None or stamp < oldest:
                oldest = stamp
        now = time.time()
        if oldest is not None and (now - oldest) >= self._max_hold_seconds:
            return True
        return (now - newest) >= self._settle_seconds

    # -- ordering ------------------------------------------------------------
    def _fill_ordered_buffer(self):
        items = self._pending
        self._pending = []

        # Key every item first; this also teaches _code_connector from any snapshot.
        keyed = []
        has_events = False
        for item in items:
            kind, connector_id = self._fault_event_key(item)
            keyed.append((item, kind, connector_id))
            if connector_id is not None:
                has_events = True

        ordered = []
        run = []
        run_kind = None

        def flush():
            # Stable sort: several faults on one connector keep their own order.
            run.sort(key=lambda pair: pair[0])
            for _cid, entry in run:
                ordered.append(entry)
            del run[:]

        for item, kind, connector_id in keyed:
            if connector_id is None:
                if has_events and getattr(item, "topic", None) == self._status_topic:
                    # The collector interleaves its periodic status snapshot with the
                    # individual events. Letting it through here is harmful twice over:
                    # it splits the burst so each half is sorted only among itself
                    # (that is what produced C0, C2, C1), and applying it around a
                    # clear can resurrect a fault that was just cleared. The events in
                    # this batch are the precise, current signal; the snapshot is a
                    # reconciliation that the collector repeats, so skip this one.
                    continue
                if run:
                    flush()
                    run_kind = None
                ordered.append(item)
                continue
            if run and kind != run_kind:
                flush()
            run.append((connector_id, item))
            run_kind = kind

        if run:
            flush()
        self._ordered_buffer = ordered

    def _fault_event_key(self, item):
        """Return ("raised"|"cleared", connectorId), or (None, None) if not sortable."""
        topic = getattr(item, "topic", None)
        if topic == self._status_topic:
            self._learn_status_codes(self._payload_object(item))
            return None, None
        if topic != self._event_topic:
            return None, None

        payload = self._payload_object(item)
        if payload is None:
            return None, None

        code = str(payload.get("code") or "").strip()
        event = str(payload.get("event") or "").strip().lower()
        if not code:
            return None, None

        if event == "raised":
            connector_id = self._meta_connector_id(payload.get("ocpp_1_6j"))
            if connector_id is None:
                return None, None
            self._code_connector[code] = connector_id
            return "raised", connector_id

        if event == "cleared":
            # No connectorId in a clear envelope - use the one seen when it was raised.
            return "cleared", self._code_connector.get(code)

        return None, None

    def _learn_status_codes(self, payload):
        """Remember code -> connectorId from a retained status snapshot."""
        if not isinstance(payload, dict):
            return
        issues = payload.get("issues")
        if not isinstance(issues, list):
            return
        for issue in issues:
            if not isinstance(issue, dict):
                continue
            code = str(issue.get("code") or "").strip()
            connector_id = self._meta_connector_id(issue.get("ocpp_1_6j"))
            if code and connector_id is not None:
                self._code_connector[code] = connector_id

    @staticmethod
    def _meta_connector_id(meta):
        if not isinstance(meta, dict):
            return None
        try:
            connector_id = int(meta.get("connectorId"))
        except (TypeError, ValueError):
            return None
        return connector_id if connector_id in (0, 1, 2) else None

    @staticmethod
    def _payload_object(item):
        payload = getattr(item, "payload", None)
        if isinstance(payload, (bytes, bytearray)):
            payload = payload.decode("utf-8", errors="replace")
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except (ValueError, TypeError):
                return None
        return payload if isinstance(payload, dict) else None



class _DropVectorRaisedWarning(logging.Filter):
    """Silence the bridge's one-WARNING-per-raised-fault line.

    helpf/vector_fault_ocpp.py logs "[VECTOR-FAULT] OCPP raised connector=..." for
    every fault, which just repeats the StatusNotification the ocpp logger already
    prints. That file is not ours to edit, so drop the record here instead.
    Its other warnings (malformed payload, dropped after retries, inbox full) are
    real diagnostics and still get through.
    """

    def filter(self, record):
        return not str(record.msg).startswith("[VECTOR-FAULT] OCPP raised")


# Installed once at import: a new ChargePoint is built on every reconnect, and
# attaching the filter there would stack a duplicate on each one.
logging.getLogger("VECTOR_FAULT_OCPP").addFilter(_DropVectorRaisedWarning())

VECTOR_FAULT_INBOX = OrderedVectorFaultInbox(
    maxsize=2000,
    event_topic=VECTOR_FAULT_EVENT_TOPIC,
    status_topic=VECTOR_FAULT_STATUS_TOPIC,
)

client = mqtt.Client()

def on_connect(client, userdata, flags, rc):
    print(f"[MQTT-OCPP] Connected with result code {rc}")

def on_message(client, userdata, msg):
    try:
        # This MQTT client publishes heartbeat/meter data and has no subscribed
        # application topics. Vector fault topics live on the plant broker and are
        # served by plc_function's client, not by this one.
        payload_str = msg.payload.decode("utf-8", errors="replace")
        logging.debug("[MQTT-OCPP] ignored topic=%s payload=%s", msg.topic, payload_str)
    except Exception as e:
        logging.exception("[MQTT-OCPP] on_message error: %s", e)

client.on_connect = on_connect
client.on_message = on_message
client.connect(OCPP_BROKER, OCPP_PORT, 60)
client.loop_start()

# ============================================================
# [RFID-STOP-INVALID] Handling a card that did not start the session.
# The owner's card keeps the existing local fast path. A different card is handed
# to _rfid_stop_with_authorize() (legacy name), which rejects it locally as Invalid
# and never stops the active transaction.
#
# Two reader paths can raise that check:
#
#   1. topic/RFIDCommand (the HMI tap, and the path the plant actually uses).
#      helpf/plc_function.handle_rfid_command() already listens here and owns the
#      SAME-card stop; it refuses a different card and returns without touching any
#      state, so the callback below can take that case without the two ever
#      colliding. register_topic_callback() appends, so registering here does not
#      displace it.
#   2. The physical reader polled in _handle_rfid_auth_c1/_c2, which calls the same
#      ChargePoint helper directly.
#
# The callback runs on the paho network thread, so it only parses and filters, then
# hands the work to the asyncio loop. Anything blocking here would stall MQTT
# delivery for every topic, PLC state included.
# ============================================================
_RFID_STOP_CP = None
_RFID_STOP_LOOP = None


def _rfid_stop_command_cb(topic, payload_value):
    """topic/RFIDCommand -> reject a foreign stop card as Invalid."""
    cp = _RFID_STOP_CP
    loop = _RFID_STOP_LOOP
    if cp is None or loop is None or loop.is_closed():
        return
    try:
        if isinstance(payload_value, dict):
            data = payload_value
        else:
            data = json.loads(payload_value if isinstance(payload_value, str)
                              else str(payload_value))
        if not isinstance(data, dict):
            return
        if str(data.get("action", "")).strip().lower() != "stop":
            return
        head = int(data.get("head", 0))
        id_tag = str(data.get("idTag", "")).strip()
    except Exception:
        return                      # malformed payloads are plc_function's to log

    if head not in (1, 2) or not id_tag:
        return
    # The same card is handled by plc_function's own fast path, which needs no
    # Authorize (OCPP 1.6 does not ask for one) and still works while offline.
    if id_tag == str(getattr(cp, f"accepted_tag{head}", "0") or "0").strip():
        return

    try:
        asyncio.run_coroutine_threadsafe(
            cp._rfid_stop_with_authorize(head, id_tag), loop)
    except Exception as e:
        logging.error("[RFID-STOP-INVALID] C%s could not dispatch idTag=%s: %s",
                      head, id_tag, e)


try:
    plc.on_topic(RFID_COMMAND_TOPIC, _rfid_stop_command_cb)
    logging.info("[RFID-STOP-INVALID] watching %s for foreign-card attempts",
                 RFID_COMMAND_TOPIC)
except Exception as e:
    logging.error("[RFID-STOP-INVALID] cannot register %s callback: %s",
                  RFID_COMMAND_TOPIC, e)


# ============================================================
# [RFID-STOP-INVALID] Direct panel watch.
#
# topic/RFIDCommand above only carries taps the HMI decided to forward, and the HMI
# applies its own same-card rule first - a card that did not start the session is
# dropped there and never reaches this process, so the local Invalid rejection
# could never be recorded.
# Reading the two raw signals instead makes the decision ours:
#
#   HMI_TOUCH_TOPIC ("RFID")      the Stop button press -> which connector
#   RFID_UID_TOPIC ("topic/RFID0") the card reader UID  -> which card
#
# The operator presses the head's Stop button and then taps, exactly as before; only
# the arbitration moves here. The owner's own card is still left to the existing
# path, so nothing about a normal stop changes.
# ============================================================
_RFID_STOP_SELECT = {"head": 0, "expires": 0.0}


def _note_hmi_touch(payload):
    """A DWIN touch arrived on the panel topic. Arm or cancel a pending stop."""
    try:
        data = payload if isinstance(payload, dict) else json.loads(
            payload.decode() if isinstance(payload, (bytes, bytearray)) else str(payload))
        vp = int(data.get("vp_raw"))
    except Exception:
        return                       # anything else on this topic is not ours

    head = HMI_VP_STOP.get(vp)
    if head:
        _RFID_STOP_SELECT["head"] = head
        _RFID_STOP_SELECT["expires"] = time.monotonic() + RFID_STOP_SELECT_TTL_SECONDS
        logging.info("[RFID-STOP-INVALID] C%s stop button pressed; waiting %ss for a card",
                     head, RFID_STOP_SELECT_TTL_SECONDS)
    elif vp in HMI_VP_START:
        # Selecting start cancels a stop the operator did not follow through on.
        _RFID_STOP_SELECT["head"] = 0


def _rfid_uid_cb(topic, payload_value):
    """A card was presented. Only act when a Stop press is still armed for a head.

    Runs on the paho network thread: parse, filter, hand off. Requiring the button
    press first is what makes this unambiguous - one reader serves both connectors,
    so a bare UID on its own could not say which session was meant.
    """
    cp = _RFID_STOP_CP
    loop = _RFID_STOP_LOOP
    if cp is None or loop is None or loop.is_closed():
        return

    head = int(_RFID_STOP_SELECT.get("head") or 0)
    if head not in (1, 2):
        return
    if time.monotonic() > float(_RFID_STOP_SELECT.get("expires") or 0.0):
        _RFID_STOP_SELECT["head"] = 0
        return

    try:
        id_tag = (payload_value.decode() if isinstance(payload_value, (bytes, bytearray))
                  else str(payload_value)).strip()
    except Exception:
        return
    # The reader publishes a retained level and clears it to "0" once the card is
    # taken away, so most callbacks here carry nothing worth acting on.
    if not id_tag or id_tag in ("0", "None", "null"):
        return
    # The owner's own card: the HMI forwards that as topic/RFIDCommand and
    # plc_function stops it without an Authorize. Leave it alone.
    if id_tag == str(getattr(cp, f"accepted_tag{head}", "0") or "0").strip():
        return

    _RFID_STOP_SELECT["head"] = 0        # one press, one card
    try:
        asyncio.run_coroutine_threadsafe(
            cp._rfid_stop_with_authorize(head, id_tag), loop)
    except Exception as e:
        logging.error("[RFID-STOP-INVALID] C%s could not dispatch idTag=%s: %s",
                      head, id_tag, e)


try:
    # topic/RFID0 matches plc_function's existing "topic/#" subscription, so this only
    # needs a callback. The panel topic is a bare "RFID" that the wildcard misses, so it
    # needs an explicit plc.subscribe() - see the registration block below.
    plc.on_topic(RFID_UID_TOPIC, _rfid_uid_cb)
    logging.info("[RFID-STOP-INVALID] watching %s + %s for foreign-card attempts",
                 HMI_TOUCH_TOPIC, RFID_UID_TOPIC)
except Exception as e:
    logging.error("[RFID-STOP-INVALID] cannot register %s callback: %s",
                  RFID_UID_TOPIC, e)


# ============================================================
# Plant-broker topics that plc_function's wildcards do not cover.
#
# These used to need a paho client of their own (fault_client) to the SAME host with
# the SAME credentials: two TCP sessions, and two copies of the broker settings that
# could drift apart - the host came from VECTOR_FAULT_BROKER here but from MQTT_HOST
# there, so moving the plant broker moved only one of them. They now ride on the
# single session helpf/plc_function already owns, whose _on_connect re-subscribes
# them on every reconnect, so a broker restart still cannot leave this process deaf.
#
# plc subscribes only "vsecc/#" and "topic/#" and these topics match neither, hence
# the explicit plc.subscribe() beside each plc.on_topic().
#
# Both callbacks run on the paho network thread, exactly as the fault client's did,
# so they may only parse and enqueue. Blocking here would stall MQTT delivery for
# every topic, PLC state included.
# ============================================================
def _vector_fault_mqtt_cb(topic, payload_value):
    """BPC_200kW/fault/{event,status} -> the inbox the asyncio bridge drains."""
    try:
        VECTOR_FAULT_INBOX.put(topic, payload_value)
    except Exception as e:
        logging.exception("[VECTOR-FAULT-MQTT] inbox put failed topic=%s: %s", topic, e)


def _hmi_touch_mqtt_cb(topic, payload_value):
    """plc callbacks are (topic, value); _note_hmi_touch takes the payload alone.

    plc_function has already decoded and JSON-parsed it, so this arrives as a dict -
    _note_hmi_touch accepts dict, bytes and str alike.
    """
    _note_hmi_touch(payload_value)


# The HMI Stop button belongs to the RFID-stop feature, not to the fault bridge, so it
# is registered unconditionally. It was only ever tied to VECTOR_FAULT_BRIDGE_ENABLED
# because it happened to share that client.
try:
    # on_topic() BEFORE subscribe(), never the other way round: plc.mqtt is already
    # connected by the time this module reaches here (it connects during the import at
    # the top of this file), so the SUBSCRIBE goes out at once and the broker replays
    # any retained value on the paho thread immediately. Subscribing first would race
    # that replay against the callback registration and lose it.
    plc.on_topic(HMI_TOUCH_TOPIC, _hmi_touch_mqtt_cb)
    plc.subscribe(HMI_TOUCH_TOPIC, qos=0)
    logging.info("[RFID-STOP-INVALID] watching %s on the PLC broker client",
                 HMI_TOUCH_TOPIC)
except Exception as e:
    logging.error("[RFID-STOP-INVALID] cannot subscribe %s: %s", HMI_TOUCH_TOPIC, e)

if VECTOR_FAULT_BRIDGE_ENABLED:
    try:
        for _fault_topic in (VECTOR_FAULT_EVENT_TOPIC, VECTOR_FAULT_STATUS_TOPIC):
            # Callback first here too, and it matters more: fault/status is RETAINED,
            # so the broker replays it the moment the SUBSCRIBE is acked. Losing that
            # snapshot would leave the bridge blind until the collector's next publish.
            plc.on_topic(_fault_topic, _vector_fault_mqtt_cb)
            # qos=1 as the dedicated client used: a raised/cleared event is sent once
            # and is gone if missed - a dropped "cleared" leaves a connector stuck
            # Faulted. plc's own wildcards stay at qos=0, which is enough for values
            # that are republished continuously.
            plc.subscribe(_fault_topic, qos=1)
        logging.info(
            "[VECTOR-FAULT-MQTT] on the PLC broker client; subscribed: %s, %s",
            VECTOR_FAULT_EVENT_TOPIC, VECTOR_FAULT_STATUS_TOPIC,
        )
    except Exception as e:
        logging.exception("[VECTOR-FAULT-MQTT] cannot subscribe fault topics: %s", e)
else:
    logging.info("[VECTOR-FAULT-MQTT] bridge disabled; %s / %s not subscribed",
                 VECTOR_FAULT_EVENT_TOPIC, VECTOR_FAULT_STATUS_TOPIC)

# VECTOR_FAULT_BROKER / VECTOR_FAULT_PORT are gone: the host and port come from
# helpf.plc_function.MQTTConfig. Say so out loud rather than silently ignoring a unit
# file that still sets them - a fault path pointed at the wrong broker is exactly the
# kind of thing that stays invisible until a fault fails to reach the CSMS.
for _retired_env in ("VECTOR_FAULT_BROKER", "VECTOR_FAULT_PORT"):
    if os.getenv(_retired_env):
        logging.warning(
            "[VECTOR-FAULT-MQTT] %s=%r is IGNORED - the fault topics share "
            "plc_function's MQTT session to %s:%s; set MQTT_HOST / MQTT_PORT instead",
            _retired_env, os.getenv(_retired_env),
            plc.mqtt.config.broker, plc.mqtt.config.port,
        )

def line_notify(message):
    # Telegram alert. The bot token and chat id are not kept in the source:
    # set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in the service environment.
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not bot_token or not chat_id:
        return "telegram-not-configured"
    try:
        import requests
        url  = f'https://api.telegram.org/bot{bot_token}/sendMessage'
        data = {'chat_id': chat_id, 'text': message}
        response = requests.post(url, data=data, timeout=5).json()
    except Exception as e:
        response = "telegramerror"
    return response

# ============================================================================
# Watch-Dog
# ============================================================================
async def systemd_watchdog_task():
    while True:
        notifier.notify("WATCHDOG=1")
        await asyncio.sleep(10)

# ============================================================================
# ASYNC WRAPPERS FOR BLOCKING PLC CALLS
# ============================================================================
async def async_run_blocking(func, *args, **kwargs):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, functools.partial(func, *args, **kwargs))

async def async_get_connector_status(plc_interface, ip_addr):
    return await async_run_blocking(plc_interface.get_connector_status, ip_addr)

async def async_get_soc(plc_interface, ip_addr):
    return await async_run_blocking(plc_interface.get_soc, ip_addr)

async def async_get_requested_voltage(plc_interface, ip_addr):
    return await async_run_blocking(plc_interface.get_requested_voltage, ip_addr)

async def async_get_requested_current(plc_interface, ip_addr):
    return await async_run_blocking(plc_interface.get_requested_current, ip_addr)

async def async_get_rfid(plc_interface, ip_addr):
    return await async_run_blocking(plc_interface.get_rfid, ip_addr)

async def async_get_finishing_status(plc_interface, ip_addr):
    return await async_run_blocking(plc_interface.get_finishing_status, ip_addr)

async def async_get_emergency_stop(plc_interface, ip_addr):
    return await async_run_blocking(plc_interface.get_emergency_stop, ip_addr)

async def async_remote_start_plc(ip_addr, value):
    return await async_run_blocking(remote_start_plc, ip_addr, value)

async def async_remote_stop_plc(ip_addr, value):
    return await async_run_blocking(remote_stop_plc, ip_addr, value)

# ============================================================================
# Time helper
# ============================================================================
def convert_timeformat(current_time):
    return current_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + 'Z'

async def edgebox_temp(period=60):
    global LATEST_EDGEBOX_TEMP_C
    while True:
        try:
            t = None
            try:
                temps = psutil.sensors_temperatures(fahrenheit=False) or {}
                for readings in temps.values():
                    if readings and hasattr(readings[0], "current"):
                        t = float(readings[0].current)
                        break
            except Exception:
                pass
            if t is None:
                with open('/sys/class/thermal/thermal_zone0/temp') as f:
                    t = int(f.read().strip()) / 1000.0

            # Publish and MeterValues share the same validated Celsius reading.
            LATEST_EDGEBOX_TEMP_C = float(t)
            data = {
                "edgeboxTemp": LATEST_EDGEBOX_TEMP_C,
                "timestamp": datetime.now(pytz.timezone('Asia/Bangkok')).strftime("%Y-%m-%dT%H:%M:%S.%f")
            }
            payload = json.dumps(data)
        except Exception as e:
            LATEST_EDGEBOX_TEMP_C = None
            print("edgebox_temp error:", e)
        else:
            try:
                client.publish(f"OCPP/{OCPPSTATION}/edgeboxTemp", payload)
                print(data)
            except Exception as e:
                # A broker failure does not invalidate the sensor reading used by
                # MeterValues; retry MQTT publishing on the next sampling cycle.
                print("edgebox_temp publish error:", e)
        finally:
            await asyncio.sleep(period)

# ============================================================================
# Config-file helpers
# ============================================================================
def _read_cfg_text(path: str, default: str) -> str:
    try:
        if not os.path.exists(path):
            with open(path, "w") as f:
                f.write(str(default))
            return str(default)
        with open(path, "r") as f:
            data = str(f.read()).strip()
        if data == "":
            with open(path, "w") as f:
                f.write(str(default))
            return str(default)
        return data
    except Exception:
        return str(default)

def _write_configuration_file(path, value):
    """Commit a setting before publishing its new in-memory value.

    The PLC helper intentionally logs and swallows I/O errors for its older
    callers. OCPP configuration needs a failure signal instead of Accepted for
    a value that could not be persisted.
    """
    import tempfile
    target = os.path.abspath(os.fspath(path))
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", delete=False,
                                         dir=os.path.dirname(target),
                                         prefix="." + os.path.basename(target) + ".") as stream:
            temporary = stream.name
            stream.write(str(value))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, target)
        temporary = None
    finally:
        if temporary is not None:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass

def _read_safe_integer_cfg(path, default, minimum=0):
    raw = _read_cfg_text(path, str(default))
    try:
        if not str(raw).strip().lstrip("+").isdecimal():
            raise ValueError("Expected a nonnegative integer")
        value = int(raw)
        if not minimum <= value <= 2147483647:
            raise ValueError("Configuration outside bounds")
        return str(value)
    except (ValueError, TypeError):
        _write_configuration_file(path, str(default))
        logging.warning("Repaired invalid integer configuration in %s", os.path.basename(path))
        return str(default)


def _parse_csv_list(value) -> list:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return [x for x in (str(x).strip() for x in value) if x]
    s = str(value).strip()
    if s == "" or s.lower() in ("none", "null"):
        return []
    if (s.startswith("[") and s.endswith("]")) or (s.startswith("(") and s.endswith(")")):
        try:
            parsed = ast.literal_eval(s)
            if isinstance(parsed, (list, tuple)):
                return [x for x in (str(x).strip() for x in parsed) if x]
        except Exception:
            pass
    return [x for x in (x.strip() for x in s.split(",")) if x]

def _normalize_csv_string(value, default: str = "0") -> str:
    items = _parse_csv_list(value)
    if not items:
        return str(default)
    if len(items) == 1 and items[0] == "0":
        return "0"
    return ",".join(items)

def _normalize_bool_string(value, default: str = "true") -> str:
    """Return an OCPP configuration boolean as canonical lowercase text."""
    normalized = str(value).strip().lower()
    if normalized in ("true", "false"):
        return normalized
    return "true" if str(default).strip().lower() == "true" else "false"

# ============================================================================
# Logging setup
# ============================================================================
log_name = datetime.now(pytz.timezone('Asia/Bangkok')).strftime("%Y%m%d_%H%M%S")
logging.basicConfig(level=logging.INFO)
file_handler = logging.FileHandler(log_name + '.log')
file_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger = logging.getLogger()
logger.addHandler(file_handler)

# Register once per process. Retained EVCCIDs that arrived before registration
# are seeded from the MQTT cache when a ChargePoint instance is created.
for _autocharge_head in ("1", "2"):
    try:
        plc.on_topic(
            AUTOCHARGE_EVCCID_TOPIC.format(connector=_autocharge_head),
            _autocharge_evccid_callback,
        )
    except Exception as e:
        logging.warning(
            f"[AUTOCHARGE] cannot register EVCCID callback for C{_autocharge_head}: {e}"
        )

# Register EV-stop observers once per process.  The charging-state topics are
# already covered by the shared vsecc/# subscription; V2G/TestEgat is outside
# those wildcards and therefore needs one explicit reconnect-safe subscription.
try:
    plc.on_topic(EV_STOP_V2G_TOPIC, _v2g_ev_stop_callback)
    plc.subscribe(EV_STOP_V2G_TOPIC, qos=1)
    for _ev_stop_head in ("1", "2"):
        plc.on_topic(
            EV_STOP_STATE_TOPIC.format(connector=_ev_stop_head),
            _charging_state_ev_stop_callback,
        )
    logging.info("[EV-STOP] watching live V2G and charging-state shutdown events")
except Exception as e:
    logging.error("[EV-STOP] cannot register shutdown observers: %s", e)

# ============================================================================
# Start-up: read PLC state
# ============================================================================
ip_adr1, ip_adr2   = '1', '2'
ip_Meter1, ip_Meter2 = '/dev/ttyUSB0', '/dev/ttyACM0'


def _read_startup_meter_wh(connector_id, device):
    """Require a valid cumulative reading before this station can boot OCPP."""
    try:
        reading = get_meter_value_pilot(device)
        if not isinstance(reading, dict):
            raise ValueError("meter response is not a dictionary")
        return energy_wh(reading['energyTotal'])
    except Exception:
        logging.exception("[METER-STARTUP] C%s device=%s read failed", connector_id, device)
        raise

plc_inst = plc
icp_ini1, usi_ini1 = plc_inst.get_connector_status(ip_adr1)
icp_ini2, usi_ini2 = plc_inst.get_connector_status(ip_adr2)

# ============================================================================
# OCPP 1.6 — Local Authorization / Authorization Cache
# ============================================================================
class AuthorizationCache:
    def __init__(self, size: int = 50):
        self.size = int(size)
        self.cache = {}

    def add(self, id_tag: str, id_tag_info: dict):
        if len(self.cache) >= self.size:
            try:
                del self.cache[next(iter(self.cache))]
            except Exception:
                self.cache = {}
        self.cache[str(id_tag)] = {
            "idTagInfo": id_tag_info,
            "timestamp": convert_timeformat(datetime.now(timezone.utc).replace(tzinfo=timezone.utc)),
        }

    def get(self, id_tag: str):
        return self.cache.get(str(id_tag))

    def clear(self):
        self.cache = {}

class LocalAuthorizationList:
    def __init__(self, max_length: int = 1000,
                 storage_path: str = "local_auth_list.json",
                 version_path: str = "local_auth_list_version.txt"):
        self.max_length   = int(max_length)
        self.storage_path = storage_path
        self.version_path = version_path
        # One atomically replaced record owns both list and version. Keep the
        # old two files as mirrors for tooling and migration/rollback; they are
        # never used over a valid snapshot by this version of the controller.
        self.snapshot_path = os.fspath(storage_path) + ".snapshot"
        self.identifiers  = {}
        self.list_version = 0
        self._load_from_disk()

    def _load_from_disk(self):
        if os.path.exists(self.snapshot_path):
            try:
                with open(self.snapshot_path, "r", encoding="utf-8") as stream:
                    snapshot = json.load(stream)
                version = snapshot["list_version"]
                identifiers = snapshot["identifiers"]
                if (snapshot.get("format") != "ocpp-local-auth-list-v1"
                        or type(version) is not int or version < 0
                        or not isinstance(identifiers, dict)):
                    raise ValueError("Invalid local authorization snapshot")
                self.list_version = version
                self.identifiers = identifiers
            except Exception:
                # Do not resurrect an older mirrored authorization list when
                # its authoritative record is corrupt or unreadable.
                self.list_version = 0
                self.identifiers = {}
                logging.error("Local authorization snapshot could not be loaded")
            return
        try:
            if os.path.exists(self.version_path):
                with open(self.version_path, "r") as f:
                    self.list_version = int(str(f.read()).strip() or "0")
        except Exception:
            self.list_version = 0
        try:
            if os.path.exists(self.storage_path):
                with open(self.storage_path, "r") as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    self.identifiers = data
        except Exception:
            self.identifiers = {}

    @staticmethod
    def _atomic_write(path, text):
        import tempfile
        target = os.path.abspath(os.fspath(path))
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", delete=False,
                                             dir=os.path.dirname(target),
                                             prefix="." + os.path.basename(target) + ".") as stream:
                temporary = stream.name
                stream.write(text)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, target)
            temporary = None
        finally:
            if temporary is not None:
                try:
                    os.unlink(temporary)
                except FileNotFoundError:
                    pass

    def _persist(self, identifiers, list_version):
        snapshot = {"format": "ocpp-local-auth-list-v1", "list_version": list_version,
                    "identifiers": identifiers}
        # Serialize before touching files. A serialization/replace failure leaves
        # the previous canonical record, both mirrors and in-memory state alone.
        serialized = json.dumps(snapshot, ensure_ascii=False, allow_nan=False)
        self._atomic_write(self.snapshot_path, serialized)
        # These compatibility mirrors are not a second commit point. A mirror
        # failure cannot undo an already committed authoritative snapshot.
        for path, text in ((self.storage_path, json.dumps(identifiers, ensure_ascii=False)),
                           (self.version_path, str(list_version))):
            try:
                self._atomic_write(path, text)
            except Exception:
                logging.warning("Local authorization compatibility mirror could not be updated")

    def _save_to_disk(self):
        self._persist(self.identifiers, int(self.list_version))

    def clear(self):
        self._persist({}, int(self.list_version))
        self.identifiers = {}

    def replace_list_full(self, new_entries: dict):
        if len(new_entries) > self.max_length:
            raise ValueError("LocalAuthList exceeds max_length")
        self.identifiers = new_entries

    def upsert(self, id_tag: str, id_tag_info: dict):
        if (id_tag not in self.identifiers) and (len(self.identifiers) >= self.max_length):
            raise ValueError("LocalAuthList exceeds max_length")
        self.identifiers[str(id_tag)] = id_tag_info

    def delete(self, id_tag: str):
        self.identifiers.pop(str(id_tag), None)

    def apply_send_local_list(self, list_version, update_type, local_authorization_list):
        if isinstance(list_version, bool) or not isinstance(list_version, (int, str)):
            return "Failed"
        try:
            lv = int(list_version)
        except Exception:
            return "Failed"
        if not 0 <= lv <= 2147483647:
            return "Failed"
        ut = str(getattr(update_type, "value", update_type))
        if ut not in ("Full", "Differential"):
            return "Failed"
        if ut == "Differential" and lv <= int(self.list_version):
            return "VersionMismatch"
        if local_authorization_list is None:
            local_authorization_list = []
        if not isinstance(local_authorization_list, (list, tuple)):
            return "Failed"
        if lv == 0 and local_authorization_list:
            return "Failed"

        def getv(obj, camel, snake):
            if obj is None:
                return None
            if isinstance(obj, dict):
                return obj.get(snake) if snake in obj else obj.get(camel)
            return getattr(obj, snake, getattr(obj, camel, None))

        # Work on a deep candidate. In particular a capacity error late in a
        # Differential request must not partially change an existing card.
        try:
            candidate = {} if ut == "Full" else json.loads(json.dumps(self.identifiers))
            seen_tags = set()
            for item in local_authorization_list:
                id_tag      = getv(item, "idTag", "id_tag")
                id_tag_info = getv(item, "idTagInfo", "id_tag_info")
                if not isinstance(id_tag, str) or not id_tag.strip() or len(id_tag) > 20:
                    return "Failed"
                if id_tag in seen_tags:
                    return "Failed"
                seen_tags.add(id_tag)
                if id_tag_info is None:
                    if ut == "Full":
                        return "Failed"
                    candidate.pop(id_tag, None)
                    continue
                if not isinstance(id_tag_info, dict):
                    try:
                        id_tag_info = dict(id_tag_info)
                    except Exception:
                        id_tag_info = vars(id_tag_info)
                info = json.loads(json.dumps(id_tag_info, allow_nan=False))
                status = str(getattr(id_tag_info.get("status"), "value", id_tag_info.get("status")))
                if status not in ("Accepted", "Blocked", "Expired", "Invalid", "ConcurrentTx"):
                    return "Failed"
                info["status"] = status
                expiry = getv(info, "expiryDate", "expiry_date")
                if expiry is not None:
                    if not isinstance(expiry, str):
                        return "Failed"
                    parsed = datetime.fromisoformat(expiry.replace("Z", "+00:00"))
                    if parsed.tzinfo is None:
                        return "Failed"
                parent = getv(info, "parentIdTag", "parent_id_tag")
                if parent is not None and (not isinstance(parent, str) or not parent or len(parent) > 20):
                    return "Failed"
                candidate[id_tag] = info
            if len(candidate) > self.max_length:
                return "Failed"
            self._persist(candidate, lv)
        except Exception:
            logging.error("Local authorization update could not be committed")
            return "Failed"
        self.identifiers = candidate
        self.list_version = lv
        return "Accepted"

# ============================================================================
# Charger hardware ratings  ── SINGLE SOURCE OF TRUTH ──
# เปลี่ยนรุ่นตู้/สเปกฮาร์ดแวร์ ให้แก้แค่ 2 ค่านี้ที่เดียว
# ============================================================================
RATED_POWER_W   = 180_000   # กำลังไฟสูงสุดที่ตู้จ่ายได้จริง (120 kW)
RATED_CURRENT_A = 500       # กระแส DC สูงสุดของหัวชาร์จ/สาย (A) — ตั้งตาม rating จริงของ connector

# [RFID-ORDER-V11] RFID start sequence is strictly:
# Authorize accepted -> wait for real Charging -> StatusNotification(Charging)
# -> StartTransaction -> MeterValues. No pre-transaction SuspendedEV is sent.
#
# [START-ORDER-V14] ลำดับข้างบนถูกขยายให้ใช้กับ RemoteStart ด้วย (เดิมบังคับเฉพาะ
# RFID) RemoteStart จึงไม่เปิด transaction ทันทีที่ CSMS สั่ง แต่รอจนรถดึงกระแส
# จริงก่อน ทำให้ไม่มี SuspendedEV โผล่ระหว่าง handshake/precharge
# ผลข้างเคียงที่ยอมรับแล้ว: StartTransaction จะตามหลัง RemoteStartTransaction.conf
# ราว 10-20 วิ (เท่าที่รถใช้เตรียมตัว) และถ้ารถไม่ดึงไฟเลย transaction จะไม่ถูก
# เปิด — พฤติกรรมเดียวกับเส้นทาง RFID
#
# [METER-GATE-V12] MeterValues จะเริ่มส่งเมื่อ transaction ปัจจุบัน "เห็นพลังงาน
# ไหลจริง" ครั้งแรกแล้วเท่านั้น (current/power เกิน threshold) ตู้จะจ่ายไฟเข้ารถ
# หลัง authorize เสมอ ดังนั้นช่วงที่ controller ไล่ sequence เอง (insulation
# test / precharge, power = 0 kW, ยังไม่ authorize เช่น txn ที่ resume หลัง
# reconnect) จะไม่มี MeterValues ออกไปหา CSMS; เมื่อ authorize แล้ว power ขึ้น
# จริงจึงเริ่มส่ง และส่งต่อเนื่องจนจบ txn แม้ EV พักกลางคัน (SuspendedEV)
# latch นี้ตั้งใจไม่ persist ลงไฟล์ — process ใหม่ต้องยืนยันพลังงานใหม่เสมอ
#
# [PHYSICAL-STATUS-V10]
# Meter รุ่นนี้มี zero-offset ประมาณ 0.15 kW. ห้ามใช้ abs(raw - offset)
# เพราะ raw=0 จะกลายเป็น 0.15 kW แบบผิด ๆ; ต้องหักจากค่าสัมบูรณ์แล้ว clamp ที่ 0.
METER_POWER_OFFSET_KW = 0.15
ENERGY_FLOW_CURRENT_THRESHOLD_A = 1.0
ENERGY_FLOW_POWER_THRESHOLD_KW = 0.5
ENERGY_STATE_SAMPLE_INTERVAL_S = 1.5
ENERGY_STATE_STALE_AFTER_S = 5.0
ENERGY_SUSPEND_CONFIRM_SECONDS = 3.0

# ============================================================================
# ChargePoint
# ============================================================================
class ChargePoint(OCPPProtocolMixin, cp):
    async def call(self, payload, *args, **kwargs):
        runtime = getattr(self, "_protocol_runtime", None)
        if runtime is not None:
            if self._ocpp_hides_service_status(payload):
                return call_result.StatusNotification()
            payload = runtime.filter_status(payload)
        # Observe successful calls only; preserve the original result/exception.
        if self._dual_manager is not None:
            response = await self._dual_manager.call(payload, *args, **kwargs)
        else:
            response = await super().call(payload, *args, **kwargs)
            if (runtime is not None and type(payload).__name__ == "StartTransaction"
                    and response.id_tag_info.get("status") == "Accepted"):
                await runtime.transaction_started(payload.connector_id, runtime.local_owner,
                                                  payload.id_tag, response.transaction_id,
                                                  started_at=payload.timestamp)
        try:
            if type(payload).__name__ == "BootNotification":
                self._v2g_boot_accepted = getattr(response, "status", None) == "Accepted"
            bridge = getattr(self, "_v2g_bridge", None)
            if bridge is not None:
                bridge.observe_ocpp(payload, response)
        except Exception:
            logging.exception("[V2G] OCPP observation failed")
        return response

    def __init__(self, identifier, websocket,
                 iCpStateCss1=icp_ini1,
                 iCpStateCss2=icp_ini2,
                 usiLinkStateCcs1=usi_ini1,
                 usiLinkStateCcs2=usi_ini2):
        super().__init__(identifier, websocket, response_timeout=45)
        self._ws = websocket
        self._dual_manager = websocket if getattr(websocket, "is_dual", False) else None
        self._dual_ready = False
        self._dual_aborting = set()
        self._dual_local_claims = set()
        self._v2g_boot_accepted = False
        self._v2g_bridge = get_bridge(plc.mqtt, OCPPSTATION)

        # ---- Connector 1 defaults ----
        self.iCpStateCss1       = iCpStateCss1
        self.myconnector_id_c1  = 1
        self.myip_adr_c1        = '1'
        self.myremotestart_c1   = 0
        self.myid_tag_c1        = "Default"
        self.my_meter_ip_c1     = '/dev/ttyUSB0'
        self.stage1_sent_done_c1 = 0
        self.transactionId_c1   = None
        self.my_remote_stop_c1  = 0
        # [REMOTE-STOP-LATCH] หลังรับ RemoteStop ให้คง Finishing จนกว่าจะถอดสายเป็น Available
        self.remote_stop_finishing_c1 = 0
        # [SESSION-FINISHING] OCPP 1.6: transaction ที่ถูกตัดด้วย Emergency ต้องคง
        # Finishing จนกว่าจะถอดสายจริง — latch เดียวกับ RemoteStop ด้านบน
        self.session_finishing_c1 = 0
        self.emergency_sent_c1  = '1'
        self.emergency_sent_Sw_c1 = 1
        self.emergency_cp0_sent = 0
        self.usiLinkStateCcs1   = usiLinkStateCcs1
        self.myid_tag_ath_c1    = "Default"
        self.finshing_sent_c1   = 0
        self.finshing_sent_sw_c1 = 1
        self.start_trans_SW_c1  = 1
        self.block1             = 0
        self.accepted_tag1      = '0'
        self.accepted_tag2      = '0'
        # [RFID-OCPP-V7] Explicit local-RFID session latch. RemoteStartOnly may
        # remain enabled; a freshly authorized topic/RFIDCommand start is allowed
        # to open a normal OCPP transaction through this separate latch.
        self.rfid_start_received_c1 = 0
        # [LOCAL-STATUS-V16] latch ของ session ที่เริ่มจากหน้าจอ — แยกขาดจาก
        # remote_start_received_cX (OCPP) และ rfid_start_received_cX (บัตร)
        # เพราะทั้ง reason ของ StopTransaction และ branch ตอนจบ session
        # ตัดสินใจจาก latch เหล่านี้คนละแบบกัน
        self.local_status_session_c1 = 0
        self._local_status_consumed_ts_c1 = 0.0
        self._local_status_log_ts_c1 = 0.0
        self._pending_local_stop_ts_c1 = 0.0
        self.active_id_tag_c1       = "Default"
        self._rfid_file_check_ts_c1 = 0.0
        self._rfid_consumed_auth_ts_c1 = 0.0
        # [METER-RECOVERY-V9] Preserve an authorized local start across a
        # CP process/WebSocket restart even if StartTransaction had not been sent yet.
        self.rfid_recovery_pending_c1 = 0
        self._start_state_log_ts_c1 = 0.0
        self._rfid_wait_charging_log_ts_c1 = 0.0
        # [PHYSICAL-STATUS-V10] Track the last OCPP status and a throttled
        # physical-energy snapshot so SuspendedEV <-> Charging transitions are
        # based on real current/power, not only on the PLC control-flow mapper.
        self._last_ocpp_status_c1 = None
        self._energy_state_check_ts_c1 = 0.0
        self._energy_state_success_ts_c1 = 0.0
        self._energy_state_valid_c1 = False
        self._energy_flow_c1 = False
        self._energy_current_c1 = 0.0
        self._energy_power_c1 = 0.0
        self._no_energy_since_c1 = 0.0
        # [METER-GATE-V12] ส่ง MeterValues ได้เมื่อ txn ปัจจุบันเคยเห็นพลังงาน
        # ไหลจริงแล้วเท่านั้น (ไม่ persist — restart แล้วต้องยืนยันใหม่)
        self._txn_energy_confirmed_c1 = False
        self._meter_gate_log_ts_c1 = 0.0
        self._meter_remote_gate_log_ts_c1 = 0.0
        self.start_transaction_SW1     = 1
        self.start_sendMeterValue_SW1  = 1
        self.meter_start1 = _read_startup_meter_wh(1, self.my_meter_ip_c1)
        self.meter_stop1  = self.meter_start1
        self.timestamp1   = datetime.now(pytz.timezone('Asia/Bangkok')).strftime("%Y-%m-%dT%H:%M:%S.%f")

        # ---- Restore connector-1 state from files ----
        self.block1                   = int(open("block1.txt").read()) if os.path.exists("block1.txt") else 0
        self.emergency_sent_c1        = str(open("emergency_sent_c1.txt").read()).strip() if os.path.exists("emergency_sent_c1.txt") else '1'
        self.emergency_sent_Sw_c1     = int(open("emergency_sent_Sw_c1.txt").read()) if os.path.exists("emergency_sent_Sw_c1.txt") else 1
        self.finshing_sent_c1         = int(open("finshing_sent_c1.txt").read()) if os.path.exists("finshing_sent_c1.txt") else 0
        self.finshing_sent_sw_c1      = int(open("finshing_sent_sw_c1.txt").read()) if os.path.exists("finshing_sent_sw_c1.txt") else 1
        self.my_remote_stop_c1        = int(open("my_remote_stop_c1.txt").read()) if os.path.exists("my_remote_stop_c1.txt") else 0
        self.remote_stop_finishing_c1  = int(open("remote_stop_finishing_c1.txt").read()) if os.path.exists("remote_stop_finishing_c1.txt") else 0
        self.session_finishing_c1   = self._read_int_state("session_finishing_c1.txt", 0)
        self.preparing_timeout_c1   = self._read_int_state("preparing_timeout_c1.txt", 0)
        self.myid_tag_ath_c1          = open("myid_tag_ath_c1.txt").read() if os.path.exists("myid_tag_ath_c1.txt") else "Default"
        self.myid_tag_c1              = open("myid_tag_c1.txt").read() if os.path.exists("myid_tag_c1.txt") else "Default"
        self.myremotestart_c1         = int(open("myremotestart_c1.txt").read()) if os.path.exists("myremotestart_c1.txt") else 0
        self.remote_start_received_c1 = int(open("remote_start_received_c1.txt").read()) if os.path.exists("remote_start_received_c1.txt") else 0
        self.local_status_session_c1   = self._read_int_state("local_status_session_c1.txt", 0)   # [LOCAL-STATUS-V16]
        self.start_trans_SW_c1        = int(open("start_trans_SW_c1.txt").read()) if os.path.exists("start_trans_SW_c1.txt") else 1
        self.start_transaction_SW1    = int(open("start_transaction_SW1.txt").read()) if os.path.exists("start_transaction_SW1.txt") else 1
        self.start_sendMeterValue_SW1 = int(open("start_sendMeterValue_SW1.txt").read()) if os.path.exists("start_sendMeterValue_SW1.txt") else 1
        self.iCp1_regis               = str(open("iCpStateCss1.txt").read()) if os.path.exists("iCpStateCss1.txt") else "Available"
        self.transactionId_c1         = int(open("translogs1.txt").read()) if os.path.exists("translogs1.txt") else 0
        self.usiLinkState1_regis      = str(open("usiLinkState1_regis.txt").read()) if os.path.exists("usiLinkState1_regis.txt") else "Available"
        # ── PERSIST-FIX: restore accepted_tag1 จาก disk ────────────────
        # plc_function.handle_rfid_command() เขียนไฟล์นี้ตอน RFID start
        # หลัง reboot/reconnect ต้อง restore เพื่อให้ RFID stop ใบเดิม match ได้
        # ถ้าไม่มีไฟล์ หรือมี translogs=0 (ไม่มี active txn) ให้ default เป็น '0'
        if os.path.exists("accepted_tag1.txt") and self.transactionId_c1 and self.transactionId_c1 > 0:
            self.accepted_tag1 = open("accepted_tag1.txt").read().strip() or '0'
        else:
            self.accepted_tag1 = '0'

        try:
            persisted_rfid_latch_c1 = int(
                open("rfid_start_received_c1.txt").read().strip() or "0"
            ) if os.path.exists("rfid_start_received_c1.txt") else 0
        except Exception:
            persisted_rfid_latch_c1 = 0
        self.rfid_start_received_c1 = persisted_rfid_latch_c1
        self.active_id_tag_c1 = (
            open("active_id_tag_c1.txt").read().strip()
            if os.path.exists("active_id_tag_c1.txt") else "Default"
        ) or "Default"
        # [METER-RECOVERY-V9]
        # If the process died after RFID authorization/PLC start but before
        # StartTransaction, translogs is still 0 while the local-start latch and
        # active idTag are already persisted. Preserve that pending start so the
        # reconnect session can create StartTransaction and begin MeterValues.
        if self.transactionId_c1 and self.transactionId_c1 > 0:
            if self.accepted_tag1 not in ('0', '', 'None'):
                self.rfid_start_received_c1 = 1
                self.active_id_tag_c1 = self.accepted_tag1
        elif persisted_rfid_latch_c1 == 1 and self._valid_id_tag(self.active_id_tag_c1):
            self.rfid_start_received_c1 = 1
            self.rfid_recovery_pending_c1 = 1
            self.accepted_tag1 = self.active_id_tag_c1
            self.myid_tag_ath_c1 = self.active_id_tag_c1
            self.start_transaction_SW1 = 1
            self.start_sendMeterValue_SW1 = 1
        else:
            self.rfid_start_received_c1 = 0
            self.rfid_recovery_pending_c1 = 0

        # ---- Connector 2 defaults ----
        self.iCpStateCss2       = iCpStateCss2
        self.myconnector_id_c2  = 2
        self.myip_adr_c2        = '2'
        self.myremotestart_c2   = 0
        self.myid_tag_c2        = None
        self.my_meter_ip_c2     = '/dev/ttyACM0'
        self.stage1_sent_done_c2 = 0
        self.transactionId_c2   = None
        self.my_remote_stop_c2  = 0
        # [REMOTE-STOP-LATCH] หลังรับ RemoteStop ให้คง Finishing จนกว่าจะถอดสายเป็น Available
        self.remote_stop_finishing_c2 = 0
        # [SESSION-FINISHING] ดู Connector 1
        self.session_finishing_c2 = 0
        self.emergency_sent_c2  = 1
        self.emergency_sent_Sw_c2 = 1
        self.usiLinkStateCcs2   = usiLinkStateCcs2
        self.myid_tag_ath_c2    = None
        self.finshing_sent_c2   = 0
        self.finshing_sent_sw_c2 = 1
        self.start_trans_SW_c2  = 1
        self.block2             = 0
        # [RFID-OCPP-V7] See Connector 1.
        self.rfid_start_received_c2 = 0
        # [LOCAL-STATUS-V16] ดู Connector 1
        self.local_status_session_c2 = 0
        self._local_status_consumed_ts_c2 = 0.0
        self._local_status_log_ts_c2 = 0.0
        self._pending_local_stop_ts_c2 = 0.0
        self.active_id_tag_c2       = "Default"
        self._rfid_file_check_ts_c2 = 0.0
        self._rfid_consumed_auth_ts_c2 = 0.0
        # [METER-RECOVERY-V9] Same recovery state as Connector 1.
        self.rfid_recovery_pending_c2 = 0
        self._start_state_log_ts_c2 = 0.0
        self._rfid_wait_charging_log_ts_c2 = 0.0
        # [PHYSICAL-STATUS-V10] Same state tracking as Connector 1.
        self._last_ocpp_status_c2 = None
        self._energy_state_check_ts_c2 = 0.0
        self._energy_state_success_ts_c2 = 0.0
        self._energy_state_valid_c2 = False
        self._energy_flow_c2 = False
        self._energy_current_c2 = 0.0
        self._energy_power_c2 = 0.0
        self._no_energy_since_c2 = 0.0
        # [METER-GATE-V12] เหมือน Connector 1
        self._txn_energy_confirmed_c2 = False
        self._meter_gate_log_ts_c2 = 0.0
        self._meter_remote_gate_log_ts_c2 = 0.0
        self.start_transaction_SW2     = 1
        self.start_sendMeterValue_SW2  = 1
        self.meter_start2 = _read_startup_meter_wh(2, self.my_meter_ip_c2)
        self.meter_stop2  = self.meter_start2
        self.timestamp2   = datetime.now(pytz.timezone('Asia/Bangkok')).strftime("%Y-%m-%dT%H:%M:%S.%f")

        # ---- Restore connector-2 state from files ----
        self.block2                   = int(open("block2.txt").read()) if os.path.exists("block2.txt") else 0
        self.emergency_sent_c2        = str(open("emergency_sent_c2.txt").read()).strip() if os.path.exists("emergency_sent_c2.txt") else '1'
        self.emergency_sent_Sw_c2     = int(open("emergency_sent_Sw_c2.txt").read()) if os.path.exists("emergency_sent_Sw_c2.txt") else 1
        self.finshing_sent_c2         = int(open("finshing_sent_c2.txt").read()) if os.path.exists("finshing_sent_c2.txt") else 0
        self.finshing_sent_sw_c2      = int(open("finshing_sent_sw_c2.txt").read()) if os.path.exists("finshing_sent_sw_c2.txt") else 1
        self.my_remote_stop_c2        = int(open("my_remote_stop_c2.txt").read()) if os.path.exists("my_remote_stop_c2.txt") else 0
        self.remote_stop_finishing_c2  = int(open("remote_stop_finishing_c2.txt").read()) if os.path.exists("remote_stop_finishing_c2.txt") else 0
        self.session_finishing_c2   = self._read_int_state("session_finishing_c2.txt", 0)
        self.preparing_timeout_c2   = self._read_int_state("preparing_timeout_c2.txt", 0)
        self.myid_tag_ath_c2          = open("myid_tag_ath_c2.txt").read() if os.path.exists("myid_tag_ath_c2.txt") else "Default"
        self.myid_tag_c2              = open("myid_tag_c2.txt").read() if os.path.exists("myid_tag_c2.txt") else "Default"
        self.myremotestart_c2         = int(open("myremotestart_c2.txt").read()) if os.path.exists("myremotestart_c2.txt") else 0
        self.remote_start_received_c2 = int(open("remote_start_received_c2.txt").read()) if os.path.exists("remote_start_received_c2.txt") else 0
        self.local_status_session_c2   = self._read_int_state("local_status_session_c2.txt", 0)   # [LOCAL-STATUS-V16]
        self.start_trans_SW_c2        = int(open("start_trans_SW_c2.txt").read()) if os.path.exists("start_trans_SW_c2.txt") else 1
        self.start_transaction_SW2    = int(open("start_transaction_SW2.txt").read()) if os.path.exists("start_transaction_SW2.txt") else 1
        self.start_sendMeterValue_SW2 = int(open("start_sendMeterValue_SW2.txt").read()) if os.path.exists("start_sendMeterValue_SW2.txt") else 1
        self.iCp2_regis               = str(open("iCpStateCss2.txt").read()) if os.path.exists("iCpStateCss2.txt") else "Available"
        self.transactionId_c2         = int(open("translogs2.txt").read()) if os.path.exists("translogs2.txt") else 0
        self.usiLinkState2_regis      = str(open("usiLinkState2_regis.txt").read()) if os.path.exists("usiLinkState2_regis.txt") else "Available"
        # ── PERSIST-FIX: restore accepted_tag2 จาก disk ────────────────
        if os.path.exists("accepted_tag2.txt") and self.transactionId_c2 and self.transactionId_c2 > 0:
            self.accepted_tag2 = open("accepted_tag2.txt").read().strip() or '0'
        else:
            self.accepted_tag2 = '0'

        try:
            persisted_rfid_latch_c2 = int(
                open("rfid_start_received_c2.txt").read().strip() or "0"
            ) if os.path.exists("rfid_start_received_c2.txt") else 0
        except Exception:
            persisted_rfid_latch_c2 = 0
        self.rfid_start_received_c2 = persisted_rfid_latch_c2
        self.active_id_tag_c2 = (
            open("active_id_tag_c2.txt").read().strip()
            if os.path.exists("active_id_tag_c2.txt") else "Default"
        ) or "Default"
        if self.transactionId_c2 and self.transactionId_c2 > 0:
            if self.accepted_tag2 not in ('0', '', 'None'):
                self.rfid_start_received_c2 = 1
                self.active_id_tag_c2 = self.accepted_tag2
        elif persisted_rfid_latch_c2 == 1 and self._valid_id_tag(self.active_id_tag_c2):
            self.rfid_start_received_c2 = 1
            self.rfid_recovery_pending_c2 = 1
            self.accepted_tag2 = self.active_id_tag_c2
            self.myid_tag_ath_c2 = self.active_id_tag_c2
            self.start_transaction_SW2 = 1
            self.start_sendMeterValue_SW2 = 1
        else:
            self.rfid_start_received_c2 = 0
            self.rfid_recovery_pending_c2 = 0

        # ---- Configuration ----
        self.HeartbeatInterval_cfg        = _read_safe_integer_cfg("HeartbeatInterval_cfg.txt", 30, 1)
        self.AuthorizationCacheEnabled_cfg = _read_cfg_text("AuthorizationCacheEnabled_cfg.txt", "true")
        self.LocalAuthListEnabled_cfg     = _read_cfg_text("LocalAuthListEnabled_cfg.txt", "true")
        self.AutoChargeEnabled_cfg        = _normalize_bool_string(
            _read_cfg_text("AutoChargeEnabled_cfg.txt", "true"), default="true"
        )
        self.AutoChargeIdTagPrefix_cfg    = _normalize_autocharge_prefix(
            _read_cfg_text("AutoChargeIdTagPrefix_cfg.txt", AUTOCHARGE_ID_TAG_PREFIX)
        ) or AUTOCHARGE_ID_TAG_PREFIX
        self.MeterValuesSampledData_cfg   = _normalize_csv_string(_read_cfg_text("MeterValuesSampledData_cfg.txt", "0"), default="0")
        self.LocalAuthListMaxLength_cfg   = int(_read_cfg_text("LocalAuthListMaxLength_cfg.txt", "100"))
        self.StopTxnSampledData_cfg       = _normalize_csv_string(_read_cfg_text("StopTxnSampledData_cfg.txt", "0"), default="0")
        self.AllowOfflineTxForUnknownId_cfg  = _read_cfg_text("AllowOfflineTxForUnknownId_cfg.txt", "false")
        self.AuthorizeRemoteTxRequests_cfg   = _read_cfg_text("AuthorizeRemoteTxRequests_cfg.txt", "false")
        self.LocalAuthorizeOffline_cfg       = _read_cfg_text("LocalAuthorizeOffline_cfg.txt", "false")
        self.LocalPreAuthorize_cfg           = _read_cfg_text("LocalPreAuthorize_cfg.txt", "false")
        self.MeterValueSampleInterval_cfg    = _read_safe_integer_cfg("MeterValueSampleInterval_cfg.txt", 60, 0)
        self.StopTransactionOnInvalidId_cfg  = _read_cfg_text("StopTransactionOnInvalidId_cfg.txt", "true")
        self.TransactionMessageAttempts_cfg      = _read_safe_integer_cfg("TransactionMessageAttempts_cfg.txt", 3, 0)
        self.TransactionMessageRetryInterval_cfg = _read_safe_integer_cfg("TransactionMessageRetryInterval_cfg.txt", 10, 0)
        self.WebSocketPingInterval_cfg           = _read_safe_integer_cfg("WebSocketPingInterval_cfg.txt", 20, 0)
        # [REMOTE-ONLY] "1"/"true" = session ต้องเริ่มจาก RemoteStartTransaction เท่านั้น (ปิด RFID start)
        self.remote_start_only = _read_cfg_text("RemoteStartOnly_cfg.txt", "1").strip().lower() in ("1", "true")
        # Explicit MQTT RFID commands are trusted local commands and may be
        # enabled independently of the physical-reader/remote-only policy.
        self.allow_rfid_command_start = _read_cfg_text(
            "AllowRfidCommandStart_cfg.txt", "true"
        ).strip().lower() in ("1", "true", "yes", "on")
        # [LOCAL-STATUS-V16] กด Start/Stop จากหน้าจอ HMI (หน้าจอ publish
        # topic/RemoteStart{n} ตรง ๆ) ถือเป็นคำสั่ง local ที่เชื่อถือได้เช่นเดียวกับ
        # topic/RFIDCommand จึงเปิด/ปิดแยกจาก RemoteStartOnly ได้ —
        # RemoteStartOnly ยังคุมแหล่ง start อื่นเหมือนเดิมทุกประการ
        # session แบบนี้ "รายงานสถานะอย่างเดียว" ไม่เปิด OCPP transaction
        self.allow_local_start = _read_cfg_text(
            "AllowLocalStart_cfg.txt", "true"
        ).strip().lower() in ("1", "true", "yes", "on")

        self.already_remote_start = None
        self.block_sw1 = int(open("block_sw1.txt").read()) if os.path.exists("block_sw1.txt") else 0
        self.block_sw2 = int(open("block_sw2.txt").read()) if os.path.exists("block_sw2.txt") else 1
        self.block_sw3 = int(open("block_sw3.txt").read()) if os.path.exists("block_sw3.txt") else 1
        self.block_sw1_C1 = int(open("block_sw1_C1.txt").read()) if os.path.exists("block_sw1_C1.txt") else 0
        self.block_sw2_C1 = int(open("block_sw2_C1.txt").read()) if os.path.exists("block_sw2_C1.txt") else 1
        self.block_sw3_C1 = int(open("block_sw3_C1.txt").read()) if os.path.exists("block_sw3_C1.txt") else 1

        # ---- Local Auth objects ----
        self.authorization_cache = AuthorizationCache(size=50)
        self.local_auth_list = LocalAuthorizationList(
            max_length=int(self.LocalAuthListMaxLength_cfg),
            storage_path="local_auth_list.json",
            version_path="local_auth_list_version.txt",
        )

        self.auth_timestamps = {}

        # AutoCharge decisions run independently per connector. The shared lock
        # only protects ownership/in-flight bookkeeping, never the CSMS round trip.
        self._autocharge_done_c1 = ""
        self._autocharge_done_c2 = ""
        self._autocharge_inflight = {}
        self._autocharge_lock = asyncio.Lock()
        self._autocharge_task = {}

        # [REMOTE-STOP-V3] execute Remote Stop immediately in its own task.
        # Do not depend on main_run_program(), which may be delayed by PLC/meter I/O.
        self._remote_stop_lock_c1 = asyncio.Lock()
        self._remote_stop_lock_c2 = asyncio.Lock()
        self._remote_stop_task_c1 = None
        self._remote_stop_task_c2 = None
        # [EV-STOP] Transaction-scoped claims make PowerDelivery, WeldingDetection
        # and SessionStop idempotent without borrowing any legacy stop switches.
        self._ev_stop_claimed_txn_c1 = 0
        self._ev_stop_claimed_txn_c2 = 0
        self._ev_stop_retry_txn_c1 = 0
        self._ev_stop_retry_txn_c2 = 0
        _ev_stop_guard_now = time.monotonic()
        self._ev_stop_guard_txn_c1 = self._active_transaction_id(1)
        self._ev_stop_guard_txn_c2 = self._active_transaction_id(2)
        self._ev_stop_guard_since_c1 = _ev_stop_guard_now
        self._ev_stop_guard_since_c2 = _ev_stop_guard_now
        # [EMERGENCY-STOP-FIRST] The Vector fault task and the internal emergency
        # handler both close the transactions on a press. Serialise them so the
        # second caller sees the transactions already closed instead of racing.
        self._emergency_stop_lock = asyncio.Lock()
        # MeterValues send operations are serialized per connector so a slow CSMS
        # response cannot create overlapping sends for the same connector.
        self._meter_send_lock_c1 = asyncio.Lock()
        self._meter_send_lock_c2 = asyncio.Lock()

        # [VECTOR-FAULT-OCPP] Consume exact fault metadata produced by the
        # Vector/Node-RED fault collector.  No Phoenix index mapping is used.
        self.vector_fault_bridge = VectorFaultEventBridge(
            inbox=VECTOR_FAULT_INBOX,
            event_topic=VECTOR_FAULT_EVENT_TOPIC,
            status_topic=VECTOR_FAULT_STATUS_TOPIC,
            send_fault=self._send_vector_fault_status_notification,
            send_clear=self._send_vector_fault_clear_notification,
            send_optional_alarms=VECTOR_FAULT_SEND_OPTIONAL_ALARMS,
            status_max_age_seconds=VECTOR_FAULT_STATUS_MAX_AGE_SECONDS,
            logger=logging.getLogger("VECTOR_FAULT_OCPP"),
        )

    # ========================================================================
    # Authorize
    # ========================================================================
    def _dual_connector_busy(self, connector_id):
        """Physical/core guard. The router checks its own persistent claim too."""
        return bool(self._dual_busy_reason(connector_id))

    def _dual_busy_reason(self, connector_id):
        """Why _dual_connector_busy() refuses a new start; "" when the head is free.

        [R3.1.1] Same conditions in the same order as R3.1 (take_local_request() and
        the PLC read still run only when every earlier check passed). The text is
        only for the log and never contains an idTag.

        [R3.1.6] Right before the start flags are checked, flags left by a RemoteStart
        that ended before StartTransaction are released (_heal_stale_start_latch), so
        they no longer refuse this start. A start that may still be running is kept."""
        cid = int(connector_id)
        runtime = getattr(self, "_protocol_runtime", None)
        if runtime is not None:
            if cid in runtime.faulted:
                return "connector faulted after a PLC limit failure"
            if runtime.smart.inhibited:
                return "smart charging inhibited (%s)" % (runtime.smart.last_error or "unknown error")
            if not runtime.services._operative(cid):
                return "connector Inoperative"
        if not self._dual_ready:
            return "charger startup not finished"
        if cid in self._dual_aborting:
            return "previous start is being aborted"
        if cid in self._dual_local_claims:
            return "local RFID start in progress"
        txn = self._active_transaction_id(cid)
        if txn > 0:
            return "transaction %s is active" % txn
        if self._remote_stop_worker_running(cid):
            return "remote stop still running"
        if self._session_finishing_active(cid):
            return "previous session finishing - unplug the cable first"
        if self._local_status_session_active(cid):
            return "session started from the charger screen"
        self._heal_stale_start_latch(cid)   # [R3.1.6] logs what it releases
        for name in (f"remote_start_received_c{cid}", f"myremotestart_c{cid}",
                     f"rfid_start_received_c{cid}", f"rfid_recovery_pending_c{cid}"):
            if int(getattr(self, name, 0) or 0) == 1:
                return name + "=1"
        if int(getattr(self, f"start_transaction_SW{cid}", 0) or 0) == 3:
            return "start already sent to the PLC (start_transaction_SW%s=3)" % cid
        if self._valid_id_tag(getattr(self, f"accepted_tag{cid}", "0")):
            return "an accepted RFID tag is waiting for its start"
        if take_local_request("start", cid) > 0:
            return "start request from the charger screen pending"
        try:
            state, _ = plc.get_connector_status(str(cid))
        except Exception as error:
            return "PLC state unreadable (%s)" % type(error).__name__
        # [R3.1.2] An EV in CP state C without CurrentDemand is reported by the PLC as
        # SuspendedEV while it waits for authorisation. With no transaction (checked above)
        # _ocpp_physical_status() and the CSMS see Preparing, so it must not block a
        # RemoteStart. Charging (energy flowing) without a transaction stays busy.
        if state not in ("Available", "Preparing", "SuspendedEV", "SuspendedEVSE"):
            return "PLC reports %s" % state
        return ""

    # ------------------------------------------------------------------------
    # [START-LATCH] R3.1.6
    # remote_start_transaction() sets myremotestart_cN=1 and remote_start_received_cN=1,
    # main_run_program() pulses the PLC and sets start_transaction_SWN=3 until
    # StartTransaction. A RemoteStart whose session ends before StartTransaction has to
    # release all three, as the card path does with rfid_start_received_cN. On 2026-11-10
    # (Edgebox 192.168.3.15) the EV ended the session 18 s after the pulse, the cable was
    # removed, start_transaction_SW2 stayed 3 and every later RemoteStart on head 2 was
    # refused until CP.py was restarted.
    # ------------------------------------------------------------------------
    def _remote_start_latch_age(self, connector_id):
        """Seconds since this head's RemoteStart latch was set; None when unknown."""
        cid = int(connector_id)
        since = getattr(self, f"_remote_start_latch_mono_c{cid}", None)
        if since is not None:
            return max(0.0, time.monotonic() - float(since))
        # After a restart only the state files are left; they were written when it was set.
        if int(getattr(self, f"start_transaction_SW{cid}", 0) or 0) == 3:
            name = f"start_transaction_SW{cid}.txt"
        else:
            name = f"remote_start_received_c{cid}.txt"
        try:
            age = time.time() - os.path.getmtime(name)
        except (OSError, TypeError, ValueError):
            return None
        return age if age >= 0 else None

    def _stale_start_latch_reason(self, connector_id):
        """Why this head's RemoteStart flags, or the CSMS claim pending on it, are stale.

        "" when nothing is latched or a start may still be running. Text for the log
        only; never contains an idTag. The PLC is read only for a latch older than
        REMOTE_START_LATCH_TIMEOUT_SECONDS."""
        cid = int(connector_id)
        if cid not in (1, 2) or not getattr(self, "_dual_ready", False):
            return ""
        if cid in self._dual_aborting or cid in self._dual_local_claims:
            return ""
        if self._active_transaction_id(cid) > 0 or self._remote_stop_worker_running(cid):
            return ""
        if self._session_finishing_active(cid) or self._local_status_session_active(cid):
            return ""
        if int(getattr(self, f"myremotestart_c{cid}", 0) or 0) == 1:
            return ""                                   # the PLC start pulse is still to come
        for name in (f"rfid_start_received_c{cid}", f"rfid_recovery_pending_c{cid}"):
            if int(getattr(self, name, 0) or 0) == 1:
                return ""                               # a card start owns the head
        if self._valid_id_tag(getattr(self, f"accepted_tag{cid}", "0")):
            return ""
        received = int(getattr(self, f"remote_start_received_c{cid}", 0) or 0) == 1
        pulsed = int(getattr(self, f"start_transaction_SW{cid}", 0) or 0) == 3
        if pulsed and not received:
            # remote_start_received is cleared only when the session ends (cable out,
            # StopTransaction, abort); nothing can use this pulse any more.
            return "start_transaction_SW%s=3 left after its RemoteStart session ended" % cid
        if received and not pulsed:
            # set together with myremotestart (still 1 until the pulse) - this one lost it
            return "remote_start_received_c%s=1 without a PLC start pulse" % cid
        if received:
            age = self._remote_start_latch_age(cid)
            if age is None or age < REMOTE_START_LATCH_TIMEOUT_SECONDS:
                return ""                               # the EV may still be starting
            try:
                state, _ = plc.get_connector_status(str(cid))
            except Exception:
                return ""
            if state not in ("Available", "Preparing"):
                return ""                               # an EV session is running: keep it
            return ("RemoteStart sent to the PLC %d s ago and no StartTransaction (PLC %s)"
                    % (age, state))
        manager = getattr(self, "_dual_manager", None)
        claim_age = manager.reserved_age(cid) if hasattr(manager, "reserved_age") else None
        if claim_age is not None and claim_age >= REMOTE_START_LATCH_TIMEOUT_SECONDS:
            return "CSMS claim pending %d s without a start" % claim_age
        return ""

    def _clear_remote_start_latch(self, connector_id, reason, release_claim=True):
        """Release a RemoteStart that ended before StartTransaction: myremotestart_cN,
        remote_start_received_cN, start_transaction_SWN=3 and, with release_claim, the
        pending CSMS claim in the dual-CSMS router. Never touches a head with an open
        transaction. Logs and returns True when something was released."""
        cid = int(connector_id)
        if cid not in (1, 2) or self._active_transaction_id(cid) > 0:
            return False
        released = []
        for name, latched, idle in ((f"myremotestart_c{cid}", 1, 0),
                                    (f"remote_start_received_c{cid}", 1, 0),
                                    (f"start_transaction_SW{cid}", 3, 1)):
            if int(getattr(self, name, idle) or 0) == latched:
                setattr(self, name, idle)
                _write_file(name + ".txt", idle)
                released.append("%s=%s" % (name, latched))
        setattr(self, f"_remote_start_latch_mono_c{cid}", None)
        manager = getattr(self, "_dual_manager", None)
        if release_claim and manager is not None and manager.release_pending(cid):
            released.append("CSMS claim")
        if released:
            logging.warning("[START-LATCH] C%s released %s: %s", cid, ", ".join(released), reason)
        return bool(released)

    def _heal_stale_start_latch(self, connector_id):
        """Release stale RemoteStart flags on this head instead of refusing the next start.

        Called before a start is admitted: by the dual-CSMS router before its claim check
        and by _dual_busy_reason() for RemoteStart and card starts. A pending CSMS claim
        younger than REMOTE_START_LATCH_TIMEOUT_SECONDS is kept: it may belong to a start
        being admitted right now. Returns the reason when something was released."""
        try:
            cid = int(connector_id)
            reason = self._stale_start_latch_reason(cid)
            if not reason:
                return ""
            manager = getattr(self, "_dual_manager", None)
            claim_age = manager.reserved_age(cid) if hasattr(manager, "reserved_age") else None
            old_claim = claim_age is not None and claim_age >= REMOTE_START_LATCH_TIMEOUT_SECONDS
            if self._clear_remote_start_latch(cid, "stale, " + reason, release_claim=old_claim):
                return reason
        except Exception as error:
            logging.warning("[START-LATCH] C%s stale start check failed (%s)",
                            connector_id, type(error).__name__)
        return ""

    async def _dual_rfid_command_start(self, connector_id, id_tag):
        """Run MQTT RFID arbitration on the same loop as the OCPP handlers."""
        cid = int(connector_id)
        manager = self._dual_manager
        if manager is None or not self.allow_rfid_command_start or not self._valid_id_tag(id_tag):
            return False
        gate = getattr(self, "_ocpp_start_allowed", None)
        # [R3.1.4] the CSMS that last accepted this tag (local_csms when unknown)
        if gate is not None and not await gate(cid, manager.local_owner_for(id_tag), id_tag):
            return False
        if self._dual_connector_busy(cid) or not manager.claim_local(cid, id_tag):
            logging.warning("[DUAL] RFID start rejected: connector %s is busy", cid)
            return False
        self._dual_local_claims.add(cid)
        armed = False
        try:
            result = await self.authorize(id_tag)
            info = result.id_tag_info
            if (info.get("status") if isinstance(info, dict) else getattr(info, "status", None)) != "Accepted":
                return False
            # Claim was held across Authorize; no remote start can overtake it.
            state, _ = plc.get_connector_status(str(cid))
            if state not in ("Available", "Preparing") or self._active_transaction_id(cid) > 0:
                return False
            self._clear_stale_stop_latch_for_new_start(cid, "RFIDCommand")
            for name, value in ((f"myid_tag_c{cid}", id_tag), (f"myid_tag_ath_c{cid}", id_tag),
                                (f"accepted_tag{cid}", id_tag), (f"active_id_tag_c{cid}", id_tag),
                                (f"rfid_start_received_c{cid}", 1), (f"start_transaction_SW{cid}", 1)):
                setattr(self, name, value)
                _write_file(name + ".txt", value)
            armed = True
            await async_remote_start_plc(str(cid), 1)
            try:
                await asyncio.sleep(3)
            finally:
                await async_remote_start_plc(str(cid), 0)
            return True
        except Exception:
            if armed:
                await self._abort_session_immediately(cid)
            raise
        finally:
            self._dual_local_claims.discard(cid)
            if not armed:
                manager.release_pending(cid)

    async def _dual_start_transaction(self, request):
        """Never restart the whole hardware loop on an upstream start failure."""
        try:
            return await self._call_txn_message_with_retry(request, msg_name="StartTransaction")
        except DualCSMSError as error:
            cid = int(request.connector_id)
            logging.error("[DUAL] C%s StartTransaction failed: %s", cid, error)
            # The legacy flow may have pulsed start before sending StartTransaction.
            # Stop locally, but retain an unknown router claim for reconciliation.
            lookup = getattr(self._dual_manager, "_live", None)
            mapped = lookup(cid) if lookup else None
            if mapped and mapped["state"] in ("active", "rejected"):
                await self._abort_session_immediately(cid, transaction_id=mapped["local_id"], id_tag=request.id_tag)
            else:
                await self._abort_session_immediately(cid, id_tag=request.id_tag)
            return None

    async def authorize(self, id_tag):
        print(f"?? AUTHORIZE CALLED: {id_tag}")
        # [R3.1.4] Dual mode asks local_csms first, then the other CSMS; the session
        # belongs to the CSMS that accepted. Record only Accepted results so a
        # timeout/rejection cannot be reused as successful auth.
        if self._dual_manager is None:
            self.auth_timestamps[id_tag] = time.time()

        use_cache = str(self.AuthorizationCacheEnabled_cfg).strip().lower() == "true"

        if use_cache:
            try:
                cached = self.authorization_cache.get(id_tag)
                if cached:
                    cached_info = cached.get("idTagInfo")
                    if isinstance(cached_info, dict) and cached_info.get("status") == "Accepted":
                        self.auth_timestamps[id_tag] = time.time()
                        logging.info(f"Authorize: use cached Accepted for idTag={id_tag}")
                        return SimpleNamespace(id_tag_info=cached_info)
            except Exception:
                pass

        request = call.Authorize(id_tag=id_tag)
        try:
            response = await self.call(request)
        except Exception as e:
            print(f"## authorize error: {e}")
            raise

        if use_cache:
            try:
                self.authorization_cache.add(id_tag, response.id_tag_info)
            except Exception:
                pass

        if self._dual_manager is not None:
            info = response.id_tag_info
            if (info.get("status") if isinstance(info, dict) else getattr(info, "status", None)) == "Accepted":
                self.auth_timestamps[id_tag] = time.time()
            else:
                self.auth_timestamps.pop(id_tag, None)
        return response

    # ========================================================================
    # AutoCharge (EVCCID -> OCPP Authorize -> existing local-start funnel)
    # ========================================================================
    def _autocharge_enabled(self):
        return _normalize_bool_string(
            getattr(self, "AutoChargeEnabled_cfg", "true"), default="true"
        ) == "true"

    def _autocharge_prefix(self):
        return getattr(self, "AutoChargeIdTagPrefix_cfg", AUTOCHARGE_ID_TAG_PREFIX)

    @staticmethod
    def _normalize_evccid(raw, prefix=AUTOCHARGE_ID_TAG_PREFIX):
        """Build an OCPP 1.6 CiString20 idTag from a vSECC EVCCID."""
        text = "".join(
            ch for ch in str(raw or "").strip()
            if ch.isascii() and ch.isalnum()
        ).upper()
        # Accept already-prefixed values without creating AUTO-AUTO... and also
        # tolerate the VID: convention used by older AutoCharge deployments.
        # A configured prefix is stripped only when it contains a non-hex
        # character, otherwise it could eat the first digits of a hex EVCCID.
        custom = "".join(ch for ch in str(prefix or "") if ch.isalnum()).upper()
        if custom and custom not in ("AUTO", "VID") and \
                any(ch not in "0123456789ABCDEF" for ch in custom) and \
                text.startswith(custom):
            text = text[len(custom):]
        elif text.startswith("AUTO"):
            text = text[4:]
        elif text.startswith("VID"):
            text = text[3:]
        if not text:
            return ""
        tag = f"{prefix}{text}" if prefix else text
        if len(tag) > 20:
            logging.error(
                "[AUTOCHARGE] idTag %r is %d chars; OCPP 1.6 allows at most 20",
                tag, len(tag),
            )
            return ""
        return tag

    @staticmethod
    def _autocharge_cached_evccid(connector_id):
        conn = str(connector_id)
        with AUTOCHARGE_EVCCID_LOCK:
            latest = AUTOCHARGE_EVCCID_LATEST.get(conn, "")
        if latest:
            return latest
        try:
            value = plc.mqtt.get_value(
                AUTOCHARGE_EVCCID_TOPIC.format(connector=conn)
            )
            return str(value if value is not None else "").strip()
        except Exception:
            return ""

    def _autocharge_session_armed(self, connector_id):
        conn = str(connector_id)
        tag = str(getattr(self, f"_autocharge_done_c{conn}", "") or "").strip()
        accepted = str(getattr(self, f"accepted_tag{conn}", "") or "").strip()
        latch = int(getattr(self, f"rfid_start_received_c{conn}", 0) or 0)
        return bool(tag and accepted.lower() == tag.lower() and latch == 1)

    def _autocharge_owner(self, connector_id, id_tag=None):
        """Return the current owner that prevents AutoCharge from claiming a head."""
        conn = int(connector_id)
        if self._dual_manager is not None and self._dual_manager.connector_reserved(conn):
            return "dual-csms-reserved"
        if self._active_transaction_id(conn) > 0:
            return "transaction-open"
        if int(getattr(self, f"remote_start_received_c{conn}", 0) or 0) == 1:
            return "remote-start-owns-session"
        if int(getattr(self, f"myremotestart_c{conn}", 0) or 0) == 1:
            return "remote-start-pending"
        if int(getattr(self, f"start_transaction_SW{conn}", 0) or 0) == 3:
            return "remote-start-arming"
        if int(getattr(self, f"rfid_start_received_c{conn}", 0) or 0) == 1:
            return "local-start-already-armed"
        if self._local_status_session_active(conn):
            return "local-screen-start"
        accepted = getattr(self, f"accepted_tag{conn}", "0")
        if self._valid_id_tag(accepted):
            return "another-tag-owns-session"
        if self._session_finishing_active(conn):
            return "session-finishing"

        if id_tag:
            other = 2 if conn == 1 else 1
            for attr in (f"accepted_tag{other}", f"active_id_tag_c{other}"):
                held = str(getattr(self, attr, "") or "").strip()
                if held and held.lower() == str(id_tag).strip().lower():
                    return f"token-held-by-connector-{other}"
            holder = self._autocharge_inflight.get(id_tag)
            if holder is not None and holder != conn:
                return f"token-in-flight-on-connector-{holder}"
        return None

    def _rollback_autocharge_arm(self, connector_id, id_tag):
        """Undo only state still owned by this AutoCharge decision."""
        conn = str(connector_id)
        if getattr(self, f"_pin_autocharge_start_intent_c{conn}", "") == id_tag:
            setattr(self, f"_pin_autocharge_start_intent_c{conn}", "")
        if str(getattr(self, f"accepted_tag{conn}", "") or "") == id_tag:
            setattr(self, f"accepted_tag{conn}", "0")
            _write_file(f"accepted_tag{conn}.txt", "0")
        if str(getattr(self, f"myid_tag_ath_c{conn}", "") or "") == id_tag:
            setattr(self, f"myid_tag_ath_c{conn}", "Default")
            _write_file(f"myid_tag_ath_c{conn}.txt", "Default")
        if self._active_transaction_id(int(conn)) <= 0:
            setattr(self, f"rfid_start_received_c{conn}", 0)
            _write_file(f"rfid_start_received_c{conn}.txt", 0)
            if self._dual_manager is not None:
                self._dual_manager.release_pending(int(conn))

    async def _pulse_autocharge_start(self, connector_id):
        """Pulse RemoteStart and guarantee the falling edge on every exit path."""
        conn = str(connector_id)
        high_ok = False
        try:
            result = await async_remote_start_plc(conn, 1)
            result_text = str(result).strip().lower()
            high_ok = "successful" in result_text and "failed" not in result_text
            if not high_ok:
                logging.error(
                    "[AUTOCHARGE] C%s RemoteStart high pulse failed: %s", conn, result
                )
                return False
            await asyncio.sleep(0.5)
            return True
        finally:
            try:
                reset_result = await asyncio.shield(async_remote_start_plc(conn, 0))
                if "failed" in str(reset_result).strip().lower():
                    logging.error(
                        "[AUTOCHARGE] C%s RemoteStart reset failed: %s",
                        conn, reset_result,
                    )
            except Exception as e:
                logging.exception(
                    "[AUTOCHARGE] C%s could not reset RemoteStart pulse: %s", conn, e
                )

    async def _handle_autocharge(self, connector_id, raw_evccid):
        conn = int(connector_id)
        # Capture the prefix once so a ChangeConfiguration during Authorize
        # cannot make the stale-EVCCID check below compare two different formats.
        prefix = self._autocharge_prefix()
        id_tag = self._normalize_evccid(raw_evccid, prefix)
        logging.warning(
            "[AUTOCHARGE] C%s EVCCID raw=%r -> idTag=%r", conn, raw_evccid, id_tag
        )
        if not self._autocharge_enabled() or not id_tag:
            return
        if getattr(self, f"_autocharge_done_c{conn}", "") == id_tag:
            return

        async with self._autocharge_lock:
            owner = self._autocharge_owner(conn, id_tag)
            if owner:
                logging.info("[AUTOCHARGE] C%s skipped: %s", conn, owner)
                return
            self._autocharge_inflight[id_tag] = conn
            setattr(self, f"_autocharge_done_c{conn}", id_tag)

        armed = False
        try:
            try:
                response = await self.authorize(id_tag)
            except Exception as e:
                logging.warning("[AUTOCHARGE] C%s Authorize failed: %s", conn, e)
                setattr(self, f"_autocharge_done_c{conn}", "")
                return

            try:
                info = response.id_tag_info
                status = info.get("status") if isinstance(info, dict) else getattr(info, "status", None)
            except Exception:
                status = None
            if status != "Accepted":
                logging.warning(
                    "[AUTOCHARGE] C%s rejected idTag=%s status=%s",
                    conn, id_tag, status,
                )
                return

            # Configuration and ownership may change while waiting for the CSMS.
            if not self._autocharge_enabled():
                logging.info("[AUTOCHARGE] C%s disabled while Authorize was pending", conn)
                setattr(self, f"_autocharge_done_c{conn}", "")
                return
            owner = self._autocharge_owner(conn, id_tag)
            if owner:
                logging.warning(
                    "[AUTOCHARGE] C%s accepted but superseded by %s", conn, owner
                )
                return

            plc_state, _ = plc.get_connector_status(str(conn))
            if plc_state not in ("Preparing", "SuspendedEV", "SuspendedEVSE", "Charging"):
                logging.info(
                    "[AUTOCHARGE] C%s vehicle no longer connected (state=%s)",
                    conn, plc_state,
                )
                setattr(self, f"_autocharge_done_c{conn}", "")
                return
            current_id_tag = self._normalize_evccid(
                self._autocharge_cached_evccid(conn), prefix
            )
            if current_id_tag != id_tag:
                logging.warning(
                    "[AUTOCHARGE] C%s stale EVCCID decision (%s != %s)",
                    conn, current_id_tag, id_tag,
                )
                setattr(self, f"_autocharge_done_c{conn}", "")
                return

            if self._dual_manager is not None and not self._dual_manager.claim_local(conn, id_tag):
                return
            setattr(self, f"_pin_autocharge_start_intent_c{conn}", id_tag)
            setattr(self, f"myid_tag_ath_c{conn}", id_tag)
            setattr(self, f"accepted_tag{conn}", id_tag)
            setattr(self, f"rfid_start_received_c{conn}", 1)
            setattr(self, f"rfid_recovery_pending_c{conn}", 0)
            setattr(self, f"start_transaction_SW{conn}", 1)
            _write_file(f"myid_tag_ath_c{conn}.txt", id_tag)
            _write_file(f"accepted_tag{conn}.txt", id_tag)
            _write_file(f"rfid_start_received_c{conn}.txt", 1)
            _write_file(f"start_transaction_SW{conn}.txt", 1)
            armed = True

            if not await self._pulse_autocharge_start(conn):
                self._rollback_autocharge_arm(conn, id_tag)
                setattr(self, f"_autocharge_done_c{conn}", "")
                return
            logging.warning("[AUTOCHARGE] C%s accepted and armed idTag=%s", conn, id_tag)
        except asyncio.CancelledError:
            if armed and self._active_transaction_id(conn) <= 0:
                self._rollback_autocharge_arm(conn, id_tag)
            setattr(self, f"_autocharge_done_c{conn}", "")
            raise
        finally:
            async with self._autocharge_lock:
                if self._autocharge_inflight.get(id_tag) == conn:
                    self._autocharge_inflight.pop(id_tag, None)

    async def _autocharge_task_body(self, connector_id, evccid):
        try:
            await self._handle_autocharge(connector_id, evccid)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logging.exception(
                "[AUTOCHARGE] C%s handler error: %s", connector_id, e
            )
            setattr(self, f"_autocharge_done_c{connector_id}", "")
        finally:
            current = asyncio.current_task()
            if self._autocharge_task.get(int(connector_id)) is current:
                self._autocharge_task.pop(int(connector_id), None)

    def _drain_autocharge_events(self):
        newest = {}
        while True:
            try:
                head, evccid = AUTOCHARGE_EVCCID_EVENTS.get_nowait()
            except queue.Empty:
                break
            try:
                newest[int(head)] = str(evccid or "").strip()
            except (TypeError, ValueError):
                continue

        if not self._autocharge_enabled():
            return
        for conn, evccid in newest.items():
            if conn not in (1, 2):
                continue
            if not evccid:
                task = self._autocharge_task.get(conn)
                if task is not None and not task.done() and self._active_transaction_id(conn) <= 0:
                    task.cancel()
                if self._active_transaction_id(conn) <= 0:
                    setattr(self, f"_autocharge_done_c{conn}", "")
                continue
            task = self._autocharge_task.get(conn)
            if task is not None and not task.done():
                logging.info(
                    "[AUTOCHARGE] C%s decision already running; dropped %r",
                    conn, evccid,
                )
                continue
            self._autocharge_task[conn] = asyncio.create_task(
                self._autocharge_task_body(conn, evccid)
            )

    def _seed_autocharge_events(self):
        """Replace stale reconnect events with the live cached EVCCID per head."""
        while True:
            try:
                AUTOCHARGE_EVCCID_EVENTS.get_nowait()
            except queue.Empty:
                break
        if not self._autocharge_enabled():
            return
        for conn in (1, 2):
            try:
                evccid = self._autocharge_cached_evccid(conn)
                state, _ = plc.get_connector_status(str(conn))
                if evccid and state not in ("Available", "Faulted", "Unavailable"):
                    AUTOCHARGE_EVCCID_EVENTS.put_nowait((str(conn), evccid))
            except Exception as e:
                logging.warning("[AUTOCHARGE] C%s seed failed: %s", conn, e)

    def _release_session_identity(self, connector_id):
        """Release persisted RFID/vehicle identity when the cable is out."""
        conn = str(connector_id)
        if self._dual_manager is not None:
            self._dual_manager.release_pending(int(connector_id))
        self._clear_pending_local_stop(connector_id)
        setattr(self, f"accepted_tag{conn}", "0")
        setattr(self, f"myid_tag_ath_c{conn}", "Default")
        setattr(self, f"rfid_start_received_c{conn}", 0)
        _write_file(f"accepted_tag{conn}.txt", "0")
        _write_file(f"myid_tag_ath_c{conn}.txt", "Default")
        _write_file(f"rfid_start_received_c{conn}.txt", 0)
        setattr(self, f"_autocharge_done_c{conn}", "")
        task = self._autocharge_task.pop(int(connector_id), None)
        if task is not None and not task.done() and task is not asyncio.current_task():
            task.cancel()

    # ========================================================================
    # SetChargingProfile / ClearChargingProfile / GetCompositeSchedule
    # ========================================================================
    @on(Action.set_charging_profile)
    async def on_set_charging_profile(self, **kwargs):
        return await self._ocpp_protocol().set_profile(**kwargs)

    @on(Action.clear_charging_profile)
    async def on_clear_chargingprofile(self, **kwargs):
        return await self._ocpp_protocol().clear_profiles(**kwargs)

    @on(Action.get_composite_schedule)
    async def on_get_compositeSchedul(self, **kwargs):
        return await self._ocpp_protocol().composite(**kwargs)

    # ========================================================================
    # Configuration Handlers
    # ========================================================================
    @on(Action.change_availability)
    async def on_change_availability(self, **kwargs):
        runtime = self._ocpp_protocol()
        return await runtime.services.change_availability(command_origin(runtime.admin_owner), **kwargs)

    @on(Action.reserve_now)
    async def on_reserve_now(self, **kwargs):
        runtime = self._ocpp_protocol()
        return await runtime.services.reserve_now(command_origin(runtime.local_owner), **kwargs)

    @on(Action.cancel_reservation)
    async def on_cancel_reservation(self, **kwargs):
        runtime = self._ocpp_protocol()
        return await runtime.services.cancel_reservation(command_origin(runtime.local_owner), **kwargs)

    @on(Action.get_diagnostics)
    async def on_get_diagnostics(self, **kwargs):
        runtime = self._ocpp_protocol()
        return await runtime.services.get_diagnostics(command_origin(runtime.admin_owner), **kwargs)

    @on(Action.update_firmware)
    async def on_update_firmware(self, **kwargs):
        runtime = self._ocpp_protocol()
        return await runtime.services.update_firmware(command_origin(runtime.admin_owner), **kwargs)

    @on(Action.data_transfer)
    async def on_data_transfer(self, **kwargs):
        return call_result.DataTransfer(status="UnknownVendorId")

    async def _ocpp_apply_limits(self, connector, limits):
        # PLC helpers report publish failures as strings rather than raising.
        for writer, value in ((limitpower, limits["W"]), (limitcurrent, limits["A"])):
            result = await asyncio.to_thread(writer, str(connector), value)
            if not isinstance(result, str) or "Failed" in result or " ok" not in result:
                raise RuntimeError("PLC limit publish failed")
        snapshot = getattr(self, "_ocpp_limit_commands", {})
        snapshot[connector] = dict(limits)
        self._ocpp_limit_commands = snapshot
        if (not getattr(self, "_ocpp_legacy_limit_mode_cleared", False)
                and set(snapshot) == {1, 2}
                and all(cap == 0 for values in snapshot.values() for cap in values.values())):
            # R2's PLC sharing switch must not divide R3's per-head allocations
            # again. Clear it only after publishing zero caps to both heads.
            for cid in (1, 2):
                result = await asyncio.to_thread(limit2connector, str(cid), 0)
                if not isinstance(result, str) or "Failed" in result or " ok" not in result:
                    raise RuntimeError("PLC legacy sharing reset failed")
            self._ocpp_legacy_limit_mode_cleared = True

    async def _ocpp_limit_fault_stop(self, connector):
        transaction_id = self._ocpp_protocol().transaction_id(connector)
        tag = getattr(self, f"active_id_tag_c{connector}", None)
        await self._abort_session_immediately(connector, transaction_id=transaction_id or None, id_tag=tag)

    async def _ocpp_cancel_pending_start(self, connector):
        if self._active_transaction_id(connector) <= 0:
            await self._abort_session_immediately(connector)

    def _ocpp_vector_fault_active(self, connector):
        try:
            return bool(self._vector_fault_blocks_normal_status(connector))
        except Exception:
            return False

    def _ocpp_hides_service_status(self, payload):
        """[R3.1.2] While the Vector fault bridge reports a connector Faulted, a status from the
        R3 service layer (startup announcement, availability, reservation) must not replace it:
        on 2026-11-09 the startup announcement sent Available over an active PM fault."""
        if type(payload).__name__ != "StatusNotification" or not service_status_active():
            return False
        connector = getattr(payload, "connector_id", None)
        if not self._ocpp_vector_fault_active(connector):
            return False
        self._log_vector_fault_suppression(connector, "service %s" % payload.status)
        return True

    def _ocpp_physical_status(self, connector):
        if self._ocpp_vector_fault_active(connector):
            return "Faulted"   # [R3.1.2] what the CSMS is shown while a Vector fault is active
        if connector == 0:
            return "Available"
        raw = str(getattr(self, f"iCpStateCss{connector}", "Available"))
        if raw in ("Charging", "SuspendedEV", "SuspendedEVSE") and self._active_transaction_id(connector) <= 0:
            return "Preparing"
        return raw if raw in ("Available", "Preparing", "Charging", "SuspendedEV", "SuspendedEVSE", "Finishing", "Faulted", "Unavailable") else "Preparing"

    @on(Action.get_configuration)
    async def on_get_configuration(self, **kwargs):
        full_configuration = [
            {'key': 'NumberOfConnectors',             'readonly': True,  'value': '2'},
            {'key': 'SupportedFeatureProfiles',       'readonly': True,  'value': str(getattr(self, 'SupportedFeatureProfiles_cfg', 'Core,LocalAuthListManagement,RemoteTrigger,SmartCharging'))},
            {'key': 'HeartbeatInterval',              'readonly': False, 'value': str(self.HeartbeatInterval_cfg)},
            {'key': 'AuthorizationCacheEnabled',       'readonly': False, 'value': str(self.AuthorizationCacheEnabled_cfg)},
            {'key': 'LocalAuthListEnabled',            'readonly': False, 'value': str(self.LocalAuthListEnabled_cfg)},
            {'key': 'AutoChargeEnabled',               'readonly': False, 'value': str(self.AutoChargeEnabled_cfg)},
            {'key': 'AutoChargeIdTagPrefix',           'readonly': False, 'value': str(self._autocharge_prefix())},
            {'key': 'LocalAuthListMaxLength',          'readonly': True,  'value': str(self.LocalAuthListMaxLength_cfg)},
            {'key': 'StopTxnSampledData',              'readonly': False, 'value': str(self.StopTxnSampledData_cfg)},
            {'key': 'MeterValuesSampledData',          'readonly': False, 'value': str(self.MeterValuesSampledData_cfg)},
            {'key': 'AllowOfflineTxForUnknownId',      'readonly': False, 'value': str(self.AllowOfflineTxForUnknownId_cfg)},
            {'key': 'AuthorizeRemoteTxRequests',       'readonly': False, 'value': str(self.AuthorizeRemoteTxRequests_cfg)},
            {'key': 'LocalAuthorizeOffline',           'readonly': False, 'value': str(self.LocalAuthorizeOffline_cfg)},
            {'key': 'LocalPreAuthorize',               'readonly': False, 'value': str(self.LocalPreAuthorize_cfg)},
            {'key': 'MeterValueSampleInterval',        'readonly': False, 'value': str(self.MeterValueSampleInterval_cfg)},
            {'key': 'StopTransactionOnInvalidId',      'readonly': False, 'value': str(self.StopTransactionOnInvalidId_cfg)},
            {'key': 'TransactionMessageAttempts',      'readonly': False, 'value': str(self.TransactionMessageAttempts_cfg)},
            {'key': 'TransactionMessageRetryInterval', 'readonly': False, 'value': str(self.TransactionMessageRetryInterval_cfg)},
            {'key': 'WebSocketPingInterval',           'readonly': False, 'value': str(self.WebSocketPingInterval_cfg)},
        ]
        full_configuration.extend(stop_pin_config.configuration_items())
        for key, value in {
            "ChargeProfileMaxStackLevel": "100", "ChargingScheduleAllowedChargingRateUnit": "Current,Power",
            "ChargingScheduleMaxPeriods": "256", "MaxChargingProfilesInstalled": "128",
            "ReserveConnectorZeroSupported": "false", "SupportedFileTransferProtocols": "HTTP,HTTPS",
            "SendLocalListMaxLength": str(self.LocalAuthListMaxLength_cfg),
            "FirmwareUpdateMode": "DownloadAndStage", "GetConfigurationMaxKeys": "256",
        }.items():
            full_configuration.append({"key": key, "readonly": True, "value": value})
        if self._v2g_bridge is not None:
            full_configuration.extend(self._v2g_bridge.configuration_items())
        configuration_by_key = {
            item['key']: item for item in full_configuration
        }

        raw_keys = kwargs.get('key')
        if raw_keys is None:
            requested_keys = []
        elif isinstance(raw_keys, str):
            requested_keys = [raw_keys]
        else:
            try:
                requested_keys = list(raw_keys)
            except TypeError:
                requested_keys = [raw_keys]

        # OCPP 1.6 uses an omitted key field to request all configuration.
        # This CSMS sends [""] for the same purpose, so ignore blank entries
        # and treat a request containing only blanks as "get all" as well.
        requested_keys = [
            str(key).strip()
            for key in requested_keys
            if key is not None and str(key).strip()
        ]

        if not requested_keys:
            configuration_key = [dict(item) for item in full_configuration]
            unknown_key = []
        else:
            configuration_key = []
            unknown_key = []
            seen_keys = set()
            for key in requested_keys:
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                item = configuration_by_key.get(key)
                if item is None:
                    unknown_key.append(key)
                else:
                    configuration_key.append(dict(item))

        return call_result.GetConfiguration(
            configuration_key=configuration_key,
            unknown_key=unknown_key or None,
        )

    @on(Action.change_configuration)
    async def on_change_configuration(self, key, value, **kwargs):
        if not isinstance(key, str):
            return call_result.ChangeConfiguration(status='Rejected')
        if key in STOP_PIN_KEYS:
            status = await asyncio.to_thread(stop_pin_config.change_configuration, key, value)
            return call_result.ChangeConfiguration(status=status)
        if key.startswith("V2G") or key in (
            "TimeToFullSampledInterval", "TimeToFullSamapIedInterval",
            "CurrentDemandSampledInterval",
        ):
            status = (self._v2g_bridge.change_configuration(key, value)
                      if self._v2g_bridge is not None else "NotSupported")
            return call_result.ChangeConfiguration(status=status)
        readonly_keys = {'LocalAuthListMaxLength', 'NumberOfConnectors', 'SupportedFeatureProfiles',
                         'ChargeProfileMaxStackLevel', 'ChargingScheduleAllowedChargingRateUnit',
                         'ChargingScheduleMaxPeriods', 'MaxChargingProfilesInstalled',
                         'ReserveConnectorZeroSupported', 'SupportedFileTransferProtocols',
                         'SendLocalListMaxLength', 'FirmwareUpdateMode', 'GetConfigurationMaxKeys'}
        if key in readonly_keys:
            return call_result.ChangeConfiguration(status='Rejected')
        configurable_keys = [
            'HeartbeatInterval', 'AuthorizationCacheEnabled',
            'LocalAuthListEnabled', 'AutoChargeEnabled', 'AutoChargeIdTagPrefix', 'StopTxnSampledData', 'MeterValuesSampledData',
            'AllowOfflineTxForUnknownId', 'AuthorizeRemoteTxRequests', 'LocalAuthorizeOffline',
            'LocalPreAuthorize', 'MeterValueSampleInterval', 'StopTransactionOnInvalidId',
            'TransactionMessageAttempts', 'TransactionMessageRetryInterval', 'WebSocketPingInterval',
        ]
        if key not in configurable_keys:
            return call_result.ChangeConfiguration(status='NotSupported')

        numeric_keys = {'HeartbeatInterval', 'MeterValueSampleInterval',
                        'TransactionMessageAttempts', 'TransactionMessageRetryInterval',
                        'WebSocketPingInterval'}
        boolean_keys = {'AuthorizationCacheEnabled', 'LocalAuthListEnabled', 'AutoChargeEnabled',
                        'AllowOfflineTxForUnknownId', 'AuthorizeRemoteTxRequests',
                        'LocalAuthorizeOffline', 'LocalPreAuthorize', 'StopTransactionOnInvalidId'}
        if key in numeric_keys:
            if isinstance(value, bool) or not isinstance(value, (str, int)):
                return call_result.ChangeConfiguration(status='Rejected')
            text = str(value).strip()
            if not text or any(char not in '0123456789' for char in text):
                return call_result.ChangeConfiguration(status='Rejected')
            number = int(text)
            minimum = 1 if key == 'HeartbeatInterval' else 0
            if not minimum <= number <= 2147483647:
                return call_result.ChangeConfiguration(status='Rejected')
            normalized = str(number)
        elif key in boolean_keys:
            if not isinstance(value, str) or value.strip().lower() not in ('true', 'false'):
                return call_result.ChangeConfiguration(status='Rejected')
            normalized = value.strip().lower()
        elif key == 'AutoChargeIdTagPrefix':
            normalized = _normalize_autocharge_prefix(value)
            if normalized is None:
                return call_result.ChangeConfiguration(status='Rejected')
        else:
            normalized = _normalize_csv_string(value, default='0')

        # Persist first. A rejected disk write must leave the runtime setting
        # unchanged; the previous PLC write helper swallowed that failure.
        try:
            _write_configuration_file(f'{key}_cfg.txt', normalized)
        except Exception:
            logging.error('OCPP configuration could not be persisted: %s', key)
            return call_result.ChangeConfiguration(status='Rejected')
        setattr(self, f'{key}_cfg', normalized)
        if key == 'AutoChargeEnabled' and normalized == 'true':
            try:
                self._seed_autocharge_events()
            except Exception:
                logging.exception('Could not seed retained AutoCharge events')
        return call_result.ChangeConfiguration(status='Accepted')

    # ========================================================================
    # Cache and Local Auth List Handlers
    # ========================================================================
    @on(Action.clear_cache)
    async def on_clear_cache(self, **kwargs):
        try:
            self.authorization_cache.clear()
            logging.info("ClearCache: authorization cache cleared.")
            return call_result.ClearCache(status="Accepted")
        except Exception as e:
            logging.exception(f"ClearCache failed: {e}")
            return call_result.ClearCache(status="Rejected")

    @on(Action.get_local_list_version)
    async def on_get_local_list_version(self, **kwargs):
        try:
            lv = int(getattr(self.local_auth_list, "list_version", 0) or 0)
            return call_result.GetLocalListVersion(list_version=lv)
        except Exception:
            return call_result.GetLocalListVersion(list_version=0)

    @on(Action.send_local_list)
    async def on_send_local_list(self, list_version=None, update_type=None,
                                 local_authorization_list=None, **kwargs):
        if list_version is None: list_version = kwargs.get("list_version")
        if update_type is None: update_type = kwargs.get("update_type")
        if local_authorization_list is None: local_authorization_list = kwargs.get("local_authorization_list")
        try:
            status = self.local_auth_list.apply_send_local_list(list_version, update_type, local_authorization_list)
            return call_result.SendLocalList(status=status)
        except Exception as e:
            logging.exception(f"SendLocalList failed: {e}")
            return call_result.SendLocalList(status="Failed")

    # ========================================================================
    # TriggerMessage & UnlockConnector
    # ========================================================================
    @on(Action.trigger_message)
    async def on_trigger_message(self, requested_message=None, connector_id=None, **kwargs):
        msg = requested_message or kwargs.get("requested_message")
        try:
            if hasattr(msg, "value"): msg = msg.value
        except: pass
        if msg is None: return call_result.TriggerMessage(status="Rejected")
        msg = str(msg)
        if connector_id is not None and (type(connector_id) is not int or connector_id not in (0, 1, 2)):
            return call_result.TriggerMessage(status="Rejected")
        supported = {"Heartbeat", "BootNotification", "StatusNotification", "MeterValues",
                     "DiagnosticsStatusNotification", "FirmwareStatusNotification"}
        if msg not in supported: return call_result.TriggerMessage(status="NotImplemented")
        asyncio.create_task(self._send_triggered_message(msg, connector_id))
        return call_result.TriggerMessage(status="Accepted")

    async def _send_triggered_message(self, msg: str, connector_id):
        await asyncio.sleep(0)
        try:
            if msg == "Heartbeat": await self.call(call.Heartbeat())
            elif msg == "BootNotification": await self.boot_notification_cp()
            elif msg == "StatusNotification":
                if connector_id is None:
                    await self.status_notification_cp(1)
                    await self.status_notification_cp(2)
                else:
                    await self.status_notification_cp(int(connector_id))
            elif msg == "MeterValues":
                cid = int(connector_id) if connector_id is not None else 1
                sender = getattr(self, "send_triggered_meter_values", self.send_meter_values)
                await sender(cid)
            elif msg in ("DiagnosticsStatusNotification", "FirmwareStatusNotification"):
                await self._ocpp_trigger_service_status("A", msg)
        except Exception as e:
            logging.exception(f"_send_triggered_message failed: {e}")

    @on(Action.unlock_connector)
    async def on_unlock_connector(self, connector_id=None, **kwargs):
        if connector_id is None: connector_id = kwargs.get("connector_id")
        try: cid = int(connector_id)
        except Exception: return call_result.UnlockConnector(status="UnlockFailed")
        if cid in (1, 2):
            logging.info(f"UnlockConnector: connector {cid} unlocked.")
            return call_result.UnlockConnector(status="Unlocked")
        return call_result.UnlockConnector(status="NotSupported")

    # ========================================================================
    # BootNotification & Network loss register
    # ========================================================================
    async def boot_notification_cp(self):
        request = call.BootNotification(
            charge_point_vendor="Flexxfast by EDS",
            charge_point_model="FD150A",
            firmware_version="V16072567a1_PLC_03122024",
            charge_point_serial_number="01272026",
        )
        response = await self.call(request)
        if response.status == 'Rejected':
            print('Reboot')
            os.system("sudo reboot")

    async def loss_network_regis(self, stable_states=None):
        print(f" transactionId C1={self.transactionId_c1}  C2={self.transactionId_c2}")
        try:
            iCpStateCss1, usiLinkStateCcs1 = plc.get_connector_status(self.myip_adr_c1)
            iCpStateCss2, usiLinkStateCcs2 = plc.get_connector_status(self.myip_adr_c2)
        except Exception as e:
            print(e)

        if self._dual_manager is not None and stable_states:
            iCpStateCss1 = stable_states.get(1) or iCpStateCss1
            iCpStateCss2 = stable_states.get(2) or iCpStateCss2

        ACTIVE_SESSION_STATES = ["Charging", "SuspendedEV", "SuspendedEVSE", "Preparing", "Finishing"]

        # ── RESET-GUARD: ถ้า reboot นี้เกิดจาก CSMS Reset → ห้าม resume txn เก่า ──
        # _reset_cleanup_and_reboot() จะเขียน flag นี้ไว้ก่อน reboot
        # หลัง boot กลับมา ถ้าเจอ flag = ถือว่า txn เก่าถูก stop ไปแล้ว (หรือควรถือว่าจบแล้ว
        # แม้ network ล่มกลางคัน) จะไม่ resume และเคลียร์ state ทุกอย่างทิ้ง
        _reset_pending = os.path.exists("reset_pending.flag")
        if _reset_pending:
            logging.info("[INIT] reset_pending.flag detected — discarding any leftover transactions")
            try:
                os.remove("reset_pending.flag")
            except Exception as e:
                logging.warning(f"[INIT] cannot remove reset_pending.flag: {e}")

        for connector_id, iCpState, txn_attr, meter_sw_attr, sent_attr, sent_sw_attr, \
            stop_attr, cp_attr, usi_attr, blk_sw1, blk_sw2, blk_sw3, blk_sw1_name, \
            blk_sw2_name, blk_sw3_name, trans_file in [
            (1, iCpStateCss1, 'transactionId_c1', 'start_sendMeterValue_SW1',
             'finshing_sent_c1', 'finshing_sent_sw_c1', 'my_remote_stop_c1',
             'iCpStateCss1', 'usiLinkStateCcs1', 'block_sw1_C1', 'block_sw2_C1', 'block_sw3_C1',
             'block_sw1_C1.txt', 'block_sw2_C1.txt', 'block_sw3_C1.txt', 'translogs1.txt'),
            (2, iCpStateCss2, 'transactionId_c2', 'start_sendMeterValue_SW2',
             'finshing_sent_c2', 'finshing_sent_sw_c2', 'my_remote_stop_c2',
             'iCpStateCss2', 'usiLinkStateCcs2', 'block_sw1', 'block_sw2', 'block_sw3',
             'block_sw1.txt', 'block_sw2.txt', 'block_sw3.txt', 'translogs2.txt'),
        ]:
            try:
                txn = getattr(self, txn_attr)
                # ── RESET-PENDING: force reset block_sw flags ทุก connector ──
                # ไม่ว่าจะมี txn ค้างหรือไม่ เพราะ _clear_connector_state ใน Reset
                # cleanup ตั้ง block_sw1=0, translogs=0 ก่อน reboot ทำให้ branch
                # `if txn > 0` ไม่ถูก trigger → block_sw1 ค้างที่ 0 → main loop
                # ไม่ส่ง StatusNotification(Available) เมื่อ user ถอดสาย
                # ที่ถูก: block_sw1=block_sw2=block_sw3=1 → พร้อมส่งทุก state
                if _reset_pending:
                    if txn is not None and txn > 0:
                        logging.info(f"[INIT] Discarding transaction {txn} on C{connector_id} (reset_pending)")
                        setattr(self, txn_attr, 0)
                        _write_file(trans_file, 0)
                    else:
                        logging.info(f"[INIT] C{connector_id} reset_pending — re-arming block flags")
                    # block_sw1=1 → พร้อมส่ง Available (เมื่อสายถูกถอด)
                    # block_sw2=1 → พร้อมส่ง Preparing (เมื่อเสียบสายใหม่)
                    # block_sw3=1 → พร้อมส่ง Charging (เมื่อเริ่ม session ใหม่)
                    setattr(self, blk_sw1, 1)
                    setattr(self, blk_sw2, 1)
                    setattr(self, blk_sw3, 1)
                    _write_file(blk_sw1_name, 1)
                    _write_file(blk_sw2_name, 1)
                    _write_file(blk_sw3_name, 1)
                elif txn is not None and txn > 0:
                    if iCpState == "Available":
                        logging.info(f"[INIT] Orphaned transaction {txn} on C{connector_id} — stopping")
                        setattr(self, meter_sw_attr, 2)
                        await self.send_meter_values(connector_id)
                        await self.send_stop_transaction(connector_id, reasonf='PowerLoss')
                    elif iCpState in ACTIVE_SESSION_STATES:
                        logging.info(f"[INIT] Resuming active transaction {txn} on C{connector_id}")
                        setattr(self, meter_sw_attr, 2)
                        _write_file(f"start_sendMeterValue_SW{connector_id}.txt", 2)
                        setattr(self, sent_attr, 0)
                        _write_file(f"finshing_sent_c{connector_id}.txt", 0)
                        setattr(self, stop_attr, 0)
                        _write_file(f"my_remote_stop_c{connector_id}.txt", 0)
                        setattr(self, sent_sw_attr, 1)
                        _write_file(f"finshing_sent_sw_c{connector_id}.txt", 1)
                        is_charging = (iCpState == "Charging")
                        setattr(self, blk_sw1, 1)
                        setattr(self, blk_sw2, 1)
                        setattr(self, blk_sw3, 0 if is_charging else 1)
                        _write_file(blk_sw1_name, 1)
                        _write_file(blk_sw2_name, 1)
                        _write_file(blk_sw3_name, 0 if is_charging else 1)
            except Exception as e:
                logging.error(f"[INIT] Error handling transaction C{connector_id}: {e}")

    # ========================================================================
    # StatusNotification helpers
    # ========================================================================
    def _active_transaction_id(self, connector_id):
        """Return active OCPP transaction id from memory or persistent file."""
        if connector_id == 1:
            mem_txn = self.transactionId_c1
            trans_file = "translogs1.txt"
        elif connector_id == 2:
            mem_txn = self.transactionId_c2
            trans_file = "translogs2.txt"
        else:
            return 0

        try:
            mem_txn = int(mem_txn or 0)
        except Exception:
            mem_txn = 0

        try:
            file_txn = int(open(trans_file).read().strip() or "0") if os.path.exists(trans_file) else 0
        except Exception:
            file_txn = 0

        return max(mem_txn, file_txn)

    @staticmethod
    def _valid_id_tag(value):
        text = str(value or "").strip()
        return bool(text and text.lower() not in ("0", "none", "null", "default"))

    @staticmethod
    def _read_runtime_text(path, default=""):
        try:
            if not os.path.exists(path):
                return str(default)
            return open(path, "r").read().strip() or str(default)
        except Exception:
            return str(default)

    def _authorization_timestamp(self, id_tag):
        tag = str(id_tag or "").strip()
        if not tag:
            return 0.0
        ts = self.auth_timestamps.get(tag, 0)
        if not ts:
            tag_lower = tag.lower()
            for known_tag, known_ts in self.auth_timestamps.items():
                if str(known_tag).strip().lower() == tag_lower:
                    ts = known_ts
                    break
        try:
            return float(ts or 0.0)
        except Exception:
            return 0.0

    def _recently_authorized(self, id_tag, window=300.0):
        """Return True only for a fresh Authorize call made by the RFID handler."""
        ts = self._authorization_timestamp(id_tag)
        try:
            age = time.time() - ts
            return ts > 0.0 and 0.0 <= age <= float(window)
        except Exception:
            return False

    def _rfid_session_authorized(self, connector_id):
        if (not getattr(self, "allow_rfid_command_start", False) and
                not self._autocharge_session_armed(connector_id)):
            return False
        if connector_id == 1:
            latch = self.rfid_start_received_c1
            tag = self.active_id_tag_c1 if self._valid_id_tag(self.active_id_tag_c1) else self.accepted_tag1
        elif connector_id == 2:
            latch = self.rfid_start_received_c2
            tag = self.active_id_tag_c2 if self._valid_id_tag(self.active_id_tag_c2) else self.accepted_tag2
        else:
            return False
        return int(latch or 0) == 1 and self._valid_id_tag(tag)

    def _remote_stop_worker_running(self, connector_id):
        task = self._remote_stop_task_c1 if connector_id == 1 else self._remote_stop_task_c2
        return task is not None and not task.done()

    def _clear_stale_stop_latch_for_new_start(self, connector_id, source):
        """Release a persisted RemoteStop/Finishing latch before a new explicit start.

        A phase-1/phase-2 latch can survive a process or WebSocket restart. If the
        old OCPP transaction is already gone and there is no remote-stop worker in
        this process, that latch is stale. Leaving it set makes the status handler
        return before StartTransaction, while the PLC can already be delivering
        power. That is the exact condition that suppresses MeterValues.

        [SESSION-FINISHING] It deliberately does NOT touch session_finishing_cX.
        That latch is the OCPP reported status, not a block switch, and none of the
        switches released here gate on it. This runs from the armed branch of
        _sync_rfid_command_latch() and from the pending_start branch of
        _handle_status_notification_c1/_c2 - both at the 0.2s main-loop rate - so
        while an RFID start sits armed it used to zero the latch AND persist the 0
        five times a second. A session that ended with the cable still in would then
        report Preparing, and the persisted 0 made the next boot repeat it.
        Arming a start is not the end of the Finishing state. The latch has exactly
        two correct releases, both in _normalize_ocpp_status(): a genuinely NEW
        transaction id appears, or the PLC reports the cable is out.
        """
        if connector_id not in (1, 2):
            return False
        if self._active_transaction_id(connector_id) > 0:
            return False
        if self._remote_stop_worker_running(connector_id):
            return False

        if connector_id == 1:
            phase = int(self.remote_stop_finishing_c1 or 0)
            old_remote = int(self.my_remote_stop_c1 or 0)
            changed = (
                phase != 0 or old_remote != 0 or int(self.finshing_sent_c1 or 0) != 0 or
                int(self.finshing_sent_sw_c1 or 0) != 1 or self.start_sendMeterValue_SW1 != 1 or
                int(self.block1 or 0) != 0 or self.block_sw1_C1 != 1 or self.block_sw3_C1 != 1
            )
            self.remote_stop_finishing_c1 = 0
            self.my_remote_stop_c1 = 0
            self.finshing_sent_c1 = 0
            self.finshing_sent_sw_c1 = 1
            self.start_sendMeterValue_SW1 = 1
            self.block1 = 0
            self.block_sw1_C1 = 1
            self.block_sw3_C1 = 1
            files = {
                "remote_stop_finishing_c1.txt": 0,
                "my_remote_stop_c1.txt": 0,
                "finshing_sent_c1.txt": 0,
                "finshing_sent_sw_c1.txt": 1,
                "start_sendMeterValue_SW1.txt": 1,
                "block1.txt": 0,
                "block_sw1_C1.txt": 1,
                "block_sw3_C1.txt": 1,
            }
        else:
            phase = int(self.remote_stop_finishing_c2 or 0)
            old_remote = int(self.my_remote_stop_c2 or 0)
            changed = (
                phase != 0 or old_remote != 0 or int(self.finshing_sent_c2 or 0) != 0 or
                int(self.finshing_sent_sw_c2 or 0) != 1 or self.start_sendMeterValue_SW2 != 1 or
                int(self.block2 or 0) != 0 or self.block_sw1 != 1 or self.block_sw3 != 1
            )
            self.remote_stop_finishing_c2 = 0
            self.my_remote_stop_c2 = 0
            self.finshing_sent_c2 = 0
            self.finshing_sent_sw_c2 = 1
            self.start_sendMeterValue_SW2 = 1
            self.block2 = 0
            self.block_sw1 = 1
            self.block_sw3 = 1
            files = {
                "remote_stop_finishing_c2.txt": 0,
                "my_remote_stop_c2.txt": 0,
                "finshing_sent_c2.txt": 0,
                "finshing_sent_sw_c2.txt": 1,
                "start_sendMeterValue_SW2.txt": 1,
                "block2.txt": 0,
                "block_sw1.txt": 1,
                "block_sw3.txt": 1,
            }

        if changed:
            for file_name, value in files.items():
                _write_file(file_name, value)
        return True

    def _repair_authorized_start_switches(self, connector_id, source):
        """Make an authorized pending start independent of stale block files."""
        if self._active_transaction_id(connector_id) > 0:
            return
        if connector_id == 1:
            changed = (
                self.start_transaction_SW1 != 1 or self.block_sw3_C1 != 1 or
                self.start_sendMeterValue_SW1 != 1 or int(self.finshing_sent_c1 or 0) != 0 or
                int(self.finshing_sent_sw_c1 or 0) != 1 or self.block_sw1_C1 != 1
            )
            self.start_transaction_SW1 = 1
            self.start_sendMeterValue_SW1 = 1
            self.finshing_sent_c1 = 0
            self.finshing_sent_sw_c1 = 1
            self.block_sw1_C1 = 1
            self.block_sw3_C1 = 1
            files = {
                "start_transaction_SW1.txt": 1,
                "start_sendMeterValue_SW1.txt": 1,
                "finshing_sent_c1.txt": 0,
                "finshing_sent_sw_c1.txt": 1,
                "block_sw1_C1.txt": 1,
                "block_sw3_C1.txt": 1,
            }
        else:
            changed = (
                self.start_transaction_SW2 != 1 or self.block_sw3 != 1 or
                self.start_sendMeterValue_SW2 != 1 or int(self.finshing_sent_c2 or 0) != 0 or
                int(self.finshing_sent_sw_c2 or 0) != 1 or self.block_sw1 != 1
            )
            self.start_transaction_SW2 = 1
            self.start_sendMeterValue_SW2 = 1
            self.finshing_sent_c2 = 0
            self.finshing_sent_sw_c2 = 1
            self.block_sw1 = 1
            self.block_sw3 = 1
            files = {
                "start_transaction_SW2.txt": 1,
                "start_sendMeterValue_SW2.txt": 1,
                "finshing_sent_c2.txt": 0,
                "finshing_sent_sw_c2.txt": 1,
                "block_sw1.txt": 1,
                "block_sw3.txt": 1,
            }
        if changed:
            for file_name, value in files.items():
                _write_file(file_name, value)

    def _sync_rfid_command_latch(self, connector_id, plc_state):
        """Bridge helpf.plc_function.handle_rfid_command() into the CP state machine.

        The MQTT RFID callback already performs Authorize and starts the PLC. Older
        CP code, however, still required remote_start_received=1 and therefore never
        sent Charging/StartTransaction/MeterValues for that local session. This
        method detects the freshly authorized accepted_tag and arms the local OCPP
        StartTransaction path without weakening RemoteStartOnly for other sources.
        """
        if not getattr(self, "allow_rfid_command_start", False):
            return False

        if connector_id == 1:
            tag_attr = "accepted_tag1"
            latch_attr = "rfid_start_received_c1"
            active_tag_attr = "active_id_tag_c1"
            auth_tag_attr = "myid_tag_ath_c1"
            remote_attr = "remote_start_received_c1"
            my_remote_start_attr = "myremotestart_c1"
            start_sw_attr = "start_transaction_SW1"
            meter_sw_attr = "start_sendMeterValue_SW1"
            finishing_sw_attr = "finshing_sent_sw_c1"
            block_reason_attr = "block1"
            block_sw1_attr, block_sw3_attr = "block_sw1_C1", "block_sw3_C1"
            tag_file = "accepted_tag1.txt"
            latch_file = "rfid_start_received_c1.txt"
            active_tag_file = "active_id_tag_c1.txt"
            auth_tag_file = "myid_tag_ath_c1.txt"
            start_sw_file = "start_transaction_SW1.txt"
            meter_sw_file = "start_sendMeterValue_SW1.txt"
            finishing_sw_file = "finshing_sent_sw_c1.txt"
            block_reason_file = "block1.txt"
            block_sw1_file, block_sw3_file = "block_sw1_C1.txt", "block_sw3_C1.txt"
            file_check_attr = "_rfid_file_check_ts_c1"
            consumed_auth_attr = "_rfid_consumed_auth_ts_c1"
        elif connector_id == 2:
            tag_attr = "accepted_tag2"
            latch_attr = "rfid_start_received_c2"
            active_tag_attr = "active_id_tag_c2"
            auth_tag_attr = "myid_tag_ath_c2"
            remote_attr = "remote_start_received_c2"
            my_remote_start_attr = "myremotestart_c2"
            start_sw_attr = "start_transaction_SW2"
            meter_sw_attr = "start_sendMeterValue_SW2"
            finishing_sw_attr = "finshing_sent_sw_c2"
            block_reason_attr = "block2"
            block_sw1_attr, block_sw3_attr = "block_sw1", "block_sw3"
            tag_file = "accepted_tag2.txt"
            latch_file = "rfid_start_received_c2.txt"
            active_tag_file = "active_id_tag_c2.txt"
            auth_tag_file = "myid_tag_ath_c2.txt"
            start_sw_file = "start_transaction_SW2.txt"
            meter_sw_file = "start_sendMeterValue_SW2.txt"
            finishing_sw_file = "finshing_sent_sw_c2.txt"
            block_reason_file = "block2.txt"
            block_sw1_file, block_sw3_file = "block_sw1.txt", "block_sw3.txt"
            file_check_attr = "_rfid_file_check_ts_c2"
            consumed_auth_attr = "_rfid_consumed_auth_ts_c2"
        else:
            return False

        # Never replace an OCPP RemoteStart that is already being processed.
        if (int(getattr(self, remote_attr, 0) or 0) == 1 or
                int(getattr(self, my_remote_start_attr, 0) or 0) == 1 or
                int(getattr(self, start_sw_attr, 0) or 0) == 3):
            return False

        if self._active_transaction_id(connector_id) > 0:
            return int(getattr(self, latch_attr, 0) or 0) == 1

        tag = str(getattr(self, tag_attr, "0") or "0").strip()
        if not self._valid_id_tag(tag):
            active_tag = str(getattr(self, active_tag_attr, "Default") or "Default").strip()
            if self._valid_id_tag(active_tag):
                tag = active_tag
                setattr(self, tag_attr, tag)
        now = time.time()
        # The callback normally updates the CP object directly. The throttled file
        # fallback also supports older plc_function versions that only persist it.
        if not self._valid_id_tag(tag):
            last_check = float(getattr(self, file_check_attr, 0.0) or 0.0)
            if now - last_check >= 0.5:
                setattr(self, file_check_attr, now)
                file_tag = self._read_runtime_text(tag_file, "0")
                if self._valid_id_tag(file_tag):
                    tag = file_tag
                    setattr(self, tag_attr, tag)

        if not self._valid_id_tag(tag):
            return int(getattr(self, latch_attr, 0) or 0) == 1

        # A persisted/already-armed RFID start must be self-healing. This covers
        # both a stale block_sw3 file and a reconnect that happened between PLC
        # start and StartTransaction.
        if int(getattr(self, latch_attr, 0) or 0) == 1:
            self._clear_stale_stop_latch_for_new_start(connector_id, "RFIDCommand")
            self._repair_authorized_start_switches(connector_id, "RFIDCommand")
            metadata_changed = (
                str(getattr(self, active_tag_attr, "")) != tag or
                str(getattr(self, auth_tag_attr, "")) != tag or
                int(getattr(self, remote_attr, 0) or 0) != 0 or
                int(getattr(self, block_reason_attr, 0) or 0) != 0
            )
            setattr(self, active_tag_attr, tag)
            setattr(self, auth_tag_attr, tag)
            setattr(self, remote_attr, 0)
            setattr(self, block_reason_attr, 0)
            if metadata_changed:
                _write_file(active_tag_file, tag)
                _write_file(auth_tag_file, tag)
                _write_file(block_reason_file, 0)
            if connector_id == 1 and self.rfid_recovery_pending_c1:
                logging.warning(f"[RFID-RECOVERY-V9] C1 resuming pending local start idTag={tag}")
                self.rfid_recovery_pending_c1 = 0
            elif connector_id == 2 and self.rfid_recovery_pending_c2:
                logging.warning(f"[RFID-RECOVERY-V9] C2 resuming pending local start idTag={tag}")
                self.rfid_recovery_pending_c2 = 0
            return True

        # A fresh, not-yet-consumed Authorize call is the anti-stale guard for
        # accepted_tag*.txt.  This also prevents the old tag file from re-arming
        # immediately after StopTransaction while the RFID callback is clearing it.
        if int(getattr(self, latch_attr, 0) or 0) != 1:
            auth_ts = self._authorization_timestamp(tag)
            consumed_ts = float(getattr(self, consumed_auth_attr, 0.0) or 0.0)
            if not self._recently_authorized(tag) or auth_ts <= consumed_ts:
                return False
            if str(plc_state or "") not in (
                    "Preparing", "Charging", "SuspendedEV", "SuspendedEVSE"):
                return False

            self._clear_stale_stop_latch_for_new_start(connector_id, "RFIDCommand")
            setattr(self, f"_pin_autocharge_start_intent_c{connector_id}", "")
            setattr(self, consumed_auth_attr, auth_ts)
            setattr(self, latch_attr, 1)
            setattr(self, active_tag_attr, tag)
            setattr(self, auth_tag_attr, tag)
            setattr(self, start_sw_attr, 1)       # select start_transaction_ath()
            setattr(self, meter_sw_attr, 1)
            setattr(self, finishing_sw_attr, 1)
            setattr(self, remote_attr, 0)
            setattr(self, block_reason_attr, 0)   # StopTransaction reason = Local
            setattr(self, block_sw1_attr, 1)
            setattr(self, block_sw3_attr, 1)      # re-arm Charging transition

            _write_file(latch_file, 1)
            _write_file(active_tag_file, tag)
            _write_file(auth_tag_file, tag)
            _write_file(start_sw_file, 1)
            _write_file(meter_sw_file, 1)
            _write_file(finishing_sw_file, 1)
            _write_file(block_reason_file, 0)
            _write_file(block_sw1_file, 1)
            _write_file(block_sw3_file, 1)

        return True

    # ========================================================================
    # [LOCAL-STATUS-V16] Screen/HMI-initiated session (status only, no transaction)
    # ========================================================================
    def _local_status_session_active(self, connector_id):
        """session ที่เริ่มจากหน้าจอกำลังทำงานอยู่หรือไม่"""
        if not getattr(self, "allow_local_start", False):
            return False
        if connector_id == 1:
            return int(getattr(self, "local_status_session_c1", 0) or 0) == 1
        if connector_id == 2:
            return int(getattr(self, "local_status_session_c2", 0) or 0) == 1
        return False

    def _mark_pending_local_stop(self, connector_id):
        """Remember that the next PLC Finishing event came from a local user."""
        if connector_id in (1, 2):
            setattr(self, f"_pending_local_stop_ts_c{connector_id}", time.monotonic())

    def _clear_pending_local_stop(self, connector_id):
        if connector_id in (1, 2):
            setattr(self, f"_pending_local_stop_ts_c{connector_id}", 0.0)

    def _pending_local_stop(self, connector_id):
        """Return True only for a recent RFID/HMI stop request."""
        if connector_id not in (1, 2):
            return False
        ts = float(getattr(self, f"_pending_local_stop_ts_c{connector_id}", 0.0) or 0.0)
        if ts <= 0.0:
            return False
        if time.monotonic() - ts <= LOCAL_STOP_REASON_TTL_SECONDS:
            return True
        self._clear_pending_local_stop(connector_id)
        logging.info(
            "[STOP-REASON] C%s expired stale Local stop marker; next PLC finish is EVDisconnected",
            connector_id,
        )
        return False

    def _finishing_stop_reason(self, connector_id):
        """Map the source of a PLC Finishing event to an OCPP 1.6 Reason."""
        block = self.block1 if connector_id == 1 else self.block2
        if int(block or 0) == 1:
            return "Remote"
        if self._pending_local_stop(connector_id):
            return "Local"
        return "EVDisconnected"

    @staticmethod
    def _is_ev_shutdown_state(value):
        normalized = "".join(ch for ch in str(value or "").lower() if ch.isalnum())
        return "weldingdetection" in normalized or "sessionstop" in normalized

    def _arm_ev_stop_transaction_guard(self, connector_id, transaction_id):
        """Bind future shutdown observations to this exact transaction generation."""
        if connector_id not in (1, 2):
            return
        setattr(self, f"_ev_stop_guard_txn_c{connector_id}", int(transaction_id or 0))
        setattr(self, f"_ev_stop_guard_since_c{connector_id}", time.monotonic())
        setattr(self, f"_ev_stop_claimed_txn_c{connector_id}", 0)
        setattr(self, f"_ev_stop_retry_txn_c{connector_id}", 0)

    def _ev_stop_block_reason(self, connector_id, transaction_id):
        """Return why an established stop path must retain ownership, if any."""
        claimed = int(getattr(self, f"_ev_stop_claimed_txn_c{connector_id}", 0) or 0)
        if claimed == transaction_id:
            return "duplicate EV shutdown event"
        if self._remote_stop_worker_running(connector_id):
            return "RemoteStop worker running"
        if int(getattr(self, f"remote_stop_finishing_c{connector_id}", 0) or 0) in (1, 2):
            return "RemoteStop phase active"
        if int(getattr(self, f"my_remote_stop_c{connector_id}", 0) or 0) == 1:
            return "existing local/RFID/PIN stop pending"
        if self._pending_local_stop(connector_id):
            return "local/RFID stop origin pending"
        if int(getattr(self, f"finshing_sent_c{connector_id}", 0) or 0) == 1:
            return "legacy PLC Finishing handler owns stop"
        retry_txn = int(getattr(self, f"_ev_stop_retry_txn_c{connector_id}", 0) or 0)
        if self._session_finishing_active(connector_id) and retry_txn != transaction_id:
            return "session already finishing"
        if str(getattr(self, f"emergency_sent_c{connector_id}", "1")) != "1":
            return "emergency stop active"
        block = self.block1 if connector_id == 1 else self.block2
        if int(block or 0) in (1, 3):
            return "remote/emergency stop origin active"
        bridge = getattr(self, "vector_fault_bridge", None)
        if bridge is not None and bridge.has_active(connector_id):
            return "Vector fault active"
        return ""

    async def _handle_ev_stop_event(self, event):
        """Close one active transaction proven to be stopped by the EV."""
        try:
            connector_id = int(event.get("connector_id") or 0)
            received_at = float(event.get("received_at") or 0.0)
        except (AttributeError, TypeError, ValueError):
            return False
        if connector_id not in (1, 2):
            return False

        age = time.monotonic() - received_at
        if received_at <= 0.0 or age < 0.0 or age > EV_STOP_EVENT_MAX_AGE_SECONDS:
            logging.info("[EV-STOP] C%s ignored stale %s event age=%.1fs",
                         connector_id, event.get("detail", "unknown"), max(age, 0.0))
            return False

        # The callback itself received a fresh, non-retained shutdown edge. Do not
        # re-read the MQTT cache here: WeldingDetection/SessionStop are transient
        # and field controllers may already have published an empty/next state by
        # the time the asyncio loop drains this queue. Freshness and the transaction
        # generation boundary below prevent replay into a later transaction.

        transaction_id = self._active_transaction_id(connector_id)
        if transaction_id <= 0:
            logging.info("[EV-STOP] C%s ignored %s: no active transaction",
                         connector_id, event.get("detail", "shutdown"))
            return False

        guard_txn = int(getattr(self, f"_ev_stop_guard_txn_c{connector_id}", 0) or 0)
        guard_since = float(getattr(self, f"_ev_stop_guard_since_c{connector_id}", 0.0) or 0.0)
        if guard_txn != transaction_id:
            # This can occur when a transaction was restored or opened by a path
            # unknown to an older build. Establish the generation boundary now and
            # require a later shutdown observation rather than risking the new txn.
            self._arm_ev_stop_transaction_guard(connector_id, transaction_id)
            logging.info("[EV-STOP] C%s txn=%s ignored %s: transaction generation changed",
                         connector_id, transaction_id, event.get("detail", "shutdown"))
            return False
        if received_at < guard_since:
            logging.info("[EV-STOP] C%s txn=%s ignored %s: event predates transaction",
                         connector_id, transaction_id, event.get("detail", "shutdown"))
            return False

        blocker = self._ev_stop_block_reason(connector_id, transaction_id)
        if blocker:
            logging.info("[EV-STOP] C%s txn=%s ignored %s: %s",
                         connector_id, transaction_id,
                         event.get("detail", "shutdown"), blocker)
            return False

        claim_attr = f"_ev_stop_claimed_txn_c{connector_id}"
        setattr(self, claim_attr, transaction_id)  # no await before ownership is recorded
        logging.warning(
            "[EV-STOP] C%s txn=%s source=%s detail=%s -> "
            "StopTransaction(reason=EVDisconnected)",
            connector_id, transaction_id, event.get("source"), event.get("detail"),
        )
        try:
            # The established sender owns final meter reading, retry, persistence,
            # transaction cleanup and the finishing latch. The EV is already shutting
            # down, therefore this route deliberately sends no PLC stop pulse.
            await self.send_stop_transaction(connector_id, reasonf="EVDisconnected")
            await self.status_notification_usiLink(connector_id)

            setattr(self, f"finshing_sent_sw_c{connector_id}", 2)
            _write_file(f"finshing_sent_sw_c{connector_id}.txt", 2)
            if connector_id == 1:
                self.block_sw1_C1 = 1
                _write_file("block_sw1_C1.txt", 1)
            else:
                self.block_sw1 = 1
                _write_file("block_sw1.txt", 1)
            setattr(self, f"_ev_stop_retry_txn_c{connector_id}", 0)
            logging.info("[EV-STOP] C%s txn=%s completed; Finishing reported",
                         connector_id, transaction_id)
            return True
        except Exception as error:
            # send_stop_transaction keeps the transaction persisted on failure.
            # Release only that failed claim so the later WeldingDetection or
            # SessionStop observation can retry without duplicating a successful stop.
            if self._active_transaction_id(connector_id) == transaction_id:
                setattr(self, claim_attr, 0)
                setattr(self, f"_ev_stop_retry_txn_c{connector_id}", transaction_id)
            logging.exception("[EV-STOP] C%s txn=%s failed: %s",
                              connector_id, transaction_id, error)
            return False

    async def _drain_ev_stop_events(self):
        """Move queued paho-thread observations into the OCPP state machine."""
        while True:
            try:
                event = EV_STOP_EVENTS.get_nowait()
            except queue.Empty:
                return
            await self._handle_ev_stop_event(event)

    def _set_local_status_session(self, connector_id, value):
        """ตั้ง latch + persist (pattern เดียวกับ _set_session_finishing)"""
        if connector_id not in (1, 2):
            return
        value = 1 if int(value or 0) == 1 else 0
        attr = f"local_status_session_c{connector_id}"
        if int(getattr(self, attr, 0) or 0) == value:
            return
        setattr(self, attr, value)
        _write_file(f"local_status_session_c{connector_id}.txt", value)

    def _sync_local_status_latch(self, connector_id, plc_state):
        """Bridge topic/RemoteStart{n} / topic/RemoteStop{n} (กดจากหน้าจอ) เข้า state machine.

        หน้าจอ/Node-RED publish topic เหล่านี้ตรง ๆ เพื่อสั่งฮาร์ดแวร์ แต่เดิม
        CP.py ไม่เคย subscribe เลย (write-only) ทำให้ session ที่เริ่มจากหน้าจอ
        มองไม่เห็นในฝั่ง OCPP: RemoteStartOnly ปิดประตู Charging ไว้ CSMS จึงค้าง
        ที่ Preparing ขณะรถชาร์จอยู่จริง

        callback ใน plc_function อยู่บน paho thread ตั้งแค่ pending flag;
        ฟังก์ชันนี้ถูกเรียกจาก main_run_program ทุก 0.2 วิ เป็นผู้ตัดสินใจจริง
        (โครงสร้างลอกจาก _sync_rfid_command_latch)

        session แบบนี้เป็น "status only" จึง **ไม่แตะ** start_transaction_SWX /
        start_sendMeterValue_SWX / accepted_tagX / myid_tag_* เลย
        ไม่มี StartTransaction ไม่มี MeterValues

        คืนค่า: "stop" = ผู้เรียกต้องส่ง Finishing, True/False = สถานะ latch
        """
        if not getattr(self, "allow_local_start", False):
            return False
        if connector_id not in (1, 2):
            return False

        # ---- STOP มาก่อนเสมอ: ถ้ากด Stop ในรอบเดียวกันต้องชนะ ----
        stop_ts = take_local_request("stop", connector_id)
        if stop_ts > 0.0:
            take_local_request("stop", connector_id, consume=True)
            if self._active_transaction_id(connector_id) > 0:
                self._mark_pending_local_stop(connector_id)
                # มี transaction จริงอยู่ (RemoteStart/RFID) -> เจ้าของคือ
                # _handle_remote_stop_cX / handle_rfid_command() ห้ามแย่ง
                logging.info(
                    f"[LOCAL-STATUS-V16] C{connector_id} RemoteStop edge ระหว่างมี "
                    "transaction — ปล่อยให้เส้นทางเดิมจัดการ"
                )
            elif self._local_status_session_active(connector_id):
                logging.warning(
                    f"[LOCAL-STATUS-V16] C{connector_id} STOP จากหน้าจอ -> Finishing"
                )
                self._set_local_status_session(connector_id, 0)
                clear_local_request("start", connector_id)
                return "stop"
            return self._local_status_session_active(connector_id)

        # ---- ห้ามแย่ง start ที่มาจากแหล่งอื่นที่กำลังทำงานอยู่ ----
        # (echo guard ชั้นสอง: ชั้นแรกคือ timestamp ใน plc_function ซึ่งกันเสียง
        #  สะท้อนจาก publish ของเราเองไปแล้ว ชั้นนี้กันเวลาเหลื่อม/พัลส์ยาวผิดปกติ)
        if connector_id == 1:
            busy = (int(self.remote_start_received_c1 or 0) == 1 or
                    int(self.myremotestart_c1 or 0) == 1 or
                    int(self.start_transaction_SW1 or 0) == 3 or
                    int(self.remote_stop_finishing_c1 or 0) in (1, 2))
        else:
            busy = (int(self.remote_start_received_c2 or 0) == 1 or
                    int(self.myremotestart_c2 or 0) == 1 or
                    int(self.start_transaction_SW2 or 0) == 3 or
                    int(self.remote_stop_finishing_c2 or 0) in (1, 2))
        if ((self._dual_manager is not None and self._dual_manager.connector_reserved(connector_id)) or
                busy or self._rfid_session_authorized(connector_id) or
                self._active_transaction_id(connector_id) > 0 or
                self._remote_stop_worker_running(connector_id)):
            clear_local_request("start", connector_id)
            return self._local_status_session_active(connector_id)

        if self._local_status_session_active(connector_id):
            return True                       # arm อยู่แล้ว

        start_ts = take_local_request("start", connector_id)
        if start_ts <= 0.0:
            return False

        consumed_attr = f"_local_status_consumed_ts_c{connector_id}"
        if start_ts <= float(getattr(self, consumed_attr, 0.0) or 0.0):
            return False

        # ต้องเป็นสถานะที่รถเสียบอยู่จริง — เกณฑ์เดียวกับ _sync_rfid_command_latch
        if str(plc_state or "") not in (
                "Preparing", "Charging", "SuspendedEV", "SuspendedEVSE"):
            now = time.time()
            log_attr = f"_local_status_log_ts_c{connector_id}"
            if now - float(getattr(self, log_attr, 0.0) or 0.0) >= 5.0:
                setattr(self, log_attr, now)
                logging.info(
                    f"[LOCAL-STATUS-V16] C{connector_id} พักคำขอไว้: "
                    f"PLC={plc_state} ยังไม่ใช่สถานะเสียบสาย"
                )
            return False                      # ค้างไว้ retry จนกว่าจะหมด TTL

        take_local_request("start", connector_id, consume=True)
        setattr(self, consumed_attr, start_ts)
        self._clear_stale_stop_latch_for_new_start(connector_id, "LocalStart")
        self._set_local_status_session(connector_id, 1)
        logging.warning(
            f"[LOCAL-STATUS-V16] C{connector_id} ARMED จากหน้าจอ: PLC={plc_state} "
            "(status-only, รอไฟไหลจริงก่อนส่ง Charging)"
        )
        return True

    @staticmethod
    def _corrected_power_kw(raw_power):
        """Remove the meter zero-offset without turning a real zero into 0.15 kW."""
        try:
            return round(max(0.0, abs(float(raw_power or 0.0)) - METER_POWER_OFFSET_KW), 2)
        except (TypeError, ValueError):
            return 0.0

    def _remember_reported_status(self, connector_id, status):
        status = str(status)
        if connector_id in (1, 2):
            attr = f'_reported_status_seq_c{connector_id}'
            setattr(self, attr, int(getattr(self, attr, 0)) + 1)
        if connector_id == 1:
            self._last_ocpp_status_c1 = status
            if status in ('Available', 'Preparing', 'Finishing', 'Faulted'):
                self._no_energy_since_c1 = 0.0
        elif connector_id == 2:
            self._last_ocpp_status_c2 = status
            if status in ('Available', 'Preparing', 'Finishing', 'Faulted'):
                self._no_energy_since_c2 = 0.0

    def _last_reported_status(self, connector_id):
        if connector_id == 1:
            return self._last_ocpp_status_c1
        if connector_id == 2:
            return self._last_ocpp_status_c2
        return None

    async def _read_energy_state(self, connector_id, force=False):
        """Return a throttled physical energy snapshot for one connector.

        A valid snapshot is used to decide between Charging and SuspendedEV.
        On a short meter read failure we reuse the most recent successful value;
        after ENERGY_STATE_STALE_AFTER_S we fall back to the raw PLC state.
        """
        if connector_id == 1:
            meter_ip = self.my_meter_ip_c1
            check_ts_attr = '_energy_state_check_ts_c1'
            success_ts_attr = '_energy_state_success_ts_c1'
            valid_attr = '_energy_state_valid_c1'
            flow_attr = '_energy_flow_c1'
            current_attr = '_energy_current_c1'
            power_attr = '_energy_power_c1'
        elif connector_id == 2:
            meter_ip = self.my_meter_ip_c2
            check_ts_attr = '_energy_state_check_ts_c2'
            success_ts_attr = '_energy_state_success_ts_c2'
            valid_attr = '_energy_state_valid_c2'
            flow_attr = '_energy_flow_c2'
            current_attr = '_energy_current_c2'
            power_attr = '_energy_power_c2'
        else:
            return {'valid': False, 'flowing': False, 'current': 0.0, 'power': 0.0}

        now = time.time()
        last_check = float(getattr(self, check_ts_attr, 0.0) or 0.0)
        if not force and now - last_check < ENERGY_STATE_SAMPLE_INTERVAL_S:
            last_success = float(getattr(self, success_ts_attr, 0.0) or 0.0)
            valid = bool(getattr(self, valid_attr, False)) and (
                now - last_success <= ENERGY_STATE_STALE_AFTER_S
            )
            return {
                'valid': valid,
                'flowing': bool(getattr(self, flow_attr, False)) if valid else False,
                'current': float(getattr(self, current_attr, 0.0) or 0.0),
                'power': float(getattr(self, power_attr, 0.0) or 0.0),
            }

        setattr(self, check_ts_attr, now)
        try:
            live = await asyncio.wait_for(
                async_run_blocking(get_livemeasure_pilot, meter_ip),
                timeout=1.5,
            )
            current = abs(float(live.get('current', 0) or 0))
            power = self._corrected_power_kw(live.get('power', 0))
            flowing = (
                current > ENERGY_FLOW_CURRENT_THRESHOLD_A or
                power > ENERGY_FLOW_POWER_THRESHOLD_KW
            )
            setattr(self, success_ts_attr, now)
            setattr(self, valid_attr, True)
            setattr(self, flow_attr, flowing)
            setattr(self, current_attr, current)
            setattr(self, power_attr, power)
            return {
                'valid': True,
                'flowing': flowing,
                'current': current,
                'power': power,
            }
        except Exception as e:
            last_success = float(getattr(self, success_ts_attr, 0.0) or 0.0)
            cached_valid = bool(getattr(self, valid_attr, False)) and (
                now - last_success <= ENERGY_STATE_STALE_AFTER_S
            )
            logging.debug(f'[ENERGY-STATE-V10] C{connector_id} meter check failed: {e}')
            return {
                'valid': cached_valid,
                'flowing': bool(getattr(self, flow_attr, False)) if cached_valid else False,
                'current': float(getattr(self, current_attr, 0.0) or 0.0),
                'power': float(getattr(self, power_attr, 0.0) or 0.0),
            }

    async def _pending_start_energy_flow(self, connector_id):
        """Meter fallback while a RemoteStart/RFID start is already authorized."""
        snapshot = await self._read_energy_state(connector_id)
        if snapshot['valid'] and snapshot['flowing']:
            logging.warning(
                f"[START-ENERGY] C{connector_id} physical energy flow detected "
                f"current={snapshot['current']:.2f}A power={snapshot['power']:.2f}kW; "
                'opening transaction'
            )
            return True
        return False

    async def _desired_active_status(self, connector_id, raw_iCp, raw_usi, initial=False):
        """Choose Charging/SuspendedEV/SuspendedEVSE from the physical state.

        `initial=True` is used while evaluating a new transaction. RemoteStart may
        still use the returned suspended status according to its flow, while the
        RFID V11 path waits until this function returns Charging before it sends
        StartTransaction.
        """
        raw_iCp = str(raw_iCp or '')
        raw_usi = str(raw_usi or '')
        snapshot = await self._read_energy_state(connector_id)

        if connector_id == 1:
            no_flow_since_attr = '_no_energy_since_c1'
        else:
            no_flow_since_attr = '_no_energy_since_c2'

        if snapshot['valid']:
            if snapshot['flowing']:
                setattr(self, no_flow_since_attr, 0.0)
                return 'Charging'

            now = time.time()
            no_flow_since = float(getattr(self, no_flow_since_attr, 0.0) or 0.0)
            if no_flow_since <= 0.0:
                no_flow_since = now
                setattr(self, no_flow_since_attr, no_flow_since)

            # Avoid status chatter when one short meter sample becomes zero while
            # Charging. Initial startup has no debounce because it has never been
            # confirmed as Charging yet.
            if (not initial and self._last_reported_status(connector_id) == 'Charging' and
                    now - no_flow_since < ENERGY_SUSPEND_CONFIRM_SECONDS):
                return 'Charging'

            if 'SuspendedEVSE' in (raw_iCp, raw_usi):
                return 'SuspendedEVSE'
            return 'SuspendedEV'

        # Meter unavailable: use the PLC state instead of inventing a suspension.
        setattr(self, no_flow_since_attr, 0.0)
        if 'SuspendedEVSE' in (raw_iCp, raw_usi):
            return 'SuspendedEVSE'
        if 'SuspendedEV' in (raw_iCp, raw_usi):
            return 'SuspendedEV'
        if 'Charging' in (raw_iCp, raw_usi):
            return 'Charging'
        return 'SuspendedEV'

    async def _sync_active_transaction_status(self, connector_id, raw_iCp, raw_usi):
        """Send only real active-session status transitions.

        This fixes two common cases for this PLC mapper:
        1. It can stay at SuspendedEV while the meter is already delivering energy.
        2. It can say Charging while current/power are still zero during startup.
        """
        txn_id = self._active_transaction_id(connector_id)
        if txn_id <= 0:
            return

        if connector_id == 1:
            if (self.remote_stop_finishing_c1 != 0 or self.my_remote_stop_c1 or
                    int(self.finshing_sent_c1 or 0) == 1 or str(self.emergency_sent_c1) != '1'):
                return
        else:
            if (self.remote_stop_finishing_c2 != 0 or self.my_remote_stop_c2 or
                    int(self.finshing_sent_c2 or 0) == 1 or str(self.emergency_sent_c2) != '1'):
                return

        raw_active_states = {'Charging', 'SuspendedEV', 'SuspendedEVSE'}
        raw_active = str(raw_iCp) in raw_active_states or str(raw_usi) in raw_active_states
        snapshot = await self._read_energy_state(connector_id)
        if not raw_active and not (snapshot['valid'] and snapshot['flowing']):
            return

        desired = await self._desired_active_status(connector_id, raw_iCp, raw_usi)
        previous = self._last_reported_status(connector_id)
        if desired == previous:
            return

        await self.status_notification_with_status(connector_id, desired)
        logging.info(
            f"[PHYSICAL-STATUS-V10] C{connector_id} txn={txn_id} "
            f"{previous or 'Unknown'} -> {desired}; "
            f"PLC={raw_iCp}/{raw_usi} current={snapshot['current']:.2f}A "
            f"power={snapshot['power']:.2f}kW"
        )

    @staticmethod
    def _read_int_state(file_name, default=0):
        """Read a small integer state file without dying on '', '0\\n' or junk."""
        try:
            if not os.path.exists(file_name):
                return default
            return int(open(file_name).read().strip() or default)
        except Exception:
            return default

    def _session_finishing_active(self, connector_id):
        if connector_id == 1:
            return int(self.session_finishing_c1 or 0) == 1
        if connector_id == 2:
            return int(self.session_finishing_c2 or 0) == 1
        return False

    def _stop_latched_txn(self, connector_id):
        """The transaction id whose StopTransaction armed the SESSION-FINISHING latch.

        Set by send_stop_transaction() before it sends. While that id is still the
        open one, the latch belongs to it and is NOT stale - see
        _normalize_ocpp_status(). A genuinely new session gets a new id from the
        CSMS, so the comparison there still spots a stale latch.
        """
        try:
            return int(getattr(self, f"_stop_latched_txn_c{connector_id}", 0) or 0)
        except (TypeError, ValueError):
            return 0

    def _set_session_finishing(self, connector_id, value):
        """Set the latch and remember the generation of the latest stop event.
        An unchanged value does not rewrite the state file or wear the SD card."""
        value = 1 if value else 0
        if connector_id in (1, 2):
            # Remember the status-report generation of this actual stop event.
            # An earlier Available must NOT cancel a newly armed finishing latch.
            setattr(self, f'_finishing_report_seq_c{connector_id}',
                    int(getattr(self, f'_reported_status_seq_c{connector_id}', 0)) if value else -1)
        if connector_id == 1:
            if int(self.session_finishing_c1 or 0) == value:
                return
            self.session_finishing_c1 = value
            _write_file("session_finishing_c1.txt", value)
        elif connector_id == 2:
            if int(self.session_finishing_c2 or 0) == value:
                return
            self.session_finishing_c2 = value
            _write_file("session_finishing_c2.txt", value)

    # ------------------------------------------------------------------------
    # [PREPARING-TIMEOUT] latch: Faulted until the cable is removed
    # ------------------------------------------------------------------------
    def _preparing_timeout_active(self, connector_id):
        if connector_id in (1, 2):
            return int(getattr(self, f"preparing_timeout_c{connector_id}", 0) or 0) == 1
        return False

    def _set_preparing_timeout(self, connector_id, value):
        if connector_id not in (1, 2):
            return
        value = 1 if value else 0
        if int(getattr(self, f"preparing_timeout_c{connector_id}", 0) or 0) == value:
            return
        setattr(self, f"preparing_timeout_c{connector_id}", value)
        _write_file(f"preparing_timeout_c{connector_id}.txt", value)

    def _status_error_fields(self, connector_id, status):
        """errorCode/info/vendorErrorCode for a normal StatusNotification."""
        if str(status) == 'Faulted' and self._preparing_timeout_active(connector_id):
            return {
                'error_code': PREPARING_TIMEOUT_ERROR_CODE,
                'info': PREPARING_TIMEOUT_INFO,
                'vendor_error_code': PREPARING_TIMEOUT_VENDOR_ERROR_CODE,
            }
        return {'error_code': 'NoError'}

    async def status_notification_preparing_timeout(self, connector_id):
        """Controller ended a Preparing session that never got a RemoteStart."""
        self._set_preparing_timeout(connector_id, 1)
        if self._dual_manager is not None:
            self._dual_manager.release_pending(int(connector_id))
        if self._vector_fault_blocks_normal_status(connector_id):
            self._log_vector_fault_suppression(connector_id, "Faulted(PREPARING TIMEOUT)")
            return
        if self._last_reported_status(connector_id) == 'Faulted':
            return
        await self.call(call.StatusNotification(
            connector_id=connector_id,
            status='Faulted',
            timestamp=convert_timeformat(datetime.now(timezone.utc).replace(tzinfo=timezone.utc)),
            vendor_id='EgatDimomdService',
            **self._status_error_fields(connector_id, 'Faulted'),
        ))
        self._remember_reported_status(connector_id, 'Faulted')

    def _session_finishing_already_reported(self, connector_id, status):
        """True when Finishing was already sent for this emergency-stopped session.

        Both the vector fault-clear path and the emergency-release path resolve to
        Finishing. Whichever runs first reports it; the other must not repeat it.
        """
        return (
            str(status) == 'Finishing'
            and self._session_finishing_active(connector_id)
            and self._last_reported_status(connector_id) == 'Finishing'
        )

    def _normalize_ocpp_status(self, connector_id, status):
        """Prevent transaction-only suspended states before StartTransaction exists.

        SuspendedEV and SuspendedEVSE describe a temporary interruption during an
        active OCPP transaction. If the PLC reports either state while no active
        transaction exists, expose Preparing instead: the EV is connected and the
        charging process is being prepared, but no OCPP transaction has started.
        """
        normalized = str(status or "Available")
        if connector_id in (1, 2) and normalized in ("SuspendedEV", "SuspendedEVSE"):
            txn_id = self._active_transaction_id(connector_id)
            # [LOCAL-STATUS-V16] session ที่เริ่มจากหน้าจอไม่มี transaction "โดยเจตนา"
            # แต่รถเสียบอยู่และเคยชาร์จจริง SuspendedEV จึงเป็นความจริงที่ควรบอก
            # CSMS ไม่ใช่ Preparing (Preparing แปลว่า "ยังไม่เคยเริ่ม" ซึ่งผิด)
            if txn_id <= 0 and not self._local_status_session_active(connector_id):
                logging.info(
                    f"[OCPP-STATE-GUARD] C{connector_id} {normalized} without active "
                    "transaction -> Preparing"
                )
                normalized = "Preparing"

        # Physical unplug releases ALL end-of-session markers before any early
        # return. A timeout must never bypass finishing/local-stop cleanup.
        if connector_id in (1, 2) and normalized == 'Available':
            if self._session_finishing_active(connector_id):
                logging.info('[SESSION-FINISHING] C%s physical Available clears finishing and local-stop markers', connector_id)
            self._set_session_finishing(connector_id, 0)
            self._clear_pending_local_stop(connector_id)

        # [PREPARING-TIMEOUT] Faulted from the timeout until the cable is removed.
        # A real PLC Faulted also stays Faulted; only Available (unplug) or a new
        # transaction releases the latch.
        if connector_id in (1, 2) and self._preparing_timeout_active(connector_id):
            if self._active_transaction_id(connector_id) > 0:
                self._set_preparing_timeout(connector_id, 0)
            elif normalized == "Available":
                self._set_preparing_timeout(connector_id, 0)
                logging.info(
                    f"[PREPARING-TIMEOUT] C{connector_id} cable removed: "
                    "Faulted -> Available"
                )
                return normalized
            else:
                return "Faulted"

        # [SESSION-FINISHING] OCPP 1.6: once a session has ended the connector stays
        # Finishing while the cable is still connected, and only becomes Available
        # after the driver unplugs.  Preparing means "plugged in, about to charge",
        # so it must never be used for a connector that has already finished.
        #
        # The PLC cannot tell the two apart - both report cp_state B - so the latch
        # supplies the missing history and the PLC supplies the truth about the cable.
        # This is the single choke point for every StatusNotification sender, so it
        # covers the internal emergency path, the Vector fault-clear path and boot.
        if connector_id in (1, 2):
            if self._session_finishing_active(connector_id):
                open_txn = self._active_transaction_id(connector_id)
                available_after_stop = (
                    normalized == 'Preparing' and open_txn <= 0
                    and self._last_reported_status(connector_id) == 'Available'
                    and int(getattr(self, f'_reported_status_seq_c{connector_id}', 0))
                        > int(getattr(self, f'_finishing_report_seq_c{connector_id}', -1))
                )
                # "Stale" means a NEW session has started, so the id has to differ
                # from the one whose StopTransaction armed the latch. Comparing
                # against >0 alone was a race: send_stop_transaction() arms the latch
                # and only zeroes transactionId_cX/translogsN.txt AFTER the send
                # returns, so for the whole round trip the latch is armed while the
                # id being closed is still there. main_run_program polls every 0.2s,
                # so a status read landing in that window used to clear the latch -
                # and persist the 0 - which is what made an emergency stop during an
                # RFID session come back as Preparing instead of Finishing.
                # It also keeps the latch when the stop could not be sent at all.
                if available_after_stop:
                    self._set_session_finishing(connector_id, 0)
                    self._clear_pending_local_stop(connector_id)
                    logging.info('[SESSION-FINISHING] C%s stale latch after later Available; new plug -> Preparing', connector_id)
                elif open_txn > 0 and open_txn != self._stop_latched_txn(connector_id):
                    # A new session already started on this connector — latch is stale.
                    self._set_session_finishing(connector_id, 0)
                elif normalized == "Available":
                    self._set_session_finishing(connector_id, 0)
                    self._clear_pending_local_stop(connector_id)
                    logging.info(
                        f"[SESSION-FINISHING] C{connector_id} cable removed: "
                        "Finishing -> Available"
                    )
                elif normalized == "Preparing":
                    logging.info(
                        f"[SESSION-FINISHING] C{connector_id} Preparing -> Finishing "
                        "(session ended, cable still connected)"
                    )
                    normalized = "Finishing"
                # Faulted and anything else pass through untouched.  plc.get_connector_
                # status() returns Faulted on head_error or CP state E/F, and a real
                # hardware fault must never be hidden behind Finishing.
            elif (normalized == "Finishing"
                  and self._active_transaction_id(connector_id) <= 0):
                # The PLC reported Finishing on its own (its finishing flag or
                # SessionStop).  Remember it, otherwise once that flag drops with the
                # cable still connected the mapper falls back to Preparing.
                self._set_session_finishing(connector_id, 1)
            elif (normalized == "Available"
                  and self._active_transaction_id(connector_id) <= 0):
                self._clear_pending_local_stop(connector_id)
        return normalized

    # ========================================================================
    # Vector fault MQTT -> OCPP 1.6J
    # ========================================================================
    def _vector_fault_blocks_normal_status(self, connector_id):
        return (
            VECTOR_FAULT_BRIDGE_ENABLED
            and connector_id in (0, 1, 2)
            and self.vector_fault_bridge.has_active(connector_id)
        )

    def _log_vector_fault_suppression(self, connector_id, status):
        """[VECTOR-FAULT-SUPPRESS] ทำให้ StatusNotification ที่ถูกกลืน "มองเห็นได้"

        ประตูที่ _vector_fault_blocks_normal_status() ยังทำงานเหมือนเดิมทุกประการ
        (fault จริงต้องไม่ถูกซ่อน) เปลี่ยนแค่ระดับ log ที่เดิมเป็น debug — ซึ่งถูก
        logging.basicConfig(level=INFO) ใน plc_function ตัดทิ้ง 100% ทำให้สถานะที่
        หายไปไม่ปรากฏใน log เลย — ให้เป็น warning แบบ throttle 10 วิ/หัว เพราะ
        main_run_program วน 0.2 วิ และ _sync_active_transaction_status จะพยายาม
        ส่งซ้ำทุก tick ตลอดไป
        """
        now = time.time()
        ts_attr = f"_vf_suppress_ts_c{connector_id}"
        cnt_attr = f"_vf_suppress_cnt_c{connector_id}"
        setattr(self, cnt_attr, int(getattr(self, cnt_attr, 0) or 0) + 1)
        if now - float(getattr(self, ts_attr, 0.0) or 0.0) < 10.0:
            return
        setattr(self, ts_attr, now)
        try:
            active = self.vector_fault_bridge.active_codes(connector_id)
        except Exception:
            active = "?"
        logging.warning(
            "[VECTOR-FAULT-SUPPRESS] C%s StatusNotification(%s) DROPPED; "
            "active=%s dropped_total=%s lastReported=%s",
            connector_id, status, active,
            getattr(self, cnt_attr, 0),
            self._last_reported_status(connector_id),
        )
        setattr(self, cnt_attr, 0)

    def _log_local_status_block(self, connector_id, reason):
        """[LOCAL-STATUS-V16] บอกว่า session จากหน้าจอถูกประตูไหนกั้นไว้

        สอง return เงียบใน Charging branch (finishing==1 และ usi!='Charging')
        คือจุดที่มีโอกาสงับมากที่สุดหน้างาน throttle 10 วิ/หัวเช่นกัน
        """
        now = time.time()
        ts_attr = f"_local_status_block_ts_c{connector_id}"
        if now - float(getattr(self, ts_attr, 0.0) or 0.0) < 10.0:
            return
        setattr(self, ts_attr, now)
        logging.warning(
            "[LOCAL-STATUS-V16] C%s armed แต่ยังส่ง Charging ไม่ได้: %s",
            connector_id, reason,
        )

    async def _emergency_stop_open_transactions(self):
        """Close every open transaction before any emergency Faulted goes out.

        The Faulted notifications and the StopTransaction are produced by two
        different tasks - the Vector fault bridge and main_run_program - so their
        order was decided by whichever task happened to run first, and the Faulted
        usually won. An emergency must reach the CSMS as "the transaction ended,
        and here is why", so the stop is pulled forward to the very start of the
        emergency handling.

        Both callers can fire for the same press, so the lock serialises them: the
        second one waits, then finds the transactions already closed and does
        nothing. Without it the two ran concurrently, the second passed the
        STOP-GUARD on a transaction the first was still closing, and by the time it
        built its request the id had become 0 - a StopTransaction with
        transactionId 0 went to the CSMS.
        """
        async with self._emergency_stop_lock:
            for connector_id in (1, 2):
                active_txn = self._active_transaction_id(connector_id)
                if active_txn <= 0:
                    continue
                try:
                    logging.info(
                        "[EMERGENCY-STOP-FIRST] C%s stopping txn=%s before the Faulted "
                        "notification", connector_id, active_txn,
                    )
                    await self.send_stop_transaction(connector_id, reasonf='EmergencyStop')
                    if connector_id == 1:
                        self.block1 = 3
                        _write_file("block1.txt", 3)
                        _write_file("translogs1.txt", 0)
                    else:
                        self.block2 = 3
                        _write_file("block2.txt", 3)
                        _write_file("translogs2.txt", 0)
                except Exception as e:
                    logging.exception(
                        "[EMERGENCY-STOP-FIRST] C%s stop failed: %s", connector_id, e
                    )

    async def _send_vector_fault_status_notification(self, record: VectorFaultRecord):
        """Send the exact OCPP metadata supplied by the Vector fault collector.

        One StatusNotification per connector, not per fault code.  An emergency stop
        raises several codes on the same connector (F11010 + F11011), but OCPP 1.6
        carries a single status per connector, so the extra ones would only repeat
        Faulted.  The bridge still tracks every code, so the connector stays Faulted
        until the last one clears and exactly one clear notification follows.

        A fault also ends whatever charging attempt was in progress, so arm the
        SESSION-FINISHING latch: once the fault clears the connector reports
        Finishing while the cable is still in, not Preparing. The latch releases
        itself as soon as the PLC reports Available (cable out) or a new
        transaction opens, so an idle or unplugged connector is unaffected.
        """
        # SYS_EMERGENCY_STOP is the first record of an emergency burst (the inbox
        # sorts the batch by connectorId and this one is C0), so closing the
        # transactions here puts StopTransaction ahead of every Faulted on the wire.
        if record.code == EMERGENCY_STOP_FAULT_CODE:
            await self._emergency_stop_open_transactions()

        if record.connector_id in (1, 2):
            self._set_session_finishing(record.connector_id, 1)

        if self.vector_fault_bridge.active_count(record.connector_id) > 0:
            logging.info(
                "[VECTOR-FAULT-OCPP] C%s already Faulted (%s); %s not sent separately",
                record.connector_id,
                ",".join(self.vector_fault_bridge.active_codes(record.connector_id)),
                record.vendor_error_code,
            )
            self._remember_reported_status(record.connector_id, 'Faulted')
            return

        await self.call(call.StatusNotification(
            connector_id=record.connector_id,
            error_code=record.error_code,
            status='Faulted',
            info=record.info,
            timestamp=convert_timeformat(
                datetime.now(timezone.utc).replace(tzinfo=timezone.utc)
            ),
            vendor_id=VECTOR_FAULT_VENDOR_ID,
            vendor_error_code=record.vendor_error_code,
        ))
        self._remember_reported_status(record.connector_id, 'Faulted')

    async def _vector_fault_restore_status(self, connector_id):
        if connector_id == 0:
            return 'Available'

        ip_addr = self.myip_adr_c1 if connector_id == 1 else self.myip_adr_c2
        fallback = self.iCpStateCss1 if connector_id == 1 else self.iCpStateCss2
        try:
            status, usi = plc.get_connector_status(ip_addr)
        except Exception as exc:
            logging.warning(
                "[VECTOR-FAULT-OCPP] cannot read restore status C%s: %s",
                connector_id,
                exc,
            )
            status, usi = fallback, ''

        status = str(status or fallback or 'Available')
        usi = str(usi or '')
        if status == 'Faulted':
            # A PLC still sitting at Faulted says nothing about the cable, so never
            # resolve it to Available here. get_connector_status() returns
            # (Faulted, Faulted) in this case - usi can never be Preparing/Charging -
            # so the old expression always produced Available, and
            # _normalize_ocpp_status reads Available as "the driver unplugged" and
            # drops the SESSION-FINISHING latch. That is what made the status after an
            # emergency come out Preparing at random instead of Finishing.
            # Assume the cable is still connected; if it really is out, the PLC settles
            # to state A and the main loop sends Available on its next pass.
            status = 'Preparing'
        return self._normalize_ocpp_status(connector_id, status)

    async def _send_vector_fault_clear_notification(
        self,
        connector_id: int,
        cleared_record: VectorFaultRecord,
    ):
        """Send NoError only after the last sendable Vector fault is cleared."""
        status = await self._vector_fault_restore_status(connector_id)
        info = f"Issue {cleared_record.vendor_error_code} cleared."
        if len(info) > 50:
            raise ValueError("Vector clear info exceeds OCPP 1.6 limit")
        error_code = 'NoError'
        vendor_error_code = cleared_record.vendor_error_code
        if status == 'Faulted' and self._preparing_timeout_active(connector_id):
            # [PREPARING-TIMEOUT] the Vector fault is gone but the timeout is not
            fields = self._status_error_fields(connector_id, status)
            error_code = fields['error_code']
            info = fields['info']
            vendor_error_code = fields['vendor_error_code']

        await self.call(call.StatusNotification(
            connector_id=connector_id,
            error_code=error_code,
            status=status,
            info=info,
            timestamp=convert_timeformat(
                datetime.now(timezone.utc).replace(tzinfo=timezone.utc)
            ),
            vendor_id=VECTOR_FAULT_VENDOR_ID,
            vendor_error_code=vendor_error_code,
        ))
        self._remember_reported_status(connector_id, status)
        logging.info(
            "[VECTOR-FAULT-OCPP] cleared C%s %s; restored status=%s",
            connector_id,
            cleared_record.vendor_error_code,
            status,
        )

    async def vector_fault_startup_sync(self):
        if not VECTOR_FAULT_BRIDGE_ENABLED:
            return 0
        processed = await self.vector_fault_bridge.startup_sync(wait_seconds=1.5)
        logging.info(
            "[VECTOR-FAULT-OCPP] startup sync processed=%s active=%s",
            processed,
            self.vector_fault_bridge.all_active(),
        )
        return processed

    async def vector_fault_event_loop(self):
        if not VECTOR_FAULT_BRIDGE_ENABLED:
            while True:
                await asyncio.sleep(3600)
        await self.vector_fault_bridge.run(interval=0.2)

    async def status_notification_cp(self, connector_id):
        if self._vector_fault_blocks_normal_status(connector_id):
            self._log_vector_fault_suppression(connector_id, "poll")
            return
        if connector_id == 0:
            status = 'Available'
        elif connector_id == 1:
            self.iCpStateCss1, _ = plc.get_connector_status(self.myip_adr_c1)
            status = self.iCpStateCss1
        else:
            self.iCpStateCss2, _ = plc.get_connector_status(self.myip_adr_c2)
            status = self.iCpStateCss2

        status = self._normalize_ocpp_status(connector_id, status)
        if self._session_finishing_already_reported(connector_id, status):
            return

        await self.call(call.StatusNotification(
            connector_id=connector_id,
            status=status,
            timestamp=convert_timeformat(datetime.now(timezone.utc).replace(tzinfo=timezone.utc)),
            vendor_id='EgatDimomdService',
            **self._status_error_fields(connector_id, status),
        ))
        self._remember_reported_status(connector_id, status)

    async def status_notification_with_status(self, connector_id, status):
        if self._vector_fault_blocks_normal_status(connector_id):
            self._log_vector_fault_suppression(connector_id, status)
            return
        status = self._normalize_ocpp_status(connector_id, status)
        if self._session_finishing_already_reported(connector_id, status):
            return
        if (status == 'Faulted' and self._preparing_timeout_active(connector_id)
                and self._last_reported_status(connector_id) == 'Faulted'):
            return   # [PREPARING-TIMEOUT] already on the wire
        await self.call(call.StatusNotification(
            connector_id=connector_id,
            status=status,
            timestamp=convert_timeformat(datetime.now(timezone.utc).replace(tzinfo=timezone.utc)),
            vendor_id='EgatDimomdService',
            **self._status_error_fields(connector_id, status),
        ))
        self._remember_reported_status(connector_id, status)
        print(f"## Sent StatusNotification connector {connector_id} status={status}")

    async def status_notification_usiLink(self, connector_id):
        # [SESSION-FINISHING] This sender hardcodes Finishing, so it bypasses
        # _normalize_ocpp_status() and the latch it would otherwise arm there.
        # Several callers end a session WITHOUT a StopTransaction (an RFID session
        # that never reached StartTransaction, a start with no RemoteStart, the
        # Reset path), and those used to put Finishing on the wire while leaving the
        # latch at 0 - so the very next status poll fell back to Preparing, and a
        # reboot reported Preparing too. Reporting Finishing IS the record that the
        # session ended, so record it. Armed before the two early returns below: the
        # session ended either way, and a fault only postpones announcing it.
        self._set_session_finishing(connector_id, 1)
        if self._vector_fault_blocks_normal_status(connector_id):
            self._log_vector_fault_suppression(connector_id, "Finishing")
            return
        if self._session_finishing_already_reported(connector_id, 'Finishing'):
            return
        await self.call(call.StatusNotification(
            connector_id=connector_id,
            error_code='NoError',
            status='Finishing',
            timestamp=convert_timeformat(datetime.now(timezone.utc).replace(tzinfo=timezone.utc)),
            vendor_id='EgatDimomdService',
        ))
        self._remember_reported_status(connector_id, 'Finishing')

    async def status_notification_emer(self, connector_id):
        # Same rule as the Vector path: the transactions go first. This is the
        # fallback used when the collector is not alive, and connector 0 is the
        # first emer notification of the burst.
        if connector_id == 0:
            await self._emergency_stop_open_transactions()

        if (
            VECTOR_FAULT_BRIDGE_ENABLED
            and (
                self.vector_fault_bridge.has_active(0)
                or self.vector_fault_bridge.source_recent(
                    max(90.0, VECTOR_FAULT_STATUS_MAX_AGE_SECONDS)
                )
            )
        ):
            # Emergency is reported by SYS_EMERGENCY_STOP from the live Vector
            # fault collector. Keep the existing stop/relay logic, but avoid
            # duplicate OCPP messages with different vendor codes. If the
            # collector is not alive, the original emergency notification remains
            # as a fallback.
            logging.info(
                "[VECTOR-FAULT-OCPP] internal emergency OCPP notification suppressed C%s",
                connector_id,
            )
            return
        await self.call(call.StatusNotification(
            connector_id=connector_id,
            error_code='NoError',
            status='Faulted',
            info='EmergencyButtonPressed',
            timestamp=convert_timeformat(datetime.now(timezone.utc).replace(tzinfo=timezone.utc)),
            vendor_id='EgatDimomdService',
        ))
        self._remember_reported_status(connector_id, 'Faulted')

    # ========================================================================
    # Heartbeat & Retry / Websocket Loop
    # ========================================================================
    async def send_heartbeat(self):
        request = call.Heartbeat()
        while True:
            try:
                await self.call(request)
            except Exception as e:
                _write_file("MDBlog.txt", f"{e} | {datetime.now(pytz.timezone('Asia/Bangkok')).strftime('%Y-%m-%dT%H:%M:%S.%f')}\n")
                logging.warning(f"[Heartbeat] Failed: {e}")
                # ห้ามให้ Telegram HTTP request block OCPP event loop ตอน network ล่ม
                try:
                    await asyncio.wait_for(
                        asyncio.to_thread(
                            line_notify,
                            f"{OCPPSTATION} (Disconnect): Error is {e}",
                        ),
                        timeout=7,
                    )
                except Exception as notify_error:
                    logging.debug(f"[Heartbeat] disconnect notification skipped: {notify_error}")
            data = {
                "heartbeat": 1,
                "timestamp": datetime.now(pytz.timezone('Asia/Bangkok')).strftime("%Y-%m-%dT%H:%M:%S.%f")
            }
            client.publish(f"OCPP/{OCPPSTATION}/heartbeat", json.dumps(data))
            try:
                interval = max(1, int(self.HeartbeatInterval_cfg or 30))
            except (ValueError, TypeError):
                interval = 30
            await asyncio.sleep(interval)

    async def _call_txn_message_with_retry(self, request, msg_name: str = "",
                                           skip_schema_validation: bool = False):
        try:
            retries_left = max(0, int(str(self.TransactionMessageAttempts_cfg).strip() or "0"))
        except Exception:
            retries_left = 0
        try:
            retry_interval = max(0, int(str(self.TransactionMessageRetryInterval_cfg).strip() or "0"))
        except Exception:
            retry_interval = 0

        attempt_no = 0
        while True:
            attempt_no += 1
            try:
                if skip_schema_validation:
                    return await self.call(request, skip_schema_validation=True)
                return await self.call(request)
            except Exception as e:
                if self._dual_manager is not None and getattr(e, "retryable", True) is False:
                    raise
                if retries_left <= 0:
                    logging.exception(f"[TXN-RETRY] {msg_name} failed after {attempt_no} attempt(s): {e}")
                    raise
                logging.warning(f"[TXN-RETRY] {msg_name} failed (attempt={attempt_no}). Retry in {retry_interval}s. Error: {e}")
                retries_left -= 1
                await asyncio.sleep(retry_interval)

    async def _send_stop_transaction(self, connector_id, request):
        """Send StopTransaction with connectorId, falling back to the plain form.

        See _StopTransactionWithConnectorId: connectorId is an extension, so the
        CSMS is allowed to refuse it. A CALLERROR makes self.call() return None
        (responses are suppressed), and that is the signal to resend the standard
        payload so the transaction is still closed. Network failures keep raising
        exactly as before - only an outright rejection triggers the fallback.
        """
        if self._dual_manager is not None:
            # Standard StopTransaction for both endpoints. The manager durably
            # queues it before allowing the physical controller to clear state.
            return await self._dual_manager.call(request)
        if not STOP_TXN_CONNECTOR_ID_SUPPORTED:
            return await self._call_txn_message_with_retry(
                request, msg_name="StopTransaction")

        extended = _StopTransactionWithConnectorId(
            meter_stop=request.meter_stop,
            timestamp=request.timestamp,
            transaction_id=request.transaction_id,
            connector_id=connector_id,
            reason=request.reason,
            id_tag=request.id_tag,
            transaction_data=request.transaction_data,
        )
        response = await self._call_txn_message_with_retry(
            extended, msg_name="StopTransaction", skip_schema_validation=True)
        if response is not None:
            return response

        logging.warning(
            "[STOP-CONNECTORID] C%s CSMS rejected StopTransaction carrying "
            "connectorId; resending the standard payload",
            connector_id,
        )
        return await self._call_txn_message_with_retry(
            request, msg_name="StopTransaction")

    async def websocket_ping_loop(self):
        while True:
            try:
                interval = int(str(getattr(self, "WebSocketPingInterval_cfg", "0")).strip() or "0")
            except Exception: interval = 0
            if interval <= 0:
                await asyncio.sleep(5)
                continue
            try:
                ws = getattr(self, "_ws", None) or getattr(self, "_connection", None)
                if ws is not None:
                    pong_waiter = ws.ping()
                    try:
                        await asyncio.wait_for(pong_waiter, timeout=max(5, min(10, interval)))
                    except Exception:
                        pass
            except Exception:
                pass
            await asyncio.sleep(interval)

    # ========================================================================
    # RemoteStartTransaction / RemoteStopTransaction
    # ========================================================================
    @on(Action.remote_start_transaction)
    async def remote_start_transaction(self, id_tag, connector_id, **kwargs):
        try:
            connector_id = int(connector_id)
        except Exception:
            return call_result.RemoteStartTransaction(status=RemoteStartStopStatus.rejected)
        if connector_id not in (1, 2):
            return call_result.RemoteStartTransaction(status=RemoteStartStopStatus.rejected)
        gate = getattr(self, "_ocpp_start_allowed", None)
        manager = getattr(self, "_dual_manager", None)
        if gate is not None and not await gate(connector_id, command_origin(
                manager.config["local_csms"] if manager is not None else "A"), id_tag):
            explain = getattr(self, "_ocpp_start_refusal", None)
            try:
                reason = explain(connector_id) if explain else "start not allowed"
            except Exception as error:
                reason = "start not allowed (%s)" % type(error).__name__
            logging.warning(f"[REMOTE-START] reject C{connector_id}: {reason}")
            return call_result.RemoteStartTransaction(status=RemoteStartStopStatus.rejected)
        if self._active_transaction_id(connector_id) > 0 or self._remote_stop_worker_running(connector_id):
            logging.warning(
                f"[REMOTE-START-V9] reject C{connector_id}: active transaction or stop worker"
            )
            return call_result.RemoteStartTransaction(status=RemoteStartStopStatus.rejected)

        self._clear_stale_stop_latch_for_new_start(connector_id, "RemoteStart")
        if connector_id == 1:
            self.myremotestart_c1 = 1
            self._pin_autocharge_start_intent_c1 = ""
            self.myid_tag_c1 = id_tag
            self.remote_start_received_c1 = 1
            self._remote_start_latch_mono_c1 = time.monotonic()   # [START-LATCH] latch age
            self.rfid_start_received_c1 = 0
            self.rfid_recovery_pending_c1 = 0
            self.block_sw3_C1 = 1
            _write_file("myremotestart_c1.txt", 1)
            _write_file("myid_tag_c1.txt", id_tag)
            _write_file("remote_start_received_c1.txt", 1)
            _write_file("rfid_start_received_c1.txt", 0)
            _write_file("block_sw3_C1.txt", 1)
            # [LOCAL-STATUS-V16] OCPP RemoteStart เข้ามาแทนที่ session จากหน้าจอ
            self._set_local_status_session(1, 0)
            clear_local_request("start", 1)
        else:
            self.myremotestart_c2 = 1
            self._pin_autocharge_start_intent_c2 = ""
            self.myid_tag_c2 = id_tag
            self.remote_start_received_c2 = 1
            self._remote_start_latch_mono_c2 = time.monotonic()   # [START-LATCH] latch age
            self.rfid_start_received_c2 = 0
            self.rfid_recovery_pending_c2 = 0
            self.block_sw3 = 1
            _write_file("myremotestart_c2.txt", 1)
            _write_file("myid_tag_c2.txt", id_tag)
            _write_file("remote_start_received_c2.txt", 1)
            _write_file("rfid_start_received_c2.txt", 0)
            _write_file("block_sw3.txt", 1)
            # [LOCAL-STATUS-V16] OCPP RemoteStart เข้ามาแทนที่ session จากหน้าจอ
            self._set_local_status_session(2, 0)
            clear_local_request("start", 2)
        return call_result.RemoteStartTransaction(status=RemoteStartStopStatus.accepted)

    @on(Action.remote_stop_transaction)
    async def remote_stop_transaction(self, transaction_id, **kwargs):
        # [REMOTE-STOP-V3] Match against both memory and persisted transaction ID.
        # Schedule the stop sequence immediately instead of waiting for main loop.
        def _read_txn(path):
            try:
                return int(open(path).read().strip() or "0")
            except Exception:
                return 0

        txn_c1 = max(int(self.transactionId_c1 or 0), _read_txn("translogs1.txt"))
        txn_c2 = max(int(self.transactionId_c2 or 0), _read_txn("translogs2.txt"))

        if int(transaction_id) == txn_c1 and txn_c1 > 0:
            if int(getattr(self, "_ev_stop_claimed_txn_c1", 0) or 0) == txn_c1:
                logging.info(
                    f"[REMOTE-STOP-V3] C1 txn={transaction_id} already closing by EV; accepted without duplicate"
                )
                return call_result.RemoteStopTransaction(status=RemoteStartStopStatus.accepted)
            task = self._remote_stop_task_c1
            if task is not None and not task.done():
                logging.info(f"[REMOTE-STOP-V3] C1 duplicate request txn={transaction_id}; worker already running")
                return call_result.RemoteStopTransaction(status=RemoteStartStopStatus.accepted)

            self.remote_stop_finishing_c1 = 1
            self.block_sw1_C1 = 0
            self.start_sendMeterValue_SW1 = 1
            self.block1 = 1
            self.my_remote_stop_c1 = 0  # old main-loop handler must not run in parallel
            _write_file("remote_stop_finishing_c1.txt", 1)
            _write_file("block_sw1_C1.txt", 0)
            _write_file("start_sendMeterValue_SW1.txt", 1)
            _write_file("block1.txt", 1)
            _write_file("my_remote_stop_c1.txt", 0)
            self._remote_stop_task_c1 = asyncio.create_task(
                self._execute_remote_stop_v3(1, txn_c1)
            )
            logging.info(f"[REMOTE-STOP-V3] C1 worker scheduled txn={txn_c1}")
            return call_result.RemoteStopTransaction(status=RemoteStartStopStatus.accepted)

        if int(transaction_id) == txn_c2 and txn_c2 > 0:
            if int(getattr(self, "_ev_stop_claimed_txn_c2", 0) or 0) == txn_c2:
                logging.info(
                    f"[REMOTE-STOP-V3] C2 txn={transaction_id} already closing by EV; accepted without duplicate"
                )
                return call_result.RemoteStopTransaction(status=RemoteStartStopStatus.accepted)
            task = self._remote_stop_task_c2
            if task is not None and not task.done():
                logging.info(f"[REMOTE-STOP-V3] C2 duplicate request txn={transaction_id}; worker already running")
                return call_result.RemoteStopTransaction(status=RemoteStartStopStatus.accepted)

            self.remote_stop_finishing_c2 = 1
            self.block_sw1 = 0
            self.start_sendMeterValue_SW2 = 1
            self.block2 = 1
            self.my_remote_stop_c2 = 0
            _write_file("remote_stop_finishing_c2.txt", 1)
            _write_file("block_sw1.txt", 0)
            _write_file("start_sendMeterValue_SW2.txt", 1)
            _write_file("block2.txt", 1)
            _write_file("my_remote_stop_c2.txt", 0)
            self._remote_stop_task_c2 = asyncio.create_task(
                self._execute_remote_stop_v3(2, txn_c2)
            )
            logging.info(f"[REMOTE-STOP-V3] C2 worker scheduled txn={txn_c2}")
            return call_result.RemoteStopTransaction(status=RemoteStartStopStatus.accepted)

        logging.warning(
            f"[REMOTE-STOP-V3] Reject unknown transaction_id={transaction_id}; "
            f"C1={txn_c1}, C2={txn_c2}"
        )
        return call_result.RemoteStopTransaction(status=RemoteStartStopStatus.rejected)

    async def _execute_remote_stop_v3(self, connector_id: int, transaction_id: int):
        lock = self._remote_stop_lock_c1 if connector_id == 1 else self._remote_stop_lock_c2
        ip_addr = self.myip_adr_c1 if connector_id == 1 else self.myip_adr_c2

        async with lock:
            logging.info(f"[REMOTE-STOP-V3] C{connector_id} worker started txn={transaction_id}")

            # Preserve the exact transaction requested by CSMS.
            if connector_id == 1:
                self.transactionId_c1 = transaction_id
            else:
                self.transactionId_c2 = transaction_id

            # PLC stop is best-effort. A PLC timeout must never prevent StopTransaction.
            try:
                await asyncio.wait_for(async_remote_stop_plc(ip_addr, 1), timeout=5.0)
            except Exception as e:
                logging.error(f"[REMOTE-STOP-V3] C{connector_id} PLC stop ON failed/timeout: {e}")

            await asyncio.sleep(3)

            try:
                await asyncio.wait_for(async_remote_stop_plc(ip_addr, 0), timeout=5.0)
            except Exception as e:
                logging.error(f"[REMOTE-STOP-V3] C{connector_id} PLC stop OFF failed/timeout: {e}")

            # Always attempt StopTransaction even when PLC communication failed.
            # Re-assert the exact requested transaction immediately before send.
            # This is a final safety net against any unrelated task changing the
            # in-memory/file transaction id while the PLC stop pulse is running.
            if connector_id == 1:
                self.transactionId_c1 = transaction_id
                _write_file("translogs1.txt", transaction_id)
            else:
                self.transactionId_c2 = transaction_id
                _write_file("translogs2.txt", transaction_id)

            try:
                logging.info(f"[REMOTE-STOP-V3] C{connector_id} sending StopTransaction txn={transaction_id}")
                await self.send_stop_transaction(connector_id, reasonf='Remote')
                logging.info(f"[REMOTE-STOP-V3] C{connector_id} StopTransaction completed txn={transaction_id}")
            except Exception as e:
                # Keep transaction and phase=1 persisted so a retry/restart can recover it.
                if connector_id == 1:
                    self.transactionId_c1 = transaction_id
                    _write_file("translogs1.txt", transaction_id)
                else:
                    self.transactionId_c2 = transaction_id
                    _write_file("translogs2.txt", transaction_id)
                logging.exception(f"[REMOTE-STOP-V3] C{connector_id} StopTransaction failed: {e}")
                return

            try:
                await self.status_notification_usiLink(connector_id)
            except Exception as e:
                logging.exception(f"[REMOTE-STOP-V3] C{connector_id} Finishing notification failed: {e}")
                return

            if connector_id == 1:
                self.remote_stop_finishing_c1 = 2
                self.finshing_sent_sw_c1 = 2
                self.block_sw1_C1 = 1
                self.block1 = 0
                _write_file("remote_stop_finishing_c1.txt", 2)
                _write_file("finshing_sent_sw_c1.txt", 2)
                _write_file("block_sw1_C1.txt", 1)
                _write_file("block1.txt", 0)
            else:
                self.remote_stop_finishing_c2 = 2
                self.finshing_sent_sw_c2 = 2
                self.block_sw1 = 1
                self.block2 = 0
                _write_file("remote_stop_finishing_c2.txt", 2)
                _write_file("finshing_sent_sw_c2.txt", 2)
                _write_file("block_sw1.txt", 1)
                _write_file("block2.txt", 0)

            logging.info(
                f"[REMOTE-STOP-V3] C{connector_id} sequence completed; "
                "phase=2 waiting for Available"
            )

    # ========================================================================
    # StartTransaction (RFID/Local flow) - ZERO WAIT + SPEED FIX
    # ========================================================================
    async def start_transaction_ath(self, connector_id, **kwargs):
        pin_autocharge_start = self._take_pin_start_origin(
            connector_id, getattr(self, f"myid_tag_ath_c{connector_id}", ""))
        if connector_id == 1:
            # [SPEED-FIX] 1. อ่านมิเตอร์แบบ Async (บังคับว่าต้องเสร็จภายใน 2 วินาที)
            print(f"## C1: Reading meter (Max 2 seconds)...")
            try:
                meter_data = await asyncio.wait_for(
                    async_run_blocking(get_meter_value_pilot, self.my_meter_ip_c1),
                    timeout=2.0
                )
                self.meter_start1 = energy_wh(meter_data['energyTotal'])
            except Exception as e:
                print(f"## C1 meter read timeout/error! Using last known value. Error: {e}")
                if not hasattr(self, 'meter_start1'): self.meter_start1 = 0

            # [SPEED-FIX] 2. ส่ง Authorize (ข้ามถ้าเพิ่ง auth ภายใน 5 นาที)
            _AUTH_REUSE_WINDOW = 300
            _last_auth = self.auth_timestamps.get(self.myid_tag_ath_c1, 0)
            _age = time.time() - _last_auth
            if _last_auth > 0 and _age < _AUTH_REUSE_WINDOW:
                print(f"## C1: Skip re-Authorize (auth was {int(_age)}s ago, reuse window={_AUTH_REUSE_WINDOW}s)")
            else:
                try:
                    auth_res = await self.authorize(self.myid_tag_ath_c1)
                    auth_status = auth_res.id_tag_info.get("status") if isinstance(auth_res.id_tag_info, dict) else str(auth_res.id_tag_info)
                    if auth_status != "Accepted":
                        print(f"## start_transaction_ath C1 aborted: re-auth={auth_status}")
                        await self._abort_session_immediately(1)
                        return None
                except Exception as e:
                    print(f"## start_transaction_ath C1 authorize error: {e}")
                    return None

            # [SPEED-FIX] 3. ส่ง StartTransaction ติดกันทันที!
            request = call.StartTransaction(
                connector_id=1, id_tag=self.myid_tag_ath_c1,
                meter_start=self.meter_start1,
                timestamp=convert_timeformat(datetime.now(timezone.utc).replace(tzinfo=timezone.utc)),
            )
            res = await self._dual_start_transaction(request) if self._dual_manager is not None else await self._call_txn_message_with_retry(request, msg_name="StartTransaction")
            if res is None:
                return
            
            txn_status = "Invalid"
            try:
                if isinstance(res.id_tag_info, dict): txn_status = res.id_tag_info.get("status", "Invalid")
                else: txn_status = getattr(res.id_tag_info, "status", "Invalid")
            except: pass

            if res.transaction_id > 0 and txn_status == "Accepted":
                self.transactionId_c1 = res.transaction_id
                self._arm_ev_stop_transaction_guard(1, res.transaction_id)
                self._remember_pin_start_origin(1, res.transaction_id, pin_autocharge_start)
                meter_energy.begin(1, res.transaction_id, self.meter_start1)
                self.active_id_tag_c1 = self.myid_tag_ath_c1
                self.rfid_start_received_c1 = 1
                self.rfid_recovery_pending_c1 = 0
                _write_file("translogs1.txt", self.transactionId_c1)
                _write_file("active_id_tag_c1.txt", self.active_id_tag_c1)
                _write_file("rfid_start_received_c1.txt", 1)
                print(f"## StartTransaction C1 SUCCESS: txn_id={res.transaction_id}")
            else:
                print(f"## StartTransaction C1 REJECTED: txn_id={res.transaction_id}, status={txn_status}")
                # [ABORT-STOP] the CSMS opened this id before refusing the tag - close it
                await self._abort_session_immediately(
                    1, transaction_id=getattr(res, "transaction_id", 0),
                    id_tag=self.myid_tag_ath_c1)
            return res

        elif connector_id == 2:
            # [SPEED-FIX] 1. อ่านมิเตอร์แบบ Async (บังคับว่าต้องเสร็จภายใน 2 วินาที)
            print(f"## C2: Reading meter (Max 2 seconds)...")
            try:
                meter_data = await asyncio.wait_for(
                    async_run_blocking(get_meter_value_pilot, self.my_meter_ip_c2),
                    timeout=2.0
                )
                self.meter_start2 = energy_wh(meter_data['energyTotal'])
            except Exception as e:
                print(f"## C2 meter read timeout/error! Using last known value. Error: {e}")
                if not hasattr(self, 'meter_start2'): self.meter_start2 = 0

            # [SPEED-FIX] 2. ส่ง Authorize (ข้ามถ้าเพิ่ง auth ภายใน 5 นาที)
            _AUTH_REUSE_WINDOW = 300
            _last_auth = self.auth_timestamps.get(self.myid_tag_ath_c2, 0)
            _age = time.time() - _last_auth
            if _last_auth > 0 and _age < _AUTH_REUSE_WINDOW:
                print(f"## C2: Skip re-Authorize (auth was {int(_age)}s ago, reuse window={_AUTH_REUSE_WINDOW}s)")
            else:
                try:
                    auth_res = await self.authorize(self.myid_tag_ath_c2)
                    auth_status = auth_res.id_tag_info.get("status") if isinstance(auth_res.id_tag_info, dict) else str(auth_res.id_tag_info)
                    if auth_status != "Accepted":
                        print(f"## start_transaction_ath C2 aborted: re-auth={auth_status}")
                        await self._abort_session_immediately(2)
                        return None
                except Exception as e:
                    print(f"## start_transaction_ath C2 authorize error: {e}")
                    return None

            # [SPEED-FIX] 3. ส่ง StartTransaction ติดกันทันที!
            request = call.StartTransaction(
                connector_id=2, id_tag=self.myid_tag_ath_c2,
                meter_start=self.meter_start2,
                timestamp=convert_timeformat(datetime.now(timezone.utc).replace(tzinfo=timezone.utc)),
            )
            res = await self._dual_start_transaction(request) if self._dual_manager is not None else await self._call_txn_message_with_retry(request, msg_name="StartTransaction")
            if res is None:
                return
            
            txn_status = "Invalid"
            try:
                if isinstance(res.id_tag_info, dict): txn_status = res.id_tag_info.get("status", "Invalid")
                else: txn_status = getattr(res.id_tag_info, "status", "Invalid")
            except: pass

            if res.transaction_id > 0 and txn_status == "Accepted":
                self.transactionId_c2 = res.transaction_id
                self._arm_ev_stop_transaction_guard(2, res.transaction_id)
                self._remember_pin_start_origin(2, res.transaction_id, pin_autocharge_start)
                meter_energy.begin(2, res.transaction_id, self.meter_start2)
                self.active_id_tag_c2 = self.myid_tag_ath_c2
                self.rfid_start_received_c2 = 1
                self.rfid_recovery_pending_c2 = 0
                _write_file("translogs2.txt", self.transactionId_c2)
                _write_file("active_id_tag_c2.txt", self.active_id_tag_c2)
                _write_file("rfid_start_received_c2.txt", 1)
                print(f"## StartTransaction C2 SUCCESS: txn_id={res.transaction_id}")
            else:
                print(f"## StartTransaction C2 REJECTED: txn_id={res.transaction_id}, status={txn_status}")
                # [ABORT-STOP] the CSMS opened this id before refusing the tag - close it
                await self._abort_session_immediately(
                    2, transaction_id=getattr(res, "transaction_id", 0),
                    id_tag=self.myid_tag_ath_c2)
            return res

    # ========================================================================
    # StartTransaction (สำหรับ Remote Start - ZERO WAIT + SPEED FIX)
    # ========================================================================
    async def start_transaction(self, connector_id, **kwargs):
        pin_autocharge_start = False
        setattr(self, f"_pin_autocharge_start_intent_c{connector_id}", "")
        if connector_id == 1:
            try:
                meter_data = await asyncio.wait_for(
                    async_run_blocking(get_meter_value_pilot, self.my_meter_ip_c1),
                    timeout=2.0
                )
                self.meter_start1 = energy_wh(meter_data['energyTotal'])
            except Exception:
                if not hasattr(self, 'meter_start1'): self.meter_start1 = 0
                
            request = call.StartTransaction(
                connector_id=1, id_tag=self.myid_tag_c1,
                meter_start=self.meter_start1,
                timestamp=convert_timeformat(datetime.now(timezone.utc).replace(tzinfo=timezone.utc)),
            )
            res = await self._dual_start_transaction(request) if self._dual_manager is not None else await self._call_txn_message_with_retry(request, msg_name="StartTransaction")
            if res is None:
                return
            print(f"\n[START TRANSACTION Head 1] idTag={self.myid_tag_c1} meter_start={self.meter_start1} Wh txn_id={res.transaction_id}")

            # [START-AUTH-V13] StartTransaction.conf แจก transactionId เสมอ (required field)
            # ตัวตัดสินว่าบัตรได้รับอนุญาตจริงคือ idTagInfo.status — ต้อง Accepted เท่านั้น
            # ถ้า Invalid/Blocked/Expired ห้ามจ่ายไฟ (เดิม RemoteStart เช็คแค่ txn_id>0
            # ทำให้บัตร Invalid ยังชาร์จได้ ต่างจากเส้นทาง RFID ที่เช็ค status อยู่แล้ว)
            txn_status = "Invalid"
            try:
                if isinstance(res.id_tag_info, dict): txn_status = res.id_tag_info.get("status", "Invalid")
                else: txn_status = getattr(res.id_tag_info, "status", "Invalid")
            except Exception: pass

            if res.transaction_id > 0 and txn_status == "Accepted":
                self.transactionId_c1 = res.transaction_id
                self._arm_ev_stop_transaction_guard(1, res.transaction_id)
                self._remember_pin_start_origin(1, res.transaction_id, pin_autocharge_start)
                meter_energy.begin(1, res.transaction_id, self.meter_start1)
                self.active_id_tag_c1 = self.myid_tag_c1
                self.rfid_start_received_c1 = 0
                _write_file("translogs1.txt", self.transactionId_c1)
                _write_file("active_id_tag_c1.txt", self.active_id_tag_c1)
                _write_file("rfid_start_received_c1.txt", 0)
            else:
                print(f"## StartTransaction C1 REJECTED: txn_id={res.transaction_id}, status={txn_status} → aborting session")
                # [ABORT-STOP] the CSMS opened this id before refusing the tag - close it
                await self._abort_session_immediately(
                    1, transaction_id=getattr(res, "transaction_id", 0),
                    id_tag=self.myid_tag_c1)

        elif connector_id == 2:
            try:
                meter_data = await asyncio.wait_for(
                    async_run_blocking(get_meter_value_pilot, self.my_meter_ip_c2),
                    timeout=2.0
                )
                self.meter_start2 = energy_wh(meter_data['energyTotal'])
            except Exception:
                if not hasattr(self, 'meter_start2'): self.meter_start2 = 0

            request = call.StartTransaction(
                connector_id=2, id_tag=self.myid_tag_c2,
                meter_start=self.meter_start2,
                timestamp=convert_timeformat(datetime.now(timezone.utc).replace(tzinfo=timezone.utc)),
            )
            res = await self._dual_start_transaction(request) if self._dual_manager is not None else await self._call_txn_message_with_retry(request, msg_name="StartTransaction")
            if res is None:
                return
            print(f"[START TRANSACTION Head 2] txn_id={res.transaction_id}")

            # [START-AUTH-V13] เช็ค idTagInfo.status เหมือน C1 — ต้อง Accepted เท่านั้น
            txn_status = "Invalid"
            try:
                if isinstance(res.id_tag_info, dict): txn_status = res.id_tag_info.get("status", "Invalid")
                else: txn_status = getattr(res.id_tag_info, "status", "Invalid")
            except Exception: pass

            if res.transaction_id > 0 and txn_status == "Accepted":
                self.transactionId_c2 = res.transaction_id
                self._arm_ev_stop_transaction_guard(2, res.transaction_id)
                self._remember_pin_start_origin(2, res.transaction_id, pin_autocharge_start)
                meter_energy.begin(2, res.transaction_id, self.meter_start2)
                self.active_id_tag_c2 = self.myid_tag_c2
                self.rfid_start_received_c2 = 0
                _write_file("translogs2.txt", self.transactionId_c2)
                _write_file("active_id_tag_c2.txt", self.active_id_tag_c2)
                _write_file("rfid_start_received_c2.txt", 0)
            else:
                print(f"## StartTransaction C2 REJECTED: txn_id={res.transaction_id}, status={txn_status} → aborting session")
                # [ABORT-STOP] the CSMS opened this id before refusing the tag - close it
                await self._abort_session_immediately(
                    2, transaction_id=getattr(res, "transaction_id", 0),
                    id_tag=self.myid_tag_c2)

    async def _send_orphan_stop_transaction(self, connector_id, transaction_id, id_tag=None):
        """Close a transaction the CSMS opened but then refused the tag for.

        StartTransaction.conf carries a transactionId ALWAYS - it is a required
        field - and a refusal is signalled only through idTagInfo.status. So a
        rejected start still leaves a real open transaction on the CSMS side.
        Nothing local points at it (self.transactionId_cX is assigned only in the
        accepted branch), so this is the one place that can close it.

        Deliberately does NOT go through send_stop_transaction():
          - that one reads and writes self.transactionId_cX. Parking the id there
            would make _normalize_ocpp_status() read "latch armed + transaction
            open" as a stale latch and CLEAR it, so the connector would report
            Preparing instead of Finishing with the cable still in - and the clear
            is persisted, so a reboot would repeat it.
          - it would also let _emergency_stop_open_transactions() and
            remote_stop_transaction() see the borrowed id and fire a SECOND stop
            for it, and _execute_remote_stop_v3() would write it back to
            translogsN.txt where [INIT] resumes it on the next boot.
          - it resets session state this caller already handles itself.

        Never raises and never blocks for long: the abort has to finish even when
        the CSMS is unreachable.  Returns True when the close was sent.
        """
        try:
            txn = int(transaction_id)
        except (TypeError, ValueError):
            return False
        if txn <= 0:
            # Nothing was ever opened: the re-Authorize sites never sent a
            # StartTransaction, and this CSMS family answers -1 when it has no id.
            return False

        if not self._valid_id_tag(id_tag):
            fallback = getattr(self, f"active_id_tag_c{connector_id}", None)
            id_tag = (fallback if self._valid_id_tag(fallback)
                      else getattr(self, f"myid_tag_c{connector_id}", "Default"))

        # Nothing was delivered - the start was refused before power. Reporting
        # meter_start makes the CSMS book 0 Wh instead of a bogus total.
        meter_stop = getattr(self, f"meter_start{connector_id}", 0) or 0
        request = call.StopTransaction(
            meter_stop=meter_stop,
            timestamp=convert_timeformat(datetime.now(timezone.utc).replace(tzinfo=timezone.utc)),
            transaction_id=txn, id_tag=id_tag,
            reason="DeAuthorized", transaction_data=None,
        )
        logging.warning(
            "[ABORT-STOP] C%s closing orphan transaction %s (idTag=%s, "
            "CSMS issued it then refused the tag)", connector_id, txn, id_tag)

        # Bounded INLINE and cancelled at the cap - deliberately NOT handed to a
        # background task. ocpp's ChargePoint.call() holds one per-ChargePoint
        # _call_lock across the send AND the whole response wait
        # (charge_point.py: "async with self._call_lock"), so a send left running
        # keeps every other CALL queued behind it: the StatusNotification below,
        # Heartbeat, MeterValues, an RFID Authorize, the emergency
        # StopTransaction. Detaching the send bounds this function but not the
        # loop. wait_for cancels into call(), and that "async with" releases the
        # lock as it unwinds, so this cap is a real cap.
        # The websocket write is the first thing under that lock, so by the time
        # the cap expires the StopTransaction is already on the wire - only the
        # .conf is given up, and this CSMS closes the id either way.
        try:
            await asyncio.wait_for(
                self._send_stop_transaction(connector_id, request),
                timeout=ABORT_STOP_WAIT_SECONDS)
            logging.info("[ABORT-STOP] C%s transaction %s closed", connector_id, txn)
        except asyncio.TimeoutError:
            logging.warning(
                "[ABORT-STOP] C%s transaction %s sent but not confirmed within "
                "%ss; giving up the wait so the loop keeps running",
                connector_id, txn, ABORT_STOP_WAIT_SECONDS)
        except Exception as e:
            # Swallowed on purpose: _call_txn_message_with_retry re-raises once
            # its attempts are spent, and this runs inside main_run_program.
            # Letting it out would unwind start_transaction ->
            # _handle_status_notification -> main_run_program -> gather and tear
            # down the whole WebSocket session for BOTH connectors.
            logging.error("[ABORT-STOP] C%s could not close transaction %s: %s",
                          connector_id, txn, e)
        return True

    # Helper สำหรับหยุดเซสชันทันทีเมื่อโดน Reject
    async def _abort_session_immediately(self, connector_id, transaction_id=None, id_tag=None):
        self._dual_aborting.add(int(connector_id))
        print(f"## Aborting session for Connector {connector_id} due to CSMS Reject")
        # [SESSION-FINISHING] The charging attempt is over even though no transaction
        # was ever opened. The driver has to unplug before anything else can happen,
        # so report Finishing rather than Preparing while the cable stays in.
        self._set_session_finishing(connector_id, 1)

        # Resolve the idTag while it is still known: the reset block below sets
        # active_id_tag_cX to "Default", and the orphan stop runs after it.
        if not self._valid_id_tag(id_tag):
            _known_tag = getattr(self, f"active_id_tag_c{connector_id}", None)
            if self._valid_id_tag(_known_tag):
                id_tag = _known_tag

        # The reset runs BEFORE the awaits below, not after. remote_start_
        # transaction() and the RFIDCommand handler live in the cp.start() task,
        # so a start accepted while this coroutine is suspended would otherwise be
        # wiped by this block on the way out: remote_start_received_cX would go to
        # 0 while myremotestart_cX stayed 1, and the loop would pulse the PLC start
        # relay without ever sending StartTransaction. Clearing first means such a
        # start survives and is honoured on the next pass.
        if connector_id == 1:
            self.transactionId_c1 = 0
            self.accepted_tag1 = '0'
            self.start_sendMeterValue_SW1 = 1
            self.remote_start_received_c1 = 0          # [REMOTE-ONLY] ต้องสั่ง RemoteStart ใหม่หลัง reject
            self.rfid_start_received_c1 = 0
            self.active_id_tag_c1 = "Default"
            _write_file("translogs1.txt", 0)
            _write_file("start_sendMeterValue_SW1.txt", 1)
            _write_file("remote_start_received_c1.txt", 0)
            _write_file("rfid_start_received_c1.txt", 0)
            _write_file("active_id_tag_c1.txt", "Default")
        else:
            self.transactionId_c2 = 0
            self.accepted_tag2 = '0'
            self.start_sendMeterValue_SW2 = 1
            self.remote_start_received_c2 = 0          # [REMOTE-ONLY] ต้องสั่ง RemoteStart ใหม่หลัง reject
            self.rfid_start_received_c2 = 0
            self.active_id_tag_c2 = "Default"
            _write_file("translogs2.txt", 0)
            _write_file("start_sendMeterValue_SW2.txt", 1)
            _write_file("remote_start_received_c2.txt", 0)
            _write_file("rfid_start_received_c2.txt", 0)
            _write_file("active_id_tag_c2.txt", "Default")
        # [START-LATCH] R3.1.6: also before the awaits, for the same reason. A pending pulse
        # belongs to the start being aborted; start_transaction_SWN=3 left here refused every
        # later start. The CSMS claim is released below, after the orphan stop, as before.
        self._clear_remote_start_latch(int(connector_id), "start aborted", release_claim=False)

        ip_addr = self.myip_adr_c1 if connector_id == 1 else self.myip_adr_c2
        await async_remote_stop_plc(ip_addr, 1)
        await asyncio.sleep(2)
        await async_remote_stop_plc(ip_addr, 0)

        # [ABORT-STOP] Close the transaction the CSMS just issued, before the
        # StatusNotification goes out.
        await self._send_orphan_stop_transaction(connector_id, transaction_id, id_tag)
        if self._dual_manager is not None:
            self._dual_manager.release_pending(int(connector_id))

        # [SESSION-FINISHING] Re-arm after the awaits above, not only before them.
        # _clear_stale_stop_latch_for_new_start() sets session_finishing_cX = 0 AND
        # persists it, and it runs from remote_start_transaction() and the
        # RFIDCommand handler in the cp.start() task - concurrent with this
        # coroutine. Its "is this latch stale?" test is _active_transaction_id() <= 0,
        # which is always true here because the orphan id is deliberately never
        # parked in local state. Without re-arming, the notification below would go
        # out as Preparing for a connector whose session just ended with the cable
        # still in, and the persisted 0 would repeat it after a reboot.
        self._set_session_finishing(connector_id, 1)
        await self.status_notification_with_status(connector_id, "Preparing")
        self._dual_aborting.discard(int(connector_id))

    # ========================================================================
    # StopTransaction (SPEED FIX - Async Meter Read)
    # ========================================================================
    async def _read_stop_energy(self, connector_id, transaction_id):
        meter_ip = getattr(self, f"my_meter_ip_c{connector_id}")
        # The persisted baseline belongs to this exact transaction, including
        # after a restart when the in-memory meter_start may not be restored.
        snapshot = meter_energy.snapshot(connector_id, transaction_id)
        baseline = int(snapshot['start']) if snapshot is not None else int(
            getattr(self, f"meter_start{connector_id}", 0) or 0)
        for attempt in (1, 2):
            try:
                data = await asyncio.wait_for(
                    async_run_blocking(get_meter_value_pilot, meter_ip), timeout=2.0)
                value = meter_energy.observe(
                    connector_id, transaction_id, data['energyTotal'],
                    baseline)
                if value != baseline:
                    logging.info('[METER-STOP-FRESH] C%s txn=%s attempt=%s start=%s stop=%s Wh',
                                 connector_id, transaction_id, attempt, baseline, value)
                    return value
                if attempt == 2:
                    # A genuine zero-energy session is valid. Never invent an
                    # increment just to make start and stop differ.
                    logging.warning('[METER-STOP-EQUAL] C%s txn=%s start=stop=%s Wh '
                                    'after two reads; no measured energy increase',
                                    connector_id, transaction_id, value)
                    return value
                logging.warning('[METER-STOP-EQUAL-RETRY] C%s txn=%s start=stop=%s Wh; '
                                'reading meter again', connector_id, transaction_id, value)
            except Exception as error:
                logging.warning(
                    "[METER-STOP] C%s txn=%s attempt=%s failed: %s: %s",
                    connector_id, transaction_id, attempt, type(error).__name__, error,
                    exc_info=True)
            if attempt == 1:
                await asyncio.sleep(0.2)
        snapshot = meter_energy.snapshot(connector_id, transaction_id)
        if snapshot is not None:
            logging.warning(
                "[METER-STOP-FALLBACK] C%s txn=%s using last valid %s Wh age=%.1fs; "
                "final energy unavailable, may undercount",
                connector_id, transaction_id, snapshot['wh'],
                time.time() - snapshot['observed_at'])
            return int(snapshot['wh'])
        # Without a same-transaction sample there is no measured final value.
        # Keep closing the session but make this degraded case explicitly visible.
        logging.error("[METER-STOP-NO-SAMPLE] C%s txn=%s no valid cached energy; "
                      "using start=%s Wh, energy total unknown",
                      connector_id, transaction_id, baseline)
        return baseline

    async def send_stop_transaction(self, connector_id, reasonf, **kwargs):
        # [STOP-GUARD] No open transaction means there is nothing to stop. Without
        # this a caller could emit a StopTransaction carrying transactionId 0 - a
        # transaction the CSMS never issued - and arm the SESSION-FINISHING latch
        # for a session that never ran.
        # The id is captured here and used to build the request below. Reading
        # self.transactionId_cX again after the meter await would let a concurrent
        # task that has just finished its own stop turn it into 0 mid-flight.
        active_txn = self._active_transaction_id(connector_id)
        if active_txn <= 0:
            logging.warning(
                "[STOP-GUARD] C%s %s stop ignored: no active transaction",
                connector_id, reasonf,
            )
            return

        if connector_id == 1:
            self.start_sendMeterValue_SW1 = 1
            _write_file("start_sendMeterValue_SW1.txt", 1)
            
            self.meter_stop1 = await self._read_stop_energy(1, active_txn)
                
            timestamp = convert_timeformat(datetime.now(timezone.utc).replace(tzinfo=timezone.utc))
            self.timestamp1 = datetime.now(pytz.timezone('Asia/Bangkok')).strftime("%Y-%m-%dT%H:%M:%S.%f")
            client.publish(f"OCPP/{OCPPSTATION}/meter", json.dumps({
                "meter1": self.meter_stop1, "meter2": self.meter_stop2,
                "timestamp1": self.timestamp1, "timestamp2": self.timestamp2, "timestamp": timestamp
            }))

            stop_txn_cfg = _normalize_csv_string(getattr(self, "StopTxnSampledData_cfg", "0"), default="0")
            stop_measurands = [] if stop_txn_cfg == "0" else _parse_csv_list(stop_txn_cfg)
            stop_id_tag = (self.active_id_tag_c1 if self._valid_id_tag(self.active_id_tag_c1)
                           else self.myid_tag_c1)

            if not stop_measurands:
                request = call.StopTransaction(
                    meter_stop=self.meter_stop1, timestamp=timestamp,
                    transaction_id=active_txn, id_tag=stop_id_tag,
                    reason=reasonf, transaction_data=None
                )
            else:
                try:
                    live_data = await asyncio.wait_for(async_run_blocking(get_livemeasure_pilot, self.my_meter_ip_c1), timeout=2.0)
                except Exception as error:
                    logging.warning("[METER-STOP-DATA] C1 unavailable: %s: %s",
                                    type(error).__name__, error, exc_info=True)
                    live_data = None
                soc = await async_get_soc(plc, self.myip_adr_c1)
                sampled = self._build_sampled_values(live_data, soc, stop_measurands) if live_data is not None else []
                request = call.StopTransaction(
                    meter_stop=self.meter_stop1, timestamp=timestamp,
                    transaction_id=active_txn, id_tag=stop_id_tag,
                    reason=reasonf,
                    transaction_data=[{'timestamp': convert_timeformat(datetime.now(timezone.utc).replace(tzinfo=timezone.utc)), 'sampledValue': sampled}] if sampled else None
                )

            # [SESSION-FINISHING] Every way a session can end funnels through here
            # (emergency, RemoteStop, local/RFID, PLC finishing, Reset, PowerLoss), so
            # this is the one place that has to remember "this connector finished a
            # session". _normalize_ocpp_status() turns that into Finishing for as long
            # as the PLC still reports the cable connected.
            # Recorded BEFORE the send: _call_txn_message_with_retry re-raises once the
            # retries are exhausted, and a session that ends while the WebSocket is down
            # must still be remembered - otherwise the next boot reports Preparing.
            # The write is idempotent and every clear path still releases it.
            # Remember WHICH transaction armed the latch, so a status poll landing
            # while this stop is still on the wire does not mistake the id being
            # closed for a new session and clear the latch.
            self._stop_latched_txn_c1 = active_txn
            self._set_session_finishing(1, 1)
            await self._send_stop_transaction(1, request)
            self._clear_pending_local_stop(1)
            self.accepted_tag1 = '0'
            self.transactionId_c1 = 0
            self.remote_start_received_c1 = 0              # [REMOTE-ONLY]
            self.rfid_start_received_c1 = 0
            self.active_id_tag_c1 = "Default"
            _write_file("translogs1.txt", 0)
            _write_file("remote_start_received_c1.txt", 0)
            _write_file("rfid_start_received_c1.txt", 0)
            _write_file("active_id_tag_c1.txt", "Default")

        elif connector_id == 2:
            self.start_sendMeterValue_SW2 = 1
            _write_file("start_sendMeterValue_SW2.txt", 1)
            
            self.meter_stop2 = await self._read_stop_energy(2, active_txn)
                
            timestamp = convert_timeformat(datetime.now(timezone.utc).replace(tzinfo=timezone.utc))
            self.timestamp2 = datetime.now(pytz.timezone('Asia/Bangkok')).strftime("%Y-%m-%dT%H:%M:%S.%f")
            client.publish(f"OCPP/{OCPPSTATION}/meter", json.dumps({
                "meter1": self.meter_stop1, "meter2": self.meter_stop2,
                "timestamp1": self.timestamp1, "timestamp2": self.timestamp2, "timestamp": timestamp
            }))

            stop_txn_cfg = _normalize_csv_string(getattr(self, "StopTxnSampledData_cfg", "0"), default="0")
            stop_measurands = [] if stop_txn_cfg == "0" else _parse_csv_list(stop_txn_cfg)
            stop_id_tag = (self.active_id_tag_c2 if self._valid_id_tag(self.active_id_tag_c2)
                           else self.myid_tag_c2)

            if not stop_measurands:
                request = call.StopTransaction(
                    meter_stop=self.meter_stop2, timestamp=timestamp,
                    transaction_id=active_txn, id_tag=stop_id_tag,
                    reason=reasonf, transaction_data=None
                )
            else:
                try:
                    live_data = await asyncio.wait_for(async_run_blocking(get_livemeasure_pilot, self.my_meter_ip_c2), timeout=2.0)
                except Exception as error:
                    logging.warning("[METER-STOP-DATA] C2 unavailable: %s: %s",
                                    type(error).__name__, error, exc_info=True)
                    live_data = None
                soc = await async_get_soc(plc, self.myip_adr_c2)
                sampled = self._build_sampled_values(live_data, soc, stop_measurands) if live_data is not None else []
                request = call.StopTransaction(
                    meter_stop=self.meter_stop2, timestamp=timestamp,
                    transaction_id=active_txn, id_tag=stop_id_tag,
                    reason=reasonf,
                    transaction_data=[{'timestamp': convert_timeformat(datetime.now(timezone.utc).replace(tzinfo=timezone.utc)), 'sampledValue': sampled}] if sampled else None
                )

            # [SESSION-FINISHING] See connector 1 - one latch for every session end,
            # recorded before the send so a WebSocket outage cannot lose it.
            # See connector 1: the latch belongs to this id until a new one appears.
            self._stop_latched_txn_c2 = active_txn
            self._set_session_finishing(2, 1)
            await self._send_stop_transaction(2, request)
            self._clear_pending_local_stop(2)
            self.accepted_tag2 = '0'
            self.transactionId_c2 = 0
            self.remote_start_received_c2 = 0              # [REMOTE-ONLY]
            self.rfid_start_received_c2 = 0
            self.active_id_tag_c2 = "Default"
            _write_file("translogs2.txt", 0)
            _write_file("remote_start_received_c2.txt", 0)
            _write_file("rfid_start_received_c2.txt", 0)
            _write_file("active_id_tag_c2.txt", "Default")

    def _build_sampled_values(self, live_data, soc, measurands):
        corrected_power = self._corrected_power_kw(live_data.get('power', 0))
        source = {
            'Voltage':                         {'unit': 'V',       'value': str(live_data['voltage'])},
            'Current.Import':                  {'unit': 'A',       'value': str(live_data['current'])},
            'Energy.Active.Import.Register':   {'unit': 'kWh',     'value': str(live_data['energyTotal'])},
            'Power.Active.Import':             {'unit': 'kW',      'value': str(corrected_power)},
            'SoC':                             {'unit': 'Percent', 'value': str(soc)},
            'Power.Offered':                   {'unit': 'kW',      'value': str(RATED_POWER_W / 1000)},
            'Current.Offered':                 {'unit': 'A',       'value': str(RATED_CURRENT_A)},
        }
        return [{'measurand': m, **source[m]} for m in measurands if m in source]

    # ========================================================================
    # MeterValues (SPEED FIX - Async Meter Read)
    # ========================================================================
    def _meter_values_remote_start_allowed(self, connector_id):
        """Return whether this connector may emit MeterValues for this session.

        RemoteStartOnly is a deployment policy. When it is disabled, preserve
        every existing local/RFID/AutoCharge path. When it is enabled, an OCPP
        RemoteStart or an explicitly authorized RFID session may emit values.
        Both origins use persisted per-connector latches across reconnects.
        """
        if not getattr(self, "remote_start_only", False):
            return True
        if connector_id == 1:
            remote_start_received = getattr(self, "remote_start_received_c1", 0)
        elif connector_id == 2:
            remote_start_received = getattr(self, "remote_start_received_c2", 0)
        else:
            return False
        try:
            if int(remote_start_received or 0) == 1:
                return True
        except (TypeError, ValueError):
            pass

        # topic/RFIDCommand is intentionally allowed independently from the
        # RemoteStartOnly policy. Once its Authorize + StartTransaction path has
        # established an active transaction, it must receive the same periodic
        # MeterValues service as a RemoteStart transaction. This only changes the
        # meter gate; the existing RFID stop path and its Local reason stay intact.
        return (
            self._active_transaction_id(connector_id) > 0
            and self._rfid_session_authorized(connector_id)
        )

    def _log_meter_values_remote_start_blocked(self, connector_id, transaction_id=0,
                                                source="send"):
        """Log a blocked MeterValues path at most once every 10 seconds/head."""
        if connector_id == 1:
            log_ts_attr = "_meter_remote_gate_log_ts_c1"
        elif connector_id == 2:
            log_ts_attr = "_meter_remote_gate_log_ts_c2"
        else:
            return
        now = time.time()
        if now - float(getattr(self, log_ts_attr, 0.0) or 0.0) < 10.0:
            return
        setattr(self, log_ts_attr, now)
        logging.warning(
            "[METER-REMOTE-GATE] C%s holding MeterValues: "
            "RemoteStartOnly=1 but no RemoteStart or authorized RFID session "
            "(txn=%s source=%s)",
            connector_id, transaction_id or 0, source,
        )

    async def send_meter_values(self, connector_id):
        if connector_id not in (1, 2):
            return
        # [METER-REMOTE-GATE] Hard guard for every call path, including
        # TriggerMessage, startup/orphan cleanup and the periodic watchdog.
        if not self._meter_values_remote_start_allowed(connector_id):
            transaction_id = getattr(self, f"transactionId_c{connector_id}", 0)
            self._log_meter_values_remote_start_blocked(
                connector_id, transaction_id, source="send_meter_values")
            return

        if connector_id == 1:
            try:
                file_txn = int(open("translogs1.txt").read())
                if file_txn > 0: self.transactionId_c1 = file_txn
            except: pass
            if self.start_sendMeterValue_SW1 != 2: return
            
            meter_txn = self.transactionId_c1
            try:
                live_data = await asyncio.wait_for(async_run_blocking(get_livemeasure_pilot, self.my_meter_ip_c1), timeout=2.0)
                if self.transactionId_c1 != meter_txn:
                    return
                meter_energy.observe(1, meter_txn, live_data['energyTotal'], self.meter_start1)
            except Exception as error:
                logging.warning("[METER-VALUES-SKIP] C1 txn=%s read invalid: %s: %s",
                                meter_txn, type(error).__name__, error, exc_info=True)
                return  # Missing measurement is not a zero cumulative register.
            soc       = await async_get_soc(plc, self.myip_adr_c1)
            TargetV   = await async_get_requested_voltage(plc, self.myip_adr_c1)
            TargetC   = await async_get_requested_current(plc, self.myip_adr_c1)
            limit     = self._get_effective_limit(connector_id)
            item_list = self._build_meter_item_list(
                live_data, soc, limit, TargetV, TargetC, LATEST_EDGEBOX_TEMP_C,
                current_limit=self._ocpp_protocol().smart.effective_limits(connector_id)[0]["A"]
            )

        elif connector_id == 2:
            try:
                file_txn = int(open("translogs2.txt").read())
                if file_txn > 0: self.transactionId_c2 = file_txn
            except: pass
            if self.start_sendMeterValue_SW2 != 2: return
            
            meter_txn = self.transactionId_c2
            try:
                live_data = await asyncio.wait_for(async_run_blocking(get_livemeasure_pilot, self.my_meter_ip_c2), timeout=2.0)
                if self.transactionId_c2 != meter_txn:
                    return
                meter_energy.observe(2, meter_txn, live_data['energyTotal'], self.meter_start2)
            except Exception as error:
                logging.warning("[METER-VALUES-SKIP] C2 txn=%s read invalid: %s: %s",
                                meter_txn, type(error).__name__, error, exc_info=True)
                return  # Missing measurement is not a zero cumulative register.
            soc       = await async_get_soc(plc, self.myip_adr_c2)
            TargetV   = await async_get_requested_voltage(plc, self.myip_adr_c2)
            TargetC   = await async_get_requested_current(plc, self.myip_adr_c2)
            limit     = self._get_effective_limit(connector_id)
            item_list = self._build_meter_item_list(
                live_data, soc, limit, TargetV, TargetC, LATEST_EDGEBOX_TEMP_C,
                current_limit=self._ocpp_protocol().smart.effective_limits(connector_id)[0]["A"]
            )
        else:
            return

        txn_id = self.transactionId_c1 if connector_id == 1 else self.transactionId_c2
        request = call.MeterValues(
            connector_id=connector_id,
            transaction_id=int(txn_id) if txn_id > 0 else None,
            meter_value=[{
                'timestamp': convert_timeformat(datetime.now(timezone.utc).replace(tzinfo=timezone.utc)),
                'sampledValue': item_list
            }]
        )
        await self._call_txn_message_with_retry(request, msg_name="MeterValues")

    def _get_effective_limit(self, connector_id: int) -> float:
        return float(self._ocpp_protocol().smart.effective_limits(connector_id)[0]["W"])

    def _build_meter_item_list(self, live_data, soc, limit, target_v, target_c, edgebox_temp_c=None, current_limit=None):
        meter_cfg = _normalize_csv_string(getattr(self, "MeterValuesSampledData_cfg", "0"), default="0")
        measurands = _parse_csv_list(meter_cfg)
        default_meas = ['Voltage', 'Current.Import', 'Energy.Active.Import.Register',
                        'Power.Active.Import', 'SoC', 'Power.Offered', 'Current.Offered',
                        'Temperature']
        if not measurands or (len(measurands) == 1 and measurands[0] == '0'):
            measurands = default_meas

        # Sanitize: ถ้าเป็น None → skip (SoC) หรือ fallback "0" (ค่าหลัก)
        def _num_or_none(v):
            if v is None: return None
            s = str(v).strip()
            if s == "" or s.lower() == "none": return None
            try:
                float(s); return s
            except (ValueError, TypeError):
                return None

        v_voltage = _num_or_none(live_data.get('voltage'))
        v_current = _num_or_none(live_data.get('current'))
        v_energy  = _num_or_none(live_data.get('energyTotal'))
        _pwr_raw  = live_data.get('power')
        v_power   = _num_or_none(self._corrected_power_kw(_pwr_raw)) if isinstance(_pwr_raw, (int, float)) else None
        v_soc     = _num_or_none(soc)
        v_limit   = _num_or_none(abs(limit / 1000)) if isinstance(limit, (int, float)) else None
        v_temp    = _num_or_none(edgebox_temp_c)
        # Current.Offered = เพดานกระแสที่ "เสนอ" ณ ขณะนั้น (ไม่ใช่กระแสที่วัดได้)
        #   = min( rating ของหัวชาร์จ , limit_W / แรงดันปัจจุบัน )
        try:
            _volt_now = float(v_voltage) if v_voltage else 0.0
            if isinstance(limit, (int, float)) and _volt_now > 0:
                _cur_off = min(float(RATED_CURRENT_A if current_limit is None else current_limit), abs(limit) / _volt_now)
                v_cur_off = str(int(round(_cur_off)))
            else:
                v_cur_off = str(int(RATED_CURRENT_A if current_limit is None else current_limit))
        except Exception:
            v_cur_off = None

        cdict = {
            'Voltage':                        {'unit': 'V',       'value': v_voltage},
            'Current.Import':                 {'unit': 'A',       'value': v_current},
            'Energy.Active.Import.Register':  {'unit': 'kWh',     'value': v_energy},
            'Power.Active.Import':            {'unit': 'kW',      'value': v_power},
            'SoC':                            {'unit': 'Percent', 'value': v_soc},
            'Power.Offered':                  {'unit': 'kW',      'value': v_limit},
            'Current.Offered':                {'unit': 'A',       'value': v_cur_off},
            'Temperature':                    {'unit': 'Celsius', 'value': v_temp},
        }

        FALLBACK_ZERO = {'Voltage', 'Current.Import', 'Energy.Active.Import.Register',
                         'Power.Active.Import', 'Power.Offered', 'Current.Offered'}

        item_list = []
        for m in measurands:
            if m not in cdict: continue
            entry = dict(cdict[m])
            if entry.get('value') is None:
                if m in FALLBACK_ZERO:
                    entry['value'] = "0"
                else:
                    continue
            item_list.append({'measurand': m, **entry})

        if not item_list:
            for m in default_meas:
                entry = dict(cdict.get(m, {}))
                if entry.get('value') is None:
                    if m in FALLBACK_ZERO: entry['value'] = "0"
                    else: continue
                item_list.append({'measurand': m, **entry})

        tv = _num_or_none(target_v)
        tc = _num_or_none(target_c)
        if tv is not None:
            item_list.append({'measurand': 'Voltage',        'unit': 'V', 'value': tv, 'location': 'EV'})
        if tc is not None:
            item_list.append({'measurand': 'Current.Import', 'unit': 'A', 'value': tc, 'location': 'EV'})
        return item_list

    # ========================================================================
    # Reset (OPTION B: Stop active transactions cleanly before reboot)
    # ========================================================================
    @on(Action.reset)
    async def hard_Reset(self, **kwargs):
        """เมื่อ CSMS สั่ง Reset:
        1. ตอบ Accepted ทันที (CSMS ไม่ timeout)
        2. Background task: หยุด active transactions → reboot
        
        ผลดี: หลัง reboot กลับมา ไม่มี transaction ค้าง, CSMS state sync กับ charger
              ผู้ใช้ต้อง tap card ใหม่เพื่อเริ่ม session ใหม่
        """
        reset_type = kwargs.get('type', 'Soft')
        logging.info(f"[RESET] received type={reset_type} — stopping active txns before reboot")
        asyncio.create_task(self._reset_cleanup_and_reboot(reset_type))
        return call_result.Reset(status='Accepted')

    async def _reset_cleanup_and_reboot(self, reset_type):
        """หยุด transaction ที่ active ทั้งหมด แล้วค่อย reboot"""
        # หน่วง 0.5s ให้ ResetPayload ถูกส่งออกไปก่อน
        await asyncio.sleep(0.5)

        reason = 'SoftReset' if reset_type == 'Soft' else 'HardReset'

        # ── เขียน flag บอก [INIT] หลัง reboot ว่า "ห้าม resume txn เก่า" ──
        # กรณี StopTransaction ส่งล้มเหลว (network ล่ม) translogs จะค้างอยู่
        # flag นี้ทำให้แม้ translogs จะค้าง [INIT] ก็จะ discard ไม่ resume
        try:
            _write_file("reset_pending.flag", reset_type)
            logging.info(f"[RESET] reset_pending.flag written ({reset_type})")
        except Exception as e:
            logging.error(f"[RESET] cannot write reset_pending.flag: {e}")

        # Stop C1 ถ้ามี transaction active
        try:
            file_txn_1 = int(open("translogs1.txt").read().strip() or "0") if os.path.exists("translogs1.txt") else 0
        except Exception:
            file_txn_1 = 0
        mem_txn_1 = int(self.transactionId_c1 or 0)
        txn_c1 = max(file_txn_1, mem_txn_1)
        if txn_c1 > 0:
            try:
                logging.info(f"[RESET] C1 stopping txn={txn_c1} (reason={reason})")
                self.transactionId_c1 = txn_c1  # sync memory
                await async_remote_stop_plc(self.myip_adr_c1, 1)
                await self.send_stop_transaction(1, reasonf=reason)
                await async_remote_stop_plc(self.myip_adr_c1, 0)
                self._clear_connector_state(1)
                logging.info(f"[RESET] C1 txn={txn_c1} stopped cleanly ✓")
            except Exception as e:
                logging.error(f"[RESET] C1 stop error: {e}")
        else:
            logging.info("[RESET] C1 no active txn — skip")

        # Stop C2 ถ้ามี transaction active
        try:
            file_txn_2 = int(open("translogs2.txt").read().strip() or "0") if os.path.exists("translogs2.txt") else 0
        except Exception:
            file_txn_2 = 0
        mem_txn_2 = int(self.transactionId_c2 or 0)
        txn_c2 = max(file_txn_2, mem_txn_2)
        if txn_c2 > 0:
            try:
                logging.info(f"[RESET] C2 stopping txn={txn_c2} (reason={reason})")
                self.transactionId_c2 = txn_c2
                await async_remote_stop_plc(self.myip_adr_c2, 1)
                await self.send_stop_transaction(2, reasonf=reason)
                await async_remote_stop_plc(self.myip_adr_c2, 0)
                self._clear_connector_state(2)
                logging.info(f"[RESET] C2 txn={txn_c2} stopped cleanly ✓")
            except Exception as e:
                logging.error(f"[RESET] C2 stop error: {e}")
        else:
            logging.info("[RESET] C2 no active txn — skip")

        # ── FINISHING-FIX: ส่ง StatusNotification = Finishing แบบเดียวกับ app/RFID stop ──
        # ใช้ status_notification_usiLink() ซึ่ง hardcode 'Finishing' (line 975-982)
        # — pattern เดียวกับ _handle_remote_stop_c1/c2 (line 1943) และ
        #    _handle_finishing_c1/c2 (line 1997) ที่ใช้กันใน flow stop ปกติ
        # — ไม่ query PLC เพราะ PLC อาจอยู่ใน transient SuspendedEV หลัง stop pulse
        # — ส่งเฉพาะ connector ที่มี active txn ตอน reset (เหมือน flow ปกติที่
        #   เช็ค if transactionId_cX > 0 ก่อนส่ง Finishing)
        try:
            if txn_c1 > 0:
                await self.status_notification_usiLink(1)
            if txn_c2 > 0:
                await self.status_notification_usiLink(2)
        except Exception as e:
            logging.warning(f"[RESET] final StatusNotification error: {e}")

        await asyncio.sleep(1)

        # ดำเนินการ reboot (Soft vs Hard)
        logging.info(f"[RESET] executing {reset_type} reboot now")
        if reset_type == 'Soft':
            os.system("sudo reboot")
        else:
            try:
                GPIO.setmode(GPIO.BCM)
                GPIO.setup(23, GPIO.OUT)
                GPIO.output(23, 1)
                time.sleep(5)
                GPIO.output(23, 0)
            except Exception as e:
                logging.error(f"[RESET] GPIO error: {e}")
            await asyncio.sleep(5)
            os.system("sudo reboot")

    def _clear_connector_state(self, head: int):
        """เคลียร์ state file ของ connector หลัง stop transaction
        ใช้ pattern เดียวกับ _reset_connector ใน main() boot flow
        """
        self._clear_pending_local_stop(head)
        files = {
            f"translogs{head}.txt": "0",
            f"block{head}.txt": "0",
            f"my_remote_stop_c{head}.txt": "0",
            f"remote_stop_finishing_c{head}.txt": "0",
            f"finshing_sent_c{head}.txt": "0",
            f"finshing_sent_sw_c{head}.txt": "1",
            f"start_transaction_SW{head}.txt": "1",
            f"start_sendMeterValue_SW{head}.txt": "1",
            f"remote_start_received_c{head}.txt": "0",
            f"rfid_start_received_c{head}.txt": "0",
            f"local_status_session_c{head}.txt": "0",   # [LOCAL-STATUS-V16]
            f"active_id_tag_c{head}.txt": "Default",
            f"emergency_sent_c{head}.txt": "1",
            f"emergency_sent_Sw_c{head}.txt": "1",
        }
        if head == 1:
            files.update({"block_sw1_C1.txt": "0", "block_sw2_C1.txt": "1", "block_sw3_C1.txt": "1"})
            self.block_sw1_C1, self.block_sw2_C1, self.block_sw3_C1 = 0, 1, 1
            self.start_transaction_SW1 = 1
            self.start_sendMeterValue_SW1 = 1
            self.accepted_tag1 = '0'
            self.my_remote_stop_c1 = 0
            self.remote_stop_finishing_c1 = 0
            self.remote_start_received_c1 = 0
            self.rfid_start_received_c1 = 0
            self.active_id_tag_c1 = "Default"
            self.block1 = 0
        else:
            files.update({"block_sw1.txt": "0", "block_sw2.txt": "1", "block_sw3.txt": "1"})
            self.block_sw1, self.block_sw2, self.block_sw3 = 0, 1, 1
            self.start_transaction_SW2 = 1
            self.start_sendMeterValue_SW2 = 1
            self.accepted_tag2 = '0'
            self.my_remote_stop_c2 = 0
            self.remote_stop_finishing_c2 = 0
            self.remote_start_received_c2 = 0
            self.rfid_start_received_c2 = 0
            self.active_id_tag_c2 = "Default"
            self.block2 = 0
        for fname, val in files.items():
            _write_file(fname, val)

    # ========================================================================
    # main_run_program
    # ========================================================================
    async def main_run_program(self, interval):
        while True:
            iCpStateCss1, usiLinkStateCcs1 = plc.get_connector_status(self.myip_adr_c1)
            iCpStateCss2, usiLinkStateCcs2 = plc.get_connector_status(self.myip_adr_c2)
            # The final pulse guard also covers queued local/AutoCharge intents.
            # If a front-panel/PLC action bypassed Python, request the existing
            # local stop path before it can open a new OCPP transaction.
            for cid, physical in ((1, iCpStateCss1), (2, iCpStateCss2)):
                if (self._active_transaction_id(cid) <= 0
                        and physical in ("Charging", "SuspendedEV", "SuspendedEVSE")
                        and not self._ocpp_hardware_start_allowed(cid)):
                    stamp = getattr(self, f"_ocpp_inhibit_stop_{cid}", 0)
                    if time.monotonic() - stamp > 5:
                        setattr(self, f"_ocpp_inhibit_stop_{cid}", time.monotonic())
                        await self._abort_session_immediately(cid)

            # [RFID-OCPP-V7] The MQTT RFID callback runs outside this loop. Pull its
            # accepted tag into the OCPP state machine before evaluating Charging.
            self._sync_rfid_command_latch(1, iCpStateCss1)
            self._sync_rfid_command_latch(2, iCpStateCss2)

            # EVCCID callbacks only enqueue. Dispatch OCPP Authorize tasks here
            # without awaiting them so the 0.2 s connector loop stays responsive.
            self._drain_autocharge_events()

            # [LOCAL-STATUS-V16] คำสั่ง Start/Stop จากหน้าจอมาถึงบน paho thread
            # เช่นกัน ดึงเข้ามาก่อนคำนวณ pending ด้านล่าง
            local_evt_c1 = self._sync_local_status_latch(1, iCpStateCss1)
            local_evt_c2 = self._sync_local_status_latch(2, iCpStateCss2)
            # status_notification_usiLink() ส่ง Finishing และตั้ง session_finishing
            # latch ให้ด้วย -> สถานะจะค้าง Finishing จนกว่าจะถอดสาย แล้ว
            # _normalize_ocpp_status() จะปลดเองเมื่อ PLC ขึ้น Available
            if local_evt_c1 == "stop":
                await self.status_notification_usiLink(1)
            if local_evt_c2 == "stop":
                await self.status_notification_usiLink(2)

            # [EV-STOP] Explicit local/remote origins above get first refusal.
            # Only an otherwise-unowned live shutdown event may become
            # StopTransaction(reason=EVDisconnected).
            await self._drain_ev_stop_events()

            # ---- Remote Start C1 ----
            if self.start_trans_SW_c1 == 1:
                if self.myremotestart_c1 == 1:
                    await async_remote_start_plc(self.myip_adr_c1, 1)
                    await asyncio.sleep(2)
                    await async_remote_start_plc(self.myip_adr_c1, 0)
                    self.start_transaction_SW1 = 3
                    self.myremotestart_c1 = 0
                    self.start_trans_SW_c1 = 2
                    self._remote_start_latch_mono_c1 = time.monotonic()   # [START-LATCH]
                    _write_file("start_transaction_SW1.txt", 3)
                    _write_file("myremotestart_c1.txt", 0)
                    _write_file("start_trans_SW_c1.txt", 2)
            else:
                # [DEADLOCK-FIX] re-arm กลับ ready เสมอ ไม่ผูกกับ myremotestart_c1
                # การยิงด้านบน clear myremotestart_c1=0 อยู่แล้ว -> request เดียวยิงครั้งเดียว ไม่ยิงซ้ำ
                # ถ้ามี request ใหม่ค้าง (==1) จะถูกยิงใน loop ถัดไป -> สถานะ (2,1) มีทางออกเสมอ
                if self.start_trans_SW_c1 != 1:
                    self.start_trans_SW_c1 = 1
                    _write_file("start_trans_SW_c1.txt", 1)

            # ---- Remote Start C2 ----
            if self.start_trans_SW_c2 == 1:
                if self.myremotestart_c2 == 1:
                    await async_remote_start_plc(self.myip_adr_c2, 1)
                    await asyncio.sleep(2)
                    await async_remote_start_plc(self.myip_adr_c2, 0)
                    self.start_transaction_SW2 = 3
                    self.myremotestart_c2 = 0
                    self.start_trans_SW_c2 = 2
                    self._remote_start_latch_mono_c2 = time.monotonic()   # [START-LATCH]
                    _write_file("start_transaction_SW2.txt", 3)
                    _write_file("myremotestart_c2.txt", 0)
                    _write_file("start_trans_SW_c2.txt", 2)
            else:
                # [DEADLOCK-FIX] re-arm กลับ ready เสมอ ไม่ผูกกับ myremotestart_c2
                # การยิงด้านบน clear myremotestart_c2=0 อยู่แล้ว -> request เดียวยิงครั้งเดียว ไม่ยิงซ้ำ
                # ถ้ามี request ใหม่ค้าง (==1) จะถูกยิงใน loop ถัดไป -> สถานะ (2,1) มีทางออกเสมอ
                if self.start_trans_SW_c2 != 1:
                    self.start_trans_SW_c2 = 1
                    _write_file("start_trans_SW_c2.txt", 1)

            # ---- Normalize PLC state for control-flow decisions ----
            # PLC/vSECC ของตู้รุ่นนี้ใช้ SuspendedEV/SuspendedEVSE เป็น hold state
            # ระหว่างรถเชื่อมต่อแล้วแต่ยังรอ RemoteStartTransaction
            # - ก่อน Remote Start: ปฏิบัติเป็น Preparing
            # - หลัง Remote Start/RFID และกำลังรอเปิด transaction: ใช้ Charging
            #   เฉพาะเป็น internal control state เพื่อเข้าถึง handler.
            #   RFID V11 จะยังไม่ส่ง Charging/StartTransaction จน meter ยืนยันว่า
            #   มี energy flow จริง; SuspendedEV ก่อน transaction จะไม่ถูกส่ง.
            flow_iCp1, flow_usi1 = iCpStateCss1, usiLinkStateCcs1
            flow_iCp2, flow_usi2 = iCpStateCss2, usiLinkStateCcs2

            remote_pending_c1 = (self.remote_start_received_c1 == 1 and self.start_transaction_SW1 == 3)
            remote_pending_c2 = (self.remote_start_received_c2 == 1 and self.start_transaction_SW2 == 3)
            rfid_pending_c1 = (self._rfid_session_authorized(1) and self.start_transaction_SW1 == 1)
            rfid_pending_c2 = (self._rfid_session_authorized(2) and self.start_transaction_SW2 == 1)
            # [LOCAL-STATUS-V16] session จากหน้าจอไม่มี start_transaction_SW เป็น
            # ตัวบ่งชี้ (เพราะไม่เปิด transaction) จึงใช้ latch ตรง ๆ
            local_status_c1 = self._local_status_session_active(1)
            local_status_c2 = self._local_status_session_active(2)

            c1_start_state = (
                iCpStateCss1 in ('Charging', 'SuspendedEV', 'SuspendedEVSE') or
                usiLinkStateCcs1 in ('Charging', 'SuspendedEV', 'SuspendedEVSE')
            )
            c2_start_state = (
                iCpStateCss2 in ('Charging', 'SuspendedEV', 'SuspendedEVSE') or
                usiLinkStateCcs2 in ('Charging', 'SuspendedEV', 'SuspendedEVSE')
            )

            # If the hardware has already begun delivering energy but the PLC status
            # mapper still says Preparing, the meter is the final ground truth.
            if (remote_pending_c1 or rfid_pending_c1 or local_status_c1) and not c1_start_state:
                c1_start_state = await self._pending_start_energy_flow(1)
            if (remote_pending_c2 or rfid_pending_c2 or local_status_c2) and not c2_start_state:
                c2_start_state = await self._pending_start_energy_flow(2)

            if (remote_pending_c1 or rfid_pending_c1 or local_status_c1) and c1_start_state:
                flow_iCp1 = flow_usi1 = 'Charging'
            elif iCpStateCss1 in ('SuspendedEV', 'SuspendedEVSE') and self._active_transaction_id(1) <= 0:
                flow_iCp1 = flow_usi1 = 'Preparing'

            if (remote_pending_c2 or rfid_pending_c2 or local_status_c2) and c2_start_state:
                flow_iCp2 = flow_usi2 = 'Charging'
            elif iCpStateCss2 in ('SuspendedEV', 'SuspendedEVSE') and self._active_transaction_id(2) <= 0:
                flow_iCp2 = flow_usi2 = 'Preparing'

            # ---- Update usiLink state ----
            if self.usiLinkStateCcs1 != usiLinkStateCcs1:
                self.usiLinkStateCcs1 = usiLinkStateCcs1
                _write_file("usiLinkState1_regis.txt", usiLinkStateCcs1)
            if self.usiLinkStateCcs2 != usiLinkStateCcs2:
                self.usiLinkStateCcs2 = usiLinkStateCcs2
                _write_file("usiLinkState2_regis.txt", usiLinkStateCcs2)

            # ---- StatusNotification C1 ----
            if flow_iCp1 in ['Available', 'Preparing', 'Charging']:
                self.iCpStateCss1 = flow_iCp1
                if self.emergency_sent_c1 == '1':
                    await self._handle_status_notification_c1(
                        flow_iCp1, flow_usi1,
                        raw_iCp=iCpStateCss1, raw_usi=usiLinkStateCcs1,
                    )

            # ---- StatusNotification C2 ----
            if flow_iCp2 in ['Available', 'Preparing', 'Charging']:
                self.iCpStateCss2 = flow_iCp2
                if self.emergency_sent_c2 == '1':
                    await self._handle_status_notification_c2(
                        flow_iCp2, flow_usi2,
                        raw_iCp=iCpStateCss2, raw_usi=usiLinkStateCcs2,
                    )

            # ---- Remote Stop C1 ----
            await self._handle_remote_stop_c1()

            # ---- Remote Stop C2 ----
            await self._handle_remote_stop_c2()

            # ---- Local/Finishing Stop C1 ----
            self.finshing_sent_c1 = await async_get_finishing_status(plc, self.myip_adr_c1)
            await self._handle_finishing_c1()

            # ---- Local/Finishing Stop C2 ----
            self.finshing_sent_c2 = await async_get_finishing_status(plc, self.myip_adr_c2)
            await self._handle_finishing_c2()

            # ---- Active transaction physical status sync ----
            # Keep the OCPP transaction open while switching only the status:
            # SuspendedEV/SuspendedEVSE <-> Charging according to real energy flow.
            await self._sync_active_transaction_status(1, iCpStateCss1, usiLinkStateCcs1)
            await self._sync_active_transaction_status(2, iCpStateCss2, usiLinkStateCcs2)

            # ---- RFID Auth C1 (physical reader) ----
            await self._handle_rfid_auth_c1(iCpStateCss1, usiLinkStateCcs1)

            # ---- RFID Auth C2 (physical reader) ----
            await self._handle_rfid_auth_c2(iCpStateCss2, usiLinkStateCcs2)

            # ---- MeterValues ----
            await self._handle_meter_values(flow_iCp1, flow_usi1,
                                            flow_iCp2, flow_usi2)

            # ---- Emergency C1 ----
            self.emergency_sent_c1 = await async_get_emergency_stop(plc, self.myip_adr_c1)
            await self._handle_emergency_c1(iCpStateCss1, usiLinkStateCcs1)

            # ---- Emergency C2 ----
            self.emergency_sent_c2 = await async_get_emergency_stop(plc, self.myip_adr_c2)
            await self._handle_emergency_c2(iCpStateCss2, usiLinkStateCcs2)

            await asyncio.sleep(interval)

    # ---- StatusNotification sub-handlers ----
    async def _handle_status_notification_c1(self, iCp, usi, raw_iCp=None, raw_usi=None):
        raw_iCp = iCp if raw_iCp is None else raw_iCp
        raw_usi = usi if raw_usi is None else raw_usi
        rfid_authorized = self._rfid_session_authorized(1)
        local_status = self._local_status_session_active(1)   # [LOCAL-STATUS-V16]
        pending_start = (
            (self.remote_start_received_c1 == 1 and self.start_transaction_SW1 == 3) or
            (rfid_authorized and self.start_transaction_SW1 == 1)
        )
        if pending_start and self._active_transaction_id(1) <= 0:
            source = "RemoteStart" if self.remote_start_received_c1 == 1 else "RFIDCommand"
            self._clear_stale_stop_latch_for_new_start(1, source)
            if rfid_authorized:
                self._repair_authorized_start_switches(1, source)

        # [REMOTE-STOP-SEQUENCE-V2]
        # phase 1 = RemoteStop accepted, StopTransaction ยังไม่เสร็จ: block ทุก StatusNotification
        # phase 2 = StopTransaction + Finishing สำเร็จแล้ว: รอ PLC Available แล้วค่อยส่ง Available
        if self.remote_stop_finishing_c1 in (1, 2):
            if self.remote_stop_finishing_c1 == 2 and iCp == 'Available':
                await self.status_notification_with_status(1, 'Available')
                self.remote_stop_finishing_c1 = 0
                self.remote_start_received_c1 = 0
                self.block_sw1_C1, self.block_sw2_C1, self.block_sw3_C1 = 0, 1, 1
                _write_file("remote_stop_finishing_c1.txt", 0)
                _write_file("remote_start_received_c1.txt", 0)
                self._set_local_status_session(1, 0)      # [LOCAL-STATUS-V16]
                clear_local_request("start", 1)
                _write_file("iCpStateCss1.txt", 'Available')
                self._release_session_identity(1)
                _write_file("block_sw1_C1.txt", 0)
                _write_file("block_sw2_C1.txt", 1)
                _write_file("block_sw3_C1.txt", 1)
                logging.info("[REMOTE-STOP] C1 cable removed: Finishing -> Available")
            return

        # ---- ARCH2 guard (ชั้นสอง) ----
        # ระหว่าง vSECC กั๊กกระแส 0 A รอ RemoteStart รถอยู่ใน state_c + CurrentDemand
        # ถ้าปล่อยผ่านเป็น Charging จะเปิด transaction ก่อนลูกค้าสั่ง
        # ปกติ plc.get_connector_status() แปลงให้เป็น Preparing แล้ว อันนี้กันพลาดซ้ำ
        try:
            if (iCp == 'Charging' and plc.get_arch2_hold(self.myip_adr_c1) is True and
                    self.remote_start_received_c1 != 1 and not rfid_authorized and
                    not local_status):
                iCp = usi = 'Preparing'
        except AttributeError:
            pass   # plc_function เวอร์ชันเก่า
        finishing = await async_get_finishing_status(plc, self.myip_adr_c1)

        if iCp == 'Available' and self.block_sw1_C1 == 1:
            if finishing == 1:
                return
            await self.status_notification_with_status(1, 'Available')
            self.remote_start_received_c1 = 0              # [REMOTE-ONLY] session ใหม่ต้องมี RemoteStart ใหม่
            _write_file("remote_start_received_c1.txt", 0)
            # [START-LATCH] R3.1.6: with it the rest of a RemoteStart that never reached
            # StartTransaction - a start_transaction_SW1=3 left here refused every later start
            self._clear_remote_start_latch(1, "cable removed before StartTransaction")
            # [LOCAL-STATUS-V16] ถอดสาย = จบ session แน่นอน ต้องกดจากหน้าจอใหม่
            self._set_local_status_session(1, 0)
            clear_local_request("start", 1)
            if self.accepted_tag1 != '0':
                print(f"## Reset accepted_tag1 (cable unplugged)")
                self.accepted_tag1 = '0'
            try:
                await self._stop_orphaned_transaction_c1()
            finally:
                # Available was observed before the await. The orphan close can
                # re-arm finishing while sending StopTransaction; release that
                # old session's markers before the next physical status poll.
                self._set_session_finishing(1, 0)
                self._clear_pending_local_stop(1)
            self.rfid_start_received_c1 = 0
            self.rfid_recovery_pending_c1 = 0
            self.active_id_tag_c1 = "Default"
            _write_file("rfid_start_received_c1.txt", 0)
            _write_file("active_id_tag_c1.txt", "Default")
            self._release_session_identity(1)
            self.block_sw1_C1, self.block_sw2_C1, self.block_sw3_C1 = 0, 1, 1
            _write_file("iCpStateCss1.txt", iCp)
            _write_file("block_sw1_C1.txt", 0); _write_file("block_sw2_C1.txt", 1); _write_file("block_sw3_C1.txt", 1)

        elif iCp == 'Preparing' and self.block_sw2_C1 == 1:
            if finishing == 1:
                return
            try: active_txn = int(open("translogs1.txt").read())
            except Exception: active_txn = 0
            if active_txn > 0: return
            await self.status_notification_with_status(1, 'Preparing')
            self.block_sw1_C1, self.block_sw2_C1, self.block_sw3_C1 = 1, 0, 1
            _write_file("iCpStateCss1.txt", iCp)
            _write_file("block_sw1_C1.txt", 1); _write_file("block_sw2_C1.txt", 0); _write_file("block_sw3_C1.txt", 1)

        elif iCp == 'Charging' and (self.block_sw3_C1 == 1 or pending_start or local_status):
            if pending_start and self.block_sw3_C1 != 1:
                self.block_sw3_C1 = 1
                _write_file("block_sw3_C1.txt", 1)
                logging.warning("[START-RECOVERY-V10] C1 forced transaction-start latch open")
            if finishing == 1:
                if local_status:
                    self._log_local_status_block(1, "topic/Finishing1 == 1")
                return
            if (self.remote_start_only and self.remote_start_received_c1 != 1 and
                    not rfid_authorized and not local_status):
                return
            if usi != 'Charging':
                if local_status:
                    self._log_local_status_block(1, f"usi={usi} ยังไม่ใช่ Charging")
                return

            # [LOCAL-STATUS-V16] session ที่เริ่มจากหน้าจอ: รายงานสถานะอย่างเดียว
            # ไม่เปิด OCPP transaction จึงข้ามบล็อก StartTransaction ทั้งหมด และไม่
            # แตะ start_transaction_SW1 / start_sendMeterValue_SW1 (ไม่มี MeterValues)
            # _desired_active_status() ใช้มิเตอร์ Modbus เป็นตัวตัดสิน Charging vs
            # SuspendedEV เหมือนเส้นทาง RemoteStart เป๊ะ ๆ — ห้ามส่ง Charging
            # เพราะเห็น edge จาก MQTT อย่างเดียว ต้องมีไฟไหลจริง
            if local_status and self._active_transaction_id(1) <= 0:
                physical_status = await self._desired_active_status(
                    1, raw_iCp, raw_usi, initial=False
                )
                if physical_status != self._last_reported_status(1):
                    await self.status_notification_with_status(1, physical_status)
                self.block_sw1_C1, self.block_sw2_C1, self.block_sw3_C1 = 1, 1, 1
                _write_file("iCpStateCss1.txt", physical_status)
                _write_file("block_sw1_C1.txt", 1)
                _write_file("block_sw2_C1.txt", 1)
                _write_file("block_sw3_C1.txt", 1)
                return

            active_before = self._active_transaction_id(1)
            opening_transaction = (
                active_before <= 0 and self.start_transaction_SW1 in (1, 3)
            )
            physical_status = await self._desired_active_status(
                1, raw_iCp, raw_usi, initial=opening_transaction
            )

            # [START-ORDER-V15] ทุกเส้นทาง (RFID และ RemoteStart) ใช้ลำดับเดียวกัน:
            #   authorized/RemoteStart accepted -> รอ *ไฟไหลจริง*
            #   -> StartTransaction -> StatusNotification(Charging) -> MeterValues
            # (V14 ส่ง Charging ก่อน StartTransaction — สลับแล้วเพื่อให้ CSMS มี
            #  transaction อยู่ก่อนที่จะถูกบอกว่ากำลังชาร์จ)
            # ห้ามเปิด transaction ขณะสภาพจริงยังเป็น SuspendedEV/SuspendedEVSE
            # และห้ามส่ง SuspendedEV ก่อนเปิด transaction
            if opening_transaction and physical_status != 'Charging':
                return

            # [START-ORDER-V15] StartTransaction goes out BEFORE the Charging
            # StatusNotification, so the CSMS has the transaction before it is told
            # the connector is charging. The Charging notification is sent further
            # down, once the transaction id exists, by the physical_status block.
            # A failed StartTransaction therefore no longer leaves the CSMS believing
            # the connector is charging with no transaction behind it.
            if self.start_transaction_SW1 == 3 and active_before <= 0:
                await self.start_transaction(1)
                if self.transactionId_c1 > 0:
                    self.start_transaction_SW1 = 4
                    self.start_sendMeterValue_SW1 = 2
                    self.finshing_sent_sw_c1 = 1
                    _write_file("start_transaction_SW1.txt", 4)
                    _write_file("start_sendMeterValue_SW1.txt", 2)
                    _write_file("finshing_sent_sw_c1.txt", 1)
                else:
                    return
            elif self.start_transaction_SW1 == 1 and active_before <= 0:
                await self.start_transaction_ath(1)
                if self.transactionId_c1 > 0:
                    self.start_transaction_SW1 = 2
                    self.start_sendMeterValue_SW1 = 2
                    self.finshing_sent_sw_c1 = 1
                    _write_file("start_transaction_SW1.txt", 2)
                    _write_file("start_sendMeterValue_SW1.txt", 2)
                    _write_file("finshing_sent_sw_c1.txt", 1)
                else:
                    return
            elif active_before <= 0:
                logging.warning(
                    f"[START-RECOVERY-V10] C1 cannot open transaction: "
                    f"startSW={self.start_transaction_SW1}"
                )
                return

            if self._active_transaction_id(1) <= 0:
                return

            if physical_status != self._last_reported_status(1):
                await self.status_notification_with_status(1, physical_status)

            self.block_sw1_C1, self.block_sw2_C1, self.block_sw3_C1 = 1, 1, 0
            _write_file("iCpStateCss1.txt", physical_status)
            _write_file("block_sw1_C1.txt", 1); _write_file("block_sw2_C1.txt", 1); _write_file("block_sw3_C1.txt", 0)

    async def _handle_status_notification_c2(self, iCp, usi, raw_iCp=None, raw_usi=None):
        raw_iCp = iCp if raw_iCp is None else raw_iCp
        raw_usi = usi if raw_usi is None else raw_usi
        rfid_authorized = self._rfid_session_authorized(2)
        local_status = self._local_status_session_active(2)   # [LOCAL-STATUS-V16]
        pending_start = (
            (self.remote_start_received_c2 == 1 and self.start_transaction_SW2 == 3) or
            (rfid_authorized and self.start_transaction_SW2 == 1)
        )
        if pending_start and self._active_transaction_id(2) <= 0:
            source = "RemoteStart" if self.remote_start_received_c2 == 1 else "RFIDCommand"
            self._clear_stale_stop_latch_for_new_start(2, source)
            if rfid_authorized:
                self._repair_authorized_start_switches(2, source)

        # [REMOTE-STOP-SEQUENCE-V2]
        # phase 1 = RemoteStop accepted, StopTransaction ยังไม่เสร็จ: block ทุก StatusNotification
        # phase 2 = StopTransaction + Finishing สำเร็จแล้ว: รอ PLC Available แล้วค่อยส่ง Available
        if self.remote_stop_finishing_c2 in (1, 2):
            if self.remote_stop_finishing_c2 == 2 and iCp == 'Available':
                await self.status_notification_with_status(2, 'Available')
                self.remote_stop_finishing_c2 = 0
                self.remote_start_received_c2 = 0
                self.block_sw1, self.block_sw2, self.block_sw3 = 0, 1, 1
                _write_file("remote_stop_finishing_c2.txt", 0)
                _write_file("remote_start_received_c2.txt", 0)
                self._set_local_status_session(2, 0)      # [LOCAL-STATUS-V16]
                clear_local_request("start", 2)
                _write_file("iCpStateCss2.txt", 'Available')
                self._release_session_identity(2)
                _write_file("block_sw1.txt", 0)
                _write_file("block_sw2.txt", 1)
                _write_file("block_sw3.txt", 1)
                logging.info("[REMOTE-STOP] C2 cable removed: Finishing -> Available")
            return

        # ---- ARCH2 guard (ชั้นสอง) ----
        # ระหว่าง vSECC กั๊กกระแส 0 A รอ RemoteStart รถอยู่ใน state_c + CurrentDemand
        # ถ้าปล่อยผ่านเป็น Charging จะเปิด transaction ก่อนลูกค้าสั่ง
        # ปกติ plc.get_connector_status() แปลงให้เป็น Preparing แล้ว อันนี้กันพลาดซ้ำ
        try:
            if (iCp == 'Charging' and plc.get_arch2_hold(self.myip_adr_c2) is True and
                    self.remote_start_received_c2 != 1 and not rfid_authorized and
                    not local_status):
                iCp = usi = 'Preparing'
        except AttributeError:
            pass   # plc_function เวอร์ชันเก่า
        finishing = await async_get_finishing_status(plc, self.myip_adr_c2)

        if iCp == 'Available' and self.block_sw1 == 1:
            if finishing == 1:
                return
            await self.status_notification_with_status(2, 'Available')
            self.remote_start_received_c2 = 0              # [REMOTE-ONLY] session ใหม่ต้องมี RemoteStart ใหม่
            _write_file("remote_start_received_c2.txt", 0)
            # [START-LATCH] R3.1.6: with it the rest of a RemoteStart that never reached
            # StartTransaction - a start_transaction_SW2=3 left here refused every later start
            self._clear_remote_start_latch(2, "cable removed before StartTransaction")
            # [LOCAL-STATUS-V16] ถอดสาย = จบ session แน่นอน ต้องกดจากหน้าจอใหม่
            self._set_local_status_session(2, 0)
            clear_local_request("start", 2)
            if self.accepted_tag2 != '0':
                print(f"## Reset accepted_tag2 (cable unplugged)")
                self.accepted_tag2 = '0'
            try:
                await self._stop_orphaned_transaction_c2()
            finally:
                self._set_session_finishing(2, 0)
                self._clear_pending_local_stop(2)
            self.rfid_start_received_c2 = 0
            self.rfid_recovery_pending_c2 = 0
            self.active_id_tag_c2 = "Default"
            _write_file("rfid_start_received_c2.txt", 0)
            _write_file("active_id_tag_c2.txt", "Default")
            self._release_session_identity(2)
            self.block_sw1, self.block_sw2, self.block_sw3 = 0, 1, 1
            _write_file("iCpStateCss2.txt", iCp)
            _write_file("block_sw1.txt", 0); _write_file("block_sw2.txt", 1); _write_file("block_sw3.txt", 1)

        elif iCp == 'Preparing' and self.block_sw2 == 1:
            if finishing == 1:
                return
            try: active_txn = int(open("translogs2.txt").read())
            except Exception: active_txn = 0
            if active_txn > 0: return
            await self.status_notification_with_status(2, 'Preparing')
            self.block_sw1, self.block_sw2, self.block_sw3 = 1, 0, 1
            _write_file("iCpStateCss2.txt", iCp)
            _write_file("block_sw1.txt", 1); _write_file("block_sw2.txt", 0); _write_file("block_sw3.txt", 1)

        elif iCp == 'Charging' and (self.block_sw3 == 1 or pending_start or local_status):
            if pending_start and self.block_sw3 != 1:
                self.block_sw3 = 1
                _write_file("block_sw3.txt", 1)
                logging.warning("[START-RECOVERY-V10] C2 forced transaction-start latch open")
            if finishing == 1:
                if local_status:
                    self._log_local_status_block(2, "topic/Finishing2 == 1")
                return
            if (self.remote_start_only and self.remote_start_received_c2 != 1 and
                    not rfid_authorized and not local_status):
                return
            if usi != 'Charging':
                if local_status:
                    self._log_local_status_block(2, f"usi={usi} ยังไม่ใช่ Charging")
                return

            # [LOCAL-STATUS-V16] session ที่เริ่มจากหน้าจอ: รายงานสถานะอย่างเดียว
            # ไม่เปิด OCPP transaction จึงข้ามบล็อก StartTransaction ทั้งหมด และไม่
            # แตะ start_transaction_SW2 / start_sendMeterValue_SW2 (ไม่มี MeterValues)
            # _desired_active_status() ใช้มิเตอร์ Modbus เป็นตัวตัดสิน Charging vs
            # SuspendedEV เหมือนเส้นทาง RemoteStart เป๊ะ ๆ — ห้ามส่ง Charging
            # เพราะเห็น edge จาก MQTT อย่างเดียว ต้องมีไฟไหลจริง
            if local_status and self._active_transaction_id(2) <= 0:
                physical_status = await self._desired_active_status(
                    2, raw_iCp, raw_usi, initial=False
                )
                if physical_status != self._last_reported_status(2):
                    await self.status_notification_with_status(2, physical_status)
                self.block_sw1, self.block_sw2, self.block_sw3 = 1, 1, 1
                _write_file("iCpStateCss2.txt", physical_status)
                _write_file("block_sw1.txt", 1)
                _write_file("block_sw2.txt", 1)
                _write_file("block_sw3.txt", 1)
                return

            active_before = self._active_transaction_id(2)
            opening_transaction = (
                active_before <= 0 and self.start_transaction_SW2 in (1, 3)
            )
            physical_status = await self._desired_active_status(
                2, raw_iCp, raw_usi, initial=opening_transaction
            )

            # [START-ORDER-V14] ลำดับเดียวกับ Connector 1 — ทั้ง RFID และ RemoteStart
            if opening_transaction and physical_status != 'Charging':
                return

            # [START-ORDER-V15] See connector 1 - StartTransaction first, then the
            # Charging StatusNotification from the physical_status block below.
            if self.start_transaction_SW2 == 3 and active_before <= 0:
                await self.start_transaction(2)
                if self.transactionId_c2 > 0:
                    self.start_transaction_SW2 = 4
                    self.start_sendMeterValue_SW2 = 2
                    self.finshing_sent_sw_c2 = 1
                    _write_file("start_transaction_SW2.txt", 4)
                    _write_file("start_sendMeterValue_SW2.txt", 2)
                    _write_file("finshing_sent_sw_c2.txt", 1)
                else:
                    return
            elif self.start_transaction_SW2 == 1 and active_before <= 0:
                await self.start_transaction_ath(2)
                if self.transactionId_c2 > 0:
                    self.start_transaction_SW2 = 2
                    self.start_sendMeterValue_SW2 = 2
                    self.finshing_sent_sw_c2 = 1
                    _write_file("start_transaction_SW2.txt", 2)
                    _write_file("start_sendMeterValue_SW2.txt", 2)
                    _write_file("finshing_sent_sw_c2.txt", 1)
                else:
                    return
            elif active_before <= 0:
                logging.warning(
                    f"[START-RECOVERY-V10] C2 cannot open transaction: "
                    f"startSW={self.start_transaction_SW2}"
                )
                return

            if self._active_transaction_id(2) <= 0:
                return

            if physical_status != self._last_reported_status(2):
                await self.status_notification_with_status(2, physical_status)

            self.block_sw1, self.block_sw2, self.block_sw3 = 1, 1, 0
            _write_file("iCpStateCss2.txt", physical_status)
            _write_file("block_sw1.txt", 1); _write_file("block_sw2.txt", 1); _write_file("block_sw3.txt", 0)

    # ---- Orphan transaction helpers ----
    async def _stop_orphaned_transaction_c1(self):
        try: orphan = int(open("translogs1.txt").read())
        except Exception: orphan = 0
        if orphan > 0:
            print(f"## Orphaned txn C1={orphan} — stopping")
            self.transactionId_c1 = orphan
            try:
                await self.send_meter_values(1)
                await self.send_stop_transaction(1, reasonf='Other')
            except: pass
            self.transactionId_c1 = 0
            _write_file("translogs1.txt", 0)

    async def _stop_orphaned_transaction_c2(self):
        try: orphan = int(open("translogs2.txt").read())
        except Exception: orphan = 0
        if orphan > 0:
            print(f"## Orphaned txn C2={orphan} — stopping")
            self.transactionId_c2 = orphan
            try:
                await self.send_meter_values(2)
                await self.send_stop_transaction(2, reasonf='Other')
            except: pass
            self.transactionId_c2 = 0
            _write_file("translogs2.txt", 0)

    # ---- Remote Stop handlers ----
    async def _handle_remote_stop_c1(self):
        if self.my_remote_stop_c1 != 1: return
        self.start_sendMeterValue_SW1 = 1
        self.block_sw1_C1 = 0
        _write_file("start_sendMeterValue_SW1.txt", 1)
        _write_file("block_sw1_C1.txt", 0)
        await async_remote_stop_plc(self.myip_adr_c1, 1)
        await asyncio.sleep(3)
        await async_remote_stop_plc(self.myip_adr_c1, 0)
        self.transactionId_c1 = int(open("translogs1.txt").read()) if os.path.exists("translogs1.txt") else 0
        if self.transactionId_c1 > 0:
            reason = 'Remote' if self.block1 == 1 else 'Local'
            await self.send_stop_transaction(1, reasonf=reason)
            await self.status_notification_usiLink(1)
            self.remote_stop_finishing_c1 = 2
            _write_file("remote_stop_finishing_c1.txt", 2)
            logging.info("[REMOTE-STOP] C1 StopTransaction accepted; phase=2 waiting for Available")
            _write_file("translogs1.txt", 0)
            self.block_sw1_C1 = 1
            self.finshing_sent_sw_c1 = 2
            _write_file("block_sw1_C1.txt", 1)
            _write_file("finshing_sent_sw_c1.txt", 2)
        else:
            if self.transactionId_c1 == -1:
                _write_file("translogs1.txt", 0)
                self.transactionId_c1 = 0
            self.block_sw1_C1 = 1
            _write_file("block_sw1_C1.txt", 1)
        self.my_remote_stop_c1 = 0
        self.block1 = 0
        _write_file("my_remote_stop_c1.txt", 0)
        _write_file("block1.txt", 0)

    async def _handle_remote_stop_c2(self):
        if self.my_remote_stop_c2 != 1: return
        self.start_sendMeterValue_SW2 = 1
        self.block_sw1 = 0
        _write_file("start_sendMeterValue_SW2.txt", 1)
        _write_file("block_sw1.txt", 0)
        await async_remote_stop_plc(self.myip_adr_c2, 1)
        await asyncio.sleep(3)
        await async_remote_stop_plc(self.myip_adr_c2, 0)
        self.transactionId_c2 = int(open("translogs2.txt").read()) if os.path.exists("translogs2.txt") else 0
        if self.transactionId_c2 > 0:
            reason = 'Remote' if self.block2 == 1 else 'Local'
            await self.send_stop_transaction(2, reasonf=reason)
            await self.status_notification_usiLink(2)
            self.remote_stop_finishing_c2 = 2
            _write_file("remote_stop_finishing_c2.txt", 2)
            logging.info("[REMOTE-STOP] C2 StopTransaction accepted; phase=2 waiting for Available")
            _write_file("translogs2.txt", 0)
            self.block_sw1 = 1
            self.finshing_sent_sw_c2 = 2
            _write_file("block_sw1.txt", 1)
            _write_file("finshing_sent_sw_c2.txt", 2)
        else:
            if self.transactionId_c2 == -1:
                _write_file("translogs2.txt", 0)
                self.transactionId_c2 = 0
            self.block_sw1 = 1
            _write_file("block_sw1.txt", 1)
        self.my_remote_stop_c2 = 0
        self.block2 = 0
        _write_file("my_remote_stop_c2.txt", 0)
        _write_file("block2.txt", 0)

    # ---- Finishing / local stop handlers ----
    async def _handle_finishing_c1(self):
        # [REMOTE-STOP-V4] Dedicated remote-stop worker is the only owner of
        # StopTransaction while phase 1/2 is active.  The PLC finishing flag
        # can rise during the worker's 3-second stop pulse; without this guard
        # the local-finishing path sends a second StopTransaction concurrently.
        if self.remote_stop_finishing_c1 in (1, 2):
            return

        if self.finshing_sent_sw_c1 == 1 and self.finshing_sent_c1 == 1:
            reason = self._finishing_stop_reason(1)
            self.transactionId_c1 = int(open("translogs1.txt").read()) if os.path.exists("translogs1.txt") else 0
            if self.transactionId_c1 > 0:
                await self.send_stop_transaction(1, reasonf=reason)
                await self.status_notification_usiLink(1)
                _write_file("translogs1.txt", 0)
                self.block_sw1_C1 = 1
                _write_file("block_sw1_C1.txt", 1)
            elif self.transactionId_c1 == -1:
                _write_file("translogs1.txt", 0)
                self.transactionId_c1 = 0
            elif (self.rfid_start_received_c1 == 1 and self.block_sw2_C1 == 0):
                logging.info(
                    "[RFID-OCPP-V7] C1 session ended before StartTransaction; "
                    "sending Finishing without StopTransaction"
                )
                await self.status_notification_usiLink(1)
                self.rfid_start_received_c1 = 0
                self.active_id_tag_c1 = "Default"
                _write_file("rfid_start_received_c1.txt", 0)
                _write_file("active_id_tag_c1.txt", "Default")
                self.block_sw1_C1 = 1
                _write_file("block_sw1_C1.txt", 1)
            elif (self.remote_start_received_c1 == 1 and self.block_sw2_C1 == 0):
                # [START-LATCH] R3.1.6: the card branch above, for a RemoteStart. The
                # session ended before StartTransaction (on 2026-11-10 the EV sent
                # SessionStop 18 s after the start pulse). Without this branch nothing
                # was sent and start_transaction_SW1=3 refused every later start.
                logging.info(
                    "[START-LATCH] C1 RemoteStart session ended before StartTransaction; "
                    "sending Finishing without StopTransaction"
                )
                await self.status_notification_usiLink(1)
                self._clear_remote_start_latch(1, "session ended before StartTransaction")
                self.block_sw1_C1 = 1
                _write_file("block_sw1_C1.txt", 1)
            elif self._local_status_session_active(1):
                # [LOCAL-STATUS-V16] session จากหน้าจอจบลง — ไม่มี transaction ให้ปิด
                # จึงแจ้ง Finishing อย่างเดียว  ปกติ topic/RemoteStop1 จะมาถึงก่อน
                # และจัดการไปแล้ว อันนี้คือ safety net กรณี PLC จบ session เอง
                # (ชาร์จเต็ม / รถตัด / หน้าจอไม่ได้ส่ง RemoteStop)
                logging.info(
                    "[LOCAL-STATUS-V16] C1 screen session ended by PLC; "
                    "sending Finishing without StopTransaction"
                )
                await self.status_notification_usiLink(1)
                self._set_local_status_session(1, 0)
                clear_local_request("start", 1)
                self.block_sw1_C1 = 1
                _write_file("block_sw1_C1.txt", 1)
            elif (self.remote_start_only and
                  self.remote_start_received_c1 != 1 and
                  self.block_sw2_C1 == 0):
                # เสียบสายแล้วส่ง Preparing ไปแล้ว แต่ไม่มี RemoteStartTransaction
                # เมื่อ PLC จบ session/ขึ้นให้ถอดสาย ให้แจ้ง CSMS เป็น Finishing
                # โดยไม่สร้าง StopTransaction เพราะไม่มี transaction ถูกเปิด
                # [PREPARING-TIMEOUT] ไม่มี RemoteStart ภายในเวลาที่ controller ตั้งไว้
                # controller จบ session เอง -> แจ้ง Faulted "PREPARING TIMEOUT"
                # แทน Finishing จนกว่าจะถอดสาย (ถอดแล้วส่ง Available ตามปกติ)
                logging.warning(
                    "[PREPARING-TIMEOUT] C1 no RemoteStart before controller timeout "
                    "— sending Faulted (PREPARING TIMEOUT)"
                )
                await self.status_notification_preparing_timeout(1)
                self.block_sw1_C1 = 1
                _write_file("block_sw1_C1.txt", 1)
            self.finshing_sent_sw_c1 = 2
            _write_file("finshing_sent_sw_c1.txt", 2)
        elif self.finshing_sent_sw_c1 != 1 and self.finshing_sent_c1 == 0:
            self.finshing_sent_sw_c1 = 1
            self.block1 = 0
            _write_file("finshing_sent_sw_c1.txt", 1)
            _write_file("block1.txt", 0)

    async def _handle_finishing_c2(self):
        # [REMOTE-STOP-V4] See C1: do not let the local-finishing handler race
        # the dedicated remote-stop worker and duplicate StopTransaction.
        if self.remote_stop_finishing_c2 in (1, 2):
            return

        if self.finshing_sent_sw_c2 == 1 and self.finshing_sent_c2 == 1:
            reason = self._finishing_stop_reason(2)
            self.transactionId_c2 = int(open("translogs2.txt").read()) if os.path.exists("translogs2.txt") else 0
            if self.transactionId_c2 > 0:
                await self.send_stop_transaction(2, reasonf=reason)
                await self.status_notification_usiLink(2)
                _write_file("translogs2.txt", 0)
                self.block_sw1 = 1
                _write_file("block_sw1.txt", 1)
            elif self.transactionId_c2 == -1:
                _write_file("translogs2.txt", 0)
                self.transactionId_c2 = 0
            elif (self.rfid_start_received_c2 == 1 and self.block_sw2 == 0):
                logging.info(
                    "[RFID-OCPP-V7] C2 session ended before StartTransaction; "
                    "sending Finishing without StopTransaction"
                )
                await self.status_notification_usiLink(2)
                self.rfid_start_received_c2 = 0
                self.active_id_tag_c2 = "Default"
                _write_file("rfid_start_received_c2.txt", 0)
                _write_file("active_id_tag_c2.txt", "Default")
                self.block_sw1 = 1
                _write_file("block_sw1.txt", 1)
            elif (self.remote_start_received_c2 == 1 and self.block_sw2 == 0):
                # [START-LATCH] R3.1.6: the card branch above, for a RemoteStart. The
                # session ended before StartTransaction (on 2026-11-10 the EV sent
                # SessionStop 18 s after the start pulse). Without this branch nothing
                # was sent and start_transaction_SW2=3 refused every later start.
                logging.info(
                    "[START-LATCH] C2 RemoteStart session ended before StartTransaction; "
                    "sending Finishing without StopTransaction"
                )
                await self.status_notification_usiLink(2)
                self._clear_remote_start_latch(2, "session ended before StartTransaction")
                self.block_sw1 = 1
                _write_file("block_sw1.txt", 1)
            elif self._local_status_session_active(2):
                # [LOCAL-STATUS-V16] session จากหน้าจอจบลง — ไม่มี transaction ให้ปิด
                # จึงแจ้ง Finishing อย่างเดียว  ปกติ topic/RemoteStop2 จะมาถึงก่อน
                # และจัดการไปแล้ว อันนี้คือ safety net กรณี PLC จบ session เอง
                # (ชาร์จเต็ม / รถตัด / หน้าจอไม่ได้ส่ง RemoteStop)
                logging.info(
                    "[LOCAL-STATUS-V16] C2 screen session ended by PLC; "
                    "sending Finishing without StopTransaction"
                )
                await self.status_notification_usiLink(2)
                self._set_local_status_session(2, 0)
                clear_local_request("start", 2)
                self.block_sw1 = 1
                _write_file("block_sw1.txt", 1)
            elif (self.remote_start_only and
                  self.remote_start_received_c2 != 1 and
                  self.block_sw2 == 0):
                # เสียบสายแล้วส่ง Preparing ไปแล้ว แต่ไม่มี RemoteStartTransaction
                # เมื่อ PLC จบ session/ขึ้นให้ถอดสาย ให้แจ้ง CSMS เป็น Finishing
                # โดยไม่สร้าง StopTransaction เพราะไม่มี transaction ถูกเปิด
                # [PREPARING-TIMEOUT] ไม่มี RemoteStart ภายในเวลาที่ controller ตั้งไว้
                # controller จบ session เอง -> แจ้ง Faulted "PREPARING TIMEOUT"
                # แทน Finishing จนกว่าจะถอดสาย (ถอดแล้วส่ง Available ตามปกติ)
                logging.warning(
                    "[PREPARING-TIMEOUT] C2 no RemoteStart before controller timeout "
                    "— sending Faulted (PREPARING TIMEOUT)"
                )
                await self.status_notification_preparing_timeout(2)
                self.block_sw1 = 1
                _write_file("block_sw1.txt", 1)
            self.finshing_sent_sw_c2 = 2
            self.block2 = 0
            _write_file("finshing_sent_sw_c2.txt", 2)
            _write_file("block2.txt", 0)
        elif self.finshing_sent_sw_c2 != 1 and self.finshing_sent_c2 == 0:
            self.finshing_sent_sw_c2 = 1
            self.block2 = 0
            _write_file("finshing_sent_sw_c2.txt", 1)
            _write_file("block2.txt", 0)

    def _session_start_id_tag(self, connector_id):
        """[RFID-STOP-SAME-CARD] คืน idTag ของ "บัตรที่เริ่มชาร์จ" session นี้ ("" ถ้าไม่รู้).

        ไล่ตามลำดับความน่าเชื่อถือ ทุกแหล่งถูกเขียนตอน "เริ่ม" session เท่านั้น
        ไม่ใช่ตอนแตะบัตร:
          1. accepted_tagN      ตั้งตอน RFID start (plc_function / _handle_rfid_auth_cN)
          2. active_id_tag_cN   ตั้งตอน StartTransaction สำเร็จ - ครอบคลุม RemoteStart ด้วย
          3. ไฟล์ของทั้งสองค่า  กันกรณี process restart กลางคัน แล้ว memory ยังว่าง

        จงใจ "ไม่" ใช้ myid_tag_ath_cN / myid_tag_cN เพราะสองค่านั้นถูกเขียนทับทุกครั้ง
        ที่มีการแตะบัตร (รวมบัตรใบอื่น - ดู _handle_rfid_auth_c1) จึงไม่ใช่ใบที่เริ่มชาร์จ
        """
        if connector_id not in (1, 2):
            return ""
        for tag in (
            getattr(self, f"accepted_tag{connector_id}", None),
            getattr(self, f"active_id_tag_c{connector_id}", None),
            self._read_runtime_text(f"accepted_tag{connector_id}.txt", "0"),
            self._read_runtime_text(f"active_id_tag_c{connector_id}.txt", "Default"),
        ):
            if self._valid_id_tag(tag):
                return str(tag).strip()
        return ""

    @staticmethod
    def _same_id_tag(tag_a, tag_b):
        """เทียบ idTag แบบตัดช่องว่างหัวท้ายและไม่สนตัวพิมพ์ใหญ่/เล็ก.

        เครื่องอ่านส่ง UID เป็น hex ตัวใหญ่ แต่ idTag ที่ CSMS/HMI ส่งมาอาจเป็นตัวเล็ก
        การเทียบแบบ case-insensitive จึงกันเคส "ใบเดิมแต่หยุดไม่ได้" ที่ไม่ควรเกิด
        """
        a = str(tag_a or "").strip().lower()
        b = str(tag_b or "").strip().lower()
        return bool(a) and a == b

    def _take_pin_start_origin(self, connector_id, id_tag):
        attr = f'_pin_autocharge_start_intent_c{connector_id}'
        intent = getattr(self, attr, '')
        setattr(self, attr, '')  # One start attempt only; never inherit a stale arm.
        return bool(intent) and str(intent) == str(id_tag)

    def _remember_pin_start_origin(self, connector_id, transaction_id, autocharge):
        origins = getattr(self, '_pin_stop_auto_txn', None)
        if origins is None:
            origins = self._pin_stop_auto_txn = {}
        origins[connector_id] = int(transaction_id) if autocharge else 0
        logging.info('[PIN-STOP-ORIGIN] C%s txn=%s source=%s', connector_id,
                     transaction_id, 'AutoCharge' if autocharge else 'Manual')

    def _pin_stop_is_autocharge(self, connector_id, transaction_id):
        try:
            transaction_id = int(transaction_id)
            return (connector_id in (1, 2) and transaction_id != 0 and
                    getattr(self, '_pin_stop_auto_txn', {}).get(connector_id, 0) == transaction_id)
        except (ValueError, TypeError):
            return False

    def _pin_stop_session_valid(self, connector_id, transaction_id):
        return (transaction_id > 0
                and self._active_transaction_id(connector_id) == transaction_id
                and self._pin_stop_is_autocharge(connector_id, transaction_id)
                and int(getattr(self, f"remote_stop_finishing_c{connector_id}", 0) or 0) == 0
                and int(getattr(self, f"my_remote_stop_c{connector_id}", 0) or 0) == 0
                and not self._remote_stop_worker_running(connector_id))

    async def _pin_stop_with_config(self, connector_id, password, reply):
        async def reject(reason):
            print(f'[PIN-DEBUG][VECTOR] event=REJECTED head={connector_id} reason={reason}', flush=True)
            await reply(dict(head=connector_id, status='Rejected', reason=reason))

        if connector_id not in (1, 2) or not valid_pin(password):
            await reject('Invalid request')
            return False
        busy = getattr(self, '_pin_stop_busy', None)
        if busy is None:
            busy = self._pin_stop_busy = set()
        if connector_id in busy:
            await reject('Request already pending')
            return False
        busy.add(connector_id)
        try:
            transaction_id = self._active_transaction_id(connector_id)
            if not self._pin_stop_is_autocharge(connector_id, transaction_id):
                await reject('PIN stop is only allowed for AutoCharge transactions')
                return False
            if not self._pin_stop_session_valid(connector_id, transaction_id):
                await reject('No active session or stop already pending')
                return False
            matched, config_version, configured = stop_pin_config.check(connector_id, password)
            print(f'[PIN-DEBUG][VECTOR] event=CHECK head={connector_id} txn={transaction_id} configured={configured} matched={matched}', flush=True)
            if not configured:
                await reject('PIN not configured')
                return False
            if not matched:
                await reject('Wrong PIN')
                return False
            if not self._pin_stop_session_valid(connector_id, transaction_id):
                print(f'[PIN-DEBUG][VECTOR] event=ABORT head={connector_id} reason=session_changed_before_response', flush=True)
                await reject('Session changed or stop already pending')
                return False
            # Reply before arming stop; PIN comparison is local, no Authorize call.
            if not await reply(dict(head=connector_id, status='Accepted')):
                print(f'[PIN-DEBUG][VECTOR] event=ABORT head={connector_id} reason=response_publish_failed', flush=True)
                logging.warning('[PIN-STOP] C%s response delivery failed; no stop armed', connector_id)
                return False
            if not self._pin_stop_session_valid(connector_id, transaction_id):
                print(f'[PIN-DEBUG][VECTOR] event=ABORT head={connector_id} reason=session_changed_after_response', flush=True)
                return False
            if not stop_pin_config.unchanged(connector_id, config_version):
                print(f'[PIN-DEBUG][VECTOR] event=ABORT head={connector_id} reason=config_changed_after_response', flush=True)
                logging.warning('[PIN-STOP] C%s config changed during reply; no stop armed', connector_id)
                return False
            self._mark_pending_local_stop(connector_id)
            setattr(self, f'block{connector_id}', 0)
            setattr(self, f'my_remote_stop_c{connector_id}', 1)
            setattr(self, f'start_sendMeterValue_SW{connector_id}', 1)
            _write_file(f'block{connector_id}.txt', 0)
            _write_file(f'my_remote_stop_c{connector_id}.txt', 1)
            _write_file(f'start_sendMeterValue_SW{connector_id}.txt', 1)
            # Existing _handle_remote_stop_cN owns MQTT pulse and StopTransaction.
            print(f'[PIN-DEBUG][VECTOR] event=STOP_ARMED head={connector_id} txn={transaction_id}', flush=True)
            logging.info('[PIN-STOP] C%s txn=%s configured PIN matched; local stop armed',
                         connector_id, transaction_id)
            return True
        finally:
            busy.discard(connector_id)

    def _arm_rfid_local_stop(self, connector_id, transaction_id, id_tag, source):
        """Hand one validated RFID stop to the existing Local stop sequence."""
        if connector_id not in (1, 2):
            return False
        if self._active_transaction_id(connector_id) != int(transaction_id or 0):
            logging.warning(
                "[RFID-STOP] C%s idTag=%s %s ignored: transaction changed",
                connector_id, id_tag, source)
            return False
        if (self._remote_stop_worker_running(connector_id) or
                int(getattr(self, f"remote_stop_finishing_c{connector_id}", 0) or 0) in (1, 2)):
            logging.warning(
                "[RFID-STOP] C%s idTag=%s %s ignored: RemoteStop owns the session",
                connector_id, id_tag, source)
            return False
        if int(getattr(self, f"my_remote_stop_c{connector_id}", 0) or 0) == 1:
            return False

        self._mark_pending_local_stop(connector_id)
        setattr(self, f"block{connector_id}", 0)  # existing handler maps this to Local
        setattr(self, f"my_remote_stop_c{connector_id}", 1)
        setattr(self, f"start_sendMeterValue_SW{connector_id}", 1)
        _write_file(f"block{connector_id}.txt", 0)
        _write_file(f"my_remote_stop_c{connector_id}.txt", 1)
        _write_file(f"start_sendMeterValue_SW{connector_id}.txt", 1)
        logging.warning(
            "[RFID-STOP] C%s txn=%s idTag=%s source=%s accepted; Local stop armed",
            connector_id, transaction_id, id_tag, source)
        return True

    async def _rfid_stop_with_authorize(self, connector_id, id_tag, plc_state=None):
        """Stop for the owner card; reject every other card locally as Invalid."""
        if connector_id not in (1, 2) or not self._valid_id_tag(id_tag):
            return False

        transaction_id = self._active_transaction_id(connector_id)
        if transaction_id <= 0:
            logging.info("[RFID-STOP] C%s ignoring idTag=%s: no open transaction",
                         connector_id, id_tag)
            return False
        if (self._remote_stop_worker_running(connector_id) or
                int(getattr(self, f"remote_stop_finishing_c{connector_id}", 0) or 0) in (1, 2)):
            logging.info("[RFID-STOP] C%s ignoring idTag=%s: RemoteStop in progress",
                         connector_id, id_tag)
            return False
        if int(getattr(self, f"my_remote_stop_c{connector_id}", 0) or 0) == 1:
            return False

        # The physical-reader caller supplies raw PLC state. A suspended transaction
        # is still stoppable, but a detached/idle connector is not.
        if plc_state is not None and str(plc_state) not in (
                "Charging", "SuspendedEV", "SuspendedEVSE"):
            return False

        # A retained reader level can deliver the same card repeatedly. Claim the
        # attempt before any await so a refused/slow CSMS response is also deduped.
        tag_attr = f"_rfid_stop_auth_tag_c{connector_id}"
        ts_attr = f"_rfid_stop_auth_ts_c{connector_id}"
        normalized_tag = str(id_tag).strip().lower()
        now = time.monotonic()
        if (str(getattr(self, tag_attr, "") or "").strip().lower() == normalized_tag and
                now - float(getattr(self, ts_attr, 0.0) or 0.0)
                < RFID_STOP_AUTH_COOLDOWN_SECONDS):
            return False
        setattr(self, tag_attr, normalized_tag)
        setattr(self, ts_attr, now)

        owner_tag = self._session_start_id_tag(connector_id)
        if owner_tag and self._same_id_tag(owner_tag, id_tag):
            logging.info(
                "[RFID-STOP] C%s txn=%s idTag=%s matches session owner; "
                "stopping without CSMS Authorize", connector_id, transaction_id, id_tag)
            return self._arm_rfid_local_stop(
                connector_id, transaction_id, id_tag, "owner-card")

        logging.warning(
            "[RFID-STOP-INVALID] C%s txn=%s idTag=%s owner=%s status=Invalid; "
            "card does not own this session and charging continues",
            connector_id, transaction_id, id_tag, owner_tag or "unknown")
        return False

    # ---- Physical RFID reader handlers (SPEED FIX applied) ----
    async def _handle_rfid_auth_c1(self, iCp, usi):
        authorize1 = await async_get_rfid(plc, self.myip_adr_c1)
        if not authorize1 or len(authorize1) <= 2: return
        if (self._dual_manager is not None and self._active_transaction_id(1) <= 0
                and (not self._dual_ready or self._dual_manager.connector_reserved(1))):
            return
        self.myid_tag_ath_c1 = authorize1
        _write_file("myid_tag_ath_c1.txt", authorize1)

        if authorize1 == self.accepted_tag1 and iCp == 'Charging':
            self._mark_pending_local_stop(1)
            await async_remote_stop_plc(self.myip_adr_c1, 1)
            await asyncio.sleep(3)
            await async_remote_stop_plc(self.myip_adr_c1, 0)
            self.accepted_tag1 = '0'

        # [RFID-STOP-INVALID] A different card cannot stop this session.
        elif self._active_transaction_id(1) > 0:
            await self._rfid_stop_with_authorize(1, authorize1, plc_state=iCp)

        elif iCp == 'Preparing' and usi == 'Available':
            if self.remote_start_only:
                return   # [REMOTE-ONLY] ปิดเส้นทาง RFID start
            if self._dual_manager is not None:
                if self._dual_connector_busy(1) or not self._dual_manager.claim_local(1, authorize1):
                    return
            self._pin_autocharge_start_intent_c1 = ""
            self.accepted_tag1 = authorize1
            if self.transactionId_c1 not in (0, -1, None): return
            await async_remote_start_plc(self.myip_adr_c1, 1)
            await asyncio.sleep(0.5) # [SPEED FIX] ลดจาก 2 วิ เหลือ 0.5 วิ
            await async_remote_start_plc(self.myip_adr_c1, 0)
            self.start_transaction_SW1 = 1
            self.block1 = 0
            _write_file("start_transaction_SW1.txt", 1)
            _write_file("block1.txt", 0)

    async def _handle_rfid_auth_c2(self, iCp, usi):
        authorize2 = await async_get_rfid(plc, self.myip_adr_c2)
        if not authorize2 or len(authorize2) <= 2: return
        if (self._dual_manager is not None and self._active_transaction_id(2) <= 0
                and (not self._dual_ready or self._dual_manager.connector_reserved(2))):
            return
        self.myid_tag_ath_c2 = authorize2
        _write_file("myid_tag_ath_c2.txt", authorize2)

        if authorize2 == self.accepted_tag2 and iCp == 'Charging':
            self._mark_pending_local_stop(2)
            await async_remote_stop_plc(self.myip_adr_c2, 1)
            await asyncio.sleep(3)
            await async_remote_stop_plc(self.myip_adr_c2, 0)
            self.accepted_tag2 = '0'

        # [RFID-STOP-INVALID] A different card cannot stop this session.
        elif self._active_transaction_id(2) > 0:
            await self._rfid_stop_with_authorize(2, authorize2, plc_state=iCp)

        elif iCp == 'Preparing' and usi == 'Available':
            if self.remote_start_only:
                return   # [REMOTE-ONLY] ปิดเส้นทาง RFID start
            if self._dual_manager is not None:
                if self._dual_connector_busy(2) or not self._dual_manager.claim_local(2, authorize2):
                    return
            self._pin_autocharge_start_intent_c2 = ""
            self.accepted_tag2 = authorize2
            if self.transactionId_c2 not in (0, -1, None): return
            await async_remote_start_plc(self.myip_adr_c2, 1)
            await asyncio.sleep(0.5) # [SPEED FIX] ลดจาก 2 วิ เหลือ 0.5 วิ
            await async_remote_start_plc(self.myip_adr_c2, 0)
            self.start_transaction_SW2 = 1
            self.block2 = 0
            _write_file("start_transaction_SW2.txt", 1)
            _write_file("block2.txt", 0)
        
    # ---- MeterValues handler ----
    async def _txn_energy_gate_open(self, connector_id, iCp, usi):
        """[METER-GATE-V12] เปิดทางส่ง MeterValues เมื่อเห็นพลังงานจริงแล้วเท่านั้น

        ก่อน authorize ตู้ยังไม่จ่ายไฟเข้ารถ (power = 0) แม้ controller จะไล่
        sequence ของมันเอง (insulation test / precharge → PLC ขึ้น SuspendedEV)
        โดยเฉพาะ txn ที่ resume หลัง reconnect — ช่วงนี้ห้ามส่ง MeterValues
        เมื่อ authorize แล้วรถเริ่มดึงกระแส ครั้งแรกที่ meter เห็น current/power
        เกิน threshold จะ latch แล้วส่งต่อเนื่องจนจบ transaction (รวมช่วง EV
        พักกลางคันเป็น SuspendedEV ด้วย)
        """
        if connector_id == 1:
            confirmed_attr = '_txn_energy_confirmed_c1'
            log_ts_attr = '_meter_gate_log_ts_c1'
        elif connector_id == 2:
            confirmed_attr = '_txn_energy_confirmed_c2'
            log_ts_attr = '_meter_gate_log_ts_c2'
        else:
            return False

        if getattr(self, confirmed_attr, False):
            return True

        snapshot = await self._read_energy_state(connector_id)
        if snapshot['valid'] and snapshot['flowing']:
            setattr(self, confirmed_attr, True)
            logging.info(
                f"[METER-GATE-V12] C{connector_id} charging power confirmed "
                f"current={snapshot['current']:.2f}A power={snapshot['power']:.2f}kW "
                "-> MeterValues enabled"
            )
            return True

        # Meter อ่านไม่ได้ (stale/เสีย) แต่ PLC ยืนยัน Charging ทั้งสองฝั่ง
        # -> เปิดตาม PLC เพื่อไม่ให้ตู้ที่ meter มีปัญหาเงียบไปทั้ง txn
        # (fallback แนวเดียวกับ _desired_active_status ของ V10)
        if not snapshot['valid'] and iCp == 'Charging' and usi == 'Charging':
            setattr(self, confirmed_attr, True)
            logging.warning(
                f"[METER-GATE-V12] C{connector_id} meter unavailable but PLC "
                "reports Charging -> MeterValues enabled via PLC fallback"
            )
            return True

        now = time.time()
        if now - float(getattr(self, log_ts_attr, 0.0) or 0.0) >= 10.0:
            setattr(self, log_ts_attr, now)
            logging.info(
                f"[METER-GATE-V12] C{connector_id} holding MeterValues: no real "
                f"charging power yet (PLC={iCp}/{usi} "
                f"current={snapshot['current']:.2f}A "
                f"power={snapshot['power']:.2f}kW valid={snapshot['valid']})"
            )
        return False

    async def _handle_meter_values(self, iCp1, usi1, iCp2, usi2):
        METER_STATES = ['Charging', 'SuspendedEV', 'SuspendedEVSE']

        def _to_int(v):
            try:
                return int(v) if v not in ['', None] else 0
            except Exception:
                return 0

        fc1, fc2 = _to_int(self.finshing_sent_c1), _to_int(self.finshing_sent_c2)
        c1_active = usi1 in METER_STATES and iCp1 in METER_STATES
        c2_active = usi2 in METER_STATES and iCp2 in METER_STATES
        txn1 = self._active_transaction_id(1)
        txn2 = self._active_transaction_id(2)
        remote_allowed_c1 = self._meter_values_remote_start_allowed(1)
        remote_allowed_c2 = self._meter_values_remote_start_allowed(2)

        # [METER-GATE-V12] จบ/ไม่มี txn แล้ว -> session ถัดไปต้องยืนยันพลังงานใหม่
        if txn1 <= 0:
            self._txn_energy_confirmed_c1 = False
        if txn2 <= 0:
            self._txn_energy_confirmed_c2 = False

        # [METER-WATCHDOG-V9] A valid active transaction is the source of truth.
        # Re-arm a stale MeterValues switch after reconnect or partial state-file
        # updates instead of silently returning forever.
        if txn1 > 0 and c1_active and not remote_allowed_c1:
            self._log_meter_values_remote_start_blocked(
                1, txn1, source="periodic_watchdog")
        if txn2 > 0 and c2_active and not remote_allowed_c2:
            self._log_meter_values_remote_start_blocked(
                2, txn2, source="periodic_watchdog")

        if (remote_allowed_c1 and txn1 > 0 and c1_active and not fc1 and
                self.start_sendMeterValue_SW1 != 2):
            self.transactionId_c1 = txn1
            self.start_sendMeterValue_SW1 = 2
            _write_file("start_sendMeterValue_SW1.txt", 2)
            logging.warning(f"[METER-WATCHDOG-V9] C1 re-armed MeterValues txn={txn1}")
        if (remote_allowed_c2 and txn2 > 0 and c2_active and not fc2 and
                self.start_sendMeterValue_SW2 != 2):
            self.transactionId_c2 = txn2
            self.start_sendMeterValue_SW2 = 2
            _write_file("start_sendMeterValue_SW2.txt", 2)
            logging.warning(f"[METER-WATCHDOG-V9] C2 re-armed MeterValues txn={txn2}")

        if self.start_sendMeterValue_SW1 != 2 and self.start_sendMeterValue_SW2 != 2:
            return

        send_c1 = (remote_allowed_c1 and self.start_sendMeterValue_SW1 == 2 and
                   txn1 > 0 and c1_active and
                   not self.my_remote_stop_c1 and not fc1 and
                   self.remote_stop_finishing_c1 == 0)
        send_c2 = (remote_allowed_c2 and self.start_sendMeterValue_SW2 == 2 and
                   txn2 > 0 and c2_active and
                   not self.my_remote_stop_c2 and not fc2 and
                   self.remote_stop_finishing_c2 == 0)

        # [METER-GATE-V12] ยังไม่เห็นพลังงานจริง (ยังไม่ authorize / รถยังไม่ดึงไฟ)
        # -> ระงับ MeterValues ไว้ก่อน
        if send_c1:
            send_c1 = await self._txn_energy_gate_open(1, iCp1, usi1)
        if send_c2:
            send_c2 = await self._txn_energy_gate_open(2, iCp2, usi2)

        if send_c1:
            async with self._meter_send_lock_c1:
                await self.send_meter_values(1)
        if send_c2:
            async with self._meter_send_lock_c2:
                await self.send_meter_values(2)
        if send_c1 or send_c2:
            await asyncio.sleep(4)

        if _to_int(self.finshing_sent_c1) == 1:
            self.start_sendMeterValue_SW1 = 1
            _write_file("start_sendMeterValue_SW1.txt", 1)
        if _to_int(self.finshing_sent_c2) == 1:
            self.start_sendMeterValue_SW2 = 1
            _write_file("start_sendMeterValue_SW2.txt", 1)

    # ---- Emergency handlers ----
    async def _handle_emergency_c1(self, iCp, usi):
        if self.emergency_sent_Sw_c1 == 1:
            if self.emergency_sent_c1 == "0":
                if self.emergency_cp0_sent == 0:
                    await self.status_notification_emer(0)
                    self.emergency_cp0_sent = 1
                await self.status_notification_emer(1)
                self.emergency_sent_Sw_c1 = 2
                _write_file("emergency_sent_Sw_c1.txt", 2)
                # [EMERGENCY-STOP-TXN] Gate on the OCPP transaction, not the cached PLC
                # state. The fault knocks the PLC out of Charging before this runs, so
                # `iCpStateCss1 == 'Charging'` was already False and StopTransaction was
                # skipped while a transaction was still open - which also left the
                # SESSION-FINISHING latch unarmed, so the connector reported Preparing.
                if self._active_transaction_id(1) > 0:
                    try:
                        await self.send_stop_transaction(1, reasonf='EmergencyStop')
                        self.block1 = 3
                        _write_file("block1.txt", 3)
                        _write_file("translogs1.txt", 0)
                        # [SESSION-FINISHING] latch ตั้งใน send_stop_transaction() แล้ว
                        # ครอบคลุมการจบ session ทุกแบบ ไม่ต้องตั้งซ้ำที่นี่
                    except Exception as e:
                        print(f"## emergency stop C1 error: {e}")
        else:
            if self.emergency_sent_c1 == '1':
                if self.emergency_cp0_sent == 1:
                    await self.status_notification_cp(0)
                    self.emergency_cp0_sent = 0
                await asyncio.sleep(0.5)
                iCp_cur, usi_cur = plc.get_connector_status(self.myip_adr_c1)
                if iCp_cur == 'Faulted':
                    # [EMERGENCY-RELEASE] Do NOT guess 'Available' here. When the PLC
                    # is Faulted, get_connector_status() returns (Faulted, Faulted) -
                    # usi_cur can never be Preparing/Charging - so the old expression
                    # always produced 'Available'. _normalize_ocpp_status reads
                    # Available as "the driver unplugged" and releases the
                    # SESSION-FINISHING latch, which is why the status after an
                    # emergency came out Preparing at random and why the latch was
                    # already gone by the next reboot. Faulted does not tell us
                    # anything about the cable, so report nothing and let the Vector
                    # fault-clear path (or the next main-loop pass) decide once the
                    # PLC has settled - both re-read it.
                    logging.info(
                        "[EMERGENCY-RELEASE] C1 PLC still Faulted; leaving the status "
                        "to the fault-clear path"
                    )
                else:
                    await self.status_notification_with_status(1, iCp_cur)
                    if iCp_cur == 'Available': self.block_sw1_C1, self.block_sw2_C1 = 0, 1
                    else: self.block_sw1_C1, self.block_sw2_C1 = 1, 0
                    _write_file("block_sw1_C1.txt", self.block_sw1_C1)
                    _write_file("block_sw2_C1.txt", self.block_sw2_C1)
                self.emergency_sent_Sw_c1 = 1
                _write_file("emergency_sent_Sw_c1.txt", 1)

    async def _handle_emergency_c2(self, iCp, usi):
        if self.emergency_sent_Sw_c2 == 1:
            if self.emergency_sent_c2 == "0":
                if self.emergency_cp0_sent == 0:
                    await self.status_notification_emer(0)
                    self.emergency_cp0_sent = 1
                await self.status_notification_emer(2)
                self.emergency_sent_Sw_c2 = 2
                _write_file("emergency_sent_Sw_c2.txt", 2)
                # [EMERGENCY-STOP-TXN] See connector 1.
                if self._active_transaction_id(2) > 0:
                    try:
                        await self.send_stop_transaction(2, reasonf='EmergencyStop')
                        self.block2 = 3
                        _write_file("block2.txt", 3)
                        _write_file("translogs2.txt", 0)
                        # [SESSION-FINISHING] ดู Connector 1
                    except Exception as e:
                        print(f"## emergency stop C2 error: {e}")
        else:
            if self.emergency_sent_c2 == '1':
                if self.emergency_cp0_sent == 1:
                    await self.status_notification_cp(0)
                    self.emergency_cp0_sent = 0
                await asyncio.sleep(0.5)
                iCp_cur, usi_cur = plc.get_connector_status(self.myip_adr_c2)
                if iCp_cur == 'Faulted':
                    # [EMERGENCY-RELEASE] See connector 1.
                    logging.info(
                        "[EMERGENCY-RELEASE] C2 PLC still Faulted; leaving the status "
                        "to the fault-clear path"
                    )
                else:
                    await self.status_notification_with_status(2, iCp_cur)
                    if iCp_cur == 'Available': self.block_sw1, self.block_sw2 = 0, 1
                    else: self.block_sw1, self.block_sw2 = 1, 0
                    _write_file("block_sw1.txt", self.block_sw1)
                    _write_file("block_sw2.txt", self.block_sw2)
                self.emergency_sent_Sw_c2 = 1
                _write_file("emergency_sent_Sw_c2.txt", 1)


# ============================================================================
# main
# ============================================================================
@asynccontextmanager
async def _ocpp_connection():
    if DUAL_CSMS_CONFIG.get("enabled", False):
        manager = DualCSMSManager(DUAL_CSMS_CONFIG, state_dir=os.getcwd())
        try:
            yield manager
        finally:
            await manager.close()
    else:
        async with websockets.connect(
            "wss://csms-a.example.com/ocpp/CP-A-0001",
            subprotocols=["ocpp1.6"], open_timeout=20,
            ping_interval=30, ping_timeout=60, close_timeout=10,
        ) as ws:
            yield ws


async def main():
    pin_stop_bridge.bind(None, None)
    async with _ocpp_connection() as ws:
        logging.info("[CONNECT] Starting %s controller",
                     "dual-CSMS" if getattr(ws, "is_dual", False) else "single-CSMS")
        cp_cls = ChargePoint('CP_1', ws, iCpStateCss1=icp_ini1, iCpStateCss2=icp_ini2)
        cp_cls._ocpp_state_dir = Path.cwd()
        cp_cls.SupportedFeatureProfiles_cfg = "Core,LocalAuthListManagement,RemoteTrigger,SmartCharging,Reservation"
        cp_cls._ocpp_protocol()
        if cp_cls._dual_manager is not None:
            ws.bind_core(cp_cls)
        notifier.notify("READY=1")

        # ---- Inject references into plc_function RFID handler ----
        global_main_loop = asyncio.get_running_loop()
        set_cp_instance(cp_cls, global_main_loop)

        # [RFID-STOP-INVALID] Point the topic/RFIDCommand callback at THIS session.
        # main() reconnects in a loop and builds a new ChargePoint each time, so the
        # callback itself is registered once at import and only these two references
        # are re-pointed - re-registering per connect would stack dead callbacks.
        global _RFID_STOP_CP, _RFID_STOP_LOOP
        _RFID_STOP_CP = cp_cls
        _RFID_STOP_LOOP = global_main_loop

        # Step 1: Start message handler
        start_task = asyncio.create_task(ws.start() if cp_cls._dual_manager is not None else cp_cls.start())
        watchdog_task = asyncio.create_task(systemd_watchdog_task())
        await asyncio.sleep(0.1)

        # Step 2: BootNotification
        logging.info("[INIT] Sending BootNotification...")
        await cp_cls.boot_notification_cp()
        if cp_cls._dual_manager is not None:
            logging.info("[DUAL] Router initialized; upstream registration is independent")
        else:
            logging.info("[INIT] BootNotification accepted!")
        pin_stop_bridge.bind(cp_cls, global_main_loop)
        cp_cls._seed_autocharge_events()

        # Process the retained Vector fault snapshot before normal status output.
        # This prevents an active fault from being overwritten by NoError at boot.
        await cp_cls.vector_fault_startup_sync()

        # Step 3: StatusNotifications
        # ── BOOT-STABILIZE (สำหรับ unexpected reboot ที่ resume txn) ──
        # หลัง process restart/power loss, PLC ใช้เวลา ~15 วิ กว่าจะ report state ถูก
        # ถ้ามี active transaction (translogs > 0) ต้องรอ PLC stable ก่อนส่ง StatusNotification
        # ใช้ meter เป็น ground truth (PLC อาจ cache stale state นาน)
        # 
        # หมายเหตุ: หลัง Soft Reset (Option B) translogs จะถูกล้างเป็น 0 → ข้าม stabilization
        logging.info("[INIT] Sending StatusNotifications...")

        # Connector 0 ส่งทันที (Available — ไม่ขึ้นกับ PLC)
        await cp_cls.status_notification_cp(0)
        await asyncio.sleep(0.5)

        def _has_active_txn(head: int) -> int:
            try:
                return int(open(f"translogs{head}.txt").read().strip() or "0")
            except Exception:
                return 0

        async def _wait_plc_stable(head: int, ip_addr: str, meter_ip: str, max_wait: float = 25.0):
            """รอให้ state เสถียร — ใช้ meter เป็น ground truth
            
            ลำดับความน่าเชื่อถือ:
            1. PLC report Charging/SuspendedEVSE → ใช้เลย
            2. Meter มี current>1A หรือ power>0.5kW → return "Charging" (override PLC stale)
            3. Timeout → ใช้ค่า PLC สุดท้าย
            """
            elapsed = 0.0
            last_plc_state = None
            charging_count = 0

            while elapsed < max_wait:
                try:
                    icp_now, _ = plc.get_connector_status(ip_addr)
                    last_plc_state = icp_now or last_plc_state
                except Exception:
                    icp_now = None

                if icp_now in ("Charging", "SuspendedEVSE"):
                    logging.info(f"[INIT] C{head} PLC reports {icp_now} directly")
                    return icp_now

                try:
                    live = await asyncio.wait_for(
                        async_run_blocking(get_livemeasure_pilot, meter_ip),
                        timeout=1.5
                    )
                    current = float(live.get('current', 0) or 0)
                    power   = float(live.get('power', 0) or 0)
                    if current > 1.0 or power > 0.5:
                        charging_count += 1
                        if charging_count >= 2:
                            logging.info(
                                f"[INIT] C{head} charging confirmed via meter "
                                f"(current={current:.2f}A, power={power:.2f}kW) "
                                f"— override PLC state={last_plc_state}"
                            )
                            return "Charging"
                    else:
                        charging_count = 0
                except Exception:
                    pass

                await asyncio.sleep(1.0)
                elapsed += 1.0

            logging.warning(
                f"[INIT] C{head} stabilize timeout ({max_wait}s) — using PLC state={last_plc_state}"
            )
            return last_plc_state

        _meter_ip_map = {1: cp_cls.my_meter_ip_c1, 2: cp_cls.my_meter_ip_c2}
        boot_stable_states = {}

        for cid in (1, 2):
            ip_addr = '1' if cid == 1 else '2'
            active_txn = _has_active_txn(cid)
            if active_txn > 0:
                logging.info(f"[INIT] C{cid} resuming txn={active_txn} — waiting for PLC to stabilize...")
                # sync memory ↔ file
                if cid == 1 and cp_cls.transactionId_c1 != active_txn:
                    cp_cls.transactionId_c1 = active_txn
                elif cid == 2 and cp_cls.transactionId_c2 != active_txn:
                    cp_cls.transactionId_c2 = active_txn

                stable_state = await _wait_plc_stable(
                    cid, ip_addr, _meter_ip_map[cid], max_wait=25.0
                )
                boot_stable_states[cid] = stable_state
                logging.info(f"[INIT] C{cid} final state → {stable_state}  (txn={active_txn} preserved)")

                if cid == 1:
                    cp_cls.iCpStateCss1 = stable_state or cp_cls.iCpStateCss1
                else:
                    cp_cls.iCpStateCss2 = stable_state or cp_cls.iCpStateCss2
                await cp_cls.status_notification_with_status(cid, stable_state or 'SuspendedEV')
            else:
                # ไม่มี active txn (เช่นหลัง Soft Reset ที่ Option B ล้างไปแล้ว)
                # → ส่ง StatusNotification ตาม PLC state ปกติ
                await cp_cls.status_notification_cp(cid)
            await asyncio.sleep(0.5)

        # Step 4: Auto-reset + resume sessions
        logging.info("[INIT] Checking PLC state...")
        try:
            icp1_boot, _ = plc.get_connector_status('1')
            icp2_boot, _ = plc.get_connector_status('2')
            logging.info(f"[INIT] Boot PLC state: C1={icp1_boot}, C2={icp2_boot}")

            def _reset_connector(head):
                files = {
                    f"translogs{head}.txt": "0",
                    f"block{head}.txt": "0",
                    f"my_remote_stop_c{head}.txt": "0",
                    f"remote_stop_finishing_c{head}.txt": "0",
                    f"finshing_sent_c{head}.txt": "0",
                    f"finshing_sent_sw_c{head}.txt": "1",
                    f"start_transaction_SW{head}.txt": "1",
                    f"start_sendMeterValue_SW{head}.txt": "1",
                    f"rfid_start_received_c{head}.txt": "0",
                    f"local_status_session_c{head}.txt": "0",   # [LOCAL-STATUS-V16]
                    # [START-LATCH] R3.1.6: a RemoteStart from before the restart has no
                    # session and no CSMS claim left (both checked by the caller)
                    f"myremotestart_c{head}.txt": "0",
                    f"remote_start_received_c{head}.txt": "0",
                    f"active_id_tag_c{head}.txt": "Default",
                    f"emergency_sent_c{head}.txt": "1",
                    f"emergency_sent_Sw_c{head}.txt": "1",
                    # [SESSION-FINISHING] only reached when the PLC already reports
                    # Available, i.e. the cable is out - the connector is free again.
                    f"session_finishing_c{head}.txt": "0",
                }
                files.update({
                    "block_sw1_C1.txt": "0",
                    "block_sw2_C1.txt": "1",
                    "block_sw3_C1.txt": "1",
                } if head == 1 else {
                    "block_sw1.txt": "0",
                    "block_sw2.txt": "1",
                    "block_sw3.txt": "1",
                })
                for fname, val in files.items(): _write_file(fname, val)

            # Keep mapped sessions for loss_network_regis to close/resume explicitly.
            if icp1_boot == 'Available' and not (cp_cls._dual_manager is not None and ws.connector_reserved(1)):
                _reset_connector(1)
                cp_cls.transactionId_c1 = 0
                cp_cls.accepted_tag1 = '0'
                cp_cls.block1 = 0
                cp_cls.block_sw1_C1 = 0
                cp_cls.block_sw2_C1 = cp_cls.block_sw3_C1 = 1
                cp_cls.start_transaction_SW1 = cp_cls.start_sendMeterValue_SW1 = 1
                cp_cls.my_remote_stop_c1 = 0
                cp_cls.remote_stop_finishing_c1 = 0
                cp_cls.rfid_start_received_c1 = 0
                cp_cls.myremotestart_c1 = cp_cls.remote_start_received_c1 = 0   # [START-LATCH]
                cp_cls.local_status_session_c1 = 0        # [LOCAL-STATUS-V16]
                cp_cls.session_finishing_c1 = 0
                cp_cls.active_id_tag_c1 = "Default"

            if icp2_boot == 'Available' and not (cp_cls._dual_manager is not None and ws.connector_reserved(2)):
                _reset_connector(2)
                cp_cls.transactionId_c2 = 0
                cp_cls.accepted_tag2 = '0'
                cp_cls.block2 = 0
                cp_cls.block_sw1 = 0
                cp_cls.block_sw2 = cp_cls.block_sw3 = 1
                cp_cls.start_transaction_SW2 = cp_cls.start_sendMeterValue_SW2 = 1
                cp_cls.my_remote_stop_c2 = 0
                cp_cls.remote_stop_finishing_c2 = 0
                cp_cls.rfid_start_received_c2 = 0
                cp_cls.myremotestart_c2 = cp_cls.remote_start_received_c2 = 0   # [START-LATCH]
                cp_cls.local_status_session_c2 = 0        # [LOCAL-STATUS-V16]
                cp_cls.session_finishing_c2 = 0
                cp_cls.active_id_tag_c2 = "Default"

            await cp_cls.loss_network_regis(stable_states=boot_stable_states)

        except Exception as e:
            logging.warning(f"[INIT] auto-reset error: {e}")

        # Step 5: Run all background tasks
        cp_cls._dual_ready = True
        logging.info("[INIT] Starting background tasks...")
        background_tasks = [
            start_task,
            cp_cls._ocpp_protocol().run(),
            cp_cls.send_heartbeat(),
            cp_cls.websocket_ping_loop(),
            cp_cls.main_run_program(0.2),
            watchdog_task,
            edgebox_temp(),
        ]
        if VECTOR_FAULT_BRIDGE_ENABLED:
            background_tasks.append(cp_cls.vector_fault_event_loop())
        if cp_cls._v2g_bridge is not None:
            background_tasks.append(cp_cls._v2g_bridge.send_loop(cp_cls))
        await asyncio.gather(*background_tasks)

# ============================================================================
# Entry point with reliable auto-reconnect
# ============================================================================
RECONNECT_INITIAL_DELAY = 5
RECONNECT_MAX_DELAY = 5
RECONNECT_STABLE_SECONDS = 60


def _report_connection_failure(error, attempt: int, retry_delay: int, uptime: float):
    """Log/publish a disconnect without ever raising another exception.

    สำคัญ: exception handler เดิมอ้างตัวแปร res_te ที่ไม่มีอยู่ ทำให้ process
    ตายด้วย NameError ก่อนวนกลับไป asyncio.run(main()) รอบใหม่
    """
    now_bkk = datetime.now(pytz.timezone('Asia/Bangkok'))
    timestamp = now_bkk.strftime('%Y-%m-%dT%H:%M:%S.%f')
    error_text = f"{type(error).__name__}: {error}"

    logging.error(
        f"[RECONNECT] OCPP connection ended: {error_text}; "
        f"uptime={uptime:.1f}s attempt={attempt} retry_in={retry_delay}s"
    )
    print(f"## main error: {error_text}")

    try:
        with open("MDBlog.txt", "a") as f:
            f.write(
                f"{error_text} | uptime={uptime:.1f}s | "
                f"attempt={attempt} | retry_in={retry_delay}s | {timestamp}\n"
            )
    except Exception as log_error:
        logging.warning(f"[RECONNECT] cannot write MDBlog.txt: {log_error}")

    # ห้ามให้ MQTT/Telegram error ทำลาย reconnect loop
    try:
        payload = json.dumps({
            "error": error_text,
            "attempt": attempt,
            "retryIn": retry_delay,
            "timestamp": timestamp,
        }, ensure_ascii=False)
        client.publish(f"OCPP/{OCPPSTATION}/error", payload)
    except Exception as mqtt_error:
        logging.warning(f"[RECONNECT] cannot publish disconnect to MQTT: {mqtt_error}")

    # ส่ง Telegram ใน daemon thread เพื่อไม่หน่วง reconnect loop
    try:
        threading.Thread(
            target=line_notify,
            args=(
                f"{OCPPSTATION} disconnected: {error_text}; "
                f"retry in {retry_delay}s",
            ),
            daemon=True,
            name="ocpp-disconnect-notify",
        ).start()
    except Exception as notify_error:
        logging.debug(f"[RECONNECT] Telegram notification skipped: {notify_error}")


def _sleep_before_reconnect(seconds: int):
    """Sleep in short chunks and keep the systemd watchdog alive."""
    remaining = max(0.0, float(seconds))
    while remaining > 0:
        try:
            notifier.notify("WATCHDOG=1")
            notifier.notify(f"STATUS=OCPP reconnect in {remaining:.0f}s")
        except Exception:
            pass
        chunk = min(5.0, remaining)
        time.sleep(chunk)
        remaining -= chunk


if __name__ == "__main__":
    reconnect_delay = RECONNECT_INITIAL_DELAY
    reconnect_attempt = 0

    while True:
        session_started = time.monotonic()
        try:
            logging.info(
                f"[RECONNECT] Starting OCPP session "
                f"(next failure delay={reconnect_delay}s)"
            )
            asyncio.run(main())

            # main() ไม่ควรจบเองระหว่าง service ทำงาน
            raise ConnectionError("OCPP session ended without an exception")

        except KeyboardInterrupt:
            logging.info("[RECONNECT] Stopped by operator")
            break

        except Exception as e:
            uptime = time.monotonic() - session_started

            # ถ้า connection ใช้งานได้นานแล้ว ให้เริ่ม retry ใหม่ที่ 5 วินาที
            if uptime >= RECONNECT_STABLE_SECONDS:
                reconnect_delay = RECONNECT_INITIAL_DELAY
                reconnect_attempt = 0

            reconnect_attempt += 1
            _report_connection_failure(
                e,
                attempt=reconnect_attempt,
                retry_delay=reconnect_delay,
                uptime=uptime,
            )
            _sleep_before_reconnect(reconnect_delay)

            # Retry every 5 seconds, including a startup meter failure.
            if uptime < RECONNECT_STABLE_SECONDS:
                reconnect_delay = min(
                    RECONNECT_MAX_DELAY,
                    max(RECONNECT_INITIAL_DELAY, reconnect_delay * 2),
                )
            else:
                reconnect_delay = RECONNECT_INITIAL_DELAY
