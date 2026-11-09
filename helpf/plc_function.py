#!/usr/bin/env python3
"""
PLC Function SIM - MQTT Interface (Thread-Safe Version)
========================================================
Key fixes:
- Read operations don't need async wrappers (they read from cache)
- Thread-safe cache access
- Better debugging
- Topic callback hook (NEW): external code can subscribe to incoming MQTT
  topics via `PLCInterface.on_topic(topic, callback)` without touching the
  cache logic.

Author: Claude AI
Date: 2024-12-31
"""

import os
import time
import json
import logging
import threading
import pytz
import asyncio
from datetime import datetime
from typing import Optional, Any, Dict, List, Tuple, Union, Callable
from dataclasses import dataclass
from enum import Enum

import paho.mqtt.client as mqtt

# ============================================================================
# Logging Configuration
# ============================================================================
def _configure_logging() -> None:
    """Configure PLC logging only when the PLC runtime is started."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
        datefmt='%H:%M:%S'
    )


logger = logging.getLogger('PLC_SIM')


# ============================================================================
# Configuration
# ============================================================================
@dataclass
class MQTTConfig:
    """MQTT Configuration"""
    broker: str = os.getenv("MQTT_HOST", "192.168.3.11")  ## Vector
    port: int = int(os.getenv("MQTT_PORT", "1883"))
    username: Optional[str] = os.getenv("MQTT_USER", 'admin')          ## Vector
    password: Optional[str] = os.getenv("MQTT_PASS", 'rootpassword')   ## Vector
    keepalive: int = 60
    reconnect_delay: float = 5.0
    qos: int = 0
    retained_wait_time: float = 2.0


# ============================================================================
# Enums for Status
# ============================================================================
class ConnectorStatus(Enum):
    AVAILABLE = "Available"
    PREPARING = "Preparing"
    CHARGING = "Charging"
    FINISHING = "Finishing"
    SUSPENDED_EV = "SuspendedEV"
    SUSPENDED_EVSE = "SuspendedEVSE"
    FAULTED = "Faulted"
    UNAVAILABLE = "Unavailable"


class CPState(Enum):
    STATE_A = "state_a"
    STATE_B = "state_b"
    STATE_C = "state_c"
    STATE_D = "state_d"
    STATE_E = "state_e"
    STATE_F = "state_f"


# ============================================================================
# Topic Templates
# ============================================================================
class Topics:
    """MQTT Topic templates"""

    # Read topics
    SOC = "vsecc/connector/{connector}/ev/soc"
    CP_STATE = "vsecc/connector/{connector}/ev/cp_state"
    CHARGING_STATE = "vsecc/connector/{connector}/status/charging_session_state"
    REQUESTED_VOLTAGE = "vsecc/connector/{connector}/ev/requested_voltage"
    REQUESTED_CURRENT = "vsecc/connector/{connector}/ev/requested_current"
    EMERGENCY_STOP = "vsecc/io/d_in/8/value"

    # Custom topics
    RFID = "topic/RFID{connector}"
    FINISHING = "topic/Finishing{connector}"
    HEAD_DONE = "topic/Finishing{connector}"
    HEAD_ERROR = "topic/Head{connector}Error"

    # Write topics
    REMOTE_START = "topic/RemoteStart{connector}"
    REMOTE_STOP = "topic/RemoteStop{connector}"
    RESET = "topic/reset{connector}"
    LOW_PRIORITY = "topic/lowpiority{connector}"
    LIMIT_POWER = "topic/limitpower{connector}"
    LIMIT_CURRENT = "topic/limitcurrent{connector}"
    CONNECTOR_LIMIT = "topic/connector0"

    @classmethod
    def format(cls, template: str, connector: Union[int, str]) -> str:
        return template.format(connector=connector)


# ============================================================================
# MQTT Client Singleton (Thread-Safe)
# ============================================================================
class MQTTClientSingleton:
    """
    Thread-safe singleton MQTT client.

    IMPORTANT:
    - All read operations are thread-safe (use RLock)
    - Read from cache is INSTANT (no blocking)
    - No need for async wrappers for read operations!
    - External code can register per-topic callbacks via
      `register_topic_callback()` — callback is invoked from the MQTT
      network thread (paho loop thread), so make sure it is non-blocking.
    """

    _instance: Optional['MQTTClientSingleton'] = None
    _lock = threading.RLock()

    def __new__(cls, config: MQTTConfig = None):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
        return cls._instance

    def __init__(self, config: MQTTConfig = None):
        if self._initialized:
            return

        with self._lock:
            if self._initialized:
                return

            self.config = config or MQTTConfig()
            self._client: Optional[mqtt.Client] = None
            self._connected = threading.Event()
            self._data_lock = threading.RLock()
            self._latest_data: Dict[str, Dict[str, Any]] = {}
            self._subscribed_topics: set = set()
            # Exact topics that the "vsecc/#" / "topic/#" wildcards do not cover,
            # registered by external code (CP.py) via subscribe_topic().  topic -> qos.
            # Re-armed in _on_connect() on EVERY connect: with clean_session=True the
            # broker forgets every subscription when the session drops, so without this
            # a broker restart would leave us deaf.
            self._extra_subscriptions: Dict[str, int] = {}
            self._message_count = 0
            self._message_callbacks: Dict[str, List[Callable[..., None]]] = {}

            self._setup_client()
            self._initialized = True

    def _setup_client(self):
        try:
            self._client = mqtt.Client(
                client_id=f"plc_sim_{int(time.time())}",
                protocol=mqtt.MQTTv311,
                clean_session=True
            )

            if self.config.username:
                self._client.username_pw_set(
                    self.config.username,
                    self.config.password
                )

            self._client.on_connect = self._on_connect
            self._client.on_disconnect = self._on_disconnect
            self._client.on_message = self._on_message

            self._client.reconnect_delay_set(
                min_delay=1,
                max_delay=int(self.config.reconnect_delay)
            )

            self._connect()

        except Exception as e:
            logger.error(f"Failed to setup MQTT client: {e}")
            raise

    def _connect(self):
        try:
            logger.info(f"Connecting to MQTT broker {self.config.broker}:{self.config.port}")
            # connect_async() never raises on an unreachable host.  The blocking
            # connect() did, and it raised BEFORE loop_start() ever ran - so a broker
            # that was down at boot left this singleton with no network thread and
            # therefore no reconnect for the whole life of the process.  The Vector
            # fault topics ride on this same session now (see CP.py), and the client
            # they used to have used connect_async() for exactly this reason.
            self._client.connect_async(
                self.config.broker,
                self.config.port,
                self.config.keepalive
            )
            self._client.loop_start()

            if not self._connected.wait(timeout=10):
                logger.warning("Connection timeout; paho keeps retrying in background")
                return

            logger.info(f"Waiting {self.config.retained_wait_time}s for retained messages...")
            time.sleep(self.config.retained_wait_time)

            with self._data_lock:
                count = len(self._latest_data)
            logger.info(f"Received {count} retained values")

            self._log_cached_topics()

        except Exception as e:
            logger.error(f"Failed to connect to MQTT broker: {e}")

    def _log_cached_topics(self):
        with self._data_lock:
            logger.info("=" * 50)
            logger.info("Cached Topics:")
            for topic in sorted(self._latest_data.keys()):
                value = self._latest_data[topic]["value"]
                logger.info(f"  {topic} = {repr(value)}")
            logger.info("=" * 50)

    def _on_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0:
            logger.info("Connected to MQTT broker")
            self._connected.set()
            client.subscribe("vsecc/#", qos=self.config.qos)
            client.subscribe("topic/#", qos=self.config.qos)
            self._subscribed_topics.add("vsecc/#")
            self._subscribed_topics.add("topic/#")
            logger.info("Subscribed to vsecc/# and topic/#")

            # Topics registered through subscribe_topic() have to go out again on
            # every connect, not just the first one.
            with self._data_lock:
                extras = dict(self._extra_subscriptions)
            for extra_topic, extra_qos in extras.items():
                client.subscribe(extra_topic, qos=extra_qos)
                self._subscribed_topics.add(extra_topic)
            if extras:
                logger.info(
                    f"Re-subscribed {len(extras)} extra topic(s): {sorted(extras)}"
                )
        else:
            # Hint table kept from the fault client CP.py used to run: rc=4/5 on this
            # broker means the credentials are wrong, which is otherwise a very quiet
            # failure - nothing arrives and nothing says why.
            hint = {
                1: "unacceptable protocol version",
                2: "client_id rejected",
                3: "broker unavailable",
                4: "bad username/password (check MQTT_USER / MQTT_PASS)",
                5: "not authorised (check MQTT_USER / MQTT_PASS)",
            }.get(rc, "unknown reason")
            logger.error(
                f"Connection to {self.config.broker}:{self.config.port} failed with "
                f"code: {rc} - {hint} (user={self.config.username!r})"
            )
            self._connected.clear()

    def _on_disconnect(self, client, userdata, rc):
        logger.warning(
            f"Disconnected from MQTT broker {self.config.broker}:{self.config.port} "
            f"(rc={rc}); paho will retry"
        )
        self._connected.clear()

    def _on_message(self, client, userdata, msg):
        """Handle incoming MQTT message — thread-safe"""
        try:
            self._message_count += 1
            payload = msg.payload.decode("utf-8", errors="replace")

            try:
                data = json.loads(payload)
            except json.JSONDecodeError:
                if payload == "":
                    data = ""
                else:
                    try:
                        data = float(payload) if '.' in payload else int(payload)
                    except ValueError:
                        data = payload

            callbacks: List[Callable[[str, Any], None]] = []
            with self._data_lock:
                self._latest_data[msg.topic] = {
                    "value": data,
                    "timestamp": time.time(),
                    "retained": msg.retain
                }
                cb_list = self._message_callbacks.get(msg.topic)
                if cb_list:
                    callbacks = list(cb_list)

            if msg.retain:
                logger.debug(f"RETAINED: {msg.topic} = {repr(data)}")

            for cb in callbacks:
                try:
                    # [LOCAL-STATUS] callback ที่ตั้ง cb.wants_meta = True จะได้ meta
                    # เพิ่ม (retained / timestamp) โดย callback เดิมที่รับแค่
                    # (topic, value) ไม่ได้รับผลกระทบเลย
                    # ห้ามใช้ try/except TypeError แทน เพราะ TypeError ที่เกิด
                    # *ข้างใน* callback จะถูกเข้าใจผิดว่าเป็น arity ไม่ตรง
                    if getattr(cb, "wants_meta", False):
                        cb(msg.topic, data, {"retained": bool(msg.retain),
                                             "timestamp": time.time()})
                    else:
                        cb(msg.topic, data)
                except Exception as e:
                    logger.error(f"Callback error on topic {msg.topic}: {e}")

        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def register_topic_callback(self, topic: str,
                                callback: Callable[..., None]) -> None:
        """Register a callback for a specific topic."""
        if not callable(callback):
            raise TypeError("callback must be callable")

        with self._data_lock:
            if topic not in self._message_callbacks:
                self._message_callbacks[topic] = []
            self._message_callbacks[topic].append(callback)

        logger.info(f"Registered callback for topic: {topic}")

    def subscribe_topic(self, topic: str, qos: int = 0) -> None:
        """Subscribe an exact topic that "vsecc/#" / "topic/#" do not cover.

        The topic is REMEMBERED first and only then sent to the broker, so it is
        re-subscribed by _on_connect() after every reconnect.  Calling it twice with
        the same topic is harmless - a repeated SUBSCRIBE just re-applies the QoS.

        Register the callback with register_topic_callback() BEFORE calling this:
        a retained message is delivered as soon as the SUBSCRIBE is acked, and it is
        dropped if nothing is listening yet.
        """
        topic = str(topic).strip()
        if not topic:
            raise ValueError("topic must be a non-empty string")
        qos = int(qos)
        if qos not in (0, 1, 2):
            raise ValueError("qos must be 0, 1 or 2")

        with self._data_lock:
            self._extra_subscriptions[topic] = qos
            client = self._client

        # _on_connect() sets _connected before it reads _extra_subscriptions, and we
        # record the topic before reading _connected.  So if the two interleave, the
        # topic is either in the snapshot _on_connect takes or subscribed here -
        # possibly both, which costs one redundant SUBSCRIBE.  It cannot be missed.
        if self._connected.is_set() and client is not None:
            client.subscribe(topic, qos=qos)
            with self._data_lock:
                self._subscribed_topics.add(topic)
            logger.info(f"Subscribed extra topic: {topic} (qos={qos})")
        else:
            logger.info(f"Extra topic recorded while offline: {topic} (qos={qos}); "
                        f"it will be sent on connect")

    def unregister_topic_callback(self, topic: str,
                                  callback: Callable[..., None]) -> bool:
        """Remove a previously-registered callback. Returns True if removed."""
        with self._data_lock:
            cbs = self._message_callbacks.get(topic)
            if not cbs:
                return False
            try:
                cbs.remove(callback)
                if not cbs:
                    del self._message_callbacks[topic]
                return True
            except ValueError:
                return False

    def get_value(self, topic: str, default: Any = None) -> Any:
        """Get value from cache — THREAD-SAFE and INSTANT."""
        with self._data_lock:
            if topic in self._latest_data:
                return self._latest_data[topic]["value"]
        return default

    def publish(self, topic: str, payload: Any, retain: bool = True) -> bool:
        """Publish message — thread-safe"""
        try:
            if not self._connected.wait(timeout=5):
                logger.error("Not connected to broker")
                return False

            if isinstance(payload, dict):
                payload = json.dumps(payload)
            elif not isinstance(payload, str):
                payload = str(payload)

            result = self._client.publish(
                topic,
                payload,
                qos=self.config.qos,
                retain=retain
            )
            return result.rc == mqtt.MQTT_ERR_SUCCESS

        except Exception as e:
            logger.error(f"Failed to publish to {topic}: {e}")
            return False

    def get_all_cached_data(self) -> Dict[str, Any]:
        with self._data_lock:
            return {
                topic: item["value"]
                for topic, item in self._latest_data.items()
            }

    def get_cache_size(self) -> int:
        with self._data_lock:
            return len(self._latest_data)

    def is_connected(self) -> bool:
        return self._connected.is_set()

    def disconnect(self):
        try:
            self._client.loop_stop()
            self._client.disconnect()
            self._connected.clear()
            logger.info("Disconnected from MQTT broker")
        except Exception as e:
            logger.error(f"Error disconnecting: {e}")

    @classmethod
    def get_instance(cls, config: MQTTConfig = None) -> 'MQTTClientSingleton':
        return cls(config)

    @classmethod
    def reset_instance(cls):
        with cls._lock:
            if cls._instance:
                cls._instance.disconnect()
            cls._instance = None


# ============================================================================
# PLC Interface Class (Thread-Safe)
# ============================================================================
class PLCInterface:
    """
    High-level interface for PLC communication via MQTT.

    IMPORTANT:
    - All READ methods are THREAD-SAFE and INSTANT (< 1ms)
    - No need for async wrappers for read operations!
    - Only WRITE methods may need async wrappers (if blocking is a concern)
    - Use `on_topic()` to subscribe external handlers to specific incoming topics.
    """

    DIN_STATES = [
        "VSECCLIB_SST_SECCDiscovery",
        "VSECCLIB_SST_SupportedAppProtocol",
        "VSECCLIB_SST_DIN_SessionSetup",
        "VSECCLIB_SST_DIN_ServiceDiscovery",
        "VSECCLIB_SST_DIN_ServicePaymentSelection",
        "VSECCLIB_SST_DIN_ContractAuthentication",
        "VSECCLIB_SST_DIN_ChargeParameterDiscovery",
        "VSECCLIB_SST_DIN_CableCheck",
        "VSECCLIB_SST_DIN_PreCharge",
        "VSECCLIB_SST_DIN_PowerDelivery",
        "VSECCLIB_SST_DIN_CurrentDemand",
        "VSECCLIB_SST_DIN_WeldingDetection",
        "VSECCLIB_SST_DIN_SessionStop"
    ]

    def __init__(self, config: MQTTConfig = None):
        self.mqtt = MQTTClientSingleton.get_instance(config)
        logger.info(f"PLCInterface initialized, cache size: {self.mqtt.get_cache_size()}")

    # ========================================================================
    # Topic callback registration
    # ========================================================================
    def on_topic(self, topic: str, callback: Callable[..., None]) -> None:
        self.mqtt.register_topic_callback(topic, callback)

    def off_topic(self, topic: str, callback: Callable[..., None]) -> bool:
        return self.mqtt.unregister_topic_callback(topic, callback)

    def subscribe(self, topic: str, qos: int = 0) -> None:
        """Subscribe an exact topic on the same MQTT session the PLC uses.

        For topics outside the "vsecc/#" / "topic/#" wildcards.  Call on_topic()
        first - see MQTTClientSingleton.subscribe_topic().
        """
        self.mqtt.subscribe_topic(topic, qos)

    # ========================================================================
    # READ METHODS — All THREAD-SAFE and INSTANT
    # ========================================================================

    def get_soc(self, connector: Union[int, str]) -> Optional[float]:
        topic = Topics.format(Topics.SOC, connector)
        value = self.mqtt.get_value(topic)
        if value is not None:
            try:
                return float(value)
            except (ValueError, TypeError):
                pass
        return value

    def get_cp_state(self, connector: Union[int, str]) -> Optional[str]:
        topic = Topics.format(Topics.CP_STATE, connector)
        return self.mqtt.get_value(topic)

    def get_charging_session_state(self, connector: Union[int, str]) -> Optional[str]:
        topic = Topics.format(Topics.CHARGING_STATE, connector)
        return self.mqtt.get_value(topic)

    def get_connector_status(self, connector: Union[int, str]) -> Tuple[str, str]:
        cp_state = self.get_cp_state(connector)
        session_state = self.get_charging_session_state(connector)
        finishing = self.get_finishing_status(connector)
        head_error = self.get_head_error(connector)

        status_v1 = ConnectorStatus.AVAILABLE.value
        status_v2 = ConnectorStatus.AVAILABLE.value

        if head_error is not None:
            try:
                if int(head_error) == 1:
                    return ConnectorStatus.FAULTED.value, ConnectorStatus.FAULTED.value
            except (ValueError, TypeError):
                pass

        if cp_state == CPState.STATE_E.value or cp_state == CPState.STATE_F.value:
            return ConnectorStatus.FAULTED.value, ConnectorStatus.FAULTED.value

        if cp_state is None or cp_state == CPState.STATE_A.value:
            return ConnectorStatus.AVAILABLE.value, ConnectorStatus.AVAILABLE.value

        if cp_state == CPState.STATE_B.value:
            is_finishing = False
            if finishing is not None:
                try:
                    is_finishing = int(finishing) == 1
                except (ValueError, TypeError):
                    pass

            if is_finishing:
                return ConnectorStatus.FINISHING.value, ConnectorStatus.FINISHING.value

            if session_state and "SessionStop" in str(session_state):
                return ConnectorStatus.FINISHING.value, ConnectorStatus.FINISHING.value

            return ConnectorStatus.PREPARING.value, ConnectorStatus.PREPARING.value

        if cp_state == CPState.STATE_C.value:
            if session_state and "CurrentDemand" in str(session_state):
                return ConnectorStatus.CHARGING.value, ConnectorStatus.CHARGING.value
            return ConnectorStatus.SUSPENDED_EV.value, ConnectorStatus.SUSPENDED_EV.value

        return status_v1, status_v2

    def get_requested_voltage(self, connector: Union[int, str]) -> Optional[float]:
        topic = Topics.format(Topics.REQUESTED_VOLTAGE, connector)
        value = self.mqtt.get_value(topic)
        if value is not None:
            try:
                return float(value)
            except (ValueError, TypeError):
                pass
        return value

    def get_requested_current(self, connector: Union[int, str]) -> Optional[float]:
        topic = Topics.format(Topics.REQUESTED_CURRENT, connector)
        value = self.mqtt.get_value(topic)
        if value is not None:
            try:
                return float(value)
            except (ValueError, TypeError):
                pass
        return value

    def get_emergency_stop(self, connector: Union[int, str] = None) -> Optional[str]:
        value = self.mqtt.get_value(Topics.EMERGENCY_STOP)
        if value is not None:
            return str(value)
        return None

    def get_rfid(self, connector: Union[int, str]) -> Optional[str]:
        topic = Topics.format(Topics.RFID, connector)
        value = self.mqtt.get_value(topic)
        if value is not None and value != "":
            return str(value)
        return None

    def get_finishing_status(self, connector: Union[int, str]) -> Optional[Any]:
        topic = Topics.format(Topics.FINISHING, connector)
        return self.mqtt.get_value(topic)

    def get_head_done(self, connector: Union[int, str]) -> Optional[Any]:
        topic = Topics.format(Topics.HEAD_DONE, connector)
        return self.mqtt.get_value(topic)

    def get_head_error(self, connector: Union[int, str]) -> Optional[Any]:
        topic = Topics.format(Topics.HEAD_ERROR, connector)
        return self.mqtt.get_value(topic)

    # ========================================================================
    # WRITE METHODS
    # ========================================================================

    def remote_start(self, connector: Union[int, str], value: int = 1) -> bool:
        topic = Topics.format(Topics.REMOTE_START, connector)
        # [LOCAL-STATUS] จดเวลา *ก่อน* publish เสมอ — publish() block ได้ถึง 5 วิ
        # ที่ _connected.wait() และ echo อาจกลับมาถึงตอนเรายังอยู่ข้างในนั้น
        _note_self_publish("start", connector)
        return self.mqtt.publish(topic, {"remote_start": value})

    def remote_stop(self, connector: Union[int, str], value: int = 1) -> bool:
        topic = Topics.format(Topics.REMOTE_STOP, connector)
        _note_self_publish("stop", connector)      # [LOCAL-STATUS] ดู remote_start()
        return self.mqtt.publish(topic, {"remote_stop": value})

    def reset(self, connector: Union[int, str], value: int = 1) -> bool:
        topic = Topics.format(Topics.RESET, connector)
        return self.mqtt.publish(topic, {"reset": value})

    def set_low_priority(self, connector: Union[int, str], value: int = 1) -> bool:
        topic = Topics.format(Topics.LOW_PRIORITY, connector)
        return self.mqtt.publish(topic, {"lowpiority": value})

    def set_power_limit(self, connector: Union[int, str], limit_power: float) -> bool:
        topic = Topics.format(Topics.LIMIT_POWER, connector)
        return self.mqtt.publish(topic, {"limitpower": limit_power})

    def set_current_limit(self, connector: Union[int, str], limit_current: float) -> bool:
        topic = Topics.format(Topics.LIMIT_CURRENT, connector)
        return self.mqtt.publish(topic, {"limitcurrent": limit_current})

    def set_connector_limit(self, value: float, ip_adr: str = None) -> bool:
        payload = {"connector0": value}
        if ip_adr:
            payload["ip_adr"] = ip_adr
        return self.mqtt.publish(Topics.CONNECTOR_LIMIT, payload)

    # ========================================================================
    # CONVENIENCE METHODS
    # ========================================================================

    def start_charging(self, connector: Union[int, str]) -> bool:
        success = self.remote_start(connector, 1)
        if success:
            time.sleep(0.5)
            self.remote_start(connector, 0)
        return success

    def stop_charging(self, connector: Union[int, str]) -> bool:
        success = self.remote_stop(connector, 1)
        if success:
            time.sleep(0.5)
            self.remote_stop(connector, 0)
        return success

    def get_all_status(self) -> Dict[str, Any]:
        result = {
            "connected": self.mqtt.is_connected(),
            "cache_size": self.mqtt.get_cache_size(),
            "connectors": {}
        }
        for conn in [1, 2]:
            status_v1, status_v2 = self.get_connector_status(conn)
            result["connectors"][conn] = {
                "soc": self.get_soc(conn),
                "cp_state": self.get_cp_state(conn),
                "session_state": self.get_charging_session_state(conn),
                "status_v1": status_v1,
                "status_v2": status_v2,
                "requested_voltage": self.get_requested_voltage(conn),
                "requested_current": self.get_requested_current(conn),
                "rfid": self.get_rfid(conn),
                "finishing": self.get_finishing_status(conn),
                "head_done": self.get_head_done(conn),
                "head_error": self.get_head_error(conn),
            }
        result["emergency_stop"] = self.get_emergency_stop()
        return result

    def get_cached_topics(self) -> Dict[str, Any]:
        return self.mqtt.get_all_cached_data()

    def is_connected(self) -> bool:
        return self.mqtt.is_connected()

    def close(self):
        self.mqtt.disconnect()


# ============================================================================
# Backward Compatibility Functions (Legacy API)
# ============================================================================
_plc_interface: Optional[PLCInterface] = None
_plc_lock = threading.Lock()


def _get_plc() -> PLCInterface:
    global _plc_interface
    if _plc_interface is None:
        with _plc_lock:
            if _plc_interface is None:
                _configure_logging()
                _plc_interface = PLCInterface()
    return _plc_interface


def get_soc_R(ip_adr: str) -> Optional[float]:
    return _get_plc().get_soc(ip_adr)

def read_tag_status_R(ip_adr: str) -> Tuple[str, str]:
    return _get_plc().get_connector_status(ip_adr)

def read_Emergency_stop_R(ip_adr: str) -> Optional[Any]:
    return _get_plc().get_emergency_stop(ip_adr)

def HeadFinishing_R(ip_adr: str) -> Optional[Any]:
    return _get_plc().get_finishing_status(ip_adr)

def read_tag_read_R(ip_adr: str) -> Optional[str]:
    return _get_plc().get_rfid(ip_adr)

def finish_one_c_R(ip_adr: str) -> Optional[Any]:
    return _get_plc().get_head_done(ip_adr)

def h1_error_R(ip_adr: str) -> Optional[Any]:
    return _get_plc().get_head_error(ip_adr)

def TargetVoltageEvCcs_R(ip_adr: str) -> Optional[float]:
    return _get_plc().get_requested_voltage(ip_adr)

def TargetCurrentCcs_R(ip_adr: str) -> Optional[float]:
    return _get_plc().get_requested_current(ip_adr)

def remote_start_plc_W(ip_adr: str, value: int) -> str:
    controller = globals().get("_cp_instance")
    gate = getattr(controller, "_ocpp_hardware_start_allowed", None)
    if value == 1:
        # CP dispatch and its SQLite state belong to the CP event loop.  This
        # function normally runs in asyncio.to_thread() or an MQTT worker: never
        # query that state on the publishing worker, and never disable SQLite's
        # thread guard merely to make a start pulse succeed.
        loop = globals().get("_main_loop")
        if not callable(gate) or loop is None or loop.is_closed() or not loop.is_running():
            return 'Remote Start Failed: controller admission unavailable'
        try:
            connector = int(ip_adr)
            if connector not in (1, 2) or str(ip_adr).strip() not in ("1", "2"):
                return 'Remote Start Failed: invalid connector'
            try:
                current_loop = asyncio.get_running_loop()
            except RuntimeError:
                current_loop = None
            if current_loop is loop:
                allowed = bool(gate(connector))
            else:
                cancelled = threading.Event()
                async def admission_on_cp_loop():
                    if (cancelled.is_set() or globals().get("_cp_instance") is not controller or
                            globals().get("_main_loop") is not loop):
                        return False
                    return bool(gate(connector))
                request = admission_on_cp_loop()
                try:
                    decision = asyncio.run_coroutine_threadsafe(request, loop)
                except Exception:
                    request.close()
                    raise
                try:
                    allowed = bool(decision.result(timeout=2.0))
                except Exception:
                    cancelled.set()
                    decision.cancel()
                    raise
            if (not allowed or globals().get("_cp_instance") is not controller or
                    globals().get("_main_loop") is not loop):
                return 'Remote Start Failed: availability or reservation'
        except Exception:
            # Timeout, shutdown, stale binding or a failed gate is a denied pulse.
            # A zero reset below intentionally does not depend on admission.
            return 'Remote Start Failed: controller admission unavailable'
    success = _get_plc().remote_start(ip_adr, value)
    return 'Remote Start Successful' if success else 'Remote Start Failed'

def remote_stop_plc_W(ip_adr: str, value: int) -> str:
    success = _get_plc().remote_stop(ip_adr, value)
    return 'Remote Stop Successful' if success else 'Remote Stop Failed'

def read_OCPP_HardReset_W(ip_adr: str, value: int) -> str:
    success = _get_plc().reset(ip_adr, value)
    return 'Hard reset' if success else 'Hard reset Failed'

def cmd_low_W(ip_adr: str, value: int) -> str:
    success = _get_plc().set_low_priority(ip_adr, value)
    return 'force limit current' if success else 'force limit current Failed'

def limitpower_W(ip_adr: str, limit_power: float) -> str:
    success = _get_plc().set_power_limit(ip_adr, limit_power)
    return 'limitpower ok' if success else 'limitpower Failed'

def limitcurrent_W(ip_adr: str, value: float) -> str:
    success = _get_plc().set_current_limit(ip_adr, value)
    return 'limitcurrent ok' if success else 'limitcurrent Failed'

def limit2connector_W(ip_adr: str, value: float) -> str:
    success = _get_plc().set_connector_limit(value, ip_adr)
    if success:
        return f'limit2connector ok. (ip_adr={ip_adr}, value={value})'
    return f'limit2connector Failed (ip_adr={ip_adr}, value={value})'

# Legacy aliases
remote_start_plc = remote_start_plc_W
remote_stop_plc  = remote_stop_plc_W
limitpower       = limitpower_W
limitcurrent     = limitcurrent_W
limit2connector  = limit2connector_W
cmd_low          = cmd_low_W


def convert_timeformat(current_time: datetime) -> str:
    return current_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + 'Z'


# ============================================================================
# RFID Command Handler
# ============================================================================
# ฟังก์ชัน RFID handler ทั้งหมดถูกย้ายมาจาก CP.py เพื่อแยก concern ให้ชัดเจน
# - CP.py จัดการ OCPP protocol เท่านั้น
# - plc_function.py จัดการ PLC/MQTT + RFID command routing
#
# การ inject ChargePoint reference ทำผ่าน set_cp_instance()
# ที่เรียกจาก main() ใน CP.py หลังสร้าง ChargePoint instance แล้ว
# ============================================================================

start_lock = threading.Lock()
last_sent: Dict[str, int] = {"1": 0, "2": 0}

# ============================================================================
# [LOCAL-STATUS] ตรวจจับการกด Start/Stop จากหน้าจอ HMI
# ----------------------------------------------------------------------------
# HMI (Node-RED) publish topic/RemoteStart{n} / topic/RemoteStop{n} ตรง ๆ เพื่อ
# สั่งฮาร์ดแวร์ ในขณะที่ process นี้ก็ publish topic เดียวกันบน broker เดียวกัน
# และ subscribe "topic/#" อยู่แล้ว -> ต้องแยก "ของเราเอง" ออกจาก "ของหน้าจอ"
# ให้ได้ก่อนเป็นอันดับแรก มิฉะนั้นทุกครั้งที่เราสั่ง RemoteStart จะถูกอ่านกลับ
# เป็นการกดปุ่มจากหน้าจอ
#
# callback วิ่งบน paho network thread จึงทำแค่ parse + filter แล้วฝากคำขอไว้ใน
# _pending_request; ผู้ตัดสินใจจริงคือ CP.main_run_program (tick 0.2 วิ)
# ============================================================================
_local_evt_lock = threading.RLock()
# เวลาที่ process นี้ publish ครั้งล่าสุด (monotonic) แยกตามชนิดและหัวชาร์จ
_self_publish_ts: Dict[str, Dict[str, float]] = {
    "start": {"1": 0.0, "2": 0.0},
    "stop":  {"1": 0.0, "2": 0.0},
}
# ระดับล่าสุดที่ "รับเข้ามา" ใช้ตรวจ rising edge 0 -> 1
_last_level: Dict[str, Dict[str, int]] = {
    "start": {"1": 0, "2": 0},
    "stop":  {"1": 0, "2": 0},
}
# คำขอที่ยังไม่ถูก consume: head -> monotonic timestamp
_pending_request: Dict[str, Dict[str, float]] = {"start": {}, "stop": {}}

# พัลส์ที่ยาวที่สุดของเราเอง = 1 -> sleep 3 -> 0 (_send_remote_start)
LOCAL_EVT_ECHO_WINDOW_S = float(os.getenv("LOCAL_EVT_ECHO_WINDOW_S", "12"))
LOCAL_EVT_REQUEST_TTL_S = float(os.getenv("LOCAL_EVT_REQUEST_TTL_S", "60"))


def _note_self_publish(kind: str, connector: Union[int, str]) -> None:
    """จดเวลาที่ process นี้ publish RemoteStart/RemoteStop เพื่อกรอง echo ตัวเอง"""
    key = str(connector).strip()
    if key in ("1", "2"):
        with _local_evt_lock:
            _self_publish_ts[kind][key] = time.monotonic()

# References injected from CP.py via set_cp_instance()
_cp_instance: Any = None
_main_loop: Any = None


def set_cp_instance(cp_inst: Any, loop: Any) -> None:
    """Inject ChargePoint instance and asyncio event loop.

    Must be called from CP.py's main() after creating the ChargePoint:
        set_cp_instance(cp_cls, asyncio.get_running_loop())
    """
    global _cp_instance, _main_loop
    _cp_instance = cp_inst
    _main_loop = loop
    logger.info("[RFID] cp_instance and main_loop injected")


def _write_file(path: str, value: Any) -> None:
    """Thread-safe helper to write a single value to a state file."""
    try:
        with open(path, "w") as f:
            f.write(str(value))
    except Exception as e:
        logger.error(f"_write_file {path} error: {e}")


def safe_publish(head: int, value: int) -> None:
    """Publish remote_start only when value actually changes (dedup)."""
    key = str(head)
    if last_sent.get(key) == value:
        logger.info(f"[SKIP] duplicate value {value} head {head}")
        return
    last_sent[key] = value
    remote_start_plc(str(head), value)


def _send_remote_start(head: int) -> None:
    """Pulse topic/RemoteStart{head}: 1 → wait 3s → 0  (blocking, run in thread)."""
    if not start_lock.acquire(blocking=False):
        logger.info(f"[RFID-CMD] IGNORE duplicate start head {head}")
        return
    try:
        now = datetime.now(pytz.timezone('Asia/Bangkok')).strftime("%H:%M:%S.%f")[:-3]
        # logger.info(
        #     f"\n------------------------------------------------------------\n"
        #     f"[PULSE START head {head}] time={now}  publish=1\n"
        #     f"------------------------------------------------------------"
        # )
        safe_publish(head, 1)
        time.sleep(3)
        now = datetime.now(pytz.timezone('Asia/Bangkok')).strftime("%H:%M:%S.%f")[:-3]
        # logger.info(f"[PULSE START head {head}] time={now}  publish=0 (done)")
        safe_publish(head, 0)
    except Exception as e:
        logger.error(f"[RFID-CMD] _send_remote_start head {head} error: {e}")
    finally:
        start_lock.release()


def _send_remote_stop(head: int) -> None:
    """Pulse topic/RemoteStop{head}: 1 → wait 3s → 0  (blocking, run in thread)."""
    try:
        # logger.info(f"[RFID-CMD] STOP head {head}")
        remote_stop_plc(str(head), 1)
        time.sleep(3)
        # logger.info(f"[RFID-CMD] RESET STOP head {head}")
        remote_stop_plc(str(head), 0)
    except Exception as e:
        logger.error(f"[RFID-CMD] _send_remote_stop head {head} error: {e}")


def handle_rfid_command(payload_str: str) -> None:
    """Process a JSON payload from topic/RFIDCommand.

    Expected payload format:
        {"head": 1, "idTag": "E4C61B02", "action": "start"}
        {"head": 1, "idTag": "E4C61B02", "action": "stop"}
    """
    now = datetime.now(pytz.timezone('Asia/Bangkok')).strftime("%H:%M:%S.%f")[:-3]
    # logger.info(
    #     f"\n============================================================\n"
    #     f"[RFID TAP via HMI] time={now}\n"
    #     f"[RFID TAP via HMI] payload={payload_str}\n"
    #     f"============================================================"
    # )

    try:
        data = json.loads(payload_str)
    except Exception as e:
        logger.error(f"[RFID-CMD] invalid JSON: {e}")
        return

    try:
        head = int(data.get("head", 0))
    except Exception:
        logger.error(f"[RFID-CMD] invalid head: {data.get('head')}")
        return

    id_tag = str(data.get("idTag", "")).strip()
    action = str(data.get("action", "")).strip().lower()
    logger.info(f"[RFID-CMD] parsed: head={head}, idTag={id_tag}, action={action}")

    if head not in (1, 2):
        logger.warning(f"[RFID-CMD] unsupported head={head}")
        return

    # ------------------------------------------------------------------ START
    if action == "start":
        if not id_tag:
            logger.warning("[RFID-CMD] START: missing idTag")
            return

        if _cp_instance is None or _main_loop is None:
            logger.error("[RFID-CMD] START: cp_instance/main_loop not ready — call set_cp_instance() first")
            return

        if getattr(_cp_instance, "_dual_manager", None) is not None:
            # The main loop owns connector claims in dual-CSMS mode. Do not
            # overwrite tags or pulse the PLC from this MQTT callback thread.
            command = _cp_instance._dual_rfid_command_start(head, id_tag)
            try:
                future = asyncio.run_coroutine_threadsafe(command, _main_loop)
            except Exception:
                command.close()
                logger.exception("[RFID-CMD] Cannot schedule dual-CSMS start head=%s", head)
                return

            def _dual_start_done(result):
                if result.cancelled():
                    logger.warning("[RFID-CMD] Dual-CSMS start cancelled head=%s", head)
                    return
                try:
                    result.result()
                except Exception:
                    logger.exception("[RFID-CMD] Dual-CSMS start failed head=%s", head)

            future.add_done_callback(_dual_start_done)
            return

        # Pre-store idTag so ChargePoint can use it in StartTransaction.
        # NOTE: ใช้ทั้ง myid_tag_cX และ myid_tag_ath_cX เพราะหลังจากแก้ไข
        # ด้านล่างเราจะ route MQTT-RFID flow ไปทาง start_transaction_ath()
        # (SW=1) ซึ่งใช้ myid_tag_ath_cX แทน myid_tag_cX
        if head == 1:
            _cp_instance.myid_tag_c1 = id_tag
            _cp_instance.myid_tag_ath_c1 = id_tag
            _write_file("myid_tag_c1.txt", id_tag)
            _write_file("myid_tag_ath_c1.txt", id_tag)
        else:
            _cp_instance.myid_tag_c2 = id_tag
            _cp_instance.myid_tag_ath_c2 = id_tag
            _write_file("myid_tag_c2.txt", id_tag)
            _write_file("myid_tag_ath_c2.txt", id_tag)

        def _authorize_then_start() -> None:
            try:
                # logger.info(f"[RFID-CMD] AUTHORIZE head {head}, idTag={id_tag}")
                future = asyncio.run_coroutine_threadsafe(
                    _cp_instance.authorize(id_tag),
                    _main_loop
                )
                response = future.result(timeout=30)

                # Extract status from response (handle both dict and object)
                status = None
                try:
                    status = response.id_tag_info.get("status")
                except Exception:
                    try:
                        status = response.id_tag_info["status"]
                    except Exception:
                        status = None

                # logger.info(f"[RFID-CMD] AUTHORIZE result head {head}: status={status}")

                if status != "Accepted":
                    logger.warning(
                        f"[RFID-CMD] REJECT head {head}: idTag={id_tag} not accepted (status={status})"
                    )
                    return

                # Accepted → set transaction flags + pulse PLC
                # ── FIX (txn_id=-1 issue): ใช้ SW=1 → start_transaction_ath() ──
                # เดิมตั้ง SW=3 ทำให้ไปวิ่ง start_transaction() ซึ่ง "ไม่" ส่ง
                # Authorize ซ้ำก่อน StartTransaction ผลคือถ้าผู้ใช้เสียบสาย/
                # EV negotiate ช้า (>~30s) ทำให้ CSMS หมดอายุ session แล้ว
                # reject ด้วย Invalid + transactionId=-1
                #
                # SW=1 → start_transaction_ath() จะส่ง Authorize ใหม่ติดกับ
                # StartTransaction ทันที (ใช้ cache ถ้าเปิด AuthorizationCache)
                # → ไม่มี gap ระหว่าง 2 message
                # ── PERSIST-FIX: เขียน accepted_tagX ลง disk ด้วย ──
                # เดิมเก็บแค่ in-memory → reboot กลางคันแล้วหา accepted_tag ไม่เจอ
                # ทำให้ STOP RFID ใบเดิมโดน reject ("expected='0'")
                # ใช้ pattern เดียวกับที่ STOP handler line ~960 เคลียร์ค่ากลับเป็น '0'
                if head == 1:
                    _cp_instance.accepted_tag1 = id_tag
                    _cp_instance.start_transaction_SW1 = 1
                    _write_file("start_transaction_SW1.txt", 1)
                    _write_file("accepted_tag1.txt", id_tag)
                else:
                    _cp_instance.accepted_tag2 = id_tag
                    _cp_instance.start_transaction_SW2 = 1
                    _write_file("start_transaction_SW2.txt", 1)
                    _write_file("accepted_tag2.txt", id_tag)

                # logger.info(f"[RFID-CMD] ACCEPTED head {head} → pulse PLC")
                _send_remote_start(head)

            except asyncio.TimeoutError:
                logger.error(f"[RFID-CMD] AUTHORIZE timeout head {head}")
            except Exception as e:
                logger.error(f"[RFID-CMD] AUTHORIZE error head {head}: {e}")

        threading.Thread(target=_authorize_then_start, daemon=True).start()
        return

    # ------------------------------------------------------------------- STOP
    if action == "stop":
        if _cp_instance is None:
            logger.error("[RFID-CMD] STOP: cp_instance not ready")
            return

        expected_tag = getattr(
            _cp_instance, "accepted_tag1" if head == 1 else "accepted_tag2", "0"
        )

        if not expected_tag or expected_tag == "0":
            logger.warning(
                f"[RFID-CMD] STOP REJECTED head {head}: "
                f"no active accepted_tag (expected={expected_tag!r})"
            )
            return

        if not id_tag:
            logger.warning(f"[RFID-CMD] STOP REJECTED head {head}: missing idTag")
            return

        if id_tag != expected_tag:
            logger.warning(
                f"[RFID-CMD] STOP REJECTED head {head}: "
                f"idTag {id_tag!r} != accepted_tag {expected_tag!r}"
            )
            return

        logger.info(f"[RFID-CMD] STOP ACCEPTED head {head}: idTag={id_tag}")

        if head == 1:
            _cp_instance.start_sendMeterValue_SW1 = 1
            _cp_instance.block_sw1_C1 = 0
            _cp_instance.my_remote_stop_c1 = 1
            _write_file("start_sendMeterValue_SW1.txt", 1)
            _write_file("block_sw1_C1.txt", 0)
            _write_file("my_remote_stop_c1.txt", 1)
        else:
            _cp_instance.start_sendMeterValue_SW2 = 1
            _cp_instance.block_sw1 = 0
            _cp_instance.my_remote_stop_c2 = 1
            _write_file("start_sendMeterValue_SW2.txt", 1)
            _write_file("block_sw1.txt", 0)
            _write_file("my_remote_stop_c2.txt", 1)

        def _stop_and_reset() -> None:
            _send_remote_stop(head)
            time.sleep(1)
            if head == 1:
                logger.info("## RESET accepted_tag1 AFTER FINISHING")
                _cp_instance.accepted_tag1 = "0"
                _write_file("accepted_tag1.txt", "0")
            else:
                logger.info("## RESET accepted_tag2 AFTER FINISHING")
                _cp_instance.accepted_tag2 = "0"
                _write_file("accepted_tag2.txt", "0")

        threading.Thread(target=_stop_and_reset, daemon=True).start()
        return

    logger.warning(f"[RFID-CMD] unknown action={action!r}")


def _rfid_command_cb(topic: str, payload_value: Any) -> None:
    """MQTT callback — invoked from paho network thread, must be non-blocking."""
    try:
        # logger.info(f"[RFID-CMD] callback fired: topic={topic}, value={payload_value!r}")
        payload_str = (
            json.dumps(payload_value)
            if isinstance(payload_value, (dict, list))
            else str(payload_value)
        )
        handle_rfid_command(payload_str)
    except Exception as e:
        logger.error(f"[RFID-CMD] callback error: {e}")


# ============================================================================
# [LOCAL-STATUS] Rising-edge detector สำหรับ topic/RemoteStart{n} / RemoteStop{n}
# ============================================================================

def _parse_pulse_value(payload_value: Any, *keys: str) -> Optional[int]:
    """payload -> 0/1  (None = อ่านไม่ออก)

    รองรับทุกรูปแบบที่หน้าจอ/Node-RED อาจส่งมา: {"remote_start": 1}, "1", 1, true
    """
    raw = payload_value
    if isinstance(raw, dict):
        for k in keys:
            if k in raw:
                raw = raw[k]
                break
    if isinstance(raw, bool):
        return 1 if raw else 0
    try:
        return 1 if int(float(str(raw).strip())) == 1 else 0
    except (TypeError, ValueError):
        return None


def _make_pulse_cb(kind: str, marker: str, *keys: str) -> Callable[..., None]:
    """สร้าง callback ตรวจ rising edge 0->1 ที่ "ไม่ใช่เสียงสะท้อนของเราเอง"

    รันบน paho network thread: parse + filter เท่านั้น ห้าม block และห้ามแตะ
    CP state ตรง ๆ  การ arm latch จริงทำใน CP.main_run_program ผ่าน
    take_local_request()
    """
    tag = f"[LOCAL-{kind.upper()}]"

    def _cb(topic: str, payload_value: Any, meta: Dict[str, Any]) -> None:
        try:
            head = topic.rsplit(marker, 1)[-1].strip()
            if head not in ("1", "2"):
                return

            value = _parse_pulse_value(payload_value, *keys)
            if value is None:
                logger.warning(f"{tag} head {head}: payload อ่านไม่ออก {payload_value!r}")
                return

            now = time.monotonic()
            with _local_evt_lock:
                previous = _last_level[kind].get(head, 0)
                _last_level[kind][head] = value

                # retained replay หลัง broker reconnect ไม่ใช่การกดปุ่มใหม่
                # (_on_connect subscribe ใหม่ -> broker ส่ง retained ซ้ำทันที)
                if meta.get("retained"):
                    logger.info(f"{tag} head {head}: retained level={value} "
                                "(replay) -> sync level อย่างเดียว ไม่ arm")
                    return

                # falling edge = ปลายพัลส์ปกติ ห้าม cancel latch/transaction ใด ๆ
                if value != 1:
                    return

                # level เดิม ไม่ใช่ edge
                #
                # start: หน้าจอยิงเป็นพัลส์จริง (1 -> ~5s -> 0) edge detection จึงใช้ได้
                #   และ "ต้อง" ใช้ edge เพื่อกันการ re-arm session ซ้ำ
                #
                # stop: จากการสังเกต traffic จริง หน้าจอ "ไม่เคย" ยิง 0 กลับ —
                #   topic/RemoteStop{n} ค้างที่ 1 ไปเรื่อย ๆ จนกว่า CP.py จะรีเซ็ต
                #   ตอน init_remote_topics()  ถ้าใช้ edge กับ stop ด้วย การกด Stop
                #   ครั้งที่สองในช่วงที่ level ยังเป็น 1 อยู่จะเงียบหายไปทั้งครั้ง
                #   -> stop จึงเป็น level-triggered: ทุก publish ที่เป็น 1 และไม่ใช่
                #   retained/echo ถือเป็นคำสั่ง  ปลอดภัยเพราะ stop เป็น idempotent
                #   (_sync_local_status_latch คืน "stop" เฉพาะตอนมี session อยู่จริง
                #    และ _session_finishing_already_reported กัน Finishing ซ้ำบนสาย)
                if kind != "stop" and previous == 1:
                    return

                age = now - float(_self_publish_ts[kind].get(head, 0.0) or 0.0)
                if age <= LOCAL_EVT_ECHO_WINDOW_S:
                    logger.info(f"{tag} head {head}: ข้าม rising edge — "
                                f"echo ของเราเอง ({age:.2f}s ที่แล้ว)")
                    return

                _pending_request[kind][head] = now

            logger.warning(f"{tag} head {head}: ตรวจพบคำสั่งจากหน้าจอ "
                           "(rising edge 0->1 ไม่ใช่ของเราเอง)")
        except Exception as e:
            logger.error(f"{tag} callback error on {topic}: {e}")

    _cb.wants_meta = True
    return _cb


_remote_start_cb = _make_pulse_cb("start", "RemoteStart", "remote_start", "RemoteStart")
_remote_stop_cb = _make_pulse_cb("stop", "RemoteStop", "remote_stop", "RemoteStop")


def take_local_request(kind: str, head: Union[int, str],
                       consume: bool = False) -> float:
    """คืน monotonic ts ของคำขอจากหน้าจอที่ค้างอยู่ (0.0 = ไม่มี)

    consume=True เมื่อ CP จัดการสำเร็จแล้ว; ถ้ายังจัดการไม่ได้ (เช่น PLC ยังไม่
    ขึ้น Preparing) ให้ปล่อยค้างไว้แล้ววนมาใหม่ tick ถัดไปจนกว่าจะหมด TTL —
    ต่างจากเส้นทาง RFID ที่มี _recently_authorized() 300 วิเป็นหน้าต่าง retry
    ให้อยู่แล้ว แต่ MQTT edge เปล่า ๆ ไม่มีอะไรแบบนั้น
    """
    key = str(head).strip()
    with _local_evt_lock:
        ts = float(_pending_request[kind].get(key, 0.0) or 0.0)
        if ts <= 0.0:
            return 0.0
        if time.monotonic() - ts > LOCAL_EVT_REQUEST_TTL_S:
            _pending_request[kind].pop(key, None)
            logger.warning(f"[LOCAL-{kind.upper()}] head {key}: คำขอหมดอายุ "
                           f"หลัง {LOCAL_EVT_REQUEST_TTL_S}s โดยยัง arm ไม่ได้")
            return 0.0
        if consume:
            _pending_request[kind].pop(key, None)
        return ts


def clear_local_request(kind: str, head: Union[int, str]) -> None:
    """ทิ้งคำขอที่ค้างอยู่ (ใช้เมื่อมีแหล่ง start อื่นเข้ามาแทน)"""
    with _local_evt_lock:
        _pending_request[kind].pop(str(head).strip(), None)


def init_local_start_detector() -> None:
    """seed ระดับปัจจุบันจาก cache แล้ว register callback ทั้ง 4 topic

    ต้องเรียก *หลัง* init_remote_topics() เสมอ เพราะ init_remote_topics()
    publish retained 0 ทั้งสองหัวไปแล้ว -> ค่าที่ detector เห็นครั้งแรกจึงเป็น
    ค่าที่เราเพิ่งเขียนเอง ไม่ใช่ของค้างจาก session ก่อน
    """
    plc_instance = _get_plc()

    for kind, tmpl, keys, cb in (
        ("start", Topics.REMOTE_START, ("remote_start", "RemoteStart"), _remote_start_cb),
        ("stop", Topics.REMOTE_STOP, ("remote_stop", "RemoteStop"), _remote_stop_cb),
    ):
        for head in ("1", "2"):
            topic = Topics.format(tmpl, head)
            try:
                seed = _parse_pulse_value(plc_instance.mqtt.get_value(topic), *keys)
                with _local_evt_lock:
                    _last_level[kind][head] = 0 if seed is None else seed
                plc_instance.on_topic(topic, cb)
                logger.info(f"[LOCAL-STATUS] registered {topic} "
                            f"(seed level={_last_level[kind][head]})")
            except Exception as e:
                logger.error(f"[LOCAL-STATUS] register {topic} ล้มเหลว: {e}")


def init_remote_topics() -> None:
    """Reset RemoteStart/RemoteStop for all heads to 0 on boot.
    Prevents stale values from a previous session.
    """
    try:
        for head in (1, 2):
            res_start = remote_start_plc(str(head), 0)
            last_sent[str(head)] = 0
            logger.info(f"[INIT] reset topic/RemoteStart{head} = 0: {res_start}")
            res_stop = remote_stop_plc(str(head), 0)
            logger.info(f"[INIT] reset topic/RemoteStop{head} = 0: {res_stop}")
    except Exception as e:
        logger.error(f"[INIT] init_remote_topics error: {e}")


# ============================================================================
# Explicit runtime initialisation
# ============================================================================
# Keep module import side-effect free so other processes (for example the RFID
# reader service) can reuse MQTTConfig without opening a second PLC connection
# or resetting retained RemoteStart/RemoteStop topics.
plc: Optional[PLCInterface] = None
_plc_runtime_lock = threading.Lock()
_plc_runtime_initialized = False


def initialize_plc_runtime() -> PLCInterface:
    """Start the PLC MQTT runtime once and return its shared interface."""
    global plc, _plc_runtime_initialized

    if _plc_runtime_initialized and plc is not None:
        return plc

    with _plc_runtime_lock:
        if _plc_runtime_initialized and plc is not None:
            return plc

        plc = _get_plc()

        # Register RFID callback on the Vector broker (same broker as PLC).
        try:
            plc.on_topic("topic/RFIDCommand", _rfid_command_cb)
            logger.info(
                "[RFID-CMD] registered callback for topic/RFIDCommand "
                "(Vector broker)"
            )
        except Exception as e:
            logger.error(f"[RFID-CMD] failed to register callback: {e}")

        # Reset remote topics before seeding the local edge detector.
        init_remote_topics()
        init_local_start_detector()

        _plc_runtime_initialized = True
        return plc
