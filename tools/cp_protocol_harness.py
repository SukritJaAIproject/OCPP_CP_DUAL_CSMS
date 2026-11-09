"""Bind reviewed CP.py protocol methods to simulated hardware without CP startup.

Only an explicit AST whitelist is compiled. CP.py's imports, module statements,
hardware constructor, MQTT setup, GPIO and operating-system actions never run.
Each bound core gets its own globals and rooted persistence; no process-wide
working-directory change is used. This tests the actual handlers, not a copy of
their behavior, while physical AutoCharge event seeding remains a no-op.
"""
from __future__ import annotations

import ast
import asyncio
import copy
import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from types import MethodType

from ocpp.v16 import call_result
from helpf.stop_pin_config import StopPinConfig, KEYS as STOP_PIN_KEYS


SOURCE = Path(__file__).resolve().parents[1] / "CP.py"
if not SOURCE.exists():
    SOURCE = Path(__file__).with_name("cp_protocol_defs.py")
METHODS = ("on_get_configuration", "on_change_configuration", "on_clear_cache",
           "on_get_local_list_version", "on_send_local_list", "_autocharge_prefix")
DEFINITIONS = ("_write_configuration_file", "_parse_csv_list", "_normalize_csv_string",
               "_normalize_autocharge_prefix", "convert_timeformat", "AuthorizationCache",
               "LocalAuthorizationList")
DEFAULTS = {
    "HeartbeatInterval": "30", "AuthorizationCacheEnabled": "true",
    "LocalAuthListEnabled": "true", "AutoChargeEnabled": "false",
    "AutoChargeIdTagPrefix": "AUTO-", "LocalAuthListMaxLength": "100",
    "StopTxnSampledData": "0", "MeterValuesSampledData": "0",
    "AllowOfflineTxForUnknownId": "false", "AuthorizeRemoteTxRequests": "false",
    "LocalAuthorizeOffline": "false", "LocalPreAuthorize": "false",
    "MeterValueSampleInterval": "60", "StopTransactionOnInvalidId": "true",
    "TransactionMessageAttempts": "3", "TransactionMessageRetryInterval": "10",
    "WebSocketPingInterval": "20",
}
BOOL_KEYS = {"AuthorizationCacheEnabled", "LocalAuthListEnabled", "AutoChargeEnabled",
             "AllowOfflineTxForUnknownId", "AuthorizeRemoteTxRequests", "LocalAuthorizeOffline",
             "LocalPreAuthorize", "StopTransactionOnInvalidId"}
INTEGER_MINIMUMS = {"HeartbeatInterval": 1, "MeterValueSampleInterval": 0,
                    "TransactionMessageAttempts": 0, "TransactionMessageRetryInterval": 0,
                    "WebSocketPingInterval": 0, "LocalAuthListMaxLength": 1}


def _extract(source):
    tree = ast.parse(source, filename=str(SOURCE))
    selected = [copy.deepcopy(node) for node in tree.body
                if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in DEFINITIONS]
    if {node.name for node in selected} != set(DEFINITIONS):
        raise ValueError("CP protocol definitions changed; update the reviewed AST whitelist")
    cp = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "ChargePoint")
    methods = [copy.deepcopy(node) for node in cp.body
               if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in METHODS]
    if {node.name for node in methods} != set(METHODS):
        raise ValueError("CP protocol methods changed; update the reviewed AST whitelist")
    for node in methods:
        node.decorator_list = []
    selected.append(ast.ClassDef(name="CPProtocolMethods", bases=[], keywords=[], body=methods,
                                 decorator_list=[]))
    return ast.fix_missing_locations(ast.Module(body=selected, type_ignores=[]))


def _rooted(directory, name):
    path = Path(name)
    candidate = (path if path.is_absolute() else directory / path).resolve()
    try:
        candidate.relative_to(directory)
    except ValueError as error:
        raise ValueError("Protocol harness write must stay in its state directory") from error
    return candidate


def _normalized(key, raw, fallback, env):
    if key in INTEGER_MINIMUMS:
        text = str(raw).strip()
        if text and all(ch in "0123456789" for ch in text):
            value = int(text)
            if INTEGER_MINIMUMS[key] <= value <= 2147483647:
                return str(value)
        return str(fallback)
    if key in BOOL_KEYS:
        text = str(raw).strip().lower()
        return text if text in {"true", "false"} else str(fallback)
    if key == "AutoChargeIdTagPrefix":
        return env["_normalize_autocharge_prefix"](raw) or str(fallback)
    if key in {"StopTxnSampledData", "MeterValuesSampledData"}:
        return env["_normalize_csv_string"](raw, default="0")
    return str(raw)


def bind_cp_protocol_methods(core, directory, *, defaults=None):
    """Return *core* with actual CP protocol methods and isolated state bound.

    ``defaults`` can explicitly adapt the synthetic meter interval to a test
    run. Values persisted by real ChangeConfiguration requests win on reload.
    The caller owns simulated meter scheduling and all hardware-only behavior.
    """
    directory = Path(directory).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    source = SOURCE.read_text(encoding="utf-8")
    env = {"__name__": "cp_protocol_harness_defs", "ast": ast, "asyncio": asyncio,
           "os": os, "json": json, "logging": logging, "datetime": datetime, "timezone": timezone,
           "call_result": call_result, "AUTOCHARGE_PREFIX_MAX_LEN": 8,
           "AUTOCHARGE_PREFIX_ALLOWED": set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.:"),
           "AUTOCHARGE_ID_TAG_PREFIX": "AUTO-", "STOP_PIN_KEYS": STOP_PIN_KEYS,
           "stop_pin_config": StopPinConfig(directory)}
    exec(compile(_extract(source), str(SOURCE), "exec"), env)
    actual_writer = env["_write_configuration_file"]

    def write_configuration(name, value):
        actual_writer(_rooted(directory, name), value)

    env["_write_configuration_file"] = write_configuration
    effective = dict(DEFAULTS)
    if defaults:
        if set(defaults) - set(DEFAULTS):
            raise ValueError("Unsupported harness default configuration key")
        effective.update({key: str(value) for key, value in defaults.items()})
    for key, fallback in effective.items():
        validated_default = _normalized(key, fallback, DEFAULTS[key], env)
        path = directory / (key + "_cfg.txt")
        try:
            raw = path.read_text(encoding="utf-8").strip()
        except FileNotFoundError:
            raw = validated_default
        normalized = _normalized(key, raw, validated_default, env)
        if not path.exists() or raw != normalized:
            write_configuration(path, normalized)
        setattr(core, key + "_cfg", int(normalized) if key == "LocalAuthListMaxLength" else normalized)
    core.authorization_cache = env["AuthorizationCache"](size=50)
    core.local_auth_list = env["LocalAuthorizationList"](
        max_length=core.LocalAuthListMaxLength_cfg,
        storage_path=str(directory / "local_auth_list.json"),
        version_path=str(directory / "local_auth_list_version.txt"))
    core._v2g_bridge = None
    core._seed_autocharge_events = lambda: None
    methods = env["CPProtocolMethods"]
    for name in METHODS:
        setattr(core, name, MethodType(getattr(methods, name), core))
    core._cp_protocol_harness = {
        "source": str(SOURCE), "source_sha256": hashlib.sha256(source.encode("utf-8")).hexdigest(),
        "bound_methods": list(METHODS), "directory": str(directory),
        "hardware_startup_executed": False, "physical_autocharge_seed": "test-only no-op",
        "stop_pin_config": "actual pure StopPinConfig in isolated directory", "v2g_bridge": None,
    }
    return core
