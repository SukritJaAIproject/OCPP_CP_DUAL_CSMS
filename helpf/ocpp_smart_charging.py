"""Persisted OCPP 1.6 charging profiles, independent of PLC and transport.

Hardware adapters receive independent W and A ceilings.  No voltage is guessed:
when profiles in both units constrain a connector, a CompositeSchedule in one
unit is rejected rather than reporting a fabricated conversion.  Station-wide
maximum profiles use equal shares across active connectors (both when idle).

A Relative ChargePointMaxProfile is supported only when its schedule is one
constant period with no duration.  This covers CSMS constant-cap commands
without inventing a station transaction/start-time anchor: the cap applies
throughout its optional absolute validity window, including idle/restart.
Timed/multi-period Relative station schedules remain an explicit implementation
limitation; Absolute/Recurring station schedules retain their declared clocks.
"""
from __future__ import annotations

import asyncio
import copy
import inspect
import json
import math
import os
from datetime import datetime, timezone
from decimal import Decimal, ROUND_FLOOR
from pathlib import Path


class SmartChargingStateError(RuntimeError):
    """A persisted profile store cannot be trusted; do not silently unlimit."""


def _epoch(value):
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("A timezone is required")
        return value.timestamp()
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        result = float(value)
    elif isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError("A timezone is required")
        result = parsed.timestamp()
    else:
        raise ValueError("Invalid date/time")
    if not math.isfinite(result):
        raise ValueError("Invalid date/time")
    return result


def _iso(value):
    return datetime.fromtimestamp(value, timezone.utc).isoformat().replace("+00:00", "Z")


def _integer(value, minimum=0, maximum=2 ** 31 - 1):
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError("Invalid integer")
    return value


def _number(value, minimum=0, maximum=1e9):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("Invalid number")
    result = float(value)
    if not math.isfinite(result) or not minimum <= result <= maximum:
        raise ValueError("Invalid number")
    return result


def _rate(value):
    result = _number(value)
    scaled = Decimal(str(value)) * 10
    if scaled != scaled.to_integral_value():
        raise ValueError("Charging rates support at most one decimal place")
    return result


def _floor_rate(value):
    return float(Decimal(str(value)).quantize(Decimal("0.1"), rounding=ROUND_FLOOR))


def _snake(value):
    if isinstance(value, list):
        return [_snake(item) for item in value]
    if not isinstance(value, dict):
        return value
    result = {}
    for key, item in value.items():
        converted = "".join(("_" + letter.lower()) if letter.isupper() else letter for letter in key)
        if converted in result:
            raise ValueError("Duplicate profile field")
        result[converted] = _snake(item)
    return result


class SmartChargingController:
    MAX_PROFILES = 128
    MAX_PERIODS = 256
    MAX_COMPOSITE_BOUNDARIES = 512
    MAX_COMPOSITE_DURATION = 7 * 86400

    def __init__(self, state_path, apply_limits, *, clock=None,
                 active_transaction=None, transaction_started_at=None,
                 rated_power_w=180000, rated_current_a=500,
                 station_rated_power_w=None, station_rated_current_a=None,
                 fault_callback=None):
        self.path = Path(state_path)
        self.pending_path = self.path.with_name(self.path.name + ".pending")
        self.apply_limits = apply_limits
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.active_transaction = active_transaction or (lambda connector: 0)
        self.transaction_started_at = transaction_started_at
        self.fault_callback = fault_callback
        self.rated = {"W": _number(rated_power_w, minimum=.001),
                      "A": _number(rated_current_a, minimum=.001)}
        self.station_rated = {"W": self.rated["W"] if station_rated_power_w is None else
                              _number(station_rated_power_w, minimum=.001)}
        if station_rated_current_a is not None:
            self.station_rated["A"] = _number(station_rated_current_a, minimum=.001)
        self._profiles = []
        self._anchors = {}
        self._last_limits = {}
        self._lock = asyncio.Lock()
        self.last_error = None
        self.inhibited = False
        if self.pending_path.exists():
            raise SmartChargingStateError("Previous profile update outcome requires inspection")
        if self.path.exists():
            try:
                stored = json.loads(self.path.read_text(encoding="utf-8"))
                if stored.get("version") != 1 or not isinstance(stored.get("profiles"), list):
                    raise ValueError("Unsupported store")
                if len(stored["profiles"]) > self.MAX_PROFILES:
                    raise ValueError("Profile capacity exceeded")
                for entry in stored["profiles"]:
                    row = self._validate(entry["connector_id"], entry["profile"], loading=True)
                    row["installed_at"] = _epoch(entry["installed_at"])
                    row["bound_transaction_id"] = entry.get("bound_transaction_id")
                    row["excluded_connectors"] = entry.get("excluded_connectors", [])
                    if (not isinstance(row["excluded_connectors"], list) or
                            any(type(cid) is not int or cid not in (1, 2) for cid in row["excluded_connectors"]) or
                            len(set(row["excluded_connectors"])) != len(row["excluded_connectors"])):
                        raise ValueError("Invalid connector replacement state")
                    if row["excluded_connectors"] and (row["connector_id"] != 0 or row["profile"]["charging_profile_purpose"] != "TxDefaultProfile"):
                        raise ValueError("Invalid replaced station default")
                    if row["bound_transaction_id"] is not None:
                        _integer(row["bound_transaction_id"], minimum=1)
                    if row["profile"]["charging_profile_purpose"] == "TxProfile" and row["bound_transaction_id"] is None:
                        raise ValueError("Missing transaction binding")
                    if any(old["profile"]["charging_profile_id"] == row["profile"]["charging_profile_id"] for old in self._profiles):
                        raise ValueError("Duplicate profile ID")
                    self._profiles.append(row)
                for cid, value in stored.get("anchors", {}).items():
                    if cid not in ("1", "2"):
                        raise ValueError("Invalid anchor")
                    self._anchors[cid] = {"transaction_id": _integer(value["transaction_id"], minimum=1),
                                          "started_at": _epoch(value["started_at"])}
            except Exception as error:
                raise SmartChargingStateError("Invalid persisted smart charging state") from error

    @property
    def profiles(self):
        return copy.deepcopy(self._profiles)

    def _now(self):
        return _epoch(self.clock())

    @staticmethod
    def _connector(value):
        return _integer(value, maximum=2)

    def _validate(self, connector_id, raw, loading=False):
        connector_id = self._connector(connector_id)
        profile = _snake(copy.deepcopy(raw))
        if not isinstance(profile, dict):
            raise ValueError("Invalid profile")
        allowed = {"charging_profile_id", "transaction_id", "stack_level", "charging_profile_purpose",
                   "charging_profile_kind", "recurrency_kind", "valid_from", "valid_to", "charging_schedule"}
        if set(profile) - allowed:
            raise ValueError("Unsupported profile field")
        _integer(profile["charging_profile_id"])
        _integer(profile["stack_level"], maximum=100)
        purpose = profile["charging_profile_purpose"]
        kind = profile["charging_profile_kind"]
        if purpose not in ("ChargePointMaxProfile", "TxDefaultProfile", "TxProfile"):
            raise ValueError("Unsupported purpose")
        if kind not in ("Absolute", "Relative", "Recurring"):
            raise ValueError("Unsupported profile kind")
        if purpose == "ChargePointMaxProfile" and connector_id != 0:
            raise ValueError("Station maximum requires connector zero")
        if purpose == "TxProfile" and connector_id == 0:
            raise ValueError("Transaction profile requires a connector")
        transaction_id = profile.get("transaction_id")
        if transaction_id is not None:
            _integer(transaction_id, minimum=1)
            if purpose != "TxProfile":
                raise ValueError("Unexpected transaction ID")
        bound = None
        if purpose == "TxProfile" and not loading:
            current = self.active_transaction(connector_id)
            if type(current) is not int or current <= 0 or (transaction_id is not None and current != transaction_id):
                raise ValueError("No matching active transaction")
            bound = current
        if kind == "Relative" and not loading and connector_id:
            current = self.active_transaction(connector_id)
            if current and self._transaction_anchor(connector_id, current) is None:
                raise ValueError("Relative schedule needs the transaction's actual start time")
        if kind == "Recurring":
            if profile.get("recurrency_kind") not in ("Daily", "Weekly"):
                raise ValueError("Missing recurrence")
        elif profile.get("recurrency_kind") is not None:
            raise ValueError("Unexpected recurrence")
        start = _epoch(profile["valid_from"]) if profile.get("valid_from") else None
        end = _epoch(profile["valid_to"]) if profile.get("valid_to") else None
        if start is not None and end is not None and end <= start:
            raise ValueError("Invalid validity window")
        if not loading and end is not None and end <= self._now():
            raise ValueError("Expired profile")
        schedule = profile["charging_schedule"]
        if not isinstance(schedule, dict) or set(schedule) - {"duration", "start_schedule", "charging_rate_unit", "charging_schedule_period", "min_charging_rate"}:
            raise ValueError("Invalid schedule")
        unit = schedule["charging_rate_unit"]
        if unit not in self.rated:
            raise ValueError("Unsupported unit")
        if kind in ("Absolute", "Recurring"):
            _epoch(schedule["start_schedule"])
        elif schedule.get("start_schedule") is not None:
            raise ValueError("Relative profile cannot specify startSchedule")
        duration = schedule.get("duration")
        if duration is not None:
            _integer(duration, minimum=1, maximum=366 * 86400)
            if kind == "Recurring" and duration > (86400 if profile["recurrency_kind"] == "Daily" else 7 * 86400):
                raise ValueError("Duration exceeds recurrence")
        if schedule.get("min_charging_rate") is not None:
            # OCPP 1.6 describes this as an optional balancing-efficiency hint,
            # not a mandatory minimum that can override a stricter safety cap.
            _rate(schedule["min_charging_rate"])
        periods = schedule["charging_schedule_period"]
        if not isinstance(periods, list) or not 1 <= len(periods) <= self.MAX_PERIODS:
            raise ValueError("Invalid periods")
        last = -1
        for period in periods:
            if not isinstance(period, dict) or set(period) - {"start_period", "limit", "number_phases"}:
                raise ValueError("Invalid period")
            point = _integer(period["start_period"], maximum=366 * 86400)
            if point <= last or (duration is not None and point >= duration):
                raise ValueError("Unordered periods")
            _rate(period["limit"])
            if period.get("number_phases") is not None:
                _integer(period["number_phases"], minimum=1, maximum=3)
            last = point
        if periods[0]["start_period"] != 0:
            raise ValueError("Schedule must begin at zero")
        if (self._constant_relative_station_max(profile) and
                (duration is not None or len(periods) != 1)):
            # OCPP permits a situation-specific Relative origin determined by
            # the CP. We do not guess a shared origin from either head's start.
            raise ValueError("Relative station maximum supports only a constant, indefinite schedule")
        return {"connector_id": connector_id, "profile": profile,
                "installed_at": self._now(), "bound_transaction_id": bound,
                "excluded_connectors": []}

    @staticmethod
    def _write_atomic(path, data):
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(path.name + ".tmp")
        try:
            with temporary.open("w", encoding="utf-8") as stream:
                json.dump(data, stream, separators=(",", ":"), allow_nan=False)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
        finally:
            if temporary.exists():
                temporary.unlink()

    def _save(self, profiles=None, anchors=None):
        data = {"version": 1, "profiles": self._profiles if profiles is None else profiles,
                "anchors": self._anchors if anchors is None else anchors}
        self._write_atomic(self.path, data)

    async def _notify_fault(self, cid, reason):
        if self.fault_callback is not None:
            result = self.fault_callback(cid, reason)
            if inspect.isawaitable(result):
                await result

    def _transaction_anchor(self, cid, current):
        if type(current) is not int or current <= 0:
            return None
        if self.transaction_started_at is not None:
            value = self.transaction_started_at(cid)
            if value is not None:
                return _epoch(value)
        cached = self._anchors.get(str(cid))
        return cached["started_at"] if cached is not None and cached["transaction_id"] == current else None

    async def record_transaction_start(self, connector_id, transaction_id, started_at):
        """Alternative to a start-time callback: record the actual start event.

        No first-observation timestamp is guessed.  Callers without a persisted
        start-time callback should record this before enabling energy delivery.
        """
        async with self._lock:
            try:
                cid = self._connector(connector_id)
                if cid == 0 or self.inhibited:
                    raise ValueError("Invalid transaction connector")
                _integer(transaction_id, minimum=1)
                stamp = _epoch(started_at)
                if stamp > self._now():
                    raise ValueError("Transaction cannot have started in the future")
                candidate = copy.deepcopy(self._anchors)
                candidate[str(cid)] = {"transaction_id": transaction_id, "started_at": stamp}
                self._save(anchors=candidate)
                self._anchors = candidate
                return True
            except Exception as error:
                self.last_error = type(error).__name__
                return False

    def _anchor(self, row, cid):
        profile = row["profile"]
        if profile["charging_profile_kind"] == "Relative":
            return self._transaction_anchor(cid, self.active_transaction(cid))
        return _epoch(profile["charging_schedule"]["start_schedule"])

    @staticmethod
    def _constant_relative_station_max(profile):
        # _validate enforces one period at zero and no duration for this form.
        # Its value has no schedule-time transition, so no origin is needed.
        return (profile["charging_profile_purpose"] == "ChargePointMaxProfile" and
                profile["charging_profile_kind"] == "Relative")

    def _value(self, row, cid, instant):
        profile = row["profile"]
        if row["connector_id"] not in (0, cid):
            return None
        if cid in row.get("excluded_connectors", []):
            return None
        if profile["charging_profile_purpose"] == "TxProfile" and self.active_transaction(cid) != row["bound_transaction_id"]:
            return None
        if profile.get("valid_from") and instant < _epoch(profile["valid_from"]):
            return None
        if profile.get("valid_to") and instant >= _epoch(profile["valid_to"]):
            return None
        schedule = profile["charging_schedule"]
        if self._constant_relative_station_max(profile):
            elapsed = 0
        else:
            anchor = self._anchor(row, cid)
            if anchor is None and profile["charging_profile_kind"] == "Relative" and self.active_transaction(cid):
                raise SmartChargingStateError("Actual transaction start time is unavailable")
            if anchor is None or instant < anchor:
                return None
            elapsed = instant - anchor
        cycle = None
        if profile["charging_profile_kind"] == "Recurring":
            cycle = 86400 if profile["recurrency_kind"] == "Daily" else 7 * 86400
            elapsed %= cycle
        if schedule.get("duration") is not None and elapsed >= schedule["duration"]:
            return None
        period = schedule["charging_schedule_period"][0]
        for candidate in schedule["charging_schedule_period"]:
            if candidate["start_period"] <= elapsed:
                period = candidate
            else:
                break
        unit = schedule["charging_rate_unit"]
        cap = self.station_rated.get(unit, self.rated[unit] * 2) if profile["charging_profile_purpose"] == "ChargePointMaxProfile" else self.rated[unit]
        return unit, min(_number(period["limit"]), cap), period.get("number_phases")

    def _selected(self, purpose, cid, instant):
        candidates = [(row, self._value(row, cid, instant)) for row in self._profiles
                      if row["profile"]["charging_profile_purpose"] == purpose]
        candidates = [(row, value) for row, value in candidates if value is not None]
        if not candidates:
            return None
        # One profile per purpose/stack/connector; an explicit connector wins a
        # tie against a station default. Higher active stack levels take priority.
        return max(candidates, key=lambda pair: (pair[0]["profile"]["stack_level"],
                                                 pair[0]["connector_id"] == cid,
                                                 pair[0]["installed_at"]))[1]

    def effective_limits(self, connector_id, instant=None):
        cid = self._connector(connector_id)
        if cid == 0:
            raise ValueError("Hardware limits require connector 1 or 2")
        instant = self._now() if instant is None else _epoch(instant)
        limits = dict(self.rated)
        constrained = set()
        phases = {}
        active = [target for target in (1, 2) if self.active_transaction(target)]
        participants = active or [1, 2]
        def share(value):
            return value / len(participants) if cid in participants else 0.0
        for unit, rating in self.station_rated.items():
            limits[unit] = min(limits[unit], share(rating))
        station = self._selected("ChargePointMaxProfile", cid, instant)
        if station:
            unit, value, number_phases = station
            limits[unit] = min(limits[unit], share(value))
            constrained.add(unit)
            phases[unit] = number_phases
        transaction = self._selected("TxProfile", cid, instant)
        selected = transaction or self._selected("TxDefaultProfile", cid, instant)
        if selected:
            unit, value, number_phases = selected
            limits[unit] = min(limits[unit], value)
            constrained.add(unit)
            phases[unit] = number_phases
        return {unit: _floor_rate(value) for unit, value in limits.items()}, constrained, phases

    async def _write_hardware(self, cid, limits):
        try:
            value = self.apply_limits(cid, dict(limits))
            if inspect.isawaitable(value):
                value = await value
            if value is False or (isinstance(value, str) and "fail" in value.casefold()):
                raise RuntimeError("Hardware adapter rejected limits")
            self._last_limits[cid] = dict(limits)
        except Exception as error:
            self.last_error = type(error).__name__
            # A timeout may mean the PLC applied the value but its reply was
            # lost. Never use the old cache as proof of its physical limit.
            self._last_limits.clear()
            self.inhibited = True
            await self._notify_fault(cid, self.last_error)
            raise

    async def _apply(self):
        try:
            targets = {cid: self.effective_limits(cid)[0] for cid in (1, 2)}
        except Exception as error:
            self.last_error = type(error).__name__
            self.inhibited = True
            for cid in (1, 2):
                await self._notify_fault(cid, self.last_error)
            raise
        # Establish a known zero limit before raising an unknown adapter state.
        # Production adapters must implement OCPP zero as zero, never unlimited.
        for cid in (1, 2):
            if cid not in self._last_limits:
                await self._write_hardware(cid, {"W": 0.0, "A": 0.0})
        # Lower all relevant caps before raising any cap on another connector.
        # This preserves the station budget even when the sole active connector
        # changes or a second transaction starts while the other receives full W.
        for cid in (1, 2):
            lowered = {unit: min(value, targets[cid][unit]) for unit, value in self._last_limits[cid].items()}
            if lowered != self._last_limits[cid]:
                await self._write_hardware(cid, lowered)
        for cid in (1, 2):
            if targets[cid] != self._last_limits[cid]:
                await self._write_hardware(cid, targets[cid])

    async def _commit(self, candidate):
        """Do not acknowledge an update until persistence and PLC calls finish.

        A pending journal survives a process crash or unsuccessful rollback.
        Startup then fails closed instead of reporting an unknown hardware state
        as an empty/full-rating profile store.
        """
        if self.inhibited:
            return False
        previous = self._profiles
        previous_anchors = copy.deepcopy(self._anchors)
        try:
            self._write_atomic(self.pending_path, {"version": 1, "previous": previous,
                                                   "candidate": candidate})
        except Exception as error:
            self.last_error = type(error).__name__
            return False
        try:
            self._save(candidate)
            self._profiles = candidate
            await self._apply()
            # Applying a Relative profile may establish a transaction anchor.
            self._save()
            self.pending_path.unlink()
            self.last_error = None
            return True
        except Exception as error:
            original_error = type(error).__name__
            self._profiles = previous
            self._anchors = previous_anchors
            self._last_limits.clear()
            try:
                self._save()
                await self._apply()
                self.pending_path.unlink()
            except Exception:
                self.inhibited = True
                for cid in (1, 2):
                    try:
                        await self._notify_fault(cid, "ProfileRollbackFailed")
                    except Exception:
                        pass
            self.last_error = original_error
            return False

    async def import_legacy_profiles(self, profiles):
        """Migrate one legacy snapshot as a single hardware/persistence commit.

        This is a local upgrade operation, never an OCPP command. Applying the
        records one at a time could transiently lift an old cap on another head.
        """
        async with self._lock:
            if self.inhibited or self._profiles or self.path.exists():
                return False
            try:
                candidate = [self._validate(cid, profile) for cid, profile in profiles]
                ids = [row["profile"]["charging_profile_id"] for row in candidate]
                if len(candidate) > self.MAX_PROFILES or len(ids) != len(set(ids)):
                    raise ValueError("Invalid legacy profile snapshot")
            except Exception as error:
                self.last_error = type(error).__name__
                return False
            return await self._commit(candidate)

    async def set_profile(self, connector_id, profile):
        async with self._lock:
            if self.inhibited:
                return "Rejected"
            try:
                row = self._validate(connector_id, profile)
                fresh = row["profile"]
                candidate = []
                for old in self._profiles:
                    old_profile = old["profile"]
                    if old_profile["charging_profile_id"] == fresh["charging_profile_id"]:
                        continue
                    same_slot = (old_profile["charging_profile_purpose"] == fresh["charging_profile_purpose"] and
                                 old_profile["stack_level"] == fresh["stack_level"])
                    if same_slot:
                        if connector_id == 0 or old["connector_id"] == connector_id:
                            continue
                        if old["connector_id"] == 0 and fresh["charging_profile_purpose"] == "TxDefaultProfile":
                            old = copy.deepcopy(old)
                            old["excluded_connectors"] = sorted(set(old.get("excluded_connectors", [])) | {connector_id})
                            if len(old["excluded_connectors"]) == 2:
                                continue
                    candidate.append(old)
                candidate.append(row)
                if len(candidate) > self.MAX_PROFILES:
                    raise ValueError("Profile capacity exceeded")
            except Exception as error:
                self.last_error = type(error).__name__
                return "Rejected"
            return "Accepted" if await self._commit(candidate) else "Rejected"

    async def clear_profiles(self, *, profile_id=None, connector_id=None,
                             charging_profile_purpose=None, stack_level=None):
        async with self._lock:
            if self.inhibited:
                return "Unknown"
            try:
                if profile_id is not None:
                    _integer(profile_id)
                if connector_id is not None:
                    self._connector(connector_id)
                if stack_level is not None:
                    _integer(stack_level, maximum=100)
                if charging_profile_purpose is not None and charging_profile_purpose not in ("ChargePointMaxProfile", "TxDefaultProfile", "TxProfile"):
                    raise ValueError("Invalid purpose")
                def matches(row):
                    profile = row["profile"]
                    # Per OCPP, an ID identifies the profile; remaining filters
                    # are used only when no profile ID is supplied.
                    if profile_id is not None:
                        return profile["charging_profile_id"] == profile_id
                    return ((connector_id is None or row["connector_id"] == connector_id) and
                            (charging_profile_purpose is None or profile["charging_profile_purpose"] == charging_profile_purpose) and
                            (stack_level is None or profile["stack_level"] == stack_level))
                candidate = [row for row in self._profiles if not matches(row)]
                if len(candidate) == len(self._profiles):
                    return "Unknown"
            except Exception as error:
                self.last_error = type(error).__name__
                return "Unknown"
            return "Accepted" if await self._commit(candidate) else "Unknown"

    def _boundaries(self, cid, begin, end):
        points = {begin, end}
        for row in self._profiles:
            if row["connector_id"] not in (0, cid):
                continue
            profile = row["profile"]
            for field in ("valid_from", "valid_to"):
                if profile.get(field):
                    point = _epoch(profile[field])
                    if begin < point < end:
                        points.add(point)
            if self._constant_relative_station_max(profile):
                continue
            anchor = self._anchor(row, cid)
            if anchor is None:
                continue
            schedule = profile["charging_schedule"]
            cycle = (86400 if profile.get("recurrency_kind") == "Daily" else 7 * 86400) if profile["charging_profile_kind"] == "Recurring" else None
            anchors = [anchor]
            if cycle:
                first = max(0, math.floor((begin - anchor) / cycle))
                anchors = [anchor + index * cycle for index in range(first, max(first + 1, math.ceil((end - anchor) / cycle)) + 1)]
            offsets = [period["start_period"] for period in schedule["charging_schedule_period"]]
            if schedule.get("duration") is not None:
                offsets.append(schedule["duration"])
            if cycle:
                offsets.append(cycle)
            for origin in anchors:
                for offset in offsets:
                    point = origin + offset
                    if begin < point < end:
                        points.add(point)
            if len(points) > self.MAX_COMPOSITE_BOUNDARIES:
                raise ValueError("Composite calculation capacity exceeded")
        return points

    async def composite_schedule(self, connector_id, duration, charging_rate_unit=None):
        async with self._lock:
            try:
                if self.inhibited:
                    raise SmartChargingStateError("Controller inhibited")
                cid = self._connector(connector_id)
                _integer(duration, minimum=1, maximum=self.MAX_COMPOSITE_DURATION)
                if charging_rate_unit is not None and charging_rate_unit not in self.rated:
                    raise ValueError("Unsupported requested unit")
                begin = self._now()
                end = begin + duration
                targets = (1, 2) if cid == 0 else (cid,)
                boundaries = sorted(set().union(*(self._boundaries(target, begin, end) for target in targets)))
                if len(boundaries) > self.MAX_COMPOSITE_BOUNDARIES:
                    raise ValueError("Composite calculation capacity exceeded")
                samples = []
                units = set()
                for instant in boundaries[:-1]:
                    states = [self.effective_limits(target, instant) for target in targets]
                    units.update(set().union(*(state[1] for state in states)))
                    samples.append((instant, states))
                unit = charging_rate_unit or (next(iter(units)) if len(units) == 1 else "W")
                if units - {unit}:
                    raise ValueError("Conversion requires physical voltage/phase information")
                if cid == 0 and unit == "A" and "A" not in self.station_rated and "A" not in units:
                    raise ValueError("No station-wide current rating is configured")
                periods = []
                for instant, states in samples:
                    limit = float(sum((Decimal(str(state[0][unit])) for state in states), Decimal(0)))
                    if cid == 0 and unit in self.station_rated:
                        limit = min(limit, self.station_rated[unit])
                    limit = _floor_rate(limit)
                    entry = {"startPeriod": int(math.ceil(instant - begin - 1e-7)), "limit": limit}
                    phase_values = {state[2].get(unit) for state in states}
                    if len(phase_values) == 1 and next(iter(phase_values)) is not None:
                        entry["numberPhases"] = next(iter(phase_values))
                    if periods and {key: value for key, value in entry.items() if key != "startPeriod"} == {key: value for key, value in periods[-1].items() if key != "startPeriod"}:
                        continue
                    if entry["startPeriod"] < duration:
                        periods.append(entry)
                if len(periods) > self.MAX_PERIODS:
                    raise ValueError("Composite schedule too large")
                return {"status": "Accepted", "connector_id": cid, "schedule_start": _iso(begin),
                        "charging_schedule": {"duration": duration, "chargingRateUnit": unit,
                                              "chargingSchedulePeriod": periods}}
            except Exception as error:
                self.last_error = type(error).__name__
                return {"status": "Rejected"}

    async def tick(self):
        async with self._lock:
            if self.inhibited:
                return False
            candidate = [row for row in self._profiles if row["profile"]["charging_profile_purpose"] != "TxProfile" or
                         self.active_transaction(row["connector_id"]) == row["bound_transaction_id"]]
            if len(candidate) != len(self._profiles):
                return await self._commit(candidate)
            try:
                anchors = copy.deepcopy(self._anchors)
                await self._apply()
                if anchors != self._anchors:
                    self._save()
                return True
            except Exception as error:
                self.last_error = type(error).__name__
                return False

    async def run(self, *, interval=1.0):
        if interval <= 0:
            raise ValueError("Invalid polling interval")
        while True:
            await self.tick()
            await asyncio.sleep(interval)
