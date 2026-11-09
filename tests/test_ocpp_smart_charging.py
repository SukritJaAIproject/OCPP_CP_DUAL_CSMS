"""Smart charging contracts using a recording adapter; never imports CP.py."""
import asyncio
import copy
import json
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from helpf.ocpp_smart_charging import SmartChargingController, SmartChargingStateError


START = 1800000000.0


def iso(value):
    return datetime.fromtimestamp(value, timezone.utc).isoformat().replace("+00:00", "Z")


def profile(limit=10000, unit="W", *, pid=1, stack=0, purpose="TxDefaultProfile",
            kind="Absolute", start=START, duration=3600, periods=None, **extra):
    schedule = {"charging_rate_unit": unit,
                "charging_schedule_period": periods or [{"start_period": 0, "limit": limit}]}
    if duration is not None:
        schedule["duration"] = duration
    if kind != "Relative":
        schedule["start_schedule"] = iso(start)
    return {"charging_profile_id": pid, "stack_level": stack,
            "charging_profile_purpose": purpose, "charging_profile_kind": kind,
            "charging_schedule": schedule, **extra}


def csms_constant_station_max(limit=100000, unit="W"):
    """The actual CSMS quick CP Max command, with its original camelCase keys."""
    return {"chargingProfileId": 1, "stackLevel": 0,
            "chargingProfilePurpose": "ChargePointMaxProfile", "chargingProfileKind": "Relative",
            "chargingSchedule": {"chargingRateUnit": unit,
                "chargingSchedulePeriod": [{"startPeriod": 0, "limit": limit}]}}


@pytest.fixture
def harness(tmp_path):
    clock = [START]
    active = {1: 0, 2: 0}
    began = {1: START, 2: START}
    writes = []
    faults = []
    physical = {1: {"W": 0., "A": 0.}, 2: {"W": 0., "A": 0.}}

    async def apply(cid, limits):
        physical[cid] = dict(limits)
        writes.append((cid, dict(limits), copy.deepcopy(physical)))

    controller = SmartChargingController(tmp_path / "profiles.json", apply, clock=lambda: clock[0],
        active_transaction=active.get, transaction_started_at=began.get,
        fault_callback=lambda cid, reason: faults.append((cid, reason)))
    return SimpleNamespace(controller=controller, clock=clock, active=active, began=began,
                           writes=writes, faults=faults, physical=physical, path=controller.path)


@pytest.mark.parametrize("cid", [3, -1, True, False, None, 1.0, "1"])
async def test_invalid_connector_rejected_without_persistence_or_adapter_effect(harness, cid):
    assert await harness.controller.set_profile(cid, profile()) == "Rejected"
    assert not harness.path.exists()
    assert harness.writes == []


@pytest.mark.parametrize("mutate", [
    lambda p: p.update(stack_level=True),
    lambda p: p.update(stack_level=101),
    lambda p: p.update(charging_profile_id=-1),
    lambda p: p["charging_schedule"].update(charging_rate_unit="VA"),
    lambda p: p["charging_schedule"].update(duration=0),
    lambda p: p["charging_schedule"].update(charging_schedule_period=[]),
    lambda p: p["charging_schedule"]["charging_schedule_period"][0].update(start_period=1),
    lambda p: p["charging_schedule"]["charging_schedule_period"][0].update(limit=float("nan")),
    lambda p: p["charging_schedule"]["charging_schedule_period"][0].update(limit=-1),
    lambda p: p["charging_schedule"]["charging_schedule_period"][0].update(limit=50.25),
    lambda p: p["charging_schedule"]["charging_schedule_period"][0].update(number_phases=4),
    lambda p: p.update(valid_to=iso(START - 1)),
    lambda p: p.update(valid_from=iso(START + 20), valid_to=iso(START + 10)),
])
async def test_bad_profile_does_not_replace_existing_profile(harness, mutate):
    engine = harness.controller
    assert await engine.set_profile(1, profile()) == "Accepted"
    prior = harness.path.read_bytes()
    count = len(harness.writes)
    proposed = profile(20000)
    mutate(proposed)
    assert await engine.set_profile(1, proposed) == "Rejected"
    assert harness.path.read_bytes() == prior
    assert len(harness.writes) == count
    assert engine.profiles[0]["profile"]["charging_schedule"]["charging_schedule_period"][0]["limit"] == 10000


async def test_50_amp_limit_and_requested_duration_are_reported(harness):
    engine = harness.controller
    assert await engine.set_profile(1, profile(50, "A")) == "Accepted"
    result = await engine.composite_schedule(1, 60, "A")
    assert result["status"] == "Accepted"
    assert result["charging_schedule"]["duration"] == 60
    assert result["charging_schedule"]["chargingRateUnit"] == "A"
    assert result["charging_schedule"]["chargingSchedulePeriod"] == [{"startPeriod": 0, "limit": 50}]
    assert harness.physical[1]["A"] == 50


async def test_clear_without_filters_clears_all_units_and_both_heads(harness):
    engine = harness.controller
    assert await engine.set_profile(1, profile(25000)) == "Accepted"
    assert await engine.set_profile(2, profile(40, "A", pid=2)) == "Accepted"
    assert await engine.clear_profiles() == "Accepted"
    assert engine.profiles == []
    assert json.loads(harness.path.read_text())["profiles"] == []
    assert harness.physical == {1: {"W": 90000., "A": 500.}, 2: {"W": 90000., "A": 500.}}
    assert not engine.pending_path.exists()
    assert await engine.clear_profiles() == "Unknown"


async def test_clear_filters_and_id_replacement_do_not_touch_other_head(harness):
    engine = harness.controller
    await engine.set_profile(1, profile(10000, pid=1, stack=0))
    await engine.set_profile(2, profile(20000, pid=2, stack=0))
    await engine.set_profile(1, profile(5000, pid=3, stack=1))
    assert await engine.clear_profiles(connector_id=1, stack_level=1, charging_profile_purpose="TxDefaultProfile") == "Accepted"
    assert {row["profile"]["charging_profile_id"] for row in engine.profiles} == {1, 2}
    assert harness.physical[1]["W"] == 10000
    assert harness.physical[2]["W"] == 20000
    await engine.set_profile(2, profile(30000, pid=1))
    assert len(engine.profiles) == 1
    assert engine.profiles[0]["connector_id"] == 2
    assert harness.physical[1]["W"] == 90000
    assert await engine.clear_profiles(profile_id=1, connector_id=1) == "Accepted"
    assert engine.profiles == []


async def test_absolute_schedule_progresses_and_composite_has_all_boundaries(harness):
    engine = harness.controller
    periods = [{"start_period": 0, "limit": 10000}, {"start_period": 10, "limit": 20000}, {"start_period": 20, "limit": 5000}]
    assert await engine.set_profile(1, profile(duration=30, periods=periods)) == "Accepted"
    result = await engine.composite_schedule(1, 40, "W")
    assert result["charging_schedule"]["chargingSchedulePeriod"] == [
        {"startPeriod": 0, "limit": 10000}, {"startPeriod": 10, "limit": 20000},
        {"startPeriod": 20, "limit": 5000}, {"startPeriod": 30, "limit": 90000}]
    harness.clock[0] += 15
    assert await engine.tick()
    assert harness.physical[1]["W"] == 20000
    result = await engine.composite_schedule(1, 20, "W")
    assert result["charging_schedule"]["chargingSchedulePeriod"] == [
        {"startPeriod": 0, "limit": 20000}, {"startPeriod": 5, "limit": 5000}, {"startPeriod": 15, "limit": 90000}]


async def test_future_start_and_validity_window_do_not_apply_early(harness):
    engine = harness.controller
    proposed = profile(12000, start=START + 10, duration=30, valid_from=iso(START + 15), valid_to=iso(START + 25))
    assert await engine.set_profile(1, proposed) == "Accepted"
    assert harness.physical[1]["W"] == 90000
    result = await engine.composite_schedule(1, 35, "W")
    assert result["charging_schedule"]["chargingSchedulePeriod"] == [
        {"startPeriod": 0, "limit": 90000}, {"startPeriod": 15, "limit": 12000}, {"startPeriod": 25, "limit": 90000}]


async def test_higher_active_stack_falls_back_when_its_duration_ends(harness):
    engine = harness.controller
    await engine.set_profile(1, profile(20000, pid=1, duration=30))
    await engine.set_profile(1, profile(5000, pid=2, stack=1, duration=10))
    result = await engine.composite_schedule(1, 40, "W")
    assert result["charging_schedule"]["chargingSchedulePeriod"] == [
        {"startPeriod": 0, "limit": 5000}, {"startPeriod": 10, "limit": 20000}, {"startPeriod": 30, "limit": 90000}]


async def test_transaction_profile_overrides_default_and_is_removed_at_end(harness):
    engine = harness.controller
    harness.active[1] = 42
    await engine.set_profile(1, profile(20000, pid=1))
    assert await engine.set_profile(1, profile(50, "A", pid=2, purpose="TxProfile", transaction_id=42)) == "Accepted"
    result = await engine.composite_schedule(1, 60, "A")
    assert result["charging_schedule"]["chargingSchedulePeriod"][0]["limit"] == 50
    harness.active[1] = 0
    assert await engine.tick()
    assert all(row["profile"]["charging_profile_purpose"] != "TxProfile" for row in engine.profiles)
    assert harness.physical[1]["W"] == 20000
    assert harness.physical[1]["A"] == 500


@pytest.mark.parametrize("cid,transaction", [(0, 42), (1, 99), (1, None)])
async def test_transaction_profile_requires_matching_active_transaction(harness, cid, transaction):
    proposed = profile(purpose="TxProfile")
    if transaction is not None:
        proposed["transaction_id"] = transaction
    assert await harness.controller.set_profile(cid, proposed) == "Rejected"
    assert harness.writes == []


async def test_relative_profile_uses_transaction_start_and_persists_binding(harness):
    engine = harness.controller
    harness.active[1] = 101
    harness.began[1] = START - 15
    periods = [{"start_period": 0, "limit": 10000}, {"start_period": 10, "limit": 20000}]
    proposed = profile(kind="Relative", purpose="TxProfile", transaction_id=101, periods=periods, duration=30)
    assert await engine.set_profile(1, proposed) == "Accepted"
    assert harness.physical[1]["W"] == 20000
    reloaded = SmartChargingController(harness.path, AsyncMock(), clock=lambda: harness.clock[0],
        active_transaction=harness.active.get, transaction_started_at=harness.began.get)
    result = await reloaded.composite_schedule(1, 20, "W")
    assert result["charging_schedule"]["chargingSchedulePeriod"] == [
        {"startPeriod": 0, "limit": 20000}, {"startPeriod": 15, "limit": 180000}]


async def test_relative_profile_never_guesses_start_of_existing_transaction(tmp_path):
    adapter = AsyncMock()
    engine = SmartChargingController(tmp_path / "profiles.json", adapter,
        clock=lambda: START, active_transaction=lambda cid: 42 if cid == 1 else 0)
    assert await engine.set_profile(1, profile(kind="Relative", purpose="TxProfile", transaction_id=42)) == "Rejected"
    adapter.assert_not_called()
    assert not engine.path.exists()
    assert await engine.record_transaction_start(1, 42, START - 15)
    proposed = profile(kind="Relative", purpose="TxProfile", transaction_id=42,
        periods=[{"start_period": 0, "limit": 10000}, {"start_period": 10, "limit": 20000}])
    assert await engine.set_profile(1, proposed) == "Accepted"
    assert engine.effective_limits(1)[0]["W"] == 20000
    reload = SmartChargingController(engine.path, AsyncMock(), clock=lambda: START,
        active_transaction=lambda cid: 42 if cid == 1 else 0)
    assert reload.effective_limits(1)[0]["W"] == 20000


async def test_future_relative_default_inhibits_if_start_event_is_missing(tmp_path):
    active = {1: 0, 2: 0}
    faults = []
    engine = SmartChargingController(tmp_path / "profiles.json", AsyncMock(), clock=lambda: START,
        active_transaction=active.get, fault_callback=lambda *args: faults.append(args))
    assert await engine.set_profile(1, profile(kind="Relative")) == "Accepted"
    active[1] = 42
    assert not await engine.tick()
    assert engine.inhibited
    assert faults


@pytest.mark.parametrize("recurrence,cycle", [("Daily", 86400), ("Weekly", 604800)])
async def test_recurrence_repeats_after_schedule_gap(harness, recurrence, cycle):
    engine = harness.controller
    proposed = profile(12000, kind="Recurring", recurrency_kind=recurrence, duration=10)
    assert await engine.set_profile(1, proposed) == "Accepted"
    harness.clock[0] += cycle - 5
    result = await engine.composite_schedule(1, 20, "W")
    assert result["charging_schedule"]["chargingSchedulePeriod"] == [
        {"startPeriod": 0, "limit": 90000}, {"startPeriod": 5, "limit": 12000}, {"startPeriod": 15, "limit": 90000}]


async def test_station_budget_never_exceeded_during_active_head_transitions(harness):
    engine = harness.controller
    assert await engine.tick()
    assert harness.physical[1]["W"] == harness.physical[2]["W"] == 90000
    harness.active[2] = 202
    assert await engine.tick()
    assert harness.physical[1]["W"] == 0 and harness.physical[2]["W"] == 180000
    harness.active[1] = 101
    assert await engine.tick()
    assert harness.physical[1]["W"] == harness.physical[2]["W"] == 90000
    harness.active[2] = 0
    assert await engine.tick()
    assert harness.physical[1]["W"] == 180000 and harness.physical[2]["W"] == 0
    assert all(sum(state[cid]["W"] for cid in (1, 2)) <= 180000 for _, _, state in harness.writes)


async def test_station_max_is_aggregate_not_applied_twice(harness):
    engine = harness.controller
    harness.active.update({1: 101, 2: 202})
    assert await engine.set_profile(0, profile(100000, purpose="ChargePointMaxProfile")) == "Accepted"
    assert harness.physical[1]["W"] == harness.physical[2]["W"] == 50000
    result = await engine.composite_schedule(0, 60, "W")
    assert result["charging_schedule"]["chargingSchedulePeriod"][0]["limit"] == 100000


@pytest.mark.parametrize("unit,limit,share", [("W", 100000, 50000), ("A", 50.5, 25.2)])
async def test_csms_constant_relative_station_max_needs_no_transaction_anchor(harness, unit, limit, share):
    engine = harness.controller
    engine.transaction_started_at = None
    harness.active.update({1: 101, 2: 202})
    assert await engine.set_profile(0, csms_constant_station_max(limit, unit)) == "Accepted"
    assert harness.physical[1][unit] == harness.physical[2][unit] == share
    for cid, expected in ((0, share * 2), (1, share), (2, share)):
        result = await engine.composite_schedule(cid, 60, unit)
        assert result["status"] == "Accepted"
        assert result["charging_schedule"] == {"duration": 60, "chargingRateUnit": unit,
            "chargingSchedulePeriod": [{"startPeriod": 0, "limit": expected}]}
    assert json.loads(harness.path.read_text())["anchors"] == {}


async def test_relative_station_cap_is_never_doubled_during_independent_head_transitions(harness):
    engine = harness.controller
    engine.transaction_started_at = None
    assert await engine.set_profile(0, csms_constant_station_max()) == "Accepted"
    for transactions, expected in [
            ({1: 101, 2: 0}, (100000, 0)),
            ({1: 101, 2: 202}, (50000, 50000)),
            ({1: 0, 2: 202}, (0, 100000)),
            ({1: 303, 2: 202}, (50000, 50000)),
            ({1: 303, 2: 0}, (100000, 0)),
            ({1: 0, 2: 0}, (50000, 50000))]:
        harness.active.update(transactions)
        harness.clock[0] += 30
        assert await engine.tick()
        assert (harness.physical[1]["W"], harness.physical[2]["W"]) == expected
    # Checks every intermediate adapter write, not just the eventual target.
    # A joining head must receive power only after the other head is reduced.
    assert all(sum(state[cid]["W"] for cid in (1, 2)) <= 100000
               for _, _, state in harness.writes)


@pytest.mark.parametrize("advance", [-86400, 400 * 86400])
async def test_constant_station_cap_persists_across_restart_and_clock_changes(harness, advance):
    engine = harness.controller
    assert await engine.set_profile(0, csms_constant_station_max()) == "Accepted"
    stored = harness.path.read_bytes()
    harness.clock[0] += advance
    harness.active.update({1: 101, 2: 202})
    reloaded = SmartChargingController(harness.path, engine.apply_limits,
        clock=lambda: harness.clock[0], active_transaction=harness.active.get)
    assert await reloaded.tick()
    assert harness.physical[1]["W"] == harness.physical[2]["W"] == 50000
    assert harness.path.read_bytes() == stored
    assert reloaded.profiles == engine.profiles
    result = await reloaded.composite_schedule(0, 60, "W")
    assert result["charging_schedule"]["chargingSchedulePeriod"] == [{"startPeriod": 0, "limit": 100000}]


async def test_constant_relative_station_cap_keeps_absolute_validity_boundaries(harness):
    proposed = csms_constant_station_max()
    proposed.update(validFrom=iso(START + 10), validTo=iso(START + 30))
    assert await harness.controller.set_profile(0, proposed) == "Accepted"
    result = await harness.controller.composite_schedule(0, 40, "W")
    assert result["charging_schedule"]["chargingSchedulePeriod"] == [
        {"startPeriod": 0, "limit": 180000}, {"startPeriod": 10, "limit": 100000},
        {"startPeriod": 30, "limit": 180000}]
    harness.clock[0] += 15
    assert await harness.controller.tick()
    assert harness.physical[1]["W"] == harness.physical[2]["W"] == 50000
    harness.clock[0] += 15
    assert await harness.controller.tick()
    assert harness.physical[1]["W"] == harness.physical[2]["W"] == 90000


@pytest.mark.parametrize("mutate", [
    lambda p: p["chargingSchedule"].update(duration=3600),
    lambda p: p["chargingSchedule"]["chargingSchedulePeriod"].append({"startPeriod": 10, "limit": 50000}),
    lambda p: p["chargingSchedule"].update(startSchedule=iso(START)),
])
async def test_unsupported_timed_relative_station_cap_preserves_current_cap(harness, mutate):
    engine = harness.controller
    assert await engine.set_profile(0, csms_constant_station_max()) == "Accepted"
    before = harness.path.read_bytes()
    writes = len(harness.writes)
    proposed = csms_constant_station_max(150000)
    mutate(proposed)
    assert await engine.set_profile(0, proposed) == "Rejected"
    assert harness.path.read_bytes() == before
    assert len(harness.writes) == writes
    assert harness.physical[1]["W"] == harness.physical[2]["W"] == 50000


@pytest.mark.parametrize("cid", [1, 2])
async def test_relative_station_max_still_requires_connector_zero(harness, cid):
    assert await harness.controller.set_profile(cid, csms_constant_station_max()) == "Rejected"
    assert not harness.path.exists()
    assert harness.writes == []


async def test_constant_relative_station_replacement_persists_the_new_shared_cap(harness):
    engine = harness.controller
    harness.active.update({1: 101, 2: 202})
    assert await engine.set_profile(0, csms_constant_station_max(60000)) == "Accepted"
    proposed = csms_constant_station_max()
    proposed["chargingProfileId"] = 2
    assert await engine.set_profile(0, proposed) == "Accepted"
    assert len(engine.profiles) == 1
    assert harness.physical[1]["W"] == harness.physical[2]["W"] == 50000
    reloaded = SmartChargingController(harness.path, AsyncMock(), clock=lambda: START,
        active_transaction=harness.active.get)
    assert reloaded.effective_limits(1)[0]["W"] == reloaded.effective_limits(2)[0]["W"] == 50000
    assert reloaded.profiles[0]["profile"]["charging_profile_id"] == 2
    assert all(sum(state[cid]["W"] for cid in (1, 2)) <= 100000
               for _, _, state in harness.writes)


@pytest.mark.parametrize("stage", ["journal", "profile"])
async def test_relative_station_cap_persistence_failure_cannot_acknowledge_higher_limit(harness, monkeypatch, stage):
    engine = harness.controller
    assert await engine.set_profile(0, csms_constant_station_max()) == "Accepted"
    before = harness.path.read_bytes()
    original = engine._write_atomic
    failure_path = engine.pending_path if stage == "journal" else engine.path
    failed = False

    def fail_once(path, data):
        nonlocal failed
        if path == failure_path and not failed:
            failed = True
            raise OSError("Disk full")
        return original(path, data)

    monkeypatch.setattr(engine, "_write_atomic", fail_once)
    assert await engine.set_profile(0, csms_constant_station_max(160000)) == "Rejected"
    assert failed
    assert harness.path.read_bytes() == before
    assert not engine.inhibited
    assert not engine.pending_path.exists()
    assert harness.physical[1]["W"] == harness.physical[2]["W"] == 50000
    assert all(sum(state[cid]["W"] for cid in (1, 2)) <= 100000
               for _, _, state in harness.writes)


async def test_relative_station_cap_unknown_adapter_outcome_retains_failure_gate(harness):
    engine = harness.controller
    assert await engine.set_profile(0, csms_constant_station_max()) == "Accepted"
    before = harness.path.read_bytes()

    async def timed_out(cid, limits):
        raise TimeoutError("Limit response lost")

    engine.apply_limits = timed_out
    assert await engine.set_profile(0, csms_constant_station_max(160000)) == "Rejected"
    assert engine.inhibited and engine.pending_path.exists()
    assert harness.path.read_bytes() == before
    assert harness.faults
    assert not await engine.tick()
    assert (await engine.composite_schedule(0, 60, "W"))["status"] == "Rejected"
    with pytest.raises(SmartChargingStateError):
        SmartChargingController(harness.path, AsyncMock())


def test_persisted_unsupported_relative_station_timeline_fails_closed(harness):
    engine = harness.controller
    row = {"connector_id": 0, "profile": csms_constant_station_max(),
           "installed_at": START, "bound_transaction_id": None, "excluded_connectors": []}
    row["profile"]["chargingSchedule"]["chargingSchedulePeriod"].append({"startPeriod": 10, "limit": 50000})
    harness.path.write_text(json.dumps({"version": 1, "profiles": [row], "anchors": {}}))
    with pytest.raises(SmartChargingStateError):
        SmartChargingController(harness.path, AsyncMock())


async def test_fractional_station_allocation_rounds_down_without_overstating_cap(harness):
    engine = harness.controller
    assert await engine.set_profile(0, profile(50.5, "A", purpose="ChargePointMaxProfile")) == "Accepted"
    assert harness.physical[1]["A"] == harness.physical[2]["A"] == 25.2
    result = await engine.composite_schedule(0, 60, "A")
    assert result["charging_schedule"]["chargingSchedulePeriod"][0]["limit"] == 50.4


async def test_specific_replacement_of_global_default_has_future_validity_gap(harness):
    engine = harness.controller
    await engine.set_profile(0, profile(20000, pid=1))
    assert await engine.set_profile(1, profile(5000, pid=2, valid_from=iso(START + 10))) == "Accepted"
    # Replaced means the old same-stack global profile cannot silently stay in
    # force on head 1 until validFrom; it remains valid only for head 2.
    assert harness.physical[1]["W"] == 90000
    assert harness.physical[2]["W"] == 20000
    result = await engine.composite_schedule(1, 20, "W")
    assert result["charging_schedule"]["chargingSchedulePeriod"] == [
        {"startPeriod": 0, "limit": 90000}, {"startPeriod": 10, "limit": 5000}]
    reload = SmartChargingController(harness.path, AsyncMock(), clock=lambda: START)
    assert reload.effective_limits(1)[0]["W"] == 90000
    assert reload.effective_limits(2)[0]["W"] == 20000


async def test_new_global_default_replaces_same_stack_per_head_defaults(harness):
    engine = harness.controller
    await engine.set_profile(1, profile(10000, pid=1))
    await engine.set_profile(2, profile(20000, pid=2))
    assert await engine.set_profile(0, profile(30000, pid=3)) == "Accepted"
    assert len(engine.profiles) == 1
    assert harness.physical[1]["W"] == harness.physical[2]["W"] == 30000


async def test_minimum_rate_is_optional_efficiency_hint_not_an_override(harness):
    proposed = profile(10000)
    proposed["charging_schedule"]["min_charging_rate"] = 12000
    assert await harness.controller.set_profile(1, proposed) == "Accepted"
    assert harness.physical[1]["W"] == 10000


async def test_large_composite_is_rejected_before_unbounded_calculation(harness):
    engine = harness.controller
    proposed = profile(kind="Recurring", recurrency_kind="Daily", duration=86400,
        periods=[{"start_period": value * 300, "limit": 10000 + value} for value in range(256)])
    assert await engine.set_profile(1, proposed) == "Accepted"
    assert (await engine.composite_schedule(1, 7 * 86400, "W"))["status"] == "Rejected"


async def test_mixed_units_and_conversion_are_not_fabricated(harness):
    engine = harness.controller
    await engine.set_profile(0, profile(100000, purpose="ChargePointMaxProfile"))
    await engine.set_profile(1, profile(50, "A", pid=2))
    assert harness.physical[1] == {"W": 50000, "A": 50}
    assert (await engine.composite_schedule(1, 60, "A"))["status"] == "Rejected"
    assert (await engine.composite_schedule(1, 60, "W"))["status"] == "Rejected"
    assert (await engine.composite_schedule(1, 60))["status"] == "Rejected"


async def test_camel_case_profile_and_requested_phases_are_preserved(harness):
    proposed = {"chargingProfileId": 20, "stackLevel": 0, "chargingProfilePurpose": "TxDefaultProfile",
                "chargingProfileKind": "Absolute", "chargingSchedule": {"startSchedule": iso(START),
                "chargingRateUnit": "A", "duration": 120, "chargingSchedulePeriod": [{"startPeriod": 0, "limit": 32, "numberPhases": 1}]}}
    assert await harness.controller.set_profile(1, proposed) == "Accepted"
    result = await harness.controller.composite_schedule(1, 60, "A")
    assert result["charging_schedule"]["chargingSchedulePeriod"] == [{"startPeriod": 0, "limit": 32, "numberPhases": 1}]


async def test_persistence_failure_before_hardware_keeps_previous_state(harness, monkeypatch):
    engine = harness.controller
    await engine.set_profile(1, profile(10000))
    before = harness.path.read_bytes()
    count = len(harness.writes)
    original = engine._write_atomic

    def failed(path, data):
        if path == engine.pending_path:
            raise OSError("Disk full")
        return original(path, data)

    monkeypatch.setattr(engine, "_write_atomic", failed)
    assert await engine.set_profile(1, profile(20000)) == "Rejected"
    assert harness.path.read_bytes() == before
    assert len(harness.writes) == count
    assert not engine.inhibited


async def test_unknown_hardware_write_outcome_latches_fault_and_rejects_followups(harness):
    engine = harness.controller
    await engine.set_profile(1, profile(10000))
    before = harness.path.read_bytes()
    async def timed_out(cid, limits):
        raise TimeoutError("PLC reply lost")
    engine.apply_limits = timed_out
    assert await engine.set_profile(1, profile(20000)) == "Rejected"
    assert engine.inhibited
    assert harness.faults
    assert harness.path.read_bytes() == before
    assert engine.pending_path.exists()
    assert await engine.set_profile(2, profile(20000, pid=3)) == "Rejected"
    assert await engine.clear_profiles() == "Unknown"
    assert (await engine.composite_schedule(1, 60))["status"] == "Rejected"
    assert not await engine.tick()
    with pytest.raises(SmartChargingStateError):
        SmartChargingController(harness.path, AsyncMock())


@pytest.mark.parametrize("response", [False, "limitpower Failed"])
async def test_negative_adapter_results_are_not_acknowledged_as_applied(harness, response):
    async def failed(cid, limits):
        return response
    harness.controller.apply_limits = failed
    assert await harness.controller.set_profile(1, profile()) == "Rejected"
    assert harness.controller.inhibited
    assert harness.faults


def test_corrupt_profile_store_does_not_silently_reset_limits(tmp_path):
    path = tmp_path / "profiles.json"
    path.write_text('{"version":1,"profiles":[{"connector_id":3}]}')
    with pytest.raises(SmartChargingStateError):
        SmartChargingController(path, AsyncMock())


@pytest.mark.parametrize("cid,duration,unit", [(3, 60, "W"), (True, 60, "W"), (1, 0, "W"), (1, True, "W"), (1, 604801, "W"), (1, 60, "VA")])
async def test_invalid_composite_queries_rejected(harness, cid, duration, unit):
    assert await harness.controller.composite_schedule(cid, duration, unit) == {"status": "Rejected"}


async def test_run_loop_is_cancellable_without_starting_hardware_processes(harness):
    task = asyncio.create_task(harness.controller.run(interval=.01))
    await asyncio.sleep(.025)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert harness.writes
