"""R3.1.3: admin_csms may name several CSMS, so B can send what A can (asked 2026-11-09).

Transactions stay owner-scoped: an admin still stops only its own transactions, and
RFID starts at the charger still belong to local_csms.
"""
import copy

import pytest

from helpf.dual_csms import validate_config
from test_dual_csms_integration import FakeCSMS, configuration, running_manager

ADMIN_COMMANDS = [
    ("Reset", {"type": "Soft"}),
    ("ChangeConfiguration", {"key": "HeartbeatInterval", "value": "20"}),
    ("ClearChargingProfile", {}),
    ("ClearCache", {}),
    ("SendLocalList", {"listVersion": 1, "updateType": "Full"}),
    ("UnlockConnector", {"connectorId": 2}),
]


def servers():
    return [{"name": "A", "url": "wss://a.test/ocpp/X", "enabled": True},
            {"name": "B", "url": "ws://b.test:9000/ocpp/Y", "enabled": True}]


@pytest.mark.parametrize("value, admins", [("A", ["A"]), (["A", "B"], ["A", "B"]), (["B", "A"], ["B", "A"])])
def test_admin_csms_accepts_one_name_or_a_list(value, admins):
    result = validate_config({"enabled": True, "local_csms": "A", "admin_csms": value, "servers": servers()})
    assert result["admins"] == admins
    assert result["primary_admin"] == admins[0]
    assert result["admin_csms"] == value          # kept as written, for the reports


@pytest.mark.parametrize("value", [[], ["A", "A"], ["A", "C"], [1], "", "C", {"A": 1}])
def test_admin_csms_rejects_unknown_duplicate_or_empty_names(value):
    with pytest.raises(ValueError, match="admin_csms"):
        validate_config({"enabled": True, "local_csms": "A", "admin_csms": value, "servers": servers()})


def test_disabled_dual_mode_still_normalises_the_admin():
    result = validate_config({"enabled": False, "local_csms": "A", "admin_csms": ["A", "B"], "servers": []})
    assert result["primary_admin"] == "A"


@pytest.mark.asyncio
async def test_b_can_send_every_admin_command_when_listed(tmp_path):
    async with FakeCSMS() as a, FakeCSMS() as b:
        config = configuration(a, b)
        config["admin_csms"] = ["A", "B"]
        async with running_manager(copy.deepcopy(config), tmp_path) as (manager, core):
            for action, payload in ADMIN_COMMANDS:
                response = await b.request(action, payload)
                assert "callError" not in response, (action, response)
                assert response.get("status") not in ("Rejected", "Unknown", "Failed", "UnlockFailed"), (action, response)
            assert [action for action, _ in core.mutations] == [action for action, _ in ADMIN_COMMANDS]
            # A keeps the same rights
            assert await a.request("ClearCache", {}) == {"status": "Accepted"}


@pytest.mark.asyncio
async def test_b_is_still_refused_when_only_a_is_admin(tmp_path):
    async with FakeCSMS() as a, FakeCSMS() as b:
        async with running_manager(configuration(a, b), tmp_path) as (manager, core):
            for action, payload in ADMIN_COMMANDS:
                response = await b.request(action, payload)
                assert response.get("status") in ("Rejected", "Unknown", "Failed", "UnlockFailed") or "callError" in response
            assert core.mutations == []


@pytest.mark.asyncio
async def test_admin_b_still_cannot_stop_a_transaction_of_a(tmp_path):
    async with FakeCSMS() as a, FakeCSMS() as b:
        config = configuration(a, b)
        config["admin_csms"] = ["A", "B"]
        async with running_manager(copy.deepcopy(config), tmp_path) as (manager, core):
            assert await a.request("RemoteStartTransaction", {"connectorId": 1, "idTag": "A-tag"}) == {"status": "Accepted"}
            # B does not know A's transaction id; any id it sends is not one of its own sessions
            assert await b.request("RemoteStopTransaction", {"transactionId": 1}) == {"status": "Rejected"}
