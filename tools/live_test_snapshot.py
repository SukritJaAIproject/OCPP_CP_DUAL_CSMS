"""Read-only, redacted evidence for a dual-CSMS simulated-charger run.

No application modules are imported, no endpoint is contacted, and SQLite is
opened in mode=ro with query_only enabled. A registration event is historical
observation, not a probe that proves the process is still online.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path


EVENT_FIELDS = {
    "timestamp", "simulated_hardware", "event", "connector", "local_id",
    "meter_wh", "status", "ready", "both_registered", "error_type",
    "automatic_replay", "automatic_transactions", "hardware",
}


def utc(value):
    if not value:
        return None
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def load_json(path, fallback):
    if not path.exists():
        return fallback
    return json.loads(path.read_text(encoding="utf-8"))


def load_events(path):
    result = []
    malformed = 0
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                event = json.loads(line)
                result.append({key: value for key, value in event.items() if key in EVENT_FIELDS})
            except (ValueError, TypeError):
                # The final line may be in flight while a process appends it.
                malformed += 1
    return result, malformed


def payload(row):
    try:
        return json.loads(row["payload"] or "{}")
    except (ValueError, TypeError):
        return {}


def snapshot(directory, limit=20, since=None, local_ids=None):
    directory = Path(directory).resolve()
    generated = datetime.now(timezone.utc)
    cutoff = utc(since) if since else None
    ids = set(local_ids or [])
    events, malformed = load_events(directory / "simulator-events.jsonl")
    core = load_json(directory / "sim_state.json", {})
    db_path = directory / "dual_csms_state.sqlite3"
    report = {
        "generated_at_utc": generated.isoformat(),
        "read_only": True,
        "network_connections_attempted": 0,
        "state_directory": str(directory),
        "simulated_hardware": core.get("simulated_hardware"),
        "filters": {"since_utc": cutoff.isoformat() if cutoff else None, "local_ids": sorted(ids)},
        "registration_observation": next((e for e in reversed(events) if e.get("event") == "registration_state"), None),
        "registration_is_live_probe": False,
        "latest_simulator_lifecycle": next((e for e in reversed(events) if e.get("event") in {"simulator_started", "simulator_stopped"}), None),
        "heads": [], "sessions": [], "outbox_counts": [], "overlap_pairs": [],
        "recent_events": [], "warnings": [],
    }
    if malformed:
        report["warnings"].append(f"Ignored {malformed} malformed or incomplete JSONL lines")
    all_sessions = []
    outbox = []
    if db_path.exists():
        with sqlite3.connect(db_path.as_uri() + "?mode=ro", uri=True, timeout=3) as db:
            db.row_factory = sqlite3.Row
            db.execute("PRAGMA query_only=ON")
            db.execute("BEGIN")
            all_sessions = [dict(row) for row in db.execute(
                "SELECT local_id,connector,owner,remote_id,state,created,start_payload,start_result "
                "FROM sessions ORDER BY local_id")]
            outbox = [dict(row) for row in db.execute(
                "SELECT session_id,server,action,state,attempts,last_error,payload FROM outbox ORDER BY seq")]
            report["outbox_counts"] = [dict(row) for row in db.execute(
                "SELECT server,action,state,count(*) AS count FROM outbox "
                "GROUP BY server,action,state ORDER BY server,action,state")]
            db.rollback()
    else:
        report["warnings"].append("Simulator SQLite database does not exist")

    by_id = {row["local_id"]: row for row in all_sessions}
    for cid, state in sorted(core.get("heads", {}).items()):
        local_id = int(state.get("local_id") or 0)
        mapped = by_id.get(local_id, {})
        report["heads"].append({
            "connector": int(cid), "local_id": local_id,
            "owner": mapped.get("owner"), "remote_id": mapped.get("remote_id"),
            "mapping_state": mapped.get("state"), "pending": bool(state.get("pending")),
            "status": state.get("status"), "simulated_meter_wh": state.get("meter_wh"),
        })

    selected = [row for row in all_sessions if (not ids or row["local_id"] in ids)
                and (cutoff is None or float(row["created"]) >= cutoff.timestamp())]
    selected = selected[-limit:]
    for row in selected:
        local_id = row["local_id"]
        related_events = [e for e in events if e.get("local_id") == local_id]
        started = next((e.get("timestamp") for e in related_events if e.get("event") == "transaction_started"), None)
        stopped = next((e.get("timestamp") for e in related_events if e.get("event") == "stop_saved_to_outbox"), None)
        remote_stop_seen = any(e.get("event") == "remote_stop_accepted" for e in related_events)
        deliveries = [item for item in outbox if item["session_id"] == local_id]
        meters = [item for item in deliveries if item["action"] == "MeterValues"]
        stops = [item for item in deliveries if item["action"] == "StopTransaction"]
        stop = stops[-1] if stops else None
        stop_payload = payload(stop) if stop else {}
        try:
            start_payload = json.loads(row["start_payload"] or "{}")
            start_result = json.loads(row["start_result"] or "{}")
        except (ValueError, TypeError):
            start_payload, start_result = {}, {}
        start_wh = start_payload.get("meter_start")
        stop_wh = stop_payload.get("meter_stop")
        # Only safe, selected scalar fields leave this function. Raw payloads,
        # start_result, idTags and authentication data are never in the report.
        summary = {
            "local_id": local_id, "connector": row["connector"], "owner": row["owner"],
            "remote_id": row["remote_id"], "state": row["state"],
            "reserved_at_utc": datetime.fromtimestamp(row["created"], timezone.utc).isoformat(),
            "started_at_utc": started or start_payload.get("timestamp"),
            "stop_saved_at_utc": stopped or stop_payload.get("timestamp"),
            "start_authorization_status": start_result.get("id_tag_info", {}).get("status"),
            "remote_stop_command_seen": remote_stop_seen,
            "meter_messages": dict(Counter(item["state"] for item in meters)),
            "stop_delivery_state": stop.get("state") if stop else None,
            "stop_attempts": stop.get("attempts") if stop else None,
            "stop_error_type": stop.get("last_error") if stop else None,
            "stop_reason": stop_payload.get("reason"),
            "meter_start_wh": start_wh, "meter_stop_wh": stop_wh,
            "simulated_energy_wh": stop_wh - start_wh if isinstance(start_wh, (int, float)) and isinstance(stop_wh, (int, float)) else None,
            "all_transaction_messages_scoped_to_owner": all(item["server"] == row["owner"] for item in deliveries),
            "remote_completion_confirmed": row["state"] == "closed" and bool(stop) and stop["state"] == "delivered",
            "meter_delivery_confirmed": any(item["state"] == "delivered" for item in meters),
        }
        report["sessions"].append(summary)

    for index, a in enumerate(report["sessions"]):
        for b in report["sessions"][index + 1:]:
            if {a["owner"], b["owner"]} != {"A", "B"} or {a["connector"], b["connector"]} != {1, 2}:
                continue
            if not a["started_at_utc"] or not b["started_at_utc"]:
                continue
            begin = max(utc(a["started_at_utc"]), utc(b["started_at_utc"]))
            end = min(utc(a["stop_saved_at_utc"]) or generated, utc(b["stop_saved_at_utc"]) or generated)
            if end > begin:
                report["overlap_pairs"].append({
                    "local_ids": [a["local_id"], b["local_id"]],
                    "owners": [a["owner"], b["owner"]], "connectors": [a["connector"], b["connector"]],
                    "overlap_start_utc": begin.isoformat(), "overlap_end_utc": end.isoformat(),
                    "overlap_seconds": round((end - begin).total_seconds(), 3),
                    "both_remote_completions_confirmed": a["remote_completion_confirmed"] and b["remote_completion_confirmed"],
                })
    filtered_events = [e for e in events if (not cutoff or utc(e.get("timestamp")) >= cutoff)
                       and (not ids or e.get("local_id") in ids)]
    report["recent_events"] = filtered_events[-limit:]
    if any(row["state"] in {"unknown", "starting"} for row in all_sessions):
        report["warnings"].append("An ambiguous Start is retained; do not replay or delete its evidence")
    if any(item["state"] == "failed" for item in outbox):
        report["warnings"].append("Failed outbound evidence exists; it is not a remote success")
    if report["latest_simulator_lifecycle"] and report["latest_simulator_lifecycle"]["event"] == "simulator_stopped":
        report["warnings"].append("Latest lifecycle event says the simulator stopped")
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, default=Path(__file__).resolve().parents[1] / "simulator_state")
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--since", help="ISO-8601 UTC or offset timestamp; selects sessions reserved since this time")
    parser.add_argument("--local-ids", help="Comma-separated local transaction IDs to include")
    parser.add_argument("--output", type=Path, help="Optional new JSON report file; never changes simulator state")
    args = parser.parse_args(argv)
    if not 1 <= args.limit <= 100:
        parser.error("limit must be between 1 and 100")
    ids = [int(value) for value in args.local_ids.split(",")] if args.local_ids else None
    report = snapshot(args.state_dir, args.limit, args.since, ids)
    text = json.dumps(report, ensure_ascii=False, indent=2)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text + "\n", encoding="utf-8")
    print(text)


if __name__ == "__main__":
    main()
