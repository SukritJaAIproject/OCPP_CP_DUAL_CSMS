"""Read-only configuration/runtime check. Never imports CP.py or opens a socket."""
import argparse
import importlib.metadata
import inspect
import json
import sqlite3
import sys
from pathlib import Path


def inspect_state(directory):
    database = directory / "dual_csms_state.sqlite3"
    result = {"database_present": database.exists(), "controller_transactions": {},
              "sessions": [], "outbox_counts": [], "attention": []}
    for cid in (1, 2):
        path = directory / f"translogs{cid}.txt"
        if path.exists():
            try:
                result["controller_transactions"][str(cid)] = int(path.read_text().strip() or "0")
            except (OSError, ValueError):
                result["attention"].append(f"Cannot read translogs{cid}.txt")
    if not database.exists():
        if any(result["controller_transactions"].values()):
            result["attention"].append("First installation requires both connectors idle and original transactions closed.")
        return result
    with sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True) as db:
        db.row_factory = sqlite3.Row
        result["sessions"] = [dict(r) for r in db.execute(
            "SELECT local_id,connector,owner,remote_id,state FROM sessions "
            "WHERE state NOT IN ('closed','cancelled') ORDER BY local_id")]
        result["outbox_counts"] = [dict(r) for r in db.execute(
            "SELECT server,action,state,count(*) AS count FROM outbox "
            "GROUP BY server,action,state ORDER BY server,action,state")]
    for session in result["sessions"]:
        if session["state"] in ("unknown", "starting", "rejected"):
            result["attention"].append(f"Session {session['local_id']} needs reconciliation: {session['state']}")
        local = result["controller_transactions"].get(str(session["connector"]), 0)
        if session["state"] == "active" and local != session["local_id"]:
            result["attention"].append(f"Session {session['local_id']} disagrees with controller state")
    for row in result["outbox_counts"]:
        if row["state"] == "failed":
            result["attention"].append(f"{row['server']} has {row['count']} failed {row['action']} messages")
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path(__file__).with_name("dual_csms_config.json"))
    parser.add_argument("--state-dir", type=Path, default=Path.cwd())
    args = parser.parse_args(argv)
    report = {"network_connections_attempted": 0, "hardware_imported": False,
              "config": str(args.config.resolve()), "state_dir": str(args.state_dir.resolve())}
    try:
        from ocpp.v16 import ChargePoint
        from helpf.dual_csms import validate_config
        config = validate_config(json.loads(args.config.read_text(encoding="utf-8-sig")))
        report["dependencies"] = {name: importlib.metadata.version(name) for name in ("ocpp", "websockets")}
        params = inspect.signature(ChargePoint.call).parameters
        if not {"suppress", "unique_id"}.issubset(params):
            raise RuntimeError("Installed ocpp.call must support suppress and unique_id; validated version is ocpp 2.1.0")
        report["enabled"] = config.get("enabled", True)
        report["servers"] = [{"name": s["name"], "enabled": s.get("enabled", True), "url": s["url"]}
                             for s in config["servers"]]
        report["local_csms"] = config["local_csms"]
        report["admin_csms"] = config["admin_csms"]
        report["state"] = inspect_state(args.state_dir)
        report["result"] = "attention" if report["state"]["attention"] else "configuration_check_passed"
    except Exception as error:
        report["result"] = "failed"
        report["error"] = f"{type(error).__name__}: {error}"
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return 0 if report["result"] == "configuration_check_passed" else 1


if __name__ == "__main__":
    sys.exit(main())
