"""Submit one test-only simulator event and wait for its durable result."""
import argparse
import json
import os
import time
import uuid
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("request", help="JSON object containing a supported local simulator action")
parser.add_argument("--directory", type=Path, default=Path(__file__).resolve().parents[1] / "simulator_state" / "controls")
parser.add_argument("--timeout", type=float, default=25)
args = parser.parse_args()
request = json.loads(args.request)
args.directory.mkdir(parents=True, exist_ok=True)
stem = f"{time.time_ns()}-{uuid.uuid4().hex[:6]}"
source = args.directory / (stem + ".request.json")
temporary = args.directory / (stem + ".tmp")
temporary.write_text(json.dumps(request), encoding="utf-8")
os.replace(temporary, source)
result = args.directory / (stem + ".result.json")
deadline = time.monotonic() + args.timeout
while time.monotonic() < deadline:
    if result.exists():
        print(result.read_text(encoding="utf-8"))
        break
    time.sleep(.1)
else:
    raise SystemExit("Test result not received; inspect claimed/result files before retrying")
