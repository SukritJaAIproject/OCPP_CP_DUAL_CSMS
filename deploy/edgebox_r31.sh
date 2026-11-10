#!/usr/bin/env bash
# edgebox_r31.sh - install helper for OCPP dual-CSMS R3.1.6 on the Edgebox (Raspberry Pi with CP.py)
#
#   bash ~/edgebox_r31.sh check                      read-only report, changes nothing
#   bash ~/edgebox_r31.sh stage                      unpack + verify + offline tests in ~/r31_stage
#                                                    (the running charger program is not touched)
#   RATED_W=120000 bash ~/edgebox_r31.sh install <folder with CP.py>
#                                                    tests as the service user if not done yet, then
#                                                    backup, stop service, copy 10 files, start
#                                                    (keeps this Edgebox's MQTT station name; KEEP_SITE=0 does not;
#                                                    ADMIN_CSMS=A,B lets B send the admin commands too)
#   bash ~/edgebox_r31.sh logs <folder with CP.py>   show registration lines from the newest CP log
#   bash ~/edgebox_r31.sh diag [folder with CP.py]   read-only: head flags, sessions, card checks, limits, refusals
#   bash ~/edgebox_r31.sh rollback <backup folder> <folder with CP.py>
#   bash ~/edgebox_r31.sh zip [folder with CP.py]    copy the whole folder into ~/<folder>_snapshot_<time>.zip
#                                                    while the charger keeps running (changes nothing in it)
#
# bash reads this whole file before running anything (see the braces at the end), so
# uploading a new copy while an older one runs cannot change what the running copy does.
set -u
umask 022

# R3.1.1 = R3.1 + a log line naming the reason of every refused RemoteStart (behaviour unchanged)
# R3.1.2 = R3.1.1 + RemoteStart allowed while the PLC reports SuspendedEV without a transaction,
#          and R3 service statuses no longer replace a Vector Faulted status
# R3.1.3 = R3.1.2 + admin_csms may list several CSMS (B can send what A can)
# R3.1.4 = R3.1.3 + a card tapped at the charger is checked by A first, then by B;
#          the session belongs to the CSMS that accepted it
# R3.1.5 = R3.1.4 + Charging/Suspended/Finishing statuses carry info="Session of CSMS <owner> (tx <id>)"
#          to both CSMS, so either side's log says whose session a head is running
# R3.1.6 = R3.1.5 + a RemoteStart that ends before StartTransaction releases its start latch and
#          claim; a latch left behind is released by the next start instead of refusing it
PKG_NAME=OCPP_CP_DUAL_CSMS_R316_CANDIDATE_2026-11-10.zip
PKG_SHA=1a8d71b798f2e3de1d3eb2f93e4d5f444694d2815e95e40d420c09214c753d4e
HERE=$(cd "$(dirname "$0")" && pwd)
STAGE="$HOME/r31_stage"
PKG_DIR="$STAGE/OCPP_CP_DUAL_CSMS_R316"
FILES="CP.py helpf/plc_function.py helpf/v2g_datatransfer.py helpf/dual_csms.py dual_csms_config.json dual_csms_check.py helpf/ocpp_services.py helpf/ocpp_smart_charging.py helpf/ocpp_runtime.py ocpp_services_config.json"

die() { echo "STOP: $*"; exit 1; }
sha() { sha256sum "$1" | cut -d' ' -f1; }

# Versions of the three files R3.1 replaces that are known and safe to replace.
known() {
  case "$1:$2" in
    CP.py:036eb9df4287f1442d7e6b5892ab51c5dd4e1db40cdfdd4342eddab554a15c8a) echo "original (R3.1 built on this)" ;;
    CP.py:f737547e086fd817f9cf9d10ece9bcef979b034c29626a38f15a239d85f6790d) echo "R1/R2" ;;
    CP.py:02b125b5b86d9619f138f8312769a653c2efc2ebfaf378a945de40645d7279b3) echo "R3/R3.1" ;;
    CP.py:a1e11eeb57d26188cc21350178f4e03ec33069549248e444b90838213e55e1ae) echo "R3.1.1" ;;
    CP.py:9340bdfa36846e26c9d77cab0b70b0859755b3a2bf6bc735615a6fe06444f209) echo "R3.1.2/R3.1.3" ;;
    CP.py:a73e7720828814c4e7ac89e14f8a9bee72e9b0424f3aa6e3cab2da9f2876016f) echo "R3.1.4/R3.1.5" ;;
    CP.py:ae16ac09a3a95ae7288a7612f9f0b5ae228c65056eebc06394c36a5d5a58c571) echo "R3.1.6" ;;
    # The Edgebox's own CP.py found on 2026-11-09 (file time 10:44). On 2026-11-09 the user chose
    # to replace it with R3.1 without merging; install backs up the whole folder first.
    CP.py:9e925f117b15*) echo "Edgebox version of 2026-11-09 (user approved replacing it)" ;;
    helpf/plc_function.py:0b4f062526e6ec3fb444b28d61e7276e23192c29b6f6af0d1b295f158331c803) echo "original (R3.1 built on this)" ;;
    helpf/plc_function.py:86d17d214adc81641e0efee7a670c0c1514e46d8f8cf09de6e35677f92bcc215) echo "R1/R2" ;;
    helpf/plc_function.py:46dea5393fcaa711cdeff9b9487c43c4028e2ac3d762a049b5208b311cc29bb9) echo "R3-R3.1.6" ;;
    helpf/v2g_datatransfer.py:1e02b6ea7d83233bedbed485dac32a024b42115cad4079dcf5a7b8ddb1ffb43e) echo "original (R3.1 built on this)" ;;
    helpf/v2g_datatransfer.py:502ecdeaf6c60962fcf310bd7717d32e74e732745224edba36dd17bd4a43fd8c) echo "R1-R3.1.6" ;;
    *) echo "UNKNOWN" ;;
  esac
}

cp_pids() { pgrep -f 'CP\.py' || true; }

svc_user() { # user that runs the CP.py service: User= of the unit, root when it has none
  local u=""
  [ -n "${UNIT:-}" ] && u=$(systemctl show -p User --value "$UNIT" 2>/dev/null)
  echo "${u:-root}"
}

as_svc() { # run a command as the service user with that user's HOME, so Python sees the same packages
  if [ "$(id -un)" = "$SVC_USER" ]; then "$@"; else sudo -H -u "$SVC_USER" "$@"; fi
}

pkg_versions() { # python command... -> package versions and where ocpp is imported from
  "$@" - <<'PY'
import importlib.metadata as m, importlib.util
row = []
for name in ("ocpp", "websockets", "paho-mqtt", "pyserial", "pytz", "RPi.GPIO", "pytest"):
    try:
        row.append("%s=%s" % (name, m.version(name)))
    except Exception:
        row.append("%s=-" % name)
spec = importlib.util.find_spec("ocpp")
print("  ".join(row) + "\n          ocpp imported from " + (spec.origin.rsplit("/ocpp/", 1)[0] if spec and spec.origin else "-"))
PY
}

cp_urls() { # CP.py -> CSMS URLs written in it (used when no dual_csms_config.json is enabled); comments skipped
  grep -nE "[\"']wss?://" "$1" 2>/dev/null | grep -vE '^[0-9]+:[[:space:]]*#' \
    | sed -E "s/^([0-9]+):.*([\"'])(wss?:\/\/[^\"' ]+).*/line \1: \3/" | head -5
}

IDENT_KEYS="OCPP_BROKER OCPP_PORT OCPPSTATION EV_STOP_V2G_TOPIC"
cp_ident() { # CP.py -> "KEY=value" for the per-site constants that R3.1 would replace
  local k
  for k in $IDENT_KEYS; do
    echo "$k=$(sed -nE "s/^$k[[:space:]]*=[[:space:]]*[\"']?([^\"'#[:space:]]*)[\"']?.*/\1/p" "$1" 2>/dev/null | head -1)"
  done
}

config_urls() { # dual_csms_config.json -> its enabled server URLs, space separated
  "$PY" -c "import json,sys; c=json.load(open(sys.argv[1],encoding='utf-8-sig')); print(' '.join(s['url'] for s in c.get('servers',[]) if s.get('enabled',True)))" "$1" 2>/dev/null
}

deps_check() { # python... -> new CP.py's imports resolve after the copy. Reads source only; runs nothing from it
  "$@" - "$PKG_DIR" "$CP_DIR" $FILES <<'PY'
import ast, importlib.util, sys
from pathlib import Path
package, target, installed = Path(sys.argv[1]), Path(sys.argv[2]), set(sys.argv[3:])

def top_names(tree):  # names a module defines at top level, including inside if/try/with blocks
    names, todo = set(), list(tree.body)
    while todo:
        item = todo.pop()
        if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.add(item.name)
        elif isinstance(item, (ast.Assign, ast.AnnAssign, ast.AugAssign)):
            for t in (item.targets if isinstance(item, ast.Assign) else [item.target]):
                names.update(n.id for n in ast.walk(t) if isinstance(n, ast.Name))
        elif isinstance(item, (ast.Import, ast.ImportFrom)):
            names.update((a.asname or a.name).split(".")[0] for a in item.names)
        elif isinstance(item, (ast.If, ast.Try, ast.With)):
            for field in ("body", "orelse", "finalbody", "handlers"):
                for child in getattr(item, field, []) or []:
                    todo.extend(child.body if isinstance(child, ast.ExceptHandler) else [child])
    return names

problems, outside = [], set()
for rel in ["CP.py"] + sorted(f for f in installed if f.startswith("helpf/") and f.endswith(".py")):
    tree = ast.parse((package / rel).read_text(encoding="utf-8-sig"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            outside.update(a.name.split(".")[0] for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            outside.add(node.module.split(".")[0])
            if rel != "CP.py" or not node.module.startswith("helpf."):
                continue
            path_rel = node.module.replace(".", "/") + ".py"
            if path_rel in installed:
                continue
            path = target / path_rel
            if not path.is_file():
                problems.append(path_rel + " is missing on this Edgebox"); continue
            try:
                names = top_names(ast.parse(path.read_text(encoding="utf-8-sig")))
            except SyntaxError as error:
                problems.append("%s does not parse: %s" % (path_rel, error)); continue
            missing = [a.name for a in node.names if a.name != "*" and a.name not in names]
            if missing:
                problems.append("%s here has no %s" % (path_rel, ", ".join(missing)))
for name in sorted(outside - set(sys.stdlib_module_names) - {"helpf", "__future__"}):
    if importlib.util.find_spec(name) is None:
        problems.append("Python package %s is not installed for this user" % name)
for problem in problems:
    print("  dependency check: " + problem)
sys.exit(1 if problems else 0)
PY
}

# CP.py text without the per-site constant lines, per release. Install keeps this Edgebox's values,
# so the file hash of an installed CP.py differs from the package.
known_body() {
  case "$1" in
    f42576d4096656608ec47a85d0c98b05cb3e4851918d17bf533e9ebc01a913fb) echo "R3/R3.1" ;;
    3f378f010d9eabc26fed02c776a558e201c47cc911e044c991be92251a3edd7a) echo "R3.1.1" ;;
    e75749cb3732d57af400b753f37629204a982897272a7ec869cd316e98402283) echo "R3.1.2/R3.1.3" ;;
    aa47dea952d1c259eea5873c540aecb576fe6b991494cc38d790255173e7e01c) echo "R3.1.4/R3.1.5" ;;
    2a20f8a7dc86c3c5f1d82feea06f01eba8c64e58b1e1b530bcdc63b657edd889) echo "R3.1.6" ;;
    *) echo "UNKNOWN" ;;
  esac
}

cp_body_sha() { # CP.py -> sha256 of its text without the per-site constant lines
  "$PY" - "$1" $IDENT_KEYS <<'PY'
import hashlib, re, sys
path, keys = sys.argv[1], sys.argv[2:]
skip = re.compile(r"^(%s)\s*=" % "|".join(map(re.escape, keys)))
lines = [l for l in open(path, encoding="utf-8-sig", newline="").read().splitlines() if not skip.match(l)]
print(hashlib.sha256("\n".join(lines).encode("utf-8")).hexdigest())
PY
}

cp_label() { # folder -> known CP.py label, also for a release whose site constants were kept
  local label
  label=$(known CP.py "$(sha "$1/CP.py")")
  if [ "$label" = UNKNOWN ]; then
    label=$(known_body "$(cp_body_sha "$1/CP.py")")
    [ "$label" = UNKNOWN ] || label="$label with this Edgebox's site constants"
  fi
  echo "$label"
}

set_admins() { # config file, "A,B", check|write, [sudo] -> validates ADMIN_CSMS against the enabled servers
  ${4:-} "$PY" - "$1" "$2" "$3" <<'PY'
import json, sys
path, wanted, mode = sys.argv[1], sys.argv[2], sys.argv[3]
names = [n.strip() for n in wanted.replace(" ", ",").split(",") if n.strip()]
config = json.load(open(path, encoding="utf-8-sig"))
enabled = [s.get("name") for s in config.get("servers", []) if s.get("enabled", True)]
if not names or len(set(names)) != len(names) or any(n not in enabled for n in names):
    sys.exit("  ADMIN_CSMS=%s must list enabled CSMS names from %s" % (wanted, enabled))
value = names if len(names) > 1 else names[0]
if mode == "write":
    config["admin_csms"] = value
    open(path, "w", encoding="utf-8").write(json.dumps(config, ensure_ascii=False, indent=2) + "\n")
print(", ".join(names))
PY
}

servers_of() { # dual_csms_config.json -> one line per CSMS
  "$PY" - "$1" <<'PY'
import json, sys
try:
    config = json.load(open(sys.argv[1], encoding="utf-8-sig"))
except Exception as error:
    print("       (cannot read %s: %s)" % (sys.argv[1], error)); sys.exit(0)
admins = config.get("admin_csms")
admins = ", ".join(admins) if isinstance(admins, list) else admins
print("       dual CSMS %s, admin %s" % ("enabled" if config.get("enabled", False) else
      "DISABLED (CP.py uses its single built-in URL)", admins))
for server in config.get("servers", []):
    print("       %s  %s%s" % (server.get("name"), server.get("url"), "" if server.get("enabled", True) else "  (disabled)"))
PY
}

service_python() {
  local pid exe
  for pid in $(cp_pids); do
    exe=$(readlink "/proc/$pid/exe" 2>/dev/null)
    [ -z "$exe" ] && exe=$(tr '\0' '\n' < "/proc/$pid/cmdline" | head -1)
    case "$exe" in */*) ;; *) exe=$(command -v "$exe") ;; esac
    [ -x "$exe" ] && { echo "$exe"; return; }
  done
  command -v python3
}

units() {
  grep -ls 'CP\.py' /etc/systemd/system/*.service /etc/systemd/system/*/*.service \
    /lib/systemd/system/*.service 2>/dev/null | xargs -r -n1 basename | sort -u
}

cp_dirs() {
  find "$HOME" -maxdepth 4 -type f -name CP.py -not -path '*/r31_stage*' -not -path '*_backup_before_R31*' \
    -not -path '*_failed_R31*' -not -path '*/.cache/*' 2>/dev/null | xargs -r -n1 dirname
}

head_state() { # folder connector
  local s t
  s=$(head -c 40 "$1/iCpStateCss$2.txt" 2>/dev/null | tr -d '\r\n'); t=$(head -c 40 "$1/translogs$2.txt" 2>/dev/null | tr -d '\r\n')
  echo "status=${s:-?} transaction=${t:-none}"
}

head_busy() { # folder connector -> 0 if busy
  local s t
  s=$(head -c 40 "$1/iCpStateCss$2.txt" 2>/dev/null); t=$(head -c 40 "$1/translogs$2.txt" 2>/dev/null | tr -dc '0-9')
  case "$s" in *Charging*|*Suspended*|*Preparing*|*Finishing*) return 0 ;; esac
  [ -n "$t" ] && [ "$t" != 0 ] && return 0
  return 1
}

verify_stage() {
  "$PY" - "$PKG_DIR" <<'PY'
import hashlib, json, sys
from pathlib import Path
root = Path(sys.argv[1])
manifest = json.loads((root / "MANIFEST_SHA256.json").read_text(encoding="utf-8"))
bad = [n for n, row in manifest.items() if hashlib.sha256((root / n).read_bytes()).hexdigest() != row["sha256"]]
print("  package files:", "OK (%d files)" % len(manifest) if not bad else "CHANGED: " + " ".join(bad))
sys.exit(1 if bad else 0)
PY
}

check() {
  PY=$(service_python)
  echo "=== R3.1.6 CHECK (read-only)  $(date '+%F %T')  $(hostname)"
  echo "python: $PY  ($("$PY" --version 2>&1))"
  UNIT=${UNIT:-$(units | head -1)}; SVC_USER=$(svc_user)
  echo "packages as $(id -un): $(pkg_versions "$PY")"
  if [ "$SVC_USER" != "$(id -un)" ]; then
    if sudo -n true 2>/dev/null; then echo "packages as $SVC_USER (the user of ${UNIT:-the service}): $(pkg_versions sudo -n -H -u "$SVC_USER" "$PY")"
    else echo "packages as $SVC_USER (the user of ${UNIT:-the service}): sudo asks for a password - run 'sudo -v' and check again"; fi
  fi
  echo "          (R3.1 tested with ocpp=2.1.0 websockets=15.0.1 on the PC, ocpp=1.0.0 websockets=17.1 on this Edgebox)"
  echo "--- running CP.py:"
  local pid found=0
  for pid in $(cp_pids); do
    found=1
    echo "  pid $pid  user=$(ps -o user= -p "$pid" | xargs)  up=$(ps -o etime= -p "$pid" | xargs)  cwd=$(readlink "/proc/$pid/cwd" 2>/dev/null || echo '?')"
    echo "  cmd: $(tr '\0' ' ' < "/proc/$pid/cmdline")"
  done
  [ "$found" = 1 ] || echo "  (none)"
  echo "--- systemd services that start CP.py:"
  local u any=0
  for u in $(units); do
    any=1
    echo "  $u  active=$(systemctl is-active "$u" 2>/dev/null)  enabled=$(systemctl is-enabled "$u" 2>/dev/null)"
    systemctl cat "$u" 2>/dev/null | grep -E '^(ExecStart|WorkingDirectory|User|Environment|Restart)=' | sed 's/^/     /'
  done
  [ "$any" = 1 ] || echo "  (none found)"
  echo "--- other autostart entries with CP.py:"
  { crontab -l 2>/dev/null; cat /etc/rc.local ~/.config/autostart/*.desktop ~/.config/lxsession/*/autostart \
      /etc/xdg/lxsession/*/autostart ~/.config/labwc/autostart ~/.config/wayfire.ini 2>/dev/null; } | grep 'CP\.py' | sed 's/^/  /'
  echo "--- CP.py folders:"
  local d f s label
  while IFS= read -r d; do
    [ -n "$d" ] || continue
    echo "  $d   (owner $(stat -c %U "$d"), $(du -sh "$d" 2>/dev/null | cut -f1))"
    for f in CP.py helpf/plc_function.py helpf/v2g_datatransfer.py; do
      if [ -f "$d/$f" ]; then
        s=$(sha "$d/$f"); label=$(known "$f" "$s"); [ "$f" = CP.py ] && label=$(cp_label "$d")
        echo "     $f: $label  ${s:0:12}  $(date -r "$d/$f" '+%F %R')"
      else echo "     $f: MISSING"; fi
    done
    cp_urls "$d/CP.py" | sed 's/^/     CSMS in CP.py, /'
    echo "     site constants: $(cp_ident "$d/CP.py" | xargs)"
    for f in helpf/dual_csms.py dual_csms_config.json ocpp_services_config.json; do
      [ -e "$d/$f" ] && echo "     $f: present (an earlier dual-CSMS install exists)"
    done
    [ -f "$d/dual_csms_config.json" ] && servers_of "$d/dual_csms_config.json"
    echo "     head 1: $(head_state "$d" 1)"
    echo "     head 2: $(head_state "$d" 2)"
    echo "     newest log: $(ls -t "$d"/*.log 2>/dev/null | head -1)"
  done < <(cp_dirs)
  echo "--- free space: $(df -h "$HOME" | awk 'NR==2{print $4" free of "$2}')"
  if "$PY" -c "import urllib.request; urllib.request.urlopen('https://pypi.org/simple/pytest/', timeout=6)" 2>/dev/null
  then echo "--- internet (pypi): OK"; else echo "--- internet (pypi): not reachable"; fi
  if [ -f "$HERE/$PKG_NAME" ]; then
    if [ "$(sha "$HERE/$PKG_NAME")" = "$PKG_SHA" ]; then echo "--- package: OK"; else echo "--- package: SHA256 MISMATCH, upload again"; fi
  else echo "--- package: not found next to this script"; fi
  echo "=== END CHECK (nothing was changed)"
}

stage() {
  PY=${PYTHON:-$(service_python)}
  echo "=== R3.1.6 STAGE  $(date '+%F %T')  (the running charger program is not touched)"
  [ -f "$HERE/$PKG_NAME" ] || die "upload $PKG_NAME next to this script first"
  [ "$(sha "$HERE/$PKG_NAME")" = "$PKG_SHA" ] || die "package SHA256 mismatch, upload again"
  echo "python: $PY  ($("$PY" --version 2>&1))"
  [ -d "$STAGE" ] && mv "$STAGE" "${STAGE}_old_$(date +%Y%m%d_%H%M%S)"
  mkdir -p "$STAGE" && "$PY" -m zipfile -e "$HERE/$PKG_NAME" "$STAGE" || die "unzip failed"
  verify_stage || die "package content does not match its manifest"
  "$PY" -m compileall -q "$PKG_DIR" >/dev/null || die "compile failed with this Python"
  echo "  compile: OK"
  local T="$STAGE/.testlibs" rc
  "$PY" -m pip --version >/dev/null 2>&1 || die "pip is missing for $PY (sudo apt install python3-pip); compile check passed"
  "$PY" -m pip install -q --disable-pip-version-check --target "$T" "pytest==9.2.0" "pytest-asyncio==1.4.1.post1" \
    > "$STAGE/pip.log" 2>&1 || { tail -5 "$STAGE/pip.log"; die "could not install pytest into $T"; }
  UNIT=${UNIT:-$(units | head -1)}; SVC_USER=$(svc_user)
  run_tests; rc=$?
  echo "=== END STAGE  pytest exit code $rc  (full log: $STAGE/pytest.log)"
}

run_tests() { # staged offline tests as $SVC_USER (the user the charger service runs as); marks a pass
  local rc
  echo "  packages as $SVC_USER (the user of ${UNIT:-the service}): $(pkg_versions as_svc "$PY")"
  echo "  running 585 offline tests as $SVC_USER (about 1-2 minutes, uses only 127.0.0.1)..."
  ( cd "$PKG_DIR" && as_svc env PYTHONPATH="$STAGE/.testlibs" PYTHONDONTWRITEBYTECODE=1 timeout 1800 "$PY" -m pytest tests -q \
      --tb=line -p no:cacheprovider > "$STAGE/pytest.log" 2>&1 ); rc=$?
  tail -12 "$STAGE/pytest.log"
  [ "$rc" = 0 ] && echo "$PKG_SHA $PY" > "$STAGE/.tests_passed_as_$SVC_USER"
  return "$rc"
}

restore_backup() { # uses CP_DIR, BK, SUDO, UNIT, TS from install_r31; keeps the failed copy aside
  $SUDO mv "$CP_DIR" "${CP_DIR}_failed_R31_$TS" && $SUDO cp -a "$BK" "$CP_DIR" && sudo systemctl start "$UNIT"
}

stop_service() {
  sudo systemctl stop "$UNIT" || return 1
  local i
  for i in $(seq 1 40); do [ -z "$(cp_pids)" ] && return 0; sleep 1; done
  return 1
}

install_r31() {
  # sudo for every copy: the service runs as root and leaves root-only files in the folder
  # (for example .v2g_bridge/state.sqlite3), which pi cannot read for the backup.
  local CP_DIR=${1:-} f label ok TS BK SUDO=sudo owner
  [ -n "$CP_DIR" ] && [ -f "$CP_DIR/CP.py" ] || die "usage: RATED_W=<nameplate W> bash $0 install <folder with CP.py>"
  CP_DIR=$(cd "$CP_DIR" && pwd)
  [ -n "${RATED_W:-}" ] || die "set RATED_W to the charger nameplate power in W, e.g. RATED_W=120000"
  case "$RATED_W" in *[!0-9]*) die "RATED_W must be a number of watts" ;; esac
  [ "$RATED_W" -ge 10000 ] && [ "$RATED_W" -le 400000 ] || die "RATED_W=$RATED_W looks wrong (watts, e.g. 120000 for 120 kW)"
  RATED_A=${RATED_A:-500}
  case "$RATED_A" in ''|*[!0-9]*) die "RATED_A must be a number of amperes" ;; esac
  [ "$RATED_A" -ge 10 ] && [ "$RATED_A" -le 1000 ] || die "RATED_A=$RATED_A looks wrong (amperes per head)"
  PY=${PYTHON:-$(service_python)}
  [ -f "$PKG_DIR/MANIFEST_SHA256.json" ] || die "run: bash $0 stage   first"
  echo "=== R3.1.6 INSTALL into $CP_DIR  $(date '+%F %T')"
  # The user decided on 2026-11-09 that this factory test Edgebox may have any version replaced:
  # unknown versions are reported, not refused. The full folder backup below keeps them.
  for f in CP.py helpf/plc_function.py helpf/v2g_datatransfer.py; do
    label=$(known "$f" "$(sha "$CP_DIR/$f")"); [ "$f" = CP.py ] && label=$(cp_label "$CP_DIR")
    [ "$label" = UNKNOWN ] && label="not a known version - will be replaced (kept in the backup)"
    echo "  current $f: $label"
  done
  verify_stage || die "staged files changed - run stage again"
  head_busy "$CP_DIR" 1 && die "head 1 is busy ($(head_state "$CP_DIR" 1)) - wait until both heads are idle"
  head_busy "$CP_DIR" 2 && die "head 2 is busy ($(head_state "$CP_DIR" 2)) - wait until both heads are idle"
  UNIT=${UNIT:-$(units | head -1)}
  [ -n "$UNIT" ] || die "no systemd service starts CP.py - tell Claude how CP.py is started"
  SVC_USER=$(svc_user)
  echo "  service: $UNIT (runs as $SVC_USER)   python: $PY"
  echo "  nameplate: RATED_W=$RATED_W  RATED_A=$RATED_A"
  local CFG="$PKG_DIR/dual_csms_config.json" now_url same=1
  [ -f "$CP_DIR/dual_csms_config.json" ] && CFG="$CP_DIR/dual_csms_config.json"
  now_url=$(cp_urls "$CP_DIR/CP.py" | head -1 | sed 's/^line [0-9]*: //')
  echo "  CSMS now, written in $CP_DIR/CP.py:"
  cp_urls "$CP_DIR/CP.py" | sed 's/^/       /'
  echo "  CSMS after install ($CFG):"; servers_of "$CFG"
  if [ -n "${ADMIN_CSMS:-}" ]; then
    f=$(set_admins "$CFG" "$ADMIN_CSMS" check) || die "nothing was changed"
    echo "  admin CSMS after install: $f (ADMIN_CSMS)"
  fi
  case " $(config_urls "$CFG") " in *" ${now_url:-none} "*) ;; *) same=0 ;; esac
  # The site constants (MQTT station name and broker, V2G topic) identify this Edgebox, not the
  # dual-CSMS feature, so by default the new CP.py keeps this Edgebox's values. KEEP_SITE=0 uses R3.1's.
  KEEP_SITE=${KEEP_SITE:-1}
  echo "  site constants now:           $(cp_ident "$CP_DIR/CP.py" | xargs)"
  if [ "$KEEP_SITE" = 1 ]; then
    echo "  site constants after install: the same (kept). R3.1 alone would set: $(cp_ident "$PKG_DIR/CP.py" | xargs)"
  else
    echo "  site constants after install: $(cp_ident "$PKG_DIR/CP.py" | xargs)   (KEEP_SITE=0)"
    [ "$(cp_ident "$CP_DIR/CP.py")" = "$(cp_ident "$PKG_DIR/CP.py")" ] || same=0
  fi
  [ "$same" = 0 ] && echo "  NOTE: the CSMS identity changes with R3.1 (compare the lines above)."
  # Same check as after the copy, but run from the staged package before anything stops:
  # it fails when this Python's ocpp/websockets cannot run R3.1 or a session needs attention.
  local pre
  if ! pre=$(cd "$PKG_DIR" && as_svc env PYTHONDONTWRITEBYTECODE=1 "$PY" dual_csms_check.py --state-dir "$CP_DIR" 2>&1); then
    echo "$pre" | grep -E '"(result|error|dependencies|ocpp|websockets)"|attention|Session' | sed 's/^/  /'
    die "R3.1 cannot run with $PY as $SVC_USER as it is now - nothing was changed (send this to Claude)"
  fi
  echo "  pre-check with $PY as $SVC_USER: OK ($(echo "$pre" | grep -oE '"(ocpp|websockets)": "[^"]*"' | tr -d '"' | xargs))"
  deps_check as_svc "$PY" || die "the new CP.py needs something this Edgebox does not have (above) - nothing was changed (send this to Claude)"
  echo "  dependency check (helpf modules and Python packages as $SVC_USER): OK"
  if [ "$(cat "$STAGE/.tests_passed_as_$SVC_USER" 2>/dev/null)" = "$PKG_SHA $PY" ]; then
    echo "  offline tests: passed earlier as $SVC_USER"
  else
    [ -d "$STAGE/.testlibs" ] || die "run: bash $0 stage   first"
    run_tests || die "offline tests failed as $SVC_USER - nothing was changed (send this to Claude)"
  fi
  read -r -p "  No car connected and no open transaction on A or B? Type YES to stop $UNIT and install: " ok
  [ "$ok" = YES ] || die "cancelled, nothing changed"
  TS=$(date +%Y%m%d_%H%M%S); BK="${CP_DIR}_backup_before_R31_$TS"
  stop_service || { sudo systemctl start "$UNIT"; die "CP.py did not stop; service started again, nothing changed"; }
  echo "  service stopped"
  if ! $SUDO cp -a "$CP_DIR" "$BK"; then
    $SUDO mv "$BK" "${BK}_INCOMPLETE" 2>/dev/null
    sudo systemctl start "$UNIT"; die "backup failed; service started again, nothing changed"
  fi
  $SUDO touch "${BK}.complete"
  echo "  backup: $BK"
  owner=$(stat -c %U:%G "$CP_DIR")
  for f in $FILES; do
    case "$f" in dual_csms_config.json|ocpp_services_config.json)
      if [ -f "$CP_DIR/$f" ]; then echo "  kept existing $f"; continue; fi ;;
    esac
    $SUDO mkdir -p "$(dirname "$CP_DIR/$f")" && $SUDO cp "$PKG_DIR/$f" "$CP_DIR/$f" && $SUDO chown "$owner" "$CP_DIR/$f" \
      || { echo "  copy of $f failed - putting the backup back"; restore_backup; die "previous version restored"; }
  done
  echo "  copied the R3.1 files"
  if [ "$KEEP_SITE" = 1 ]; then
    $SUDO "$PY" - "$BK/CP.py" "$CP_DIR/CP.py" $IDENT_KEYS <<'PY' \
      || { echo "  could not keep the site constants - putting the backup back"; restore_backup; die "previous version restored"; }
import re, sys
old_path, new_path, keys = sys.argv[1], sys.argv[2], sys.argv[3:]
# newline="" keeps every byte of the R3.1 file (it uses CRLF) except the replaced lines
old = open(old_path, encoding="utf-8-sig", newline="").read().splitlines(keepends=True)
new = open(new_path, encoding="utf-8", newline="").read().splitlines(keepends=True)
for key in keys:
    pattern = re.compile(r"^%s\s*=" % re.escape(key))
    line = next((l for l in old if pattern.match(l)), None)
    index = next((i for i, l in enumerate(new) if pattern.match(l)), None)
    if line is None or index is None or new[index].strip() == line.strip():
        continue
    ending = new[index][len(new[index].rstrip("\r\n")):] or "\n"
    new[index] = line.rstrip("\r\n") + ending
    print("  kept this Edgebox's " + line.strip())
open(new_path, "w", encoding="utf-8", newline="").write("".join(new))
PY
  fi
  $SUDO "$PY" - "$CP_DIR/ocpp_services_config.json" "$RATED_W" "$RATED_A" <<'PY' \
      || { echo "  could not write ocpp_services_config.json - putting the backup back"; restore_backup; die "previous version restored"; }
import json, sys
path, watts, amps = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
config = json.load(open(path, encoding="utf-8"))
config["rated_power_w"] = config["station_rated_power_w"] = watts
config["rated_current_a"] = amps
open(path, "w", encoding="utf-8").write(json.dumps(config, indent=2) + "\n")
print("  ocpp_services_config.json: station %d W, %d A per head" % (watts, amps))
PY
  if [ -n "${ADMIN_CSMS:-}" ]; then
    f=$(set_admins "$CP_DIR/dual_csms_config.json" "$ADMIN_CSMS" write "$SUDO") \
      || { echo "  could not set admin_csms - putting the backup back"; restore_backup; die "previous version restored"; }
    echo "  dual_csms_config.json: admin_csms = $f"
  fi
  echo "  dual_csms_config.json servers:"
  "$PY" -c "import json;[print('     ', s['name'], s['url']) for s in json.load(open('$CP_DIR/dual_csms_config.json'))['servers']]"
  if ! ( cd "$CP_DIR" && PYTHONDONTWRITEBYTECODE=1 "$PY" -c "import ast,sys; [ast.parse(open(f,encoding='utf-8-sig').read(), f) for f in sys.argv[1:]]" \
         CP.py helpf/dual_csms.py helpf/ocpp_runtime.py helpf/ocpp_services.py helpf/ocpp_smart_charging.py \
         helpf/plc_function.py helpf/v2g_datatransfer.py && as_svc env PYTHONDONTWRITEBYTECODE=1 "$PY" dual_csms_check.py --state-dir . > /dev/null ); then
    echo "  check failed - putting the backup back"; restore_backup; die "previous version restored"
  fi
  echo "  compile + config check: OK"
  sudo systemctl start "$UNIT" || die "service did not start - run: bash $0 rollback $BK $CP_DIR"
  echo "  service started, waiting 40 s for A and B..."
  sleep 40
  logs "$CP_DIR"
  echo "=== END INSTALL  backup: $BK"
  echo "    rollback if needed: bash $0 rollback $BK $CP_DIR"
}

logs() {
  local CP_DIR=${1:-} L
  [ -n "$CP_DIR" ] || CP_DIR=$(cp_dirs | head -1)
  L=$(ls -t "$CP_DIR"/*.log 2>/dev/null | head -1)
  [ -n "$L" ] || die "no log file in $CP_DIR"
  echo "--- $L"
  # Registration and connection lines of A and B, errors, and BootNotification sends;
  # StatusNotification lines are left out (their "NoError" field used to hide the B lines).
  grep -E '\[(A|B|DUAL)\]|Traceback|CRITICAL| - ERROR - |"BootNotification"' "$L" | grep -v '"StatusNotification"' \
    | cut -c1-220 | tail -20
  echo "--- service: $(systemctl is-active "${UNIT:-$(units | head -1)}" 2>/dev/null)   CP.py pids: $(cp_pids | xargs)"
}

diag() { # read-only snapshot for field diagnosis; prints no idTag
  local CP_DIR=${1:-$HOME/ocpp} h f line L
  [ -f "$CP_DIR/CP.py" ] || die "usage: bash $0 diag <folder with CP.py>"
  PY=${PYTHON:-$(service_python)}; UNIT=${UNIT:-$(units | head -1)}
  echo "=== DIAG $(date '+%F %T')  $CP_DIR  service: $(systemctl is-active "$UNIT" 2>/dev/null)  pids: $(cp_pids | xargs)"
  echo "  CP.py: $(cp_label "$CP_DIR")"
  for h in 1 2; do
    line=""
    for f in iCpStateCss$h translogs$h session_finishing_c$h local_status_session_c$h preparing_timeout_c$h \
             remote_start_received_c$h myremotestart_c$h rfid_start_received_c$h start_transaction_SW$h start_trans_SW_c$h; do
      line="$line ${f}=$(head -c 20 "$CP_DIR/$f.txt" 2>/dev/null | tr -d '\r\n')"
    done
    echo "  head $h:$line"
  done
  sudo "$PY" - "$CP_DIR/dual_csms_state.sqlite3" <<'PY'
import sqlite3, sys
try:
    db = sqlite3.connect("file:%s?mode=ro" % sys.argv[1], uri=True)
    rows = db.execute("SELECT local_id, connector, owner, state, remote_id FROM sessions ORDER BY local_id DESC LIMIT 6").fetchall()
    print("  sessions (newest first; local_id, connector, owner, state, remote_id):", rows or "none")
    print("  outbox not delivered yet (pending, sending or failed):", db.execute("SELECT server, action, state, count(*) FROM outbox WHERE state != 'delivered' GROUP BY 1,2,3").fetchall() or "none",
          "  delivered:", db.execute("SELECT count(*) FROM outbox WHERE state = 'delivered'").fetchone()[0])
    try:
        owners = db.execute("SELECT owner, count(*) FROM auth_owners GROUP BY 1 ORDER BY 1").fetchall()
        print("  cards remembered by the CSMS that accepted them (stored as hashes):",
              ", ".join("%s=%d" % tuple(row) for row in owners) or "none yet")
    except sqlite3.OperationalError:
        print("  cards remembered: no auth_owners table (before R3.1.4)")
except Exception as error:
    print("  sessions: cannot read (%s)" % type(error).__name__)
PY
  echo "  limits sent to the PLC (last 4):"
  grep '"limit_command_published"' "$CP_DIR/ocpp_runtime_events.jsonl" 2>/dev/null | tail -4 | cut -c1-140 | sed 's/^/    /'
  L=$(ls -t "$CP_DIR"/*.log 2>/dev/null | head -1)
  echo "  log: $L"
  for f in A B; do
    echo "    CSMS $f: $(grep -E "\[$f\] (OCPP registration|BootNotification|connection unavailable)" "$L" 2>/dev/null | tail -1 | sed -E 's/^[0-9-]+ ([0-9:]+),[0-9]+ - [A-Za-z_]+ - [A-Z]+ - /\1 /' | cut -c1-100)   last reply at: $(grep -E " - $f: receive message \[3," "$L" 2>/dev/null | tail -1 | cut -c12-19)"
  done
  echo "  card checks at the charger (last 6, no idTag):"
  grep -E '\[(A|B)\] Authorize |\[DUAL\] local start on connector' "$L" 2>/dev/null | grep -v -i idtag \
    | sed -E 's/^[0-9-]+ ([0-9:]+),[0-9]+ - [A-Za-z_]+ - [A-Z]+ - /\1 /' | cut -c1-110 | tail -6 | sed 's/^/    /'
  echo "  session owners in the statuses sent to both CSMS (last 4, from this log):"
  grep -oE 'A: send .*"StatusNotification",\{"connectorId":[12],.*"info":"Session of CSMS [^"]*"' "$L" 2>/dev/null \
    | sed -E 's/^A: send \[2,"[^"]*","StatusNotification",\{"connectorId":([12]),.*"status":"([A-Za-z]+)".*"info":"([^"]*)".*/    C\1 \2: \3/' | tail -4
  echo "  last StatusNotification per connector (as sent to A):"
  for h in 0 1 2; do
    line=$(grep -E "A: send \[2,\"[^\"]*\",\"StatusNotification\",\{\"connectorId\":$h," "$L" 2>/dev/null | tail -1)
    echo "    C$h: $(echo "$line" | grep -oE '"(status|errorCode|info|vendorErrorCode)":"[^"]*"' | cut -d'"' -f4 | xargs)   at $(echo "$line" | cut -c12-19)"
  done
  f=$(grep -oE 'unmapped/non-sendable issue ignored code=[A-Za-z0-9_]+' "$L" 2>/dev/null | sed 's/.*code=//' | sort | uniq -c | xargs)
  [ -n "$f" ] && echo "  Vector issues ignored as unmapped (count code): $f   last at $(grep 'unmapped/non-sendable issue ignored' "$L" | tail -1 | cut -c12-19)"
  echo "  start latch released, R3.1.6 (last 8 in this log):"
  f=$(grep -a '\[START-LATCH\]' "$L" 2>/dev/null | cut -c1-210 | tail -8)
  if [ -n "$f" ]; then echo "$f" | sed 's/^/    /'; else echo "    none in this log"; fi
  echo "  RemoteStart refused (last 6 in this log):"
  f=$(grep -a -E 'RemoteStart C[0-9]+ rejected:|\[REMOTE-START[^]]*\] reject' "$L" 2>/dev/null | grep -v -i idtag | cut -c1-200 | tail -6)
  if [ -n "$f" ]; then echo "$f" | sed 's/^/    /'; else echo "    none in this log"; fi
  echo "  refusals, latches and faults (last 15, unmapped Vector lines left out):"
  grep -E 'rejected:|\[REMOTE-START|\[START-LATCH|\[SESSION-FINISHING|\[PREPARING-TIMEOUT|\[VECTOR-FAULT|\[EV-STOP|"status":"(Faulted|Finishing|Preparing|Charging)"' "$L" 2>/dev/null \
    | grep -v -e idTag -e 'unmapped/non-sendable issue ignored' | cut -c1-170 | tail -15 | sed 's/^/    /'
  echo "=== END DIAG (nothing was changed)"
}

snapshot_zip() { # whole folder -> one zip next to it; SQLite files as consistent copies
  local CP_DIR=${1:-$HOME/ocpp} OUT
  [ -d "$CP_DIR" ] || die "usage: bash $0 zip [folder]   (default ~/ocpp)"
  CP_DIR=$(cd "$CP_DIR" && pwd)
  PY=${PYTHON:-$(service_python)}
  OUT="$(dirname "$CP_DIR")/$(basename "$CP_DIR")_snapshot_$(date +%Y%m%d_%H%M%S).zip"
  echo "=== ZIP $(date '+%F %T')  $CP_DIR -> $OUT  (the running charger program is not touched)"
  echo "  free space: $(df -h "$(dirname "$CP_DIR")" | awk 'NR==2{print $4}')   folder size: $(sudo du -sh "$CP_DIR" 2>/dev/null | cut -f1)"
  # sudo: the service runs as root and leaves root-only files in the folder.
  sudo "$PY" - "$CP_DIR" "$OUT" <<'PY' || { sudo rm -f "$OUT" "$OUT.part"; die "zip failed - nothing was changed in $CP_DIR"; }
import hashlib, os, sqlite3, stat, sys, tempfile, zipfile
src, out = sys.argv[1], sys.argv[2]
top = os.path.basename(src)
snap, skipped, count, total = [], [], 0, 0

def is_sqlite(path):
    try:
        with open(path, "rb") as f:
            return f.read(16) == b"SQLite format 3\x00"
    except OSError:
        return False

dbs = set()
for root, dirs, files in os.walk(src):
    dirs.sort()
    for name in sorted(files):
        path = os.path.join(root, name)
        if os.path.isfile(path) and is_sqlite(path):
            dbs.add(path)
tmp = tempfile.mkdtemp(prefix="zipsnap_")
with zipfile.ZipFile(out + ".part", "w", zipfile.ZIP_DEFLATED, allowZip64=True) as z:
    for root, dirs, files in os.walk(src):
        dirs.sort()
        rel_root = os.path.relpath(root, src)
        arc_root = top if rel_root == "." else top + "/" + rel_root.replace(os.sep, "/")
        z.write(root, arc_root + "/")
        for name in sorted(files):
            path = os.path.join(root, name)
            arc = arc_root + "/" + name
            # live -wal/-shm files belong to the running database; the consistent copy replaces them
            if any(path == db + "-wal" or path == db + "-shm" or path == db + "-journal" for db in dbs):
                continue
            try:
                mode = os.stat(path).st_mode
            except OSError as error:
                skipped.append("%s (%s)" % (arc, type(error).__name__))
                continue
            if not stat.S_ISREG(mode):
                skipped.append("%s (not a regular file)" % arc)
                continue
            try:
                if path in dbs:
                    copy = os.path.join(tmp, "db_%d" % len(snap))
                    with sqlite3.connect("file:%s?mode=ro" % path, uri=True, timeout=10) as source, \
                            sqlite3.connect(copy) as target:
                        source.backup(target)
                    source.close()
                    target.close()
                    z.write(copy, arc)
                    os.remove(copy)
                    snap.append(arc)
                else:
                    z.write(path, arc)
                count += 1
                total += os.path.getsize(path)
            except (OSError, sqlite3.Error) as error:
                skipped.append("%s (%s)" % (arc, type(error).__name__))
os.rmdir(tmp)
with zipfile.ZipFile(out + ".part") as z:
    bad = z.testzip()
if bad:
    sys.exit("  zip test failed at %s" % bad)
os.replace(out + ".part", out)
digest = hashlib.sha256(open(out, "rb").read()).hexdigest()
print("  files: %d  (%.1f MB before compression)" % (count, total / 1e6))
print("  SQLite databases copied consistently: %s" % (", ".join(snap) or "none"))
if skipped:
    print("  left out: %s" % "; ".join(skipped[:10]) + (" ..." if len(skipped) > 10 else ""))
print("  zip: %s  %.2f MB" % (out, os.path.getsize(out) / 1e6))
print("  sha256: %s" % digest)
PY
  sudo chown "$(id -un):$(id -gn)" "$OUT" && chmod 600 "$OUT"
  echo "=== END ZIP  (it contains logs with card numbers - keep it private)"
}

rollback() {
  local BK=${1:-} CP_DIR=${2:-} ok FAILED SUDO=sudo
  [ -n "$BK" ] && [ -f "$BK/CP.py" ] && [ -n "$CP_DIR" ] && [ -d "$CP_DIR" ] || die "usage: bash $0 rollback <backup folder> <folder with CP.py>"
  UNIT=${UNIT:-$(units | head -1)}; [ -n "$UNIT" ] || die "no systemd service starts CP.py"
  [ -f "${BK%/}.complete" ] || echo "WARNING: $BK has no .complete marker - it may be an incomplete backup"
  read -r -p "Type YES to stop $UNIT and put back $BK: " ok; [ "$ok" = YES ] || die "cancelled"
  stop_service || die "CP.py did not stop"
  FAILED="${CP_DIR}_failed_R31_$(date +%Y%m%d_%H%M%S)"
  $SUDO mv "$CP_DIR" "$FAILED" && $SUDO cp -a "$BK" "$CP_DIR" || die "restore failed - folders: $FAILED and $BK"
  sudo systemctl start "$UNIT" && echo "restored; R3.1 folder kept as $FAILED"
}

{
  case "${1:-}" in
    check) check ;;
    stage) stage ;;
    install) shift; install_r31 "$@" ;;
    logs) shift; logs "$@" ;;
    rollback) shift; rollback "$@" ;;
    diag) shift; diag "$@" ;;
    zip) shift; snapshot_zip "$@" ;;
    *) sed -n '2,19p' "$0" ;;
  esac
  exit $?
}
