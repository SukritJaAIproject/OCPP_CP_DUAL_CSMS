# OCPP_CP_DUAL_CSMS

Dual-CSMS support for an OCPP 1.6J DC charge point. One charger, controlled by `CP.py` on a Raspberry Pi "Edgebox", stays connected to **two Central Systems (CSMS A and CSMS B) at the same time**. Both see the charger. Each charging session belongs to exactly one of them.

The code runs on a 120 kW two-connector factory test charger. This repository holds release R3.1.5.

## What it does

| Area | Behaviour |
|---|---|
| Connections | Two independent OCPP 1.6J WebSocket links with their own BootNotification, Heartbeat and reconnect. One CSMS going offline does not affect the other. |
| Session ownership | A session belongs to the CSMS that started it. StartTransaction, MeterValues and StopTransaction go to the owner only. No duplicate transactions and no double-counted energy. |
| Status | StatusNotification goes to both CSMS. While a head has a session, `info` names its owner: `Session of CSMS B (tx 555)`. The charger's own `info` text (for example Vector fault messages) is never replaced. |
| Remote start/stop | Either CSMS can RemoteStart a free connector. RemoteStop works only on the caller's own session. |
| RFID at the charger | A card is authorized by `local_csms` (A) first. If A does not accept it or cannot be reached, B is asked. The session belongs to the CSMS that accepted the card. The accepting CSMS is remembered per card as a SHA-256 hash; card numbers are not stored or logged. |
| Admin commands | Reset, ChangeConfiguration, SetChargingProfile, ClearChargingProfile, ClearCache, SendLocalList, UnlockConnector, ChangeAvailability, GetDiagnostics and UpdateFirmware are accepted from the CSMS listed in `admin_csms`. |
| Smart charging | The station rating is split between active connectors (120 kW → 60/60 kW). ChargePointMaxProfile (CP Max) caps the station. Limits are always lowered before they are raised. |
| Offline | Messages for an unreachable CSMS are kept in SQLite and delivered in order after it reconnects. |
| Simulator | `live_dual_simulator.py` runs the protocol stack against real CSMS with test charger IDs and no hardware (Windows or Linux). |

## Layout

| Path | Purpose |
|---|---|
| `CP.py` | Charger program: PLC/HMI over MQTT, Vector fault bridge, OCPP handlers, hooks into the router |
| `helpf/dual_csms.py` | The router: connections, session table, outbox, ownership, RFID A→B, status labels |
| `helpf/ocpp_runtime.py`, `helpf/ocpp_services.py` | Service layer: reservations, availability, local list, firmware/diagnostics jobs |
| `helpf/ocpp_smart_charging.py` | Charging profiles and the per-connector limit sent to the PLC |
| `helpf/plc_function.py`, `helpf/v2g_datatransfer.py`, `helpf/stop_pin_config.py` | PLC MQTT client, ISO 15118 / DIN data bridge, stop-PIN settings |
| `dual_csms_config.json` | Which CSMS to connect to and who is admin |
| `ocpp_services_config.json` | Station rating, firmware/diagnostics transfer rules |
| `tests/` | 526 offline tests (only 127.0.0.1 is used) |
| `tools/` | Protocol harness, CSMS API client and simulator controls used by the tests and the simulator |
| `deploy/edgebox_r31.sh` | Install helper for the Edgebox (see below) |
| `R31*_*.md`, `README_*_TH.md` | Release notes and operator guides (Thai) |
| `CHANGES_*.patch` | Diff between consecutive releases |

## Configuration

`dual_csms_config.json` (the hosts and charger IDs in this repository are placeholders):

```json
{
  "enabled": true,
  "local_csms": "A",
  "admin_csms": ["A", "B"],
  "request_timeout": 15,
  "reconnect_seconds": 5,
  "meter_queue_limit": 2000,
  "servers": [
    {"name": "A", "enabled": true, "url": "wss://csms-a.example.com/ocpp/CP-A-0001"},
    {"name": "B", "enabled": true, "url": "ws://csms-b.example.com:9000/ocpp/CP-B-0001"}
  ]
}
```

- `local_csms`: the CSMS asked first for cards tapped at the charger.
- `admin_csms`: one name or a list of names allowed to send the admin commands.
- `ocpp_services_config.json`: `station_rated_power_w` and `rated_current_a` must match the nameplate. The installer sets them from `RATED_W` and `RATED_A`.
- Site constants at the top of `CP.py` (`OCPP_BROKER`, `OCPP_PORT`, `OCPPSTATION`, `EV_STOP_V2G_TOPIC`) describe the local MQTT setup. The installer keeps the values already on the charger.
- Telegram alerts are optional. Set `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` in the service environment (for example with `Environment=` in the systemd unit). Without them, alerts are skipped.

## Running the tests

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements-test.txt
python -m pytest tests -q
```

The tests also pass with the versions on the Edgebox (Python 3.11, ocpp 1.0.0, websockets 17.1).

## Installing on an Edgebox

`deploy/edgebox_r31.sh` runs on the Raspberry Pi as the `pi` user next to a release ZIP (a folder with these files plus `MANIFEST_SHA256.json`). Set `PKG_NAME` and `PKG_SHA` at the top of the script to the ZIP you built.

```bash
bash ~/edgebox_r31.sh stage                                    # unpack, verify, run the tests as the service user
RATED_W=120000 bash ~/edgebox_r31.sh install /home/pi/ocpp     # backup, stop, copy, keep site settings, start
bash ~/edgebox_r31.sh diag                                     # read-only: sessions, owners, limits, card checks
bash ~/edgebox_r31.sh zip                                      # snapshot of the whole folder while it runs
bash ~/edgebox_r31.sh rollback <backup folder> /home/pi/ocpp
```

## Security notes

- OCPP 1.6J over plain `ws://` has no authentication. Anyone who knows the URL can connect as the charger. Use `wss://` with HTTP Basic authentication (OCPP security profile 2) in production.
- No credentials are kept in this repository. CSMS addresses, the MQTT broker and charger IDs are placeholders.
- Logs written on the charger contain card numbers from StartTransaction and Authorize payloads. Keep logs and `zip` snapshots private.

## Release history

| Release | Change |
|---|---|
| R3.1 | Dual-CSMS router (continues R3), CP Max support, Windows simulator |
| R3.1.1 | Logs the reason whenever the charger refuses a RemoteStart |
| R3.1.2 | A plugged-in car in SuspendedEV no longer blocks RemoteStart. Vector faults are no longer overwritten at boot |
| R3.1.3 | `admin_csms` can list several CSMS (B can send the admin commands too) |
| R3.1.4 | RFID cards are authorized by A first, then B. The session belongs to the CSMS that accepted the card |
| R3.1.5 | Status `info` names the CSMS that owns each session, for both CSMS |

Each `R31x_CHANGES_TH.md` file explains its release in detail, and `CHANGES_<from>_TO_<to>.patch` holds the exact diff between releases.

## สรุปภาษาไทย

ตู้ชาร์จหนึ่งตู้เชื่อมต่อ CSMS สองระบบ (A และ B) พร้อมกันด้วย OCPP 1.6J

- รายการชาร์จเป็นของ CSMS ที่เริ่มรายการ ข้อมูล StartTransaction, MeterValues และ StopTransaction ส่งเฉพาะเจ้าของ จึงไม่มีการนับพลังงานซ้ำ
- StatusNotification ส่งให้ทั้งสองฝั่ง และระบุเจ้าของรายการในช่อง `info`
- บัตร RFID ถาม A ก่อน ถ้า A ไม่รับจึงถาม B รายการเป็นของฝั่งที่รับบัตร
- คำสั่งผู้ดูแลตู้รับจาก CSMS ที่อยู่ใน `admin_csms`
- กำลังไฟของสถานีแบ่งให้หัวที่ชาร์จอยู่ และลดก่อนเพิ่มเสมอ

รายละเอียดแต่ละรุ่นอยู่ในไฟล์ `R31x_CHANGES_TH.md` และวิธีติดตั้งบนตู้อยู่ใน `R31_TEST_AND_INSTALL_STEPS_TH.md`

## License

MIT, see [LICENSE](LICENSE).
