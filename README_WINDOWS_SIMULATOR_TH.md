# R3.1 CANDIDATE — 9 พฤศจิกายน 2026

R3.1 คือ R3 ที่เพิ่มการรองรับปุ่ม **CP Max 100kW** ของหน้า CSMS A ผ่านการทดสอบกับ **CSMS A และ B จริง** ครบ 7 ชุด (147/147 รายการตรวจ) โดยใช้ฮาร์ดแวร์จำลองและ ID ทดสอบ และผ่านชุดทดสอบบนเครื่อง 456/456 กรณี

**ยังไม่ใช่รุ่นที่ยืนยันว่าพร้อมลงตู้ใช้งานจริง** ต้องผ่านการตรวจฮาร์ดแวร์บนตู้ทดสอบตาม `R31_TEST_AND_INSTALL_STEPS_TH.md` ก่อน

ผลละเอียดอยู่ใน `evidence/R31_LIVE_TEST_RESULTS_TH.md` และ `evidence/R31_LIVE_TEST_RESULTS.json`

## สิ่งที่เปลี่ยนจาก R3

| ไฟล์ | ใช้ที่ | สิ่งที่เปลี่ยน |
|---|---|---|
| `helpf/ocpp_smart_charging.py` | **ลงตู้** | รับ `ChargePointMaxProfile` ชนิด `Relative` เมื่อ schedule มีช่วงเดียวและไม่มี duration (เพดานคงที่) ซึ่งตรงกับสิ่งที่ปุ่ม CP Max 100kW ของหน้า A ส่งมา ส่วน Relative ที่มีหลายช่วงหรือมี duration ยังตอบ Rejected เพราะไม่มีจุดเริ่มเวลาร่วมของทั้งสถานีที่ไม่ต้องเดา |
| `live_dual_simulator.py` | ตัวจำลองเท่านั้น | จำลอง Reset และ UnlockConnector (ไม่มีการ reboot หรือปลดล็อกจริง) และแก้เวลารอลงทะเบียนใหม่หลัง Reset ให้คิดจาก `reconnect_seconds + request_timeout + 5` วินาที |
| `tests/` | ทดสอบ | เพิ่ม `test_cpmax_simulator_r31.py`, `test_csms_api_client.py`, `test_simulator_reset_r31.py` และกรณีใน `test_ocpp_smart_charging.py` รวมเป็น 456 กรณี |
| `tools/csms_api_client.py` | ทดสอบ | เรียก API ของ A/B โดยตรงแทนการควบคุมเบราว์เซอร์ เก็บเฉพาะ session cookie ที่เข้ารหัสด้วย Windows DPAPI ไม่เก็บรหัสผ่าน |

ไฟล์ runtime อีก 9 ไฟล์ **ตรงกับ R3 ทุกไบต์** ได้แก่ `CP.py`, `helpf/plc_function.py`, `helpf/v2g_datatransfer.py`, `helpf/dual_csms.py`, `helpf/ocpp_services.py`, `helpf/ocpp_runtime.py`, `dual_csms_config.json`, `dual_csms_check.py` และ `ocpp_services_config.json` ดูผลต่างทั้งหมดได้ใน `CHANGES_R3_TO_R31.patch`

## พฤติกรรม CP Max 100 kW ที่ยืนยันกับ A จริง

| สถานการณ์ | ผลที่ได้ |
|---|---|
| A กดปุ่ม CP Max 100kW (connector 0, ChargePointMaxProfile, Relative, 100,000 W ช่วงเดียว ไม่มี duration) | **Accepted** (R3 ตอบ Rejected) |
| B สั่งเพดานทั้งสถานี | Rejected เพราะ A เป็นผู้ดูแลตู้ |
| A ชาร์จหัว 1 และ B ชาร์จหัว 2 พร้อมกัน | Composite Schedule หัวละ **50,000 W** และคำสั่ง limit ที่ส่งไป PLC (จำลอง) หัวละ 50 kW คงที่ตลอดการชาร์จพร้อมกัน 128 วินาที |
| หยุดหัวหนึ่ง | หัวที่ยังชาร์จได้ **100,000 W** |
| ล้าง profile | Accepted แล้วกลับไปใช้ค่าพิกัดของสถานี 180 kW (หัวละ 90 kW) ล้างซ้ำได้ Unknown |

ลำดับคำสั่ง limit ทุกครั้งเป็น **ลดก่อนเพิ่ม** ผลรวมของสองหัวไม่เกินเพดานระหว่างที่มีรายการชาร์จ ขณะหัวหนึ่งชาร์จ หัวที่ว่างจะถูกตั้งเป็น 0 W และได้ส่วนแบ่งของตัวเองก่อนรายการใหม่เริ่ม จุดนี้ต้องตรวจกับ PLC จริง

## นโยบายสอง CSMS (เหมือน R3)

- A และ B เริ่มหัวที่ว่างได้ และหยุดได้เฉพาะรายการของตัวเอง RemoteStop ด้วย transaction ID ของอีกฝั่งตอบ Rejected และไม่หยุดรายการนั้น
- StartTransaction, MeterValues และ StopTransaction ส่งเฉพาะเจ้าของรายการ ส่วน StatusNotification ส่งให้ทั้งสองฝั่ง
- A เป็นผู้ดูแลตู้ คำสั่ง Reset, ChangeConfiguration, SetChargingProfile, ClearChargingProfile, ClearCache, SendLocalList, UnlockConnector, ChangeAvailability, GetDiagnostics และ UpdateFirmware จาก B ถูกปฏิเสธ
- UpdateFirmware เป็นแบบ **ดาวน์โหลดและจัดเก็บเท่านั้น** ไม่มีตัวติดตั้ง ไม่รันไฟล์ และไม่ส่ง Installing/Installed

## ไฟล์สำหรับอัปเกรดตู้ (10 ไฟล์ เหมือน R3)

```text
CP.py
helpf/plc_function.py
helpf/v2g_datatransfer.py
helpf/dual_csms.py
dual_csms_config.json
dual_csms_check.py
helpf/ocpp_services.py
helpf/ocpp_smart_charging.py
helpf/ocpp_runtime.py
ocpp_services_config.json
```

ถ้าตู้ติดตั้ง R3 อยู่แล้ว ไฟล์ที่ต่างมีเพียง `helpf/ocpp_smart_charging.py` แต่ถ้าตู้ยังเป็น R1/R2 หรือโค้ดเดิม ต้องลงครบทั้ง 10 ไฟล์เป็นชุดเดียวกัน อย่าเขียนทับไฟล์ตั้งค่าเฉพาะตู้ ฐานข้อมูล รายการบัตร log และคิวที่ค้างอยู่

## สิ่งที่ยังต้องตรวจบนตู้ทดสอบ

1. PLC รับค่า limit ต่อหัวในหน่วย W และ A จาก `limitpower`/`limitcurrent` ถูกต้อง รวมถึงหัวละ 50 kW ใต้เพดาน 100 kW
2. หัวที่ว่างถูกตั้ง 0 W ระหว่างที่อีกหัวชาร์จ ต้องเริ่มรายการใหม่บนหัวนั้นได้หลังค่าถูกยกขึ้น
3. มิเตอร์ contactor การล็อก/ปลดล็อกสาย ปุ่มฉุกเฉิน Reset/reboot และการกู้สถานะหลังไฟดับ
4. ตรวจค่าพิกัด `station_rated_power_w` 180000 และ `rated_current_a` 500 ใน `ocpp_services_config.json` กับ nameplate ของตู้


---

# ขั้นตอนทดสอบและติดตั้ง R3.1

เอกสารนี้เป็นขั้นตอน ไม่ใช่หลักฐานผลทดสอบ ผลที่ผ่านแล้วดู `evidence/R31_LIVE_TEST_RESULTS_TH.md`

## 0. สถานะก่อนเริ่ม

- ผ่านแล้วบน PC: ชุดทดสอบบนเครื่อง 456/456 และการทดสอบกับ CSMS A/B จริง 147/147 รายการตรวจ โดยใช้ฮาร์ดแวร์จำลองและ ID ทดสอบ
- ยังไม่ผ่าน: ทุกอย่างที่เกี่ยวกับฮาร์ดแวร์จริง (PLC, มิเตอร์, contactor, ล็อกสาย, ปุ่มฉุกเฉิน, reboot)

## 1. (ทางเลือก) ทดสอบซ้ำกับ A/B ด้วยตัวจำลองบน Windows

ใช้แพ็ก `OCPP_DUAL_CSMS_R31_SIMULATOR_2026-11-09.zip` ซึ่งไม่มี CP.py เต็มและไม่แตะฮาร์ดแวร์

1. แตก ZIP ลงโฟลเดอร์ใหม่ เตรียม Python ครั้งแรก:

   ```powershell
   py -m venv .venv
   .\.venv\Scripts\python.exe -m pip install -r requirements-simulator.txt
   ```

2. เปิด `RUN_WINDOWS_SIMULATOR_R31.cmd` แล้วรอให้ทั้ง A และ B ตอบ BootNotification = Accepted
3. ID ที่ใช้ต้องเป็น ID ทดสอบเท่านั้น:

   | CSMS | WebSocket | หน้าเว็บ |
   |---|---|---|
   | A | `wss://csms-a.example.com/ocpp/CP-A-0001-TEST-PC-9F3A` | https://csms-a.example.com/ |
   | B | `ws://csms-b.example.com:9000/ocpp/CP-B-0001-TEST-PC-9F3A` | http://csms-b.example.com:8080/ |

4. ทดสอบจากหน้าเว็บ: A เริ่มหัว 1, B เริ่มหัว 2 พร้อมกัน แล้วกด **CP Max 100kW** ที่หน้า A ตรวจ Composite Schedule ว่าได้หัวละ 50,000 W หยุดหัวหนึ่งแล้วตรวจว่าอีกหัวได้ 100,000 W จากนั้นหยุดอีกหัว
5. ห้ามเปิดตัวจำลองสองโปรแกรมพร้อมกันด้วย ID เดียวกัน ปิดตัวจำลองด้วย Ctrl+C แล้วตรวจว่าไม่มีรายการหรือคิวค้าง

## 2. ตรวจฮาร์ดแวร์บนตู้ทดสอบ (ต้องผ่านก่อนลงตู้ใช้งาน)

ทำบนตู้ทดสอบที่ไม่มีลูกค้าใช้งาน และยังใช้ ID ทดสอบในไฟล์ `dual_csms_config.json` ของตู้ทดสอบ

1. **เพดานกำลัง:** ตั้ง CP Max 100kW จาก A ชาร์จสองหัวพร้อมกัน วัดกำลังจริงแต่ละหัวไม่เกิน 50 kW และรวมไม่เกิน 100 kW หยุดหัวหนึ่งแล้วตรวจว่าอีกหัวขึ้นได้ถึง 100 kW
2. **หัวที่ว่างถูกตั้ง 0 W:** ระหว่างหัว 1 ชาร์จใต้เพดาน ให้เริ่มหัว 2 ด้วย RFID และด้วย RemoteStart ต้องเริ่มได้และได้ 50 kW ไม่ fault หรือค้าง Preparing
3. **ล้างเพดาน:** A ล้าง profile แล้วตรวจว่ากลับไปใช้ค่าพิกัดตาม nameplate
4. **หน่วยและค่า:** ตรวจว่า PLC ตีความค่า W และ A จาก `limitpower`/`limitcurrent` ถูกต้อง และค่า 0 หยุดจ่ายไฟจริง
5. **ฮาร์ดแวร์อื่น:** มิเตอร์อ่านค่าถูก contactor เปิด/ปิดตามคำสั่ง UnlockConnector ปลดล็อกจริง ปุ่มฉุกเฉินหยุดและส่ง StopTransaction ให้เจ้าของรายการ
6. **Reset/ไฟดับ:** Soft และ Hard Reset จาก A ขณะสองหัวชาร์จ ต้องส่ง StopTransaction ให้ A และ B ฝั่งละหนึ่งครั้งก่อน reboot ทดสอบตัดไฟระหว่างชาร์จแล้วตรวจว่า Stop ที่ค้างส่งครั้งเดียวหลังต่อกลับ
7. **ค่าพิกัด:** ตรวจ `station_rated_power_w` (180000) และ `rated_current_a` (500) ใน `ocpp_services_config.json` กับ nameplate ของตู้ก่อนใช้งาน

## 3. ติดตั้งลงตู้

1. จบรายการชาร์จทุกหัวด้วยระบบเดิม ตรวจใน CSMS ทั้งสองฝั่งว่าไม่มี transaction เปิด
2. หยุด service ของ CP แล้วสำรองโฟลเดอร์ `OCPP_CP` ทั้งโฟลเดอร์ (ทั้ง source และไฟล์สถานะ)
3. คัดลอก 10 ไฟล์ตาม `R31_RELEASE_NOTES_TH.md` ลงโฟลเดอร์เดิม ห้ามคัดลอกโฟลเดอร์ `simulator_state*`, `.api_auth` หรือฐานข้อมูลจาก PC ลงตู้
4. ตรวจ `dual_csms_config.json` ว่าใช้ URL ของตู้จริง:
   - A = `wss://csms-a.example.com/ocpp/CP-A-0001`
   - B = `ws://csms-b.example.com:9000/ocpp/CP-B-0001`
5. ตรวจ config ด้วย Python environment เดิมของ service จากโฟลเดอร์เดิม:

   ```bash
   python3 dual_csms_check.py --state-dir .
   ```

6. เริ่ม service แล้วตรวจ log ให้เห็นทั้ง `[A] OCPP registration accepted` และ `[B] OCPP registration accepted`
7. ทดสอบสั้น ๆ: A เริ่ม/หยุดหัว 1, B เริ่ม/หยุดหัว 1 จากนั้นค่อยทดสอบสองหัวพร้อมกันและ CP Max

## 4. ย้อนรุ่น

1. จบรายการชาร์จทั้งหมดและตรวจว่าไม่มี Stop ค้างในคิว
2. หยุด service แล้วคืนโฟลเดอร์จากชุดสำรองในขั้น 3.2 ทั้ง source และไฟล์สถานะพร้อมกัน
3. ห้ามลบ outbox หรือส่ง Start ที่ผลยังไม่ทราบซ้ำ
4. ถ้าตู้เป็น R3 อยู่แล้ว การย้อนจาก R3.1 กลับ R3 ทำได้โดยคืน `helpf/ocpp_smart_charging.py` ไฟล์เดียว แต่ profile ชนิด Relative ที่ R3.1 รับไว้จะไม่ถูกต้องสำหรับ R3 ให้ A ล้าง profile ก่อนย้อน

## คำสั่งทดสอบบนเครื่อง (ไม่เริ่ม CP.py)

```powershell
python -m pip install -r requirements-test.txt
python -m pytest tests -q --tb=short
```
