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
