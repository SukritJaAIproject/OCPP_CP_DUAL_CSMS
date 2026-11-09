# R3.1.1 — 9 พฤศจิกายน 2026

R3.1.1 คือ R3.1 ที่เพิ่ม **บรรทัด log บอกเหตุผลทุกครั้งที่ตู้ปฏิเสธ RemoteStartTransaction** พฤติกรรมการรับหรือปฏิเสธคำสั่งไม่เปลี่ยน

## ที่มา

ตอนทดสอบบน Edgebox ทดสอบในโรงงาน (192.168.3.14) วันที่ 9 พ.ย. 2026 เวลา 14:50 A สั่ง RemoteStart หัว 1 แล้วตู้ตอบ Rejected ทันที แต่ R3.1 ไม่เขียนเหตุผลลง log จึงหาสาเหตุไม่ได้

## สิ่งที่เปลี่ยน

| ไฟล์ | สิ่งที่เปลี่ยน |
|---|---|
| `helpf/dual_csms.py` | ทุกจุดที่ปฏิเสธ RemoteStart เขียน `[A] RemoteStart C1 rejected: <เหตุผล>` (ระดับ WARNING) เช่น CSMS นี้ยังไม่ Accepted BootNotification, ตู้ยังเริ่มทำงานไม่เสร็จ, หัวถูกจองโดยรายการของอีกฝั่ง หรือเหตุผลจาก CP.py ด้านล่าง |
| `CP.py` | แยกเงื่อนไขเดิมของ `_dual_connector_busy()` ออกเป็น `_dual_busy_reason()` ซึ่งคืนข้อความเหตุผล เช่น `previous session finishing - unplug the cable first`, `PLC reports Finishing`, `start request from the charger screen pending` เงื่อนไขและลำดับเหมือน R3.1 ทุกข้อ และ RemoteStart ที่ถูกปฏิเสธที่ gate ของ CP.py เองก็เขียนเหตุผลด้วย |
| `helpf/ocpp_runtime.py` | เพิ่ม `start_refusal()` บอกเหตุผลที่ runtime ไม่ยอมให้เริ่ม (ยังไม่พร้อม, limit ผิดพลาด, Inoperative, สถานะหัว, การจอง) |
| `tests/test_r311_start_refusal_logging.py` | ทดสอบใหม่ 25 กรณี: ข้อความเหตุผล, ไม่มี idTag ใน log, และลำดับเงื่อนไขของ CP.py เหมือน R3.1 |
| `tools/cp_protocol_defs.py` | สร้างใหม่จาก CP.py ใหม่ (เนื้อหาเหมือนเดิม เปลี่ยนเฉพาะบรรทัด hash ของ CP.py) |

ข้อความใน log ไม่มี idTag ของผู้ใช้

## ผลทดสอบ

- ชุดทดสอบบนเครื่อง 481/481 (เดิม 456 + ใหม่ 25) บน Linux Python 3.13, ocpp 2.1.0, websockets 15.0.1
- ยังไม่ได้รันชุดทดสอบกับ CSMS A/B จริงซ้ำหลังการแก้นี้ เพราะเปลี่ยนเฉพาะการเขียน log ในเส้นทางที่ปฏิเสธคำสั่ง

ดูผลต่างทั้งหมดได้ใน `CHANGES_R31_TO_R311.patch`
