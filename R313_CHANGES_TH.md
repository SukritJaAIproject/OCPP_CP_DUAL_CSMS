# R3.1.3 — 9 พฤศจิกายน 2026

R3.1.3 คือ R3.1.2 ที่ให้ **B สั่งคำสั่งผู้ดูแลตู้ได้เท่า A** ตามที่ขอเมื่อ 9 พ.ย. 2026

## สิ่งที่เปลี่ยน

`admin_csms` ใน `dual_csms_config.json` ระบุได้ทั้งชื่อเดียว (`"A"`) หรือหลายชื่อ (`["A", "B"]`) ทุก CSMS ที่อยู่ในรายการสั่งคำสั่งเหล่านี้ได้:

Reset, ChangeConfiguration, SetChargingProfile, ClearChargingProfile, ClearCache, SendLocalList, UnlockConnector, ChangeAvailability, GetDiagnostics, UpdateFirmware

ค่าเริ่มต้นในแพ็กเกจเปลี่ยนเป็น `["A", "B"]`

## สิ่งที่ยังเหมือนเดิม

- **RemoteStop** หยุดได้เฉพาะรายการของตัวเองทั้ง A และ B
- **RemoteStart** ทั้งสองฝั่งเริ่มได้เฉพาะหัวที่ว่าง
- **RFID หน้าตู้** รายการที่เริ่มด้วย RFID ยังเป็นของ `local_csms` (A)
- **StartTransaction, MeterValues และ StopTransaction** ส่งเฉพาะเจ้าของรายการ ส่วน StatusNotification ส่งให้ทั้งสองฝั่ง
- **Firmware/Diagnostics** ความคืบหน้าส่งกลับไปที่ CSMS ที่สั่ง ถ้าไม่รู้ว่าใครสั่ง (เช่นหลัง restart) จะส่งให้ admin ตัวแรกในรายการ

## สิ่งที่ควรรู้เมื่อมี admin สองฝั่ง

- **ค่า configuration, เพดาน (charging profile), local list และ availability เป็นชุดเดียวของตู้** ถ้า B ล้าง profile, B ส่ง local list ใหม่ หรือ B ตั้ง Inoperative ก็มีผลกับ A ด้วย และกลับกัน
- **Reset และ UnlockConnector จากฝั่งใดก็หยุดรายการของอีกฝั่งได้** แต่ StopTransaction ยังส่งไปที่เจ้าของรายการ

## ติดตั้งบนตู้ที่มี `dual_csms_config.json` อยู่แล้ว

สคริปต์ติดตั้งคงไฟล์ config เดิมไว้ ให้ระบุ `ADMIN_CSMS` ตอนติดตั้ง:

```
RATED_W=120000 ADMIN_CSMS=A,B bash ~/edgebox_r31.sh install /home/pi/ocpp
```

## ไฟล์ที่เปลี่ยนจาก R3.1.2

| ไฟล์ | สิ่งที่เปลี่ยน |
|---|---|
| `helpf/dual_csms.py` | `validate_config` รับรายการ admin และเพิ่ม `admins` กับ `primary_admin`, ตัวกรองคำสั่งผู้ดูแลใช้รายการ, Firmware/Diagnostics ที่ไม่รู้ผู้สั่งส่งให้ admin ตัวแรก |
| `helpf/ocpp_runtime.py` | `admin_owner` เป็น admin ตัวแรก |
| `dual_csms_config.json` | `"admin_csms": ["A", "B"]` |
| `tests/test_r313_multi_admin.py` | ทดสอบใหม่ 14 กรณี |

`CP.py` ไม่เปลี่ยนจาก R3.1.2

## ผลทดสอบ

- ชุดทดสอบบนเครื่อง 505/505 บน Linux Python 3.13, ocpp 2.1.0, websockets 15.0.1
- ยังไม่ได้ทดสอบกับ CSMS A/B จริง ตัวจำลองบน PC (`dual_csms_simulator_config.json`) ยังตั้ง admin เป็น A อย่างเดียว เพราะชุดทดสอบกับ CSMS จริงตรวจว่า B สั่ง Reset ไม่ได้

ผลต่างทั้งหมดอยู่ใน `CHANGES_R312_TO_R313.patch`
