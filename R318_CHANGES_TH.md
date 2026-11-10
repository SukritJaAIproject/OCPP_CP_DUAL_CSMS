# R3.1.8 — 10 พฤศจิกายน 2026

R3.1.8 คือ R3.1.7 ที่แก้ **สถานะ Faulted ของปุ่มฉุกเฉินใช้ `errorCode: NoError`** ที่พบบนตู้ทดสอบเมื่อ 10 พ.ย. 2026

## อาการที่พบ

เมื่อกดปุ่มฉุกเฉิน (16:11:34, 16:12:03 และ 16:38:42) ตู้ส่ง StatusNotification ให้ทั้ง A และ B ของ connector 0, 1 และ 2 เป็น

```
"errorCode":"NoError","status":"Faulted","info":"EmergencyButtonPressed"
```

ตาม OCPP 1.6 `NoError` หมายถึงไม่มีความผิดปกติ CSMS ที่อ่านเฉพาะ `errorCode` จึงไม่เห็นว่าเป็นเหตุฉุกเฉิน สาเหตุอยู่ในช่อง `info` เท่านั้น

## สิ่งที่แก้

สถานะ Faulted ของปุ่มฉุกเฉินเป็น

```
"errorCode":"OtherError","status":"Faulted","info":"EmergencyButtonPressed","vendorId":"EgatDimomdService","vendorErrorCode":"SYS_EMERGENCY_STOP"
```

- OCPP 1.6 ไม่มี errorCode เฉพาะของปุ่มฉุกเฉิน จึงใช้ `OtherError` คู่กับ `vendorErrorCode` แบบเดียวกับ PREPARING TIMEOUT
- `vendorErrorCode` เป็นรหัสเดียวกับที่ทาง Vector fault ใช้กับปุ่มเดียวกัน (`SYS_EMERGENCY_STOP`) เปลี่ยนได้ด้วย environment `EMERGENCY_STOP_FAULT_CODE` ของ service ปกติไม่ต้องตั้ง
- `info` และ `vendorId` เหมือนเดิม

## สิ่งที่ไม่เปลี่ยน

- ลำดับ: ปิด transaction ที่เปิดอยู่ (StopTransaction เหตุผล `EmergencyStop` ถึงเจ้าของ) ก่อนส่ง Faulted
- จำนวนข้อความ: connector 0, 1, 2 อย่างละครั้งต่อ CSMS ต่อการกดหนึ่งครั้ง
- สถานะหลังปลดปุ่ม (Available หรือ Finishing) ยังใช้ `NoError`
- เมื่อ Vector fault collector ทำงานอยู่ ทางนั้นเป็นผู้รายงานแทนเหมือนเดิม

## ไฟล์ที่เปลี่ยนจาก R3.1.7

| ไฟล์ | สิ่งที่เปลี่ยน |
|---|---|
| `CP.py` | `status_notification_emer()`: `errorCode` และ `vendorErrorCode` |
| `tests/test_r318_emergency_status.py` | ทดสอบใหม่ 7 กรณี |

ไฟล์อื่นที่ลงตู้ตรงกับ R3.1.7 ทุกไบต์ ไม่มีการตั้งค่าเพิ่ม ฐานข้อมูลเดิมใช้ต่อได้ ผลต่างทั้งหมดอยู่ใน `CHANGES_R317_TO_R318.patch`

## ติดตั้ง

ทั้งสองหัวต้องว่าง (ถอดสาย) ก่อน install ใส่ `RATED_W` เป็นกำลังตามป้ายของตู้

```
bash ~/edgebox_r31.sh stage
RATED_W=120000 bash ~/edgebox_r31.sh install /home/pi/ocpp
```

## ผลทดสอบ

- ชุดทดสอบบนเครื่อง 602/602 บน Linux Python 3.13.18, ocpp 2.1.0, websockets 15.0.1, pytest 9.1.1
- ชุดทดสอบใหม่เมื่อรันกับโค้ด R3.1.7 ไม่ผ่าน
- ติดตั้งบนตู้ทดสอบ 10 พ.ย. 2026: ชุดทดสอบผ่าน 602/602 บนตู้ (Python 3.11.2, ocpp 1.0.0, websockets 17.1)
- กดปุ่มฉุกเฉินจริงหลังติดตั้ง (ตู้ว่าง): A และ B ได้ Faulted ของ connector 0, 1, 2 อย่างละครั้ง ทุกข้อความเป็น `OtherError` และ `SYS_EMERGENCY_STOP`
