# R3.1.5 — 9 พฤศจิกายน 2026

R3.1.5 คือ R3.1.4 ที่ให้ **ทั้ง A และ B เห็นว่ารายการชาร์จบนแต่ละหัวเป็นของ CSMS ไหน** ตามทางที่ 1 ที่เลือกเมื่อ 9 พ.ย. 2026

## วิธีทำงาน

StatusNotification ส่งให้ทั้งสองฝั่งอยู่แล้วตั้งแต่ R3 รุ่นนี้เติมช่อง `info` ของสถานะที่เกี่ยวกับรายการชาร์จ:

| สถานะ | `info` ที่เติม |
|---|---|
| Charging, SuspendedEV, SuspendedEVSE | `Session of CSMS B (tx 555)` = รายการของ B หมายเลข transaction 555 ของฝั่ง B |
| Finishing (หลังรายการจบ สายยังเสียบ) | ข้อความเดียวกันของรายการที่เพิ่งจบ (ภายใน 10 นาทีหลัง StopTransaction) |
| Available, Preparing, Reserved, Faulted, Unavailable | ไม่เติม |

- ส่งข้อความเดียวกันให้ทั้งสองฝั่ง ฝั่งเจ้าของเห็นหมายเลข transaction ของตัวเอง อีกฝั่งเห็นว่าหัวนี้ถูกใช้โดยอีก CSMS
- ไม่สร้าง transaction ที่สอง ไม่ส่ง StartTransaction/MeterValues/StopTransaction ให้ฝั่งที่ไม่ใช่เจ้าของ จึงไม่มีการนับ kWh หรือรายได้ซ้ำ และฝั่งที่ไม่ใช่เจ้าของสั่ง RemoteStop รายการนี้ไม่ได้เหมือนเดิม
- ไม่มีเลขบัตรในข้อความ
- ถ้าตู้ใส่ `info` มาเองอยู่แล้ว (เช่น `Issue F71202 cleared.` ของ Vector) จะคงข้อความของตู้ไว้ ไม่ทับ
- ถ้าหัวชาร์จอยู่แต่ไม่มีรายการ OCPP (เช่น กด Start จากหน้าจอ HMI แบบรายงานสถานะอย่างเดียว) ไม่เติม เพราะไม่มี CSMS เจ้าของ
- RemoteStart ที่รับแล้วแต่ยังไม่ได้คำตอบ StartTransaction เติม `Session of CSMS B` โดยไม่มีหมายเลข transaction
- ข้อความไม่เกิน 50 ตัวอักษรตาม OCPP 1.6 ชื่อ CSMS ยาวจะถูกตัด
- สถานะที่ส่งซ้ำตอนเชื่อมต่อใหม่ และที่ CSMS ขอด้วย TriggerMessage ก็มีข้อความนี้

## ดูย้อนหลัง

- **หน้าเว็บ/log ของ A และ B**: ดูช่อง info ของ StatusNotification ของหัวนั้น
- **log ฝั่งตู้**: `grep "Session of CSMS" /home/pi/ocpp/*.log`
- **`bash ~/edgebox_r31.sh diag`**: บรรทัด `last StatusNotification per connector` แสดง info ด้วย

ตัวอย่างใน log ของตู้ (บรรทัด `A: send` และ `B: send` มีข้อความเดียวกัน):

```
A: send [2,"...","StatusNotification",{"connectorId":1,"errorCode":"NoError","status":"Charging","info":"Session of CSMS B (tx 555)",...
```

## ไฟล์ที่เปลี่ยนจาก R3.1.4

| ไฟล์ | สิ่งที่เปลี่ยน |
|---|---|
| `helpf/dual_csms.py` | `_label_status()` เติม info ก่อนเก็บสถานะ, `session_label()`, `_session_for_status()`, จำเวลาที่ StopTransaction ถูกคิวต่อหัวสำหรับ Finishing |
| `tests/test_r315_status_owner.py` | ทดสอบใหม่ 11 กรณี |

`CP.py` และไฟล์อื่นที่ลงตู้ตรงกับ R3.1.4 ทุกไบต์ ไม่มีการตั้งค่าเพิ่ม ฐานข้อมูลเดิมใช้ต่อได้

## ติดตั้งบนตู้ที่ลง R3.1.4 แล้ว

```
bash ~/edgebox_r31.sh stage
RATED_W=120000 bash ~/edgebox_r31.sh install /home/pi/ocpp
```

## ผลทดสอบ

- ชุดทดสอบบนเครื่อง 526/526 บน Linux Python 3.13.18, ocpp 2.1.0, websockets 15.0.1
- ยังไม่ได้ทดสอบกับ CSMS A/B จริง ควรดูว่าหน้าเว็บของ A และ B แสดงช่อง info หรือไม่ (บาง CSMS เก็บไว้ใน log อย่างเดียว)

ผลต่างทั้งหมดอยู่ใน `CHANGES_R314_TO_R315.patch`
