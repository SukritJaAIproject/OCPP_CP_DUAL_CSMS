# R3.1.6 — 10 พฤศจิกายน 2026

R3.1.6 คือ R3.1.5 ที่แก้ **RemoteStart ถูกปฏิเสธค้างทั้ง A และ B หลังรายการที่ไม่ได้เริ่มชาร์จ** ที่พบบนตู้ Edgebox 192.168.3.15 เมื่อ 10 พ.ย. 2026

## อาการที่พบ (หัว 2)

| เวลา | เหตุการณ์ |
|---|---|
| 10:37:49 | A สั่ง RemoteStart ตู้ตอบ Accepted และสั่ง PLC เริ่ม |
| 10:38:07 | รถจบ session DIN เอง (SessionStop) ยังไม่มี StartTransaction |
| 10:38:22 | ถอดสาย ตู้ส่ง Available |
| 10:40:36 เป็นต้นไป | RemoteStart ทุกครั้งของหัว 2 ทั้งจาก A และ B ถูกปฏิเสธ: `start already sent to the PLC (start_transaction_SW2=3)` จนกว่าจะ restart โปรแกรม |

## สาเหตุ

RemoteStart ใช้ธง 3 ตัวต่อหัว

| ธง | ความหมาย |
|---|---|
| `myremotestart_cN=1` | รับ RemoteStart แล้ว รอยิง PLC |
| `remote_start_received_cN=1` | รายการนี้มาจาก RemoteStart |
| `start_transaction_SWN=3` | ยิง PLC แล้ว รอ StartTransaction |

ทางบัตรมีขั้นตอนล้างธงเมื่อ session จบก่อน StartTransaction อยู่แล้ว (`[RFID-OCPP-V7]`) แต่ทาง RemoteStart ไม่มี ตอนถอดสายล้างแค่ `remote_start_received` ธง `start_transaction_SW=3` จึงค้าง และตัวตรวจก่อนเริ่มชาร์จถือว่าหัวยังไม่ว่าง

## สิ่งที่แก้

ล้างธงเมื่อ RemoteStart จบก่อน StartTransaction (แบบเดียวกับบัตร):

1. **PLC จบ session** (ธง Finishing ของ PLC ขึ้น): ส่ง Finishing ให้ทั้ง A และ B โดยไม่มี StopTransaction (ไม่มี transaction ให้ปิด) ล้างธงทั้ง 3 ตัว และปล่อยการจองหัวของ CSMS ที่สั่ง เหมือนบัตร สถานะค้าง Finishing จนถอดสาย แล้วเป็น Available
2. **ถอดสาย**: ล้างธงทั้ง 3 ตัวและการจองหัว (เดิมล้างแค่ `remote_start_received`) กรณีของวันที่ 10 พ.ย. คือข้อนี้
3. **เริ่มแล้วถูกยกเลิก** (CSMS ปฏิเสธ StartTransaction, ChangeAvailability Inoperative): ล้างธงก่อนสั่ง PLC หยุด พร้อมธงอื่นที่ล้างอยู่แล้ว
4. **เปิดโปรแกรมใหม่ขณะสายไม่ได้เสียบและไม่มีการจองค้าง**: ล้าง `myremotestart` และ `remote_start_received` ด้วย (เดิมล้างแค่ `start_transaction_SW`)

ตู้ปลดธงที่ค้างเองแทนการปฏิเสธ: ทุกคำสั่งเริ่มชาร์จใหม่ (RemoteStart จาก A หรือ B, บัตร) ตู้ตรวจธงของหัวนั้นก่อน

| สภาพ | ผล |
|---|---|
| `start_transaction_SW=3` แต่ `remote_start_received=0` (session จบแล้ว) | ปลดทันที รับคำสั่งใหม่ |
| `remote_start_received=1` โดยไม่มีการยิง PLC และไม่มีคำสั่งรอยิง | ปลดทันที |
| ยิง PLC แล้วไม่มี StartTransaction เกิน 180 วินาที และ PLC เป็น Available หรือ Preparing (ไม่มี session ของรถ) | ปลด พร้อมการจองหัวของคำสั่งเดิม |
| การจองหัวใน router ค้างเกิน 180 วินาทีโดยไม่มีอะไรเริ่ม | ปลด |
| ยิง PLC ไม่ถึง 180 วินาที, หรือ PLC เป็น Charging / SuspendedEV / SuspendedEVSE / Finishing / Faulted, หรืออ่านสถานะ PLC ไม่ได้ | **ไม่ปลด** รถอาจกำลังเริ่มชาร์จ ปฏิเสธเหมือนเดิม |
| มี transaction, คำสั่งรอยิง PLC, บัตรกำลังเริ่ม, หัวกำลัง Finishing, เริ่มจากหน้าจอ | **ไม่ปลด** |

- การจองหัวที่อายุไม่ถึง 180 วินาทีจะไม่ถูกปลด เพราะอาจเป็นคำสั่งที่กำลังรับอยู่พอดี
- ระหว่าง Finishing (สายยังเสียบ) คำสั่งเริ่มใหม่ยังถูกปฏิเสธ `previous session finishing - unplug the cable first` ตาม OCPP เหมือนเดิม ต้องถอดสายแล้วเสียบใหม่
- เวลา 180 วินาทีเปลี่ยนได้ด้วย environment `REMOTE_START_LATCH_TIMEOUT_SECONDS` ของ service (ต่ำสุด 30) ปกติไม่ต้องตั้ง
- ไม่มีเลขบัตรใน log ที่เพิ่ม

## ดูใน log

```
[START-LATCH] C2 RemoteStart session ended before StartTransaction; sending Finishing without StopTransaction
[START-LATCH] C2 released remote_start_received_c2=1, start_transaction_SW2=3, CSMS claim: session ended before StartTransaction
[START-LATCH] C2 released start_transaction_SW2=3, CSMS claim: cable removed before StartTransaction
[START-LATCH] C2 released start_transaction_SW2=3: stale, start_transaction_SW2=3 left after its RemoteStart session ended
[START-LATCH] C2 released remote_start_received_c2=1, start_transaction_SW2=3, CSMS claim: stale, RemoteStart sent to the PLC 200 s ago and no StartTransaction (PLC Preparing)
```

- `grep "START-LATCH" /home/pi/ocpp/*.log`
- `bash ~/edgebox_r31.sh diag` แสดงบรรทัดเหล่านี้ในส่วน `refusals, latches and faults` และค่าธงของแต่ละหัวในบรรทัด `head 1` / `head 2`

## ไฟล์ที่เปลี่ยนจาก R3.1.5

| ไฟล์ | สิ่งที่เปลี่ยน |
|---|---|
| `CP.py` | `_clear_remote_start_latch()`, `_heal_stale_start_latch()`, `_stale_start_latch_reason()`, `_remote_start_latch_age()`; เรียกจาก Finishing, ถอดสาย, ยกเลิก, ตัวตรวจก่อนเริ่มชาร์จ; จำเวลาที่รับ RemoteStart และที่ยิง PLC; boot reset ล้างธง RemoteStart |
| `helpf/dual_csms.py` | `reserved_age()`; RemoteStart ให้ตู้ปลดธงค้างก่อนตรวจการจองหัว |
| `tests/test_r316_start_latch.py` | ทดสอบใหม่ 59 กรณี รวมการเล่นซ้ำเหตุการณ์ 10 พ.ย. ผ่าน router จริงกับ A/B จำลอง |
| `tests/test_r311_start_refusal_logging.py` | 2 กรณีที่ใช้ธงตัวเดียว (ซึ่ง R3.1.6 ปลดให้) เปลี่ยนเป็นธงของคำสั่งที่ยังอาจเริ่มอยู่ |

ไฟล์อื่นที่ลงตู้ตรงกับ R3.1.5 ทุกไบต์ ไม่มีการตั้งค่าเพิ่ม ฐานข้อมูลเดิมใช้ต่อได้ ผลต่างทั้งหมดอยู่ใน `CHANGES_R315_TO_R316.patch`

## ติดตั้ง

ทั้งสองหัวต้องว่าง (ถอดสาย) ก่อน install ใส่ `RATED_W` เป็นกำลังตามป้ายของตู้

```
bash ~/edgebox_r31.sh stage
RATED_W=120000 bash ~/edgebox_r31.sh install /home/pi/ocpp
```

## ผลทดสอบ

- ชุดทดสอบบนเครื่อง 585/585 บน Linux Python 3.13.18, ocpp 2.1.0, websockets 15.0.1, pytest 9.1.1
- ชุดทดสอบใหม่เมื่อรันกับโค้ด R3.1.5 ไม่ผ่าน และได้ข้อความปฏิเสธเดียวกับที่ตู้บันทึกไว้ วันที่ 10 พ.ย.
- ติดตั้งบนตู้ทดสอบ 10 พ.ย. 2026: ชุดทดสอบผ่าน 585/585 บนตู้ (Python 3.11.2, ocpp 1.0.0, websockets 17.1) ชาร์จหัวเดียวและสองหัวพร้อมกันได้ตามปกติ
- กรณีรายการจบก่อน StartTransaction ยังไม่เกิดซ้ำกับรถจริงหลังติดตั้ง ยืนยันด้วยชุดทดสอบเท่านั้น
