# R3.1.7 — 10 พฤศจิกายน 2026

R3.1.7 คือ R3.1.6 ที่แก้ **แตะบัตรแล้ว CSMS ตอบ Accepted แต่ตู้ไม่เริ่มชาร์จ** ที่พบบนตู้ทดสอบเมื่อ 10 พ.ย. 2026

## อาการที่พบ (หัว 1)

| เวลา | เหตุการณ์ |
|---|---|
| 14:17:49 | เสียบสาย |
| 14:17:51 | รถเข้าสถานะ C (CableCheck) ซึ่ง PLC รายงานเป็น SuspendedEV |
| 14:17:54 | แตะบัตร A ตอบ Accepted แต่ตู้ไม่สั่ง PLC เริ่ม และไม่มีบรรทัดใน log |
| 14:18:54 | รถรอครบแล้วจบ session เอง |

เกิดซ้ำอีกครั้งเวลา 14:19:18

## สาเหตุ

ทางบัตร (`topic/RFIDCommand`) เริ่มได้เฉพาะตอน PLC เป็น Available หรือ Preparing รถเข้าสถานะรอ (SuspendedEV) ภายใน 2-3 วินาทีหลังเสียบสาย การแตะบัตรหลังจากนั้นจึงถูกทิ้งเงียบ ๆ ทั้งที่บัตรผ่านแล้ว R3.1.2 แก้เรื่องเดียวกันนี้ให้ RemoteStart ไปแล้ว แต่ทางบัตรยังไม่ได้แก้

## สิ่งที่แก้

1. **บัตรเริ่มชาร์จได้ขณะรถรอ** PLC เป็น Preparing, SuspendedEV หรือ SuspendedEVSE (ไม่มี transaction) เหมือน RemoteStart
2. **แตะบัตรตอนยังไม่ได้เสียบสาย ตู้ไม่ทำอะไร** ไม่ถาม CSMS ไม่จองหัว ไม่สั่ง PLC เดิมตู้สั่ง PLC แล้วยกเลิกฝั่งตัวเองทันที แต่ controller ถือสิทธิ์ชาร์จของหัวนั้นค้างไว้ 5 นาทีโดยไม่มีรายการ OCPP รองรับ **ต้องเสียบสายก่อนแตะบัตร**
3. **ทุกครั้งที่บัตรไม่ได้เริ่มชาร์จ มีเหตุผลใน log** ไม่มีเลขบัตร

ยังปฏิเสธเหมือนเดิม: PLC เป็น Charging / Finishing / Faulted, มี transaction อยู่, หัวกำลัง Finishing, หัวถูกจองโดยคำสั่งอื่น ถ้ารถค้างใน CurrentDemand แล้ว (PLC รายงาน Charging ประมาณ 26 วินาทีหลังเสียบสายบนตู้ทดสอบ) ทั้งบัตรและ RemoteStart ถูกปฏิเสธด้วยเหตุผล `PLC reports Charging`

## ดูใน log

```
[CARD-START] C1 card accepted, start sent to the PLC (PLC SuspendedEV)
[CARD-START] C2 card not started: no vehicle connected - plug in, then tap the card
[CARD-START] C1 card accepted but not started: PLC reports Charging
[CARD-START] C1 card not started: previous session finishing - unplug the cable first
[CARD-START] C1 card not started: not accepted by a CSMS
```

- `grep -a "CARD-START" /home/pi/ocpp/*.log`
- `bash ~/edgebox_r31.sh diag` แสดงบรรทัดเหล่านี้ในส่วน `card checks at the charger`

## ไฟล์ที่เปลี่ยนจาก R3.1.6

| ไฟล์ | สิ่งที่เปลี่ยน |
|---|---|
| `CP.py` | `_dual_rfid_command_start()`: สถานะที่เริ่มได้, ไม่ทำอะไรเมื่อไม่มีรถ, log เหตุผล |
| `tests/test_r317_card_start.py` | ทดสอบใหม่ 10 กรณี รวมการเล่นซ้ำเหตุการณ์ 14:17:54 ผ่าน router จริง |
| `tests/test_cp_dual_hooks.py` | เพิ่มตัวแทน `_dual_busy_reason` 1 บรรทัดให้กรณีเดิม |

ไฟล์อื่นที่ลงตู้ตรงกับ R3.1.6 ทุกไบต์ ไม่มีการตั้งค่าเพิ่ม ฐานข้อมูลเดิมใช้ต่อได้ ผลต่างทั้งหมดอยู่ใน `CHANGES_R316_TO_R317.patch`

## ติดตั้ง

ทั้งสองหัวต้องว่าง (ถอดสาย) ก่อน install ใส่ `RATED_W` เป็นกำลังตามป้ายของตู้

```
bash ~/edgebox_r31.sh stage
RATED_W=120000 bash ~/edgebox_r31.sh install /home/pi/ocpp
```

## ผลทดสอบ

- ชุดทดสอบบนเครื่อง 595/595 บน Linux Python 3.13.18, ocpp 2.1.0, websockets 15.0.1, pytest 9.1.1
- ชุดทดสอบใหม่เมื่อรันกับโค้ด R3.1.6 ไม่ผ่าน
- ติดตั้งบนตู้ทดสอบ 10 พ.ย. 2026: ชุดทดสอบผ่าน 595/595 บนตู้ (Python 3.11.2, ocpp 1.0.0, websockets 17.1)
- ทดสอบกับบัตรและรถจริงบนตู้ทดสอบหลังติดตั้ง: บัตรเริ่มชาร์จได้ทั้งจาก Preparing และ SuspendedEV ทั้งสองหัว, บัตรที่ A ตอบ Invalid แล้ว B ตอบ Accepted ได้ session ของ B, บัตรที่ A ตอบ Accepted ได้ session ของ A, สองหัวชาร์จพร้อมกันโดยเจ้าของคนละ CSMS, แตะบัตรใบเดิมหยุดชาร์จได้, แตะบัตรตอนไม่มีรถตู้ไม่เริ่มและมีเหตุผลใน log
- หยุดด้วยบัตรคนละใบกับที่เริ่ม: ตู้ชาร์จต่อ (บัตรถูกกันที่ flow Node-RED ของหน้าจอ ซึ่งไม่ได้อยู่ในแพ็กเกจนี้)
