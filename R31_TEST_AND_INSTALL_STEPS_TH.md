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
