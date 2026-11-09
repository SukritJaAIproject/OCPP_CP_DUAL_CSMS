# R3.1.2 — 9 พฤศจิกายน 2026

R3.1.2 คือ R3.1.1 ที่แก้ 2 จุดซึ่งเจอตอนทดสอบบน Edgebox ทดสอบในโรงงาน (192.168.3.14) วันที่ 9 พ.ย. 2026 ทั้งสองจุดอยู่ในส่วนที่ R3 เพิ่มเข้ามา โค้ดเดิมของตู้ไม่มีปัญหานี้

## 1. RemoteStart หลังเสียบสายถูกปฏิเสธ (PLC reports SuspendedEV)

**อาการ:** เสียบรถแล้วกด RemoteStart จาก A ตู้ตอบ Rejected ทุกครั้ง (log ของ R3.1.1: `[A] RemoteStart C1 rejected: PLC reports SuspendedEV`) จนรถรอครบเวลาแล้วยกเลิกเอง และตู้ส่ง Faulted (PREPARING TIMEOUT)

**สาเหตุ:** รถที่เสียบแล้วรอการอนุญาตจะอยู่ใน CP state C แต่ยังไม่มี CurrentDemand ซึ่ง PLC รายงานเป็น SuspendedEV ส่วนที่เช็กว่าหัวว่างของ R3 รับเฉพาะ Available/Preparing ทั้งที่ตอนรายงานสถานะ CP.py ถือว่า SuspendedEV ที่ยังไม่มีรายการคือ Preparing

**การแก้:** `_dual_busy_reason()` ถือว่า SuspendedEV/SuspendedEVSE ที่ยังไม่มีรายการเป็นหัวที่ว่าง เหมือนที่ CP.py รายงานต่อ CSMS ส่วน Charging ที่มีกระแสจริงแต่ไม่มีรายการยังถือว่าไม่ว่าง เงื่อนไขอื่นทั้งหมดและลำดับการเช็กเหมือนเดิม

## 2. ตอนบูต R3 ส่ง Available ทับ Faulted ของ Vector

**อาการ:** 15:15 ตอนบูตขณะตัวควบคุม Vector ยังมีฟอลต์โมดูลกำลังไฟ (หัว 1: PM_G1_A1, PM_G1_A2 / หัว 2: PM_G2_A3) ตู้ส่ง Faulted ไป A/B ก่อน แล้วไม่ถึงวินาทีต่อมาส่ง Available ทับ ทำให้หน้าเว็บเห็นว่าหัวว่างทั้งที่ฟอลต์ยังอยู่

**สาเหตุ:** ส่วนบริการของ R3 (ประกาศสถานะตอนเริ่ม, ChangeAvailability, การจอง) ส่ง StatusNotification ของตัวเอง โดยใช้สถานะจาก PLC ซึ่งไม่ได้ดูฟอลต์ของ Vector

**การแก้:**
- ระหว่างที่ Vector ยังมีฟอลต์ของหัวใด สถานะจากส่วนบริการของ R3 สำหรับหัวนั้นจะถูกตัดทิ้ง (log `[VECTOR-FAULT-SUPPRESS] ... StatusNotification(service ...) DROPPED`) เหมือนที่โค้ดเดิมทำกับสถานะปกติ
- สถานะที่ใช้ตัดสินการเริ่มชาร์จจะเห็นหัวนั้นเป็น Faulted ทำให้ RemoteStart ถูกปฏิเสธพร้อมเหตุผล `connector status Faulted` แทนที่จะรับคำสั่งแล้วจ่ายไฟไม่ได้
- เมื่อฟอลต์หาย โค้ดเดิมของ Vector bridge จะส่งสถานะจริงกลับไปเหมือนเดิม

## ไฟล์ที่เปลี่ยนจาก R3.1.1

| ไฟล์ | สิ่งที่เปลี่ยน |
|---|---|
| `CP.py` | ข้อ 1 และข้อ 2 (`_dual_busy_reason`, `_ocpp_physical_status`, `_ocpp_vector_fault_active`, `_ocpp_hides_service_status`, `ChargePoint.call`) |
| `helpf/ocpp_runtime.py` | เพิ่ม `service_status_active()` บอกว่ากำลังส่งสถานะจากส่วนบริการ |
| `tests/test_r312_vector_status.py` | ทดสอบใหม่ข้อ 2 |
| `tests/test_r311_start_refusal_logging.py` | ปรับกรณี SuspendedEV ตามข้อ 1 และเพิ่ม SuspendedEVSE, Charging, Faulted |
| `tools/cp_protocol_defs.py` | สร้างใหม่ เปลี่ยนเฉพาะบรรทัด hash ของ CP.py |

## ผลทดสอบ

- ชุดทดสอบบนเครื่อง 491/491 บน Linux Python 3.13, ocpp 2.1.0, websockets 15.0.1
- ยังไม่ได้รันชุดทดสอบกับ CSMS A/B จริงซ้ำหลังการแก้นี้

ผลต่างทั้งหมดอยู่ใน `CHANGES_R311_TO_R312.patch` ส่วนที่มาของ R3.1.1 ดู `R311_CHANGES_TH.md`
