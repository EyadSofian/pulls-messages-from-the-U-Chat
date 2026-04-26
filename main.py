import requests
import time
import csv
import os
import sys
from datetime import datetime

# ==========================================
# الإعدادات من Environment Variables
# ==========================================
UCHAT_API_TOKEN = os.environ["UCHAT_API_TOKEN"]
CHATWOOT_BASE_URL = os.environ.get("CHATWOOT_BASE_URL", "https://chat.engosoft.com")
CHATWOOT_API_TOKEN = os.environ["CHATWOOT_API_TOKEN"]
ACCOUNT_ID = int(os.environ.get("ACCOUNT_ID", "2"))
INBOX_ID = int(os.environ.get("INBOX_ID", "25"))
CSV_FILE_PATH = os.environ.get("CSV_FILE_PATH", "data.csv")
PROCESSED_FILE = os.environ.get("PROCESSED_FILE", "processed_numbers.txt")

# Soft timeout — exit cleanly قبل ما GitHub Actions يقتل الـ job (6h hard limit)
MAX_RUNTIME_SECONDS = int(os.environ.get("MAX_RUNTIME_SECONDS", "19800"))  # 5.5h default
START_TIME = time.time()

def time_up() -> bool:
    return (time.time() - START_TIME) >= MAX_RUNTIME_SECONDS

# ==========================================
# Resume system
# ==========================================
def load_processed():
    if not os.path.exists(PROCESSED_FILE):
        return set()
    with open(PROCESSED_FILE, "r", encoding="utf-8") as f:
        return set(line.strip() for line in f if line.strip())

def mark_processed(phone):
    with open(PROCESSED_FILE, "a", encoding="utf-8") as f:
        f.write(f"{phone}\n")
        f.flush()
        os.fsync(f.fileno())  # ضمان الكتابة على disk قبل أي crash

# ==========================================
# UChat / Chatwoot helpers
# ==========================================
def fetch_uchat_messages(phone, user_ns):
    url = "https://www.uchat.com.au/api/subscriber/chat-messages"
    headers = {"Authorization": f"Bearer {UCHAT_API_TOKEN}", "Accept": "application/json"}
    params = {"user_ns": user_ns} if user_ns else {"user_id": phone.replace("+", "")}
    try:
        r = requests.get(url, headers=headers, params=params, timeout=30)
        if r.status_code == 200:
            return r.json().get("data", [])
        return []
    except Exception:
        return []

def get_or_create_contact(phone, name):
    headers = {"api_access_token": CHATWOOT_API_TOKEN, "Content-Type": "application/json"}
    safe_phone = phone if phone.startswith("+") else f"+{phone}"
    try:
        r = requests.get(
            f"{CHATWOOT_BASE_URL}/api/v1/accounts/{ACCOUNT_ID}/contacts/search?q={safe_phone}",
            headers=headers, timeout=30
        )
        if r.status_code == 200:
            results = r.json().get("payload", [])
            if results:
                return results[0]["id"]
        r2 = requests.post(
            f"{CHATWOOT_BASE_URL}/api/v1/accounts/{ACCOUNT_ID}/contacts",
            headers=headers,
            json={"name": name, "phone_number": safe_phone},
            timeout=30,
        )
        if r2.status_code == 200:
            return r2.json()["payload"]["contact"]["id"]
    except Exception as e:
        print(f"  ❌ خطأ contact: {e}")
    return None

def create_conversation(contact_id):
    headers = {"api_access_token": CHATWOOT_API_TOKEN, "Content-Type": "application/json"}
    
    # 🚨 التعديل الهام هنا 🚨
    # استخدام Timestamp بالميلي ثانية يولد رقم فريد من 13 خانة (أقل من الحد الأقصى 15)
    source_id = str(int(time.time() * 1000))
    
    payload = {"source_id": source_id, "inbox_id": INBOX_ID, "contact_id": contact_id, "status": "resolved"}
    try:
        r = requests.post(
            f"{CHATWOOT_BASE_URL}/api/v1/accounts/{ACCOUNT_ID}/conversations",
            headers=headers, json=payload, timeout=30,
        )
        if r.status_code == 200:
            return r.json()["id"]
        print(f"  ❌ فشل إنشاء محادثة: {r.text}")
    except Exception as e:
        print(f"  ❌ خطأ conversation: {e}")
    return None

def send_note(conv_id, content, retries=3):
    headers = {"api_access_token": CHATWOOT_API_TOKEN, "Content-Type": "application/json"}
    url = f"{CHATWOOT_BASE_URL}/api/v1/accounts/{ACCOUNT_ID}/conversations/{conv_id}/messages"
    payload = {"content": content, "message_type": "outgoing", "private": True}
    for attempt in range(retries):
        try:
            requests.post(url, headers=headers, json=payload, timeout=30)
            return True
        except Exception:
            print(f"  ⚠️ إعادة المحاولة ({attempt+1}/{retries})...")
            time.sleep(3)
    return False

def migrate_user(phone, user_ns, name):
    messages = fetch_uchat_messages(phone, user_ns)
    if not messages:
        print("  📭 لا توجد رسائل.")
        return
    messages.reverse()
    contact_id = get_or_create_contact(phone, name)
    if not contact_id:
        return
    conv_id = create_conversation(contact_id)
    if not conv_id:
        return
    print(f"  ⏳ حقن {len(messages)} رسالة...")
    count = 0
    for msg in messages:
        ts = msg.get("ts", 0)
        t = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M") if ts else "وقت غير معروف"
        sender = "👤 [العميل]" if msg.get("type") == "in" else "🎧 [الموظف/البوت]"
        if msg.get("msg_type") in ["image", "file", "audio", "video"]:
            text = f"📎 مرفق:\n{msg.get('payload', {}).get('url', '')}"
        else:
            text = msg.get("content") or msg.get("payload", {}).get("text") or "رسالة غير مدعومة"
        note = f"📅 {t}\n{sender}:\n{text}"
        if send_note(conv_id, note):
            count += 1
        time.sleep(0.5)
    print(f"  🎉 {count}/{len(messages)} رسالة بنجاح.")

# ==========================================
# Entrypoint
# ==========================================
def run():
    print(f"📂 فتح الملف: {CSV_FILE_PATH}")
    print(f"⏱️  Soft timeout: {MAX_RUNTIME_SECONDS}s ({MAX_RUNTIME_SECONDS/3600:.1f}h)")
    processed = load_processed()
    print(f"📌 تم معالجتهم مسبقاً: {len(processed)}")

    if not os.path.exists(CSV_FILE_PATH):
        print(f"❌ الملف غير موجود: {CSV_FILE_PATH}")
        sys.exit(1)

    with open(CSV_FILE_PATH, mode="r", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))

    total = len(rows)
    session_count = 0

    for i, row in enumerate(rows, 1):
        if time_up():
            print(f"\n⏹️  وصلنا للـ soft timeout — exiting cleanly. هيكمل في الـ run الجاي.")
            print(f"📊 معالجة في هذه الجلسة: {session_count}")
            sys.exit(0)

        user_ns = row.get("user_ns", "").strip()
        phone = row.get("phone", "").strip()
        name = (row.get("name") or row.get("first_name") or "عميل مستورد").strip()

        if not phone or phone in ["n.a", ""]:
            continue
        if phone in processed:
            continue  # مفيش طباعة عشان مش يضرب الـ logs بـ 40K سطر

        print(f"\n{'='*40}")
        print(f"🔄 [{i}/{total}] {name} | {phone}")
        print("=" * 40)

        migrate_user(phone, user_ns, name)
        mark_processed(phone)
        processed.add(phone)
        session_count += 1

        if session_count % 500 == 0:
            print("\n🛑 استراحة 60 ثانية لحماية الـ API...")
            time.sleep(60)
        else:
            time.sleep(2)

    print(f"\n✅ اكتملت عملية النقل بالكامل! ({session_count} في هذه الجلسة)")

if __name__ == "__main__":
    run()
