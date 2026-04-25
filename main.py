import requests
import json
import time
import csv
import os
from datetime import datetime

# ==========================================
# إعدادات UChat و Chatwoot
# ==========================================
UCHAT_API_TOKEN = "vGxEMeYgSBK1k6OGCulg47Ei9JBdzJKCjrVtDdsNYR5hPpYu3p6THVeGGJsM"
CHATWOOT_BASE_URL = "https://chat.engosoft.com"
CHATWOOT_API_TOKEN = "Buvw2SUpLEJPydCEywhdUd8H"
ACCOUNT_ID = 2
INBOX_ID = 25
CSV_FILE_PATH = "data.csv"
PROCESSED_FILE = "processed_numbers.txt"

# ==========================================
# نظام Resume (حفظ التقدم)
# ==========================================
def load_processed():
    if not os.path.exists(PROCESSED_FILE):
        return set()
    with open(PROCESSED_FILE, "r", encoding="utf-8") as f:
        return set(line.strip() for line in f if line.strip())

def mark_processed(phone):
    with open(PROCESSED_FILE, "a", encoding="utf-8") as f:
        f.write(f"{phone}\n")

# ==========================================
# دوال UChat و Chatwoot
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
    except:
        return []

def get_or_create_contact(phone, name):
    headers = {"api_access_token": CHATWOOT_API_TOKEN, "Content-Type": "application/json"}
    safe_phone = phone if phone.startswith("+") else f"+{phone}"
    try:
        r = requests.get(f"{CHATWOOT_BASE_URL}/api/v1/accounts/{ACCOUNT_ID}/contacts/search?q={safe_phone}", headers=headers, timeout=30)
        if r.status_code == 200:
            results = r.json().get("payload", [])
            if results:
                return results[0]["id"]
        r2 = requests.post(f"{CHATWOOT_BASE_URL}/api/v1/accounts/{ACCOUNT_ID}/contacts", headers=headers, json={"name": name, "phone_number": safe_phone}, timeout=30)
        if r2.status_code == 200:
            return r2.json()["payload"]["contact"]["id"]
    except Exception as e:
        print(f"  ❌ خطأ contact: {e}")
    return None

def create_conversation(contact_id):
    headers = {"api_access_token": CHATWOOT_API_TOKEN, "Content-Type": "application/json"}
    source_id = datetime.now().strftime('%Y%m%d%H%M%S') + str(contact_id)
    payload = {"source_id": source_id, "inbox_id": INBOX_ID, "contact_id": contact_id, "status": "resolved"}
    try:
        r = requests.post(f"{CHATWOOT_BASE_URL}/api/v1/accounts/{ACCOUNT_ID}/conversations", headers=headers, json=payload, timeout=30)
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
        except:
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
        t = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M') if ts else "وقت غير معروف"
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
# نقطة البداية
# ==========================================
def run():
    print(f"📂 فتح الملف: {CSV_FILE_PATH}")
    processed = load_processed()
    print(f"📌 تم معالجتهم مسبقاً: {len(processed)}")

    try:
        with open(CSV_FILE_PATH, mode='r', encoding='utf-8-sig') as f:
            rows = list(csv.DictReader(f))
            total = len(rows)
            session_count = 0

            for i, row in enumerate(rows, 1):
                user_ns = row.get('user_ns', '').strip()
                phone = row.get('phone', '').strip()
                name = (row.get('name') or row.get('first_name') or "عميل مستورد").strip()

                if not phone or phone in ["n.a", ""]:
                    continue
                if phone in processed:
                    print(f"⏭️ [{i}/{total}] تخطي: {phone}")
                    continue

                print(f"\n{'='*40}")
                print(f"🔄 [{i}/{total}] {name} | {phone}")
                print('='*40)

                migrate_user(phone, user_ns, name)
                mark_processed(phone)
                session_count += 1

                # استراحة كل 500 عميل
                if session_count % 500 == 0:
                    print("\n🛑 استراحة 60 ثانية لحماية الـ API...")
                    time.sleep(60)
                else:
                    time.sleep(2)

        print("\n✅ اكتملت عملية النقل بالكامل!")

    except FileNotFoundError:
        print(f"❌ الملف غير موجود: {CSV_FILE_PATH}")
    except Exception as e:
        print(f"❌ خطأ: {e}")

if __name__ == "__main__":
    run()
