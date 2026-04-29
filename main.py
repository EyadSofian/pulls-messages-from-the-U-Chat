import requests
import time
import csv
import os
import sys
from datetime import datetime

# ==========================================
# الإعدادات من Environment Variables
# ==========================================
# استخدام .get() مع وضع قيم افتراضية لتتمكن من تشغيله محلياً (VS Code) دون الحاجة لضبط المتغيرات
UCHAT_API_TOKEN = os.environ.get("UCHAT_API_TOKEN", "vGxEMeYgSBK1k6OGCulg47Ei9JBdzJKCjrVtDdsNYR5hPpYu3p6THVeGGJsM")
CHATWOOT_BASE_URL = os.environ.get("CHATWOOT_BASE_URL", "https://chat.engosoft.com")
CHATWOOT_API_TOKEN = os.environ.get("CHATWOOT_API_TOKEN", "Buvw2SUpLEJPydCEywhdUd8H")
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
def fetch_uchat_messages(phone, user_ns, retries=3):
    url = "https://www.uchat.com.au/api/subscriber/chat-messages"
    headers = {"Authorization": f"Bearer {UCHAT_API_TOKEN}", "Accept": "application/json"}
    params = {"user_ns": user_ns} if user_ns else {"user_id": phone.replace("+", "")}
    
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=30)
            if r.status_code == 200:
                return r.json().get("data", [])
            return []
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            time.sleep(3)
        except Exception:
            return []
    return []

def get_or_create_contact(phone, name, retries=3):
    headers = {"api_access_token": CHATWOOT_API_TOKEN, "Content-Type": "application/json"}
    safe_phone = phone if phone.startswith("+") else f"+{phone}"
    
    for attempt in range(retries):
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
            else:
                print(f"  ❌ فشل إنشاء العميل (شات ووت): {r2.text}")
                return None
                
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            print(f"  ⚠️ تأخر سيرفر Chatwoot في جهة الاتصال ({attempt+1}/{retries})...")
            time.sleep(5)
        except Exception as e:
            print(f"  ❌ خطأ contact: {e}")
            return None
            
    print("  ❌ فشل نهائي في الاتصال بـ Chatwoot لجهة الاتصال.")
    return None

def create_conversation(contact_id, retries=3):
    headers = {"api_access_token": CHATWOOT_API_TOKEN, "Content-Type": "application/json"}
    source_id = str(int(time.time() * 1000))
    payload = {"source_id": source_id, "inbox_id": INBOX_ID, "contact_id": contact_id, "status": "resolved"}
    
    for attempt in range(retries):
        try:
            r = requests.post(
                f"{CHATWOOT_BASE_URL}/api/v1/accounts/{ACCOUNT_ID}/conversations",
                headers=headers, json=payload, timeout=30,
            )
            if r.status_code == 200:
                return r.json()["id"]
            print(f"  ❌ فشل إنشاء محادثة: {r.text}")
            return None
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            print(f"  ⚠️ تأخر سيرفر Chatwoot في المحادثة ({attempt+1}/{retries})...")
            time.sleep(5)
        except Exception as e:
            print(f"  ❌ خطأ conversation: {e}")
            return None
            
    print("  ❌ فشل نهائي في الاتصال بـ Chatwoot للمحادثة.")
    return None

def send_note(conv_id, content, retries=3):
    headers = {"api_access_token": CHATWOOT_API_TOKEN, "Content-Type": "application/json"}
    url = f"{CHATWOOT_BASE_URL}/api/v1/accounts/{ACCOUNT_ID}/conversations/{conv_id}/messages"
    payload = {"content": content, "message_type": "outgoing", "private": True}
    for attempt in range(retries):
        try:
            requests.post(url, headers=headers, json=payload, timeout=30)
            return True
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            print(f"  ⚠️ إعادة المحاولة ({attempt+1}/{retries})...")
            time.sleep(3)
        except Exception:
            pass
    return False

def migrate_user(phone, user_ns, name):
    messages = fetch_uchat_messages(phone, user_ns)
    if not messages:
        print("  📭 لا توجد رسائل.")
        return True # إرجاع True لأنه لا يوجد خطأ، فقط لا توجد رسائل
        
    messages.reverse()
    
    contact_id = get_or_create_contact(phone, name)
    if not contact_id:
        return False # إرجاع False لمنع حفظ الرقم كمكتمل
        
    conv_id = create_conversation(contact_id)
    if not conv_id:
        return False # إرجاع False لمنع حفظ الرقم كمكتمل
        
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
    return True

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
            continue

        print(f"\n{'='*40}")
        print(f"🔄 [{i}/{total}] {name} | {phone}")
        print("=" * 40)

        # 🚨 التعديل الهام: التأكد من نجاح النقل قبل حفظ التقدم 🚨
        success = migrate_user(phone, user_ns, name)
        
        if success:
            mark_processed(phone)
            processed.add(phone)
            session_count += 1
        else:
            print("  ⚠️ فشل الاتصال بسيرفر شات ووت. لم يتم حفظ الرقم كمكتمل. سيتم إيقاف السكريبت مؤقتاً لمدة 30 ثانية...")
            time.sleep(30) # الانتظار لعل السيرفر يعود للعمل

        if session_count > 0 and session_count % 500 == 0:
            print("\n🛑 استراحة 60 ثانية لحماية الـ API...")
            time.sleep(60)
        else:
            time.sleep(2)

    print(f"\n✅ اكتملت عملية النقل بالكامل! ({session_count} في هذه الجلسة)")

if __name__ == "__main__":
    run()
