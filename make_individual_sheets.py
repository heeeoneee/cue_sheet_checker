import ssl
ssl._create_default_https_context = ssl._create_unverified_context

import os
import gspread
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from datetime import datetime
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

# 🔐 인증 설정
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]
TOKEN_PATH = 'token.json'
CREDENTIALS_PATH = 'client_secret.json'

def authorize():
    creds = None
    if os.path.exists(TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_PATH, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_PATH, 'w') as token:
            token.write(creds.to_json())
    return creds

# 🌐 서비스 객체 생성
creds = authorize()
gc = gspread.authorize(creds)
drive_service = build("drive", "v3", credentials=creds)

# 📄 데이터 준비
spreadsheet = gc.open_by_key("11zYr2RK27OFRRL5iTX9BN7UyQgwP5TB2qnTb-l9Davw")
source_sheet = spreadsheet.worksheet("8.14(목)")
data = source_sheet.get_all_values()
participants = ["남윤범", "안가현", "이희언", "김지혜"]
group_starts = [6, 9, 12, 15, 18, 21]
header_rows = data[1:3]
body_rows = data[3:]
folder_id = "1E7qIyPd9DCu1Mhgc5haevO4r-CVusTQT"

# 📦 상태 변수
success_list = []
fail_list = []
lock = Lock()
created_count = 0
moved_count = 0
start_count = 0

def delete_all_files_in_folder():
    with lock:
        print("🧹 Deleting all existing files in the folder...")
    local_drive_service = build("drive", "v3", credentials=creds)
    query = f"'{folder_id}' in parents"
    try:
        results = local_drive_service.files().list(q=query, fields="files(id, name)").execute()
        files = results.get("files", [])
        for f in files:
            for attempt in range(3):
                try:
                    local_drive_service.files().delete(fileId=f["id"]).execute()
                    with lock:
                        print(f"🗑️ Deleted: {f['name']}")
                    break
                except Exception as e:
                    with lock:
                        print(f"❌ Retry {attempt + 1} failed to delete {f['name']}: {e}")
                    time.sleep(2)
    except Exception as e:
        with lock:
            print(f"❌ Failed to delete all files: {e}")

def move_to_folder(file_id, name, local_drive_service, index, total, max_retries=3):
    global moved_count
    for attempt in range(1, max_retries + 1):
        with lock:
            print(f"📁 ({index}/{total}) Moving file to folder for {name} (Attempt {attempt})...")
        try:
            local_drive_service.files().update(
                fileId=file_id,
                addParents=folder_id,
                fields="id, parents"
            ).execute()
            with lock:
                moved_count += 1
                print(f"✅ ({index}/{total}) {name} → Moved to folder successfully")
            return True
        except Exception as e:
            with lock:
                print(f"❌ {name} folder move failed (Attempt {attempt}): {e}")
            time.sleep(2)
    with lock:
        print(f"🔥 {name} → Failed to move file to folder after {max_retries} attempts")
    return False

def make_sheet_file(name):
    global created_count, moved_count, start_count
    index = None
    with lock:
        start_count += 1
        index = start_count
        print(f"\n📝 ({index}/{len(participants)}) Generating sheet for {name}...")

    local_drive_service = build("drive", "v3", credentials=creds)
    active_set_indexes = [
        i for i, s in enumerate(group_starts)
        if any(name in row[s+1] or name in row[s+2] for row in body_rows)
    ]
    if not active_set_indexes:
        with lock:
            print(f"⚠️ {name}: No assigned roles. Skipping.")
            success_list.append(name)
        return

    try:
        result = []
        for header_row in header_rows:
            new_header = header_row[:6]
            for i in active_set_indexes:
                s = group_starts[i]
                new_header += header_row[s:s+3]
            result.append(new_header)

        for row in body_rows:
            new_row = row[:6]
            for i in active_set_indexes:
                s = group_starts[i]
                def mark(cell): return cell.replace(name, f"[{name}]") if name in cell else cell
                new_row += [row[s], mark(row[s+1]), mark(row[s+2])]
            result.append(new_row)

        now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
        sheet_name_str = datetime.now().strftime("%Y%m%d_%H%M")
        sheet_title = f"{name}_Sheet_{sheet_name_str}"
        new_sheet = gc.create(sheet_title)
        file_id = new_sheet.id
        sheet = new_sheet.sheet1
        sheet.update(range_name="A1", values=[[f"{name} Sheet (Updated {now_str})"]])
        sheet.update(range_name="A3", values=result)

        with lock:
            created_count += 1
            print(f"🛠️  ({index}/{len(participants)}) Created sheet: {sheet_title}")

        if move_to_folder(file_id, name, local_drive_service, index, len(participants)):
            with lock:
                success_list.append(name)
        else:
            with lock:
                fail_list.append(name)
    except Exception as e:
        with lock:
            print(f"❌ Error while processing {name}: {e}")
            fail_list.append(name)

# ▶️ 실행
delete_all_files_in_folder()

with ThreadPoolExecutor(max_workers=2) as executor:
    executor.map(make_sheet_file, participants)

# 📊 결과 요약
print("\n📊 Summary")
print(f"✅ Completed: {len(success_list)} → {', '.join(success_list) if success_list else 'None'}")
print(f"❌ Failed: {len(fail_list)} → {', '.join(fail_list) if fail_list else 'None'}")
