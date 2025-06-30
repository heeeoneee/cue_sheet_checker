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

# 필요한 범위 정의
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

# 인증 파일 경로
TOKEN_PATH = 'token.json'
CREDENTIALS_PATH = 'client_secret.json'

# OAuth 인증 흐름
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

# 인증 객체 생성
creds = authorize()
gc = gspread.authorize(creds)
drive_service = build("drive", "v3", credentials=creds)

# 루트 시트 열기
spreadsheet = gc.open_by_key("11zYr2RK27OFRRL5iTX9BN7UyQgwP5TB2qnTb-l9Davw")
source_sheet = spreadsheet.worksheet("8.14(목)")
data = source_sheet.get_all_values()

# 운영위원 이름
participants = ["남윤범", "안가현", "이희언", "김지혜"]

# 각 역할 세트 시작 열 인덱스
group_starts = [6, 9, 12, 15, 18, 21]
header_rows = data[1:3]
body_rows = data[3:]

# 업로드할 Google Drive 폴더 ID
folder_id = "1E7qIyPd9DCu1Mhgc5haevO4r-CVusTQT"

# 드라이브 폴더 이동 함수 (재시도 포함)
def move_to_folder(file_id, name, max_retries=3):
    for attempt in range(1, max_retries + 1):
        try:
            drive_service.files().update(
                fileId=file_id,
                addParents=folder_id,
                fields="id, parents"
            ).execute()
            print(f"✅ {name} → 폴더 이동 완료 (시도 {attempt})")
            return
        except Exception as e:
            print(f"❌ {name} 폴더 이동 실패 (시도 {attempt}): {e}")
            time.sleep(2)
    print(f"🔥 {name} → 최종 폴더 이동 실패")

# 큐시트 생성 함수
def make_sheet_file(name):
    active_set_indexes = [
        i for i, s in enumerate(group_starts)
        if any(name in row[s+1] or name in row[s+2] for row in body_rows)
    ]
    if not active_set_indexes:
        print(f"⚠️ {name}: 할당된 역할이 없어 시트 생성을 생략합니다.")
        return

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
    new_sheet = gc.create(f"{name}_큐시트")
    file_id = new_sheet.id
    sheet = new_sheet.sheet1
    sheet.update(range_name="A1", values=[[f"{name} 큐시트 ({now_str} 업데이트)"]])
    sheet.update(range_name="A3", values=result)

    move_to_folder(file_id, name)
    time.sleep(1)

# 병렬 처리 (최대 2명 동시 처리)
with ThreadPoolExecutor(max_workers=2) as executor:
    executor.map(make_sheet_file, participants)
