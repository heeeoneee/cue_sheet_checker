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
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer
from reportlab.lib.units import inch
from io import BytesIO
import csv

# 🔐 인증 설정
# Google Sheets에서 데이터를 읽어오므로 'spreadsheets' 스코프가 필요합니다.
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets', # Google Sheets에서 데이터 읽기용
    'https://www.googleapis.com/auth/drive'         # Google Drive에 PDF 업로드용
]
TOKEN_PATH = 'token.json'
CREDENTIALS_PATH = 'client_secret.json'

def authorize():
    """Google API 인증 정보를 로드하거나 새로 생성합니다."""
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

# 🗓️ 날짜 및 Google Drive 폴더 매핑 설정
sheet_options = {
    "1": "8.13(수)"
}
day_tag_map = {
    "수": "wed",
    "목": "thu",
    "금": "fri",
    "토": "sat",
    "일": "sun"
}
day_folder_map = {
    "wed": "1pPZ82GHrFb4HxMOyRxhnFWddp0vOHJ74", # 여기에 실제 수요일 Google Drive 폴더 ID를 입력하세요!
    "thu": "1xALX7GS0PuqpCgfiqkApPABthniSDmoy",
    "fri": "1CRsyJlPCqW7eSA2Cn_lsD3W6qTOIizJk",
    "sat": "19gdA0OTvQ7sZBGvgxvsh-9Bq2BXP-yS5",
    "sun": "1n6zRW-V8XUDUOSucAxecB9ps2jzdSLxF"
}

print("📋 요청에 따라 수요일 시트만 처리합니다.")
choice = "1"
sheet_name = sheet_options[choice]
csv_filename = sheet_name + ".csv" # 로컬에 저장될 CSV 파일 이름

day_kor = sheet_name.split("(")[-1].strip(")")
day_tag = day_tag_map.get(day_kor)
if not day_tag:
    raise ValueError(f"❌ 알 수 없는 요일 태그: {day_kor}")
folder_id = day_folder_map.get(day_tag)
if not folder_id:
    raise ValueError(f"❌ 해당 요일에 매핑된 폴더 ID가 없습니다: {day_tag}")

# 🌐 서비스 객체 생성 (Sheets 및 Drive)
creds = authorize()
gc = gspread.authorize(creds)
drive_service = build("drive", "v3", credentials=creds)

# 스프레드시트 키
SPREADSHEET_ID = "1Vu6j1GYGu7_mCLSMfjbxkYDOrXBavnbTNzQOjZiIUgk"

def download_sheet_as_csv(spreadsheet_id, sheet_name, output_filename, gspread_client):
    """
    Google 스프레드시트의 특정 시트 데이터를 가져와 CSV 파일로 로컬에 저장합니다.
    """
    print(f"\n📥 Google Sheet '{sheet_name}'에서 데이터 추출 중...")
    try:
        spreadsheet = gspread_client.open_by_key(spreadsheet_id)
        source_sheet = spreadsheet.worksheet(sheet_name)
        all_values = source_sheet.get_all_values()

        with open(output_filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerows(all_values)
        print(f"✅ '{sheet_name}' 시트 데이터가 '{output_filename}'으로 성공적으로 저장되었습니다.")
        return True, all_values
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"❌ 오류: 스프레드시트 ID '{spreadsheet_id}'를 찾을 수 없습니다.")
        return False, None
    except gspread.exceptions.WorksheetNotFound:
        print(f"❌ 오류: 시트 이름 '{sheet_name}'을(를) 찾을 수 없습니다.")
        return False, None
    except Exception as e:
        print(f"❌ Google Sheet에서 CSV 추출 중 오류 발생: {e}")
        return False, None

# --- [새로 추가된 데이터 전처리 함수: 모든 빈 셀을 위 셀 내용으로 채움] ---
def fill_data_down_all_columns(data_rows):
    """
    주어진 2D 리스트(테이블 데이터)에서 각 열의 빈 셀을 바로 위 행의 같은 열 값으로 채워 넣습니다.
    스프레드시트의 병합 해제 시뮬레이션에 유용합니다.
    """
    if not data_rows:
        return []

    # 원본 데이터 수정 방지를 위해 깊은 복사 (리스트의 리스트)
    processed_data = [list(row) for row in data_rows]

    # 모든 행이 동일한 길이를 가지도록 최대 열 수를 기준으로 확장
    max_cols = max(len(row) for row in processed_data)
    for r_idx in range(len(processed_data)):
        while len(processed_data[r_idx]) < max_cols:
            processed_data[r_idx].append("") # 빈 문자열로 채워 길이 맞춤

    # 각 열별로 마지막으로 채워진 값을 저장할 리스트
    # 첫 행의 값이 0이거나 빈 문자열인 경우를 대비해 None으로 초기화
    last_filled_values = [None] * max_cols

    for r_idx, row in enumerate(processed_data):
        for c_idx in range(max_cols):
            current_cell_value = row[c_idx].strip() # 현재 셀 값 (공백 제거)

            if current_cell_value: # 현재 셀에 값이 있으면
                last_filled_values[c_idx] = current_cell_value # 이 값을 '마지막 채워진 값'으로 업데이트
            elif last_filled_values[c_idx] is not None: # 현재 셀이 비어있고, 이전에 채워진 값이 있다면
                row[c_idx] = last_filled_values[c_idx] # 이전 값으로 현재 셀을 채움
            # 만약 현재 셀이 비어있고, 'last_filled_values[c_idx]'도 None이면 (예: 해당 열의 첫 행부터 비어있는 경우),
            # 해당 셀은 비워둔 채로 유지됩니다.

        processed_data[r_idx] = row # 수정된 행을 다시 할당

    return processed_data
# --- [새로 추가된 데이터 전처리 함수 끝] ---


# 📄 데이터 준비: Google Sheet에서 CSV를 다운로드하고 데이터를 로드합니다.
success, raw_data = download_sheet_as_csv(SPREADSHEET_ID, sheet_name, csv_filename, gc)
if not success or not raw_data:
    print("❌ 데이터 로드에 실패했습니다. 스크립트를 종료합니다.")
    exit()

# CSV 데이터의 모든 빈 셀을 위 셀 내용으로 채우는 전처리
print("🔧 CSV 데이터의 모든 빈 셀을 위 셀 내용으로 채우는 중...")
data = fill_data_down_all_columns(raw_data)
print("✅ 데이터 전처리 완료.")


# 참가자 목록 및 그룹 시작 열 인덱스
participants = ["남윤범", "안가현", "이희언","김지혜", "최윤영", "장정현", "최현수"]
# 전처리된 데이터를 기반으로 헤더와 본문 분리
# CSV로 내보내졌을 때 스프레드시트의 1행부터 데이터가 시작한다고 가정하면,
# header_rows = data[0:2] (첫 2행), body_rows = data[2:] (나머지 행)가 될 수 있습니다.
# CSV 파일의 실제 구조를 확인하고 필요에 따라 이 값을 조정하세요.
header_rows = data[1:3] # 전처리된 데이터의 2번째, 3번째 행을 헤더로 사용 (0-based index)
body_rows = data[3:]   # 전처리된 데이터의 4번째 행부터 본문으로 사용 (0-based index)

# 그룹 시작 열 (고정된 첫 6개 열 다음부터 시작하는 각 그룹의 첫 열 인덱스)
group_starts = [6, 9, 12, 15, 18, 21]

# 📦 상태 변수
success_list = []
fail_list = []
skipped_list = []
lock = Lock()
created_count = 0
moved_count = 0
start_count = 0

def delete_all_files_in_folder():
    """지정된 Google Drive 폴더의 모든 파일을 삭제합니다."""
    with lock:
        print("🧹 Google Drive 폴더 내 기존 파일들을 모두 삭제합니다...")
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
                        print(f"🗑️ 삭제됨: {f['name']}")
                    break
                except Exception as e:
                    with lock:
                        print(f"❌ {f['name']} 삭제 시도 {attempt + 1} 실패: {e}")
                    time.sleep(2)
    except Exception as e:
        with lock:
            print(f"❌ 모든 파일 삭제 실패: {e}")

def create_pdf(data_rows, title):
    """주어진 데이터를 사용하여 PDF 파일을 메모리상에서 생성합니다."""
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter)
    styles = getSampleStyleSheet()
    story = []

    story.append(Paragraph(f"<b>{title}</b>", styles['h1']))
    story.append(Spacer(1, 0.2 * inch))

    for row_index, row in enumerate(data_rows):
        row_text = []
        for cell in row:
            row_text.append(str(cell).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))
        
        if row_index == 0:
            story.append(Paragraph("<b>" + " | ".join(row_text) + "</b>", styles['Normal']))
        else:
            story.append(Paragraph(" | ".join(row_text), styles['Normal']))
        story.append(Spacer(1, 0.05 * inch))

    doc.build(story)
    buffer.seek(0)
    return buffer

def upload_file_to_drive(file_buffer, file_name, mime_type, local_drive_service, index, total, max_retries=3):
    """Google Drive에 파일을 업로드합니다."""
    global moved_count
    file_metadata = {
        'name': file_name,
        'parents': [folder_id],
        'mimeType': mime_type
    }
    media_body = {'mime_type': mime_type, 'body': file_buffer}

    for attempt in range(1, max_retries + 1):
        with lock:
            print(f"⬆️ ({index}/{total}) '{file_name}' 업로드 중 (시도 {attempt})...")
        try:
            file = local_drive_service.files().create(
                body=file_metadata,
                media_body=media_body,
                fields='id'
            ).execute()
            with lock:
                moved_count += 1
                print(f"✅ ({index}/{total}) '{file_name}' → 성공적으로 업로드됨. 파일 ID: {file.get('id')}")
            return True
        except Exception as e:
            with lock:
                print(f"❌ '{file_name}' 업로드 실패 (시도 {attempt}): {e}")
            time.sleep(2)
    with lock:
        print(f"🔥 '{file_name}' → {max_retries}번 시도 후 업로드 실패")
    return False

def make_sheet_file(name):
    """각 참가자별 데이터를 처리하여 PDF를 생성하고 Google Drive에 업로드합니다."""
    global created_count, moved_count, start_count
    index = None
    with lock:
        start_count += 1
        index = start_count
        print(f"\n📝 ({index}/{len(participants)}) '{name}'의 데이터 처리 중...")

    local_drive_service = build("drive", "v3", credentials=creds)

    active_set_indexes = []
    for i, s in enumerate(group_starts):
        for row in body_rows:
            if len(row) > s + 2 and (name in row[s+1] or name in row[s+2]):
                active_set_indexes.append(i)
                break

    if not active_set_indexes:
        with lock:
            print(f"⚠️ '{name}': 할당된 역할이 없습니다. 건너뜁니다.")
            skipped_list.append(name)
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
                if len(row) > s + 2:
                    new_row += [row[s], mark(row[s+1]), mark(row[s+2])]
                else:
                    new_row += ["", "", ""]
            result.append(new_row)

        now_time_str = datetime.now().strftime("%H%M%S")
        pdf_file_name = f"{name}_{day_tag}_{now_time_str}.pdf"
        pdf_buffer = create_pdf(result, f"{name} Sheet (생성일: {datetime.now().strftime('%Y-%m-%d %H:%M')})")

        with lock:
            print(f"🛠️  ({index}/{len(participants)}) PDF 버퍼 생성 완료: '{pdf_file_name}'")

        if upload_file_to_drive(pdf_buffer, pdf_file_name, 'application/pdf', local_drive_service, index, len(participants)):
            with lock:
                success_list.append(name)
        else:
            with lock:
                fail_list.append(name)
    except Exception as e:
        with lock:
            print(f"❌ '{name}' 처리 중 오류 발생: {e}")
            fail_list.append(name)

# ▶️ 스크립트 실행 시작
delete_all_files_in_folder()

with ThreadPoolExecutor(max_workers=4) as executor:
    executor.map(make_sheet_file, participants)

# 📊 최종 결과 요약
print("\n📊 요약")
print(f"✅ 완료: {len(success_list)}명 → {', '.join(success_list) if success_list else '없음'}")
print(f"⚠️ 건너뜀 (역할 없음): {len(skipped_list)}명 → {', '.join(skipped_list) if skipped_list else '없음'}")
print(f"❌ 실패: {len(fail_list)}명 → {', '.join(fail_list) if fail_list else '없음'}")