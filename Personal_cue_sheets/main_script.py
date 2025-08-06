import pandas as pd
import os
import re
import pickle
from weasyprint import HTML, CSS
from weasyprint.text.fonts import FontConfiguration
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# --- 설정 ---
CUESHEET_FILE = '/Users/heeeonlee/2025KYSA/QueueSheets/initial_csv_files/2025 KYSA 운영위원 통합 큐시트_도우미 배정용서기용.csv'
HELPERS_FILE = '/Users/heeeonlee/2025KYSA/QueueSheets/initial_csv_files/2025 KYSA 운영위원 통합 큐시트_도우미 명단.csv'
OUTPUT_FOLDER = 'output'
PARENT_FOLDER_ID = '1aCox5dOJcpePxGleo9qPAJBvAhOGWk74'
SCOPES = ['https://www.googleapis.com/auth/drive']

COLUMNS_TO_DROP_FOR_PDF = ['요일', '세부 내용', '필요 도우미 수', '도우미 역할\n(최대한 구체적으로)']
DAY_MAP = {'목요일': '목', '금요일': '금', '토요일': '토', '일요일': '일'}
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_FILE = os.path.join(SCRIPT_DIR, 'template.html')
CSS_FILE = os.path.join(SCRIPT_DIR, 'style.css')
os.makedirs(os.path.join(SCRIPT_DIR, OUTPUT_FOLDER), exist_ok=True)


# --- 구글 드라이브 연동 함수 ---
def get_gdrive_service():
    creds = None
    token_path = os.path.join(SCRIPT_DIR, 'token.pickle')
    credentials_path = os.path.join(SCRIPT_DIR, 'credentials.json')
    if os.path.exists(token_path):
        with open(token_path, 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(credentials_path, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(token_path, 'wb') as token:
            pickle.dump(creds, token)
    return build('drive', 'v3', credentials=creds)

def upload_to_drive(service, day_to_upload):
    print(f"\n☁️ '{day_to_upload}' 폴더를 구글 드라이브에 업로드합니다...")
    query = f"'{PARENT_FOLDER_ID}' in parents and name='{day_to_upload}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    response = service.files().list(q=query, fields='files(id, name)').execute()
    day_folder = response.get('files', [])
    if not day_folder:
        print(f"'{day_to_upload}' 폴더를 새로 생성합니다.")
        file_metadata = {'name': day_to_upload, 'mimeType': 'application/vnd.google-apps.folder', 'parents': [PARENT_FOLDER_ID]}
        day_folder_id = service.files().create(body=file_metadata, fields='id').execute().get('id')
    else:
        day_folder_id = day_folder[0].get('id')
        print(f"기존 '{day_to_upload}' 폴더의 내용을 삭제합니다.")
        folder_items = service.files().list(q=f"'{day_folder_id}' in parents and trashed=false", fields='files(id)').execute().get('files', [])
        for item in folder_items:
            service.files().delete(fileId=item['id']).execute()
    local_folder_path = os.path.join(SCRIPT_DIR, OUTPUT_FOLDER, day_to_upload)
    for filename in os.listdir(local_folder_path):
        if filename.endswith('.pdf'):
            print(f"  - 업로드 중: {filename}")
            file_metadata = {'name': filename, 'parents': [day_folder_id]}
            media = MediaFileUpload(os.path.join(local_folder_path, filename), mimetype='application/pdf')
            service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    print("✅ 업로드 완료!")


# --- 기존 로직 함수들 ---
def get_helpers_by_day(selected_day_abbr):
    try:
        df_full = pd.read_csv(HELPERS_FILE, header=None, on_bad_lines='skip', dtype=str)
        name_row_idx = df_full[df_full[0] == '이름'].index[0]
        all_helpers = df_full.iloc[name_row_idx].dropna().tolist()[1:]
        day_row_idx = df_full[df_full[0] == selected_day_abbr].index[0]
        day_availability = df_full.iloc[day_row_idx].tolist()
        available_helpers = [h for i, h in enumerate(all_helpers) if i < len(day_availability) - 1 and str(day_availability[i+1]).strip() == '1']
        return available_helpers
    except Exception as e:
        print(f"❌ '{selected_day_abbr}' 요일 처리 중 오류: {e}"); return None

def clean_contact_cell(content):
    if not isinstance(content, str): return content
    s_content = content.strip()
    if s_content.isdigit() and len(s_content) == 10 and s_content.startswith('1'): return '0' + s_content
    def fmt(m):
        n, num = m.group(1), m.group(2)
        if len(num) == 10 and num.startswith('1'): num = '0' + num
        return f'{n} ({num})'
    return re.sub(r'([가-힣A-Za-z]+)\s+(\d+)', fmt, content).replace('\n', ', ').strip()

def generate_sheets_for_day(selected_day, cuesheet_df):
    print(f"\n✅ '{selected_day}'의 큐시트 생성을 시작합니다.")
    selected_day_abbr = DAY_MAP[selected_day]
    available_helpers = get_helpers_by_day(selected_day_abbr)
    if not available_helpers: print(f"'{selected_day}'에 참석 가능한 도우미가 없습니다."); return
    day_df = cuesheet_df[cuesheet_df['요일'] == selected_day].copy()
    if day_df.empty: print(f"'{selected_day}'에 해당하는 일정이 없습니다."); return
    day_output_folder = os.path.join(SCRIPT_DIR, OUTPUT_FOLDER, selected_day)
    os.makedirs(day_output_folder, exist_ok=True)
    print("\n" + "="*40)
    for name in available_helpers:
        indices = [i for i, row in day_df.iterrows() if name in " ".join([str(c) for c in row.values])]
        if not indices: continue
        pdf = day_df.loc[list(set(indices))].copy().sort_values(by=['시작시간_정렬용']).drop(columns=['시작시간_정렬용'])
        csv_path = os.path.join(day_output_folder, f"{name}_큐시트.csv")
        pdf.to_csv(csv_path, index=False, encoding='utf-8-sig', na_rep='')
        print(f"📄 CSV 생성: {selected_day}/{os.path.basename(csv_path)}")
        try:
            pdf_df = pdf.drop(columns=COLUMNS_TO_DROP_FOR_PDF, errors='ignore').copy()
            for col in pdf_df.columns:
                if pdf_df[col].dtype == 'object':
                    if col in ['담당자\n(프로그램 팀원 명)', '담당자 연락처']: pdf_df[col] = pdf_df[col].apply(clean_contact_cell)
                    else: pdf_df[col] = pdf_df[col].astype(str).str.replace('\n', '<br>', regex=False)
            pdf_path = os.path.join(day_output_folder, f"{name}_큐시트.pdf")
            html_table = pdf_df.to_html(index=False, na_rep='', escape=False).replace('<th>', '<th style="text-align: center;">')
            with open(TEMPLATE_FILE, 'r', encoding='utf-8') as f: template = f.read()
            final_html = template.replace('{{HELPER_NAME}}', name).replace('{{SELECTED_DAY}}', selected_day).replace('{{SCHEDULE_TABLE}}', html_table)
            HTML(string=final_html, base_url=SCRIPT_DIR).write_pdf(pdf_path, stylesheets=[CSS(filename=CSS_FILE)])
            print(f"🎨 PDF 생성: {selected_day}/{os.path.basename(pdf_path)}")
        except Exception as e: print(f"❗ PDF 생성 실패: {e}")
        print("-"*40)

# --- 메인 실행 로직 ---
if __name__ == '__main__':
    # 1. 기존 파일 확인 및 업로드 여부 질문
    existing_days = [d for d in DAY_MAP.keys() if os.path.isdir(os.path.join(SCRIPT_DIR, OUTPUT_FOLDER, d)) and any(f.endswith('.pdf') for f in os.listdir(os.path.join(SCRIPT_DIR, OUTPUT_FOLDER, d)))]
    if existing_days:
        print("\n🔎 기존에 생성된 PDF 파일이 있습니다.")
        if input("📤 기존 파일들을 구글 드라이브에 업로드하시겠습니까? (y/n): ").lower().strip() == 'y':
            print("\n어떤 요일을 업로드하시겠습니까?")
            for i, day in enumerate(existing_days): print(f"  {i+1}. {day}")
            print(f"  {len(existing_days)+1}. 전체")
            try:
                choice = int(input(">> 번호를 입력하세요: ")) - 1
                if 0 <= choice <= len(existing_days):
                    service = get_gdrive_service()
                    days_to_upload = existing_days if choice == len(existing_days) else [existing_days[choice]]
                    for day in days_to_upload:
                        upload_to_drive(service, day)
                else: print("❌ 잘못된 번호입니다.")
            except (ValueError, IndexError): print("❌ 잘못된 입력입니다.")
            except Exception as e: print(f"❌ 구글 드라이브 처리 중 오류: {e}")

    # 2. 새로 파일 생성 여부 질문
    if input("\n📝 새로 큐시트 파일을 생성하시겠습니까? (y/n): ").lower().strip() == 'y':
        days = list(DAY_MAP.keys())
        print("\n🗓️ 개인별 큐시트를 생성할 요일을 선택해주세요.")
        for i, day in enumerate(days): print(f"  {i+1}. {day}")
        print(f"  {len(days)+1}. 전체 요일")
        try:
            choice = int(input(">> 번호를 입력하세요: ")) - 1
            if 0 <= choice <= len(days):
                cuesheet_df = pd.read_csv(CUESHEET_FILE, dtype=str)
                cuesheet_df['요일'] = cuesheet_df['요일'].astype(pd.api.types.CategoricalDtype(categories=days, ordered=True))
                cuesheet_df['시작시간_정렬용'] = pd.to_datetime(cuesheet_df['시작시간'], format='%H:%M', errors='coerce').dt.time
                
                days_to_process = days if choice == len(days) else [days[choice]]
                for day in days_to_process:
                    generate_sheets_for_day(day, cuesheet_df)
                
                print("\n✨ 모든 파일 생성이 완료되었습니다!")
                
                # 3. 새로 생성된 파일 업로드 여부 질문
                if input("\n📤 방금 생성된 PDF 파일들을 구글 드라이브에 업로드하시겠습니까? (y/n): ").lower().strip() == 'y':
                    service = get_gdrive_service()
                    for day in days_to_process:
                        upload_to_drive(service, day)
            else: print("❌ 잘못된 번호입니다.")
        except (ValueError, IndexError): print("❌ 잘못된 입력입니다.")
        except Exception as e: print(f"❌ 파일 생성 또는 업로드 중 오류: {e}")

    print("\n프로그램을 종료합니다.")