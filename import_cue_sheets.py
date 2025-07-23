import gspread
import csv
from datetime import datetime
import os
import sys
# Google Drive API를 직접 사용하기 위해 필요한 라이브러리 임포트
from google.oauth2 import service_account
from googleapiclient.discovery import build

# --- 설정 (이 부분을 사용자 환경에 맞게 수정하세요) ---
# 1. Google Sheets API 및 Google Drive API 활성화, 서비스 계정 생성:
#    - Google Cloud Console (console.cloud.google.com)에 접속합니다.
#    - 새 프로젝트를 생성하거나 기존 프로젝트를 선택합니다.
#    - 'API 및 서비스' -> '라이브러리'에서 'Google Sheets API'와 'Google Drive API'를 검색하여 **둘 다 활성화**합니다.
#    - 'API 및 서비스' -> '사용자 인증 정보'에서 '사용자 인증 정보 만들기' -> '서비스 계정'을 선택합니다.
#    - 서비스 계정 이름을 지정하고 역할을 '프로젝트' -> '뷰어' 또는 'Google Sheets API' -> 'Google Sheets 뷰어' 및 'Google Drive API' -> 'Google Drive 뷰어'로 설정합니다.
#      (읽기 전용이므로 '뷰어' 권한으로 충분합니다.)
#    - 서비스 계정 생성 후, JSON 키를 생성하고 다운로드합니다. 이 파일의 경로를 아래 `SERVICE_ACCOUNT_FILE_PATH`에 입력하세요.
#    - 다운로드한 JSON 파일의 이름은 일반적으로 '프로젝트이름-서비스계정ID.json' 형식입니다.

# --- 중요: 서비스 계정 JSON 파일의 전체 경로를 여기에 입력하세요. ---
# 이 방법이 가장 권장되며, private_key 문자열 오류를 방지합니다.
# 예시 (macOS): '/Users/heeeonlee/Downloads/queuesheetsmaker-abcdef123456.json'
# 예시 (Windows): 'C:\\Users\\heeeonlee\\Downloads\\queuesheetsmaker-abcdef123456.json'
SERVICE_ACCOUNT_FILE_PATH = '/Users/heeeonlee/2025KYSA/QueueSheets/queuesheetsmaker-1505a6a800f7.json' # 다운로드한 서비스 계정 JSON 파일의 전체 경로를 여기에 입력하세요.


# 2. 큐시트 파일들이 들어있는 Google Drive 폴더 ID를 여기에 입력하세요.
#    폴더 ID는 Google Drive에서 해당 폴더의 URL에서 찾을 수 있습니다.
#    예: https://drive.google.com/drive/folders/YOUR_FOLDER_ID
#    주의: URL 매개변수 (예: ?ths=true)를 제외하고 순수한 폴더 ID만 입력해야 합니다.
SOURCE_FOLDER_ID = '1_B_ISMcVGWzYhbgitdyq25NASher8rkb' # 개별 큐시트 스프레드시트 파일들이 있는 Google Drive 폴더의 ID를 여기에 입력하세요.

# 3. CSV 파일을 저장할 로컬 디렉토리 경로를 지정하세요.
#    이 폴더가 없으면 스크립트가 자동으로 생성합니다.
OUTPUT_DIRECTORY = 'downloaded_csv_files'

# --- 함수 정의 ---

def download_each_sheet_to_csv(source_folder_id, output_directory, service_account_file_path):
    """
    지정된 Google Drive 폴더 내의 각 Google 스프레드시트 파일을 읽어
    각각의 내용을 별도의 로컬 CSV 파일로 다운로드합니다.

    - 소스 파일: 지정된 폴더 내의 모든 Google 스프레드시트 파일
    - 각 소스 파일의 첫 번째 시트를 다운로드 대상으로 간주합니다.
    - 각 CSV 파일은 원본 스프레드시트 파일의 제목을 기반으로 이름을 가집니다.
    """
    print(f"현재 실행 중인 Python 인터프리터: {sys.executable}")
    print(f"gspread 버전: {gspread.__version__}")

    # 서비스 계정 파일 경로 유효성 검사
    if not os.path.exists(service_account_file_path):
        print(f"오류: 서비스 계정 JSON 파일 경로가 올바르지 않거나 파일이 존재하지 않습니다: '{service_account_file_path}'")
        print("경로를 다시 확인하거나, 파일을 해당 경로에 놓아주세요.")
        sys.exit(1) # 스크립트 종료

    # 출력 디렉토리 생성 (없으면)
    os.makedirs(output_directory, exist_ok=True)
    print(f"CSV 파일을 '{output_directory}' 디렉토리에 저장합니다.")

    try:
        # gspread를 사용하여 Google Sheets API에 인증 (스프레드시트 열기용)
        print(f"서비스 계정 파일 '{service_account_file_path}'로 gspread 인증 시도 중...")
        gc = gspread.service_account(filename=service_account_file_path)
        print(f"gspread 인증 성공! gc 객체 타입: {type(gc)}")

        # Google Drive API를 직접 사용하여 파일 목록을 가져오기 위한 인증
        print("Google Drive API 서비스 인증 시도 중...")
        creds = service_account.Credentials.from_service_account_file(
            service_account_file_path,
            scopes=['https://www.googleapis.com/auth/drive.readonly'] # Drive 읽기 전용 스코프
        )
        # Drive API 서비스 빌드
        drive_service = build('drive', 'v3', credentials=creds)
        print("Google Drive API 서비스 인증 성공!")

        print(f"폴더 '{source_folder_id}'에서 스프레드시트 파일 검색 중...")
        
        # Drive API 쿼리
        query = f"'{source_folder_id}' in parents and mimeType='application/vnd.google-apps.spreadsheet' and trashed = false"
        
        # files().list() 메서드를 사용하여 파일 목록을 가져옵니다.
        # fields='files(id, name)'를 통해 필요한 정보(id, name)만 요청하여 효율성을 높입니다.
        response = drive_service.files().list(q=query, fields='files(id, name)').execute()
        spreadsheet_files_raw = response.get('files', [])

        # gspread의 list_spreadsheet_files와 유사한 형식으로 변환
        spreadsheet_files = [{'id': f['id'], 'title': f['name']} for f in spreadsheet_files_raw]


        if not spreadsheet_files:
            print(f"오류: 폴더 '{source_folder_id}'에서 Google 스프레드시트 파일을 찾을 수 없습니다. 폴더 ID와 파일 존재 여부를 확인하세요.")
            sys.exit(1)

        print(f"찾은 스프레드시트 파일: {[f['title'] for f in spreadsheet_files]}")

        download_count = 0
        # 각 스프레드시트 파일을 순회하며 개별 CSV로 다운로드
        for file_info in spreadsheet_files:
            spreadsheet_id = file_info['id']
            # 파일 제목에서 특수 문자를 제거하고 CSV 파일 이름으로 사용
            safe_file_title = "".join(c for c in file_info['title'] if c.isalnum() or c in (' ', '.', '_')).strip()
            if not safe_file_title: # 제목이 비어있을 경우 대체 이름 사용
                safe_file_title = f"untitled_spreadsheet_{spreadsheet_id[:8]}"

            output_csv_filename = os.path.join(output_directory, f"{safe_file_title}.csv")
            
            print(f"스프레드시트 파일 '{file_info['title']}' (ID: {spreadsheet_id}) 처리 중...")

            try:
                # 스프레드시트 ID로 파일 열기 (open_by_key 사용)
                current_spreadsheet = gc.open_by_key(spreadsheet_id)
                
                # 각 파일의 첫 번째 시트를 다운로드 대상으로 간주
                # 시트가 여러 개일 수 있으므로, 첫 번째 시트의 제목을 가져와 사용
                if not current_spreadsheet.worksheets():
                    print(f"경고: 스프레드시트 '{file_info['title']}'에 시트가 없습니다. 건너뜝니다.")
                    continue

                worksheet_to_download = current_spreadsheet.worksheets()[0]
                print(f"  -> 시트 '{worksheet_to_download.title}'의 모든 데이터 가져오기 중...")
                all_values = worksheet_to_download.get_all_values()

                if not all_values:
                    print(f"  경고: 시트 '{worksheet_to_download.title}'에 데이터가 없습니다. 빈 CSV 파일이 생성됩니다.")

                # CSV 파일로 데이터 쓰기
                with open(output_csv_filename, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerows(all_values) # 가져온 모든 행을 CSV 파일에 씁니다.

                print(f"  -> '{file_info['title']}'의 시트 '{worksheet_to_download.title}' 내용이 '{output_csv_filename}'(으)로 성공적으로 다운로드되었습니다.")
                download_count += 1

            except gspread.exceptions.SpreadsheetNotFound:
                print(f"경고: 스프레드시트 ID '{spreadsheet_id}'를 찾을 수 없거나 접근 권한이 없습니다. 건너뜝니다.")
                continue
            except Exception as e:
                print(f"경고: 스프레드시트 '{file_info['title']}'를 처리하는 중 오류 발생 ({e}). 건너뜝니다.")
                # 상세한 트레이스백 출력 (디버깅용)
                # import traceback
                # traceback.print_exc()
                continue

        print(f"\n총 {download_count}개의 스프레드시트 파일이 개별 CSV 파일로 다운로드되었습니다.")
        print(f"모든 CSV 파일은 '{output_directory}' 디렉토리에 저장되었습니다.")

    except gspread.exceptions.APIError as e:
        print(f"Google API 오류가 발생했습니다: {e}")
        print("서비스 계정 권한 또는 Google Sheets/Drive API 활성화 상태를 확인하세요.")
        sys.exit(1)
    except Exception as e:
        print(f"예상치 못한 오류가 발생했습니다: {e}")
        print("자세한 오류 정보:")
        import traceback
        traceback.print_exc() # 상세한 트레이스백 출력
        sys.exit(1)

# --- 스크립트 실행 ---
if __name__ == "__main__":
    # 'path/to/your/service_account.json' 부분을 다운로드한 JSON 파일의 실제 경로로 변경해야 합니다.
    # SOURCE_FOLDER_ID에 스프레드시트 파일들이 있는 Google Drive 폴더의 ID를 입력해야 합니다.
    # 주의: SOURCE_FOLDER_ID는 URL 매개변수 (예: ?ths=true) 없이 순수한 폴더 ID만 입력해야 합니다.
    download_each_sheet_to_csv(SOURCE_FOLDER_ID, OUTPUT_DIRECTORY, SERVICE_ACCOUNT_FILE_PATH)
