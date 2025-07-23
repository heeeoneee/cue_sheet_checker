import ssl
ssl._create_default_https_context = ssl._create_unverified_context # SSL 인증 우회 (테스트 목적)

import os
import gspread
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

# 🔐 인증 설정 (기존 코드와 동일)
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive' # Drive 권한도 포함하여 혹시 모를 문제 방지
]
TOKEN_PATH = 'token.json'
CREDENTIALS_PATH = 'client_secret.json' # client_secret.json이 이 폴더에 있어야 합니다.

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

# 🌐 서비스 객체 생성 및 인증
creds = authorize()
gc = gspread.authorize(creds)
# https://docs.google.com/spreadsheets/d/1Adi9E4d_W1SRLzI3IotePrMzgQQ6Rc_xTDCYb3HaiZ0/edit?gid=0#gid=0
# 📄 테스트할 스프레드시트 ID (★★★★★ 여기에 새로 만든 스프레드시트 ID를 넣으세요 ★★★★★)
TEST_SPREADSHEET_ID = "1Adi9E4d_W1SRLzI3IotePrMzgQQ6Rc_xTDCYb3HaiZ0"
TEST_SHEET_NAME = "Sheet1" # 새로 만든 스프레드시트의 기본 시트 이름은 보통 'Sheet1'입니다.

try:
    print(f"Attempting to open spreadsheet with ID: {TEST_SPREADSHEET_ID}")
    spreadsheet = gc.open_by_key(TEST_SPREADSHEET_ID)
    print(f"Successfully opened spreadsheet: {spreadsheet.title}")

    worksheet = spreadsheet.worksheet(TEST_SHEET_NAME)
    data = worksheet.get_all_values()
    print(f"Successfully read data from '{TEST_SHEET_NAME}':")
    for row in data:
        print(row)
    print("Test successful!")

except gspread.exceptions.APIError as e:
    print(f"❌ API Error occurred: {e}")
    print("Please ensure:")
    print("1. The Google account you authenticated with has 'Editor' access to the spreadsheet.")
    print("2. Google Sheets API and Google Drive API are enabled in your Google Cloud project.")
    print("3. Your 'token.json' file was deleted before running the script (forcing re-authentication).")
    print("4. Your 'client_secret.json' file is correctly formatted for 'installed' application.")
except Exception as e:
    print(f"⚠️ An unexpected error occurred: {e}")