import gspread
from google.oauth2.service_account import Credentials
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

# 인증
scopes = ["https://www.googleapis.com/auth/spreadsheets"]
credentials = Credentials.from_service_account_file("credentials.json", scopes=scopes)
gc = gspread.authorize(credentials)

# 루트 큐시트 열기
spreadsheet = gc.open_by_key("11zYr2RK27OFRRL5iTX9BN7UyQgwP5TB2qnTb-l9Davw")
source_sheet = spreadsheet.worksheet("8.14(목)")
data = source_sheet.get_all_values()

# 참가자 이름
participants = ["남윤범", "안가현", "이희언", "김지혜"]

# 역할 세트 시작 열 인덱스 (G:6, J:9, ..., X:21)
group_starts = [6, 9, 12, 15, 18, 21]

# 헤더는 2~3행
header_rows = data[1:3]
body_rows = data[3:]

def make_sheet(name):
    # 포함된 세트 탐색
    active_set_indexes = [
        i for i, s in enumerate(group_starts)
        if any(name in row[s+1] or name in row[s+2] for row in body_rows)
    ]

    if not active_set_indexes:
        return  # 아무 세트에도 없으면 생성 안 함

    # 헤더 구성
    result = []
    for header_row in header_rows:
        new_header = header_row[:6]
        for i in active_set_indexes:
            s = group_starts[i]
            new_header += header_row[s:s+3]
        result.append(new_header)

    # 본문 구성
    for row in body_rows:
        new_row = row[:6]
        for i in active_set_indexes:
            s = group_starts[i]
            new_row += row[s:s+3]
        result.append(new_row)

    # 시트 갱신
    try:
        sheet = spreadsheet.worksheet(name)
        spreadsheet.del_worksheet(sheet)
    except:
        pass
    sheet = spreadsheet.add_worksheet(name, rows=len(result) + 2, cols=len(result[0]))

    # 업데이트 시간
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    sheet.update("A1", [[f"{name} 큐시트 ({now_str} 업데이트)"]])
    sheet.update("A3", result)

    # 실시간 출력
    print(f"✅ [{now_str}] {name} 시트가 업데이트되었습니다.")

# 병렬 실행
with ThreadPoolExecutor(max_workers=5) as executor:
    executor.map(make_sheet, participants)
