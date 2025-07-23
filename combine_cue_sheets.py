import os
import csv
from datetime import datetime, timedelta
import re # 정규 표현식 모듈 임포트

# --- 설정 (이 부분을 사용자 환경에 맞게 수정하세요) ---
# 1. 다듬어진 CSV 파일들이 있는 디렉토리 경로를 지정하세요.
#    이전 스크립트에서 다듬어진 CSV 파일들이 저장된 'processed_csv_files' 디렉토리를 지정하면 됩니다.
PROCESSED_INPUT_DIRECTORY = 'processed_csv_files'

# 2. 요일별로 병합된 CSV 파일들을 저장할 새로운 디렉토리 경로를 지정하세요.
#    이 폴더가 없으면 스크립트가 자동으로 생성합니다.
DAILY_OUTPUT_DIRECTORY = 'daily_combined_csvs'

# 3. 각 요일에 해당하는 파일명 접두사 (숫자) 매핑을 정의합니다.
#    '9'번 파일은 금요일과 토요일에 모두 포함됩니다.
#    이 리스트의 순서가 최종 CSV 파일에서 열이 붙는 순서가 됩니다.
#    --- 현재는 '목요일'만 처리하도록 임시 설정되었습니다. ---
DAY_MAPPINGS = {
    "목요일": [1, 2, 3, 4],
    # "금요일": [5, 6, 7, 8, 9], # 다른 요일은 주석 처리하여 이번 실행에서는 제외
    # "토요일": [10, 11, 12, 9],
    # "일요일": [13, 14]
}

# --- 함수 정의 ---

def generate_time_intervals(start_hour=5, start_minute=30, end_hour=24, end_minute=0, interval_minutes=5):
    """
    지정된 시간 범위 내에서 5분 단위의 시간 문자열 리스트를 생성합니다.
    예: "05:30", "05:35", ..., "24:00"
    """
    times = []
    # 기준 날짜는 중요하지 않으므로 임의의 날짜를 사용합니다.
    current_time = datetime(2000, 1, 1, start_hour, start_minute)

    # 끝 시간 datetime 객체 생성
    # 만약 end_hour가 24라면, 다음 날 00:00으로 처리합니다.
    if end_hour == 24:
        # end_dt를 다음 날 00:00으로 설정하여 24:00을 포함하도록 함
        end_dt = datetime(2000, 1, 2, 0, 0)
    else:
        # 일반적인 경우, 같은 날의 지정된 시간으로 설정
        end_dt = datetime(2000, 1, 1, end_hour, end_minute)

    while current_time <= end_dt:
        # 24:00 (다음 날 00:00)인 경우 특별 처리
        if current_time.hour == 0 and current_time.day == 2:
            times.append("24:00")
        else:
            times.append(current_time.strftime("%H:%M"))
        current_time += timedelta(minutes=interval_minutes)
    
    return times

def load_processed_csv_data(filepath):
    """
    다듬어진 CSV 파일에서 데이터를 읽어와 시간(키)과 나머지 데이터(값 리스트)의 딕셔너리로 반환합니다.
    다듬어진 CSV의 첫 번째 열이 시간이라고 가정합니다.
    """
    data = {}
    try:
        with open(filepath, 'r', newline='', encoding='utf-8') as infile:
            reader = csv.reader(infile)
            for row in reader:
                if row and row[0].strip(): # 행이 비어있지 않고 첫 번째 열(시간)이 비어있지 않다면
                    time_str = row[0].strip()
                    other_data = [col.strip() for col in row[1:]] # 시간 열 제외한 나머지 데이터
                    data[time_str] = other_data
    except FileNotFoundError:
        print(f"경고: '{os.path.basename(filepath)}' 파일을 찾을 수 없습니다. 건너킵니다.")
    except Exception as e:
        print(f"경고: '{os.path.basename(filepath)}' 파일 읽기 중 오류 발생 ({e}). 건너킵니다.")
    return data

def combine_daily_csv_files(input_dir, output_dir, day_mappings):
    """
    다듬어진 CSV 파일들을 요일별로 그룹화하고 병합하여 새로운 CSV 파일로 저장합니다.
    """
    if not os.path.exists(input_dir):
        print(f"오류: 입력 디렉토리 '{input_dir}'를 찾을 수 없습니다.")
        sys.exit(1)

    os.makedirs(output_dir, exist_ok=True)
    print(f"요일별 병합된 CSV 파일들을 '{output_dir}' 디렉토리에 저장합니다.")

    # 입력 디렉토리의 모든 CSV 파일 목록을 가져옵니다.
    all_processed_files = [f for f in os.listdir(input_dir) if f.endswith('.csv')]

    # 모든 요일에 대한 시간 간격 리스트를 미리 생성합니다.
    all_time_intervals = generate_time_intervals()

    for day, prefixes in day_mappings.items():
        print(f"\n--- {day} 파일 병합 시작 ---")
        
        # 현재 요일에 해당하는 파일들을 정해진 순서(prefixes)대로 수집합니다.
        ordered_day_files = []
        # 임시 딕셔너리: {접두사_숫자: [해당_파일경로1, 해당_파일경로2, ...]}
        # 동일한 접두사를 가진 파일이 여러 개 있을 경우를 대비 (예: '5_프로그램A.csv', '5_프로그램B.csv')
        found_files_by_prefix = {p: [] for p in prefixes} 

        for filename in all_processed_files:
            file_prefix_num = None
            # 파일명에서 숫자 접두사를 추출합니다. (예: '1_파일.csv' 또는 'processed_1_파일.csv')
            match = re.match(r'^(\d+)[._\s-]', filename) 
            if match:
                file_prefix_num = int(match.group(1))
            else: # 'processed_' 접두사가 붙은 경우도 고려
                match_processed = re.match(r'^processed_(\d+)[._\s-]', filename)
                if match_processed:
                    file_prefix_num = int(match_processed.group(1))
            
            if file_prefix_num is not None and file_prefix_num in prefixes:
                found_files_by_prefix[file_prefix_num].append(os.path.join(input_dir, filename))
        
        # DAY_MAPPINGS에 정의된 prefixes 순서대로 파일을 ordered_day_files에 추가합니다.
        for prefix_num in prefixes:
            # 해당 접두사를 가진 파일이 여러 개 있다면, 그 파일들은 os.listdir() 순서대로 추가됩니다.
            # 문제 해결의 초점은 '다른 접두사'를 가진 파일들의 순서이므로 이 방식은 유효합니다.
            ordered_day_files.extend(found_files_by_prefix.get(prefix_num, []))


        if not ordered_day_files:
            print(f"경고: {day}에 해당하는 파일을 '{input_dir}'에서 찾을 수 없습니다. 이 요일은 건너킵니다.")
            continue

        print(f"{day}에 포함될 파일 (정렬된 순서): {[os.path.basename(f) for f in ordered_day_files]}")

        # 요일별 병합 데이터를 저장할 딕셔너리 (시간: 병합된 데이터 리스트)
        daily_merged_data = {}
        max_cols_for_day = 1 # '시간' 열 포함

        # 각 파일에서 데이터를 로드하고 병합합니다. (ordered_day_files 순서대로)
        for filepath in ordered_day_files: # <-- 여기서 정렬된 순서대로 파일을 처리합니다.
            file_data = load_processed_csv_data(filepath)
            for time_str, data_cols in file_data.items():
                if time_str not in daily_merged_data:
                    daily_merged_data[time_str] = []
                # 기존 데이터 뒤에 새 파일의 데이터를 추가 (수평 병합)
                daily_merged_data[time_str].extend(data_cols)
                # 현재 시간대의 최대 열 개수를 업데이트합니다.
                if len(daily_merged_data[time_str]) + 1 > max_cols_for_day: # +1은 시간 열
                    max_cols_for_day = len(daily_merged_data[time_str]) + 1
        
        # 병합된 데이터를 최종 출력 형식으로 준비합니다.
        output_rows = []
        # 헤더 생성: '시간' + 동적으로 생성된 '컬럼 N'
        header = ['시간']
        for i in range(1, max_cols_for_day):
            header.append(f'컬럼 {i}') # 또는 더 의미 있는 이름 (예: '프로그램1_데이터1', '프로그램1_데이터2', '프로그램2_데이터1' 등)
        output_rows.append(header)

        # 모든 시간 간격에 대해 데이터를 채웁니다.
        for time_interval in all_time_intervals:
            row_data = [time_interval]
            if time_interval in daily_merged_data:
                row_data.extend(daily_merged_data[time_interval])
            
            # 모든 행의 열 개수를 max_cols_for_day에 맞춥니다.
            while len(row_data) < max_cols_for_day:
                row_data.append('')
            
            output_rows.append(row_data)
        
        # 요일별 CSV 파일로 저장합니다.
        output_filepath = os.path.join(output_dir, f"{day}.csv")
        with open(output_filepath, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.writer(outfile)
            writer.writerows(output_rows)
        
        print(f"--- {day} 병합 완료. '{output_filepath}'로 저장되었습니다. ---")

    print("\n모든 요일별 CSV 파일 병합이 완료되었습니다.")


# --- 스크립트 실행 ---
if __name__ == "__main__":
    # PROCESSED_INPUT_DIRECTORY에 이전 단계에서 다듬어진 CSV 파일들이 있는 폴더 경로를 지정하세요.
    # DAILY_OUTPUT_DIRECTORY에 요일별로 병합된 CSV 파일들이 저장될 새로운 폴더 경로를 지정하세요.
    combine_daily_csv_files(PROCESSED_INPUT_DIRECTORY, DAILY_OUTPUT_DIRECTORY, DAY_MAPPINGS)
