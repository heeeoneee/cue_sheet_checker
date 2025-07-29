import pandas as pd
import os

# --- 실제 CSV 파일 경로 ---
file_path_new_csv = '/Users/heeeonlee/2025KYSA/QueueSheets/initial_csv_files/2025 KYSA 운영위원 통합 큐시트_8.14목.csv'

# --- 1. CSV 파일 불러오기 (헤더 없이 불러옵니다) ---
df_new = pd.read_csv(file_path_new_csv, header=None)

print("--- 새 파일, 헤더 없이 불러온 DataFrame의 첫 5행 ---")
print(df_new.head())
print("\n")

# --- 2. 실제 헤더 행 설정 및 불필요한 상위 행 제거 ---
df_new.columns = df_new.iloc[2]
df_new = df_new.iloc[3:].reset_index(drop=True)

print("--- 새 파일, 열 이름 지정 및 행 제거 후 DataFrame의 열 이름 ---")
print(df_new.columns.tolist())
print("\n")

# --- 3. 컬럼명이 "일정"을 포함하는 모든 열에 ffill() 적용 (1차 채우기) ---
schedule_cols = []
for col_name in df_new.columns:
    if "일정" in str(col_name):
        df_new[col_name] = df_new[col_name].ffill()
        schedule_cols.append(col_name)
        print(f"'{col_name}' 열의 빈칸을 ffill()로 채웠습니다.")
print("\n")

# --- 4. '장소', '세부내용' 등의 열을 "일정" 열을 기준으로 채우기 또는 '-' 넣기 ---
# 대상이 되는 상세 정보 열들 정의
target_detail_cols = [
    "장소", "세부 내용", "재료", "담당자\n(프로그램 팀원 명)",
    "필요 도우미 수", "도우미 역할\n(최대한 구체적으로)", "배정된 도우미 이름"
]

# 실제 데이터프레임에 존재하는 열만 필터링
existing_target_detail_cols = [col for col in target_detail_cols if col in df_new.columns]

if not existing_target_detail_cols:
    print("지정된 '장소', '세부내용' 등의 상세 정보 열이 데이터에 없습니다. 해당 작업은 건너뜁니다.")
else:
    # '일정' 컬럼이 하나도 없는 경우 (예외 처리)
    if not schedule_cols:
        print("경고: '일정' 컬럼이 발견되지 않아, 상세 정보 컬럼 채우기 로직이 정상적으로 동작하지 않을 수 있습니다.")
        # 이 경우, 모든 상세 컬럼을 '-'로 채울지 아니면 기존 ffill을 유지할지 결정해야 합니다.
        # 여기서는 일단 기존 ffill 후 '-' 로직은 스킵합니다.
    else:
        # 모든 '일정' 컬럼이 NaN인 행을 식별 (즉, 해당 행에 정의된 '일정'이 없는 경우)
        # .all(axis=1)은 지정된 컬럼들 모두가 NaN일 때 True 반환
        is_schedule_row_completely_empty = df_new[schedule_cols].isnull().all(axis=1)

        for col in existing_target_detail_cols:
            # 먼저 해당 상세 정보 열 자체를 ffill() 합니다.
            # (일정 내용이 있을 때 자신의 이전 유효값으로 채워지도록)
            df_new[col] = df_new[col].ffill()

            # 모든 '일정' 컬럼이 비어있는(NaN) 행에 대해서는 해당 상세 정보 컬럼을 '-'로 채움
            # (이전 ffill() 결과를 덮어쓰게 됩니다.)
            df_new.loc[is_schedule_row_completely_empty, col] = '-'
            print(f"'{col}' 열: '일정'이 없는 행에 '-'를 적용했습니다.")
print("\n")

print("--- 최종 처리 후 DataFrame의 첫 5행 ---")
print(df_new.head())
print("\n")

# --- 5. 수정된 DataFrame을 새로운 CSV 파일로 저장 ---
output_file_path_new_csv = '/Users/heeeonlee/2025KYSA/QueueSheets/processed_csv_files/2025 KYSA 운영위원 통합 큐시트_8.14목_processed.csv'

# 출력 디렉토리 경로 추출 및 존재하지 않으면 생성
output_directory_new_csv = os.path.dirname(output_file_path_new_csv)
if not os.path.exists(output_directory_new_csv):
    os.makedirs(output_directory_new_csv)
    print(f"디렉토리 '{output_directory_new_csv}'를 생성했습니다.")

# 수정된 DataFrame을 새로운 CSV 파일로 저장
df_new.to_csv(output_file_path_new_csv, index=False)

print(f"새 파일이 성공적으로 처리되어 '{output_file_path_new_csv}'에 저장되었습니다.")