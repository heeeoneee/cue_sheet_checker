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
df_new.columns = df_new.iloc[2] # 3번째 행(인덱스 2)을 헤더로 설정
df_new = df_new.iloc[3:].reset_index(drop=True) # 헤더와 그 이전 행 제거 및 인덱스 재설정

print("--- 새 파일, 열 이름 지정 및 행 제거 후 DataFrame의 열 이름 ---")
print(df_new.columns.tolist())
print("\n")

# --- 3. 첫 번째(0번 인덱스)와 두 번째(1번 인덱스) 열의 빈 셀을 조건 없이 ffill()로 채우기 ---
# 열이 충분히 존재하는지 확인하여 오류 방지
if len(df_new.columns) > 0:
    first_col_name = df_new.columns[0]
    df_new[first_col_name] = df_new[first_col_name].ffill()
    print(f"'{first_col_name}' 열의 빈칸을 조건 없이 ffill()로 채웠습니다.")

if len(df_new.columns) > 1:
    second_col_name = df_new.columns[1]
    df_new[second_col_name] = df_new[second_col_name].ffill()
    print(f"'{second_col_name}' 열의 빈칸을 조건 없이 ffill()로 채웠습니다.")
print("\n")

# --- 4. 컬럼명이 "일정"을 포함하는 모든 열에 ffill() 적용 ---
# (이 부분은 첫 두 열이 '일정' 컬럼인 경우 중복 ffill이 발생할 수 있으나 결과에는 영향 없음)
schedule_cols = []
for col_name in df_new.columns:
    if "일정" in str(col_name):
        df_new[col_name] = df_new[col_name].ffill()
        schedule_cols.append(col_name)
        print(f"'{col_name}' 열의 빈칸을 ffill()로 채웠습니다.")
print("\n")

# --- 5. '장소', '세부 내용' 등의 열을 "일정" 열을 기준으로 채우기 또는 '-' 넣기 ---
# 대상이 되는 상세 정보 열들 정의 (사용자 정의 컬럼명 반영)
target_detail_cols = [
    "장소", "세부 내용", "재료", "담당자\n(프로그램 팀원 명)",
    "필요 도우미 수", "도우미 역할\n(최대한 구체적으로)", "배정된 도우미 이름"
]

# 실제 데이터프레임에 존재하는 열만 필터링하여 작업 안정성 확보
existing_target_detail_cols = [col for col in target_detail_cols if col in df_new.columns]

# '일정'이 없는 경우 '-'로 채워지는 로직에서 제외해야 하는 첫 두 열을 정의합니다.
cols_to_exclude_from_dash_fill = []
if len(df_new.columns) > 0:
    cols_to_exclude_from_dash_fill.append(df_new.columns[0])
if len(df_new.columns) > 1:
    cols_to_exclude_from_dash_fill.append(df_new.columns[1])

# '일정'이 없거나 '-'인 경우 '-'로 채워질 대상 컬럼 리스트를 최종적으로 정의
# 즉, 'existing_target_detail_cols' 중 첫 두 열이 아닌 것들만 대상이 됩니다.
cols_for_conditional_dash_fill = [
    col for col in existing_target_detail_cols
    if col not in cols_to_exclude_from_dash_fill
]


if not existing_target_detail_cols:
    print("지정된 '장소', '세부 내용' 등의 상세 정보 열이 데이터에 없습니다. 해당 작업은 건너뜁니다.")
else:
    if not schedule_cols:
        print("경고: '일정' 컬럼이 발견되지 않아, 상세 정보 컬럼 채우기 로직이 정상적으로 동작하지 않을 수 있습니다.")
    else:
        # 모든 '일정' 컬럼이 NaN이거나 '-'인 행을 식별
        # '일정' 컬럼은 여러 개일 수 있으므로, 모든 '일정' 컬럼이 해당 조건을 만족해야 합니다.
        is_schedule_row_empty_or_dash = (df_new[schedule_cols].isnull() | (df_new[schedule_cols] == '-')).all(axis=1)

        for col in existing_target_detail_cols:
            # 모든 상세 정보 열에 대해 먼저 자체 ffill()을 적용합니다.
            df_new[col] = df_new[col].ffill()

            # 첫 두 열이 아니고, '일정'이 없거나 '-'인 행에 대해서만 해당 컬럼을 '-'로 채움
            if col in cols_for_conditional_dash_fill:
                df_new.loc[is_schedule_row_empty_or_dash, col] = '-'
                print(f"'{col}' 열: '일정'이 없거나 '-'인 행에 '-'를 적용했습니다.")
            else:
                # 첫 두 열은 해당 로직에서 제외되었음을 알림 (선택 사항)
                print(f"'{col}' 열은 첫 두 열이므로 '일정' 기준의 '-' 적용 로직에서 제외되었습니다.")

# --- 5.5. 모든 남은 빈 셀을 '-'로 채우는 로직 추가 ---
# 이전의 모든 로직이 적용된 후, 최종적으로 NaN 값을 '-'로 대체합니다.
df_new = df_new.fillna('-')
print("--- 모든 남은 빈 셀을 '-'로 채웠습니다. ---")
print("\n")


print("\n--- 최종 처리 후 DataFrame의 첫 5행 ---")
print(df_new.head())
print("\n")

# --- 6. 수정된 DataFrame을 새로운 CSV 파일로 저장 ---
output_file_path_new_csv = '/Users/heeeonlee/2025KYSA/QueueSheets/processed_csv_files/2025 KYSA 운영위원 통합 큐시트_8.14목_processed.csv'

# 출력 디렉토리 경로 추출 및 존재하지 않으면 생성
output_directory_new_csv = os.path.dirname(output_file_path_new_csv)
if not os.path.exists(output_directory_new_csv):
    os.makedirs(output_directory_new_csv)
    print(f"디렉토리 '{output_directory_new_csv}'를 생성했습니다.")

# 수정된 DataFrame을 새로운 CSV 파일로 저장
df_new.to_csv(output_file_path_new_csv, index=False)

print(f"새 파일이 성공적으로 처리되어 '{output_file_path_new_csv}'에 저장되었습니다.")