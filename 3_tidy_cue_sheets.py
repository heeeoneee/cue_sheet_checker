import pandas as pd
import os

# --- 실제 CSV 파일 경로 ---
file_path_new_csv = '/Users/heeeonlee/2025KYSA/QueueSheets/initial_csv_files/2025 KYSA 운영위원 통합 큐시트_8.14목.csv'

# --- 1. CSV 파일 불러오기 (헤더 없이 불러옵니다) ---
# 제공해주신 CSV 데이터 구조를 보니, 세 번째 행이 실제 헤더이므로 header=None으로 불러와야 합니다.
df_new = pd.read_csv(file_path_new_csv, header=None)

print("--- 새 파일, 헤더 없이 불러온 DataFrame의 첫 5행 ---")
print(df_new.head())
print("\n")

# --- 2. 실제 헤더 행 설정 및 불필요한 상위 행 제거 ---
# 제공된 데이터에서 3번째 행(pandas 인덱스 2)이 실제 열 이름들입니다.
# 이 행의 내용으로 열 이름을 설정합니다.
df_new.columns = df_new.iloc[2]

# 열 이름 설정 후, 실제 헤더로 사용한 행(인덱스 2)과 그 이전의 두 행(인덱스 0, 1)을 삭제합니다.
# 이제 데이터는 원래 엑셀의 4번째 행부터 시작하며, 인덱스는 0부터 재설정됩니다.
df_new = df_new.iloc[3:].reset_index(drop=True)


print("--- 새 파일, 열 이름 지정 및 행 제거 후 DataFrame의 열 이름 ---")
print(df_new.columns.tolist())
print("\n")

# --- 3. 첫 번째(0번 인덱스)와 두 번째(1번 인덱스) 열의 빈 셀을 ffill()로 채우기 ---
# 열 이름이 정확하게 설정되었으므로, 열 인덱스로 접근하여 ffill()을 적용합니다.
# 첫 번째 열의 이름: df_new.columns[0] (예: '전체 일정')
# 두 번째 열의 이름: df_new.columns[1] (예: '시설 일정 \n(준비 등)' 또는 '시설 일정 (준비 등)')
first_col_name = df_new.columns[0]
second_col_name = df_new.columns[1]

df_new[first_col_name] = df_new[first_col_name].ffill()
print(f"'{first_col_name}' 열의 빈칸을 채웠습니다.")

df_new[second_col_name] = df_new[second_col_name].ffill()
print(f"'{second_col_name}' 열의 빈칸을 채웠습니다.")
print("\n")


# --- 4. 수정된 DataFrame을 새로운 CSV 파일로 저장 ---
# 이 파일은 원본과 다른 이름 및 다른 폴더에 저장하는 것이 좋습니다.
output_file_path_new_csv = '/Users/heeeonlee/2025KYSA/QueueSheets/processed_csv_files/2025 KYSA 운영위원 통합 큐시트_8.14목_processed.csv'

# 출력 디렉토리 경로 추출 및 존재하지 않으면 생성
output_directory_new_csv = os.path.dirname(output_file_path_new_csv)
if not os.path.exists(output_directory_new_csv):
    os.makedirs(output_directory_new_csv)
    print(f"디렉토리 '{output_directory_new_csv}'를 생성했습니다.")

# 수정된 DataFrame을 새로운 CSV 파일로 저장
df_new.to_csv(output_file_path_new_csv, index=False)

print(f"새 파일이 성공적으로 처리되어 '{output_file_path_new_csv}'에 저장되었습니다.")