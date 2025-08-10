import pandas as pd
import os


file_path = '/Users/heeeonlee/2025KYSA/cue_sheet_checker/initial_csv_files/2025 KYSA 운영위원 통합 큐시트_운영위 명단.csv'
df = pd.read_csv(file_path, header=None)
new_columns_list = df.iloc[1].tolist()
df_columns_map = {
    0: '소속',
    1: '역할',
    2: '이름',
    3: '성별',
    4: '수', 
    5: '목',
    6: '금',
    7: '토',
    8: '일',
    9: '연락처' 
}
df = df.rename(columns=df_columns_map)
df = df.iloc[2:].reset_index(drop=True)

df.loc[0:, '역할'] = df.loc[0:, '역할'].ffill()
print(f"'역할' 열의 수직 빈칸을 채웠습니다.")

df.loc[[0], '수':'일'] = df.loc[[0], '수':'일'].ffill(axis=1)
print(f"첫 번째 데이터 행 ('수'열부터 '일'열까지)의 가로 빈칸을 채웠습니다.")

try:
    k_column_index = df.columns.get_loc('연락처')
    df = df.iloc[:, :k_column_index + 1]
    print(f"K열('연락처') 이후의 열이 성공적으로 삭제되었습니다.")
except KeyError:
    print(f"'연락처' 열을 찾을 수 없습니다. K열 삭제 로직을 건너뜽니다.")
    print(f"현재 열 이름: {df.columns.tolist()}")

output_file_path = '/Users/heeeonlee/2025KYSA/QueueSheets/modified_csv_files/2025 KYSA 운영위원 통합 큐시트_운영위 명단_processed_final.csv'

output_directory = os.path.dirname(output_file_path)
if not os.path.exists(output_directory):
    os.makedirs(output_directory)
    print(f"디렉토리 '{output_directory}'를 생성했습니다.")

df.to_csv(output_file_path, index=False)
print(f"\n파일이 성공적으로 수정되어 '{output_file_path}'에 저장되었습니다.")