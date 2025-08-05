import pandas as pd
import os
import sys

if len(sys.argv) != 3:
    print("❌ 오류: 파일 경로가 올바르게 전달되지 않았습니다.")
    print("사용법: python 3_tidy_cue_sheets.py <입력_파일_경로> <출력_파일_경로>")
    sys.exit(1)

file_path_new_csv = sys.argv[1]
output_file_path_new_csv = sys.argv[2]

try:
    df_new = pd.read_csv(file_path_new_csv, header=None)
    
    df_new.columns = df_new.iloc[2]
    df_new.columns = df_new.columns.str.strip()
    
    df_new = df_new.iloc[3:].reset_index(drop=True)

    if len(df_new.columns) > 0:
        df_new.iloc[:, 0] = df_new.iloc[:, 0].ffill()
    if len(df_new.columns) > 1:
        df_new.iloc[:, 1] = df_new.iloc[:, 1].ffill()

    schedule_cols = [col for col in df_new.columns if "일정" in str(col)]
    for col_name in schedule_cols:
        df_new[col_name] = df_new[col_name].ffill()

    target_detail_cols = [
        "일정","장소", "세부 내용", "재료", "담당자\n(프로그램 팀원 명)",
        "필요 도우미 수", "도우미 역할\n(최대한 구체적으로)", "배정된 도우미 이름"
    ]
    target_detail_cols = [col.strip() for col in target_detail_cols]
    
    existing_target_detail_cols = [col for col in target_detail_cols if col in df_new.columns]

    if existing_target_detail_cols and schedule_cols:
        is_schedule_row_empty_or_dash = (df_new[schedule_cols].isnull() | (df_new[schedule_cols] == '-')).all(axis=1)
        
        for col in existing_target_detail_cols:
            df_new[col] = df_new[col].ffill()
            if col not in df_new.columns[:2]:
                df_new.loc[is_schedule_row_empty_or_dash, col] = '-'

    df_new = df_new.fillna('-')

    output_directory_new_csv = os.path.dirname(output_file_path_new_csv)
    if not os.path.exists(output_directory_new_csv):
        os.makedirs(output_directory_new_csv)

    df_new.to_csv(output_file_path_new_csv, index=False, encoding='utf-8-sig')
    print(f"✅ 파일이 성공적으로 처리되어 '{output_file_path_new_csv}'에 저장되었습니다.")

except FileNotFoundError:
    print(f"❌ 오류: 입력 파일 '{file_path_new_csv}'을(를) 찾을 수 없습니다.")
    sys.exit(1)
except Exception as e:
    print(f"데이터 처리 중 오류가 발생했습니다: {e}")
    sys.exit(1) # ❗ [핵심 수정] 실패 시 종료 코드 1 반환