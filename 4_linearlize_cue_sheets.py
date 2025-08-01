import pandas as pd
import numpy as np
import sys

if len(sys.argv) != 3:
    print("❌ 오류: 파일 경로가 올바르게 전달되지 않았습니다.")
    print("사용법: python 4_linearlize_cue_sheets.py <입력_파일_경로> <출력_파일_경로>")
    sys.exit(1)

file_path = sys.argv[1]
processed_file_path_updated = sys.argv[2]

try:
    df = pd.read_csv(file_path, index_col=0)

    tasks = []
    
    num_blocks = 0
    for col in df.columns:
        if '일정' in col and '시설' not in col:
             num_blocks += 1

    for i in range(num_blocks):
        suffix = f'.{i}' if i > 0 else ''
        
        schedule_col = f'일정{suffix}'
        location_col = f'장소{suffix}' if f'장소{suffix}' in df.columns else None
        details_col = f'세부 내용{suffix}' if f'세부 내용{suffix}' in df.columns else None
        helpers_needed_col = f'필요 도우미 수{suffix}' if f'필요 도우미 수{suffix}' in df.columns else None
        assigned_helper_col = f'배정된 도우미 이름{suffix}' if f'배정된 도우미 이름{suffix}' in df.columns else None

        manager_col_v1 = f'담당자\n(프로그램 팀원 명){suffix}'
        manager_col_v2 = f'담당자 \n(프로그램 팀원 명){suffix}'
        manager_col = None
        if manager_col_v1 in df.columns:
            manager_col = manager_col_v1
        elif manager_col_v2 in df.columns:
            manager_col = manager_col_v2

        if not helpers_needed_col:
            continue

        for time, row in df.iterrows():
            helpers_needed_val = row[helpers_needed_col]
            
            if pd.notna(helpers_needed_val) and str(helpers_needed_val).strip() not in ['-', '0', '0.0']:
                task_info = {
                    '시간': time,
                    '일정': row[schedule_col] if pd.notna(row[schedule_col]) else '-',
                    '장소': row[location_col] if location_col and pd.notna(row[location_col]) else '-',
                    '세부 내용': row[details_col] if details_col and pd.notna(row[details_col]) else '-',
                    '담당자': row[manager_col] if manager_col and pd.notna(row[manager_col]) else '-',
                    '필요 도우미 수': str(helpers_needed_val).strip(),
                    '배정된 도우미': row[assigned_helper_col] if assigned_helper_col and pd.notna(row[assigned_helper_col]) else '-'
                }
                tasks.append(task_info)

    tasks_df_updated = pd.DataFrame(tasks)
    
    tasks_df_updated.to_csv(processed_file_path_updated, index=False, encoding='utf-8-sig')
    
    print(f"✅ 데이터 정제 성공! '{processed_file_path_updated}' 파일이 생성되었습니다.")

except FileNotFoundError:
    print(f"❌ 파일을 찾을 수 없습니다! 아래 경로가 정확한지 다시 확인해주세요:\n{file_path}")
    sys.exit(1)
except Exception as e:
    print(f"데이터를 처리하는 중 오류가 발생했습니다: {e}")
    sys.exit(1) # ❗ [핵심 수정] 실패 시 종료 코드 1 반환