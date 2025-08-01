import pandas as pd
import numpy as np

# ❗ 사용자가 알려준 정확한 전체 파일 경로로 수정했습니다.
file_path = '/Users/heeeonlee/2025KYSA/QueueSheets/processed_csv_files/2025 KYSA 운영위원 통합 큐시트_8.14목_processed.csv'

try:
    # pandas 라이브러리를 사용하여 지정된 경로의 CSV 파일을 읽어옵니다.
    df = pd.read_csv(file_path, index_col=0)

    # 정제된 작업 목록을 저장할 빈 리스트를 생성합니다.
    tasks = []
    
    # '일정'이라는 이름의 열 개수를 파악하여, 가로로 나열된 프로그램 블록이 총 몇 개인지 추정합니다.
    num_blocks = 0
    for col in df.columns:
        if '일정' in col and '시설' not in col:
             num_blocks += 1

    # 파악된 블록 수만큼 반복하여 각 블록의 데이터를 추출합니다.
    for i in range(num_blocks):
        suffix = f'.{i}' if i > 0 else ''
        
        # 현재 블록에서 정보를 가져올 열(Column)들의 전체 이름을 구성합니다.
        schedule_col = f'일정{suffix}'
        location_col = f'장소{suffix}' if f'장소{suffix}' in df.columns else None
        details_col = f'세부 내용{suffix}' if f'세부 내용{suffix}' in df.columns else None
        helpers_needed_col = f'필요 도우미 수{suffix}' if f'필요 도우미 수{suffix}' in df.columns else None
        assigned_helper_col = f'배정된 도우미 이름{suffix}' if f'배정된 도우미 이름{suffix}' in df.columns else None

        # '담당자' 열 이름 처리 (줄바꿈, 띄어쓰기 등 다양한 경우 고려)
        manager_col_v1 = f'담당자\n(프로그램 팀원 명){suffix}'
        manager_col_v2 = f'담당자 \n(프로그램 팀원 명){suffix}'
        manager_col = None
        if manager_col_v1 in df.columns:
            manager_col = manager_col_v1
        elif manager_col_v2 in df.columns:
            manager_col = manager_col_v2

        if not helpers_needed_col:
            continue

        # 시간대별(행)로 반복하면서 데이터를 한 줄씩 확인합니다.
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

    # 최종 결과를 새로운 CSV 파일로 저장합니다.
    # 이 파일은 스크립트를 실행한 폴더(QueueSheets)에 생성됩니다.
    processed_file_path_updated = 'linearlized.csv'
    tasks_df_updated.to_csv(processed_file_path_updated, index=False, encoding='utf-8-sig')
    
    print(f"✅ 데이터 정제 성공! '{processed_file_path_updated}' 파일이 생성되었습니다.")
    print("\n정제된 데이터 미리보기 (상위 5개):")
    print(tasks_df_updated.head())

except FileNotFoundError:
    print(f"❌ 파일을 찾을 수 없습니다! 아래 경로가 정확한지 다시 확인해주세요:\n{file_path}")
except Exception as e:
    print(f"데이터를 처리하는 중 오류가 발생했습니다: {e}")