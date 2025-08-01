import pandas as pd
import os
import re
from datetime import datetime

# --- 설정 ---
input_file = 'event_schedule.csv'
output_file = 'assignment_in_progress.csv'
helpers_master_list = [
    '강규이', '권유정', '김다비', '김민성', '김성수', '김지승', '김찬중', '김채린',
    '김희철', '박세영', '박소영', '박주영', '서동우', '안가현', '양하영', '오빛나',
    '유지현', '윤성관', '이민우', '이세현', '이용재', '이예진', '이진', '장정현',
    '전동국', '정성엽', '정혜원', '조성민', '최윤영', '최은서', '한지우', '허창범',
    '황예나', '김요한', '김하은', '최민준'
]
# ----------------

def to_datetime(time_str):
    """ 'PM 1:00' 같은 문자열을 datetime 객체로 변환 """
    return datetime.strptime(time_str.strip(), '%p %I:%M')

def parse_helpers_needed(text):
    try:
        if str(text).strip().isdigit(): return int(text)
        return sum([int(n) for n in re.findall(r'\d+', str(text))])
    except: return 0

def run_assignment_tool():
    if os.path.exists(output_file):
        df = pd.read_csv(output_file)
        print(f"'{output_file}'에서 이전 작업 내용을 불러왔습니다.")
    else:
        df = pd.read_csv(input_file)
        print(f"'{input_file}'에서 새로 작업을 시작합니다.")

    df['배정된 도우미'] = df['배정된 도우미'].fillna('')
    df_sorted = df.sort_values(by='시작시간', key=lambda x: pd.to_datetime(x, format='%p %I:%M')).reset_index(drop=True)

    # ❗ [핵심 로직 1] 도우미별 스케줄을 저장할 딕셔너리 생성
    helper_schedules = {name: [] for name in helpers_master_list}
    # 기존에 배정된 내용을 스케줄에 미리 등록
    for _, task in df_sorted.iterrows():
        if task['배정된 도우미']:
            start_dt = to_datetime(task['시작시간'])
            end_dt = to_datetime(task['종료시간'])
            assigned_helpers = [h.strip() for h in str(task['배정된 도우미']).split(',') if h.strip() and h.strip() != '-']
            for helper in assigned_helpers:
                if helper in helper_schedules:
                    helper_schedules[helper].append((start_dt, end_dt))

    i = 0
    while i < len(df_sorted):
        task = df_sorted.iloc[i]
        current_helpers_list = [h.strip() for h in str(task['배정된 도우미']).split(',') if h.strip() and h.strip() != '-']
        assigned_count = len(current_helpers_list)
        needed_count = parse_helpers_needed(task['필요 도우미 수'])

        print("\n" + "="*50)
        print(f"▶ 작업 [{i+1}/{len(df_sorted)}]")
        print(f"    - 시간: {task['시작시간']} ~ {task['종료시간']}")
        print(f"    - 일정: {task['일정']}")
        print(f"    - 필요 인원: {needed_count}명 (현재 {assigned_count}명 배정됨)")
        print(f"    - 현재 배정된 도우미: {', '.join(current_helpers_list) if current_helpers_list else '없음'}")
        print("="*50)

        if assigned_count >= needed_count:
            print("✅ 필요 인원이 모두 배정되었습니다.")
            i += 1
            continue

        user_input = input("배정할 도우미 (n: 다음, b: 이전, q: 저장 후 종료) >> ")

        if user_input.lower() == 'q': break
        elif user_input.lower() == 'n': i += 1; continue
        elif user_input.lower() == 'b': i = max(0, i - 1); continue
        elif not user_input: print("입력값이 없습니다."); continue

        input_names = [name.strip() for name in user_input.split(',')]
        valid_names, invalid_names, conflicted_names = [], [], []

        task_start_dt = to_datetime(task['시작시간'])
        task_end_dt = to_datetime(task['종료시간'])

        for name in input_names:
            if name not in helpers_master_list:
                invalid_names.append(name)
                continue
            
            # ❗ [핵심 로직 2] 시간 겹침 확인
            is_conflicted = False
            for scheduled_start, scheduled_end in helper_schedules.get(name, []):
                # (StartA <= EndB) and (EndA >= StartB)
                if task_start_dt < scheduled_end and task_end_dt > scheduled_start:
                    is_conflicted = True
                    conflicted_names.append(f"{name}({scheduled_start.strftime('%H:%M')}~{scheduled_end.strftime('%H:%M')})")
                    break
            
            if not is_conflicted:
                valid_names.append(name)

        # 배정 처리 및 스케줄 업데이트
        newly_assigned = []
        for name in valid_names:
            if name not in current_helpers_list:
                current_helpers_list.append(name)
                helper_schedules[name].append((task_start_dt, task_end_dt))
                newly_assigned.append(name)

        df_sorted.at[i, '배정된 도우미'] = ', '.join(current_helpers_list)

        # 결과 피드백
        if newly_assigned: print(f"✅ 배정 완료: {', '.join(newly_assigned)}")
        if invalid_names: print(f"❌ 명단에 없음: {', '.join(invalid_names)}")
        if conflicted_names: print(f"❌ 시간 중복: {', '.join(conflicted_names)} 님은 이미 다른 작업에 배정되어 추가할 수 없습니다.")
        
        new_assigned_count = len(current_helpers_list)
        if new_assigned_count < needed_count:
            print(f"⚠️ {needed_count - new_assigned_count}명이 더 필요합니다.")
        else:
            i += 1

    df_sorted.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"\n작업 내용이 '{output_file}'에 저장되었습니다. 프로그램을 종료합니다.")

if __name__ == '__main__':
    run_assignment_tool()