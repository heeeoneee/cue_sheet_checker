import pandas as pd
import os
import re
from datetime import datetime
import glob

# --- 설정 ---
HELPERS_FILE = '/Users/heeeonlee/2025KYSA/QueueSheets/initial_csv_files/2025 KYSA 운영위원 통합 큐시트_도우미 명단.csv'
# ----------------

def to_datetime(time_str):
    try:
        return datetime.strptime(time_str.strip(), '%p %I:%M')
    except (ValueError, TypeError):
        return None

def parse_helpers_needed(text):
    try:
        if str(text).strip().isdigit(): return int(text)
        return sum([int(n) for n in re.findall(r'\d+', str(text))])
    except: return 0

def run_resume_tool():
    # 1. 저장된 배정 파일 목록 보여주기 및 선택
    print("\n--- 이어서 진행할 파일 선택 ---")
    saved_files = glob.glob("assignment_*.csv")
    if not saved_files:
        print("❌ 이어서 작업할 파일이 없습니다. 먼저 '7_allocating_helpers.py'를 실행해주세요.")
        return

    for i, f in enumerate(saved_files):
        print(f"  {i+1}. {f}")
    
    try:
        choice = int(input("번호 선택 >> ")) - 1
        if not 0 <= choice < len(saved_files):
            print("❌ 잘못된 번호입니다."); return
        resume_file_path = saved_files[choice]
    except ValueError:
        print("❌ 숫자를 입력해야 합니다."); return

    # 2. 상태 복원
    print(f"\n'{resume_file_path}' 파일에서 작업을 재개합니다.")
    df_sorted = pd.read_csv(resume_file_path)
    df_sorted['배정된 도우미'] = df_sorted['배정된 도우미'].fillna('')

    try:
        day_part = resume_file_path.split('_')[1]
    except IndexError:
        print("❌ 파일 이름 형식이 잘못되어 요일을 추출할 수 없습니다. (예: assignment_목_...)"); return

    df_raw = pd.read_csv(HELPERS_FILE, header=None)
    df_transposed = df_raw.T
    df_transposed.columns = df_transposed.iloc[0]
    helpers_df = df_transposed.iloc[1:].drop(df_transposed.columns[0], axis=1).reset_index(drop=True)
    helpers_df.columns = helpers_df.columns.str.strip()
    helpers_df = helpers_df.dropna(how='all')

    selected_day_column = None
    for col in helpers_df.columns:
        if day_part in col:
            selected_day_column = col; break
    if not selected_day_column:
        print(f"❌ '{day_part}' 요일을 도우미 명단에서 찾을 수 없습니다."); return

    day_available_df = helpers_df[helpers_df[selected_day_column].astype(str) == '1']
    day_available_helpers_list = day_available_df['이름'].tolist()
    full_helpers_list = helpers_df['이름'].tolist()

    excluded_crew_members = []
    facility_row = df_sorted[df_sorted['일정'] == '시설조 활동']
    if not facility_row.empty and pd.notna(facility_row.iloc[0]['배정된 도우미']):
        excluded_crew_members = [h.strip() for h in facility_row.iloc[0]['배정된 도우미'].split(',')]

    helper_schedules = {name: [] for name in full_helpers_list}
    for _, task in df_sorted.iterrows():
        start_dt, end_dt = to_datetime(task['시작시간']), to_datetime(task['종료시간'])
        if start_dt and end_dt:
            helpers = [h.strip() for h in str(task['배정된 도우미']).split(',') if h.strip()]
            for helper in helpers:
                if helper in helper_schedules:
                    helper_schedules[helper].append((start_dt, end_dt))

    start_index = 0
    for idx, row in df_sorted.iterrows():
        needed = parse_helpers_needed(row['필요 도우미 수'])
        assigned = len([h for h in str(row['배정된 도우미']).split(',') if h.strip()])
        if assigned < needed:
            start_index = idx
            break
        start_index = idx + 1
    
    if start_index >= len(df_sorted):
        print("\n✅ 모든 작업이 이미 완료되었습니다. 수정 모드로 시작합니다.")
        start_index = 0

    # 3. 대화형 배정 루프
    i = start_index
    just_jumped = True
    while i < len(df_sorted):
        task = df_sorted.iloc[i]
        
        current_helpers_list = [h.strip() for h in str(task['배정된 도우미']).split(',') if h.strip()]
        assigned_count = len(current_helpers_list)
        needed_count = parse_helpers_needed(task['필요 도우미 수'])

        if assigned_count >= needed_count and not just_jumped:
            i += 1
            continue
        just_jumped = False

        print("\n" + "="*60)
        print(f"✅ [{selected_day_column}] 현재 배정 가능한 도우미 명단입니다. (시설조 제외)")
        final_available_df = day_available_df[~day_available_df['이름'].isin(excluded_crew_members)]
        grouped = final_available_df.groupby('팀')['이름'].apply(list)
        for team, names in grouped.items():
            if names: print(f"- {team}: {', '.join(names)}")
        print("="*60)

        print(f"\n▶ 작업 [{i+1}/{len(df_sorted)}]")
        print(f"    - 시간: {task['시작시간']} ~ {task['종료시간']}")
        print(f"    - 일정: {task['일정']}")
        print(f"    - 필요 인원: {needed_count}명 (현재 {assigned_count}명 배정됨)")
        print(f"    - 현재 배정된 도우미: {', '.join(current_helpers_list) if current_helpers_list else '없음'}")
        print("="*50)

        user_input = input("배정 (n: 다음, b: 이전, j: 점프, s: 검색, q: 종료, '-' 제외) >> ")

        if user_input.lower() == 'q':
            print("작업을 중단하고 현재까지의 내용을 저장합니다.")
            break
        
        elif user_input.lower() == 's':
            while True:
                search_choice = input("\n무엇을 검색하시겠습니까? (1: 도우미 이름, 2: 일정 번호, 3: 전체 스케줄 저장, q: 취소) >> ").strip()
                if search_choice == '1':
                    search_name = input("검색할 도우미 이름을 입력하세요 >> ").strip()
                    found_tasks = []
                    for idx, row in df_sorted.iterrows():
                        assigned_list = [h.strip() for h in str(row['배정된 도우미']).split(',')]
                        if search_name in assigned_list:
                            found_tasks.append(f"  - ({row['시작시간']}~{row['종료시간']}) {row['일정'].strip().replace('\n', ' ')}")
                    print("\n--- 🔍 이름 검색 결과 ---")
                    if found_tasks:
                        print(f"'{search_name}' 님은 아래 작업에 배정되었습니다:")
                        for found_task in found_tasks:
                            print(found_task)
                    else:
                        print(f"'{search_name}' 님은 아직 배정된 작업이 없습니다.")
                    print("--------------------")
                    input("확인했으면 Enter를 누르세요...")
                    break 
                
                elif search_choice == '2':
                    print("\n--- 검색할 일정 선택 ---")
                    for idx, row in df_sorted.iterrows():
                        schedule_name = str(row['일정']).strip().replace('\n', ' ')
                        print(f"{idx + 1}. {schedule_name:<20}", end='\t')
                        if (idx + 1) % 5 == 0: print()
                    print("\n-------------------------")
                    try:
                        choice = int(input("번호 선택 >> ")) - 1
                        if 0 <= choice < len(df_sorted):
                            chosen_task = df_sorted.iloc[choice]
                            chosen_helpers = [h.strip() for h in str(chosen_task['배정된 도우미']).split(',') if h.strip()]
                            print("\n--- 🔍 일정 검색 결과 ---")
                            print(f"일정: {chosen_task['일정'].strip().replace('\n', ' ')}")
                            if chosen_helpers:
                                print(f"배정된 도우미: {', '.join(chosen_helpers)}")
                            else:
                                print("배정된 도우미가 없습니다.")
                            print("--------------------")
                            input("확인했으면 Enter를 누르세요...")
                            break
                        else: print("❌ 잘못된 번호입니다.")
                    except ValueError: print("❌ 숫자를 입력해야 합니다.")
                
                # ❗ [핵심 기능 수정] 전체 스케줄 확인 시, 배정 안된 인원도 표시
                elif search_choice == '3':
                    print("\n--- 📋 전체 인원 스케줄 ---")
                    all_schedules_data = []
                    unassigned_helpers = []
                    
                    # 그날 참석 가능한 모든 도우미를 기준으로 반복
                    for helper_name in day_available_helpers_list:
                        found_tasks = []
                        for _, row in df_sorted.iterrows():
                            assigned_list = [h.strip() for h in str(row['배정된 도우미']).split(',')]
                            if helper_name in assigned_list:
                                task_str = f"({row['시작시간']}~{row['종료시간']}) {row['일정'].strip().replace('\n', ' ')}"
                                found_tasks.append(task_str)
                                all_schedules_data.append({'도우미 이름': helper_name,'시작시간': row['시작시간'],'종료시간': row['종료시간'],'일정': row['일정']})
                        
                        if found_tasks:
                            print(f"\n[ {helper_name} ]")
                            for task_str in found_tasks:
                                print(f"  - {task_str}")
                        else:
                            unassigned_helpers.append(helper_name)

                    if unassigned_helpers:
                        print("\n\n--- ⚪ 배정되지 않은 인원 ---")
                        print(', '.join(unassigned_helpers))
                        # 파일 저장을 위해 데이터 추가
                        for name in unassigned_helpers:
                            all_schedules_data.append({'도우미 이름': name, '시작시간': '-', '종료시간': '-', '일정': '배정 없음'})

                    save_choice = input("\n이 전체 스케줄을 파일로 저장하시겠습니까? (y/n) >> ").lower().strip()
                    if save_choice == 'y':
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
                        full_schedule_file = f'full_schedule_{selected_day_column}_{timestamp}.csv'
                        pd.DataFrame(all_schedules_data).to_csv(full_schedule_file, index=False, encoding='utf-8-sig')
                        print(f"✅ 전체 스케줄이 '{full_schedule_file}' 파일로 저장되었습니다.")
                    break

                elif search_choice.lower() == 'q':
                    break
                else:
                    print("❌ 잘못된 선택입니다. 1, 2, 3, q 중에서 입력해주세요.")
            continue
        
        # ... (이하 j, -, 이름 입력 등 모든 로직은 이전과 동일) ...
        elif user_input.lower() == 'j':
            print("\n--- 점프할 일정 선택 (상태별 분류) ---")
            unassigned, incomplete, complete = [], [], []
            for idx, row in df_sorted.iterrows():
                needed = parse_helpers_needed(row['필요 도우미 수'])
                assigned_list = [h for h in str(row['배정된 도우미']).split(',') if h.strip()]
                assigned_count = len(assigned_list)
                assigned_str = f"-> ({', '.join(assigned_list)})" if assigned_list else ""
                task_info = (idx, f"({row['시작시간']}) {str(row['일정']).strip().replace('\n', ' ')} [{assigned_count}/{needed}] {assigned_str}")
                if assigned_count == 0: unassigned.append(task_info)
                elif assigned_count < needed: incomplete.append(task_info)
                else: complete.append(task_info)
            
            jump_map = {}
            display_count = 1
            if incomplete:
                print("\n[🟠 배정 부족]")
                for idx, info in incomplete:
                    print(f"{display_count}. {info}"); jump_map[display_count] = idx; display_count += 1
            if unassigned:
                print("\n[⚪ 배정 안됨]")
                for idx, info in unassigned:
                    print(f"{display_count}. {info}"); jump_map[display_count] = idx; display_count += 1
            if complete:
                print("\n[🟢 배정 완료]")
                for idx, info in complete:
                    print(f"{display_count}. {info}"); jump_map[display_count] = idx; display_count += 1
            print("---------------------------------")
            
            try:
                choice = int(input("이동할 번호를 입력하세요 (0: 취소) >> "))
                if choice == 0: continue
                elif choice in jump_map:
                    i = jump_map[choice]
                    just_jumped = True
                    continue
                else: print("❌ 잘못된 번호입니다.")
            except ValueError: print("❌ 숫자를 입력해야 합니다.")
            continue

        elif not user_input: print("입력값이 없습니다."); continue

        task_start_dt, task_end_dt = to_datetime(task['시작시간']), to_datetime(task['종료시간'])

        if user_input.startswith('-'):
            name_to_remove = user_input[1:].strip()
            if name_to_remove in current_helpers_list:
                current_helpers_list.remove(name_to_remove)
                df_sorted.at[i, '배정된 도우미'] = ', '.join(current_helpers_list)
                if name_to_remove in helper_schedules and task_start_dt:
                    for item in helper_schedules[name_to_remove]:
                        if item[0] == task_start_dt and item[1] == task_end_dt:
                            helper_schedules[name_to_remove].remove(item); break
                print(f"  - {name_to_remove} 님을 제외했습니다.")
                just_jumped = True
                continue
            else:
                print(f"  - '{name_to_remove}' 님은 배정되어 있지 않습니다.")
                continue
        
        input_names = [name.strip() for name in user_input.split(',')]
        num_more_needed = needed_count - assigned_count
        if len(input_names) > num_more_needed:
            print(f"⚠️ 필요 인원({num_more_needed}명) 초과. {num_more_needed}명만 배정합니다.")
            input_names = input_names[:num_more_needed]

        valid_names, invalid_names, unavailable_names, conflicted_names = [], [], [], []
        available_helpers_list = final_available_df['이름'].tolist()
        for name in input_names:
            if name not in full_helpers_list: invalid_names.append(name); continue
            if name not in available_helpers_list:
                if name in excluded_crew_members: unavailable_names.append(f"{name}(시설조)")
                else: unavailable_names.append(name)
                continue
            is_conflicted = False
            for start, end in helper_schedules.get(name, []):
                if task_start_dt and end and task_end_dt > start and task_start_dt < end:
                    is_conflicted = True
                    conflicted_names.append(f"{name}({start.strftime('%H:%M')}~{end.strftime('%H:%M')})")
                    break
            if not is_conflicted: valid_names.append(name)

        newly_assigned = []
        for name in valid_names:
            if name not in current_helpers_list:
                newly_assigned.append(name)
        
        current_helpers_list.extend(newly_assigned)
        df_sorted.at[i, '배정된 도우미'] = ', '.join(current_helpers_list)
        for name in newly_assigned:
            if task_start_dt: helper_schedules[name].append((task_start_dt, task_end_dt))

        if newly_assigned: print(f"✅ 배정 완료: {', '.join(newly_assigned)}")
        if invalid_names: print(f"❌ 명단에 없음: {', '.join(invalid_names)}")
        if unavailable_names: print(f"❌ 참석 불가: {', '.join(unavailable_names)}")
        if conflicted_names: print(f"❌ 시간 중복: {', '.join(conflicted_names)}")
        
        final_assigned_count = len(current_helpers_list)
        if final_assigned_count < needed_count:
            print(f"⚠️ {needed_count - final_assigned_count}명이 더 필요합니다.")
            just_jumped = True
            continue
        else:
            i += 1

    # 4. 최종 결과 저장 (덮어쓰기)
    df_sorted.to_csv(resume_file_path, index=False, encoding='utf-8-sig')
    print(f"\n✅ 작업 내용이 '{resume_file_path}' 파일에 덮어쓰기 저장되었습니다.")

if __name__ == '__main__':
    run_resume_tool()