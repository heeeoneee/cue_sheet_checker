import pandas as pd
import os
import re
from datetime import datetime, time, timedelta
import glob

# --- 설정 ---
# HELPERS_FILE 경로를 실제 환경에 맞게 수정해주세요.
HELPERS_FILE = '/Users/heeeonlee/2025KYSA/QueueSheets/initial_csv_files/2025 KYSA 운영위원 통합 큐시트_도우미 명단.csv'
# ----------------

def to_datetime(time_str):
    """시간 문자열(예: 'AM 9:00')을 datetime 객체로 변환합니다."""
    try:
        return datetime.strptime(str(time_str).strip(), '%p %I:%M')
    except (ValueError, TypeError):
        return None

def parse_helpers_needed(text):
    """필요 도우미 수 텍스트를 파싱하여 숫자로 반환합니다."""
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

    for i, f in enumerate(sorted(saved_files)): # 파일 목록 정렬
        print(f"  {i+1}. {f}")
    
    try:
        choice = int(input("번호 선택 >> ")) - 1
        if not 0 <= choice < len(saved_files):
            print("❌ 잘못된 번호입니다."); return
        resume_file_path = sorted(saved_files)[choice]
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
    helpers_df['이름'] = helpers_df['이름'].str.strip() # 마스터 명단 이름 공백 제거

    selected_day_column = None
    for col in helpers_df.columns:
        if day_part in col:
            selected_day_column = col; break
    if not selected_day_column:
        print(f"❌ '{day_part}' 요일을 도우미 명단에서 찾을 수 없습니다."); return

    day_available_df = helpers_df[helpers_df[selected_day_column].astype(str) == '1']
    
    excluded_crew_members = set()
    facility_row = df_sorted[df_sorted['일정'] == '시설조 활동']
    if not facility_row.empty and pd.notna(facility_row.iloc[0]['배정된 도우미']):
        excluded_crew_members = {h.strip() for h in facility_row.iloc[0]['배정된 도우미'].split(',')}
    
    # 그날 배정 가능한 모든 인원 (Set으로 관리하여 중복 방지 및 빠른 연산)
    final_available_helpers = {h.strip() for h in day_available_df['이름'] if h.strip() not in excluded_crew_members}

    # "배정된 도우미 이름" 열을 기준으로 스케줄 생성 (이것이 모든 스케줄의 기준이 됨)
    print("\n저장된 배정 현황을 바탕으로 스케줄을 구성합니다...")
    helper_schedules = {} 
    for _, task in df_sorted.iterrows():
        start_dt, end_dt = to_datetime(task['시작시간']), to_datetime(task['종료시간'])
        if start_dt and end_dt:
            helpers_in_task = [h.strip() for h in str(task['배정된 도우미']).split(',') if h.strip()]
            for helper_name in helpers_in_task:
                if helper_name not in helper_schedules:
                    helper_schedules[helper_name] = []
                helper_schedules[helper_name].append((start_dt, end_dt))

    # 배정 시작 위치 탐색
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
        temp_df_for_grouping = pd.DataFrame(list(final_available_helpers), columns=['이름'])
        temp_df_for_grouping = pd.merge(temp_df_for_grouping, helpers_df[['이름', '팀']], on='이름', how='left')
        grouped = temp_df_for_grouping.groupby('팀')['이름'].apply(list)

        for team, names in sorted(grouped.items()):
            if names: print(f"- {team}: {', '.join(sorted(names))}")
        print("="*60)

        print(f"\n▶ 작업 [{i+1}/{len(df_sorted)}]")
        print(f"    - 시간: {task['시작시간']} ~ {task['종료시간']}")
        print(f"    - 일정: {task['일정']}")
        print(f"    - 필요 인원: {needed_count}명 (현재 {assigned_count}명 배정됨)")
        print(f"    - 현재 배정된 도우미: {', '.join(sorted(current_helpers_list)) if current_helpers_list else '없음'}")
        print("="*50)

        user_input = input("배정 (n: 다음, b: 이전, j: 점프, s: 검색, q: 종료, '-' 제외) >> ")

        if user_input.lower() == 'q':
            print("작업을 중단하고 현재까지의 내용을 저장합니다.")
            break
        
        elif user_input.lower() == 's':
            while True:
                search_choice = input("\n무엇을 검색하시겠습니까? (1: 도우미 이름, 2: 일정 번호, 3: 전체 스케줄, 4: 시간대별 미배정 인원, q: 취소) >> ").strip()
                if search_choice == '1':
                    search_name = input("검색할 도우미 이름을 입력하세요 >> ").strip()
                    if search_name in helper_schedules:
                        print(f"\n--- 🔍 '{search_name}' 님 검색 결과 ---")
                        for start_dt, end_dt in sorted(helper_schedules[search_name]):
                            for _, row in df_sorted.iterrows():
                                if to_datetime(row['시작시간']) == start_dt and to_datetime(row['종료시간']) == end_dt:
                                    print(f"  - ({row['시작시간']}~{row['종료시간']}) {row['일정'].strip().replace(chr(10), ' ')}")
                                    break
                    else:
                        print(f"'{search_name}' 님은 아직 배정된 작업이 없습니다.")
                    print("--------------------")
                    input("확인했으면 Enter를 누르세요...")
                    break 
                
                elif search_choice == '2':
                    print("\n--- 검색할 일정 선택 ---")
                    for idx, row in df_sorted.iterrows():
                        schedule_name = str(row['일정']).strip().replace(chr(10), ' ')
                        print(f"{idx + 1}. {schedule_name:<20}", end='\t')
                        if (idx + 1) % 5 == 0: print()
                    print("\n-------------------------")
                    try:
                        choice = int(input("번호 선택 >> ")) - 1
                        if 0 <= choice < len(df_sorted):
                            chosen_task = df_sorted.iloc[choice]
                            chosen_helpers = [h.strip() for h in str(chosen_task['배정된 도우미']).split(',') if h.strip()]
                            print("\n--- 🔍 일정 검색 결과 ---")
                            print(f"일정: {chosen_task['일정'].strip().replace(chr(10), ' ')}")
                            if chosen_helpers:
                                print(f"배정된 도우미: {', '.join(sorted(chosen_helpers))}")
                            else:
                                print("배정된 도우미가 없습니다.")
                            print("--------------------")
                            input("확인했으면 Enter를 누르세요...")
                            break
                        else: print("❌ 잘못된 번호입니다.")
                    except ValueError: print("❌ 숫자를 입력해야 합니다.")
                
                elif search_choice == '3':
                    print("\n--- 📋 전체 인원 스케줄 (시간순 정렬) ---")
                    all_schedules_data = []
                    
                    all_people_in_schedules = set(helper_schedules.keys())
                    all_possible_people = final_available_helpers.union(all_people_in_schedules)

                    for helper_name in sorted(list(all_possible_people)):
                        if helper_name in helper_schedules and helper_schedules[helper_name]:
                            print(f"\n[ {helper_name} ]")
                            sorted_schedule = sorted(helper_schedules[helper_name])
                            for start_dt, end_dt in sorted_schedule:
                                for _, row in df_sorted.iterrows():
                                    if to_datetime(row['시작시간']) == start_dt and to_datetime(row['종료시간']) == end_dt:
                                        if helper_name in [h.strip() for h in str(row['배정된 도우미']).split(',')]:
                                            task_str = f"({row['시작시간']}~{row['종료시간']}) {row['일정'].strip().replace(chr(10), ' ')}"
                                            print(f"  - {task_str}")
                                            all_schedules_data.append({'도우미 이름': helper_name, '시작시간': row['시작시간'], '종료시간': row['종료시간'], '일정': row['일정']})
                                            break
                        elif helper_name in final_available_helpers:
                            all_schedules_data.append({'도우미 이름': helper_name, '시작시간': '-', '종료시간': '-', '일정': '배정 없음'})
                    
                    unassigned_in_final_list = [d['도우미 이름'] for d in all_schedules_data if d['일정'] == '배정 없음']
                    if unassigned_in_final_list:
                        print("\n\n--- ⚪ 배정되지 않은 인원 ---")
                        print(', '.join(sorted(unassigned_in_final_list)))

                    save_choice = input("\n이 전체 스케줄을 파일로 저장하시겠습니까? (y/n) >> ").lower().strip()
                    if save_choice == 'y':
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
                        full_schedule_file = f'full_schedule_{selected_day_column}_{timestamp}.csv'
                        pd.DataFrame(all_schedules_data).to_csv(full_schedule_file, index=False, encoding='utf-8-sig')
                        print(f"✅ 전체 스케줄이 '{full_schedule_file}' 파일로 저장되었습니다.")
                    break
                
                elif search_choice == '4':
                    print("\n--- 🕒 시간대별 미배정 인원 검색 ---")
                    time_slots = pd.date_range("06:00", "23:45", freq="15min").to_pydatetime()
                    
                    free_helpers_by_slot = {}
                    for slot in time_slots:
                        slot_time = slot.time()
                        
                        busy_helpers = set()
                        for helper_name, schedules in helper_schedules.items():
                            for start_dt, end_dt in schedules:
                                if start_dt.time() <= slot_time < end_dt.time():
                                    busy_helpers.add(helper_name)
                                    break
                        
                        free_helpers = final_available_helpers - busy_helpers
                        
                        free_helpers_tuple = tuple(sorted(list(free_helpers)))
                        if free_helpers_tuple:
                            if free_helpers_tuple not in free_helpers_by_slot:
                                free_helpers_by_slot[free_helpers_tuple] = []
                            free_helpers_by_slot[free_helpers_tuple].append(slot)

                    print("결과를 분석 중입니다...")
                    if not free_helpers_by_slot:
                        print("\n모든 시간대에 인원이 배정되어 있거나, 참석 가능 인원이 없습니다.")
                    else:
                        merged_slots = []
                        for helpers, slots in free_helpers_by_slot.items():
                            if not slots: continue
                            
                            slots.sort()
                            start_chunk = slots[0]
                            
                            for i in range(1, len(slots)):
                                if (slots[i] - slots[i-1]).total_seconds() > 900:
                                    merged_slots.append((start_chunk, slots[i-1], helpers))
                                    start_chunk = slots[i]
                            
                            merged_slots.append((start_chunk, slots[-1], helpers))
                        
                        merged_slots.sort(key=lambda x: x[0])

                        for start_chunk, end_chunk, helpers in merged_slots:
                            end_time_display = end_chunk + timedelta(minutes=15)
                            print(f"\n[ {start_chunk.strftime('%p %I:%M')} ~ {end_time_display.strftime('%p %I:%M')} ]")
                            print(f"  - 미배정 ({len(helpers)}명): {', '.join(helpers)}")

                    print("\n---------------------------------")
                    input("확인했으면 Enter를 누르세요...")
                    break

                elif search_choice.lower() == 'q':
                    break
                else:
                    print("❌ 잘못된 선택입니다. 1, 2, 3, 4, q 중에서 입력해주세요.")
            continue
        
        elif user_input.lower() == 'j':
            print("\n--- 점프할 일정 선택 (상태별 분류) ---")
            unassigned, incomplete, complete = [], [], []
            for idx, row in df_sorted.iterrows():
                needed = parse_helpers_needed(row['필요 도우미 수'])
                assigned_list = [h.strip() for h in str(row['배정된 도우미']).split(',') if h.strip()]
                assigned_count = len(assigned_list)
                assigned_str = f"-> ({', '.join(sorted(assigned_list))})" if assigned_list else ""
                task_info = (idx, f"({row['시작시간']}) {str(row['일정']).strip().replace(chr(10), ' ')} [{assigned_count}/{needed}] {assigned_str}")
                
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
                df_sorted.at[i, '배정된 도우미'] = ', '.join(sorted(current_helpers_list))
                if name_to_remove in helper_schedules and task_start_dt:
                    schedules = helper_schedules[name_to_remove]
                    if (task_start_dt, task_end_dt) in schedules:
                        schedules.remove((task_start_dt, task_end_dt))
                print(f"  - {name_to_remove} 님을 제외했습니다.")
                just_jumped = True
                continue
            else:
                print(f"  - '{name_to_remove}' 님은 배정되어 있지 않습니다.")
                continue
        
        input_names = [name.strip() for name in user_input.split(',')]
        
        valid_names, invalid_names, unavailable_names, conflicted_names = [], [], [], []
        
        for name in input_names:
            if name not in final_available_helpers:
                if name in excluded_crew_members: unavailable_names.append(f"{name}(시설조)")
                else: invalid_names.append(name)
                continue

            is_conflicted = False
            for start, end in helper_schedules.get(name, []):
                if task_start_dt and end and task_end_dt > start and task_start_dt < end:
                    is_conflicted = True
                    conflicted_names.append(f"{name}({start.strftime('%p %I:%M')}~{end.strftime('%p %I:%M')})")
                    break
            if not is_conflicted:
                valid_names.append(name)

        newly_assigned = []
        for name in valid_names:
            if name not in current_helpers_list:
                newly_assigned.append(name)
        
        if newly_assigned:
            current_helpers_list.extend(newly_assigned)
            df_sorted.at[i, '배정된 도우미'] = ', '.join(sorted(current_helpers_list))
            for name in newly_assigned:
                if task_start_dt and task_end_dt:
                    if name not in helper_schedules:
                        helper_schedules[name] = []
                    helper_schedules[name].append((task_start_dt, task_end_dt))
            print(f"✅ 배정 완료: {', '.join(sorted(newly_assigned))}")

        if invalid_names: print(f"❌ 명단에 없음/참석 불가: {', '.join(invalid_names)}")
        if unavailable_names: print(f"❌ 배정 불가: {', '.join(unavailable_names)}")
        if conflicted_names: print(f"❌ 시간 중복: {', '.join(conflicted_names)}")
        
        final_assigned_count = len(current_helpers_list)
        if final_assigned_count < needed_count:
            print(f"⚠️ {needed_count - final_assigned_count}명이 더 필요합니다.")
            just_jumped = True
        else:
            i += 1

    # 4. 최종 결과 저장 (덮어쓰기)
    df_sorted.to_csv(resume_file_path, index=False, encoding='utf-8-sig')
    print(f"\n✅ 작업 내용이 '{resume_file_path}' 파일에 덮어쓰기 저장되었습니다.")

if __name__ == '__main__':
    run_resume_tool()