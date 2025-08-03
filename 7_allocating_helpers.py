import pandas as pd
import os
import re
from datetime import datetime

# --- 설정 ---
HELPERS_FILE = '/Users/heeeonlee/2025KYSA/QueueSheets/initial_csv_files/2025 KYSA 운영위원 통합 큐시트_도우미 명단.csv'
FINAL_SCHEDULE_DIR = '/Users/heeeonlee/2025KYSA/QueueSheets/final_schedule_files'
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

def run_assignment_tool():
    # 1. 도우미 명단 로드
    try:
        df_raw = pd.read_csv(HELPERS_FILE, header=None)
        df_transposed = df_raw.T
        df_transposed.columns = df_transposed.iloc[0]
        helpers_df = df_transposed.iloc[1:].drop(df_transposed.columns[0], axis=1).reset_index(drop=True)
        helpers_df.columns = helpers_df.columns.str.strip()
        helpers_df = helpers_df.dropna(how='all')
        if '이름' not in helpers_df.columns:
            raise KeyError("'이름' 열을 파일에서 찾을 수 없습니다.")
    except Exception as e:
        print(f"❌ 오류: 도우미 명단 파일('{HELPERS_FILE}') 처리 중 문제가 발생했습니다: {e}")
        return

    # 2. 요일 선택
    while True:
        user_input_day = input("\n배정할 요일을 입력하세요 (예: 목, 금, 토, 일) >> ").strip()
        matched_column = None
        day_to_filename_map = {'수': '8.13수', '목': '8.14목', '금': '8.15금', '토': '8.16토', '일': '8.17일'}
        file_day_part = day_to_filename_map.get(user_input_day)
        if file_day_part:
            for col in helpers_df.columns:
                if user_input_day in col:
                    matched_column = col
                    break
        if matched_column:
            selected_day_column = matched_column
            schedule_file_path = f"{FINAL_SCHEDULE_DIR}/2025 KYSA 운영위원 통합 큐시트_{file_day_part}_event_schedule.csv"
            print(f"Info: '{selected_day_column}' 열을 기준으로 배정을 시작합니다.")
            break
        else:
            day_cols = [c for c in helpers_df.columns if any(d in c for d in ['월','화','수','목','금','토','일'])]
            print(f"❌ '{user_input_day}'에 해당하는 요일을 찾을 수 없습니다.")
            print(f"    인식된 요일 관련 열: {day_cols}")

    day_available_df = helpers_df[helpers_df[selected_day_column].astype(str) == '1']
    day_available_helpers_list = day_available_df['이름'].tolist()

    # 3. 시설조 배정
    excluded_crew_members = []
    facility_crew_assignment_df = pd.DataFrame()
    # ... (시설조 배정 로직은 변경 없이 그대로 유지) ...
    while True:
        pre_assign_crew = input(f"\n[{selected_day_column}] '시설조'를 우선 배정하고 다른 작업에서 제외하시겠습니까? (y/n) >> ").lower().strip()
        if pre_assign_crew in ['y', 'n']:
            break
        else:
            print("y 또는 n만 입력해주세요.")

    if pre_assign_crew == 'y':
        print("\n" + "="*60)
        print(f"✅ [{selected_day_column}] 참석 가능한 도우미 명단입니다. 시설조를 입력해주세요.")
        grouped = day_available_df.groupby('팀')['이름'].apply(list)
        for team, names in grouped.items():
            if names: print(f"- {team}: {', '.join(names)}")
        print("="*60)
        
        while True:
            if excluded_crew_members:
                print(f"\n--- 현재 배정된 시설조: {', '.join(excluded_crew_members)}")
            needed = 10 - len(excluded_crew_members)
            user_input = input(f"시설조 인원을 입력하세요 ({needed}명 남음, '-' 붙이면 제외, 'n' 입력 시 완료) >> ")
            if user_input.lower() == 'n':
                print("✅ 시설조 배정을 마치고 다음 단계로 넘어갑니다.")
                break
            if user_input.startswith('-'):
                name_to_remove = user_input[1:].strip()
                if name_to_remove in excluded_crew_members:
                    excluded_crew_members.remove(name_to_remove)
                    print(f"  - {name_to_remove} 님 제외.")
                else:
                    print(f"  - '{name_to_remove}' 님은 명단에 없습니다.")
                continue
            input_names = [name.strip() for name in user_input.split(',')]
            if len(excluded_crew_members) + len(input_names) > 10:
                can_add = 10 - len(excluded_crew_members)
                print(f"⚠️ 10명 초과. {can_add}명만 배정합니다.")
                input_names = input_names[:can_add]
            for name in input_names:
                if name in day_available_helpers_list:
                    if name not in excluded_crew_members:
                        excluded_crew_members.append(name)
                        print(f"  + {name} 추가됨. ({len(excluded_crew_members)}/10)")
                    else:
                        print(f"  - {name} 님은 이미 추가되었습니다.")
                else:
                    print(f"  - '{name}' 님은 오늘 참석 가능 명단에 없습니다.")
        if excluded_crew_members:
            facility_crew_assignment_df = pd.DataFrame([{'시작시간': f'{selected_day_column} 하루 종일','종료시간': '-','일정': '시설조 활동', '장소': '-', '세부 내용': '-', '담당자': '-', '필요 도우미 수': len(excluded_crew_members),'배정된 도우미': ', '.join(excluded_crew_members)}])
            print("\n" + "="*60)
            print("✅ 아래 '시설조' 인원이 하루 동안 다른 작업에서 제외되었습니다.")
            print(f"- 제외된 인원: {', '.join(excluded_crew_members)}")
            print("="*60)

    # 4. 최종 명단 및 스케줄 파일 준비
    final_available_df = day_available_df[~day_available_df['이름'].isin(excluded_crew_members)]
    available_helpers_list = final_available_df['이름'].tolist()
    full_helpers_list = helpers_df['이름'].tolist()
    
    try:
        df = pd.read_csv(schedule_file_path)
    except FileNotFoundError:
        print(f"❌ 오류: 스케줄 파일('{schedule_file_path}')을 찾을 수 없습니다.")
        return

    df['배정된 도우미'] = ''
    df_sorted = df.sort_values(by='시작시간', key=lambda x: pd.to_datetime(x, format='%p %I:%M')).reset_index(drop=True)
    helper_schedules = {name: [] for name in full_helpers_list}
    
    # 5. 일반 배정 루프
    i = 0
    just_jumped = False # 점프 직후인지 확인하는 플래그
    while i < len(df_sorted):
        task = df_sorted.iloc[i]
        current_helpers_list = [h.strip() for h in str(task['배정된 도우미']).split(',') if h.strip()]
        assigned_count = len(current_helpers_list)
        needed_count = parse_helpers_needed(task['필요 도우미 수'])

        # ❗ [핵심 수정] 점프 직후가 아니라면, 인원이 찬 작업은 자동으로 넘어감
        if assigned_count >= needed_count and not just_jumped:
            i += 1
            continue
        
        just_jumped = False # 플래그 초기화

        print("\n" + "="*60)
        print(f"✅ [{selected_day_column}] 현재 배정 가능한 도우미 명단입니다. (시설조 제외)")
        grouped = final_available_df.groupby('팀')['이름'].apply(list)
        for team, names in grouped.items():
            if names:
                print(f"- {team}: {', '.join(names)}")
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
        elif user_input.lower() == 'n': i += 1; continue
        elif user_input.lower() == 'b': i = max(0, i - 1); continue
        elif user_input.lower() == 's':
            # ... (검색 로직은 이전과 동일) ...
            while True:
                search_choice = input("\n무엇을 검색하시겠습니까? (1: 도우미 이름, 2: 일정 번호, q: 취소) >> ").strip()
                if search_choice == '1':
                    search_name = input("검색할 도우미 이름을 입력하세요 >> ").strip()
                    found_tasks = []
                    if search_name in excluded_crew_members:
                         found_tasks.append(f"  - ({selected_day_column} 하루 종일) 시설조 활동")
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
                    print(f"  0. 시설조 활동")
                    for idx, row in df_sorted.iterrows():
                        schedule_name = str(row['일정']).strip().replace('\n', ' ')
                        print(f"{idx + 1}. {schedule_name:<20}", end='\t')
                        if (idx + 1) % 5 == 0: print()
                    print("\n-------------------------")
                    try:
                        choice = int(input("번호 선택 >> "))
                        if choice == 0:
                             chosen_helpers = excluded_crew_members
                             chosen_task_name = "시설조 활동"
                        elif 1 <= choice <= len(df_sorted):
                            chosen_task = df_sorted.iloc[choice - 1]
                            chosen_helpers = [h.strip() for h in str(chosen_task['배정된 도우미']).split(',') if h.strip()]
                            chosen_task_name = chosen_task['일정'].strip().replace('\n', ' ')
                        else: 
                            print("❌ 잘못된 번호입니다."); continue
                        
                        print("\n--- 🔍 일정 검색 결과 ---")
                        print(f"일정: {chosen_task_name}")
                        if chosen_helpers:
                            print(f"배정된 도우미: {', '.join(chosen_helpers)}")
                        else:
                            print("배정된 도우미가 없습니다.")
                        print("--------------------")
                        input("확인했으면 Enter를 누르세요...")
                        break
                    except ValueError: print("❌ 숫자를 입력해야 합니다.")
                elif search_choice.lower() == 'q':
                    break
                else:
                    print("❌ 잘못된 선택입니다. 1, 2, q 중에서 입력해주세요.")
            continue
        
        elif user_input.lower() == 'j':
            # ❗ [핵심 수정] 점프 목록에 배정된 도우미 이름 표시
            print("\n--- 점프할 일정 선택 ---")
            
            unassigned, incomplete, complete = [], [], []
            for idx, row in df_sorted.iterrows():
                needed = parse_helpers_needed(row['필요 도우미 수'])
                assigned_list = [h for h in str(row['배정된 도우미']).split(',') if h.strip()]
                assigned_count = len(assigned_list)
                
                # 배정된 사람 이름 표시 추가
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
                    print(f"{display_count}. {info}")
                    jump_map[display_count] = idx
                    display_count += 1
            if unassigned:
                print("\n[⚪ 배정 안됨]")
                for idx, info in unassigned:
                    print(f"{display_count}. {info}")
                    jump_map[display_count] = idx
                    display_count += 1
            if complete:
                print("\n[🟢 배정 완료]")
                for idx, info in complete:
                    print(f"{display_count}. {info}")
                    jump_map[display_count] = idx
                    display_count += 1

            print("---------------------------------")
            
            try:
                choice = int(input("이동할 번호를 입력하세요 (0: 취소) >> "))
                if choice == 0:
                    continue
                elif choice in jump_map:
                    i = jump_map[choice]
                    just_jumped = True # 점프 플래그 설정
                    continue
                else:
                    print("❌ 잘못된 번호입니다.")
            except ValueError:
                print("❌ 숫자를 입력해야 합니다.")
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
            else:
                print(f"  - '{name_to_remove}' 님은 배정되어 있지 않습니다.")
            continue

        input_names = [name.strip() for name in user_input.split(',')]
        num_more_needed = needed_count - assigned_count
        if len(input_names) > num_more_needed:
            print(f"⚠️ 필요 인원({num_more_needed}명) 초과. {num_more_needed}명만 배정합니다.")
            input_names = input_names[:num_more_needed]

        valid_names, invalid_names, unavailable_names, conflicted_names = [], [], [], []
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
                current_helpers_list.append(name)
                if task_start_dt: helper_schedules[name].append((task_start_dt, task_end_dt))
                newly_assigned.append(name)

        df_sorted.at[i, '배정된 도우미'] = ', '.join(current_helpers_list)

        if newly_assigned: print(f"✅ 배정 완료: {', '.join(newly_assigned)}")
        if invalid_names: print(f"❌ 명단에 없음: {', '.join(invalid_names)}")
        if unavailable_names: print(f"❌ 참석 불가: {', '.join(unavailable_names)}")
        if conflicted_names: print(f"❌ 시간 중복: {', '.join(conflicted_names)}")
        
        if len(current_helpers_list) < needed_count:
            print(f"⚠️ {needed_count - len(current_helpers_list)}명이 더 필요합니다.")
        else:
            i += 1

    # 6. 최종 결과 저장
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    final_output_file = f'assignment_{selected_day_column}_{timestamp}.csv'
    
    if not facility_crew_assignment_df.empty:
        final_df = pd.concat([df_sorted, facility_crew_assignment_df], ignore_index=True)
    else:
        final_df = df_sorted

    final_df.to_csv(final_output_file, index=False, encoding='utf-8-sig')
    print(f"\n✅ [{selected_day_column}] 배정 작업이 완료되었습니다.")
    print(f"결과가 '{final_output_file}' 파일에 저장되었습니다. 프로그램을 종료합니다.")


if __name__ == '__main__':
    run_assignment_tool()