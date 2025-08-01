import pandas as pd
import os
import re
from datetime import datetime

# --- 설정 ---
schedule_file = 'event_schedule.csv' 
helpers_file = '/Users/heeeonlee/2025KYSA/QueueSheets/initial_csv_files/2025 KYSA 운영위원 통합 큐시트_도우미 명단.csv'
# ----------------

def to_datetime(time_str):
    """ 'PM 1:00' 같은 문자열을 datetime 객체로 변환 """
    return datetime.strptime(time_str.strip(), '%p %I:%M')

def parse_helpers_needed(text):
    """ '7', '3+2' 같은 문자열을 정수(7, 5)로 변환 """
    try:
        if str(text).strip().isdigit(): return int(text)
        return sum([int(n) for n in re.findall(r'\d+', str(text))])
    except: return 0

def run_assignment_tool():
    # 1. 도우미 명단 로드
    try:
        df_raw = pd.read_csv(helpers_file, header=None)
        df_transposed = df_raw.T
        df_transposed.columns = df_transposed.iloc[0]
        helpers_df = df_transposed.iloc[1:].drop(df_transposed.columns[0], axis=1).reset_index(drop=True)
        helpers_df.columns = helpers_df.columns.str.strip()
        helpers_df = helpers_df.dropna(how='all')
        if '이름' not in helpers_df.columns:
            raise KeyError("'이름' 열을 파일에서 찾을 수 없습니다.")
    except Exception as e:
        print(f"❌ 오류: 도우미 명단 파일('{helpers_file}')을 처리하는 중 문제가 발생했습니다.")
        print(f"    (오류 메시지: {e})")
        return

    initial_full_helpers_list = helpers_df['이름'].tolist()
    
    # 2. 요일 선택
    while True:
        user_input_day = input("\n배정할 요일을 입력하세요 (예: 목, 금, 토, 일) >> ").strip()
        matched_column = None
        for col in helpers_df.columns:
            if user_input_day in col:
                matched_column = col
                break
        if matched_column:
            selected_day_column = matched_column
            print(f"Info: '{selected_day_column}' 열을 기준으로 배정을 시작합니다.")
            break
        else:
            day_cols = [c for c in helpers_df.columns if any(d in c for d in ['월','화','수','목','금','토','일'])]
            print(f"❌ '{user_input_day}'에 해당하는 요일을 찾을 수 없습니다.")
            print(f"    인식된 요일 관련 열: {day_cols}")

    day_available_df = helpers_df[helpers_df[selected_day_column].astype(str) == '1']
    day_available_helpers_list = day_available_df['이름'].tolist()

    # 3. 스케줄 파일 로드 및 '시설조' 작업 추가
    try:
        df = pd.read_csv(schedule_file)
    except FileNotFoundError:
        print(f"❌ 오류: 스케줄 파일('{schedule_file}')을 찾을 수 없습니다.")
        return

    df['배정된 도우미'] = ''
    df_sorted = df.sort_values(by='시작시간', key=lambda x: pd.to_datetime(x, format='%p %I:%M')).reset_index(drop=True)

    # ❗ [핵심 수정] '시설조 활동'을 별도의 작업으로 맨 앞에 추가
    facility_crew_task = pd.DataFrame([{'시작시간': '하루 종일', '종료시간': '-', '일정': '시설조 활동', '필요 도우미 수': '10', '배정된 도우미': ''}])
    df_sorted = pd.concat([facility_crew_task, df_sorted], ignore_index=True)
    
    full_helpers_list = helpers_df['이름'].tolist()
    helper_schedules = {name: [] for name in full_helpers_list}
    excluded_crew_members = []

    # 4. 대화형 배정 루프
    i = 0
    while i < len(df_sorted):
        task = df_sorted.iloc[i]
        
        # ❗ [핵심 수정] 현재 작업이 '시설조 활동'일 경우, 전용 로직 실행
        if task['일정'] == '시설조 활동':
            print("\n" + "="*50)
            print("▶ [특별 작업] 시설조 배정")
            print("="*50)
            
            # 시설조 배정 로직 시작
            current_crew = [h.strip() for h in str(task['배정된 도우미']).split(',') if h.strip()]
            
            print(f"✅ [{selected_day_column}] 참석 가능한 도우미 명단입니다.")
            grouped = day_available_df.groupby('팀')['이름'].apply(list)
            for team, names in grouped.items():
                if names: print(f"- {team}: {', '.join(names)}")
            
            while True:
                if current_crew:
                    print(f"\n--- 현재 배정된 시설조: {', '.join(current_crew)}")
                
                needed = 10 - len(current_crew)
                user_input = input(f"시설조 인원을 입력하세요 ({needed}명 남음, '-' 붙이면 제외, 'n' 입력 시 완료) >> ")

                if user_input.lower() == 'n':
                    excluded_crew_members = current_crew # 최종 확정
                    print(f"✅ 시설조 배정 완료. 총 {len(excluded_crew_members)}명 제외됨.")
                    break
                
                if user_input.startswith('-'):
                    name_to_remove = user_input[1:].strip()
                    if name_to_remove in current_crew:
                        current_crew.remove(name_to_remove)
                        print(f"  - {name_to_remove} 님 제외.")
                    else:
                        print(f"  - '{name_to_remove}' 님은 명단에 없습니다.")
                    continue

                input_names = [name.strip() for name in user_input.split(',')]
                
                if len(current_crew) + len(input_names) > 10:
                    can_add = 10 - len(current_crew)
                    print(f"⚠️ 10명 초과. {can_add}명만 배정합니다.")
                    input_names = input_names[:can_add]

                for name in input_names:
                    if name in day_available_helpers_list:
                        if name not in current_crew:
                            current_crew.append(name)
                            print(f"  + {name} 추가됨. ({len(current_crew)}/10)")
                        else:
                            print(f"  - {name} 님은 이미 추가되었습니다.")
                    else:
                        print(f"  - '{name}' 님은 오늘 참석 가능 명단에 없습니다.")
                
                df_sorted.at[i, '배정된 도우미'] = ', '.join(current_crew)

            i += 1
            continue

        # --- 일반 작업 배정 로직 ---
        current_helpers_list = [h.strip() for h in str(task['배정된 도우미']).split(',') if h.strip()]
        assigned_count = len(current_helpers_list)
        needed_count = parse_helpers_needed(task['필요 도우미 수'])

        print("\n" + "="*60)
        print(f"✅ [{selected_day_column}] 현재 배정 가능한 도우미 명단입니다. (시설조 제외)")
        final_available_df = day_available_df[~day_available_df['이름'].isin(excluded_crew_members)]
        grouped = final_available_df.groupby('팀')['이름'].apply(list)
        for team, names in grouped.items():
            if names:
                print(f"- {team}: {', '.join(names)}")
        print("="*60)

        print(f"\n▶ 작업 [{i}/{len(df_sorted)-1}]") # 인덱스 조정
        print(f"    - 시간: {task['시작시간']} ~ {task['종료시간']}")
        print(f"    - 일정: {task['일정']}")
        print(f"    - 필요 인원: {needed_count}명 (현재 {assigned_count}명 배정됨)")
        print(f"    - 현재 배정된 도우미: {', '.join(current_helpers_list) if current_helpers_list else '없음'}")
        print("="*50)

        if assigned_count >= needed_count:
            print("✅ 필요 인원이 모두 배정되었습니다.")
            i += 1
            continue
        
        user_input = input("\n\n배정할 도우미 (n: 다음, b: 이전, j: 점프, q: 종료, '-' 붙이면 제외) >> ")

        if user_input.lower() == 'q':
            print("작업을 중단하고 현재까지의 내용을 저장합니다.")
            break 
        elif user_input.lower() == 'n': i += 1; continue
        elif user_input.lower() == 'b': i = max(0, i - 1); continue
        elif user_input.lower() == 'j':
            print("\n--- 점프할 일정 선택 ---")
            for idx, row in df_sorted.iterrows():
                schedule_name = str(row['일정']).strip().replace('\n', ' ')
                print(f"{idx + 1}. {schedule_name:<20}", end='\t')
                if (idx + 1) % 5 == 0: print()
            print("\n-------------------------")
            
            try:
                choice = int(input("이동할 번호를 입력하세요 >> ")) - 1
                if 0 <= choice < len(df_sorted):
                    i = choice
                    continue
                else: print("❌ 잘못된 번호입니다.")
            except ValueError: print("❌ 숫자를 입력해야 합니다.")
            continue

        elif not user_input: print("입력값이 없습니다."); continue

        task_start_dt, task_end_dt = to_datetime(task['시작시간']), to_datetime(task['종료시간'])
        available_helpers_list_after_crew = final_available_df['이름'].tolist()

        if user_input.startswith('-'):
            name_to_remove = user_input[1:].strip()
            if name_to_remove in current_helpers_list:
                current_helpers_list.remove(name_to_remove)
                df_sorted.at[i, '배정된 도우미'] = ', '.join(current_helpers_list)
                if name_to_remove in helper_schedules:
                    for item in helper_schedules[name_to_remove]:
                        if item[0] == task_start_dt and item[1] == task_end_dt:
                            helper_schedules[name_to_remove].remove(item)
                            break
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
            if name not in available_helpers_list_after_crew:
                if name in excluded_crew_members: unavailable_names.append(f"{name}(시설조)")
                else: unavailable_names.append(name)
                continue
            
            is_conflicted = False
            for start, end in helper_schedules.get(name, []):
                if task_start_dt < end and task_end_dt > start:
                    is_conflicted = True
                    conflicted_names.append(f"{name}({start.strftime('%H:%M')}~{end.strftime('%H:%M')})")
                    break
            if not is_conflicted: valid_names.append(name)

        newly_assigned = []
        for name in valid_names:
            if name not in current_helpers_list:
                current_helpers_list.append(name)
                helper_schedules[name].append((task_start_dt, task_end_dt))
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

    # 5. 최종 결과 저장
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    final_output_file = f'assignment_{selected_day_column}_{timestamp}.csv'
    
    # 시설조 활동의 '필요 도우미 수'를 최종 인원으로 업데이트
    df_sorted.loc[df_sorted['일정'] == '시설조', '필요 도우미 수'] = len(excluded_crew_members)
    
    df_sorted.to_csv(final_output_file, index=False, encoding='utf-8-sig')
    print(f"\n✅ [{selected_day_column}] 배정 작업이 완료되었습니다.")
    print(f"결과가 '{final_output_file}' 파일에 저장되었습니다. 프로그램을 종료합니다.")

if __name__ == '__main__':
    run_assignment_tool()