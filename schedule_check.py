import csv
from collections import defaultdict
import datetime
import re

# --- 헬퍼 함수 (이전과 동일) ---

def parse_time(time_str: str):
    """'7:30', '오후 21:00' 등 다양한 시간 형식의 문자열을 datetime.time 객체로 변환합니다."""
    time_str = time_str.strip()
    if not time_str:
        return None
    try:
        match = re.search(r'(\d{1,2}:\d{2})', time_str)
        if match:
            time_part = match.group(1)
            hour, minute = map(int, time_part.split(':'))
            return datetime.time(hour, minute)
    except (ValueError, IndexError):
        pass
    return None

def parse_helpers(helper_str: str):
    """'김준민, 박주영(리더)' 와 같은 도우미 이름 문자열을 개별 이름 리스트로 분리하고 정제합니다."""
    if not helper_str or helper_str.strip() in ['-', '미정']:
        return []
    names = helper_str.split(',')
    cleaned_names = []
    for name in names:
        name_no_paren = re.sub(r'\(.*\)', '', name).strip()
        if name_no_paren:
            cleaned_names.append(name_no_paren)
    return cleaned_names

# --- 신규/개선된 기능 함수 ---

def load_all_helpers(file_path: str):
    """
    '도우미 명단' CSV 파일에서 전체 운영위원/도우미 명단, 팀, 참여 가능 요일을 읽어옵니다.
    반환값: {'이름': {'team': '팀이름', 'days': ['요일1', '요일2']}, ...} 형태의 딕셔너리
    """
    all_helpers_data = defaultdict(lambda: {'team': '미지정', 'days': []})
    day_map = {'수': '수요일', '목': '목요일', '금': '금요일', '토': '토요일', '일': '일요일'}
    
    try:
        with open(file_path, mode='r', encoding='utf-8') as infile:
            rows = list(csv.reader(infile))
            
            teams = rows[1]
            names = rows[2]
            availability_rows = rows[3:8]

            for i, name in enumerate(names):
                name = name.strip()
                if not name or i == 0:
                    continue
                
                team_name = teams[i].strip() if i < len(teams) else "미지정"
                all_helpers_data[name]['team'] = team_name

                for row in availability_rows:
                    day_short = row[0].strip()
                    day_full = day_map.get(day_short)
                    if day_full and len(row) > i and row[i] == '1':
                        all_helpers_data[name]['days'].append(day_full)
                        
    except FileNotFoundError:
        print(f"❌ 오류: 전체 도우미 명단 파일을 찾을 수 없습니다:\n   {file_path}")
        return None
    except Exception as e:
        print(f"도우미 명단 파일을 읽는 중 오류가 발생했습니다: {e}")
        return None
        
    return all_helpers_data

def find_available_helpers(target_day, start_search_time, end_search_time, all_helpers, assigned_schedules):
    """
    ❗ [기능 추가] 특정 요일과 '시간 간격'에 투입 가능한 인원을 찾습니다.
    (기존의 특정 시점 검색은 이 함수를 활용하여 처리)
    """
    # 1. 해당 요일에 참여 가능한 인원 필터링
    available_on_day = {name for name, data in all_helpers.items() if target_day in data['days']}
    
    # 2. 해당 시간 간격과 겹치는 일정이 있는 인원(배정 불가 인원) 찾기
    unavailable_helpers = set()
    for helper, schedules in assigned_schedules.items():
        for day, start_assigned, end_assigned, _ in schedules:
            # 요일이 같고, 검색 시간 간격과 배정된 시간이 겹치면 배정 불가
            # 겹치는 조건: 내 일정 시작시간 < 검색 종료시간 AND 검색 시작시간 < 내 일정 종료시간
            if day == target_day and start_assigned < end_search_time and start_search_time < end_assigned:
                unavailable_helpers.add(helper)
    
    # 3. 참여 가능 인원에서 배정 불가 인원을 제외하여 최종 목록 생성
    final_available_list = sorted(list(available_on_day - unavailable_helpers))
    return final_available_list

# --- 메인 분석 함수 ---

def analyze_and_search(schedule_path: str, helpers_list_path: str):
    """
    일정 파일을 분석하여 중복을 확인하고, 전체 명단과 대조하여 실시간으로 비어있는 인원을 검색합니다.
    """
    assigned_schedules = defaultdict(list)
    try:
        with open(schedule_path, mode='r', encoding='utf-8') as infile:
            reader = csv.reader(infile)
            header = next(reader)
            
            for row in reader:
                if len(row) < len(header) or not row[0].strip():
                    continue
                day, start_str, end_str, event, helpers_str = row[0], row[1], row[2], row[3], row[10]
                start_time, end_time = parse_time(start_str), parse_time(end_str)
                
                cleaned_helpers = parse_helpers(helpers_str)
                if start_time and end_time and cleaned_helpers:
                    # 💡 [개선] chr(10)은 줄바꿈 문자로, 공백으로 치환하여 가독성 확보
                    info = (day.strip(), start_time, end_time, f"{start_str.strip()}-{end_str.strip()} {event.strip().replace(chr(10), ' ')}")
                    for helper in cleaned_helpers:
                        assigned_schedules[helper].append(info)

    except FileNotFoundError:
        print(f"❌ 오류: 일정 파일을 찾을 수 없습니다:\n   {schedule_path}")
        return
    except Exception as e:
        print(f"일정 파일을 읽는 중 오류가 발생했습니다: {e}")
        return

    # 1. 중복 일정 분석
    print("=" * 60)
    print("✅ 도우미 일정 분석 결과 (1/2) - 중복 배정 확인")
    print("=" * 60)
    
    found_overlap = False
    for helper, schedules in assigned_schedules.items():
        schedules.sort(key=lambda x: (x[0], x[1])) # 요일과 시작 시간으로 정렬
        i = 0
        while i < len(schedules) - 1:
            conflict_group = [schedules[i]]
            j = i + 1
            while j < len(schedules) and schedules[i][0] == schedules[j][0] and schedules[i][2] > schedules[j][1]:
                conflict_group.append(schedules[j])
                j += 1
            
            if len(conflict_group) > 1:
                found_overlap = True
                print(f"  - ❗️ 중복 발견: {helper} ({conflict_group[0][0]})")
                for _, _, _, info in conflict_group:
                    print(f"    - {info}")
                print() 
            
            i = j

    if not found_overlap:
        print("  -> 분석 결과: 시간이 겹치게 배정된 도우미를 찾지 못했습니다.")

    # 2. 실시간 인원 검색 및 일정 조회
    print("\n" + "=" * 60)
    print("✅ 도우미 일정 분석 결과 (2/2) - 실시간 가능 인원 및 개인 일정 검색")
    print("=" * 60)
    
    all_helpers = load_all_helpers(helpers_list_path)
    if all_helpers is None: return

    # 💡 [개선] 사용자 편의성을 위한 검색 안내 문구 강화
    print("아래 형식 중 하나로 입력하여 검색하세요.")
    print("  1. 특정 인원 일정 검색: '이름' (예: 홍길동)")
    print("  2. 특정 시점 가능 인원 검색: '요일 시간' (예: 금 10:00)")
    print("  3. 특정 시간 간격 가능 인원 검색: '요일 시작시간 종료시간' (예: 토 13:00 15:30)")
    print("\n👉 검색을 종료하려면 '종료' 또는 'exit'을 입력하세요.\n")
    
    while True:
        try:
            user_input = input("검색어 입력: ").strip()
            if user_input.lower() in ['종료', 'exit']:
                print("프로그램을 종료합니다.")
                break
            
            parts = user_input.split()
            
            # ❗ [기능 추가] 입력 형식에 따라 분기 처리 (1: 이름, 2: 시점, 3: 기간)
            
            # --- 1. 이름으로 개인 일정 검색 ---
            if len(parts) == 1:
                name_to_search = parts[0]
                if name_to_search in assigned_schedules:
                    print(f"\n--- {name_to_search}님의 배정된 일정 ---")
                    # 요일별로 그룹화하여 출력
                    schedules_by_day = defaultdict(list)
                    for day, _, _, info in assigned_schedules[name_to_search]:
                        schedules_by_day[day].append(info)
                    
                    for day, day_schedules in sorted(schedules_by_day.items()):
                        print(f"  [{day}]")
                        for schedule_info in day_schedules:
                            print(f"    - {schedule_info}")
                    print("\n" + "-" * 50)
                elif name_to_search in all_helpers:
                     print(f"\n-> '{name_to_search}'님은 전체 명단에 있지만, 배정된 일정이 없습니다.\n")
                else:
                    print(f"\n-> '{name_to_search}'님을 전체 명단에서 찾을 수 없습니다.\n")
            
            # --- 2. 특정 시점/기간으로 가능 인원 검색 ---
            elif len(parts) == 2 or len(parts) == 3:
                target_day_input = parts[0]
                if not target_day_input.endswith("요일"):
                    target_day_input += "요일"

                start_search_time = parse_time(parts[1])
                # 특정 시점 검색일 경우, 종료시간을 시작시간과 동일하게 설정하여 처리
                end_search_time = parse_time(parts[2]) if len(parts) == 3 else start_search_time
                
                if start_search_time is None or end_search_time is None:
                    raise ValueError("시간 형식이 올바르지 않습니다.")
                
                if start_search_time > end_search_time:
                    raise ValueError("시작 시간이 종료 시간보다 늦을 수 없습니다.")

                available_list = find_available_helpers(target_day_input, start_search_time, end_search_time, all_helpers, assigned_schedules)
                
                time_range_str = f"{parts[1]}"
                if len(parts) == 3:
                    time_range_str += f" ~ {parts[2]}"
                
                grouped_by_team = defaultdict(list)
                for name in available_list:
                    team = all_helpers[name]['team']
                    grouped_by_team[team].append(name)

                print(f"\n--- {target_day_input} {time_range_str}에 투입 가능한 인원 ({len(available_list)}명) ---")
                if grouped_by_team:
                    for team, members in sorted(grouped_by_team.items()):
                        print(f"\n  👥 [{team} ({len(members)}명)]")
                        for i in range(0, len(members), 5):
                            print("     " + ", ".join(members[i:i+5]))
                else:
                    print("  투입 가능한 인원이 없거나, 해당 요일에 참여 가능한 인원이 없습니다.")
                print("\n" + "-" * 50)
                
            else:
                raise ValueError("입력 형식이 올바르지 않습니다.")

        except ValueError as e:
            print(f"❗️ 잘못된 형식입니다. 안내된 형식에 맞게 입력해주세요. (오류: {e})")
        except Exception as e:
            print(f"검색 중 오류가 발생했습니다: {e}")

if __name__ == "__main__":
    # ❗ 사용자의 환경에 맞게 파일 경로를 수정해주세요.
    schedule_file = '/Users/heeeonlee/2025KYSA/QueueSheets/initial_csv_files/2025 KYSA 운영위원 통합 큐시트_도우미 배정용.csv'
    helper_list_file = '/Users/heeeonlee/2025KYSA/QueueSheets/initial_csv_files/2025 KYSA 운영위원 통합 큐시트_도우미 명단.csv'
    
    analyze_and_search(schedule_file, helper_list_file)