import pandas as pd
from datetime import datetime, timedelta

# 입력 파일과 출력 파일 이름을 지정합니다.
input_file = 'helper_tasks_list_final.csv'
output_file = 'event_schedule.csv'

# [추가된 로직] 시간 문자열에 15분을 더하는 함수
def add_15_minutes(time_str):
    """ 'PM 5:30'과 같은 시간 문자열을 받아 15분을 더한 뒤, 다시 문자열로 반환합니다. """
    try:
        # AM/PM을 포함한 시간 형식에 맞춰 datetime 객체로 변환합니다.
        # 예: 'PM 5:30' -> datetime 객체 (오늘 날짜의 17:30)
        time_obj = datetime.strptime(time_str, '%p %I:%M')
        
        # 변환된 시간에 15분을 더합니다.
        new_time_obj = time_obj + timedelta(minutes=15)
        
        # 15분이 더해진 시간을 다시 'PM 5:45' 형태의 문자열로 변환하여 반환합니다.
        return new_time_obj.strftime('%p %I:%M').replace('AM', 'AM ').replace('PM', 'PM ').strip()
    except ValueError:
        # 시간 형식이 맞지 않는 등 오류가 발생하면 원래 값을 그대로 반환합니다.
        return time_str

try:
    # 최종 작업 목록 파일을 읽어옵니다.
    df = pd.read_csv(input_file)

    # 처리된 이벤트 정보를 저장할 빈 리스트를 생성합니다.
    events = []

    # '일정'과 '장소'가 동일한 연속된 행을 하나의 그룹으로 묶습니다.
    group_ids = ((df['일정'] != df['일정'].shift()) | \
                 (df['장소'] != df['장소'].shift())).cumsum()

    # 생성된 그룹 ID를 기준으로 데이터를 반복 처리합니다.
    for group_id, group_df in df.groupby(group_ids):
        first_row = group_df.iloc[0]
        start_time = first_row['시간']
        
        if len(group_df) == 1:
            raw_end_time = start_time
        else:
            raw_end_time = group_df.iloc[-1]['시간']
            
        # [수정된 로직] 종료 시간에 15분을 더하는 함수를 호출합니다.
        final_end_time = add_15_minutes(raw_end_time)
        
        # 새로운 이벤트 정보를 딕셔너리 형태로 만듭니다.
        event_info = {
            '시작시간': start_time,
            '종료시간': final_end_time, # 15분이 더해진 최종 종료 시간을 사용
            '일정': first_row['일정'],
            '장소': first_row['장소'],
            '세부 내용': first_row['세부 내용'],
            '담당자': first_row['담당자'],
            '필요 도우미 수': first_row['필요 도우미 수'],
            '배정된 도우미': first_row['배정된 도우미']
        }
        events.append(event_info)

    # 이벤트 리스트를 새로운 데이터프레임으로 변환합니다.
    events_df = pd.DataFrame(events)
    
    # 결과를 새로운 CSV 파일로 저장합니다.
    events_df.to_csv(output_file, index=False, encoding='utf-8-sig')

    print(f"✅ 종료 시간 +15분 처리 완료! '{output_file}' 파일이 생성되었습니다.")
    print("\n수정된 이벤트 스케줄 미리보기:")
    print(events_df.head())

except FileNotFoundError:
    print(f"❌ 오류: '{input_file}' 파일을 찾을 수 없습니다.")
    print("스크립트와 동일한 폴더에 파일이 있는지, 파일 이름이 정확한지 확인해주세요.")
except Exception as e:
    print(f"데이터 처리 중 오류가 발생했습니다: {e}")