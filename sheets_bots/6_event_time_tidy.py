import pandas as pd
from datetime import datetime, timedelta
import sys

if len(sys.argv) != 3:
    print("❌ 오류: 파일 경로가 올바르게 전달되지 않았습니다.")
    print("사용법: python 6_event_time_tidy.py <입력_파일_경로> <출력_파일_경로>")
    sys.exit(1)

input_file = sys.argv[1]
output_file = sys.argv[2]

def add_15_minutes(time_str):
    """ 'PM 5:30'과 같은 시간 문자열을 받아 15분을 더한 뒤, 다시 문자열로 반환합니다. """
    try:
        time_obj = datetime.strptime(time_str, '%p %I:%M')
        new_time_obj = time_obj + timedelta(minutes=15)
        return new_time_obj.strftime('%p %I:%M').replace('AM', 'AM ').replace('PM', 'PM ').strip()
    except ValueError:
        return time_str

try:
    df = pd.read_csv(input_file)

    if df.empty:
        print("경고: 입력 파일이 비어있어, 빈 출력 파일을 생성합니다.")
        pd.DataFrame().to_csv(output_file, index=False)
    else:
        events = []
        # ❗ [핵심 수정] 그룹핑 조건에 '필요 도우미 수'를 추가했습니다.
        group_ids = ((df['일정'] != df['일정'].shift()) | \
                     (df['장소'] != df['장소'].shift()) | \
                     (df['필요 도우미 수'] != df['필요 도우미 수'].shift())).cumsum()

        for group_id, group_df in df.groupby(group_ids):
            first_row = group_df.iloc[0]
            start_time = first_row['시간']
            
            if len(group_df) == 1:
                raw_end_time = start_time
            else:
                raw_end_time = group_df.iloc[-1]['시간']
                
            final_end_time = add_15_minutes(raw_end_time)
            
            event_info = {
                '시작시간': start_time,
                '종료시간': final_end_time,
                '일정': first_row['일정'],
                '장소': first_row['장소'],
                '세부 내용': first_row['세부 내용'],
                '담당자': first_row['담당자'],
                '필요 도우미 수': first_row['필요 도우미 수'],
                '배정된 도우미': first_row['배정된 도우미']
            }
            events.append(event_info)

        events_df = pd.DataFrame(events)
        
        # '시작시간'을 기준으로 데이터프레임을 올바르게 정렬합니다.
        events_df = events_df.sort_values(by='시작시간', key=lambda x: pd.to_datetime(x, format='%p %I:%M')).reset_index(drop=True)
        
        events_df.to_csv(output_file, index=False, encoding='utf-8-sig')

    print(f"✅ 종료 시간 +15분 처리 및 시간 순 정렬 완료! '{output_file}' 파일이 생성되었습니다.")

except FileNotFoundError:
    print(f"❌ 오류: '{input_file}' 파일을 찾을 수 없습니다.")
    sys.exit(1)
except Exception as e:
    print(f"데이터 처리 중 오류가 발생했습니다: {e}")
    sys.exit(1)