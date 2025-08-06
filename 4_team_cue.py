import pandas as pd

# CSV 읽기 (인코딩 주의)
df = pd.read_csv("your_file.csv", encoding='utf-8-sig')

# 컬럼 이름 정리 (중요)
df.columns = df.columns.str.strip().str.replace('\ufeff', '')

# 요일 순서를 위한 맵
weekday_order = ['수요일', '목요일', '금요일', '토요일', '일요일']
weekday_map = {day: i for i, day in enumerate(weekday_order)}

# 정렬용 컬럼 추가
df['요일_정렬'] = df['요일'].map(weekday_map)
