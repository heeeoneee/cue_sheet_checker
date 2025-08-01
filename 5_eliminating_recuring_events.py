import pandas as pd

# 이전에 '담당자'까지 포함하여 만들었던 파일을 읽어옵니다.
input_file = 'linearlized.csv'
# 최종 결과물을 저장할 파일 이름입니다.
output_file = 'recruing_events_eliminated.csv'

try:
    df = pd.read_csv(input_file)

    # --- [수정된 핵심 로직] ---
    # '일정' 또는 '장소'가 이전 행과 달라지는 지점을 정확히 찾아 그룹 ID를 부여합니다.
    # 괄호를 수정하여 '또는(|)' 조건 전체에 대해 .cumsum()이 적용되도록 바로잡았습니다.
    df['group_id'] = ((df['일정'] != df['일정'].shift()) | \
                      (df['장소'] != df['장소'].shift())).cumsum()

    # 최종적으로 유지할 행의 인덱스를 저장할 리스트를 생성합니다.
    indices_to_keep = []

    # 수정된 group_id를 기준으로 데이터를 그룹화합니다.
    for name, group in df.groupby('group_id'):
        if len(group) <= 2:
            # 그룹의 작업이 2개 이하면 모두 유지합니다.
            indices_to_keep.extend(group.index)
        else:
            # 그룹의 작업이 3개 이상이면, 첫 번째와 마지막 작업만 유지합니다.
            indices_to_keep.append(group.index[0])
            indices_to_keep.append(group.index[-1])

    # 유지하기로 한 인덱스를 사용해 최종 데이터프레임을 만듭니다.
    # 정렬을 통해 원래 순서를 보존하고, 임시로 쓴 group_id 열은 삭제합니다.
    indices_to_keep = sorted(list(set(indices_to_keep)))
    final_df = df.loc[indices_to_keep].drop(columns=['group_id'])

    # 결과를 새로운 CSV 파일로 저장합니다.
    final_df.to_csv(output_file, index=False, encoding='utf-8-sig')

    print(f"✅ 중복 작업 제거 완료! 최종 작업 목록을 '{output_file}' 파일로 저장했습니다.")
    print("\n수정된 데이터 미리보기:")
    print(final_df.head(10))

except FileNotFoundError:
    # 사용자의 로컬 환경에서 실행될 것이므로, 파일 경로를 다시 한번 확인하라는 안내를 남깁니다.
    print(f"❌ 오류: '{input_file}' 파일을 찾을 수 없습니다.")
    print("스크립트와 동일한 폴더에 파일이 있는지, 파일 이름이 정확한지 확인해주세요.")
except Exception as e:
    print(f"데이터 처리 중 오류가 발생했습니다: {e}")