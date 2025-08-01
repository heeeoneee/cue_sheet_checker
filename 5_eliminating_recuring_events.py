import pandas as pd
import sys

if len(sys.argv) != 3:
    print("❌ 오류: 파일 경로가 올바르게 전달되지 않았습니다.")
    print("사용법: python 5_eliminating_recuring_events.py <입력_파일_경로> <출력_파일_경로>")
    sys.exit(1)

input_file = sys.argv[1]
output_file = sys.argv[2]

try:
    df = pd.read_csv(input_file)
    
    if df.empty:
        print("경고: 입력 파일이 비어있어, 빈 출력 파일을 생성합니다.")
        # 빈 파일이라도 생성해야 다음 단계에서 FileNotFoundError가 나지 않음
        pd.DataFrame().to_csv(output_file, index=False)
    else:
        df['group_id'] = ((df['일정'] != df['일정'].shift()) | \
                          (df['장소'] != df['장소'].shift())).cumsum()

        indices_to_keep = []

        for name, group in df.groupby('group_id'):
            if len(group) <= 2:
                indices_to_keep.extend(group.index)
            else:
                indices_to_keep.append(group.index[0])
                indices_to_keep.append(group.index[-1])

        indices_to_keep = sorted(list(set(indices_to_keep)))
        final_df = df.loc[indices_to_keep].drop(columns=['group_id'])

        final_df.to_csv(output_file, index=False, encoding='utf-8-sig')

    print(f"✅ 중복 작업 제거 완료! 최종 작업 목록을 '{output_file}' 파일로 저장했습니다.")

except FileNotFoundError:
    print(f"❌ 오류: '{input_file}' 파일을 찾을 수 없습니다.")
    sys.exit(1)
except Exception as e:
    print(f"데이터 처리 중 오류가 발생했습니다: {e}")
    sys.exit(1) # ❗ [핵심 수정] 실패 시 종료 코드 1 반환