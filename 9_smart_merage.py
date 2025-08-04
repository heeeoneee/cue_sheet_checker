import pandas as pd
import os
import glob
import re

# --- 헬퍼 함수 ---
def parse_helpers_needed(text):
    """ '7', '3+2' 같은 문자열을 정수(7, 5)로 변환합니다. """
    try:
        if isinstance(text, (int, float)):
            return int(text)
        if str(text).strip().isdigit(): return int(text)
        return sum([int(n) for n in re.findall(r'\d+', str(text))])
    except: return 0

def run_smart_merge_tool():
    # 1. 기준이 될 '기존 배정 완료 파일' 선택
    print("\n--- 1. [기준 파일] 업데이트할 기존 배정 파일을 선택하세요 ---")
    saved_files = glob.glob("assignment_*.csv")
    if not saved_files:
        print("❌ 기존에 배정한 파일이 없습니다."); return

    for i, f in enumerate(saved_files): print(f"  {i+1}. {f}")
    
    try:
        choice = int(input("번호 선택 >> ")) - 1
        if not 0 <= choice < len(saved_files):
            print("❌ 잘못된 번호입니다."); return
        base_assignment_file = saved_files[choice]
    except (ValueError, IndexError):
        print("❌ 잘못된 선택입니다."); return

    # 2. 비교 대상이 될 '새로운 스케줄 파일' 선택
    print(f"\n--- 2. [비교 대상] 새로 전처리된 스케줄 파일을 선택하세요 ---")
    updated_schedules_dir = "/Users/heeeonlee/2025KYSA/QueueSheets/final_schedule_files"
    updated_schedules = glob.glob(f"{updated_schedules_dir}/*_event_schedule.csv")
    if not updated_schedules:
        print(f"❌ '{updated_schedules_dir}' 폴더에 새로 전처리된 스케줄 파일이 없습니다."); return

    for i, f in enumerate(updated_schedules): print(f"  {i+1}. {os.path.basename(f)}")

    try:
        choice = int(input("번호 선택 >> ")) - 1
        if not 0 <= choice < len(updated_schedules):
            print("❌ 잘못된 번호입니다."); return
        updated_schedule_file = updated_schedules[choice]
    except (ValueError, IndexError):
        print("❌ 잘못된 선택입니다."); return

    # --- 데이터 로드 ---
    df_base = pd.read_csv(base_assignment_file)
    df_new = pd.read_csv(updated_schedule_file)
    
    facility_crew_base = df_base[df_base['일정'] == '시설조 활동']
    df_base_tasks = df_base[df_base['일정'] != '시설조 활동'].copy()

    # --- 비교 및 병합 로직 ---
    merge_keys = ['시작시간', '일정', '장소']
    df_base_tasks['merge_key'] = df_base_tasks[merge_keys].astype(str).agg('-'.join, axis=1)
    df_new['merge_key'] = df_new[merge_keys].astype(str).agg('-'.join, axis=1)

    if '배정된 도우미' in df_new.columns:
        df_new_structure = df_new.drop(columns=['배정된 도우미'])
    else:
        df_new_structure = df_new

    merged_df = pd.merge(df_new_structure, df_base_tasks[['merge_key', '배정된 도우미']], on='merge_key', how='left')
    merged_df['배정된 도우미'] = merged_df['배정된 도우미'].fillna('')
    
    added_rows, modified_rows, unchanged_rows, deleted_rows = [], [], [], []
    
    for index, base_row in df_base_tasks.iterrows():
        base_key = base_row['merge_key']
        new_row_match = df_new[df_new['merge_key'] == base_key]

        if new_row_match.empty:
            deleted_rows.append(base_row)
        else:
            new_row = new_row_match.iloc[0]
            is_different = False
            # ❗ [핵심 수정] 비교 기준에 '종료시간' 추가
            compare_cols = ['시작시간', '종료시간', '일정', '장소', '필요 도우미 수']
            
            for col in compare_cols:
                if col == '필요 도우미 수':
                    if parse_helpers_needed(new_row[col]) != parse_helpers_needed(base_row[col]):
                        is_different = True; break
                elif str(new_row[col]) != str(base_row[col]):
                    is_different = True; break
            
            if is_different:
                modified_rows.append({'기존': base_row, '변경': new_row})
            else:
                unchanged_rows.append(base_row)

    added_rows = df_new[~df_new['merge_key'].isin(df_base_tasks['merge_key'])].to_dict('records')

    # --- 변경 내역 미리보기 및 사용자 승인 ---
    print("\n" + "="*70)
    print("                🔍 변경 사항 확인 및 적용 🔍")
    print("="*70)

    final_rows = [row.to_dict() for row in unchanged_rows]
    
    if modified_rows:
        print("\n[🟠 수정된 작업]\n")
        all_mod = False
        for item in modified_rows:
            schedule_name = str(item['기존']['일정']).replace('\n', ' ')
            print(f"  - ({item['기존']['시작시간']}) {schedule_name}")
            # ❗ [핵심 수정] 변경된 내용을 더 상세하게 보여주도록 개선
            if str(item['기존']['종료시간']) != str(item['변경']['종료시간']):
                print(f"    (시간 변경) {item['기존']['종료시간']} -> {item['변경']['종료시간']}")
            if parse_helpers_needed(item['기존']['필요 도우미 수']) != parse_helpers_needed(item['변경']['필요 도우미 수']):
                print(f"    (인원 변경) {item['기존']['필요 도우미 수']} -> {item['변경']['필요 도우미 수']}")
            
            if not all_mod:
                confirm = input("    이 변경사항을 적용하시겠습니까? (y/n/all) >> ").lower().strip()
                if confirm == 'all': all_mod = True
            
            if all_mod or confirm == 'y':
                new_row_with_assignment = item['변경'].copy()
                new_row_with_assignment['배정된 도우미'] = item['기존']['배정된 도우미']
                final_rows.append(new_row_with_assignment.to_dict())
                print("    -> ✅ 적용됨")
            else:
                final_rows.append(item['기존'].to_dict())
                print("    -> ❌ 변경 취소됨")
            print()

    if added_rows:
        print("\n[⚪ 추가된 작업]\n")
        all_add = False
        for row in added_rows:
            schedule_name = str(row['일정']).replace('\n', ' ')
            print(f"  - ({row['시작시간']}) {schedule_name} (필요인원: {row['필요 도우미 수']})")

            if not all_add:
                confirm = input("    이 작업을 추가하시겠습니까? (y/n/all) >> ").lower().strip()
                if confirm == 'all': all_add = True

            if all_add or confirm == 'y':
                row['배정된 도우미'] = ''
                final_rows.append(row)
                print("    -> ✅ 추가됨")
            else:
                print("    -> ❌ 추가 취소됨")
            print()

    if deleted_rows:
        print("\n[🗑️  삭제된 작업]\n")
        all_del = False
        for row in deleted_rows:
            schedule_name = str(row['일정']).replace('\n', ' ')
            print(f"  - ({row['시작시간']}) {schedule_name}")

            if not all_del:
                confirm = input("    이 작업을 삭제하시겠습니까? (y/n/all) >> ").lower().strip()
                if confirm == 'all': all_del = True

            if all_del or confirm == 'y':
                print("    -> ✅ 삭제됨")
            else:
                final_rows.append(row.to_dict())
                print("    -> ❌ 삭제 취소됨")
            print()

    # --- 최종 파일 생성 ---
    final_df = pd.DataFrame(final_rows)
    
    trimmed_info = []
    for index, row in final_df.iterrows():
        needed_count = parse_helpers_needed(row['필요 도우미 수'])
        if needed_count == 0: continue
        assigned_list = [h.strip() for h in str(row['배정된 도우미']).split(',') if h.strip()]
        if len(assigned_list) > needed_count:
            trimmed_list = assigned_list[:needed_count]
            final_df.at[index, '배정된 도우미'] = ', '.join(trimmed_list)
            schedule_name = str(row['일정']).replace('\n', ' ')
            trimmed_info.append(f"- ({row['시작시간']}) {schedule_name}: {len(assigned_list)}명 -> {len(trimmed_list)}명으로 정리됨")

    if trimmed_info:
        print("\n--- ✂️  초과 인원 자동 정리 결과 ---")
        for info in trimmed_info:
            print(info)
        print("------------------------------------")

    if not facility_crew_base.empty:
        final_df = pd.concat([final_df, facility_crew_base], ignore_index=True)

    if 'merge_key' in final_df.columns:
        final_df = final_df.drop(columns=['merge_key'])

    output_filename = base_assignment_file
    final_df.to_csv(output_filename, index=False, encoding='utf-8-sig')

    print(f"\n✅ 병합 완료! '{output_filename}' 파일에 변경사항을 덮어쓰기 저장했습니다.")
    print("   이제 '8_resume_assignment.py'로 이 파일을 열어 추가/수정된 작업을 마무리하세요.")

if __name__ == '__main__':
    run_smart_merge_tool()
