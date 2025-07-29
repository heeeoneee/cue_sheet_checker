import os
import csv
import sys
import re # 정규 표현식 모듈 임포트

# --- 설정 (이 부분을 사용자 환경에 맞게 수정하세요) ---
# 1. 이전 단계에서 다듬어진 CSV 파일들이 있는 디렉토리 경로를 지정하세요.
#    'processed_csv_files' 디렉토리를 지정하면 됩니다.
INPUT_DIRECTORY = 'processed_csv_files'

# 2. 최종 처리된 CSV 파일들을 저장할 새로운 디렉토리 경로를 지정하세요.
#    이 폴더가 없으면 스크립트가 자동으로 생성합니다.
OUTPUT_DIRECTORY = 'final_processed_csv_files'

# --- 함수 정의 ---

def post_process_single_csv_file(input_filepath, output_filepath):
    """
    단일 CSV 파일을 읽어 G열 이름 중복 제거 및 H열 숫자 합산 규칙을 적용합니다.

    규칙:
    1. G열(인덱스 5)은 이름이 겹치면 중복을 제거합니다.
    2. H열(인덱스 6)은 숫자일 경우 '명' 단어를 제거하고 정수 합산합니다. 한국어가 있다면 그대로 둡니다.

    Args:
        input_filepath (str): 원본 CSV 파일의 전체 경로.
        output_filepath (str): 처리된 데이터를 저장할 새 CSV 파일의 전체 경로.
    
    Returns:
        bool: 처리 성공 여부.
    """
    processed_rows = []
    try:
        with open(input_filepath, 'r', newline='', encoding='utf-8') as infile:
            reader = csv.reader(infile)
            
            for row in reader:
                current_row = list(row) # 현재 행을 복사하여 수정

                # G열 (인덱스 5) 처리: 이름 중복 제거
                if len(current_row) > 5: # G열이 존재하는지 확인 (새로운 인덱스 5)
                    g_cell_value = current_row[5].strip()
                    if g_cell_value:
                        all_names = set()
                        for name_part in g_cell_value.split(','):
                            if name_part.strip():
                                all_names.add(name_part.strip())
                        current_row[5] = ", ".join(sorted(list(all_names)))

                # H열 (인덱스 6) 처리: 숫자 합산 ('명' 제거, 한국어 유지)
                if len(current_row) > 6: # H열이 존재하는지 확인 (새로운 인덱스 6)
                    h_cell_value = current_row[6].strip()
                    if h_cell_value:
                        # 한국어 문자 포함 여부 확인 (한글 자모 범위)
                        has_korean = bool(re.search(r'[\uac00-\ud7a3]', h_cell_value))

                        if has_korean:
                            # 한국어가 포함되어 있다면 그대로 유지
                            pass 
                        else:
                            # 한국어가 없다면 숫자 합산 로직 적용
                            total_sum = 0
                            # '명' 글자를 제거하고 숫자만 추출 (소수점 포함)
                            cleaned_h_value = h_cell_value.replace('명', '').strip()
                            
                            # 숫자만으로 구성된 경우에만 합산
                            numbers_found = re.findall(r'[-+]?\d*\.?\d+', cleaned_h_value)
                            
                            # 모든 추출된 문자열이 숫자로만 이루어져 있는지 확인
                            if all(re.fullmatch(r'[-+]?\d*\.?\d+', num_str) for num_str in numbers_found) and numbers_found:
                                for num_str in numbers_found:
                                    try:
                                        total_sum += int(float(num_str)) # float으로 변환 후 int로 변환하여 정수 합산
                                    except ValueError:
                                        pass # 숫자가 아니면 무시 (이 경우는 fullmatch로 걸러지지만 안전을 위해)
                                current_row[6] = f"{total_sum}명" # 합산 결과에 '명' 붙여서 저장
                            # else: 숫자가 아닌 다른 내용(이름 등)이 있으면 그대로 유지 (pass)
                
                processed_rows.append(current_row)

        # 처리된 데이터를 새로운 CSV 파일로 저장
        with open(output_filepath, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.writer(outfile)
            writer.writerows(processed_rows)
        
        print(f"  -> '{os.path.basename(input_filepath)}' 파일 후처리 완료. '{os.path.basename(output_filepath)}'로 저장되었습니다.")
        return True

    except FileNotFoundError:
        print(f"오류: '{input_filepath}' 파일을 찾을 수 없습니다.")
        return False
    except Exception as e:
        print(f"오류: '{input_filepath}' 파일 처리 중 예상치 못한 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        return False

def post_process_all_csv_files_in_directory(input_dir, output_dir):
    """
    지정된 입력 디렉토리 내의 모든 CSV 파일을 후처리하고 새로운 출력 디렉토리에 저장합니다.

    Args:
        input_dir (str): 원본 CSV 파일들이 있는 디렉토리 경로.
        output_dir (str): 처리된 CSV 파일들을 저장할 디렉토리 경로.
    """
    if not os.path.exists(input_dir):
        print(f"오류: 입력 디렉토리 '{input_dir}'를 찾을 수 없습니다.")
        sys.exit(1)

    os.makedirs(output_dir, exist_ok=True)
    print(f"최종 처리된 CSV 파일들을 '{output_dir}' 디렉토리에 저장합니다.")

    processed_count = 0
    for filename in os.listdir(input_dir):
        if filename.endswith('.csv'):
            input_filepath = os.path.join(input_dir, filename)
            output_filepath = os.path.join(output_dir, filename) # 원본 파일명 유지

            print(f"'{filename}' 파일 후처리 중...")
            if post_process_single_csv_file(input_filepath, output_filepath):
                processed_count += 1
    
    print(f"\n총 {processed_count}개의 CSV 파일이 후처리되어 '{output_dir}'에 저장되었습니다.")
    if processed_count == 0 and len(os.listdir(input_dir)) > 0:
        print(f"참고: '{input_dir}' 디렉토리에 CSV 파일이 없거나 처리할 수 있는 파일이 없었습니다.")


# --- 스크립트 실행 ---
if __name__ == "__main__":
    # INPUT_DIRECTORY에 이전 단계에서 다듬어진 CSV 파일들이 있는 폴더 경로를 지정하세요.
    # OUTPUT_DIRECTORY에 최종 처리된 CSV 파일들이 저장될 새로운 폴더 경로를 지정하세요.
    post_process_all_csv_files_in_directory(INPUT_DIRECTORY, OUTPUT_DIRECTORY)
