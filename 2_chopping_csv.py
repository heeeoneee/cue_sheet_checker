import os
import csv
import sys
import re # 정규 표현식 모듈 임포트

# --- 설정 (이 부분을 사용자 환경에 맞게 수정하세요) ---
# 1. 처리할 CSV 파일들이 있는 디렉토리 경로를 지정하세요.
#    이전 스크립트에서 다운로드된 CSV 파일들이 저장된 'downloaded_csv_files' 디렉토리를 지정하면 됩니다.
INPUT_DIRECTORY = 'downloaded_csv_files'

# 2. 다듬어진 CSV 파일들을 저장할 새로운 디렉토리 경로를 지정하세요.
#    이 폴더가 없으면 스크립트가 자동으로 생성합니다.
OUTPUT_DIRECTORY = 'processed_csv_files'

# --- 함수 정의 ---

def process_csv_file(input_filepath, output_filepath):
    """
    단일 CSV 파일을 읽어 요청된 최소 규칙에 따라 데이터를 다듬고 새로운 CSV 파일로 저장합니다.
    규칙:
    1. 원본 A열 제거.
    2. 원본 H열 제거.
    3. 원본 1행부터 4행까지 제거.
    4. 원본 B열에 "추가 내용"이 적힌 행부터 그 이후는 모두 제거.
       만약 "추가 내용"이 없으면, 파일의 끝까지 모든 데이터를 유지합니다.
    5. 원본 B열 4행(B:4)의 프로그램 이름을 추출하여 반환합니다.

    Args:
        input_filepath (str): 원본 CSV 파일의 전체 경로.
        output_filepath (str): 다듬어진 데이터를 저장할 새 CSV 파일의 전체 경로.
    
    Returns:
        tuple: (성공 여부 bool, 추출된 프로그램 이름 str)
    """
    program_name = "UnknownProgram" # 기본 프로그램 이름
    try:
        with open(input_filepath, 'r', newline='', encoding='utf-8') as infile:
            reader = csv.reader(infile)
            
            # 모든 행을 리스트로 읽어들입니다.
            rows = list(reader)

            # --- 프로그램 이름 추출 (원본 B열 4행, 0-indexed: rows[3][1]) ---
            if len(rows) > 3 and len(rows[3]) > 1 and rows[3][1].strip():
                program_name = rows[3][1].strip()
            else:
                print(f"  경고: '{os.path.basename(input_filepath)}' 파일의 B열 4행에서 프로그램 이름을 찾을 수 없습니다. 'UnknownProgram'으로 처리합니다.")

            # --- 데이터 시작 행 및 끝 행 결정 ---
            # 데이터는 원본 5행부터 시작 (0-indexed: 인덱스 4)
            data_start_row_idx = 4
            
            # 기본적으로 원본 데이터의 마지막 행까지 유지
            data_end_row_idx = len(rows) 
            
            # 1. "추가 내용"이 적힌 행 찾기 (원본 B열, 인덱스 1)
            # 원본 5행부터 시작하여 검색
            for r_idx_original in range(data_start_row_idx, len(rows)):
                if len(rows[r_idx_original]) > 1 and rows[r_idx_original][1].strip() == "추가 내용":
                    data_end_row_idx = r_idx_original # "추가 내용" 행 바로 전까지 유지
                    break

            # 최종적으로 유지할 행들 슬라이싱 (원본 5행부터 위에서 결정된 끝 행까지)
            if data_start_row_idx < data_end_row_idx:
                rows = rows[data_start_row_idx:data_end_row_idx]
            else:
                # 데이터가 없거나, 시작 행이 끝 행보다 크거나 같으면 빈 리스트
                rows = []
                print(f"  경고: '{os.path.basename(input_filepath)}' 파일에서 유효한 데이터 행을 찾을 수 없습니다.")

            # --- 원본 A열 및 H열 제거 ---
            # 원본 A열(인덱스 0)과 H열(인덱스 7)을 제외하고 나머지 열만 추가
            final_processed_rows = []
            for row in rows:
                processed_row = []
                for col_idx_original, cell_value in enumerate(row):
                    if col_idx_original == 0 or col_idx_original == 7: # A열 또는 H열
                        continue
                    processed_row.append(cell_value)
                final_processed_rows.append(processed_row)
            
            # 최종적으로 처리된 행들을 processed_rows에 할당
            processed_rows = final_processed_rows

        # 다듬어진 데이터를 새로운 CSV 파일로 저장
        with open(output_filepath, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.writer(outfile)
            writer.writerows(processed_rows)
        
        print(f"  -> '{os.path.basename(input_filepath)}' 파일 다듬기 완료. '{os.path.basename(output_filepath)}'로 저장되었습니다.")
        return True, program_name

    except FileNotFoundError:
        print(f"오류: '{input_filepath}' 파일을 찾을 수 없습니다.")
        return False, program_name # 오류 발생 시에도 프로그램 이름 반환
    except Exception as e:
        print(f"오류: '{input_filepath}' 파일 처리 중 예상치 못한 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        return False, program_name # 오류 발생 시에도 프로그램 이름 반환

def process_all_csv_files_in_directory(input_dir, output_dir):
    """
    지정된 입력 디렉토리 내의 모든 CSV 파일을 다듬고 새로운 출력 디렉토리에 저장합니다.

    Args:
        input_dir (str): 원본 CSV 파일들이 있는 디렉토리 경로.
        output_dir (str): 다듬어진 CSV 파일들을 저장할 디렉토리 경로.
    """
    if not os.path.exists(input_dir):
        print(f"오류: 입력 디렉토리 '{input_dir}'를 찾을 수 없습니다.")
        sys.exit(1)

    os.makedirs(output_dir, exist_ok=True)
    print(f"다듬어진 CSV 파일들을 '{OUTPUT_DIRECTORY}' 디렉토리에 저장합니다.")

    processed_count = 0
    for filename in os.listdir(input_dir):
        if filename.endswith('.csv'):
            input_filepath = os.path.join(input_dir, filename)
            
            print(f"'{filename}' 파일 처리 중...")
            success, program_name = process_csv_file(input_filepath, "temp_output.csv") # 임시 파일명 사용

            if success:
                # 파일명을 원본과 동일하게 유지
                output_filename = filename
                output_filepath = os.path.join(output_dir, output_filename)

                # 임시 파일을 최종 파일명으로 이동
                os.rename("temp_output.csv", output_filepath)
                processed_count += 1
            else:
                # 처리 실패 시 임시 파일이 남아있을 수 있으므로 삭제 시도
                if os.path.exists("temp_output.csv"):
                    os.remove("temp_output.csv")
    
    print(f"\n총 {processed_count}개의 CSV 파일이 다듬어져 '{output_dir}'에 저장되었습니다.")
    if processed_count == 0 and len(os.listdir(input_dir)) > 0:
        print(f"참고: '{input_dir}' 디렉토리에 CSV 파일이 없거나 처리할 수 있는 파일이 없었습니다.")


# --- 스크립트 실행 ---
if __name__ == "__main__":
    # INPUT_DIRECTORY에 이전 단계에서 다운로드된 CSV 파일들이 있는 폴더 경로를 지정하세요.
    # OUTPUT_DIRECTORY에 다듬어진 CSV 파일들이 저장될 새로운 폴더 경로를 지정하세요.
    process_all_csv_files_in_directory(INPUT_DIRECTORY, OUTPUT_DIRECTORY)
