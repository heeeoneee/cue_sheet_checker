import os
import csv
import sys
import re # 정규 표현식 모듈 임포트

# --- 설정 (이 부분을 사용자 에 맞게 수정하세요) ---
# 1. 처리할 CSV 파일들이 있는 디렉토리 경로를 지정하세요.
#    이전 스크립트에서 다운로드된 CSV 파일들이 저장된 'downloaded_csv_files' 디렉토리를 지정하면 됩니다.
INPUT_DIRECTORY = 'downloaded_csv_files'

# 2. 다듬어진 CSV 파일들을 저장할 새로운 디렉토리 경로를 지정하세요.
#    이 폴더가 없으면 스크립트가 자동으로 생성합니다.
OUTPUT_DIRECTORY = 'processed_csv_files'

# --- 함수 정의 ---

def process_csv_file(input_filepath, output_filepath):
    """
    단일 CSV 파일을 읽어 요청된 규칙에 따라 데이터를 다듬고 새로운 CSV 파일로 저장합니다.
    규칙:
    1. 원본 A열 제거.
    2. 원본 H열 제거.
    3. 원본 1행부터 4행까지 제거.
    4. 원본 B열에 "추가 내용"이 적힌 행부터 그 이후는 모두 제거.
       만약 "추가 내용"이 없으면, 파일의 끝까지 모든 데이터를 유지합니다.
    5. 수정된 CSV 파일의 A, B, C, D열에서 빈칸은 바로 윗 셀의 값으로 채웁니다.
    6. 수정된 CSV 파일의 A, B, C열 값이 위 행과 동일하면, 현재 행의 D열부터 나머지 셀은 모두 위의 셀에 수직 병합합니다.
    7. 원본 B열 4행(B:4)의 프로그램 이름을 추출하여 반환합니다.

    Args:
        input_filepath (str): 원본 CSV 파일의 전체 경로.
        output_filepath (str): 다듬어진 데이터를 저장할 새 CSV 파일의 전체 경로.
    
    Returns:
        tuple: (성공 여부 bool, 추출된 프로그램 이름 str)
    """
    program_name = "UnknownProgram" # Default program name
    try:
        with open(input_filepath, 'r', newline='', encoding='utf-8') as infile:
            reader = csv.reader(infile)
            
            # Read all rows into a list.
            rows = list(reader)

            # --- Extract program name (Original B column, 4th row; 0-indexed: rows[3][1]) ---
            if len(rows) > 3 and len(rows[3]) > 1 and rows[3][1].strip():
                program_name = rows[3][1].strip()
            else:
                print(f"  Warning: Could not find program name in B column, 4th row of '{os.path.basename(input_filepath)}'. Processing as 'UnknownProgram'.")

            # --- Determine data start and end rows ---
            # Data starts from original 5th row (0-indexed: index 4)
            data_start_row_idx = 4
            
            # By default, keep until the last row of the original data
            data_end_row_idx = len(rows) 
            
            # 1. Find the row containing "추가 내용" in original B column (index 1)
            # Search from original 5th row onwards
            for r_idx_original in range(data_start_row_idx, len(rows)):
                if len(rows[r_idx_original]) > 1 and rows[r_idx_original][1].strip() == "추가 내용":
                    data_end_row_idx = r_idx_original # Keep up to the row *before* "추가 내용"
                    break

            # Slice the rows to keep (from original 5th row up to the determined end row)
            if data_start_row_idx < data_end_row_idx:
                rows = rows[data_start_row_idx:data_end_row_idx]
            else:
                # If no data or start row is greater than or equal to end row, result is an empty list
                rows = []
                print(f"  Warning: No valid data rows found in '{os.path.basename(input_filepath)}'.")

            # --- Remove original A column and H column ---
            # Exclude original A column (index 0) and H column (index 7) and add the rest
            rows_after_col_removal = []
            for row in rows:
                processed_row = []
                for col_idx_original, cell_value in enumerate(row):
                    if col_idx_original == 0 or col_idx_original == 7: # A column or H column
                        continue
                    processed_row.append(cell_value)
                rows_after_col_removal.append(processed_row)
            
            # --- 5. Fill empty cells in A, B, C, D columns (new indices 0, 1, 2, 3) with the value from the cell directly above. ---
            num_cols_to_fill_down = 4 # A, B, C, D columns (new indices after A, H removal)
            last_values_fill_down = [None] * num_cols_to_fill_down # Stores the last non-empty value for each column
            
            rows_after_fill_down = [] # This list will store the result after fill-down
            for r_idx, row in enumerate(rows_after_col_removal):
                current_row_for_fill = list(row) # Create a copy to modify

                # Ensure the row has at least enough columns for fill-down operation
                while len(current_row_for_fill) < num_cols_to_fill_down:
                    current_row_for_fill.append('')

                for col_idx_fill in range(num_cols_to_fill_down): # Iterate through A, B, C, D columns (indices 0, 1, 2, 3)
                    if col_idx_fill < len(current_row_for_fill): # Check if the column exists in the current row
                        cell_value_fill = current_row_for_fill[col_idx_fill].strip() # Get cell value and strip whitespace

                        if not cell_value_fill: # If the current cell is empty (after stripping)
                            if last_values_fill_down[col_idx_fill] is not None:
                                # Fill with the last non-empty value found in this column from an above row
                                current_row_for_fill[col_idx_fill] = last_values_fill_down[col_idx_fill]
                        else: # If the current cell is NOT empty
                            # Update the last_values_fill_down for this column with the current cell's value
                            last_values_fill_down[col_idx_fill] = cell_value_fill
                
                rows_after_fill_down.append(current_row_for_fill) # Add the processed (filled) row to the final list

            # --- 6. A, B, C 컬럼 값이 위 행과 동일하다면 D열부터 나머지 셀은 모두 위의 셀에 수직 병합 ---
            final_merged_rows = []
            if rows_after_fill_down:
                final_merged_rows.append(list(rows_after_fill_down[0])) # 첫 번째 행은 항상 추가 (list()로 복사하여 원본 변경 방지)

                for i in range(1, len(rows_after_fill_down)):
                    current_row = rows_after_fill_down[i]
                    previous_row = final_merged_rows[-1] # 마지막으로 추가된 행을 previous_row로 사용

                    # A, B, C 컬럼(인덱스 0, 1, 2) 비교
                    # 비교할 컬럼이 모두 존재하는지 확인
                    if (len(current_row) >= 3 and len(previous_row) >= 3 and
                        current_row[0].strip() == previous_row[0].strip() and # A열 비교
                        current_row[1].strip() == previous_row[1].strip() and # B열 비교
                        current_row[2].strip() == previous_row[2].strip()): # C열 비교
                        
                        # D열(인덱스 3)부터 나머지 셀을 위의 셀에 수직 병합
                        # previous_row와 current_row의 길이를 맞춰줍니다.
                        max_len_for_merge = max(len(previous_row), len(current_row)) 
                        while len(previous_row) < max_len_for_merge:
                            previous_row.append('')
                        while len(current_row) < max_len_for_merge: # current_row도 길이를 맞춰줍니다.
                            current_row.append('')

                        for col_idx in range(3, max_len_for_merge): # D열(인덱스 3)부터 시작
                            current_cell_value = current_row[col_idx].strip()
                            previous_cell_value = previous_row[col_idx].strip()

                            if current_cell_value: # 현재 셀에 값이 있다면
                                if previous_cell_value: # 이전 셀에도 값이 있다면 콤마로 연결
                                    previous_row[col_idx] = f"{previous_cell_value}, {current_cell_value}"
                                else: # 이전 셀이 비어있다면 현재 셀 값으로 채움
                                    previous_row[col_idx] = current_cell_value
                            # else: 현재 셀이 비어있다면 이전 셀 값을 그대로 유지 (아무것도 하지 않음)
                        # 현재 행은 병합되었으므로 final_merged_rows에 추가하지 않음
                    else:
                        # A,B,C가 다르다면 새로운 행으로 추가 (list()로 복사하여 원본 변경 방지)
                        final_merged_rows.append(list(current_row))
            
            # 최종적으로 처리된 행들을 processed_rows에 할당
            processed_rows = final_merged_rows

        # Save the processed data to a new CSV file
        with open(output_filepath, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.writer(outfile)
            writer.writerows(processed_rows)
        
        print(f"  -> Processed '{os.path.basename(input_filepath)}'. Saved to '{os.path.basename(output_filepath)}'.")
        return True, program_name

    except FileNotFoundError:
        print(f"Error: Could not find file '{input_filepath}'.")
        return False, program_name # Return program name even on error
    except Exception as e:
        print(f"Error: An unexpected error occurred while processing '{input_filepath}': {e}")
        import traceback
        traceback.print_exc()
        return False, program_name # Return program name even on error

def process_all_csv_files_in_directory(input_dir, output_dir):
    """
    Processes all CSV files in the specified input directory and saves them to a new output directory.

    Args:
        input_dir (str): Path to the directory containing original CSV files.
        output_dir (str): Path to the directory where processed CSV files will be saved.
    """
    if not os.path.exists(input_dir):
        print(f"Error: Input directory '{input_dir}' not found.")
        sys.exit(1)

    os.makedirs(output_dir, exist_ok=True)
    print(f"Saving processed CSV files to '{OUTPUT_DIRECTORY}' directory.")

    processed_count = 0
    for filename in os.listdir(input_dir):
        if filename.endswith('.csv'):
            input_filepath = os.path.join(input_dir, filename)
            
            print(f"Processing '{filename}'...")
            success, program_name = process_csv_file(input_filepath, "temp_output.csv") # Use temporary filename

            if success:
                # Keep the original filename
                output_filename = filename
                output_filepath = os.path.join(output_dir, output_filename)

                # Move the temporary file to the final filename
                os.rename("temp_output.csv", output_filepath)
                processed_count += 1
            else:
                # Attempt to delete temporary file if processing failed
                if os.path.exists("temp_output.csv"):
                    os.remove("temp_output.csv")
    
    print(f"\nTotal {processed_count} CSV files processed and saved to '{output_dir}'.")
    if processed_count == 0 and len(os.listdir(input_dir)) > 0:
        print(f"Note: No CSV files found in '{input_dir}' or no files could be processed.")


# --- Script execution ---
if __name__ == "__main__":
    # Specify the path to the folder containing downloaded CSV files.
    # Specify the path to the new folder where processed CSV files will be saved.
    process_all_csv_files_in_directory(INPUT_DIRECTORY, OUTPUT_DIRECTORY)
