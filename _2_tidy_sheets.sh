#!/bin/bash

# --- 스크립트 설정 ---
SCRIPT_3="3_tidy_cue_sheets.py"
SCRIPT_4="4_linearlize_cue_sheets.py"
SCRIPT_5="5_eliminating_recuring_events.py"
SCRIPT_6="6_event_time_tidy.py"

SOURCE_DIR="/Users/heeeonlee/2025KYSA/QueueSheets/initial_csv_files"
PROCESSED_DIR="/Users/heeeonlee/2025KYSA/QueueSheets/processed_csv_files"
FINAL_DIR="/Users/heeeonlee/2025KYSA/QueueSheets/final_schedule_files"

# --- 폴더 생성 ---
mkdir -p "$PROCESSED_DIR"
mkdir -p "$FINAL_DIR"

# ❗ [핵심 기능 추가] 성공 및 실패 현황을 추적하기 위한 변수 초기화
success_count=0
failure_count=0
declare -a successful_files
declare -a failed_files


# --- 파이프라인 함수 정의 ---
run_pipeline_for_file() {
    local source_file="$1"
    local base_name=$(basename "$source_file" .csv)

    echo "==========================================================="
    echo "🚀  Processing: $base_name"
    echo "==========================================================="

    local day_processed_dir="$PROCESSED_DIR/$base_name"
    mkdir -p "$day_processed_dir"
    echo "  -> Intermediate files will be saved in: $day_processed_dir"

    local output_3="$day_processed_dir/3_tidy.csv"
    local output_4="$day_processed_dir/4_linear.csv"
    local output_5="$day_processed_dir/5_eliminated.csv"
    local output_6="$FINAL_DIR/${base_name}_event_schedule.csv"

    echo "  [3/6] Tidy Cue Sheets..."
    python "$SCRIPT_3" "$source_file" "$output_3"
    if [ $? -ne 0 ]; then echo "  ❌ STAGE 3 FAILED."; return 1; fi

    echo "  [4/6] Linearize Cue Sheets..."
    python "$SCRIPT_4" "$output_3" "$output_4"
    if [ $? -ne 0 ]; then echo "  ❌ STAGE 4 FAILED."; return 1; fi

    echo "  [5/6] Eliminate Recurring Events..."
    python "$SCRIPT_5" "$output_4" "$output_5"
    if [ $? -ne 0 ]; then echo "  ❌ STAGE 5 FAILED."; return 1; fi

    echo "  [6/6] Tidy Event Times..."
    python "$SCRIPT_6" "$output_5" "$output_6"
    if [ $? -ne 0 ]; then echo "  ❌ STAGE 6 FAILED."; return 1; fi

    echo "✅  Successfully processed. Final output: $output_6"
    return 0 # 성공적으로 끝나면 0을 반환
}


# --- 메인 실행 로직 ---
echo "📂 Source directory: $SOURCE_DIR"

shopt -s extglob
files=("$SOURCE_DIR"/!(*도우미 명단*|*운영위 명단*).csv)

if [ ${#files[@]} -eq 0 ] || [ ! -e "${files[0]}" ]; then
    echo "❌ No processable CSV files found in the source directory."
    exit 1
fi

echo "-----------------------------------------------------------"
echo "📑 처리할 파일 목록:"
i=1
for file in "${files[@]}"; do
    echo "  $i. $(basename "$file")"
    ((i++))
done
echo "-----------------------------------------------------------"

read -p "처리할 파일의 번호를 입력하거나, 전체를 처리하려면 'all'을 입력하세요: " choice

if [[ "$choice" == "all" ]]; then
    echo "🔥 모든 CSV 파일에 대한 파이프라인을 시작합니다..."
    for file in "${files[@]}"; do
        run_pipeline_for_file "$file"
        # ❗ [핵심 기능 추가] 파이프라인 함수의 성공/실패 여부를 확인하고 기록
        if [ $? -eq 0 ]; then
            success_count=$((success_count + 1))
            successful_files+=("$(basename "$file")")
        else
            failure_count=$((failure_count + 1))
            failed_files+=("$(basename "$file")")
        fi
    done
elif [[ "$choice" =~ ^[0-9]+$ ]] && [ "$choice" -ge 1 ] && [ "$choice" -le ${#files[@]} ]; then
    selected_file="${files[$((choice-1))]}"
    run_pipeline_for_file "$selected_file"
    if [ $? -eq 0 ]; then
        success_count=$((success_count + 1))
        successful_files+=("$(basename "$selected_file")")
    else
        failure_count=$((failure_count + 1))
        failed_files+=("$(basename "$selected_file")")
    fi
else
    echo "❌ 잘못된 입력입니다. 1부터 ${#files[@]} 사이의 숫자 또는 'all'을 입력해야 합니다."
    exit 1
fi


# --- ❗ [핵심 기능 추가] 최종 결과 요약 ---
echo -e "\n\n==========================================================="
echo "📊                  Pipeline Summary                   "
echo "==========================================================="
echo "Total attempts: $((success_count + failure_count))"
echo "✅ Success: $success_count"
echo "❌ Failure: $failure_count"

if [ $success_count -gt 0 ]; then
    echo -e "\n--- Successful Files ---"
    for fname in "${successful_files[@]}"; do
        echo "  - $fname"
    done
fi

if [ $failure_count -gt 0 ]; then
    echo -e "\n--- Failed Files ---"
    for fname in "${failed_files[@]}"; do
        echo "  - $fname"
    done
fi
echo "==========================================================="
echo -e "\n🎉 모든 작업이 완료되었습니다."