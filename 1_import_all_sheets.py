import gspread
import csv
import os
import sys

# --- Configuration (Modify these settings as needed for your environment) ---
# 1. Google Sheets API and Google Drive API activation, service account creation:
#    - Go to Google Cloud Console (console.cloud.google.com).
#    - Create a new project or select an existing one.
#    - In 'APIs & Services' -> 'Library', search for and **enable both** 'Google Sheets API' and 'Google Drive API'.
#    - In 'APIs & Services' -> 'Credentials', select 'Create Credentials' -> 'Service Account'.
#    - Specify a service account name and set its role to 'Project' -> 'Viewer' or 'Google Sheets API' -> 'Google Sheets Viewer'
#      and 'Google Drive API' -> 'Google Drive Viewer'. (Viewer permission is sufficient for read-only access.)
#    - After creating the service account, generate and download the JSON key. Enter the path to this file below.
#    - The downloaded JSON file is typically named in the format 'yourprojectname-serviceaccountID.json'.

# --- IMPORTANT: Enter the full path to your service account JSON file here. ---
SERVICE_ACCOUNT_FILE_PATH = '/Users/heeeonlee/2025KYSA/QueueSheets/queuesheetsmaker-1505a6a800f7.json' # Full path to your downloaded service account JSON file.

# 2. Enter the ID of the specific Google Spreadsheet you want to download.
#    From the URL: https://docs.google.com/spreadsheets/d/1Vu6j1GYGu7_mCLSMfjbxkYDOrXBavnbTNzQOjZiIUgk/edit?gid=1439439840#gid=1439439840
#    The string between 'd/' and '/edit' in the URL is the Spreadsheet ID.
SPECIFIC_SPREADSHEET_ID = '1Vu6j1GYGu7_mCLSMfjbxkYDOrXBavnbTNzQOjZiIUgk'

# 3. Specify the local directory path to save the CSV files.
#    The script will create this folder if it doesn't exist.
OUTPUT_DIRECTORY = 'initial_csv_files'

# List of worksheet names to download (based on your screenshot)
WORKSHEET_NAMES_TO_DOWNLOAD = [
    "8.13(수)", # 8.13(Wed)
    "8.14(목)", # 8.14(Thu)
    "8.15(금)", # 8.15(Fri)
    "8.16(토)", # 8.16(Sat)
    "8.17(일)",  # 8.17(Sun)
    "운영위 명단"
]

# --- Function Definitions ---

def download_multiple_sheets_to_csv(spreadsheet_id, output_directory, service_account_file_path, sheet_names):
    """
    Downloads the content of multiple specified worksheets from a Google Spreadsheet
    into separate local CSV files.
    """
    print(f"Current Python interpreter: {sys.executable}")
    print(f"gspread version: {gspread.__version__}")

    # Validate service account file path
    if not os.path.exists(service_account_file_path):
        print(f"Error: Service account JSON file path is incorrect or file does not exist: '{service_account_file_path}'")
        print("Please re-check the path or place the file in the specified location.")
        sys.exit(1) # Exit script

    # Create output directory if it doesn't exist
    os.makedirs(output_directory, exist_ok=True)
    print(f"Saving CSV files to '{output_directory}' directory.")

    try:
        # Authenticate with Google Sheets API using gspread
        print(f"Attempting gspread authentication with service account file '{service_account_file_path}'...")
        gc = gspread.service_account(filename=service_account_file_path)
        print(f"gspread authentication successful! gc object type: {type(gc)}")

        print(f"Processing spreadsheet ID '{spreadsheet_id}'...")

        try:
            # Open the spreadsheet by its ID
            current_spreadsheet = gc.open_by_key(spreadsheet_id)
            print(f"Successfully opened spreadsheet '{current_spreadsheet.title}'.")

            download_count = 0
            # Iterate through each sheet name and download
            for sheet_name in sheet_names:
                print(f"\nAttempting to download sheet '{sheet_name}'...")
                try:
                    # Get the worksheet by its name
                    worksheet_to_download = current_spreadsheet.worksheet(sheet_name)
                    print(f"  -> Successfully found sheet '{worksheet_to_download.title}' corresponding to the name.")

                    # Sanitize file title for CSV filename
                    safe_file_title = "".join(c for c in current_spreadsheet.title if c.isalnum() or c in (' ', '.', '_')).strip()
                    if not safe_file_title: # Use a fallback name if title is empty
                        safe_file_title = f"untitled_spreadsheet_{spreadsheet_id[:8]}"
                    
                    # Sanitize worksheet title for CSV filename
                    safe_worksheet_title = "".join(c for c in worksheet_to_download.title if c.isalnum() or c in (' ', '.', '_')).strip()
                    if not safe_worksheet_title:
                        safe_worksheet_title = "sheet_unnamed" # Fallback if worksheet title is empty

                    output_csv_filename = os.path.join(output_directory, f"{safe_file_title}_{safe_worksheet_title}.csv")
                    
                    print(f"  -> Getting all data from sheet '{worksheet_to_download.title}'...")
                    all_values = worksheet_to_download.get_all_values()

                    if not all_values:
                        print(f"  Warning: Sheet '{worksheet_to_download.title}' has no data. An empty CSV file will be created.")

                    # Write data to CSV file
                    with open(output_csv_filename, 'w', newline='', encoding='utf-8') as f:
                        writer = csv.writer(f)
                        writer.writerows(all_values) # Write all fetched rows to the CSV file.

                    print(f"  -> Content of sheet '{worksheet_to_download.title}' from '{current_spreadsheet.title}' successfully downloaded to '{output_csv_filename}'.")
                    download_count += 1

                except gspread.exceptions.WorksheetNotFound:
                    print(f"Warning: Sheet '{sheet_name}' not found. Skipping this sheet.")
                except Exception as e:
                    print(f"Warning: An error occurred while processing sheet '{sheet_name}' ({e}). Skipping this sheet.")
                    import traceback
                    traceback.print_exc()

            print(f"\nTotal {download_count} spreadsheet sheets downloaded as individual CSV files.")

        except gspread.exceptions.SpreadsheetNotFound:
            print(f"Error: Spreadsheet ID '{spreadsheet_id}' not found or access denied. Please check the ID or ensure service account has permissions.")
            sys.exit(1)
        except Exception as e:
            print(f"Warning: An error occurred while processing spreadsheet '{spreadsheet_id}' ({e}).")
            import traceback
            traceback.print_exc()
            sys.exit(1)

    except gspread.exceptions.APIError as e:
        print(f"Google API error occurred: {e}")
        print("Please check service account permissions or Google Sheets API activation status.")
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        print("Detailed error information:")
        import traceback
        traceback.print_exc()
        sys.exit(1)

# --- Script Execution ---
if __name__ == "__main__":
    download_multiple_sheets_to_csv(SPECIFIC_SPREADSHEET_ID, OUTPUT_DIRECTORY, SERVICE_ACCOUNT_FILE_PATH, WORKSHEET_NAMES_TO_DOWNLOAD)