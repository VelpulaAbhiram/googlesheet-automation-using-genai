import gspread
import pandas as pd
import sqlite3
import json
import os

# --- Configuration ---
# Path to your service account credentials JSON file
CREDENTIALS_FILE = 'credentials.json'
# The name or URL of your Google Sheet
GOOGLE_SHEET_NAME = 'nxt' # e.g., 'Product Data'
# The name of the worksheet within the Google Sheet (e.g., 'Sheet1')
WORKSHEET_NAME = 'jul riunning'
# The name of the SQLite database file
DATABASE_NAME = 'data.db'
# The name of the table in the SQLite database
TABLE_NAME = 'google_sheet_data'

def get_service_account_client():
    """Authenticates with Google Sheets API using a service account."""
    if not os.path.exists(CREDENTIALS_FILE):
        print(f"Error: Credentials file '{CREDENTIALS_FILE}' not found.")
        print("Please download your service account JSON key and rename it to 'credentials.json'.")
        exit()
    try:
        gc = gspread.service_account(filename=CREDENTIALS_FILE)
        print("Successfully authenticated with Google Sheets API.")
        return gc
    except Exception as e:
        print(f"Error authenticating with Google Sheets API: {e}")
        print("Please ensure your 'credentials.json' file is valid and correctly configured.")
        exit()

def get_sheet_data(client, sheet_name, worksheet_name):
    """Fetches data from the specified Google Sheet and worksheet."""
    try:
        # Open the spreadsheet by name or URL
        spreadsheet = client.open(sheet_name)
        # Select the worksheet
        worksheet = spreadsheet.worksheet(worksheet_name)
        # Get all records as a list of dictionaries
        data = worksheet.get_all_records()
        print(f"Successfully fetched data from '{sheet_name}' - '{worksheet_name}'.")
        return pd.DataFrame(data)
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"Error: Google Sheet '{sheet_name}' not found. Check the name or URL.")
        exit()
    except gspread.exceptions.WorksheetNotFound:
        print(f"Error: Worksheet '{worksheet_name}' not found in '{sheet_name}'. Check the worksheet name.")
        exit()
    except Exception as e:
        print(f"Error fetching data from Google Sheet: {e}")
        exit()

def create_or_update_sqlite_table(df, db_name, table_name):
    """Creates or updates an SQLite table with DataFrame content."""
    conn = None
    try:
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        # Drop table if it exists to ensure fresh data and schema (optional, for simplicity)
        # For production, consider more sophisticated update/upsert logic
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        print(f"Dropped existing table '{table_name}' (if any).")

        # Create table from DataFrame schema
        # Using 'IF NOT EXISTS' is safer if you don't want to drop
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        print(f"Successfully created/updated table '{table_name}' in '{db_name}'.")
        print(f"Inserted {len(df)} rows.")

    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during database operation: {e}")
    finally:
        if conn:
            conn.close()

def main():
    print("Starting Google Sheet to SQL automation...")
    
    # 1. Authenticate with Google Sheets API
    gc = get_service_account_client()

    # 2. Fetch data from Google Sheet
    df = get_sheet_data(gc, GOOGLE_SHEET_NAME, WORKSHEET_NAME)
    
    if not df.empty:
        # 3. Create or update SQLite table
        create_or_update_sqlite_table(df, DATABASE_NAME, TABLE_NAME)
    else:
        print("No data fetched from Google Sheet. Skipping database update.")

    print("Google Sheet to SQL automation finished.")

if __name__ == "__main__":
    main()
