import sqlite3
import pandas as pd
import os
import sys
import traceback
from datetime import datetime

# Function to log errors
def log_error(error_msg):
    with open("error_log.txt", "a") as f:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        f.write(f"[{timestamp}] ERROR: {error_msg}\n")

# Function to log info/success
def log_info(info_msg):
    with open("error_log.txt", "a") as f:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        f.write(f"[{timestamp}] INFO: {info_msg}\n")

# Load the CSV data into a DataFrame
df = pd.read_csv('data.csv')
log_info("CSV file 'data.csv' loaded successfully.")

# Function to inspect column names and data types
def inspect_columns_and_types(dataframe):
    column_info = {}
    for column in dataframe.columns:
        column_info[column] = str(dataframe[column].dtype)
    return column_info

# Function to generate CREATE TABLE statement dynamically based on DataFrame columns and types
def generate_create_table_statement(table_name, dataframe):
    columns = inspect_columns_and_types(dataframe)
    create_statement = f"CREATE TABLE IF NOT EXISTS {table_name} ("
    
    column_definitions = []
    for column, dtype in columns.items():
        # Map pandas dtypes to SQL column types
        if dtype == 'object':  # String data
            column_definitions.append(f"{column} TEXT")
        elif dtype == 'int64':  # Integer data
            column_definitions.append(f"{column} INTEGER")
        elif dtype == 'float64':  # Floating-point data
            column_definitions.append(f"{column} REAL")
        else:
            column_definitions.append(f"{column} TEXT")  # Default to TEXT if not specifically handled
    
    create_statement += ", ".join(column_definitions) + ");"
    return create_statement

try:
    # Connect to SQLite
    conn = sqlite3.connect('company.db')
    log_info("Connected to SQLite database 'company.db'.")
    cursor = conn.cursor()

    table_name = 'employees'
    user_table_name = table_name

    # Check if the table exists and inspect its schema
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
    if cursor.fetchone():
        cursor.execute(f"PRAGMA table_info({table_name});")
        existing_columns = [col[1] for col in cursor.fetchall()]
        new_columns = df.columns.tolist()

        if set(existing_columns) == set(new_columns):
            print(f"\n Schema conflict detected for table '{table_name}'.")
            print(f"Existing columns: {existing_columns}")
            print(f"New columns from CSV: {new_columns}")
            log_info("Schema conflict detected.")

            choice = input("Choose action - (O)verwrite, (R)ename new table, or (S)kip: ").strip().lower()
            if choice == 'o':
                cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                print(f"Table '{table_name}' dropped.")
                log_info(f"Table '{table_name}' dropped.")
            elif choice == 'r':
                suffix = input("Enter a suffix for the new table name: ").strip()
                user_table_name = f"{table_name}_{suffix}"
                print(f"New table will be created as '{user_table_name}'.")
                log_info(f"New table will be created as '{user_table_name}'.")
            elif choice == 's':
                log_info("User chose to skip table creation and data insertion.")
                print("Skipping table creation and data insertion.")
                conn.close()
                sys.exit()
            else:
                log_info("Invalid input received during schema conflict resolution.")
                print("Invalid choice. Exiting.")
                conn.close()
                sys.exit()

    # Generate and execute CREATE TABLE statement
    create_table_sql = generate_create_table_statement(user_table_name, df)
    print(f"\nGenerated SQL for creating table '{user_table_name}':\n{create_table_sql}")
    cursor.execute(create_table_sql)
    log_info(f"Table '{user_table_name}' created or confirmed.")

    # Insert data
    df.to_sql(user_table_name, conn, if_exists='replace', index=False)
    log_info(f"Data inserted into table '{user_table_name}'.")

    # Fetch and display data
    cursor.execute(f"SELECT * FROM {user_table_name}")
    rows = cursor.fetchall()
    for row in rows:
        print(row)

    # Commit and close
    log_info(f"Retrieved and displayed data from '{user_table_name}'.")
    conn.commit()
    conn.close()
    log_info("SQLite connection closed. Script completed successfully.\n")

except Exception as e:
    error_details = traceback.format_exc()
    log_error(error_details)
    print(f"\n An error occurred: {e}\n(Details logged to error_log.txt)")
