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

# Function to list tables in the database
def list_tables(cursor):
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    print("\nTables in the database:")
    if tables:
        for table in tables:
            print(f"- {table[0]}")
    else:
        print("No tables found.")
    log_info("User listed tables in the database.")

# Function to load CSV file
def load_csv_to_dataframe():
    file_path = input("Enter the full path of the CSV file to load: ").strip()
    if os.path.exists(file_path):
        try:
            df = pd.read_csv(file_path)
            log_info(f"CSV file '{file_path}' loaded successfully.")
            print(f"CSV file '{file_path}' loaded successfully.\n")
            return df
        except Exception as e:
            log_error(f"Failed to load CSV file: {e}")
            print(f"Failed to load CSV file: {e}")
            return None
    else:
        log_error(f"CSV file '{file_path}' does not exist.")
        print(f"CSV file '{file_path}' does not exist.")
        return None

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

def chatbot_interaction():
    # Connect to SQLite (if the database doesn't exist, it will be created)
    conn = sqlite3.connect('company.db')
    cursor = conn.cursor()

    while True:
        # Display options for the user
        print("\nChatbot: Welcome to the SQLite chatbot! Please choose an option:")
        print("1. Load a CSV file")
        print("2. List all tables in the database")
        print("3. Run an SQL query")
        print("4. Exit")

        user_choice = input("Enter the number of your choice: ").strip()

        try:
            if user_choice == "1":
                # Load CSV file and insert into table
                df = load_csv_to_dataframe()
                if df is not None:
                    table_name = input("Enter the name of the table to create: ").strip()
                    # Generate CREATE TABLE statement and execute it
                    create_table_sql = generate_create_table_statement(table_name, df)
                    cursor.execute(create_table_sql)
                    log_info(f"Table '{table_name}' created.")
                    # Insert data
                    df.to_sql(table_name, conn, if_exists='replace', index=False)
                    log_info(f"Data inserted into table '{table_name}'.")
                    print(f"Data inserted into table '{table_name}'.")

            elif user_choice == "2":
                # List all tables in the database
                list_tables(cursor)

            elif user_choice == "3":
                # Run an SQL query
                sql_query = input("Enter the SQL query to run: ").strip()
                try:
                    cursor.execute(sql_query)
                    rows = cursor.fetchall()
                    if rows:
                        for row in rows:
                            print(row)
                    else:
                        print("No data found.")
                    log_info(f"Executed SQL query: {sql_query}")
                except Exception as e:
                    log_error(f"SQL query failed: {e}")
                    print(f"SQL query failed: {e}")

            elif user_choice == "4":
                # Exit the chatbot
                print("Goodbye!")
                log_info("User chose to exit the chatbot.")
                break

            else:
                print("Invalid choice. Please enter a valid number (1-4).")

        except Exception as e:
            error_details = traceback.format_exc()
            log_error(error_details)
            print(f"\nAn error occurred: {e}\n(Details logged to error_log.txt)")

    # Close the connection
    conn.close()
    log_info("SQLite connection closed. Script completed.")

# Start chatbot interaction
if __name__ == "__main__":
    chatbot_interaction()
