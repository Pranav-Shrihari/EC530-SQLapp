import sqlite3
import pandas as pd

# Load the CSV data into a DataFrame
df = pd.read_csv('data.csv')

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

# Connect to SQLite (if the database doesn't exist, it will be created)
conn = sqlite3.connect('company.db')
cursor = conn.cursor()

# Generate the CREATE TABLE statement dynamically
table_name = 'employees'
create_table_sql = generate_create_table_statement(table_name, df)

# Print the generated SQL statement (for debugging or inspection)
print(f"Generated SQL for creating table '{table_name}':\n{create_table_sql}")

# Execute the CREATE TABLE statement
cursor.execute(create_table_sql)

# Insert data into the 'employees' table
df.to_sql('employees', conn, if_exists='replace', index=False)

# Run a query to select all employees
cursor.execute("SELECT * FROM employees")
rows = cursor.fetchall()

# Display the results
for row in rows:
    print(row)

# Commit and close the connection
conn.commit()
conn.close()