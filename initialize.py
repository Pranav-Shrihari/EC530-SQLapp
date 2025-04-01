import sqlite3
import pandas as pd

# Load the CSV data into a DataFrame
df = pd.read_csv('data.csv')

# Connect to SQLite (if the database doesn't exist, it will be created)
conn = sqlite3.connect('company.db')
cursor = conn.cursor()

# Create the table
cursor.execute('''
CREATE TABLE IF NOT EXISTS employees (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    age INTEGER NOT NULL,
    department TEXT NOT NULL
);
''')

# Run a query to select all employees
cursor.execute("SELECT * FROM employees")
rows = cursor.fetchall()

# Display the results
for row in rows:
    print(row)

# Insert data into the 'employees' table
df.to_sql('employees', conn, if_exists='replace', index=False)

# Commit and close the connection
conn.commit()
conn.close()