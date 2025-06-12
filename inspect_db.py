import sqlite3

db_path = 'data/dw/smart_sales.db'
print(f"Inspecting database at: {db_path}\n")

try:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get all table names
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    if tables:
        print("--- Database Schema ---")
        # For each table, get the schema
        for table in tables:
            table_name = table[0]
            print(f"\n[Table: {table_name}]")

            # PRAGMA table_info() is the command to get column info
            cursor.execute(f"PRAGMA table_info({table_name});")
            columns = cursor.fetchall()

            for column in columns:
                # Column info is returned as: (id, name, type, notnull, default_value, pk)
                print(f"  - Column: {column[1]} (Type: {column[2]})")
        print("\n-----------------------")
    else:
        print("No tables found in this database.")

except sqlite3.Error as e:
    print(f"An error occurred: {e}")
finally:
    if conn:
        conn.close()




        