import sqlite3

def create_database(db_name="facilities.db", schema_file="schema.sql"):
    # Connect to SQLite
    connection = sqlite3.connect(db_name)
    cursor = connection.cursor()

    # Read the SQL schema and execute it
    with open(schema_file, 'r') as f:
        schema = f.read()
        cursor.executescript(schema)

    # Commit changes and close the connection
    connection.commit()
    connection.close()
    print(f"Database created in {db_name}")

if __name__ == "__main__":
    create_database()