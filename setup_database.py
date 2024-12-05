import sqlite3
import pandas as pd

def create_database(db_name="facilities.db", schema_file="schema.sql"):
    # Connect to SQLite
    connection = sqlite3.connect(db_name)
    cursor = connection.cursor()

    # Read the SQL schema and execute it
    with open(schema_file, 'r') as f:
        schema = f.read()
        cursor.executescript(schema)
        
    # Loading entities
    entities = pd.read_csv(
        'datasets/output/entities.csv', 
        dtype={"ccn": str},  # Adjust the column name according to your file
        low_memory=False  # Suppress warning for large files
    )
    entities.to_sql('entities', connection, if_exists='replace', index=False)

    # Loading addresses
    addresses = pd.read_csv('datasets/output/addresses.csv')
    addresses.to_sql('addresses', connection, if_exists='replace', index=False)

    # Loading states
    states = pd.read_csv('datasets/output/states.csv')
    states.to_sql('states', connection, if_exists='replace', index=False)

    print("Data loaded successfully into SQLite.")

    # Commit changes and close the connection
    connection.commit()
    connection.close()
    print(f"Database created in {db_name}")

if __name__ == "__main__":
    create_database()