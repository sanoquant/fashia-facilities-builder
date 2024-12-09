import sqlite3
import pandas as pd

# Connecting to SQLite
conn = sqlite3.connect('facilities.db')
cursor = conn.cursor()


# Identify duplicate hashes
duplicated_hashes = pd.read_sql_query("""
    SELECT address_hash
    FROM addresses
    GROUP BY address_hash
    HAVING COUNT(*) > 1;
""", conn)
# Update related entities
for hash_value in duplicated_hashes['address_hash']:
    update_query = f"""
        UPDATE entities
        SET entity_unique_to_address = FALSE
        WHERE ccn IN (
            SELECT e.ccn
            FROM entities e
            JOIN addresses a ON e.ccn = a.ccn
            WHERE a.address_hash = '{hash_value}'
        );
    """
    cursor.execute(update_query)

conn.commit()
# audit process
updated_entities = pd.read_sql_query("""
    SELECT ccn, npi, entity_unique_to_address
    FROM entities
    WHERE entity_unique_to_address = FALSE;
""", conn)
updated_entities.to_csv('datasets/output/updated_entities_log.csv', index=False)
conn.close()
print("Process completed successfully.")