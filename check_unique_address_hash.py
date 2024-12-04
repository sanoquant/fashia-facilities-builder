import sqlite3
import pandas as pd

# Connecting to SQLite
conn = sqlite3.connect('facilities.db')
cursor = conn.cursor()


# Identify duplicate hashes
duplicated_hashes = pd.read_sql_query("""
    SELECT Address_Hash
    FROM addresses
    GROUP BY Address_Hash
    HAVING COUNT(*) > 1;
""", conn)
# Update related entities
for hash_value in duplicated_hashes['Address_Hash']:
    update_query = f"""
        UPDATE entities
        SET Entity_unique_to_address = FALSE
        WHERE CCN IN (
            SELECT e.CCN
            FROM entities e
            JOIN addresses a ON e.CCN = a.CCN
            WHERE a.Address_Hash = '{hash_value}'
        );
    """
    cursor.execute(update_query)

conn.commit()
# audit process
updated_entities = pd.read_sql_query("""
    SELECT CCN, NPI, Entity_unique_to_address
    FROM entities
    WHERE Entity_unique_to_address = FALSE;
""", conn)
updated_entities.to_csv('datasets/output/updated_entities_log.csv', index=False)
conn.close()
print("Proceso completado con éxito.")