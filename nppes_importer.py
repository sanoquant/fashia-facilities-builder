import os
import pandas as pd
import hashlib

# File paths
nppes_file = "./datasets/filtered/nppes_filtered_data_1.csv"  # Input NPPES dataset
cms_file = "./datasets/output/entities.csv"      # Input CMS dataset
output_updated_cms = "./datasets/output/updated_cms_data.csv"  # Output for updated CMS records
output_new_entities = "new_entities.csv"    # Output for new entities
# Output files for the Addresses and States tables
addresses_file = "datasets/output/addresses.csv"
states_file = "datasets/output/states.csv"
# Reload the taxonomy data file
file_path_taxonomy_data = './NPPES_dictionary.csv'

# Constants
CMS_TAXONOMY_CODE = "251G00000X"  # Specific taxonomy code for comparison
TAXONOMY_KEYWORD = "Taxonomy Code"    # Keyword to find taxonomy fields

def load_datasets(nppes_file, cms_file):
    """Load the NPPES and CMS datasets into pandas DataFrames."""
    nppes_data = pd.read_csv(nppes_file, dtype={"NPI": str})
    cms_data = pd.read_csv(cms_file)
    return nppes_data, cms_data

def find_taxonomy_fields(columns):
    """Identify fields in the dataset that contain the word 'taxonomy'."""
    return [col for col in columns if TAXONOMY_KEYWORD.lower() in col.lower()]

def compare_and_update(row, cms_row):
    """Compare CMS file record name with alternatives and update CMS data."""
    alternative_fields = ["Provider Organization Name (Legal Business Name)", "Parent Organization LBN", "Provider Other Organization Name"]  # Replace with actual column names
    for alt_field in alternative_fields:
        alt_field_value = str(row[alt_field]).strip().lower() if pd.notna(row[alt_field]) else ""
        cms_name_value = str(cms_row["name"]).strip().lower() if pd.notna(cms_row["name"]) else ""
        if alt_field_value == cms_name_value:
            # Update CMS data with NPPES fields but keep CMS address
            cms_row["npi"] = row["NPI"]
            return cms_row, True
    return row, False

# Required columns and their default values
required_columns = {
    "entity_id": None,  # Unique identifier for the entity
    "name": None,  # name of the entity
    "ccn": None,  # CMS Certification Number
    "npi": None,  # National Provider Identifier
    "Type": None,  # General category (e.g., Hospital, Clinic)
    "Subtype": None,  # Specific classification (e.g., Rehabilitation Unit)
    "nucc_code": None,  # Mapping code for type/subtype
    "unique_facility_at_location": 0,  # Flag for single facility at location
    "employer_group_type": "none",  # Group type: none, single, multi
    "entity_unique_to_address": 1,  # True by default
    "multi_speciality_facility": 0,  # False by default
    "multi_speciality_employer": 0,  # False by default
    "employer_num": None  # Employer number
}


# Column mapping for alternate names
column_mapping = {
    "name": ["Provider Organization Name (Legal Business Name)", "Parent Organization LBN", "Provider Other Organization Name"],
    "npi": ["NPI"],
    #"Address": ["Provider First Line Business Practice Location Address", "Provider Second Line Business Practice Location Address"],
    #"City": ["Provider Business Practice Location Address City Name"],  # No alternatives
    #"State": ["Provider Business Practice Location Address State Name"],  # No alternatives
    #"ZipCode": ["Provider Business Practice Location Address Postal Code"]  # No alternatives
}
# Column mapping for alternate names to address
column_mapping_address = {
    "Address": ["Provider First Line Business Practice Location Address"],
    "City": ["Provider Business Practice Location Address City Name"],  # No alternatives
    "State": ["Provider Business Practice Location Address State Name"],  # No alternatives
    "ZipCode": ["Provider Business Practice Location Address Postal Code"]  # No alternatives
}

def map_row_to_entity(row, taxonomy_field, taxonomy_mapping):
    """
    Assigns the nucc_code, type, and subtype based on a given taxonomy field.
    
    Parameters:
    - row: A pandas Series representing a row in the dataset.
    - taxonomy_field: The name of the column in the dataset that corresponds to the taxonomy code.
    - taxonomy_mapping: the taxonomy data dictionary of NPPES
    
    Returns:
    - A dictionary with nucc_code, type, and subtype values.
    """
    entity = {col: default_value for col, default_value in required_columns.items()}

    # Assign values from column mapping
    for target_col, alt_names in column_mapping.items():
        for alt_name in alt_names:
            if alt_name in row and pd.notna(row[alt_name]):
                entity[target_col] = row[alt_name]
                break
    
    taxonomy_code = str(row.get(taxonomy_field, "")).strip()
    if taxonomy_code in taxonomy_mapping:
        details = taxonomy_mapping[taxonomy_code]
        entity["nucc_code"] = taxonomy_code
        entity["Type"] = details["type"]
        entity["Subtype"] = details["subtype"]
        print(entity)
    else:
        print(taxonomy_code)
        entity["nucc_code"] = taxonomy_code
        entity["Type"] = "Clinical Location"
        entity["Subtype"] = None
        print(entity)
    # Add derived fields or logic as needed
    entity["entity_id"] = generate_numeric_key(row["NPI"], taxonomy_field)
    # Extract addresses and update state_mapping
    #address = extract_addresses(row, "NPI")
    return entity#, address

# Function to generate a numeric primary key
def generate_numeric_key(npi, taxonomy_field):
    """
    Generate a numeric primary key using NPI and an additional taxonomy field.

    Parameters:
    - npi: National Provider Identifier (NPI)
    - taxonomy_field: Additional field (e.g., taxonomy code) to ensure uniqueness.

    Returns:
    - A numeric primary key (up to 9 digits).
    """
    combined_key = f"{npi}|{taxonomy_field}"  # Combine NPI and taxonomy field
    return int(hashlib.md5(combined_key.encode()).hexdigest(), 16) % (10**9)  # Limits to 9 digits

def validate_and_remove_second_duplicate_within_row(row, taxonomy_fields):
    """
    Validate and remove only the second occurrence of duplicate values within a single row for specified fields.
    The first occurrence is kept, and the second occurrence is set to None.
    
    Parameters:
        row (pd.Series): The row to validate.
        taxonomy_fields (list): List of column names to check for duplicates.
    
    Returns:
        pd.Series: The modified row with only the second duplicates removed.
    """
    seen_values = set()
    for field in taxonomy_fields:
        if field in row.index and pd.notna(row[field]):  # Check if the field exists and is not NaN
            if row[field] in seen_values:
                row[field] = None  # Remove only the second occurrence
            else:
                seen_values.add(row[field])  # Track the first occurrence
    return row

def process_nppes(nppes_data, cms_data, ):
    """Process the NPPES dataset based on the flow."""
    taxonomy_fields = find_taxonomy_fields(nppes_data.columns)
    taxonomy_data = pd.read_csv(file_path_taxonomy_data)
    # Create the taxonomy_mapping dictionary
    taxonomy_mapping = taxonomy_data.set_index("NUCC Code").T.to_dict()
    taxonomy_mapping = {key: {"type": value["Fashia - Facility Type"], "subtype": value["Fashia - Facility Subtype"]}
                        for key, value in taxonomy_mapping.items()}
    print(taxonomy_mapping)
    updated_cms_records = []
    new_entities = []
    new_address = []
    
    # Duplicate records are removed in fields by taxonomy
    nppes_data = nppes_data.apply(lambda row: validate_and_remove_second_duplicate_within_row(row, taxonomy_fields), axis=1)

    for _, nppes_row in nppes_data.iterrows():
        new_entity_address = False
        # Extract addresses and update state_mapping
        address = extract_addresses(nppes_row, "NPI")

        for taxonomy_field in taxonomy_fields:
            if pd.notna(nppes_row[taxonomy_field]):  # Ensure field is not NaN
                
                #new_address.append(extract_addresses(nppes_row, "NPI"))
                taxonomy_code = nppes_row[taxonomy_field]
                cms_match = cms_data[cms_data["nucc_code"] == taxonomy_code]

                if not cms_match.empty:
                    if taxonomy_code == CMS_TAXONOMY_CODE:
                        updated_entity = False
                        for _, cms_row in cms_match.iterrows():
                            updated_row, updated = compare_and_update(nppes_row, cms_row)
                            if updated:
                                updated_cms_records.append(updated_row)
                                updated_entity = True
                        if not updated_entity:
                            entity = map_row_to_entity(nppes_row, taxonomy_field, taxonomy_mapping)
                            new_entities.append(entity)
                            new_entity_address = True
                    else:
                        continue
                else:
                    entity = map_row_to_entity(nppes_row, taxonomy_field, taxonomy_mapping)
                    new_entities.append(entity)
                    new_entity_address = True
        if new_entity_address:
            new_address.append(address)

    return updated_cms_records, new_entities, new_address

def save_to_cms_file(updated_cms_records, new_entities, extract_addresses):
    print("\n\n\nUpdated CMS Records Preview:")
    print(pd.DataFrame(updated_cms_records).head())

    print("\n\n\nNew Entities Preview:")
    print(pd.DataFrame(new_entities).head())
    
    updated_cms_file = "./datasets/output/updated_cms_records.csv"
    if updated_cms_records:
        pd.DataFrame(updated_cms_records).to_csv(updated_cms_file, index=False)
        print(f"Updated CMS records saved to {updated_cms_file}.")
    else:
        print("No updated CMS records to save.")

    # Save new entities to a separate file
    new_entities_file = "./datasets/output/new_entities.csv"
    if new_entities:
        pd.DataFrame(new_entities).to_csv(new_entities_file, index=False)
        print(f"New entities saved to {new_entities_file}.")
    else:
        print("No new entities to save.")
    
    # Load the existing CMS entities file
    if os.path.exists(cms_file):
        cms_entities = pd.read_csv(cms_file,
            dtype={
                "entity_id": str,  # Forzar NPI como cadena
                "Type": str,  # Forzar Taxonomy Codes como cadenas
                "Subtype": str,  # Repetir para todos los campos relevantes
            },
            low_memory=False  # Desactiva la carga optimizada para evitar fragmentación de tipos
        )
    else:
        cms_entities = pd.DataFrame(columns=required_columns.keys())  # Initialize with required columns

    # Convert updated CMS records and new entities to DataFrames
    if updated_cms_records:
        updated_cms_df = pd.DataFrame(updated_cms_records)
        updated_cms_df = updated_cms_df.drop_duplicates(subset="entity_id")  # Ensure no duplicates
    else:
        updated_cms_df = pd.DataFrame(columns=cms_entities.columns)

    if new_entities:
        new_entities_df = pd.DataFrame(new_entities)
        new_entities_df = new_entities_df.drop_duplicates(subset="entity_id")  # Ensure no duplicates
    else:
        new_entities_df = pd.DataFrame(columns=cms_entities.columns)
        
    # Ensure the columns of new_entities_df match exactly with cms_entities
    missing_columns = set(cms_entities.columns) - set(new_entities_df.columns)
    for col in missing_columns:
        new_entities_df[col] = None  # Add missing columns with default value

    # Reorder columns in new_entities_df to match cms_entities
    new_entities_df = new_entities_df[cms_entities.columns]

    # Ensure unique indices for CMS entities
    if "entity_id" in cms_entities.columns:
        cms_entities = cms_entities.drop_duplicates(subset="entity_id")
        cms_entities = cms_entities.reset_index(drop=True)
        updated_cms_df = updated_cms_df.reset_index(drop=True)

    # Update the existing entities with updated CMS records
    if not updated_cms_df.empty:
        # Merge updated records into CMS entities
        cms_entities = pd.concat([cms_entities, updated_cms_df]).drop_duplicates(subset="entity_id", keep="last")

    # Ensure new_entities_df retains all columns and values
    if not new_entities_df.empty:
        missing_columns = set(cms_entities.columns) - set(new_entities_df.columns)
        for col in missing_columns:
            new_entities_df[col] = None
        new_entities_df = new_entities_df[cms_entities.columns]

        # Avoid duplicates and append new entities
        new_entities_df = new_entities_df.drop_duplicates(subset="entity_id")
        cms_entities = pd.concat([cms_entities, new_entities_df], ignore_index=True)

    # Save the updated entities back to the file
    cms_entities.to_csv(cms_file, index=False)
    
    # Save the new addresses to the addresses file
    if extract_addresses:
        if os.path.exists(addresses_file):
            pd.DataFrame(extract_addresses).to_csv(addresses_file, mode='a', index=False, header=False)
        else:
            pd.DataFrame(extract_addresses).to_csv(addresses_file, index=False)

    print(f"CMS file updated with {len(updated_cms_records)} updated records and {len(new_entities)} new entities.")
    print("Addresses saved successfully.")


# Initialize a global states mapping to assign unique StateIDs
state_mapping = {}
def initialize_state_mapping(states_file):
    """Initialize the state_mapping with the existing states CSV file."""
    if os.path.exists(states_file):
        states_df = pd.read_csv(states_file)
        for _, row in states_df.iterrows():
            state_code = row["state_code"]
            state_mapping[state_code] = {
                "state_id": row["state_id"],
                "state_code": state_code,
                "state_name": row["state_name"]
            }
        print(f"State mapping initialized with {len(state_mapping)} states from {states_file}.")
    else:
        print(f"States file {states_file} not found. State mapping will start empty.")

# Function to generate a unique address ID (hash)
def generate_address_id(npi, address, city, state, zip_code):
    """Generate a unique ID for an address based on CNN and address hash."""
    # Use only the first 5 digits of the ZIP code
    zip_trimmed = str(zip_code)[:5] if pd.notna(zip_code) else ""
    address_str = f"{address}|{city}|{state}|{zip_trimmed}"
    address_hash = hashlib.md5(address_str.encode()).hexdigest()
    return int(hashlib.md5(f"{npi}{address_hash}".encode()).hexdigest(), 16) % (10**9)  # 9-digit limit

# Function to get or assign StateID
def get_or_create_state_id(state_code):
    """Retrieve an existing StateID or create a new one for a state code."""
    if state_code not in state_mapping:
        state_id = len(state_mapping) + 1
        state_mapping[state_code] = {"state_id": state_id, "state_code": state_code, "state_name": None}
    return state_mapping[state_code]["state_id"]

# Function to extract addresses and save to CSV
def extract_addresses(row, npi_column="NPI"):
    """Extract address from the data and return address record"""
    # Dynamically map columns
    address_col = next((alt for alt in column_mapping_address["Address"] if alt in row.index), None)
    city_col = next((alt for alt in column_mapping_address["City"] if alt in row.index), None)
    state_col = next((alt for alt in column_mapping_address["State"] if alt in row.index), None)
    zip_col = next((alt for alt in column_mapping_address["ZipCode"] if alt in row.index), None)
    
    if not all([address_col, city_col, state_col, zip_col]):
        return None  # Skip if any required column is missing
    
    # Handle concatenation for Address Line 1 and Address Line 2
    if address_col == "Provider First Line Business Practice Location Address":
        address_line_1 = row["Provider First Line Business Practice Location Address"]
        address_line_2 = row.get("Provider Second Line Business Practice Location Address", "")  # Default to empty string if not present
        full_address = f"{address_line_1} {address_line_2}".strip(", ")  # Concatenate, remove trailing commas
    else:
        full_address = row[address_col]
    
    city = row[city_col]
    state = row[state_col]
    zip_code = row[zip_col]
    npi = row.get(npi_column, None)
    
    # Generate address ID
    address_id = generate_address_id(npi, full_address, city, state, zip_code)
    
    # Assign StateID using state_mapping
    state_id = get_or_create_state_id(state)
    
    # Create address hash (for tracking uniqueness)
    address_hash = int(hashlib.md5(f"{full_address}|{city}|{state}|{str(zip_code)[:5]}".encode()).hexdigest(), 16) % (10**9)
    
    # Append to records
    return {
        "address_id": address_id,
        "npi": npi,  # Placeholder, not defined in requirements
        "ccn": None,
        "address": full_address,
        "city": city,
        "state_id": state_id,
        "zip_code": str(zip_code)[:5],
        "cms_addr_id": None,  # Placeholder
        "address_hash": address_hash,
        "primary_practice_address": False
    }
    

# Load the existing states.csv file to initialize state_mapping
initialize_state_mapping(states_file)


def main():
    """Main function to orchestrate the NPPES processing."""
    print("Loading datasets...")
    nppes_data, cms_data = load_datasets(nppes_file, cms_file)
    
    print("Processing NPPES data...")
    updated_cms_records, new_entities, extract_addresses = process_nppes(nppes_data, cms_data)
    print(f"Updated CMS Records: {len(updated_cms_records)}")
    print(f"New Entities: {len(new_entities)}")
    save_to_cms_file(updated_cms_records, new_entities, extract_addresses)
    print("Processing complete.")

if __name__ == "__main__":
    main()
