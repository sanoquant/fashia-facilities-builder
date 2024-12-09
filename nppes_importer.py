import os
import pandas as pd
import hashlib

# File paths
nppes_file = "./datasets/filtered/nppes_filtered_data.csv"  # Input NPPES dataset
cms_file = "./datasets/output/entities.csv"      # Input CMS dataset
output_updated_cms = "updated_cms_data.csv"  # Output for updated CMS records
output_new_entities = "new_entities.csv"    # Output for new entities
# Output files for the Addresses and States tables
addresses_file = "datasets/output/addresses.csv"
states_file = "datasets/output/states.csv"
# Constants
CMS_TAXONOMY_CODE = "251G00000X"  # Specific taxonomy code for comparison
TAXONOMY_KEYWORD = "Taxonomy Code"    # Keyword to find taxonomy fields

def load_datasets(nppes_file, cms_file):
    """Load the NPPES and CMS datasets into pandas DataFrames."""
    nppes_data = pd.read_csv(nppes_file)
    cms_data = pd.read_csv(cms_file)
    return nppes_data, cms_data

def find_taxonomy_fields(columns):
    """Identify fields in the dataset that contain the word 'taxonomy'."""
    return [col for col in columns if TAXONOMY_KEYWORD.lower() in col.lower()]

def compare_and_update(row, cms_row):
    """Compare CMS file record name with alternatives and update CMS data."""
    alternative_fields = ["Provider Organization Name (Legal Business Name)", "Parent Organization LBN", "Provider Other Organization Name"]  # Replace with actual column names
    for alt_field in alternative_fields:
        if row[alt_field].strip().lower() == cms_row["name"].strip().lower():
            # Update CMS data with NPPES fields but keep CMS address
            cms_row["npi"] = row["NPI"]
            cms_row["Type"] = row["Entity Type Code"]
            cms_row["address"] = cms_row["address"]  # Retain CMS address
            return cms_row, True
    return row, False

# Required columns and their default values
required_columns = {
    "entity_id": None,  # Unique identifier for the entity
    "name": None,  # Name of the entity
    "ccn": None,  # CMS Certification Number
    "npi": None,  # National Provider Identifier
    "type": None,  # General category (e.g., Hospital, Clinic)
    "subtype": None,  # Specific classification (e.g., Rehabilitation Unit)
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
    "type": ["Entity Type Code"],
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

def map_row_to_entity(row, taxonomy_field):
    """
    Map a single row to the required entity format.
    """
    entity = {col: default_value for col, default_value in required_columns.items()}

    # Map each required column to its corresponding value in the row
    for target_col, alt_names in column_mapping.items():
        for alt_name in alt_names:
            if alt_name in row and pd.notna(row[alt_name]):
                entity[target_col] = row[alt_name]
                break
            elif taxonomy_field in row and pd.notna(row[taxonomy_field]):
                entity["nucc_code"] = row[taxonomy_field]
                # Add derived fields or logic as needed
                entity["entity_id"] = generate_numeric_key(row["NPI"])
    # Extract addresses and update state_mapping
    address = extract_addresses(row, "NPI")

    return entity, address
# Function to generate a numeric primary key
def generate_numeric_key(npi):
    if not str(npi).isnumeric():
        # Converts CCN into a numeric hash
        return int(hashlib.md5(str(npi).encode()).hexdigest(), 16) % (10**9)  # Limits to 9 digits
    return int(npi)

def process_nppes(nppes_data, cms_data):
    """Process the NPPES dataset based on the flow."""
    taxonomy_fields = find_taxonomy_fields(nppes_data.columns)
    updated_cms_records = []
    new_entities = []
    new_address = []

    for _, nppes_row in nppes_data.iterrows():
        taxonomy_found = False

        for taxonomy_field in taxonomy_fields:
            if pd.notna(nppes_row[taxonomy_field]):  # Ensure field is not NaN
                taxonomy_code = nppes_row[taxonomy_field]
                cms_match = cms_data[cms_data["nucc_code"] == taxonomy_code]

                if not cms_match.empty:
                    taxonomy_found = True
                    if taxonomy_code == CMS_TAXONOMY_CODE:
                        for _, cms_row in cms_match.iterrows():
                            updated_row, updated = compare_and_update(nppes_row, cms_row)
                            if updated:
                                updated_cms_records.append(updated_row)
                            else:
                                entity, address = map_row_to_entity(nppes_row, taxonomy_field)
                                new_entities.append(entity.to_dict())
                                new_address.append(address.to_dict())
                    else:
                        entity, address = map_row_to_entity(nppes_row, taxonomy_field)
                        new_entities.append(entity.to_dict())
                        new_address.append(address.to_dict())
                else:
                    entity, address = map_row_to_entity(nppes_row, taxonomy_field)
                    new_entities.append(entity.to_dict())
                    new_address.append(address.to_dict())
                break

    return updated_cms_records, new_entities, new_address

def save_to_csv(updated_cms_records, new_entities, extract_addresses):
    """Save updated CMS records and new entities to CSV files."""
    pd.DataFrame(updated_cms_records).to_csv(output_updated_cms, index=False)
    pd.DataFrame(new_entities).to_csv(output_new_entities, index=False)
    pd.DataFrame(extract_addresses).to_csv(addresses_file, mode='a', index=False, header=False)
    print("Files saved successfully.")

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
    address_col = next((alt for alt in column_mapping_address["Address"] if alt in row.columns), None)
    city_col = next((alt for alt in column_mapping_address["City"] if alt in row.columns), None)
    state_col = next((alt for alt in column_mapping_address["State"] if alt in row.columns), None)
    zip_col = next((alt for alt in column_mapping_address["ZipCode"] if alt in row.columns), None)
    
    if not all([address_col, city_col, state_col, zip_col]):
        return {}  # Skip rows with missing required columns
    
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
    save_to_csv(updated_cms_records, new_entities, extract_addresses)
    print("Processing complete.")

if __name__ == "__main__":
    main()
