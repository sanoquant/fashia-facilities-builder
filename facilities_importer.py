import os
import pandas as pd
import hashlib
print("Environment setup complete!")


# List of files
files = [
    "DFC_FACILITY.csv",
         "HH_Provider_Oct2024.csv",
         "Hospice_General-Information_Aug2024.csv",
         "Hospital_General_Information.csv",
         "Inpatient_Rehabilitation_Facility-General_Information_Sep2024.csv",
         "Long-Term_Care_Hospital-General_Information_Sep2024.csv", "NH_ProviderInfo_Oct2024.csv"]


# Create the folder for filtered files if it doesn't exist
filtered_folder = "datasets/filtered"
os.makedirs(filtered_folder, exist_ok=True)

# Output folder and file
output_folder = "datasets/output"
os.makedirs(output_folder, exist_ok=True)
output_file = os.path.join(output_folder, "entities.csv")

# Output files for the Addresses and States tables
addresses_file = "datasets/output/addresses.csv"
states_file = "datasets/output/states.csv"

# Dictionary of rules based on the file name
file_rules_mapping = {
    "DFC_FACILITY.csv": {"Type": "Clinic", "Subtype": "Dialysis Clinic", "nucc_code": "261QE0700X"},
    "NH_ProviderInfo_Oct2024.csv": {"Type": "Nursing & Assisted Living", "Subtype": "Skilled Nursing Facility", "nucc_code": "314000000X"},
    "Hospice_General-Information_Aug2024.csv": {"Type": "Agency", "Subtype": "Community Based Hospice Care Agency", "nucc_code": "251G00000X"},
    "Inpatient_Rehabilitation_Facility-General_Information_Sep2024.csv": {
        "SubRules": {
            "true": {"Type": "Hospital", "Subtype": "Rehabilitation Hospital", "nucc_code": "283X00000X"},
            "false": {"Type": "Hospital Unit", "Subtype": "Rehabilitation Hospital Unit", "nucc_code": "273Y00000X"},
            },
        "typeSubRules": "ifCnnIsNumber"
        },
    "Long-Term_Care_Hospital-General_Information_Sep2024.csv": {"Type": "Hospital", "Subtype": "Long Term Care Hospital", "nucc_code": "282E00000X"},
    "HH_Provider_Oct2024.csv": {
        "Type": "Agency",
        "Subtype": "Home Health Agency (All)",
        "nucc_code": "251E00000X",
        "SubRules": { 
            "Offers Nursing Care Services": {"Type": "Agency", "Subtype": "Home Health Agency (Nursing Care Services)", "nucc_code": "N/A"},
            "Offers Physical Therapy Services": {"Type": "Agency", "Subtype": "Home Health Agency (Physical Therapy)", "nucc_code": "N/A"},
            "Offers Occupational Therapy Services": {"Type": "Agency", "Subtype": "Home Health Agency (Occupational Therapy)", "nucc_code": "N/A"},
            "Offers Speech Pathology Services": {"Type": "Agency", "Subtype": "Home Health Agency (Speech Pathology)", "nucc_code": "N/A"},
            "Offers Medical Social Services": {"Type": "Agency", "Subtype": "Home Health Agency (Medical Social Services)", "nucc_code": "N/A"},
            "Offers Home Health Aide Services": {"Type": "Agency", "Subtype": "Home Health Agency (Home Health Aide Services)", "nucc_code": "N/A"}
        },
        "typeSubRules": "duplicateByActiveFlag"
    },
    "Hospital_General_Information.csv": 
        {
            "SubRules": { 
                "Acute Care - Veterans Administration": {"Type": "Hospital", "Subtype": "Acute care -Veterans Administration", "nucc_code": "N/A"},
                "Acute Care Hospitals": {"Type": "Hospital", "Subtype": "Acute Care", "nucc_code": "282N00000X"},
                "Childrens": {"Type": "Hospital", "Subtype": "Pediatric", "nucc_code": "282NC2000X"},
                "Critical Access Hospitals": {"Type": "Hospital", "Subtype": "Critical Access", "nucc_code": "282NC0060X"},
                "Acute Care - Department of Defense": [
                    {"Type": "Hospital", "Subtype": "Military Hospital", "nucc_code": "286500000X"},
                    {"Type": "Hospital", "Subtype": "Military General acute care hospital", "nucc_code": "2865M2000X"}
                ],
                "Psychiatric": {"Type": "Hospital", "Subtype": "Psychiatric", "nucc_code": "283Q00000X"},
                "Default": {"Type": None, "Subtype": None, "nucc_code": None}
            },
            "typeSubRules": "checkByFieldValue"
        }
}
# List of required columns and their alternatives
column_mapping = {
    "Address": ["Address Line 1", "Address", "Provider Address"],
    "City": ["City/Town"],  # No alternatives
    "State": ["State"],  # No alternatives
    "ZipCode": ["ZIP Code"]  # No alternatives
}

# Create the folder for filtered files if it doesn't exist
filtered_folder = "datasets/filtered"
os.makedirs(filtered_folder, exist_ok=True)

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

def map_columns(data):
    
    # Maps dataset columns to the required columns in the entities structure.
    # Adjusts entity_id, name, and CCN directly.
    
    data["entity_id"] = data["PrimaryKey"]  # Assign entity_id from PrimaryKey
    if "Facility Name" in data.columns:
        data["name"] = data["Facility Name"]  # Assign name from Facility Name
    else:
        data["name"] = data["Provider Name"]
    if "Facility ID" in data.columns:
        data["ccn"] = data["Facility ID"]  # Assign CCN from Facility ID
    else:
        data["ccn"] = data["CMS Certification Number (CCN)"]  # Assign CCN from CMS Certification Number (CCN)
    return data

def ensure_columns(entities):
    # Ensures required columns are present and adds missing ones with default values.
    for column, default_value in required_columns.items():
        if column not in entities.columns:
            entities[column] = default_value
    return entities[required_columns.keys()]

def save_entities_to_csv(entities, output_file):
    # Saves entities to a CSV file for later database import.
    try:
        # Convert entities to a DataFrame and ensure required columns are present
        entities_df = pd.DataFrame(entities)
        entities_df = ensure_columns(entities_df)

        # Check if the output file already exists
        if os.path.exists(output_file):
            # Append to the existing file
            entities_df.to_csv(output_file, mode='a', index=False, header=False)
        else:
            # Create a new file with a header
            entities_df.to_csv(output_file, index=False)

        print(f"Entities saved to {output_file}")
    except Exception as e:
        print(f"Error saving entities to CSV: {e}")

# Function to generate a numeric primary key
def generate_numeric_key(ccn):
    if not str(ccn).isnumeric():
        # Converts CCN into a numeric hash
        return int(hashlib.md5(str(ccn).encode()).hexdigest(), 16) % (10**9)  # Limits to 9 digits
    return int(ccn)

# Main function to process a file based on the rules dictionary
def process_file(file_name, data):
    rules = file_rules_mapping.get(file_name)
    if not rules:
        print(f"No rules found for file: {file_name}")
        return pd.DataFrame()

    entities = []

    # Process general rules
    if "Type" in rules and "Subtype" in rules and "nucc_code" in rules:
        data["Type"] = rules["Type"]
        data["Subtype"] = rules["Subtype"]
        data["nucc_code"] = rules["nucc_code"]
        entities.extend(data.to_dict(orient="records"))

    # Process subrules
    if "SubRules" in rules:
        if rules.get("typeSubRules") == "ifCnnIsNumber":
            for condition, subrule in rules["SubRules"].items():
                filtered_data = data[
                    data["CMS Certification Number (CCN)"].str.isnumeric() if condition == "true" else
                    ~data["CMS Certification Number (CCN)"].str.isnumeric()
                ].copy()
                print(f"Filtered {len(filtered_data)} rows for condition '{condition}' in 'ifCnnIsNumber'.")
                
                if not filtered_data.empty:
                    filtered_data.loc[:, "Type"] = subrule["Type"]
                    filtered_data.loc[:, "Subtype"] = subrule["Subtype"]
                    filtered_data.loc[:, "nucc_code"] = subrule["nucc_code"]
                    entities.extend(filtered_data.to_dict(orient="records"))

        elif rules.get("typeSubRules") == "duplicateByActiveFlag":
            for column, subrule in rules["SubRules"].items():
                if column in data.columns:
                    filtered_data = data[data[column] == "Yes"].copy()
                    print(f"Filtered {len(filtered_data)} rows for column '{column}' in 'duplicateByActiveFlag'.")
                    
                    if not filtered_data.empty:
                        filtered_data.loc[:, "Type"] = subrule["Type"]
                        filtered_data.loc[:, "Subtype"] = subrule["Subtype"]
                        filtered_data.loc[:, "nucc_code"] = subrule["nucc_code"]
                        entities.extend(filtered_data.to_dict(orient="records"))
                else:
                    print(f"Column '{column}' not found in {file_name}. Skipping subrule.")

        elif rules.get("typeSubRules") == "checkByFieldValue":
            if "Hospital Type" in data.columns:
                for field_value, subrule in rules["SubRules"].items():
                    if isinstance(subrule, list):  # Handle lists of subrules
                        filtered_data = data[data["Hospital Type"] == field_value].copy()
                        print(f"Filtered {len(filtered_data)} rows for field value '{field_value}' in 'checkByFieldValue'.")

                        if not filtered_data.empty:
                            for rule in subrule:
                                entity_data = filtered_data.copy()
                                entity_data.loc[:, "Type"] = rule["Type"]
                                entity_data.loc[:, "Subtype"] = rule["Subtype"]
                                entity_data.loc[:, "nucc_code"] = rule["nucc_code"]
                                entities.extend(entity_data.to_dict(orient="records"))
                    else:
                        filtered_data = data[data["Hospital Type"] == field_value].copy()
                        print(f"Filtered {len(filtered_data)} rows for field value '{field_value}' in 'checkByFieldValue'.")

                        if not filtered_data.empty:
                            filtered_data.loc[:, "Type"] = subrule["Type"]
                            filtered_data.loc[:, "Subtype"] = subrule["Subtype"]
                            filtered_data.loc[:, "nucc_code"] = subrule["nucc_code"]
                            entities.extend(filtered_data.to_dict(orient="records"))
            else:
                print(f"'Hospital Type' column not found in {file_name}. Skipping checkByFieldValue subrules.")

    print(f"Generated {len(entities)} entities for file: {file_name}")
    return pd.DataFrame(entities)


# Initialize a global states mapping to assign unique StateIDs
state_mapping = {}
def initialize_state_mapping(states_file):
    """Initialize the state_mapping with the existing states CSV file."""
    if os.path.exists(states_file):
        states_df = pd.read_csv(states_file)
        for _, row in states_df.iterrows():
            state_code = row["StateCode"]
            state_mapping[state_code] = {
                "state_id": row["state_id"],
                "state_code": state_code,
                "state_name": row["state_name"]
            }
        print(f"State mapping initialized with {len(state_mapping)} states from {states_file}.")
    else:
        print(f"States file {states_file} not found. State mapping will start empty.")


# Function to generate a unique address ID (hash)
def generate_address_id(ccn, address, city, state, zip_code):
    """Generate a unique ID for an address based on CNN and address hash."""
    # Use only the first 5 digits of the ZIP code
    zip_trimmed = str(zip_code)[:5] if pd.notna(zip_code) else ""
    address_str = f"{address}|{city}|{state}|{zip_trimmed}"
    address_hash = hashlib.md5(address_str.encode()).hexdigest()
    return int(hashlib.md5(f"{ccn}{address_hash}".encode()).hexdigest(), 16) % (10**9)  # 9-digit limit

# Function to get or assign StateID
def get_or_create_state_id(state_code):
    """Retrieve an existing StateID or create a new one for a state code."""
    if state_code not in state_mapping:
        state_id = len(state_mapping) + 1
        state_mapping[state_code] = {"state_id": state_id, "state_code": state_code, "state_name": None}
    return state_mapping[state_code]["state_id"]

# Function to extract addresses and save to CSV
def extract_addresses(data, ccn_column="CMS Certification Number (CCN)"):
    """Extract addresses from the data and save them to a CSV."""
    address_records = []
    for _, row in data.iterrows():
        # Dynamically map columns
        address_col = next((alt for alt in column_mapping["Address"] if alt in data.columns), None)
        city_col = next((alt for alt in column_mapping["City"] if alt in data.columns), None)
        state_col = next((alt for alt in column_mapping["State"] if alt in data.columns), None)
        zip_col = next((alt for alt in column_mapping["ZipCode"] if alt in data.columns), None)
        
        if not all([address_col, city_col, state_col, zip_col]):
            continue  # Skip rows with missing required columns
        
        # Handle concatenation for Address Line 1 and Address Line 2
        if address_col == "Address Line 1":
            address_line_1 = row["Address Line 1"]
            address_line_2 = row.get("Address Line 2", "")  # Default to empty string if not present
            full_address = f"{address_line_1} {address_line_2}".strip(", ")  # Concatenate, remove trailing commas
        else:
            full_address = row[address_col]
        
        city = row[city_col]
        state = row[state_col]
        zip_code = row[zip_col]
        ccn = row.get(ccn_column, None)
        
        # Generate address ID
        address_id = generate_address_id(ccn, full_address, city, state, zip_code)
        
        # Assign StateID using state_mapping
        state_id = get_or_create_state_id(state)
        
        # Create address hash (for tracking uniqueness)
        address_hash = int(hashlib.md5(f"{full_address}|{city}|{state}|{str(zip_code)[:5]}".encode()).hexdigest(), 16) % (10**9)
        
        # Append to records
        address_records.append({
            "address_id": address_id,
            "npi": None,  # Placeholder, not defined in requirements
            "ccn": ccn,
            "address": full_address,
            "city": city,
            "state_id": state_id,
            "zip_code": str(zip_code)[:5],
            "cms_addr_id": None,  # Placeholder
            "address_hash": address_hash,
            "primary_practice_address": False
        })
    
    # Save addresses to CSV
    if os.path.exists(addresses_file):
        pd.DataFrame(address_records).to_csv(addresses_file, mode='a', index=False, header=False)
    else:
        pd.DataFrame(address_records).to_csv(addresses_file, index=False)
    print(f"Addresses saved to {addresses_file}")

# Save states to CSV
def save_states_to_csv():
    """Save the unique states to the states CSV file."""
    state_records = list(state_mapping.values())
    pd.DataFrame(state_records).to_csv(states_file, index=False)
    print(f"States saved to {states_file}")
# Load the existing states.csv file to initialize state_mapping
initialize_state_mapping(states_file)

for file in files:
    try:
        # Load the current file
        df = pd.read_csv("./datasets/"+file)
        print(f"Loaded {file} successfully with {len(df)} rows.")
        print(df.head())
        
        # Create a dynamic mapping for columns present in the file
        dynamic_columns = {}
        for main_col, alternatives in column_mapping.items():
            for alt_col in [main_col] + alternatives:
                if alt_col in df.columns:
                    dynamic_columns[main_col] = alt_col
                    break
        
        # Check if all required columns (or their alternatives) are present
        if set(column_mapping.keys()).issubset(dynamic_columns.keys()):
            print("Required columns found (or alternatives). Applying filter...")
            
            # Filter rows with missing values
            subset_columns = list(dynamic_columns.values())  # Use dynamically found columns
            filtered_data = df.dropna(subset=subset_columns, how="any")
            
            
            if "Facility ID" in filtered_data.columns:
                # Generate the numeric primary key from Facility ID
                filtered_data["PrimaryKey"] = filtered_data["Facility ID"].apply(generate_numeric_key)
            else:
                # Generate the numeric primary key from CMS Certification Number (CCN)
                filtered_data["PrimaryKey"] = filtered_data["CMS Certification Number (CCN)"].apply(generate_numeric_key)
            
            # Process the CMS file data based on the rules dictionary for the file
            processed_data = process_file(file, filtered_data)
            
            
            # Extract addresses and update state_mapping
            extract_addresses(processed_data)
            
            # map the required columns
            processed_data = map_columns(processed_data)
            # Save the generated entities to the CSV file
            save_entities_to_csv(processed_data, output_file)
            
            # Save the filtered data in the specific folder
            file_name = os.path.basename(file).replace(".csv", "_filtered.csv")
            output_path = os.path.join(filtered_folder, file_name)
            filtered_data.to_csv(output_path, index=False)
            print(f"Filtered data saved to: {output_path}\n\n\n")
        
            
        else:
            print("Required columns (or alternatives) not found. Skipping file.\n\n\n")
    except Exception as e:
        print(f"Error loading {file}: {e}\n\n\n")
        
# Save states to CSV after all files are processed
save_states_to_csv()