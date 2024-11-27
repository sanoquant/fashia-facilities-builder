import os
import pandas as pd
import hashlib
print("Environment setup complete!")


# List of files
files = ["DFC_FACILITY.csv", "HH_Provider_Oct2024.csv", "Hospice_General-Information_Aug2024.csv", "Hospital_General_Information.csv", "Inpatient_Rehabilitation_Facility-General_Information_Sep2024.csv", "Long-Term_Care_Hospital-General_Information_Sep2024.csv", "NH_ProviderInfo_Oct2024.csv"]

# Dictionary of rules based on the file name
file_rules_mapping = {
    "DFC_FACILITY.csv": {"Type": "Clinic", "Subtype": "Dialysis Clinic", "NUCC Code": "261QE0700X"},
    "NH_ProviderInfo_Oct2024.csv": {"Type": "Nursing & Assisted Living", "Subtype": "Skilled Nursing Facility", "NUCC Code": "314000000X"},
    "Hospice_General-Information_Aug2024.csv": {"Type": "Agency", "Subtype": "Community Based Hospice Care Agency", "NUCC Code": "251G00000X"},
    "Inpatient_Rehabilitation_Facility-General_Information_Sep2024.csv": {
        "SubRules": {
            "true": {"Type": "Hospital", "Subtype": "Rehabilitation Hospital", "NUCC Code": "283X00000X"},
            "false": {"Type": "Hospital Unit", "Subtype": "Rehabilitation Hospital Unit", "NUCC Code": "273Y00000X"},
            },
        "typeSubRules": "ifCnnIsNumber"
        },
    "Long-Term_Care_Hospital-General_Information_Sep2024.csv": {"Type": "Hospital", "Subtype": "Long Term Care Hospital", "NUCC Code": "282E00000X"},
    "HH_Provider_Oct2024.csv": {
        "Type": "Agency",
        "Subtype": "Home Health Agency (All)",
        "NUCC Code": "251E00000X",
        "SubRules": { 
            "Offers Nursing Care Services": {"Type": "Agency", "Subtype": "Home Health Agency (Nursing Care Services)", "NUCC Code": "N/A"},
            "Offers Physical Therapy Services": {"Type": "Agency", "Subtype": "Home Health Agency (Physical Therapy)", "NUCC Code": "N/A"},
            "Offers Occupational Therapy Services": {"Type": "Agency", "Subtype": "Home Health Agency (Occupational Therapy)", "NUCC Code": "N/A"},
            "Offers Speech Pathology Services": {"Type": "Agency", "Subtype": "Home Health Agency (Speech Pathology)", "NUCC Code": "N/A"},
            "Offers Medical Social Services": {"Type": "Agency", "Subtype": "Home Health Agency (Medical Social Services)", "NUCC Code": "N/A"},
            "Offers Home Health Aide Services": {"Type": "Agency", "Subtype": "Home Health Agency (Home Health Aide Services)", "NUCC Code": "N/A"}
        },
        "typeSubRules": "duplicateByActiveFlag"
    },
    "Hospital_General_Information.csv": 
        {
            "SubRules": { 
                "Acute Care - Veterans Administration": {"Type": "Hospital", "Subtype": "Acute care -Veterans Administration", "NUCC Code": "N/A"},
                "Acute Care Hospitals": {"Type": "Hospital", "Subtype": "Acute Care", "NUCC Code": "282N00000X"},
                "Childrens": {"Type": "Hospital", "Subtype": "Pediatric", "NUCC Code": "282NC2000X"},
                "Critical Access Hospitals": {"Type": "Hospital", "Subtype": "Critical Access", "NUCC Code": "282NC0060X"},
                "Acute Care- department of Defense": [
                    {"Type": "Hospital", "Subtype": "Military Hospital", "NUCC Code": "286500000X"},
                    {"Type": "Hospital", "Subtype": "Military General acute care hospital", "NUCC Code": "2865M2000X"}
                ],
                "Psychiatric": {"Type": "Hospital", "Subtype": "Psychiatric", "NUCC Code": "283Q00000X"},
                "Default": {"Type": None, "Subtype": None, "NUCC Code": None}
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
    if "Type" in rules and "Subtype" in rules and "NUCC Code" in rules:
        data["Type"] = rules["Type"]
        data["Subtype"] = rules["Subtype"]
        data["NUCC Code"] = rules["NUCC Code"]
        entities.extend(data.to_dict(orient="records"))

    # Process subrules
    if "SubRules" in rules:
        if rules.get("typeSubRules") == "ifCnnIsNumber":
            # Subrules based on whether CCN is numeric
            for condition, subrule in rules["SubRules"].items():
                filtered_data = data[data["CMS Certification Number (CCN)"].str.isnumeric() if condition == "true" else ~data["CMS Certification Number (CCN)"].str.isnumeric()]
                filtered_data["Type"] = subrule["Type"]
                filtered_data["Subtype"] = subrule["Subtype"]
                filtered_data["NUCC Code"] = subrule["NUCC Code"]
                entities.extend(filtered_data.to_dict(orient="records"))

        elif rules.get("typeSubRules") == "duplicateByActiveFlag":
            # Subrules to duplicate entities based on flags
            for column, subrule in rules["SubRules"].items():
                filtered_data = data[data[column] == "Yes"]
                filtered_data["Type"] = subrule["Type"]
                filtered_data["Subtype"] = subrule["Subtype"]
                filtered_data["NUCC Code"] = subrule["NUCC Code"]
                entities.extend(filtered_data.to_dict(orient="records"))

        elif rules.get("typeSubRules") == "checkByFieldValue":
            # Subrules based on field values
            for field_value, subrule in rules["SubRules"].items():
                if isinstance(subrule, list):  # Handle lists of subrules
                    # Filter rows that match the field value
                    filtered_data = data[data["Hospital Type"] == field_value]
                    
                    # Create an entity for each rule in the list
                    for rule in subrule:
                        # Copy the filtered DataFrame to avoid reference conflicts
                        entity_data = filtered_data.copy()
                        
                        # Assign specific values from the current subrule
                        entity_data["Type"] = rule["Type"]
                        entity_data["Subtype"] = rule["Subtype"]
                        entity_data["NUCC Code"] = rule["NUCC Code"]
                        
                        # Add processed entities to the list
                        entities.extend(entity_data.to_dict(orient="records"))
                else:
                    # Process a single rule
                    filtered_data = data[data["Hospital Type"] == field_value]
                    filtered_data["Type"] = subrule["Type"]
                    filtered_data["Subtype"] = subrule["Subtype"]
                    filtered_data["NUCC Code"] = subrule["NUCC Code"]
                    entities.extend(filtered_data.to_dict(orient="records"))

    return pd.DataFrame(entities)


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
            
            
            if "Hospital_General_Information.csv" != file:
                # Generate the numeric primary key
                filtered_data["PrimaryKey"] = filtered_data["CMS Certification Number (CCN)"].apply(generate_numeric_key)
            
            # Process the CMS file data based on the rules dictionary for the file
            processed_data = process_file(file, filtered_data)
            
            # Save the filtered data in the specific folder
            file_name = os.path.basename(file).replace(".csv", "_filtered.csv")
            output_path = os.path.join(filtered_folder, file_name)
            filtered_data.to_csv(output_path, index=False)
            print(f"Filtered data saved to: {output_path}\n\n\n")
        
            
        else:
            print("Required columns (or alternatives) not found. Skipping file.\n\n\n")
    except Exception as e:
        print(f"Error loading {file}: {e}\n\n\n")