import pandas as pd
import pytest # type: ignore
from unittest.mock import patch
import sys
import os
import pandas as pd
from unittest.mock import patch
import hashlib

# Add the script's directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))

# Import functions from the main script
from facilities_importer import process_file, generate_numeric_key, extract_addresses, generate_address_id, get_or_create_state_id, save_states_to_csv

# Sample datasets for each file
@pytest.fixture
def sample_datasets():
    return {
        "DFC_FACILITY.csv": pd.DataFrame({
            "CMS Certification Number (CCN)": ["12345", "67890"],
            "Facility Name": ["Dialysis Center A", "Dialysis Center B"]
        }),
        "HH_Provider_Oct2024.csv": pd.DataFrame({
            "Offers Nursing Care Services": ["Yes", "No", "No", "Yes"],
            "Offers Physical Therapy Services": ["No", "Yes", "No", "Yes"],
            "Provider Name": ["Agency A", "Agency B", "Agency C", "Agency D"]
        }),
        "Hospice_General-Information_Aug2024.csv": pd.DataFrame({
            "CMS Certification Number (CCN)": ["00001", "00002"],
            "Facility Name": ["Hospice A", "Hospice B"]
        }),
        "Hospital_General_Information.csv": pd.DataFrame({
            "Facility ID": ["001", "002"],
            "Facility Name": ["Hospital A", "Hospital B"],
            "Hospital Type": ["Acute Care Hospitals", "Childrens"]
        }),
        "Inpatient_Rehabilitation_Facility-General_Information_Sep2024.csv": pd.DataFrame({
            "CMS Certification Number (CCN)": ["12345", "ABCDE"],
            "Provider Name": ["Rehab Facility A", "Rehab Facility B"]
        }),
        "Long-Term_Care_Hospital-General_Information_Sep2024.csv": pd.DataFrame({
            "CMS Certification Number (CCN)": ["67890", "54321"],
            "Provider Name": ["Long Term Care A", "Long Term Care B"]
        }),
        "NH_ProviderInfo_Oct2024.csv": pd.DataFrame({
            "CMS Certification Number (CCN)": ["11111", "22222"],
            "Provider Name": ["Nursing Home A", "Nursing Home B"]
        })
    }

# Test cases for general rules
@pytest.mark.parametrize("file_name", [
    "DFC_FACILITY.csv",
    "HH_Provider_Oct2024.csv",
    "Hospice_General-Information_Aug2024.csv",
    "NH_ProviderInfo_Oct2024.csv",
    "Long-Term_Care_Hospital-General_Information_Sep2024.csv"
])
def test_general_rules(file_name, sample_datasets):
    data = sample_datasets[file_name]
    processed_data = process_file(file_name, data)
    
    # Check the general rules application
    assert not processed_data.empty, f"Processed data for {file_name} should not be empty."
    assert "Type" in processed_data.columns, f"Type column missing for {file_name}."
    assert "Subtype" in processed_data.columns, f"Subtype column missing for {file_name}."
    assert "nucc_code" in processed_data.columns, f"nucc_code column missing for {file_name}."

# Test case for "ifCnnIsNumber" subrules
def test_if_cnn_is_number(sample_datasets):
    file_name = "Inpatient_Rehabilitation_Facility-General_Information_Sep2024.csv"
    data = sample_datasets[file_name]
    processed_data = process_file(file_name, data)

    # Verify numeric and non-numeric handling
    assert not processed_data.empty, f"Processed data for {file_name} should not be empty."
    assert processed_data[processed_data["CMS Certification Number (CCN)"].str.isnumeric()]["Type"].iloc[0] == "Hospital"
    assert processed_data[~processed_data["CMS Certification Number (CCN)"].str.isnumeric()]["Type"].iloc[0] == "Hospital Unit"

# Test case for "duplicateByActiveFlag" subrules
def test_duplicate_by_active_flag(sample_datasets):
    file_name = "HH_Provider_Oct2024.csv"
    data = sample_datasets[file_name]
    processed_data = process_file(file_name, data)

    # Verify rows are duplicated for each "Yes" condition
    assert not processed_data.empty, f"Processed data for {file_name} should not be empty."
    assert len(processed_data) > len(data), "Processed data should duplicate rows with 'Yes' conditions."

# Test case for "checkByFieldValue" subrules
def test_check_by_field_value(sample_datasets):
    file_name = "Hospital_General_Information.csv"
    data = sample_datasets[file_name]
    processed_data = process_file(file_name, data)

    # Verify specific subrule application
    assert not processed_data.empty, f"Processed data for {file_name} should not be empty."
    assert "General Acute Care Hospital" in processed_data["Subtype"].values, "'Acute Care Hospitals' rule not applied correctly."
    assert "Children's Hospital" in processed_data["Subtype"].values, "'Childrens' rule not applied correctly."

# Test case for numeric primary key generation
def test_generate_numeric_key():
    assert generate_numeric_key("12345") == 12345, "Numeric key generation failed for numeric input."
    assert isinstance(generate_numeric_key("ABCDE"), int), "Numeric key generation failed for string input."

@pytest.fixture
def sample_address_data():
    return pd.DataFrame({
        "Address Line 1": ["123 Main St", "456 Elm St"],
        "Address Line 2": ["Apt 101", "Suite 200"],
        "City/Town": ["Springfield", "Shelbyville"],
        "State": ["IL", "IL"],
        "ZIP Code": ["62701", "62702"],
        "CMS Certification Number (CCN)": ["12345", "67890"]
    })

@pytest.fixture
def sample_states_data():
    return [
        {"state_id": 1, "state_code": "IL", "state_name": "Illinois"},
        {"state_id": 2, "state_code": "CA", "state_name": "California"}
    ]

# Test for extracting addresses and generating unique address IDs
def test_extract_addresses(sample_address_data, tmpdir):
    # Mock the output files
    addresses_file = tmpdir.join("addresses.csv")
    states_file = tmpdir.join("states.csv")
    
    with patch("facilities_importer.addresses_file", str(addresses_file)), \
         patch("facilities_importer.states_file", str(states_file)):
        # Extract addresses
        extract_addresses(sample_address_data)

        # Validate addresses CSV content
        addresses_df = pd.read_csv(addresses_file)
        assert len(addresses_df) == len(sample_address_data), "Number of addresses does not match input data."
        assert "address_id" in addresses_df.columns, "Missing 'address_id' column in addresses CSV."
        assert addresses_df["address"].iloc[0] == "123 Main St Apt 101", "Concatenation of 'Address Line 1' and 'Address Line 2' failed."

# Test for state ID creation and uniqueness
def test_get_or_create_state_id(sample_states_data):
    # Initialize the state mapping
    state_mapping = {state["state_code"]: state for state in sample_states_data}
    
    # Mock the state_mapping in the function
    with patch("facilities_importer.state_mapping", state_mapping):
        # Add a new state
        new_state_id = get_or_create_state_id("TX")
        assert new_state_id == 3, "New StateID should be 3 for 'TX'."
        assert len(state_mapping) == 3, "State mapping should contain 3 entries after adding a new state."
        
        # Retrieve an existing state
        existing_state_id = get_or_create_state_id("IL")
        assert existing_state_id == 1, "Existing StateID for 'IL' should be 1."
        assert len(state_mapping) == 3, "State mapping size should not change when retrieving an existing state."

# Test for saving states to CSV
def test_save_states_to_csv(sample_states_data, tmpdir):
    states_file = tmpdir.join("states.csv")
    
    # Mock the state mapping and output file
    with patch("facilities_importer.state_mapping", {state["state_code"]: state for state in sample_states_data}), \
         patch("facilities_importer.states_file", str(states_file)):
        # Save states
        save_states_to_csv()
        
        # Validate states CSV content
        states_df = pd.read_csv(states_file)
        assert len(states_df) == len(sample_states_data), "Number of states does not match input data."
        assert "state_id" in states_df.columns, "Missing 'StateID' column in states CSV."
        assert states_df["state_code"].iloc[0] == "IL", "First state code should be 'IL'."

# Test for generating unique address IDs
def test_generate_address_id():
    cnn = "12345"
    address = "123 Main St"
    city = "Springfield"
    state = "IL"
    zip_code = "62701"

    # Generate address ID
    address_id = generate_address_id(cnn, address, city, state, zip_code)
    
    # Manually compute the expected address hash for verification
    zip_trimmed = zip_code[:5]
    address_str = f"{address}|{city}|{state}|{zip_trimmed}"
    address_hash = hashlib.md5(address_str.encode()).hexdigest()
    expected_id = int(hashlib.md5(f"{cnn}{address_hash}".encode()).hexdigest(), 16) % (10**9)
    
    assert address_id == expected_id, "Generated address ID does not match expected value."

# Test for extracting addresses with missing columns
def test_extract_addresses_with_missing_columns(sample_address_data, tmpdir):
    # Remove 'Address Line 2' column
    modified_data = sample_address_data.drop(columns=["Address Line 2"])
    
    # Mock the output files
    addresses_file = tmpdir.join("addresses.csv")
    states_file = tmpdir.join("states.csv")
    
    with patch("facilities_importer.addresses_file", str(addresses_file)), \
         patch("facilities_importer.states_file", str(states_file)):
        # Extract addresses
        extract_addresses(modified_data)
        
        # Validate addresses CSV content
        addresses_df = pd.read_csv(addresses_file)
        assert len(addresses_df) == len(modified_data), "Number of addresses does not match input data."
        assert addresses_df["address"].iloc[0] == "123 Main St", "Address should not include 'Address Line 2'."

# Test for duplicate StateID in case of the same state
def test_duplicate_state_id(sample_states_data):
    # Mock the state mapping
    state_mapping = {state["state_code"]: state for state in sample_states_data}
    
    with patch("facilities_importer.state_mapping", state_mapping):
        state_id_1 = get_or_create_state_id("IL")
        state_id_2 = get_or_create_state_id("IL")
        
        assert state_id_1 == state_id_2, "state_id should be the same for duplicate state codes."