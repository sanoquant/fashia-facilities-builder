import pandas as pd
import pytest # type: ignore
from unittest.mock import patch
import sys
import os

# Add the script's directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))

# Import functions from the main script
from facilities_importer import process_file, generate_numeric_key

# Sample datasets for each file
@pytest.fixture
def sample_datasets():
    return {
        "DFC_FACILITY.csv": pd.DataFrame({
            "CMS Certification Number (CCN)": ["12345", "67890"],
            "Facility Name": ["Dialysis Center A", "Dialysis Center B"]
        }),
        "HH_Provider_Oct2024.csv": pd.DataFrame({
            "Offers Nursing Care Services": ["Yes", "No"],
            "Offers Physical Therapy Services": ["No", "Yes"],
            "Provider Name": ["Agency A", "Agency B"]
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
    assert "NUCC_Code" in processed_data.columns, f"NUCC_Code column missing for {file_name}."

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
    assert len(processed_data) >= len(data), "Processed data should duplicate rows with 'Yes' conditions."

# Test case for "checkByFieldValue" subrules
def test_check_by_field_value(sample_datasets):
    file_name = "Hospital_General_Information.csv"
    data = sample_datasets[file_name]
    processed_data = process_file(file_name, data)

    # Verify specific subrule application
    assert not processed_data.empty, f"Processed data for {file_name} should not be empty."
    assert "Acute Care" in processed_data["Subtype"].values, "'Acute Care Hospitals' rule not applied correctly."
    assert "Pediatric" in processed_data["Subtype"].values, "'Childrens' rule not applied correctly."

# Test case for numeric primary key generation
def test_generate_numeric_key():
    assert generate_numeric_key("12345") == 12345, "Numeric key generation failed for numeric input."
    assert isinstance(generate_numeric_key("ABCDE"), int), "Numeric key generation failed for string input."
