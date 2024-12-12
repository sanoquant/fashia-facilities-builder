import pandas as pd
import pytest
from unittest.mock import patch
from io import StringIO
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))
from nppes_importer import (
    load_datasets,
    find_taxonomy_fields,
    compare_and_update,
    process_nppes,
    map_row_to_entity,
    extract_addresses
)


# Sample NPPES and CMS datasets for testing
@pytest.fixture
def sample_datasets():
    nppes_csv = StringIO("""
NPI,Taxonomy Code,Provider Organization Name (Legal Business Name),Provider Other Organization Name,Parent Organization LBN,Entity Type Code,Provider First Line Business Practice Location Address,Provider Second Line Business Practice Location Address,Provider Business Practice Location Address City Name,Provider Business Practice Location Address State Name,Provider Business Practice Location Address Postal Code
1234567890,251G00000X,Entity A,,,1,123 Main St,,Springfield,IL,62704
2345678901,282N00000X,Entity B,Alternate Name B,,2,456 Oak St,,Metropolis,NY,10001
3456789012,251G00000X,Entity C,Alternate Name C,,1,789 Pine St,,Gotham,CA,90210
""")
    cms_csv = StringIO("""
entity_id,name,ccn,npi,type,subtype,nucc_code,unique_facility_at_location,employer_group_type,entity_unique_to_address,multi_speciality_facility,multi_speciality_employer,employer_num
1,Entity A,12345,1234567890,1,,251G00000X,1,none,1,0,0,
2,Entity D,67890,,,,282N00000X,1,none,1,0,0,
""")
    nppes_data = pd.read_csv(nppes_csv)
    cms_data = pd.read_csv(cms_csv)
    return nppes_data, cms_data

# Test loading datasets
def test_load_datasets(sample_datasets):
    nppes_data, cms_data = sample_datasets
    assert not nppes_data.empty, "NPPES dataset should not be empty."
    assert not cms_data.empty, "CMS dataset should not be empty."

# Test finding taxonomy fields
def test_find_taxonomy_fields(sample_datasets):
    nppes_data, _ = sample_datasets
    taxonomy_fields = find_taxonomy_fields(nppes_data.columns)
    assert "Taxonomy Code" in taxonomy_fields, "Taxonomy Code field should be identified."

# Test compare and update function
def test_compare_and_update(sample_datasets):
    nppes_data, cms_data = sample_datasets
    nppes_row = nppes_data.iloc[0]
    cms_row = cms_data.iloc[0]

    updated_row, updated = compare_and_update(nppes_row, cms_row)
    assert updated, "The record should be marked as updated."
    assert updated_row["npi"] == nppes_row["NPI"], "NPI should be updated in CMS record."
    assert updated_row["Type"] == nppes_row["Entity Type Code"], "Type should be updated in CMS record."

# Test processing NPPES data for updates and new entities
def test_process_nppes_updates_and_creates(sample_datasets):
    nppes_data, cms_data = sample_datasets
    updated_cms_records, new_entities, new_addresses = process_nppes(nppes_data, cms_data)

    # Check updates
    assert len(updated_cms_records) > 0, "There should be updated CMS records."
    assert updated_cms_records[0]["npi"] == "1234567890", "Updated CMS record should match NPI."

    # Check new entities
    assert len(new_entities) > 0, "There should be new entities created."
    assert new_entities[0]["name"] == "Entity B", "New entity should match NPPES data."

    # Check new addresses
    assert len(new_addresses) > 0, "There should be new addresses created."
    assert new_addresses[0]["city"] == "Metropolis", "New address should match NPPES data."

# Test mapping NPPES row to entity format
def test_map_row_to_entity(sample_datasets):
    nppes_data, _ = sample_datasets
    nppes_row = nppes_data.iloc[1]
    entity, address = map_row_to_entity(nppes_row, "Taxonomy Code")

    assert entity["name"] == "Entity B", "Entity name should match NPPES data."
    assert entity["nucc_code"] == "282N00000X", "NUCC code should match taxonomy code."
    assert address["city"] == "Metropolis", "Address city should match NPPES data."

# Test extracting addresses
def test_extract_addresses(sample_datasets):
    nppes_data, _ = sample_datasets
    nppes_row = nppes_data.iloc[0]
    address = extract_addresses(nppes_row)

    assert address["address"] == "123 Main St", "Address should match NPPES data."
    assert address["city"] == "Springfield", "City should match NPPES data."
    assert address["state_id"] is not None, "State ID should be assigned."
