import dask.dataframe as dd
import time

# Measure the start time
start_time = time.time()

# File paths: input file and output file
input_file = './datasets/NPPES_file.csv'
output_file = './datasets/filtered/nppes_filtered_data.csv'

# Required columns
required_columns = [
    "NPI",
    "Entity Type Code",
    "Provider Organization Name (Legal Business Name)",
    "Provider Last Name (Legal Name)",
    "Provider First Name",
    "Provider Middle Name",
    "Provider First Line Business Practice Location Address",
    "Provider Second Line Business Practice Location Address",
    "Provider Business Practice Location Address City Name",
    "Provider Business Practice Location Address State Name",
    "Provider Business Practice Location Address Postal Code",
    "Provider Business Practice Location Address Country Code (If outside U.S.)",
    "Last Update Date",
    "NPI Deactivation Date",
    "Healthcare Provider Taxonomy Code_1",
    "Provider License Number_1",
    "Provider License Number State Code_1",
    "Healthcare Provider Primary Taxonomy Switch_1",
    "Healthcare Provider Taxonomy Code_2",
    "Provider License Number_2",
    "Provider License Number State Code_2",
    "Healthcare Provider Primary Taxonomy Switch_2",
    "Healthcare Provider Taxonomy Code_3",
    "Provider License Number_3",
    "Provider License Number State Code_3",
    "Healthcare Provider Primary Taxonomy Switch_3",
    "Healthcare Provider Taxonomy Code_4",
    "Provider License Number_4",
    "Provider License Number State Code_4",
    "Healthcare Provider Primary Taxonomy Switch_4",
    "Healthcare Provider Taxonomy Code_5",
    "Provider License Number_5",
    "Provider License Number State Code_5",
    "Healthcare Provider Primary Taxonomy Switch_5",
    "Healthcare Provider Taxonomy Code_6",
    "Provider License Number_6",
    "Provider License Number State Code_6",
    "Healthcare Provider Primary Taxonomy Switch_6",
    "Healthcare Provider Taxonomy Code_7",
    "Provider License Number_7",
    "Provider License Number State Code_7",
    "Healthcare Provider Primary Taxonomy Switch_7",
    "Healthcare Provider Taxonomy Code_8",
    "Provider License Number_8",
    "Provider License Number State Code_8",
    "Healthcare Provider Primary Taxonomy Switch_8",
    "Healthcare Provider Taxonomy Code_9",
    "Provider License Number_9",
    "Provider License Number State Code_9",
    "Healthcare Provider Primary Taxonomy Switch_9",
    "Healthcare Provider Taxonomy Code_10",
    "Provider License Number_10",
    "Provider License Number State Code_10",
    "Healthcare Provider Primary Taxonomy Switch_10",
    "Healthcare Provider Taxonomy Code_11",
    "Provider License Number_11",
    "Provider License Number State Code_11",
    "Healthcare Provider Primary Taxonomy Switch_11",
    "Healthcare Provider Taxonomy Code_12",
    "Provider License Number_12",
    "Provider License Number State Code_12",
    "Healthcare Provider Primary Taxonomy Switch_12",
    "Healthcare Provider Taxonomy Code_13",
    "Provider License Number_13",
    "Provider License Number State Code_13",
    "Healthcare Provider Primary Taxonomy Switch_13",
    "Healthcare Provider Taxonomy Code_14",
    "Provider License Number_14",
    "Provider License Number State Code_14",
    "Healthcare Provider Primary Taxonomy Switch_14",
    "Healthcare Provider Taxonomy Code_15",
    "Provider License Number_15",
    "Provider License Number State Code_15",
    "Healthcare Provider Primary Taxonomy Switch_15",
    "Certification Date",
]

# Read only the required columns to reduce memory usage
df = dd.read_csv(
    input_file, 
    dtype="str", 
    assume_missing=True, 
    low_memory=False, 
    usecols=required_columns
)

# Filter data:
# 1. 'NPI Deactivation Date' is empty or null
# 2. 'Entity Type Code' equals 1 or 2
df_filtered = df[
    (df['NPI Deactivation Date'].fillna('').str.strip() == '') &
    (df['Entity Type Code'] == '2')
]

# Optimize partitions for more efficient writing
df_filtered = df_filtered.repartition(npartitions=10)  # Adjust the number based on your system
print('Filtered data ready for processing')

# Save to a single file using pandas for faster write performance
df_filtered.compute().to_csv(output_file, index=False)

# Measure the end time
end_time = time.time()

# Calculate total execution time
execution_time = end_time - start_time

print(f"Filtered data saved to {output_file}")
print(f"Execution time: {execution_time:.2f} seconds")