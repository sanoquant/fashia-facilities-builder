import pandas as pd
print("Environment setup complete!")
files = ["DFC_FACILITY.csv", "HH_Provider_Oct2024.csv", "Hospice_General-Information_Aug2024.csv", "Hospital_General_Information.csv", "Inpatient_Rehabilitation_Facility-General_Information_Sep2024.csv", "Long-Term_Care_Hospital-General_Information_Sep2024.csv", "NH_ProviderInfo_Oct2024.csv"]
for file in files:
    try:
        df = pd.read_csv("./datasets/"+file)
        print(f"Loaded {file} successfully with {len(df)} rows.")
    except Exception as e:
        print(f"Error loading {file}: {e}")