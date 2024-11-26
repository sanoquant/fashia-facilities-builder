import os
import pandas as pd
import hashlib
print("Environment setup complete!")


# lista de archivos
files = ["DFC_FACILITY.csv", "HH_Provider_Oct2024.csv", "Hospice_General-Information_Aug2024.csv", "Hospital_General_Information.csv", "Inpatient_Rehabilitation_Facility-General_Information_Sep2024.csv", "Long-Term_Care_Hospital-General_Information_Sep2024.csv", "NH_ProviderInfo_Oct2024.csv"]
# Lista de columnas requeridas y sus alternativas
column_mapping = {
    "Address": ["Address Line 1", "Address", "Provider Address"],
    "City": ["City/Town"],  # Sin alternativos
    "State": ["State"],  # Sin alternativos
    "ZipCode": ["ZIP Code"]  # Sin alternativos
}

# Crear la carpeta para los archivos filtrados si no existe
filtered_folder = "datasets/filtered"
os.makedirs(filtered_folder, exist_ok=True)

for file in files:
    try:
        # Cargar el archivo actual
        df = pd.read_csv("./datasets/"+file)
        print(f"Loaded {file} successfully with {len(df)} rows.")
        print(df.head())
        
        # Crear un mapeo dinámico para columnas presentes en el archivo
        dynamic_columns = {}
        for main_col, alternatives in column_mapping.items():
            for alt_col in [main_col] + alternatives:
                if alt_col in df.columns:
                    dynamic_columns[main_col] = alt_col
                    break
        
        # Verificar si todas las columnas requeridas (o sus alternativas) están presentes
        if set(column_mapping.keys()).issubset(dynamic_columns.keys()):
            print("Required columns found (or alternatives). Applying filter...")
            
            # Filtrar filas con valores faltantes
            subset_columns = list(dynamic_columns.values())  # Usar las columnas dinámicas encontradas
            filtered_data = df.dropna(subset=subset_columns, how="any")
            
            # Guardar los datos filtrados en la carpeta específica
            file_name = os.path.basename(file).replace(".csv", "_filtered.csv")
            output_path = os.path.join(filtered_folder, file_name)
            filtered_data.to_csv(output_path, index=False)
            print(f"Filtered data saved to: {output_path}")
            
        else:
            print("Required columns (or alternatives) not found. Skipping file.")
    except Exception as e:
        print(f"Error loading {file}: {e}\n\n\n")