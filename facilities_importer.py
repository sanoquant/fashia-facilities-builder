import os
import pandas as pd
import hashlib
print("Environment setup complete!")


# lista de archivos
files = ["DFC_FACILITY.csv", "HH_Provider_Oct2024.csv", "Hospice_General-Information_Aug2024.csv", "Hospital_General_Information.csv", "Inpatient_Rehabilitation_Facility-General_Information_Sep2024.csv", "Long-Term_Care_Hospital-General_Information_Sep2024.csv", "NH_ProviderInfo_Oct2024.csv"]

# Diccionario de reglas basado en el nombre del archivo
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


# Función para generar una clave primaria numérica
def generate_numeric_key(ccn):
    if not str(ccn).isnumeric():
        # Convierte el CCN en un hash numérico
        return int(hashlib.md5(str(ccn).encode()).hexdigest(), 16) % (10**9)  # Limita a 9 dígitos
    return int(ccn)

# Función principal para procesar un archivo basado en el diccionario de reglas
def process_file(file_name, data):
    rules = file_rules_mapping.get(file_name)
    if not rules:
        print(f"No rules found for file: {file_name}")
        return pd.DataFrame()

    entities = []

    # Procesar reglas generales
    if "Type" in rules and "Subtype" in rules and "NUCC Code" in rules:
        data["Type"] = rules["Type"]
        data["Subtype"] = rules["Subtype"]
        data["NUCC Code"] = rules["NUCC Code"]
        entities.extend(data.to_dict(orient="records"))

    # Procesar subreglas
    if "SubRules" in rules:
        if rules.get("typeSubRules") == "ifCnnIsNumber":
            # Subreglas basadas en si el CCN es numérico
            for condition, subrule in rules["SubRules"].items():
                filtered_data = data[data["CMS Certification Number (CCN)"].str.isnumeric() if condition == "true" else ~data["CMS Certification Number (CCN)"].str.isnumeric()]
                filtered_data["Type"] = subrule["Type"]
                filtered_data["Subtype"] = subrule["Subtype"]
                filtered_data["NUCC Code"] = subrule["NUCC Code"]
                entities.extend(filtered_data.to_dict(orient="records"))

        elif rules.get("typeSubRules") == "duplicateByActiveFlag":
            # Subreglas para duplicar entidades según flags
            for column, subrule in rules["SubRules"].items():
                filtered_data = data[data[column] == "Yes"]
                filtered_data["Type"] = subrule["Type"]
                filtered_data["Subtype"] = subrule["Subtype"]
                filtered_data["NUCC Code"] = subrule["NUCC Code"]
                entities.extend(filtered_data.to_dict(orient="records"))

        elif rules.get("typeSubRules") == "checkByFieldValue":
            # Subreglas basadas en valores de campos
            for field_value, subrule in rules["SubRules"].items():
                if isinstance(subrule, list):  # Manejar listas de subreglas
                    # Filtrar las filas que coincidan con el valor del campo
                    filtered_data = data[data["Hospital Type"] == field_value]
                    
                    # Crear una entidad por cada regla en la lista
                    for rule in subrule:
                        # Copiar el DataFrame filtrado para evitar conflictos de referencia
                        entity_data = filtered_data.copy()
                        
                        # Asignar valores específicos de la subregla actual
                        entity_data["Type"] = rule["Type"]
                        entity_data["Subtype"] = rule["Subtype"]
                        entity_data["NUCC Code"] = rule["NUCC Code"]
                        
                        # Agregar las entidades procesadas a la lista
                        entities.extend(entity_data.to_dict(orient="records"))
                else:
                    # Procesar una única regla
                    filtered_data = data[data["Hospital Type"] == field_value]
                    filtered_data["Type"] = subrule["Type"]
                    filtered_data["Subtype"] = subrule["Subtype"]
                    filtered_data["NUCC Code"] = subrule["NUCC Code"]
                    entities.extend(filtered_data.to_dict(orient="records"))

    return pd.DataFrame(entities)


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
            
            
            if "Hospital_General_Information.csv" != file:
                # Generar la clave primaria numérica
                filtered_data["PrimaryKey"] = filtered_data["CMS Certification Number (CCN)"].apply(generate_numeric_key)
            
            # Se procesan los datos del archivo CMS actual a partir del diccionario de reglas por archivo
            processed_data = process_file(file, filtered_data)
            
            # Guardar los datos filtrados en la carpeta específica
            file_name = os.path.basename(file).replace(".csv", "_filtered.csv")
            output_path = os.path.join(filtered_folder, file_name)
            filtered_data.to_csv(output_path, index=False)
            print(f"Filtered data saved to: {output_path}\n\n\n")
        
            
        else:
            print("Required columns (or alternatives) not found. Skipping file.\n\n\n")
    except Exception as e:
        print(f"Error loading {file}: {e}\n\n\n")