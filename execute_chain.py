import subprocess

def execute_file(file_path):
    """
    Ejecuta un archivo Python y captura la salida.
    """
    try:
        print(f"Ejecutando: {file_path}")
        result = subprocess.run(
            ["/usr/bin/python3", file_path],  # Ruta completa
            capture_output=True,
            text=True,
            check=True
        )
        print(f"Salida de {file_path}:\n{result.stdout}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error ejecutando {file_path}: {e.stderr}")
        return False

def main():
    # Lista de archivos a ejecutar en cadena
    files_to_execute = [
        "filter_nppes_data.py",
        "facilities_importer.py",
        "setup_database.py",
        "check_unique_address_hash.py",
        "address_geocoder.py"
    ]

    for file in files_to_execute:
        success = execute_file(file)
        if not success:
            print(f"Deteniendo ejecución debido a un error en {file}.")
            break
    else:
        print("Todos los archivos se ejecutaron correctamente.")

if __name__ == "__main__":
    main()