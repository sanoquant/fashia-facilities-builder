import subprocess

def execute_file(file_path):
    """
    Run a Python file and capture the output.
    """
    try:
        print(f"Running: {file_path}")
        result = subprocess.run(
            ["/usr/bin/python3", file_path],  # Full route
            capture_output=True,
            text=True,
            check=True
        )
        print(f"Output of {file_path}:\n{result.stdout}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error executing {file_path}: {e.stderr}")
        return False

def main():
    # List of files to be executed in a chain
    files_to_execute = [
        "filter_nppes_data.py",
        "facilities_importer.py",
        "nppes_importer.py",
        "setup_database.py",
        "check_unique_address_hash.py",
        "address_geocoder.py"
    ]

    for file in files_to_execute:
        success = execute_file(file)
        if not success:
            print(f"Stopping execution due to an error in {file}.")
            break
    else:
        print("All files executed successfully.")

if __name__ == "__main__":
    main()