import os
import unittest
import pandas as pd
import sqlite3
import requests

# URLs for the datasets
url1 = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/ten00124?format=TSV&compressed=false"
url2 = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/sdg_13_10?format=TSV&compressed=false"

# Data directory
output_dir = "data"
tsv_paths = {
    "data1": os.path.join(output_dir, "final_energy_consumption_by_sector.tsv"),
    "data2": os.path.join(output_dir, "net_greenhouse_gas_emissions.tsv"),
}
excel_paths = {
    "data1": os.path.join(output_dir, "final_energy_consumption_by_sector.xlsx"),
    "data2": os.path.join(output_dir, "net_greenhouse_gas_emissions.xlsx"),
}
database_paths = {
    "database1": os.path.join(output_dir, "final_energy_consumption_by_sector.db"),
    "database2": os.path.join(output_dir, "net_greenhouse_gas_emissions.db"),
}

# Create output directory if it doesn't exist
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Function to download and save files
def download_file(url, file_path):
    if not os.path.exists(file_path):
        response = requests.get(url)
        response.raise_for_status()
        with open(file_path, 'wb') as file:
            file.write(response.content)
        print(f"Downloaded {file_path}")
    else:
        print(f"File {file_path} already exists. Skipping download.")

# Function to save DataFrames to SQLite databases
def save_to_sqlite(df, db_path, table_name):
    if os.path.exists(db_path):
        os.remove(db_path)
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.close()
    print(f"Data saved to {db_path} in table {table_name}")

# Function to save DataFrames to Excel files
def save_to_excel(df, excel_path):
    if os.path.exists(excel_path):
        os.remove(excel_path)
    df.to_excel(excel_path, index=False)
    print(f"Data saved to {excel_path}")

class TestDataPipeline(unittest.TestCase):
    
    def setUp(self):
        # Download the datasets
        print("Downloading datasets...")
        download_file(url1, tsv_paths["data1"])
        download_file(url2, tsv_paths["data2"])

        print("Download complete.")

        # Read the datasets into DataFrames
        print("Reading datasets into DataFrames...")
        self.energy_consumption = pd.read_csv(tsv_paths["data1"], delimiter='\t', encoding='ISO-8859-1')
        self.greenhouse_emissions = pd.read_csv(tsv_paths["data2"], delimiter='\t', encoding='ISO-8859-1')

    def test_fill_missing_values(self):
        # Fill missing values with 0
        print("Filling missing values...")
        self.energy_consumption.fillna(0, inplace=True)
        self.greenhouse_emissions.fillna(0, inplace=True)
        self.assertFalse(self.energy_consumption.isnull().values.any())
        self.assertFalse(self.greenhouse_emissions.isnull().values.any())

    def test_clean_column_names(self):
        # Clean column names (strip and lowercase)
        print("Cleaning column names...")
        self.energy_consumption.columns = [col.strip().lower() for col in self.energy_consumption.columns]
        self.greenhouse_emissions.columns = [col.strip().lower() for col in self.greenhouse_emissions.columns]
        for col in self.energy_consumption.columns:
            self.assertEqual(col, col.lower().strip())
        for col in self.greenhouse_emissions.columns:
            self.assertEqual(col, col.lower().strip())

    def test_save_to_excel(self):
        # Save cleaned DataFrames as Excel files
        save_to_excel(self.energy_consumption, excel_paths["data1"])
        save_to_excel(self.greenhouse_emissions, excel_paths["data2"])
        self.assertTrue(os.path.exists(excel_paths["data1"]))
        self.assertTrue(os.path.exists(excel_paths["data2"]))

    def test_save_to_sqlite(self):
        # Save DataFrames to SQLite databases
        save_to_sqlite(self.energy_consumption, database_paths["database1"], "final_energy_consumption_by_sector")
        save_to_sqlite(self.greenhouse_emissions, database_paths["database2"], "net_greenhouse_gas_emissions")
        self.assertTrue(os.path.exists(database_paths["database1"]))
        self.assertTrue(os.path.exists(database_paths["database2"]))

    def tearDown(self):
        # Clean up the test files if needed
        for path in tsv_paths.values():
            if os.path.exists(path):
                os.remove(path)
        for path in excel_paths.values():
            if os.path.exists(path):
                os.remove(path)
        for path in database_paths.values():
            if os.path.exists(path):
                os.remove(path)

if __name__ == '__main__':
    unittest.main()
