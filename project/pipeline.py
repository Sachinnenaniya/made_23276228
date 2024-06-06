import os
import pandas as pd
import sqlite3
import requests

# URLs for the datasets
url1 = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/sdg_13_10?format=TSV&compressed=false"
url2 = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/ten00124?format=TSV&compressed=false"

# Data directory
data_dir = "data"
csv_paths = {
    "data1": os.path.join(data_dir, "net_greenhouse_gas_emissions.csv"),
    "data2": os.path.join(data_dir, "final_energy_consumption_by_sector.csv")
}
database_paths = {
    "database1": os.path.join(data_dir, "database1.db"),
    "database2": os.path.join(data_dir, "database2.db")
}

# Create data directory if it doesn't exist
if not os.path.exists(data_dir):
    os.makedirs(data_dir)

# Function to download and save files
def download_file(url, file_path):
    response = requests.get(url)
    response.raise_for_status()
    with open(file_path, 'wb') as file:
        file.write(response.content)

# Download the datasets
download_file(url1, csv_paths["data1"])
download_file(url2, csv_paths["data2"])

# Read the datasets into DataFrames
df1 = pd.read_csv(csv_paths["data1"], delimiter='\t')
df2 = pd.read_csv(csv_paths["data2"], delimiter='\t')

# Fill missing values with 0
df1.fillna(0, inplace=True)
df2.fillna(0, inplace=True)

# Clean column names (strip and lowercase)
df1.columns = [col.strip().lower() for col in df1.columns]
df2.columns = [col.strip().lower() for col in df2.columns]

# Save DataFrames to SQLite databases
def save_to_sqlite(df, db_path, table_name):
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.close()

save_to_sqlite(df1, database_paths["database1"], "net_greenhouse_gas_emissions")
save_to_sqlite(df2, database_paths["database2"], "final_energy_consumption_by_sector")

print("Data pipeline execution completed.")
