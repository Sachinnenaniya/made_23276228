import os
import pytest
import subprocess
import sqlite3

# Paths to the output database files
DATABASE1_PATH = "data/database1.db"
DATABASE2_PATH = "data/database2.db"

# Run the pipeline before tests
@pytest.fixture(scope="module", autouse=True)
def run_pipeline():
    # Run the pipeline script
    result = subprocess.run(["bash", "pipeline.sh"], capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"Pipeline failed with error:\n{result.stderr}")

def test_database1_exists():
    assert os.path.isfile(DATABASE1_PATH), f"{DATABASE1_PATH} does not exist."

def test_database2_exists():
    assert os.path.isfile(DATABASE2_PATH), f"{DATABASE2_PATH} does not exist."

def test_database1_contents():
    conn = sqlite3.connect(DATABASE1_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='net_greenhouse_gas_emissions';")
    table_exists = cursor.fetchone()
    conn.close()
    assert table_exists is not None, "Table 'net_greenhouse_gas_emissions' does not exist in database1."

def test_database2_contents():
    conn = sqlite3.connect(DATABASE2_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='final_energy_consumption_by_sector';")
    table_exists = cursor.fetchone()
    conn.close()
    assert table_exists is not None, "Table 'final_energy_consumption_by_sector' does not exist in database2."
