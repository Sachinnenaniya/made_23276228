#!/bin/bash

# Define the output files expected from the pipeline
OUTPUT_FILES=("data/database1.db" "data/database2.db")

# Run the data pipeline
echo "Running the data pipeline..."
bash pipeline.sh

# Check if the output files are created
all_files_exist=true

for file in "${OUTPUT_FILES[@]}"; do
    if [ ! -f "$file" ]; then
        echo "Test failed: Output file $file does not exist."
        all_files_exist=false
    fi
done

if [ "$all_files_exist" = true ]; then
    echo "Test passed: All output files exist."
    exit 0
else
    echo "Test failed: Not all output files were created."
    exit 1
fi

echo "Test completed successfully. Bye :) "