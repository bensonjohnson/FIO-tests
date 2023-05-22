#!/bin/bash

# Directory where the INI files are located
ini_directory=$(dirname "$0")

# Test directory
test_directory="/home/benson/test"

# List of INI files to run
inis=("sequential_read.ini" "sequential_write.ini" "random_read.ini" "random_write.ini" "mixed_read_write.ini" "sequential_read_write.ini")

# Run FIO for each INI file and pipe the output to the Python script
for ini_file in "${inis[@]}"; do
    echo "Running FIO with $ini_file"
    fio --output-format=json "$ini_directory/$ini_file" --directory="$test_directory" | python3 fio_to_influx.py
done



