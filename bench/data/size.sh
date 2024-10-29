#!/bin/bash

# Set the directory name variable
datadir="data_1MB"  # You can change this to any directory name you want

# Loop through each '$datadir' directory in the current directory structure
for dir in */"$datadir"; do
  # Check if the directory exists
  if [ -d "$dir" ]; then
    # Get the parent directory name (named_directory)
    parent_dir=$(basename "$(dirname "$dir")")

    # Calculate the size of the '$datadir' directory in MB
    total_size=$(du -s "$dir" | awk '{print int($1/1024)" MB"}')
    echo "$parent_dir/$datadir: ${total_size}"

    # Loop through each CSV file in the '$datadir' directory
    for file in "$dir"/*.csv; do
      # Check if the file exists to handle any missing or non-CSV files
      if [ -f "$file" ]; then
        # Get the file name and its size in MB
        file_name=$(basename "$file")
        file_size=$(du -s "$file" | awk '{print int($1/1024)" MB"}')
        echo -e "\t$parent_dir/$file_name: ${file_size}"
      fi
    done
  fi
done
