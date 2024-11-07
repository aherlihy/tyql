#!/bin/bash

# Set the directory name variable
if [ -z "$1" ] || [ -z "$2" ]; then
    echo "Usage: $0 <directory_name> <unit>"
    echo "Example: $0 data_1MB MB"
    echo "Example: $0 data_1MB B"
    exit 1
fi

datadir="$1"
unit="$2"

# Validate the unit argument
if [[ "$unit" != "MB" && "$unit" != "B" ]]; then
    echo "Invalid unit. Please specify 'MB' or 'B'."
    exit 1
fi

# Loop through each '$datadir' directory in the current directory structure
for dir in */"$datadir"; do
  # Check if the directory exists
  if [ -d "$dir" ]; then
    # Get the parent directory name (named_directory)
    parent_dir=$(basename "$(dirname "$dir")")

    # Calculate the size of the '$datadir' directory in MB
  if [ "$unit" == "MB" ]; then
      total_size=$(du -sk "$dir" | awk '{printf "%.2f MB", $1/1024}')
  else
      total_size=$(du -sk "$dir" | awk '{printf "%d B", $1 * 1024}')
  fi

    echo "$parent_dir/$datadir: ${total_size}"

    # Loop through each CSV file in the '$datadir' directory
    for file in "$dir"/*.csv; do
      # Check if the file exists to handle any missing or non-CSV files
      if [ -f "$file" ]; then
        # Get the file name and its size in MB
        file_name=$(basename "$file")
        if [ "$unit" == "MB" ]; then
            file_size=$(du -sk "$file" | awk '{printf "%.2f MB", $1/1024}')
        else
            file_size=$(du -sk "$file" | awk '{printf "%d B", $1 * 1024}')
        fi
        echo -e "\t$parent_dir/$file_name: ${file_size}"
      fi
    done
  fi
done
