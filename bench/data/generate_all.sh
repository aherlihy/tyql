#!/bin/bash

# Check if the integer argument is provided
if [ -z "$1" ]; then
    echo "Usage: $0 <size_in_mb>"
    exit 1
fi

# Assign the first argument to a variable and append 'mb' to directory name
i="$1"
dir_suffix="data_${i}mb"

echo "Size ${i}MB"
echo "Dir: ${dir_suffix}"

# Loop through each subdirectory in the current directory
for dir in */; do
    echo "subdirectory: $dir"

    # Define the target directory
    target_dir="${dir}${dir_suffix}"

    # Create the target directory if it doesn't exist
    if [ ! -d "$target_dir" ]; then
        mkdir -p "$target_dir"
    fi

    # Check if 'csv_columns.txt' exists in the subdirectory
    csv_file="${dir}csv_columns.txt"
    if [ -f "$csv_file" ]; then
        echo "--> Found csv_columns.txt in $dir"

        # Read the entire content of the file into a single variable
                file_content=$(<"$csv_file")

                # Convert the content into an array by splitting on newline
                IFS=$'\n' lines=($file_content)

                # Loop over each line in the array
                for line in "${lines[@]}"; do
                  echo "----> input: $line"
                  # Change to the target directory and execute the Python command with `eval`
                  (
                      cd "$target_dir"
                      k=$(echo "scale=2; $i / 1024" | bc)
                      echo "Executing: python3 ../../generate.py --size $k $line"
                      eval python3 ../../generate.py --size "$k" $line
                  )
                done
    else
        echo "--> skipping"
    fi
done

