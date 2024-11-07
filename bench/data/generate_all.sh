#!/bin/bash

# Check if both arguments are provided
if [ -z "$1" ] || [ -z "$2" ]; then
    echo "Usage: $0 <size> <unit>"
    echo "Example: $0 100 MB or $0 0.1 GB"
    exit 1
fi

# Assign the first argument (size) and second argument (unit) to variables
i="$1"
unit="$2"
dir_suffix="data_${i}${unit}"

echo "Size ${i}${unit}"
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

    acyclic=""
    if [[ "$dir" == *"sssp"* ]]; then
        acyclic="--acyclic"
        base_csv_path="${target_dir}/base.csv"
        echo -e "dst,cost\n1,0" > "$base_csv_path"
        echo "    ---> Generated $base_csv_path"
    fi
    if [[ "$dir" == *"bom"* ]]; then
        acyclic="--acyclic"
    fi
    base1=""
    if [[ "$dir" == *"tc"* ]]; then
        base1="--base1"
    fi
    baseName=""
    if [[ "$dir" == *"ancestry"* ]]; then
        baseName="--acyclic"
    fi
    cba=""
    if [[ "$dir" == *"cba"* ]]; then
        cba="--cba"
    fi


    # Check if 'csv_columns.txt' exists in the subdirectory
    csv_file="${dir}csv_columns.txt"
    if [ -f "$csv_file" ]; then
#        echo "    Found csv_columns.txt in $dir"
        if [[ $dir != *"ancestry"* && $dir != *"tc"* && $dir != *"sssp"* ]]; then
            echo "skipping"
            continue
        fi

        # Read the entire content of the file into a single variable
        file_content=$(<"$csv_file")

        # Convert the content into an array by splitting on newline
        IFS=$'\n' lines=($file_content)

        # Loop over each line in the array
        for line in "${lines[@]}"; do
#            echo "----> input: $line"
            # Change to the target directory and execute the Python command with `eval`
            (
                cd "$target_dir"
                echo "    python3 ../../generate.py --size $i --units $unit $line $acyclic $base1 $baseName $cba"
                eval python3 ../../generate.py --size "$i" --units "$unit" $line $acyclic $base1 $baseName $cba
            )
        done
    else
        echo "--> skipping"
    fi
done

echo "TOTAL:"
for dir in */; do
    echo "    For $dir"
    for file in "$dir/$dir_suffix/"*.csv; do
        if [ -f "$file" ]; then
            line_count=$(wc -l < "$file")
            echo "        File: $(basename "$file"), Lines: $line_count"
            head -n 3 "$file" | awk '{print "            "$0}'  # Indent each line for clarity
        fi
    done
done