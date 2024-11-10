#!/bin/bash

# Function to find the first differing line between two files
find_first_difference() {
    file1="$1"
    file2="$2"
    line_number=1
    while true; do
        line1=$(sed -n "${line_number}p" "$file1" 2>/dev/null)
        line2=$(sed -n "${line_number}p" "$file2" 2>/dev/null)

        # If both lines are empty, we've reached the end of both files without differences
        if [ -z "$line1" ] && [ -z "$line2" ]; then
            break
        fi

        # If lines differ, output line number and content
        if [ "$line1" != "$line2" ]; then
            echo "$line_number|$line1|$line2"
            return
        fi

        ((line_number++))
    done
}

# Loop through all directories in the current directory
for dir in */; do
    # Check if the subdirectory has an 'out' directory
    out_dir="${dir}out"
    if [ ! -d "$out_dir" ]; then
        echo "$out_dir does not exist. Skipping."
        continue
    fi

    echo "Checking directory: $out_dir"

    # Define the expected files
    files=("collections.csv" "scalasql.csv" "tyql.csv")
    missing_files=()
    existing_files=()

    # Check for the presence of each expected file
    for file in "${files[@]}"; do
        if [ -f "$out_dir/$file" ]; then
            line_count=$(wc -l < "$out_dir/$file")
            existing_files+=("$file")
            # Check if the line count is 1
            if [[ "$line_count" -eq 1 ]]; then
              echo "**Error: $file only has one line."
            fi
        else
            missing_files+=("$file")
        fi


    done

    # Report any missing files
    if [ ${#missing_files[@]} -gt 0 ]; then
        echo "    *Missing files in $out_dir: ${missing_files[*]}"
    fi


    # If fewer than two files exist, skip further comparisons
    if [ ${#existing_files[@]} -lt 2 ]; then
        continue
    fi

    # Initialize variables for comparison results
    all_equal=true
    differing_files=()


    # Compare files in pairs
    for ((i = 0; i < ${#existing_files[@]}; i++)); do
        for ((j = i + 1; j < ${#existing_files[@]}; j++)); do
            file1="${existing_files[i]}"
            file2="${existing_files[j]}"
	    if diff -q "$out_dir/$file1" "$out_dir/$file2" > /dev/null; then
		echo "    ok: $file1 $file2 same"
	       continue  # Files are identical; skip further checks
	    fi
	    echo "  **diff NOT the same between $file1 and $file2"

	    diff_count=$(diff -U 0 "$out_dir/$file1" "$out_dir/$file2" | grep -c '^@') 
            echo "  =$diff_count #"
	   # Find the first differing line between the files
           # diff_result=$(find_first_difference "$out_dir/$file1" "$out_dir/$file2")
           #  if [ -n "$diff_result" ]; then
           #     all_equal=false
           #     differing_files+=("$file1|$file2|$diff_result")
           # fi
        done
    done
done
