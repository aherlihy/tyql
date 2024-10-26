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
            existing_files+=("$file")
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

            # Find the first differing line between the files
            diff_result=$(find_first_difference "$out_dir/$file1" "$out_dir/$file2")
            if [ -n "$diff_result" ]; then
                all_equal=false
                differing_files+=("$file1|$file2|$diff_result")
            fi
        done
    done

    # Output results based on comparison results
    if $all_equal; then
        echo "    ok"
    else
        if [ ${#differing_files[@]} -eq 3 ]; then
            echo "    ALL FILES DIFFER IN $out_dir. Showing first differing line in each file:"
            for diff in "${differing_files[@]}"; do
                # Split the diff result using a custom delimiter
                file1="${diff%%|*}"; rest="${diff#*|}"
                file2="${rest%%|*}"; rest="${rest#*|}"
                line_number="${rest%%|*}"; rest="${rest#*|}"
                line1="${rest%%|*}"
                line2="${rest#*|}"

                echo "        $file1 (line $line_number): expected: $line1"
                echo "        $file2 (line $line_number): actual: $line2"
            done
        else
            # If only one file differs from two identical files
            for diff in "${differing_files[@]}"; do
                # Split the diff result using a custom delimiter
                file1="${diff%%|*}"; rest="${diff#*|}"
                file2="${rest%%|*}"; rest="${rest#*|}"
                line_number="${rest%%|*}"; rest="${rest#*|}"
                line1="${rest%%|*}"
                line2="${rest#*|}"

                echo "    $file2 DIFFERS FROM $file1 at line $line_number:"
                echo "        $file1 expected: $line1"
                echo "        $file2 actual: $line2"
            done
        fi
    fi
done
