#!/bin/bash

# Check if a filename was provided
if [ -z "$1" ]; then
    echo "Usage: $0 filename"
    exit 1
fi

filename="$1"

# Check if the file exists
if [ ! -f "$filename" ]; then
    echo "File not found: $filename"
    exit 1
fi

# Determine OS and get file size in bytes
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    filesize=$(stat --format="%s" "$filename")
elif [[ "$OSTYPE" == "darwin"* ]]; then
    filesize=$(stat -f"%z" "$filename")
else
    echo "Unsupported OS: $OSTYPE"
    exit 1
fi

# Convert bytes to GB and print result
filesize_gb=$(echo "scale=2; $filesize / (1024^3)" | bc)
echo "Size of $filename: ${filesize_gb} GB"
