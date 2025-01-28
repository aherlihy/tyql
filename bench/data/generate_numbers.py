import os
import argparse

# Parse CLI arguments
parser = argparse.ArgumentParser(description="Generate a CSV file of incrementing integer up to a specified size.")
parser.add_argument("target_mbs", type=int, help="Target size of the file in megabytes (MB).")
args = parser.parse_args()

# Set the target file size in bytes (1MB = 1048576 bytes)
TARGET_SIZE = args.target_mbs * 1048576
OUTPUT_FILE = "numbers.csv"

# Start by writing the header
with open(OUTPUT_FILE, 'w') as f:
    f.write("id,value\n")

# Initialize counters
id = 1
value = 0

# Keep appending rows until the file reaches or exceeds the target size
while os.path.getsize(OUTPUT_FILE) < TARGET_SIZE:
    with open(OUTPUT_FILE, 'a') as f:
        f.write(f"{id},{value}\n")
    id += 1
    value += 1

print(f"File generated: {OUTPUT_FILE} (Target size: {TARGET_SIZE} bytes)")
