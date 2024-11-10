import os

# Set the target file size in bytes (1MB = 1048576 bytes)
TARGET_SIZE = 1048576  # Adjust this value as needed
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
