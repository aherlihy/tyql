import argparse
import random
import string
import csv
import os

def generate_varstring():
    """Generate a random variable-length string, simulating a name."""
    length = random.randint(3, 12)  # Random length between 3 and 12
    return ''.join(random.choices(string.ascii_letters, k=length))

def generate_string_n(length):
    """Generate a random fixed-length string of specified length."""
    return ''.join(random.choices(string.ascii_letters, k=length))

def generate_integer():
    """Generate a random integer between 0 and 1,000,000."""
    return random.randint(0, 1000000)

def generate_csv(filename, size_gb, columns):
    """Generate a CSV file with random data based on specified column types."""
    # Convert size from GB to bytes
    size_bytes = int(size_gb * (1024**3))

    # Extract column names and types
    col_names = [col[0] for col in columns]
    col_types = [col[1].lower() for col in columns]

    # Calculate approximate row size based on column types
    row_size_estimate = sum(
        8 if col_type == "varstring" else int(col_type[6:]) if col_type.startswith("string") else 7
        for col_type in col_types
    )

    # Determine the number of rows needed to approach target file size
    rows = size_bytes // row_size_estimate

    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)

        # Write header row
        writer.writerow(col_names)

        # Generate data rows
        for _ in range(rows):
            row = []
            for col_type in col_types:
                if col_type == "varstring":
                    cell = generate_varstring()
                elif col_type.startswith("string") and col_type[6:].isdigit():
                    cell = generate_string_n(int(col_type[6:]))
                elif col_type in {"int", "integer"}:
                    cell = generate_integer()
                else:
                    raise ValueError(f"Unsupported column type: {col_type}")
                row.append(cell)
            writer.writerow(row)

    # Report approximate file size
    actual_size = os.path.getsize(filename)
    print(f"Data generation complete. File: {filename}, Size: ~{actual_size / (1024**3):.2f} GB")

def parse_columns(columns_input):
    """Parse the columns input as a single string into a list of (name, type) tuples."""
    columns = []
    for col in columns_input.split(") ("):
        col = col.strip("()")
        name, col_type = col.split(",")
        columns.append((name.strip(), col_type.strip()))
    return columns

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic CSV data.")
    parser.add_argument("--size", type=float, required=True, help="Approximate size of the output file in GB (e.g., 0.5 for 0.5 GB).")
    parser.add_argument("--columns", type=str, required=True, help="Column definitions in the format '(name,type) (name2,type2)'")
    parser.add_argument("--filename", type=str, required=True, help="Output filename.")

    args = parser.parse_args()

    # Parse columns
    columns = parse_columns(args.columns)

    # Generate the CSV file
    generate_csv(args.filename, args.size, columns)
