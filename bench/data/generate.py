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

def generate_integer(max, min_value=0):
    """Generate a random integer between min_value and max."""
    return random.randint(min_value, max)

def generate_csv(filename, size, unit, columns, acyclic, baseId, baseName, cba):
    """Generate a CSV file with random data based on specified column types."""

    # Convert size to bytes based on unit
    if unit.lower() == 'gb':
        size_bytes = int(size * (1024 ** 3))
    elif unit.lower() == 'mb':
        size_bytes = int(size * (1024 ** 2))
    else:
        raise ValueError("Unsupported unit. Use 'GB' or 'MB'.")

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
        count = 0
        for _ in range(rows):
            row = []
            previous_integer = None  # Track the last generated integer for strictly increasing requirement
            for i, col_type in enumerate(col_types):
                if cba and col_names[i] == "y" and "term" in filename:
                    cell = random.choice(["Lit", "Var", "Abs", "App"])
                elif col_type == "varstring":
                    if baseName and random.random() < 0.0001:
                        cell = "Alice"
                    else:
                        cell = generate_varstring()
                elif col_type.startswith("string") and col_type[6:].isdigit():
                    cell = generate_string_n(int(col_type[6:]))
                elif col_type in {"int", "integer"}:
                    max_value = 99 if "shares" in filename else 1000000
                    if (acyclic or baseId) and previous_integer is None and random.random() < 0.0001:
                        cell = 1  # Set first column to "1" about 10% of the time
                    else:
                        min_value = previous_integer + 1 if acyclic and previous_integer is not None else 0
                        max_value = max_value if max_value > min_value else min_value + 1
                        cell = generate_integer(max_value, min_value)
                    # min_value = previous_integer + 1 if acyclic and previous_integer is not None else 0
                    # cell = generate_integer(max_value, min_value)
                    previous_integer = cell  # Update for the next integer column
                else:
                    raise ValueError(f"Unsupported column type: {col_type}")
                row.append(cell)
            writer.writerow(row)
            count += 1

    # Report approximate file size
    actual_size = os.path.getsize(filename)
    print(f"    --> Generated {filename}, Size: ~{actual_size / (1024**3):.2f} GB" if unit == 'gb' else f"    ---> Generated {filename}, Size: ~{actual_size / (1024**2):.2f} MB")

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
    parser.add_argument("--size", type=float, required=True, help="Approximate size of the output file.")
    parser.add_argument("--units", type=str, choices=['GB', 'MB'], required=True, help="Units for file size: 'GB' or 'MB'.")
    parser.add_argument("--columns", type=str, required=True, help="Column definitions in the format '(name,type) (name2,type2)'")
    parser.add_argument("--filename", type=str, required=True, help="Output filename.")
    parser.add_argument("--acyclic", action="store_true", help="Ensure integer columns are strictly increasing from left to right.")
    parser.add_argument("--base1", action="store_true", help="Ensure root 1 in graph")
    parser.add_argument("--baseName", action="store_true", help="Ensure root Alice in graph")
    parser.add_argument("--cba", action="store_true", help="Limit col to set")

    args = parser.parse_args()

    # Parse columns
    columns = parse_columns(args.columns)

    # Generate the CSV file
    generate_csv(args.filename, args.size, args.units, columns, args.acyclic, args.base1, args.baseName, args.cba)
