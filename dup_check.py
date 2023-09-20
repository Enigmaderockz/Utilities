import pandas as pd
import sys

# Check if at least one column name is provided as an argument
if len(sys.argv) < 3:
    print("Usage: python file.py filename col1,col2,...")
    sys.exit(1)

# Get the column names from command line arguments
columns_to_check = sys.argv[2].split(',')

# Read the file into a pandas DataFrame
try:
    df = pd.read_csv(sys.argv[1], sep='|', skipfooter=1, skiprows=1, engine='python', dtype=str)
except FileNotFoundError:
    print("File not found.")
    sys.exit(1)

# Find duplicates based on specified columns
duplicates = df[df.duplicated(columns_to_check, keep=False)]

if not duplicates.empty:
    # Count occurrences of duplicate rows
    duplicate_counts = duplicates.groupby(columns_to_check).size().reset_index(name='Occurrences')
    
    # Print duplicate rows and their occurrences
    for index, row in duplicate_counts.iterrows():
        duplicate_values = '|'.join(map(str, row[columns_to_check]))
        occurrences = row['Occurrences']
        print(f"{duplicate_values} - Occurrences {occurrences}")
else:
    print(f"No duplicate records found based on columns {', '.join(columns_to_check)}: 0")


# footer and header

import csv

file_path = "b_tmp.csv"

# Initialize variables
header = None
footer = None
total_rows = 0

with open(file_path, 'r') as file:
    # Create a CSV reader with a custom delimiter and quote character
    csv_reader = csv.reader(file, delimiter='|', quotechar='"')

    # Read the header (first line)
    header = next(csv_reader)

    # Read the rows and calculate total rows
    for row in csv_reader:
        total_rows += 1

        # Store the last row as the footer
        footer = row

# Extract the values from the header and footer
header_value = header[2] if len(header) > 2 else None
footer_value = footer[-1] if footer else None

# Print the results
print(f"Header Value (3rd column of the header): {header_value}")
print(f"Footer Value (Last column of the footer): {int(footer_value)}")
print(f"Total Rows (including header and footer): {total_rows + 2}")  # Adding 2 for header and footer

# Example output:
# Header Value (3rd column of the header): 2023-0909
# Footer Value (Last column of the footer): 00003
# Total Rows (including header and footer): 14

total_rows = sum(1 for _ in open("your_file.csv"))

