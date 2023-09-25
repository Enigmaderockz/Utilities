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

def compare_lists(list1, list2):
    set1 = set(list1)
    set2 = set(list2)

    if set1 == set2:
      print("both the feeds have same loans")
      return True
    else:
        extra_values = set2 - set1
        missing_values = set1 - set2

        if extra_values:
            print("List 2 has extra values:", ', '.join(map(str, extra_values)))
        if missing_values:
            print("List 1 has missing values:", ', '.join(map(str, missing_values)))

        return False

list1 = [45, 65, 73, 54]
list2 = [45, 65, 73, 54]


result = compare_lists(list1, list2)
print("Result:", result)


################################################3

import pandas as pd

# Read the data from a.dat and b.dat into pandas DataFrames
df_a = pd.read_csv('a.dat')
df_b = pd.read_csv('b.dat')

# Extract the 'IDN_CUST' column from both DataFrames
idn_cust_a = set(df_a['IDN_CUST'])
idn_cust_b = set(df_b['IDN_CUST'])

# Find missing values in a.dat compared to b.dat
missing_values = idn_cust_a - idn_cust_b

if missing_values:
    print("Values in a.dat that are missing in b.dat:")
    for value in missing_values:
        print(value)
else:
    print("No missing values in a.dat compared to b.dat")

#######################################33333333333


import csv
from collections import defaultdict

def process_value(value):
    if value == '':
        return 'null'
    elif ' ' in value:
        return 'blank_space'
    else:
        return value

def distinct_values_in_columns(filename, delimiter=','):
    distinct_values = defaultdict(set)

    with open(filename, 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile, delimiter=delimiter)
        header = next(reader)  # Read the header row

        for row in reader:
            for col_num, value in enumerate(row):
                distinct_values[header[col_num]].add(process_value(value))

    for col_name, values in distinct_values.items():
        distinct_values_str = ', '.join(sorted(values))
        print(f"Distinct values in {col_name}: {distinct_values_str}")

if __name__ == "__main__":
    filename = "a.csv"  # Replace with your CSV file path
    distinct_values_in_columns(filename)
