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

##########################33

def calculate_column_sum(filename, column_name):
    try:
        # Initialize the sum
        column_sum = 0.0

        # Open the file for reading
        with open(filename, 'r') as file:
            # Read the header row to get column indexes
            header = next(file)
            columns = header.strip().split('|')

            # Find the index of the specified column
            try:
                column_index = columns.index(column_name)
            except ValueError:
                print(f"Column '{column_name}' not found in the file.")
                return None

            # Read each subsequent line in the file
            for line in file:
                # Split the line by the pipe delimiter
                columns = line.strip().split('|')

                # Get the value in the specified column and convert it to a float (considering null as 0.00)
                try:
                    column_value = float(columns[column_index]) if columns[column_index] else 0.00
                except ValueError:
                    print(f"Invalid value in column '{column_name}' on line: {line}")
                    continue

                # Add the value to the sum
                column_sum += column_value

        # Format the sum to two decimal places
        formatted_sum = round(column_sum, 2)

        return formatted_sum

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return None

# Example usage:
filename = "data.dat"
column_name = "col1"
result = calculate_column_sum(filename, column_name)
if result is not None:
    print(f"Sum of values in '{column_name}' column: {result}")

#####################################

import os
import shutil
import sys

# Check if the correct number of command-line arguments are provided
if len(sys.argv) != 2:
    print("Usage: python copy_files.py <business_date>")
    sys.exit(1)

# Get the business date from the command-line argument
business_date = sys.argv[1]

# Define a dictionary mapping keywords to destination paths
keyword_paths = {
    "CREDIT_PSL": "/abc/var/tmp/credit",
    "RGBDW_RWMS": "/abc/var/tmp/rwms",
    "PSL_FTP": "/abc/var/tmp/ftp",
    "PSL_FDW": "/abc/var/tmp/fdw",
    "WM_PSL_CLTENT_REF": "/abc/var/tmp/fdw",
    "PSL_LTIQUIDITY_RISK": "/abc/var/tmp/liq",
}

# Define the directory containing the files
directory = "/var/tmp/col"

# Iterate over files in the directory
for filename in os.listdir(directory):
    # Check if the file starts with any of the keywords
    for keyword, destination_path in keyword_paths.items():
        if filename.startswith(keyword) and business_date in filename:
            # Construct the source and destination paths
            source_path = os.path.join(directory, filename)
            destination_path = os.path.join(destination_path, filename)

            # Copy the file to the destination
            shutil.copy(source_path, destination_path)

            # Print a message indicating the file was copied
            print(f"File {filename} copied to {destination_path}")

print("Copy operation completed.")



if __name__ == "__main__":
    filename = "a.csv"  # Replace with your CSV file path
    distinct_values_in_columns(filename)

########################################33


import csv
from collections import defaultdict

# Define the keywords
keywords = ['45678901234', '9876543210', '43210987654']

# Initialize a dictionary to store distinct values for each column
distinct_values = defaultdict(set)

# Open and read the input file
with open('your_file.csv', 'r') as csv_file:
    reader = csv.DictReader(csv_file, delimiter='|')

    # Initialize a list to store the filtered rows
    filtered_rows = []

    for row in reader:
        for keyword in keywords:
            if any(keyword in value for value in row.values()):
                filtered_rows.append(row)
                for col, value in row.items():
                    if isinstance(value, list):
                        value = ','.join(value)  # Convert list to a string
                    distinct_values[col].add(value if value else 'null')

# Print the header
header = '|'.join(reader.fieldnames)
print(header)

# Print the filtered rows
for row in filtered_rows:
    row_values = '|'.join(row[field] for field in reader.fieldnames)
    print(row_values)

# Print distinct values for each column
for col, values in distinct_values.items():
    values_str = ','.join(values)
    print(f"{col} = {values_str}")


