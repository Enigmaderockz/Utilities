# Check for null values in columns

import pandas as pd
import numpy as np
import argparse

# Parse command line arguments
parser = argparse.ArgumentParser()
parser.add_argument('--skip', help='Columns to skip', default='')
args = parser.parse_args()

# Convert the skip argument to a list of columns
skip_columns = args.skip.split(',')

# Read the CSV file
df = pd.read_csv('a.csv', delimiter='|')

# Replace empty strings with NaN
df.replace("", np.nan, inplace=True)

# Iterate over each column
for column in df.columns:
    if column in skip_columns:
        print(f'Null values in {column}: SKIPPED')
    else:
        # Find the null values
        null_values = df[column].isnull()

        # Get the line numbers (index + 2 because index starts from 0 and we have a header row)
        line_numbers = null_values[null_values == True].index + 2

        # Print the results
        if null_values.sum() > 0:
            print(f'Null values in {column}: {null_values.sum()} at line no {tuple(line_numbers)}')
        else:
            print(f'Null values in {column}: 0')

# Usage: python null.py --skip=Column4,Column2

# Executing macros in Teradata

import teradatasql
import csv

def execute_macro_and_return_csv(connection_string, macro_date, csv_file_path):
    # Connect to Teradata
    con = teradatasql.connect(connection_string)

    # Create a cursor
    cursor = con.cursor()

    # Execute the macro
    cursor.execute(f"EXEC MACRO('{macro_date}')")

    # Fetch the data
    data = cursor.fetchall()

    # Write the data to the CSV file
    with open(csv_file_path, 'w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerows(data)

    # Close the cursor and connection
    cursor.close()
    con.close()

    return csv_file_path

# Example usage
import teradatasql
import csv

def execute_macro_and_return_csv(connection_string, macro_date, csv_file_path):
    # Connect to Teradata
    con = teradatasql.connect(connection_string)

    # Create a cursor
    cursor = con.cursor()

    # Execute the macro
    cursor.execute(f"EXEC MACRO('{macro_date}')")

    # Fetch the data
    data = cursor.fetchall()
    total_rows = cursor.rowcount

    # Write the data to the CSV file
    with open(csv_file_path, 'w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        
        # Initialize progress variables
        rows_fetched = 0
        last_progress = 0
        
        for row in data:
            # Calculate progress and print real-time progress
            rows_fetched += 1
            progress = int(rows_fetched / total_rows * 100)
            if progress > last_progress:
                print(f"Progress: {progress}%")
                last_progress = progress
                
            writer.writerow(row)

    # Close the cursor and connection
    cursor.close()
    con.close()

    return csv_file_path

# fetch business date

formatted_date = f"{input_string.strip()[:4]}-{input_string[4:6]}-{input_string[6:8]}"
print(formatted_date)


# to fetch details from file

import re

# Read the file
with open('a.txt', 'r') as file:
    content = file.read()

# Extract values using regex
load_date_match = re.findall(r'\[INFO\]::LOAD_DATE\s*:\s*([^:\n]+)', content)
sql_script_match = re.findall(r'\[INFO\]::SQL_Script\s*:\s*([^:\n]+)', content)
output_file_match = re.findall(r'\[INFO\]::OUTPUT_FILE\s*:\s*([^:\n]+)', content)

# Get the last occurrences
load_date = load_date_match[-1].strip()
sql_script = sql_script_match[-1].strip()
output_file = output_file_match[-1].strip()

# Print the extracted values
print("LOAD_DATE:", load_date)
print("SQL_Script:", sql_script)
print("OUTPUT_FILE:", output_file)



#

# Write the data to the CSV file
    with open(csv_file_path, 'w', newline='', encoding='utf-8') as csv_file:
        for row in data:
            csv_file.write(custom_delimiter.join(map(str, row)) + "\n")




