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
connection_string = '<your_connection_string>'
macro_date = '2023-09-08'
csv_file_path = '<path_to_csv_file>'

csv_file_path = execute_macro_and_return_csv(connection_string, macro_date, csv_file_path)
print("Data has been successfully stored in the CSV file:", csv_file_path)

# macro related functions

# run macro data from SQL file

import re

with open("abc.sql", "r") as sql_file:
    sql_content = sql_file.read()

a = sql_content.strip()

match = re.search(r'EXEC\s+(\w+\.\w+)\s*\(', sql_content)
b = f"SHOW MACRO {match.group(1)}" if match else None

print("a =", a)
print("b =", b)

# fetch business date

formatted_date = f"{input_string.strip()[:4]}-{input_string[4:6]}-{input_string[6:8]}"
print(formatted_date)


