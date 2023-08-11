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
