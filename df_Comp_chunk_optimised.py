import pandas as pd

data1 = {
    'A': [1, 's', '.8', 9, 10],
    'B': [10, 20, 30, 40, 50],
    'C': [100, 200, 300, 400, 500],
    'D': [1000, 2000, 3000, 4000, 5000],
    'E': [10000, 20000, 30000, 40000, 50000]
}

data2 = {
    'A': [1, 's', '0.8', 9, 10],
    'B': [10, 20, 300, 40, 50],
    'C': [100, 200, '', 400, 500],
    'D': [1000, 2000, 3000, 400, 5000],
    'E': [10000, 20000, 30000, 40000, 50000]
}

df1 = pd.DataFrame(data1)
df2 = pd.DataFrame(data2)

# Get the original column order
original_column_order = df1.columns.tolist()

# Get the common columns dynamically
common_columns = df1.columns.intersection(df2.columns)

# Check for differences in selected columns
df_diff = df1[common_columns].ne(df2[common_columns])

# Compare None values in corresponding columns
none_comparison = df1[common_columns].isnull() & df2[common_columns].isnull()

# Exclude rows where both have None in corresponding columns
df_diff = df_diff[~none_comparison]

# Filter the rows where there is at least one difference
any_diff = df_diff.any(axis=1)

# Add 'Row#' column
df1['Row#'] = range(1, len(df1) + 1)
df2['Row#'] = range(1, len(df2) + 1)

# Add 'Files' column
df1['Files'] = 'First'
df2['Files'] = 'Second'

# Add a column that lists the columns with differences for each row
df1['Columnsss'] = df_diff.apply(lambda row: '|'.join(row.index[row]), axis=1)
df2['Columnsss'] = df_diff.apply(lambda row: '|'.join(row.index[row]), axis=1)

df_diff_filtered = pd.concat([df1[any_diff], df2[any_diff]], ignore_index=True)

# Sort the final dataframe based on 'Row#' column
df_diff_sorted = df_diff_filtered.sort_values(by='Row#')

# Reset the index before writing to CSV
df_diff_sorted = df_diff_sorted.reset_index(drop=True)

# Reorder the columns in the final dataframe
df_diff_sorted = df_diff_sorted[['Files', 'Row#', 'Columnsss'] + [col for col in original_column_order if col in df_diff_sorted.columns]]

# Write the sorted differences to a CSV file
df_diff_sorted.to_csv('differences.csv', index=False)

# Print the number of records with differences
num_records_with_diff = len(df_diff_sorted)
print(f"Number of records with differences: {num_records_with_diff}")

if num_records_with_diff > 0:
    # List columns with differences (excluding 'Files' and 'Row#' columns)
    different_columns = df_diff.columns[df_diff.any()]
    different_columns = different_columns.difference(['Files'])

    print("Columns with differences:", ', '.join(different_columns))

    # Print the differences dataframe
    print("\nDifferences:\n", df_diff_sorted)
else:
    print("no diff")
