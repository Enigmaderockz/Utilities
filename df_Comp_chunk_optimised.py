import pandas as pd

# Add a column that shows the actual difference for each column
'''
def calculate_difference(row1, row2):
    differences = []
    for x, y in zip(row1, row2):
        if pd.isnull(x) and pd.notnull(y):
            differences.append(f"NULL -> {y}")
        elif pd.notnull(x) and pd.isnull(y):
            differences.append(f"{x} -> NULL")
        elif pd.notnull(x) and pd.notnull(y) and x != y:
            differences.append(f"{x} -> {y}")
    return ', '.join(differences)

'''
def calculate_difference(row1, row2):
    differences = []
    for x, y in zip(row1, row2):
        if pd.isnull(x) and pd.notnull(y):
            differences.append(f"Missing in First: {y}")
        elif pd.notnull(x) and pd.isnull(y):
            differences.append(f"Missing in Second: {x}")
        elif pd.notnull(x) and pd.notnull(y) and x != y:
            differences.append(f"{x} -> {y}")
    return ', '.join(differences)


data1 = {
    'A': [1, 's', '.8', 9, 10],
    'B': [10, 20, 30, 40, 50],
    'C': [100, 200, 300, 400, 500],
    'D': [1000, 2000, 3000, 4000, 5000],
    'E': [10000, 20000, 30000, 40000, 50000]
}

data2 = { 
    'A': [1, '  ', '0.8', 9, 10],
    'B': [10, 20, 300, 40, 50],
    'C': [100, 200, '', 400, 500],
    'D': [1000, 2000, 3000, 400, 5000],
    'E': [10000, 20000, 30000, 40000, 50000]
}

df1 = pd.DataFrame(data1)
print(df1)
df2 = pd.DataFrame(data2)
print(df2)

# Get the original column order
original_column_order = df1.columns.tolist()

# Set the chunk size for processing
chunksize = 1000  # Adjust this based on your memory limitations

# Initialize an empty DataFrame to store differences
diff_chunks = []

# Calculate the number of chunks needed
num_chunks = len(df1) // chunksize + 1

# Iterate through the chunks
for chunk_idx in range(num_chunks):
    # Calculate the start and end indices for the current chunk
    start_idx = chunk_idx * chunksize
    end_idx = min((chunk_idx + 1) * chunksize, len(df1))

    # Get the current chunk from both dataframes
    chunk1 = df1.iloc[start_idx:end_idx].copy()  # Use .copy() to avoid SettingWithCopyWarning
    chunk2 = df2.iloc[start_idx:end_idx].copy()  # Use .copy() to avoid SettingWithCopyWarning

    # Add 'Row#' column
    chunk1['Row#'] = range(start_idx + 1, end_idx + 1)
    chunk2['Row#'] = range(start_idx + 1, end_idx + 1)

    # Add 'Files' column
    chunk1['Files'] = 'First'
    chunk2['Files'] = 'Second'

    # Get the common columns dynamically
    common_columns = chunk1.columns.intersection(chunk2.columns)

    # Exclude 'Row#' and 'Files' columns for comparison
    columns_to_compare = common_columns.difference(['Row#', 'Files'])

    # Check for differences in selected columns
    chunk_diff = chunk1[columns_to_compare].ne(chunk2[columns_to_compare])

    # Compare None values in corresponding columns
    none_comparison = (chunk1[columns_to_compare].isnull()) & (chunk2[columns_to_compare].isnull())

    # Exclude rows where both have None in corresponding columns
    chunk_diff = chunk_diff[~none_comparison]

    # Filter the rows where there is at least one difference
    any_diff = chunk_diff.any(axis=1)

    # Add a column that lists the columns with differences for each row
    chunk1['Columnsss'] = chunk_diff.apply(lambda row: ','.join(row.index[row]), axis=1)
    chunk2['Columnsss'] = chunk_diff.apply(lambda row: ','.join(row.index[row]), axis=1)

    # Add a column that shows the actual difference for each row
    chunk1['Difference'] = chunk1[columns_to_compare].apply(lambda row: calculate_difference(row, chunk2.loc[row.name, columns_to_compare]), axis=1)
    chunk2['Difference'] = chunk2[columns_to_compare].apply(lambda row: calculate_difference(row, chunk1.loc[row.name, columns_to_compare]), axis=1)
    
    # Add a column to indicate in which file the data is missing
    chunk1['Missing in'] = chunk1[columns_to_compare].isnull().apply(lambda row: 'Second' if any(row) else '', axis=1)
    chunk2['Missing in'] = chunk2[columns_to_compare].isnull().apply(lambda row: 'First' if any(row) else '', axis=1)

    # Add a column to indicate whether the difference is due to a value change or missing data
    chunk1['Difference Type'] = chunk1['Missing in'].apply(lambda x: 'Missing Data' if x else 'Value Change')
    chunk2['Difference Type'] = chunk2['Missing in'].apply(lambda x: 'Missing Data' if x else 'Value Change')
    
    chunk_diff_filtered = pd.concat([chunk1[any_diff], chunk2[any_diff]], ignore_index=True)

    # Add the chunk's differences to the list
    diff_chunks.append(chunk_diff_filtered)

# Concatenate all chunks' differences into the final DataFrame
df_diff_sorted = pd.concat(diff_chunks, ignore_index=True)

# Sort the final dataframe based on 'Row#' column
df_diff_sorted = df_diff_sorted.sort_values(by='Row#')

# Reset the index before writing to CSV
df_diff_sorted = df_diff_sorted.reset_index(drop=True)

# Reorder the columns in the final dataframe
df_diff_sorted = df_diff_sorted[['Files', 'Row#', 'Columnsss', 'Difference Type', 'Difference'] + [col for col in original_column_order if col in df_diff_sorted.columns]]

# Write the sorted differences to a CSV file
df_diff_sorted.to_csv('differences.csv', index=False, sep='|')

# Print the number of records with differences
num_records_with_diff = len(df_diff_sorted)
print(f"Number of records with differences: {num_records_with_diff}")

if num_records_with_diff > 0:
    # List columns with differences (excluding 'Files' and 'Row#' columns)
    different_columns = chunk_diff.columns[chunk_diff.any()]
    different_columns = different_columns.difference(['Files'])

    print("Columns with differences:", ', '.join(different_columns))

    # Print the differences dataframe
    print("\nDifferences:\n", df_diff_sorted)
else:
    print("no diff")
