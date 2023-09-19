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
