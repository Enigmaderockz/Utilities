import pandas as pd

def excel_to_pipe_delimited_csv(excel_file_path, csv_file_path):
    # Read Excel file into a pandas DataFrame
    df = pd.read_excel(excel_file_path)

    # Save DataFrame to pipe-delimited CSV
    df.to_csv(csv_file_path, sep='|', index=False)

# Example usage
excel_file_path = 'orig.xlsx'
csv_file_path = 'orig.csv'

excel_to_pipe_delimited_csv(excel_file_path, csv_file_path)

grep "[^[:alnum:]._-]" "filename.txt"

grep -P "[^\p{ASCII}]"
