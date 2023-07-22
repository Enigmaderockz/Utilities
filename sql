SELECT
  CASE
    WHEN max_date = '2023-04-19'
    THEN '2023-04-19'
    ELSE max_date
  END AS max_business_date
FROM
  (SELECT MAX(dt2_business) AS max_date FROM table) derived_table;
  
def generate_queries(table_names):
    query_template = "SELECT '{table_name}'; CASE WHEN MAX_DATE = DATE '2023-04-19' ELSE PICK FROM {table_name}"
    queries = [query_template.format(table_name=table_name) for table_name in table_names]
    return queries

table_names = ['ABC', 'XYZ', 'MNO']
queries = generate_queries(table_names)

for query in queries:
    print(query)

for file in *.txt.processed; do mv "$file" "${file%.processed}"; done


from pyhive import hive
import pandas as pd

# Establish the connection
conn = hive.Connection(host="your_host", port=10000, username="your_username", database="default")

# Define the query
query = "SELECT * FROM dl.rbg.refinery_table"

# Execute the query and read the result into a pandas DataFrame
df = pd.read_sql(query, conn)

# Save the DataFrame as a CSV file
df.to_csv('output.csv', index=False, sep=',')  # Change the separator if needed

#paralle processing
from pyhive import hive
import pandas as pd
import concurrent.futures

# Establish the connection
conn = hive.Connection(host="your_host", port=10000, username="your_username", database="default")

# Define the query
query = "SELECT * FROM dl.rbg.refinery_table"

# Define the number of threads to use for parallel processing
num_threads = 4  # Adjust this based on the available resources and desired concurrency

# Function to execute the query and return the result as a DataFrame
def execute_query(query):
    df = pd.read_sql(query, conn)
    return df

# Execute the query using parallel processing
with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
    # Submit the query execution tasks to the executor
    future = executor.submit(execute_query, query)

    # Retrieve the result from the future when it's ready
    df = future.result()

# Save the DataFrame as a CSV file
df.to_csv('output.csv', index=False, sep=',')  # Change the separator if needed

# Sorting without parallel processing
sorted_rows1 = sorted(list(rows1), key=lambda row: mixed_type_sort_key(row, sort_keys))
sorted_rows2 = sorted(list(rows2), key=lambda row: mixed_type_sort_key(row, sort_keys))

# Improved performance to write html differences
# Write the rows with differences to the HTML file
            
            for diff_row in diff_rows:
                for i in range(2):  # Iterate twice for both files
                    file_data = [(file1, diff_row[2]), (file2, diff_row[3])][i]
                    file_name, row_data = file_data

                    html += '<tr style="border: 1px solid black;">'

                    # Write the file name and row number
                    html += f'<td style="border: 1px solid black;">{file_name}</td>'
                    html += f'<td style="border: 1px solid black;">{diff_row[0]}</td>'

                    # Write the values of each column of the row
                    for col in header1:
                        cell_style = ''
                        cell_value = row_data.get(col)

                        if col in diff_row[1]:
                            cell_style = 'background-color: #ffcfbf;font-weight: bold;'

                        if pd.isnull(cell_value):
                            cell_value = 'NULL'

                        html += f'<td style="border: 1px solid black; {cell_style}">{cell_value}</td>'

                    html += '</tr>'

# Improved sorting

def parallel_sort_rows(rows, sort_keys=None):
    return sorted(list(rows), key=lambda row: mixed_type_sort_key(row, sort_keys))

# Parallel sorting using ThreadPoolExecutor
        with ThreadPoolExecutor() as executor:
            future1 = executor.submit(parallel_sort_rows, rows1, sort_keys)
            future2 = executor.submit(parallel_sort_rows, rows2, sort_keys)

            sorted_rows1 = future1.result()
            sorted_rows2 = future2.result()

# Unix way to fetch count of lines from csv

import subprocess

file_path = "f.csv"

# Get the line count using the 'wc' command
result = subprocess.run(["wc", "-l", file_path], capture_output=True, text=True)
line_count = int(result.stdout.split()[0])

# Print the line count
print("Number of lines in the CSV file:", line_count)

Windows way to fetch count of lines from csv

num_lines = sum(1 for line in open('a.csv'))
print(num_lines)

# Modifications to be done in iupgrade

ex_list = ['Transaction Date', 'Category']
compare_csv_files("a_tmp.csv", "b_tmp.csv", "n.html",exclude_keys=ex_list )

string_data = "A, B, C"
character_list = string_data.split(", ")
print(character_list)
