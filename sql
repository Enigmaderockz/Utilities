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
for file in *_20230405.txt; do mv "$file" "${file/_20230405.txt/_20230909.txt}"; done


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

import re

def alphanumeric_key(value):
    parts = re.split(r'(\d+)', value)  # Split into parts of numbers and strings
    return [(int(part) if part.isdigit() else part) for part in parts]

def mixed_type_sort_key(row, keys=None):
    if keys is None:
        keys = row.keys()  # Use all keys if none are specified

    sort_key = []
    for key in keys:
        value = row[key]
        if isinstance(value, str):
            sort_key.append((1, alphanumeric_key(value)))
        else:
            sort_key.append((0, value))
    return tuple(sort_key)

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

# One more way

def parallel_sort_rows(rows, sort_keys=None):
    # Number of parallel threads (adjust according to your system)
    num_threads = 4

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        future = executor.submit(sorted, rows, key=lambda row: mixed_type_sort_key(row, sort_keys))
        sorted_rows = future.result()

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


import re

get_processed_value = lambda s: re.sub(r'^.*\/(.+)_\d{8}\.txt$', lambda match: match.group(1).lower(), s)

input_string = "/sfsf/sgs/gs/gs/gs/hd/shd/hd/hdh/ABC_DEf_mehan_sfsf_fsfs_rwrw_wrwr_20230456.txt"
result = get_processed_value(input_string)

  with open(file_path, 'r') as file:
    for line_number, line in enumerate(file, start=1):
        delimiter_count = line.count('|')
        print(f"{line_number} line: {delimiter_count} count")

  SELECT column_name
FROM table_name
WHERE REGEXP_SIMILAR(column_name, '[A-Za-z]', 'i') = 1;

import requests
import json

# Set the TeamCity server URL and the build ID
teamcity_server_url = "https://teamcity.example.com"
build_id = 12345

# Create a request object and set the headers and the URL
request = requests.get(
    url=f"{teamcity_server_url}/app/rest/builds?locator=id:{build_id}",
    headers={"Accept": "application/json"},
)

# Send the request and get the response
response = request.json()

# Parse the response JSON and get the duration of the build
duration = response["build"]["duration"]

# Print the duration of the build
print(duration)


output_string = f"{int(num_tests)} tests ran in {minutes:.2f} minutes"

print(output_string)

  # chatgpt

  import requests

# Define your TeamCity server URL and build ID
teamcity_url = "https://your-teamcity-server-url"
build_id = "12345"  # Replace with the actual build ID

# Set your username and password for authentication
username = "your_username"
password = "your_password"

# Create a session with authentication
session = requests.Session()
session.auth = (username, password)

# Make the API request to get build details
response = session.get(f"{teamcity_url}/httpAuth/app/rest/builds/{build_id}")

if response.status_code == 200:
    build_data = response.json()
    build_duration = build_data['duration']
    print(f"Build {build_id} duration: {build_duration} seconds")
else:
    print(f"Failed to retrieve build details. Status code: {response.status_code}")



  import requests
import json

# Set the TeamCity server URL and the build ID
teamcity_server_url = "https://teamcity.example.com"
build_id = 12345

# Create a request object and set the headers and the URL
request = requests.get(
    url=f"{teamcity_server_url}/app/rest/builds?locator=id:{build_id}",
    headers={"Accept": "application/json"},
)

# Send the request and get the response
response = request.json()

# Parse the response JSON and get the duration of the build
duration = response["build"]["duration"]

# Print the duration of the build
print(duration)

  find /v/incoming -type f -name "*.gz" -delete 

  #!/bin/bash

# Get the start time
start=$(date +%s)

# Sleep for 30 seconds
sleep 30

# Get the end time
end=$(date +%s)

# Calculate the difference in minutes
diff=$((end - start) / 60)

# Print the execution time
echo "Execution time took $diff minutes"

# Pass the difference to the Python script
python filename.py $diff

for filename in *.dat; do echo "$filename $(grep -E '[:special:]' "$filename" | wc -l)"; done

  issueFunction in linkedIssuesOf("issueType = TestSet AND key = YOUR_TEST_SET_KEY", "isTestedBy") 


  
awk -F',' 'NR==1 { for (i=1; i<=NF; i++) if ($i == "ColumnName") col=i } NR>1 { if (!seen[$col]++) print $col }' data.csv

  result = ', '.join(["'{}'".format(value) for value in values])




