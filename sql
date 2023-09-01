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

  from bs4 import BeautifulSoup

def extract_data_from_html(html):
    # Parse the HTML using BeautifulSoup
    soup = BeautifulSoup(html, 'html.parser')

    # Find the 'td' elements with the specified text and extract the data
    build_no = soup.find('td', text='build no').find_next('td').text.strip()
    build_url = soup.find('td', text='url').find_next('td').text.strip()

    return {
        'Build no': build_no,
        'Build url': build_url
    }

# Example usage:
html = """
<tr>
<td>build no</td>
<td>6464747</td>
</tr>
<tr>
<td>url</td>
<td>abc.com</td>
</tr>
"""


data = extract_data_from_html(html)
print("Build no:", data['Build no'])
print("Build url:", data['Build url'])


from bs4 import BeautifulSoup

def extract_data_from_html(html):
    # Parse the HTML using BeautifulSoup
    soup = BeautifulSoup(html, 'html.parser')

    # Find all <td> elements
    td_elements = soup.find_all('td')

    build_no = None
    build_url = None

    # Iterate through the <td> elements to find the desired data
    for i in range(len(td_elements)):
        if td_elements[i].text.strip() == 'build no':
            build_no = td_elements[i + 1].text.strip() if i + 1 < len(td_elements) else None
        elif td_elements[i].text.strip() == 'url':
            build_url = td_elements[i + 1].text.strip() if i + 1 < len(td_elements) else None

    return {
        'Build no': build_no,
        'Build url': build_url
    }

# Example usage:
html = """
<!-- ... Your HTML content here ... -->
"""

data = extract_data_from_html(html)
print("Build no:", data['Build no'])
print("Build url:", data['Build url'])



  



  def extract_data_from_html_file(file_path):
    try:
        with open(file_path, 'r') as file:
            html_content = file.read()

            # Check for the presence of '<td>build no</td>' and extract the data
            if '<td>build no</td>' in html_content:
                build_no_start = html_content.find('<td>build no</td>') + len('<td>build no</td>')
                build_no_end = html_content.find('</td>', build_no_start)
                build_no = html_content[build_no_start:build_no_end].strip()
            else:
                build_no = None

            # Check for the presence of '<td>url</td>' and extract the data
            if '<td>url</td>' in html_content:
                url_start = html_content.find('<td>url</td>') + len('<td>url</td>')
                url_end = html_content.find('</td>', url_start)
                build_url = html_content[url_start:url_end].strip()
            else:
                build_url = None

            return {
                'Build no': build_no,
                'Build url': build_url
            }

    except FileNotFoundError:
        return None

# Example usage:
file_path = 'path/to/your/html/file.html'
data = extract_data_from_html_file(file_path)

if data:
    print("Build no:", data['Build no'])
    print("Build url:", data['Build url'])
else:
    print("HTML file not found.")







  import re

def extract_data_from_html_file(file_path):
    try:
        with open(file_path, 'r') as file:
            html_content = file.read()

            # Use regular expressions to extract the data between <td> and </td> tags
            build_no_match = re.search(r'<td>build no<\/td>\s*<td>(.*?)<\/td>', html_content)
            build_url_match = re.search(r'<td>url<\/td>\s*<td>(.*?)<\/td>', html_content)

            build_no = build_no_match.group(1).strip() if build_no_match else None
            build_url = build_url_match.group(1).strip() if build_url_match else None

            return {
                'Build no': build_no,
                'Build url': build_url
            }

    except FileNotFoundError:
        return None

# Example usage:
file_path = 'path/to/your/html/file.html'
data = extract_data_from_html_file(file_path)

if data:
    print("Build no:", data['Build no'])
    print("Build url:", data['Build url'])
else:
    print("HTML file not found.")

  

