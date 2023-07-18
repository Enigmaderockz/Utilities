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


