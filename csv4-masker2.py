import csv
import random
import string
from concurrent.futures import ProcessPoolExecutor
from threading import Thread, Lock
from datetime import datetime
import sys
import time
import os

# Constants for fixed data
KNOWN_AS_DT = "09/06/2024 00:00:00"
DATA_TYPE = "P"
NUMBER = 1559
TRUST_NAME = ""
ADDRESS_STREET = "800 Highway 400 . STE 240"
CITY = "Dawsonville"
STATE = "GA"
SSN = "454-54-6577"
NULL_COLUMNS = ["", "", "", "", ""]
DOB_FORMAT = "%Y/%m/%d"

# Thread-safe counter and lock
record_count = 0
lock = Lock()

# Generate random first name and last name with 5 alphabet characters
def generate_name():
    return ''.join(random.choices(string.ascii_uppercase + string.ascii_lowercase, k=5))

# Generate random date of birth between 1970/01/01 and 2000/12/31
def generate_dob():
    start_date = datetime.strptime("1970/01/01", DOB_FORMAT)
    end_date = datetime.strptime("2000/12/31", DOB_FORMAT)
    random_date = start_date + (end_date - start_date) * random.random()
    return random_date.strftime(DOB_FORMAT)

# Generic function to generate a integer number within a specified range
def generate_numbers_in_range(min_value, max_value):
    return random.randint(min_value, max_value)

# Generate a record based on the format provided
def generate_record():
    number = generate_numbers_in_range(1000, 9999)
    first_name = generate_name()
    last_name = generate_name()
    date_of_birth = generate_dob()
    customer_id = generate_numbers_in_range(100, 999)
    
    return [
        KNOWN_AS_DT, DATA_TYPE, number, 
        first_name, last_name, TRUST_NAME, 
        date_of_birth, ADDRESS_STREET, CITY, 
        STATE, SSN
    ] + NULL_COLUMNS + [customer_id, ""]

# Function to write a batch of records to CSV using a generator
def generate_and_write_batch(file_path, batch_size, headers=None, chunk_id=0):
    mode = 'a' if chunk_id > 0 else 'w'
    with open(file_path, mode, newline='') as file:
        writer = csv.writer(file, delimiter='|')
        if chunk_id == 0 and headers:
            writer.writerow(headers)  # Write headers only for the first chunk

        for _ in range(batch_size):
            record = generate_record()
            writer.writerow(record)

            # Update the global record count in a thread-safe way
            with lock:
                global record_count
                record_count += 1

# Function to show progress in a separate thread
def show_progress(total_records):
    while record_count < total_records:
        time.sleep(1)  # Update every second
        with lock:
            progress = (record_count / total_records) * 100
            print(f"Progress: {record_count}/{total_records} records generated ({progress:.2f}%)", end='\r')

# Function to count records in the generated CSV file
def count_records_in_file(file_path):
    with open(file_path, 'r') as file:
        reader = csv.reader(file, delimiter='|')
        next(reader)  # Skip the header
        return sum(1 for _ in reader)

# Main function to generate and write the CSV file with parallel execution and progress
def generate_large_csv(total_records, output_file):
    batch_size = 10000  # Write 10,000 records per batch to reduce memory load
    num_batches = total_records // batch_size
    remaining_records = total_records % batch_size

    # CSV Headers
    headers = [
        "#KNOWN_AS_DT", "DATA_TYPE", "NUMBER", "FIRST_NAME", "LAST_NAME", "TRUST_NAME", 
        "DATE_OF_BIRTH", "ADDRESS_STREET", "CITY", "STATE", "SSN", 
        "TRUST_INCORPORATE_DATE", "ENTITY_ADDRESS_STREET", "ENTITY_CITY", 
        "ENTITY_STATE", "ENTITY_ZIP_CODE", "CUSTOMER_ID", "TRUST_TAX_ID"
    ]

    # Start progress tracking in a separate thread
    progress_thread = Thread(target=show_progress, args=(total_records,))
    progress_thread.start()

    # Use ProcessPoolExecutor to generate and write records concurrently
    with ProcessPoolExecutor() as executor:
        futures = []

        # Submit tasks to generate and write records in parallel
        for batch_id in range(num_batches):
            futures.append(executor.submit(generate_and_write_batch, output_file, batch_size, headers if batch_id == 0 else None, batch_id))

        # Handle remaining records if any
        if remaining_records > 0:
            futures.append(executor.submit(generate_and_write_batch, output_file, remaining_records, None, num_batches))

        # Wait for all futures to complete
        for future in futures:
            future.result()

    # Wait for the progress thread to finish
    progress_thread.join()

    # Count the records in the file
    file_record_count = count_records_in_file(output_file)

    # Check if the number of records in the file matches the requested number
    if file_record_count == total_records:
        print(f"\nProgress: 100% complete. {file_record_count}/{total_records} records successfully written.")
    else:
        print(f"\nMismatch in record count! {file_record_count}/{total_records} records written.")
    
if __name__ == "__main__":
    # Get total number of records from system argument
    if len(sys.argv) != 2:
        print("Usage: python generate_large_csv.py <number_of_records>")
        sys.exit(1)

    total_records = int(sys.argv[1])

    # Output file path
    output_file = 'large_output.csv'

    # Call the main function to generate the CSV
    generate_large_csv(total_records, output_file)
