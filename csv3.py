import csv
import random
import string
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Manager
from datetime import datetime
import sys
import os
import time

# Constants for fixed data
KNOWN_AS_DT = "09/06/2024 00:00:00"
DATA_TYPE = "P"
TRUST_NAME = ""
ADDRESS_STREET = "800 Highway 400 . STE 240"
CITY = "Dawsonville"
STATE = "GA"
SSN = "454-54-6577"
NULL_COLUMNS = ["", "", "", ""]
DOB_FORMAT = "%Y/%m/%d"

# Generate random first name and last name with 5 alphabet characters
def generate_name():
    return ''.join(random.choices(string.ascii_uppercase + string.ascii_lowercase, k=5))

# Generate random date of birth between 1970/01/01 and 2000/12/31
def generate_dob():
    start_date = datetime.strptime("1970/01/01", DOB_FORMAT)
    end_date = datetime.strptime("2000/12/31", DOB_FORMAT)
    random_date = start_date + (end_date - start_date) * random.random()
    return random_date.strftime(DOB_FORMAT)

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

# Function to generate a chunk of data
def generate_data_chunk(chunk_size, progress_dict, process_id):
    chunk_data = []
    for _ in range(chunk_size):
        chunk_data.append(generate_record())
        progress_dict[process_id] += 1  # Update progress for this process
    return chunk_data

# Function to write data to CSV in chunks with pipe delimiter
def write_data_chunk(file_path, chunk_id, data_chunk, headers=None):
    mode = 'a' if chunk_id > 0 else 'w'
    with open(file_path, mode, newline='') as file:
        writer = csv.writer(file, delimiter='|')  # Changed delimiter to '|'
        if chunk_id == 0:
            writer.writerow(headers)  # Write headers only for the first chunk
        writer.writerows(data_chunk)

# Function to print progress
def show_progress(total_records, progress_dict):
    while True:
        time.sleep(1)  # Update every second
        processed_records = sum(progress_dict.values())
        progress = (processed_records / total_records) * 100
        print(f"Progress: {processed_records}/{total_records} records generated ({progress:.2f}%)", end='\r')
        if processed_records >= total_records:
            break

# Main function to generate and write the CSV file with parallel execution and progress
def generate_large_csv(total_records, output_file):
    chunk_size = 100000  # Process 100,000 records per chunk
    num_chunks = total_records // chunk_size
    remaining_records = total_records % chunk_size

    # CSV Headers
    headers = [
        "#KNOWN_AS_DT", "DATA_TYPE", "NUMBER", "FIRST_NAME", "LAST_NAME", "TRUST_NAME", 
        "DATE_OF_BIRTH", "ADDRESS_STREET", "CITY", "STATE", "SSN", 
        "TRUST_INCORPORATE_DATE", "ENTITY_ADDRESS_STREET", "ENTITY_CITY", 
        "ENTITY_STATE", "ENTITY_ZIP_CODE", "CUSTOMER_ID", "TRUST_TAX_ID"
    ]

    # Shared dictionary to track progress of each process
    with Manager() as manager:
        progress_dict = manager.dict()
        
        # Initialize the progress for each process to 0
        for i in range(num_chunks + (1 if remaining_records > 0 else 0)):
            progress_dict[i] = 0

        # Start the progress tracking in a separate thread
        from threading import Thread
        progress_thread = Thread(target=show_progress, args=(total_records, progress_dict))
        progress_thread.start()

        # Use ProcessPoolExecutor to generate data concurrently
        with ProcessPoolExecutor() as executor:
            futures = []

            # Submit chunked tasks for parallel generation
            for chunk_id in range(num_chunks):
                futures.append(executor.submit(generate_data_chunk, chunk_size, progress_dict, chunk_id))

            # Handle remaining records if any
            if remaining_records > 0:
                futures.append(executor.submit(generate_data_chunk, remaining_records, progress_dict, num_chunks))

            # Write each chunk as soon as it is completed
            for chunk_id, future in enumerate(futures):
                data_chunk = future.result()
                write_data_chunk(output_file, chunk_id, data_chunk, headers)
        
        # Wait for progress thread to finish
        progress_thread.join()

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
