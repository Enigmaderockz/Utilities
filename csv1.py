import csv
import random
import string
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from multiprocessing import Manager
from datetime import datetime
import sys
import os
import time
import itertools

# Constants
KNOWN_AS_DT = "09/06/2024 00:00:00"
DATA_TYPE = "P"
NUMBER = 1559
TRUST_NAME = ""
ADDRESS_STREET = "800 Highway 400 . STE 240"
CITY = "Dawsonville"
STATE = "GA"
SSN = "454-54-6577"
NULL_COLUMNS = ["", "", "", ""]
DOB_FORMAT = "%Y/%m/%d"

# Random data generation functions
def generate_name():
    return ''.join(random.choices(string.ascii_uppercase + string.ascii_lowercase, k=5))

def generate_dob():
    start_date = datetime.strptime("1970/01/01", DOB_FORMAT)
    end_date = datetime.strptime("2000/12/31", DOB_FORMAT)
    random_date = start_date + (end_date - start_date) * random.random()
    return random_date.strftime(DOB_FORMAT)

def generate_customer_id():
    return random.randint(100, 999)

def generate_record():
    first_name = generate_name()
    last_name = generate_name()
    date_of_birth = generate_dob()
    customer_id = generate_customer_id()
    
    return [
        KNOWN_AS_DT, DATA_TYPE, NUMBER, 
        first_name, last_name, TRUST_NAME, 
        date_of_birth, ADDRESS_STREET, CITY, 
        STATE, SSN
    ] + NULL_COLUMNS + [customer_id, ""]

# More memory-efficient record generator using an iterator
def record_generator(num_records):
    for _ in range(num_records):
        yield generate_record()

# Write CSV data chunk using multi-threading
def write_data_chunk(output_file, data_chunk, chunk_id, headers=None):
    mode = 'a' if chunk_id > 0 else 'w'
    with open(output_file, mode, newline='') as file:
        writer = csv.writer(file, delimiter='|')
        if chunk_id == 0 and headers:
            writer.writerow(headers)  # Write headers for the first chunk
        writer.writerows(data_chunk)

# Parallel writing with thread pooling for performance
def write_chunks_concurrently(output_file, data_iterator, chunk_size, total_chunks, headers):
    with ThreadPoolExecutor() as writer_executor:
        futures = []
        for chunk_id in range(total_chunks):
            chunk = list(itertools.islice(data_iterator, chunk_size))  # Stream data
            futures.append(writer_executor.submit(write_data_chunk, output_file, chunk, chunk_id, headers if chunk_id == 0 else None))
        
        # Ensure all chunks are written
        for future in futures:
            future.result()

# Show progress while generating records
def show_progress(total_records, progress_dict):
    while True:
        time.sleep(1)  # Update every second
        processed_records = sum(progress_dict.values())
        progress = (processed_records / total_records) * 100
        print(f"Progress: {processed_records}/{total_records} records generated ({progress:.2f}%)", end='\r')
        if processed_records >= total_records:
            break

# Main function to handle data generation and writing
def generate_large_csv(total_records, output_file):
    chunk_size = 100000  # Adjust as needed
    num_chunks = total_records // chunk_size
    remaining_records = total_records % chunk_size

    # CSV Headers
    headers = [
        "#KNOWN_AS_DT", "DATA_TYPE", "NUMBER", "FIRST_NAME", "LAST_NAME", "TRUST_NAME", 
        "DATE_OF_BIRTH", "ADDRESS_STREET", "CITY", "STATE", "SSN", 
        "TRUST_INCORPORATE_DATE", "ENTITY_ADDRESS_STREET", "ENTITY_CITY", 
        "ENTITY_STATE", "ENTITY_ZIP_CODE", "CUSTOMER_ID", "TRUST_TAX_ID"
    ]

    # Manager for shared progress tracking
    with Manager() as manager:
        progress_dict = manager.dict()
        
        for i in range(num_chunks + (1 if remaining_records > 0 else 0)):
            progress_dict[i] = 0

        # Start progress display thread
        from threading import Thread
        progress_thread = Thread(target=show_progress, args=(total_records, progress_dict))
        progress_thread.start()

        # Process pool for data generation
        with ProcessPoolExecutor() as executor:
            # Submit tasks for parallel record generation
            futures = [
                executor.submit(record_generator, chunk_size) for _ in range(num_chunks)
            ]
            if remaining_records > 0:
                futures.append(executor.submit(record_generator, remaining_records))

            # Generate the data and write it using streaming and threading
            data_iterator = itertools.chain.from_iterable(f.result() for f in futures)
            write_chunks_concurrently(output_file, data_iterator, chunk_size, num_chunks + (1 if remaining_records > 0 else 0), headers)

        progress_thread.join()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python generate_large_csv.py <number_of_records>")
        sys.exit(1)

    total_records = int(sys.argv[1])
    output_file = 'large_output.csv'
    generate_large_csv(total_records, output_file)
