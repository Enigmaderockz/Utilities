import re
from colorama import Fore, Style, init

# Initialize colorama
init(autoreset=True)

def search_file(filename):
    errors = []
    failures = []
    passes = []
    logs = []

    with open(filename, 'r') as file:
        for line in file:
            stripped_line = line.strip()
            if '/abc/def/sdfjsfhf/' in stripped_line:
                failures.append(stripped_line)
            elif '234/2424/24/24/2' in stripped_line:
                passes.append(stripped_line)
            elif re.search(r'\berr\b|\berror\b|\bfailure\b', stripped_line, re.IGNORECASE):
                errors.append(stripped_line)
            elif re.search(r'\b.log\b', stripped_line, re.IGNORECASE):
                logs.append(stripped_line)

    print('-------------------------------------------------------------------------------------------')
    print(f"\nResults for {filename}:\n")

    if errors:
        print(Fore.YELLOW + 'ERROR:')
        for error in errors:
            print(Fore.YELLOW + error)
    else:
        print('\nNo line contains any error or failures')

    if failures:
        print('\n')
        print(Fore.RED + 'FAILED:')
        for failure in failures:
            print(Fore.RED + failure)
    else:
        print('\nNo line contains old repository path')

    if passes:
        print('\n')
        print(Fore.GREEN + 'PASSED:')
        for passed in passes:
            print(Fore.GREEN + passed)
    else:
        print('\nNo line contains new repository path')

    if logs:
        print('\n')
        print(Fore.CYAN + 'Log filenames:')
        for log in logs:
            print(Fore.CYAN + log)
    else:
        print('\nNo line contains log file name')

    if not errors and not failures and not passes and not logs:
        print('\nNone found')

def search_multiple_files(filenames):
    for filename in filenames:
        search_file(filename)

# Example usage
filenames = ['a.txt', 'b.txt']
search_multiple_files(filenames)
