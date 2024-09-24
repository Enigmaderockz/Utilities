# Lists of first and last names for masking
first_names = ["Emma", "Liam", "Olivia", "Noah", "Ava", "Sophia", "Isabella", "Mia", "Charlotte", "Amelia",
               "Harper", "Evelyn", "Abigail", "Emily", "Elizabeth", "Mila", "Ella", "Avery", "Sofia", "Camila",
               "Jackson", "Aiden", "Lucas", "Liam", "Noah", "Ethan", "Caden", "Logan", "Mason", "Oliver",
               "Elijah", "Grayson", "Jacob", "Michael", "Benjamin", "Carter", "Alexander", "James", "Jayden",
               "John", "Matthew", "David", "Joseph", "Daniel", "Henry", "Owen", "Wyatt", "Dylan", "Gabriel",
               "William", "Nathan", "Samuel", "Andrew", "Jack", "Anthony", "Christopher", "Joshua", "Jaxon",
               "Emily", "Chloe", "Grace", "Zoe", "Nora", "Hannah", "Lily", "Addison", "Aubrey", "Zoey",
               "Elizabeth", "Ella", "Mila", "Scarlett", "Victoria", "Lillian", "Camilla", "Layla", "Penelope",
               "Riley", "Aria", "Eleanor", "Hazel", "Aurora", "Lucy", "Audrey", "Bella", "Savannah", "Claire"]

last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
              "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson",
              "Martin", "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis",
              "Robinson", "Walker", "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill",
              "Flores", "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera", "Mitchell", "Carter", "Roberts",
              "Turner", "Phillips", "Campbell", "Parker", "Evans", "Edwards", "Collins", "Stewart", "Sanchez",
              "Morris", "Rogers", "Reed", "Cook", "Morgan", "Bell", "Murphy", "Bailey", "Cooper", "Richardson"]

columns_to_mask = {
        "ACCT": ("VARCHAR", 10, None),
        "GENDER": ("VARCHAR", 1, {"allowed_values": ["F", "M"]}),
        "ID1": ("INTEGER", 4, None),
        "ID2": ("INTEGER", None, None),
        "DECIMAL_COLUMN": ("DECIMAL", (5, 4), None),
        "DATE_COLUMN": ("DATE", None, None),
        "FIRST_NAME": ("VARCHAR", 8, None),
        "LAST_NAME": ("VARCHAR", 8, None),
        "ANY_NAME": ("VARCHAR", 16, {"separator": " "}),
        "ORG_NAME": ("VARCHAR", 206, {"separator": " "}),
        "FULL_NAME": ("VARCHAR", 45, None),
        "CAL": ("VARCHAR", 45, None),
        "SIN": ("VARCHAR", 5, None),
}



import pandas as pd

def compare_empty_dataframes(df1, df2):
    # Check if both DataFrames are empty and have the same columns
    return df1.empty and df2.empty and df1.columns.equals(df2.columns)

# Example usage
df1 = pd.DataFrame(columns=['A', 'B', 'C'])
df2 = pd.DataFrame(columns=['A', 'B', 'C'])

result = compare_empty_dataframes(df1, df2)

def print_result():
    if compare_empty_dataframes(df1, df2) == True:
        print("Both DataFrames are empty and have the same columns.")
    else:
        print("DataFrames are not empty or do not have the same columns.")

print_result()



import json
import glob

def combine_cucumber_jsons(input_files, output_file):
    combined_data = []

    for file in input_files:
        with open(file, 'r') as f:
            data = json.load(f)
            combined_data.extend(data)  # Append the data (features) from each JSON file

    # Write the combined data to the output file
    with open(output_file, 'w') as outfile:
        json.dump(combined_data, outfile, indent=4)

if __name__ == "__main__":
    # List of JSON files to combine
    input_files = glob.glob("*.json")  # You can modify this to your file list or use glob to find them

    # Output JSON file
    output_file = "combined_cucumber_report.json"

    # Combine the JSON files
    combine_cucumber_jsons(input_files, output_file)

    print(f"Combined JSON saved to {output_file}")

