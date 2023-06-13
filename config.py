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
