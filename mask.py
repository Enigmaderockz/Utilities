import pandas as pd
import random
import string
from datetime import datetime, timedelta
from config import first_names, last_names  # Import the lists from the config file


class DataMasker:
    def __init__(self):
        self.first_names = first_names
        self.last_names = last_names

    def random_decimal(self, precision, scale):
        integer_part = random.randint(0, 10 ** (precision - scale) - 1)
        decimal_part = random.randint(10 ** (scale - 1), 10**scale - 1)
        return float(f"{integer_part}.{decimal_part}")

    def random_date(self, start_date="1900-01-01", end_date="2099-12-31"):
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        random_date = start + timedelta(days=random.randint(0, (end - start).days))
        return random_date.strftime("%Y-%m-%d")

    def mask_account_number(
        self, column_name, account_number, data_type, length, extra_params=None
    ):
        if extra_params is None:
            extra_params = {}

        column_name = column_name.upper()

        if data_type.upper() in ["CHAR", "VARCHAR"]:
            account_number = str(account_number)
            if column_name == "GENDER":
                allowed_values = extra_params.get("allowed_values")
                masked_number = random.choice(allowed_values)
            elif column_name == "FIRST_NAME":
                masked_number = random.choice(self.first_names)
            elif column_name == "LAST_NAME":
                masked_number = random.choice(self.last_names)
            elif column_name in ["ANY_NAME", "ORG_NAME"]:
                separator = extra_params.get("separator", " ")
                first_name = random.choice(self.first_names)
                last_name = random.choice(self.last_names)
                masked_number = f"{first_name}{separator}{last_name}"
            elif column_name == "ACCT":                                                     # For masking data with fixed length for VARCHAR type
                if len(account_number) == length:
                    masked_number = "".join(
                        random.choices(string.ascii_uppercase + string.digits, k=length)
                    )
                else:
                    masked_number = account_number
            elif column_name == "SIN":                                                      # For masking data in numbers only for VARCHAR type
                masked_number = "".join(random.choices(string.digits, k=length))
            else:                                                                           # For masking data with random length in VARCHAR
                masked_number = "".join(
                    random.choices(string.ascii_uppercase + string.digits, k=length)
                )
        elif data_type.upper() == "DECIMAL":
            precision, scale = length
            masked_number = self.random_decimal(precision, scale)
        elif data_type.upper() == "DATE":
            masked_number = self.random_date()
        elif data_type.upper() == "INTEGER":
            if length is not None:
                min_value = 10 ** (length - 1)
                max_value = 10**length - 1
            else:
                min_value = extra_params.get("min_value", 0)
                max_value = extra_params.get("max_value", 2**31 - 1)
            masked_number = random.randint(min_value, max_value)
        else:
            masked_number = account_number
        return masked_number

    def mask_csv(self, input_file, output_file, columns_to_mask):
        df = pd.read_csv(input_file, sep="|")

        for column_name, (data_type, length, extra_params) in columns_to_mask.items():
            if column_name != "FULL_NAME" and column_name in df.columns:
                print(f"Masking data in column: {column_name}")
                df[column_name] = df[column_name].apply(
                    lambda x: self.mask_account_number(
                        column_name, x, data_type, length, extra_params
                    )
                )

        if "FULL_NAME" in df.columns:
            print("Creating FULL_NAME column")
            df["FULL_NAME"] = df["FIRST_NAME"] + " " + df["LAST_NAME"]

        df.to_csv(output_file, index=False, sep="|")


if __name__ == "__main__":
    input_file = "input.csv"
    output_file = "output.csv"

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

    data_masker = DataMasker()
    data_masker.mask_csv(input_file, output_file, columns_to_mask)
