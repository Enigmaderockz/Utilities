import pandas as pd
import teradatasql
import numpy as np
'''
def clean_dataframe(df):
    df.columns = map(str.lower, df.columns)
    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    df = df.astype(object).replace(["NA", "N/A", np.nan], "None")
    return df
'''
def clean_dataframe(df):
    # Convert column names to lowercase
    df.columns = df.columns.str.lower()

    # Strip leading and trailing whitespaces from string columns
    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

    # Replace "NA", "N/A", and NaN values with "None"
    df.replace(["NA", "N/A", np.nan], "None", inplace=True)

    return df

def getDataframe_from_srcfile(input_file, column_name=None):
    try:
        if column_name is not None:
            df = pd.read_csv(input_file, sep="|", names=column_name, header=None, dtype="unicode")
        else:
            df = pd.read_csv(input_file, sep="|", dtype="unicode")
        df = clean_dataframe(df)
        return df
    except pd.errors.EmptyDataError:
        print("Empty file")

def getDataframe_from_db(sql):
    with teradatasql.connect(None, logmech="KRB5", host="dzqtdwm", user="tdddba", encryptdata="true") as conn:
        df = pd.read_sql(sql, conn)
        df = df.astype("unicode")
        columns_date = [col_desc[0] for col_desc in df.dtypes.items() if col_desc[1].name == "date"]
        df[columns_date] = df[columns_date].apply(pd.to_datetime, format="%Y-%m-%d", errors="coerce")
        df = clean_dataframe(df)
        return df

def data_frame_compare(df1, df2, diff_file, columtosort):
    if df1.equals(df2):
        print("Dataframes are identical")
        return

    df1 = df1.astype(df2.dtypes.to_dict())
    df1.sort_index(axis=1, inplace=True)
    df2.sort_index(axis=1, inplace=True)
    df1.sort_values(by=columtosort, inplace=True)
    df2.sort_values(by=columtosort, inplace=True)
    df1.reset_index(drop=True, inplace=True)
    df2.reset_index(drop=True, inplace=True)

    df_diff = pd.concat([df1, df2]).drop_duplicates(keep=False)
    df_diff.to_csv(diff_file, index=False)

df1 = getDataframe_from_srcfile("a_tmp.csv")
df2 = getDataframe_from_db(sql)
diff_file = "diff.csv"
columtosort = list(df1)
data_frame_compare(df1, df2, diff_file, columtosort)
