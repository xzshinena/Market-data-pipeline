import pandas as pd
from config import required_columns

#lowercase all column names; strip whitespaces
def standardize_columns(df : pd.DataFrame) -> pd.DataFrame :
    df = df.copy()
    df.columns = df.columns.str.lower().str.strip()
    return df

#remove trailing whitespcaes from string columns
def strip_string_columns(df : pd.DataFrame) -> pd.DataFrame :
    df = df.copy()
    string_columns = df.select_dtypes(include = ["object"]).columns

    for col in string_columns :
        df[col] = df[col].str.strip()
    
    return df

#convert columns to their proper types
def fix_data_types(df : pd.DataFrame) -> pd.DataFrame :
    df = df.copy()

    #timestamp -> datetime/NaT
    if "timestamp" in df.columns :
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors = "coerce")
    
    #price --> float / NaN
    if "price" in df.columns :
        df["price"] = pd.to_numeric(df["price"], errors = "coerce")
    
    if "product_id" in df.columns :
        df["product_id"] = df["product_id"].astype(str).str.upper()

    #currency --> string 
    if "currency" in df.columns :
        df["currency"] = df["currency"].astype(str).str.upper()

    return df


def handle_missing_values(df : pd.DataFrame) -> pd.DataFrame :
    df = df.copy()

    num_rows_before = len(df)

    #drop columns w/o required columns
    df = df.dropna(subset = list(required_columns))

    #fill optional columns 
    if "category" in df.columns :
        df["category"] = df["category"].fillna("Unknown")

    num_rows_dropped = num_rows_before - len(df)
    if num_rows_dropped > 0 :
        print(f"Dropped {num_rows_dropped} rows with missing required fields.")
    else :
        print(f"No dropped rows")

    return df

# aggregate cleaning function
def clean_data(df : pd.DataFrame) -> pd.DataFrame :
    df = standardize_columns(df)
    df = strip_string_columns(df)
    df = fix_data_types(df)
    df = handle_missing_values(df)
    return df






