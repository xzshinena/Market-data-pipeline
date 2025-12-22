import pandas as pd
from datetime import datetime
from config import required_columns, valid_currencies, suppliers, min_price, max_price

# data validation checks

# valid price
def price_check(df : pd.DataFrame) -> pd.Series :
    return df["price"] > min_price

# valid timestamp
def timestamp_check(df : pd.DataFrame) -> pd.Series :
    curr = datetime.now()
    not_null = df["timestamp"].notna()
    not_future = df["timestamp"] <= curr #not future
    return not_null & not_future

# has all the required fields
def required_field_check(df : pd.DataFrame) -> pd.Series :
    return df[required_columns].notna().all(axis = 1)

# valid currency
def currency_check(df : pd.DataFrame) -> pd.Series :
    return df["currency"].isin(valid_currencies)

# bound on price
def price_in_bounds(df : pd.DataFrame) -> pd.Series :
    all_above = df["price"] > min_price
    all_below = df["price"] < max_price
    return  all_above & all_below

# timestamp in range
def time_in_bounds(df : pd.DataFrame, start_date : str, end_date : str) -> pd.Series :
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date) + pd.Timedelta(days = 1) - pd.Timedelta(seconds = 1)

    after_start = df["timestamp"] >= start
    before_start = df["timestamp"] <= end
    return after_start & before_start

# check for unknown suppliers
def supplier_check(df : pd.DataFrame) -> pd.Series :
    return df["supplier"].isin(suppliers)

def run_quality_checkes( df : pd.DataFrame, start_date: str = None, end_date: str = None) -> tuple[pd.DataFrmae, pd.DataFrame]
    # returns [valid, rejected]
    df = df.copy()
    df["rejected_reason"] = ""

    mask = price_check(df)
    failed_count = (~mask).sum()
    if failed_count > 0 :
        print(f"{failed_count} rows failed bc price <= 0.")
        df.loc[~mask, "rejected_reason"] += "price not positive"
    else :
        print("All prices valid")

    mask = timestamp_check(df)
    failed_count = (~mask).sum()
    if failed_count > 0 :
        print(f"{failed_count} rows failed bc invalid timestamp.")
        df.loc[~mask, "rejected_reason"] += "invalid timestamp"
    else :
        print("All timestamps valid")
    
    mask = required_field_check(df)
    failed_count = (~mask).sum()
    if failed_count > 0 :
        print(f"{failed_count} rows failed bc don't have all required fields")
        df.loc[~mask, "rejected_reason"] += "doesn't have all required fields"
    else :
        print("All rows have all required fields")
    
    mask = currency_check(df)
    failed_count = (~mask).sum()
    if failed_count > 0 :
        print(f"{failed_count} rows failed bc invalid currency.")
        df.loc[~mask, "rejected_reason"] += "invalid currency"
    else :
        print("All rows have valid currency")
    
    mask = price_in_bounds(df)
    failed_count = (~mask).sum()
    if failed_count > 0 :
        print(f"{failed_count} rows failed bc price out of bounds.")
        df.loc[~mask, "rejected_reason"] += "price out of bounds"
    else : 
        print("All rows have prices within bounds")
    
    #mask = time_in_bounds(df)
    #failed_count = (~mask).sum()
    #if failed_count > 0 :
     #   print(f"{failed_count} rows failed bc time out of bounds")
    #else :
    #    print("All rows have time in bounds")
    
    mask = supplier_check(df)
    failed_count = (~mask).sum()
    if failed_count > 0 :
        print(f"{failed_count} rows failed bc invalid supplier")
        df.loc[~mask, "rejected_reason"] += "supplier unknown"
    else :
        print("All rows have valid suppliers")
    
    has_rejection = df["rejected_reason"] != ""
    rejected = df["rejected_reason"].copy()
    valid = df[~has_rejection].copy()
    valid = valid.drop(columns = ["rejected_reason"])

    return valid, rejected

def save_rejected(rejected : pd.DataFrame, output_path : str) -> None :
    if len(rejected) > 0 :
        rejected.to_csv(output_path, index = False)
        