import pandas as pd
import numpy as np
from config import rolling_window_days

def ohlc(df : pd.DataFrame) -> pd.DataFrame :
    df = df.copy()

    df["date"] =  df["timestamp"].dt.date
    df = df.sort_values("timestamp")
    grouped = df.groupby(["date", "supplier", "product_id"])

    ohlc = grouped.agg(
        open_price = ("price", "first"),
        high_price = ("price", "max"),
        low_price = ("price", "min"),
        close_price = ("price", "last")
    ).reset_index()

    return ohlc

def daily_return(df: pd.DataFrame) -> pd.DataFrame :
    df = df.copy()
    df = df.sort_values(["supplier", "product_id", "date"])
    df["daily_return"] = df.groupby(["supplier", "product_id"])["close_price"].pct_change()

    return df

def rolling_avg(df : pd.DataFrame, window: int = None) -> pd.DataFrame :
    if window is None :
        window = rolling_window_days
    
    df = df.copy()

    df = df.sort_values(["supplier", "product_id", "date"])
    df["rolling_avg_7d"] = df.groupby(["supplier","product_id"])["close_price"].transform(
        lambda x : x.rolling(window = window, min_periods = 1).mean()
    )

    df["rolling_avg_7d"] = df["rolling_avg_7d"].round(2)

    return df

def volatility(df: pd.DataFrame, window: int = None) -> pd.DataFrame :
    if window is None :
        window = rolling_window_days
    
    df = df.copy()
    
    df = df.sort_values(["supplier", "product_id", "date"])
    df["volatility_7d"] = df.groupby(["supplier", "product_id"])["daily_return"].transform(
        lambda x : x.rolling(window = window, min_periods = 2).std()
    )
    return df

def all_metrics(df: pd.DataFrame) -> pd.DataFrame :
    if df.empty :
        return pd.DataFrame() #empty df
    
    metrics_df = volatility(rolling_avg(daily_return(ohlc(df))))
    #metrics_df = ohlc(df)
    #metrics_df = daily_return(df)
    #metrics_df = rolling_avg(df)
    #metrics_df = volatility(df)

    return metrics_df

def metrics_summary(df : pd.DataFrame) -> pd.DataFrame :
    summary_df = df.groupby("product_id").agg(
        avg_price = ("close_price", "mean"),
        avg_return = ("daily_return", "mean"),
        avg_volatility = ("volatility_7d", "mean"),
        num_days = ("date", "count"),
        num_suppliers = ("supplier", "nunique")
    ).round(4)
    
    return summary_df

def get_most_volatile(df: pd.DataFrame, top_n: int = 5) -> pd.DataFrame :
    avg_vol = df.groupby("product_id")["volatility_7d"].mean().sort_values(ascending = False)
    return avg_vol.head(top_n)





