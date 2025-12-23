#project settings

from pathlib import Path

project_root = Path(__file__).parent

raw_directory = project_root / "data" / "raw"
processed_directory = project_root / "data" / "processed"
db_path = project_root / "data" / "market_data.db"

valid_currencies = {"CAD" , "RMB", "KRW" , "JPY" , "USD"}

suppliers = {
    "sephora",
    "yesstyle",
    "sukoshi",
    "stylevana",
    "amazon",
    "oliveyoung",
    "kiokii",
    "shoppers",
    "lamour",
    "oomomo",
    "axiastation",
    "cosme",
    "kiyoko",
    "komiko"
}

min_price = 0.01
max_price = 1000000

rolling_window_days = 7

required_columns = {
    "timestamp",
    "supplier",      
    "product_id",    
    "price",
    "currency"
}
