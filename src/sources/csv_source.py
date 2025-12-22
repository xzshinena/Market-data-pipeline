import pandas as pd
from pathlib import Path
from src.sources.base import DataSource

#load the price data from a csv file
class CSVSource(DataSource):
    def __init__(self, file_path: str):
        self.file_path = file_path
    
    @property
    def name(self) -> str:
        filename = Path(self.file_path).name
        return f"csv:{filename}"
    
    def fetch(self) -> pd.DataFrame:
        df = pd.read_csv(self.file_path)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df

#load and combine all csv files from a directory
class CSVDirectorySource(DataSource):
    def _init_(self, directory: str):
        self.directory = directory
    
    @property
    def name(self) -> str:
        return f"csv_dir: {self.directory}"
    
    def fetch(self) -> pd.DataFrame :
        dir_path = Path(self.directory)
        csv_files = list(dir_path.glob("*.csv"))

        if not csv_files :
            print(f"No csv files found in {self.directory}")
            return pd.DataFrame()
        
        dataframes = []
        for csv_file in csv_files :
            df = pd.read_csv(csv_file)
            dataframes.append(df)
        
        combined = pd.concat(dataframes, ignore_index = True)
        combined["timestamp"] = pd.to_datetime(df["timestamp"])
        return combined

