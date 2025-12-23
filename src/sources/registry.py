import pandas as pd
from src.sources.base import DataSource

class SourceRegistry :
    def __init__(self):
        self.sources : list[DataSource] = []
    
    # register the datasource to the registry
    def register(self, source : DataSource) -> None:
        self.sources.append(source)
        print(f"Registered source : {source.name}")

    def fetch_all(self) -> pd.DataFrame :
        dataframes = []

        for source in self.sources :
            df = source.fetch()
            df["source_name"] = source.name
            dataframes.append(df)
        
        combined = pd.concat(dataframes, ignore_index = True)
        return combined
    
    def list_sources(self) -> list[str]:
        lst = []
        for source in self.sources :
            lst.append(source.name)
        return lst
