import requests
import pandas as pd
from datetime import datetime
from time import sleep
from src.sources.base import DataSource


class LamourScraper(DataSource):
    
    BASE_URL = "https://lamourlife.com"
    
    def __init__(self, search_terms: list[str] = None, limit: int = 250):
        self.search_terms = search_terms
        self.limit = min(limit, 250)
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        }
    
    @property
    def name(self) -> str:
        return "lamour_scraper"
    
    def fetch(self) -> pd.DataFrame:
        if self.search_terms:
            products = self._search_products(self.search_terms)
        else:
            products = self._get_all_products()
        
        return self._products_to_dataframe(products)
    
    def _get_all_products(self) -> list[dict]:
        all_products = []
        page = 1
        
        while True:
            url = f"{self.BASE_URL}/products.json?limit={self.limit}&page={page}"
            
            try:
                response = requests.get(url, headers=self.headers, timeout=10)
                response.raise_for_status()
                data = response.json()
                
                products = data.get("products", [])
                if not products:
                    break
                
                all_products.extend(products)
                page += 1
                sleep(0.5)
                
            except requests.RequestException:
                break
        
        return all_products
    
    def _search_products(self, search_terms: list[str]) -> list[dict]:
        all_products = []
        seen_ids = set()
        
        for term in search_terms:
            url = f"{self.BASE_URL}/search/suggest.json?q={term}&resources[type]=product"
            
            try:
                response = requests.get(url, headers=self.headers, timeout=10)
                response.raise_for_status()
                data = response.json()
                
                resources = data.get("resources", {}).get("results", {})
                products = resources.get("products", [])
                
                for product in products:
                    if product["id"] not in seen_ids:
                        full_product = self._get_product_details(product["handle"])
                        if full_product:
                            all_products.append(full_product)
                            seen_ids.add(product["id"])
                
                sleep(0.5)
                
            except requests.RequestException:
                continue
        
        return all_products
    
    def _get_product_details(self, handle: str) -> dict:
        url = f"{self.BASE_URL}/products/{handle}.json"
        
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            return response.json().get("product")
        except requests.RequestException:
            return None
    
    def _products_to_dataframe(self, products: list[dict]) -> pd.DataFrame:
        records = []
        timestamp = datetime.now()
        
        for product in products:
            variants = product.get("variants", [])
            
            if not variants:
                continue
            
            main_variant = variants[0]
            
            records.append({
                "timestamp": timestamp,
                "supplier": "lamour",
                "product_id": f"{product.get('handle', '')}",
                "price": float(main_variant.get("price", 0)),
                "currency": "CAD"
            })
        
        return pd.DataFrame(records)
