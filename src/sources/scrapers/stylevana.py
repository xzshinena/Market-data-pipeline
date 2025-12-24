import pandas as pd
from datetime import datetime
from time import sleep
from src.sources.base import DataSource

try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False


class StylevanaScraper(DataSource):
    
    BASE_URL = "https://www.stylevana.com"
    
    def __init__(self, search_terms: list[str] = None, limit: int = 50):
        self.search_terms = search_terms
        self.limit = limit
    
    @property
    def name(self) -> str:
        return "stylevana_scraper"
    
    def fetch(self) -> pd.DataFrame:
        if not PLAYWRIGHT_AVAILABLE:
            return pd.DataFrame()
        
        if self.search_terms:
            products = self._search_products(self.search_terms)
        else:
            products = []
        
        return self._products_to_dataframe(products)
    
    def _search_products(self, search_terms: list[str]) -> list[dict]:
        all_products = []
        seen_ids = set()
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            
            for term in search_terms:
                url = f"{self.BASE_URL}/en_CA/catalogsearch/result/?q={term.replace(' ', '+')}"
                
                try:
                    page.goto(url, timeout=30000)
                    page.wait_for_selector(".product-item, .product-card, [class*='product']", timeout=10000)
                    
                    product_cards = page.query_selector_all(".product-item, .product-card")
                    
                    for card in product_cards[:self.limit]:
                        product = self._parse_product_card(card)
                        if product and product.get("id") not in seen_ids:
                            all_products.append(product)
                            seen_ids.add(product.get("id"))
                    
                    sleep(1)
                    
                except Exception:
                    continue
            
            browser.close()
        
        return all_products
    
    def _parse_product_card(self, card) -> dict:
        try:
            title_elem = card.query_selector(".product-name, .product-item-name, a[title]")
            title = title_elem.inner_text() if title_elem else None
            
            if not title:
                title_elem = card.query_selector("a")
                title = title_elem.get_attribute("title") if title_elem else None
            
            price_elem = card.query_selector(".price, [class*='price']")
            price_text = price_elem.inner_text() if price_elem else None
            
            price = None
            if price_text:
                price_clean = "".join(c for c in price_text if c.isdigit() or c == ".")
                if price_clean:
                    price = float(price_clean)
            
            if title and price and price > 0:
                product_id = title.lower().replace(" ", "-").replace("/", "-")[:50]
                return {
                    "id": product_id,
                    "title": title,
                    "price": price
                }
            
        except Exception:
            pass
        
        return None
    
    def _products_to_dataframe(self, products: list[dict]) -> pd.DataFrame:
        records = []
        timestamp = datetime.now()
        
        for product in products:
            handle = product.get("id", "").replace(" ", "-").upper()
            
            records.append({
                "timestamp": timestamp,
                "supplier": "stylevana",
                "product_id": handle,
                "price": float(product.get("price", 0)),
                "currency": "CAD"
            })
        
        return pd.DataFrame(records)