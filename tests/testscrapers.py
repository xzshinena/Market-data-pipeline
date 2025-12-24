import sys
from pathlib import Path

proj_root = Path(__file__).parent.parent
sys.path.insert(0, str(proj_root))
from src.sources.scrapers.oliveyoung import OliveYoungScraper

scraper1 = OliveYoungScraper(search_terms=["anua heartleaf"])
print(f"Scraper created, BASE_URL: {scraper1.BASE_URL}")

df = scraper1.fetch()
print(f"OliveYoung fetched {len(df)} products")
print(df.head(10) if not df.empty else "empty df")
