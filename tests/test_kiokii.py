import sys
from pathlib import Path

proj_root = Path(__file__).parent.parent
sys.path.insert(0, str(proj_root))
from src.sources.scrapers.kiokii_and import KiokiiScraper

scraper = KiokiiScraper(search_terms = "romand lip tint")
print(f"Scraper created, BASE_URL: {scraper.BASE_URL}")

df = scraper.fetch()
print(f"Fetched {len(df)} products")
print(df.head() if not df.empty else "empty df")


