import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.sources.registry import SourceRegistry
from src.sources.scrapers.oomomo import OomomoScraper
from src.sources.scrapers.axiastation import AxiastationScraper
from src.sources.scrapers.kiyoko import KiyokoScraper
from src.sources.scrapers.kiokii_and import KiokiiScraper

search_term = "aestura atobarrier 365 cream"

registry = SourceRegistry()
registry.register(OomomoScraper(search_terms=[search_term]))
registry.register(AxiastationScraper(search_terms=[search_term]))
registry.register(KiyokoScraper(search_terms=[search_term]))
registry.register(KiokiiScraper(search_terms=[search_term]))

df = registry.fetch_all()
print(df)