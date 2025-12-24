import argparse
from datetime import datetime
from config import raw_directory, db_path, processed_directory

from src.sources.registry import SourceRegistry
from src.sources.csv_source import CSVSource, CSVDirectorySource

from src.pipeline.clean import clean_data
from src.pipeline.validate import run_quality_checks, save_rejected
from src.pipeline.store import(
    get_connection,
    init_schema,
    upsert_prices,
    upsert_metrics,
    upsert_comparison,
    query_prices,
    query_latest_prices,
    query_metrics,
    query_comparison
)

from src.analysis.metrics import all_metrics
from src.analysis.compare import find_cheapest, comparison_report

from src.export import export_all

def setup_sources(source_names : list[str], search_term: str = None) -> SourceRegistry :
    registry = SourceRegistry()

    if "csv" in source_names :
        registry.register(CSVDirectorySource(str(raw_directory)))
    
    if "axiastation" in source_names : 
        from src.sources.scrapers.axiastation import AxiastationScraper
        registry.register(AxiastationScraper(search_terms=[search_term] if search_term else None))
    
    if "kiokii" in source_names : 
        from src.sources.scrapers.kiokii_and import KiokiiScraper
        registry.register(KiokiiScraper(search_terms=[search_term] if search_term else None))
    
    if "kiyoko" in source_names : 
        from src.sources.scrapers.kiyoko import KiyokoScraper
        registry.register(KiyokoScraper(search_terms=[search_term] if search_term else None))

    if "oomomo" in source_names : 
        from src.sources.scrapers.oomomo import OomomoScraper
        registry.register(OomomoScraper(search_terms=[search_term] if search_term else None))

    if "sukoshi" in source_names : 
        from src.sources.scrapers.sukoshi import SukoshiScraper
        registry.register(SukoshiScraper(search_terms=[search_term] if search_term else None))

    if "stylevana" in source_names : 
        from src.sources.scrapers.stylevana import StylevanaScraper
        registry.register(StylevanaScraper(search_terms=[search_term] if search_term else None))

    if "oliveyoung" in source_names : 
        from src.sources.scrapers.oliveyoung import OliveYoungScraper
        registry.register(OliveYoungScraper(search_terms=[search_term] if search_term else None))

    if "lamour" in source_names : 
        from src.sources.scrapers.lamour import LamourScraper
        registry.register(LamourScraper(search_terms=[search_term] if search_term else None))

    if "komiko" in source_names : 
        from src.sources.scrapers.komiko import KomikoScraper
        registry.register(KomikoScraper(search_terms=[search_term] if search_term else None))

    return registry

def pipeline(args) :
    #1. ingest
    registry = setup_sources(args.sources, args.search)

    if not registry.sources : 
        print("no sources configured")
        return
    
    raw_df = registry.fetch_all()

    if raw_df.empty:
        print("no data fetched from sources")
        return
    
    #2. clean
    clean_df =  clean_data(raw_df)

    #3. validate
    valid_df, rejected_df = run_quality_checks(clean_df, args.start, args.end)
    
    if len(rejected_df) > 0 :
        rejected_path = str(processed_directory / "rejected_rows.csv")
        save_rejected(rejected_df, rejected_path)
    
    if valid_df.empty :
        print("no valid data after validation tests")
        return
    
    #4. store
    conn = get_connection(str(db_path))
    init_schema(conn)

    rows_stored = upsert_prices(conn, valid_df)

    #5. metrics
    if not args.skip_metrics :
        prices_df = query_prices(conn, args.start, args.end)
        if not prices_df.empty :
            metrics_df = all_metrics(prices_df)
            if not metrics_df.empty:
                upsert_metrics(conn, metrics_df)
    
    #6. price comparison
    if args.compare :
        latest_prices = query_latest_prices(conn)
        
        if not latest_prices.empty :
            comparison_df = find_cheapest(latest_prices)

            if not comparison_df.empty :
                upsert_comparison(conn, comparison_df)
                report = comparison_report(comparison_df)
                print(report)
    
    #7. export
    exported_files = export_all(conn, str(processed_directory), args.start, args.end)
    conn.close()

    print(f"\n output files in {processed_directory}")

    for name, path in exported_files.items():
        print(f"  - {name}: {path}")
    

def main() : 
    parser = argparse.ArgumentParser(
        description = "beauty supply market data pipeline",
        formatter_class = argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--start",
        required = True
    )

    parser.add_argument(
        "--end",
        required = True
    )

    parser.add_argument(
        "--compare",
        action = "store_true"
    )

    parser.add_argument(
        "--sources",
        nargs = "+",
        default = ["csv"]
    )

    parser.add_argument(
        "--skip-metrics",
        action = "store_true"
    )

    parser.add_argument(
        "--search",
        default = None,
    )

    args = parser.parse_args()

    pipeline(args)

if __name__ == "__main__":
    main()


        



