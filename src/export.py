import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime

from config import processed_directory
from src.pipeline.store import query_prices, query_metrics, query_comparison
from src.analysis.compare import comparison_report


def ensure_output_dir(output_dir: str) -> Path:
    path = Path(output_dir)
    path.mkdir(parents=True, exist_ok=True)
    return path

def export_prices_csv(
    conn: sqlite3.Connection,
    output_dir: str,
    start_date: str = None,
    end_date: str = None,
    filename: str = "prices.csv"
) -> str:

    output_path = ensure_output_dir(output_dir)
    filepath = output_path / filename
    
    df = query_prices(conn, start_date, end_date)
    
    if df.empty:
        print(f"No price data to export")
        return None
    
    columns_to_drop = ["id", "ingested_at"]
    df = df.drop(columns=[c for c in columns_to_drop if c in df.columns], errors="ignore")
    
    df.to_csv(filepath, index=False)
    print(f"Exported {len(df)} price records to {filepath}")
    
    return str(filepath)


def export_metrics_csv(
    conn: sqlite3.Connection,
    output_dir: str,
    start_date: str = None,
    end_date: str = None,
    filename: str = "daily_metrics.csv"
) -> str:

    output_path = ensure_output_dir(output_dir)
    filepath = output_path / filename
    
    df = query_metrics(conn, start_date, end_date)
    
    if df.empty:
        print(f"No metrics data to export")
        return None
    
    columns_to_drop = ["id", "computed_at"]
    df = df.drop(columns=[c for c in columns_to_drop if c in df.columns], errors="ignore")
    
    df.to_csv(filepath, index=False)
    print(f"Exported {len(df)} metric records to {filepath}")
    
    return str(filepath)


def export_comparison_csv(
    conn: sqlite3.Connection,
    output_dir: str,
    snapshot_date: str = None,
    filename: str = "price_comparison.csv"
) -> str:
    output_path = ensure_output_dir(output_dir)
    filepath = output_path / filename
    
    df = query_comparison(conn, snapshot_date)
    
    if df.empty:
        print(f"No comparison data to export")
        return None
    
    columns_to_drop = ["id", "computed_at"]
    df = df.drop(columns=[c for c in columns_to_drop if c in df.columns], errors="ignore")
    
    df.to_csv(filepath, index=False)
    print(f"Exported {len(df)} comparison records to {filepath}")
    
    return str(filepath)


def export_comparison_report(
    conn: sqlite3.Connection,
    output_dir: str,
    snapshot_date: str = None,
    filename: str = "comparison_report.txt"
) -> str:

    output_path = ensure_output_dir(output_dir)
    filepath = output_path / filename
    
    df = query_comparison(conn, snapshot_date)
    
    if df.empty:
        print(f"No comparison data for report")
        return None
    
    report = comparison_report(df)
    
    with open(filepath, "w") as f:
        f.write(report)
    
    print(f"Exported comparison report to {filepath}")
    return str(filepath)


def export_all(
    conn: sqlite3.Connection,
    output_dir: str = None,
    start_date: str = None,
    end_date: str = None
) -> dict:

    if output_dir is None:
        output_dir = str(processed_directory)
    
    print("\n" + "=" * 50)
    print("EXPORTING DATA")
    print("=" * 50)
    
    exported_files = {}
    
    prices_file = export_prices_csv(conn, output_dir, start_date, end_date)
    if prices_file:
        exported_files["prices"] = prices_file
    
    metrics_file = export_metrics_csv(conn, output_dir, start_date, end_date)
    if metrics_file:
        exported_files["metrics"] = metrics_file

    comparison_file = export_comparison_csv(conn, output_dir)
    if comparison_file:
        exported_files["comparison"] = comparison_file
    
    report_file = export_comparison_report(conn, output_dir)
    if report_file:
        exported_files["report"] = report_file
    
    print("\n" + "-" * 50)
    print(f"Export complete! {len(exported_files)} files created:")
    for name, path in exported_files.items():
        print(f"  - {name}: {path}")
    
    return exported_files


def export_summary_stats(
    conn: sqlite3.Connection,
    output_dir: str,
    filename: str = "summary_stats.txt"
) -> str:
    output_path = ensure_output_dir(output_dir)
    filepath = output_path / filename
    
    lines = []
    lines.append("=" * 50)
    lines.append("DATA SUMMARY")
    lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append("=" * 50)
    lines.append("")
    
    prices_df = query_prices(conn)
    if not prices_df.empty:
        lines.append("PRICE DATA:")
        lines.append(f"  Total records: {len(prices_df)}")
        lines.append(f"  Date range: {prices_df['timestamp'].min()} to {prices_df['timestamp'].max()}")
        lines.append(f"  Venues: {prices_df['venue'].nunique()}")
        lines.append(f"  Products: {prices_df['instrument_id'].nunique()}")
        lines.append(f"  Venues list: {', '.join(prices_df['venue'].unique())}")
        lines.append("")
    
    metrics_df = query_metrics(conn)
    if not metrics_df.empty:
        lines.append("METRICS DATA:")
        lines.append(f"  Total records: {len(metrics_df)}")
        lines.append(f"  Date range: {metrics_df['date'].min()} to {metrics_df['date'].max()}")
        lines.append("")
    
    comparison_df = query_comparison(conn)
    if not comparison_df.empty:
        lines.append("COMPARISON DATA:")
        lines.append(f"  Products compared: {len(comparison_df)}")
        lines.append(f"  Average savings: {comparison_df['savings_pct'].mean():.1f}%")
        
        lines.append(f"  Most competitive venue: {winner.index[0]} ({winner.iloc[0]} products)")
        lines.append("")
    
    with open(filepath, "w") as f:
        f.write("\n".join(lines))
    
    print(f"Exported summary stats to {filepath}")
    return str(filepath)

