import pandas as pd
from datetime import datetime

def find_cheapest(df : pd.DataFrame) -> pd.DataFrame :
    if df.empty :
        return pd.DataFrame()
    
    results = []

    for product_id, group in df.groupby("product_id"):
        sorted_group = group.sort_values("price")

        cheapest = sorted_group.iloc[0]
        most_expensive = sorted_group.iloc[-1]

        price_spread = most_expensive["price"] - cheapest["price"]

        if most_expensive["price"] > 0 :
            savings_pct = (price_spread / most_expensive["price"]) * 100
        else :
            savings_pct = 0
        
        results.append({
            "product_id" : product_id,
            "cheapest_supplier" : cheapest["supplier"],
            "cheapest_price" : cheapest["price"],
            "most_expensive_supplier" : most_expensive["supplier"],
            "most_expensive_price" : most_expensive["price"],
            "price_spread" : round(price_spread, 2),
            "savings_pct" : round(savings_pct, 2),
            "num_suppliers" : len(group),
            "snapshot_date" : datetime.now().strftime("%Y-%m-%d")
        })
    
    comparison_df = pd.DataFrame(results)
    comparison_df = comparison_df.sort_values("savings_pct", ascending = False)
    return comparison_df

def comparison_report(comparison_df : pd.DataFrame) -> str :
    if comparison_df.empty :
        return "No comparison data available"
    
    lines = []
    lines.append("=" * 60)
    lines.append("price comparison report")
    lines.append(f"  for {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append("=" * 60)
    lines.append("")

    comparison_df["category"] = "products"
    grouped = comparison_df.groupby("category")

    for category, products in grouped :
        lines.append(category.upper())
        lines.append("-" * 40)

        for _, row in products.iterrows():
            lines.append(f"\n {row['product_id']}:")
            lines.append(f"     cheapest : {row['cheapest_supplier']} @ ${row['cheapest_price']:.2f}")

            if row["num_suppliers"] > 1 :
                lines.append(f"    Most expensive: {row['most_expensive_supplier']} @ ${row['most_expensive_price']:.2f}")
                lines.append(f"    Save ${row['price_spread']:.2f} ({row['savings_pct']:.1f}%)")
            else:
                lines.append(f"    (Only available from 1 supplier)")
            
        lines.append("")
    
    lines.append("=" * 60)
    lines.append("    summary")
    lines.append("=" * 60)

    total_products = len(comparison_df)
    avg_savings = comparison_df["savings_pct"].mean()
    best_deal = comparison_df.iloc[0]

    lines.append(f"  products compared: {total_products}")
    lines.append(f"  average savings available: {avg_savings:.1f}%")
    lines.append(f"  best deal: {best_deal['product_id']} at {best_deal['cheapest_supplier']}")
    lines.append(f"             save {best_deal['savings_pct']:.1f}% (${best_deal['price_spread']:.2f})")

    winner = comparison_df["cheapest_supplier"].value_counts()
    lines.append(f"\n  Cheapest venue by product count:")
    for venue, count in winner.items():
        lines.append(f"    {venue}: {count} products")
    
    lines.append("")
    
    return "\n".join(lines)

def historicaal_comparison(df : pd.DataFrame, product_id : str) -> pd.DataFrame :
    product_df = df[df["product_id"] == product_id].copy()

    if product_df.empty :
        print(f"No data for {product_id}")
        return pd.DataFrame()
    
    product_df["date"] = product_df["timestamp"].dt.date

    pivot = product_df.pivot_table(
        index = "date",
        columns = "supplier",
        values = "price",
        aggfunc = "last"
    )

    pivot["cheapest_supplier"] = pivot.idxmin(axis = 1)
    pivot["cheapest_price"] = pivot.min(axis=1)

    return pivot
