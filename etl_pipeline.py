"""
============================================================
  YTG RETAIL COMPANY — CUSTOMER ORDERS ETL PIPELINE
  Data Engineer: ETL Assignment Solution
  Database: retail_sales.db | Table: sales_summary
============================================================
"""

import pandas as pd
import sqlite3
import os
import warnings
from datetime import datetime

warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────
#  CONFIGURATION
# ─────────────────────────────────────────────
BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
DB_PATH     = os.path.join(BASE_DIR, "retail_sales.db")
LOG_LINES   = []


def log(msg: str, section: bool = False):
    """Print and store a log message."""
    if section:
        line = "\n" + "═" * 60
        print(line);         LOG_LINES.append(line)
        print(f"  {msg}");   LOG_LINES.append(f"  {msg}")
        print("═" * 60);     LOG_LINES.append("═" * 60)
    else:
        print(msg);          LOG_LINES.append(msg)


# ╔══════════════════════════════════════════════════════════╗
# ║              PHASE 1 — EXTRACT                           ║
# ╚══════════════════════════════════════════════════════════╝

def extract():
    """Read all three CSV files into Pandas DataFrames."""
    log("PHASE 1 — EXTRACT", section=True)

    files = {
        "customers": os.path.join(BASE_DIR, "customers.csv"),
        "products":  os.path.join(BASE_DIR, "products.csv"),
        "orders":    os.path.join(BASE_DIR, "orders.csv"),
    }

    dataframes = {}
    for name, path in files.items():
        df = pd.read_csv(path)
        dataframes[name] = df
        log(f"  ✔  Loaded '{name}.csv'  →  {len(df)} rows, {len(df.columns)} columns")

    log(f"\n  Raw row counts: customers={len(dataframes['customers'])} | "
        f"products={len(dataframes['products'])} | orders={len(dataframes['orders'])}")
    return dataframes


# ╔══════════════════════════════════════════════════════════╗
# ║              PHASE 2 — TRANSFORM                         ║
# ╚══════════════════════════════════════════════════════════╝

def transform(dataframes: dict) -> pd.DataFrame:
    """Clean, enrich, and merge all three DataFrames."""
    log("PHASE 2 — TRANSFORM", section=True)

    customers = dataframes["customers"].copy()
    products  = dataframes["products"].copy()
    orders    = dataframes["orders"].copy()

    # ── 2A: CLEAN CUSTOMERS ─────────────────────────────────
    log("\n  [2A] Cleaning Customers …")
    before = len(customers)
    customers.drop_duplicates(subset="customer_id", inplace=True)
    customers.dropna(subset=["customer_name"], inplace=True)
    customers["signup_date"] = pd.to_datetime(customers["signup_date"], errors="coerce")
    customers["city"] = customers["city"].str.strip().str.title()
    after = len(customers)
    log(f"       Duplicates removed: {before - after} | Missing names dropped | Rows kept: {after}")

    # ── 2B: CLEAN PRODUCTS ──────────────────────────────────
    log("\n  [2B] Cleaning Products …")
    before = len(products)
    products.drop_duplicates(subset="product_id", inplace=True)
    products["price"] = pd.to_numeric(products["price"], errors="coerce")
    products.dropna(subset=["price"], inplace=True)
    after = len(products)
    log(f"       Duplicates removed: {before - after} | Non-numeric prices dropped | Rows kept: {after}")

    # ── 2C: CLEAN ORDERS ────────────────────────────────────
    log("\n  [2C] Cleaning Orders …")
    before = len(orders)

    # Remove duplicate orders
    orders.drop_duplicates(subset="order_id", inplace=True)
    dup_removed = before - len(orders)

    # Handle missing / non-numeric quantity
    orders["quantity"] = pd.to_numeric(orders["quantity"], errors="coerce")
    missing_qty = orders["quantity"].isna().sum()
    orders.dropna(subset=["quantity"], inplace=True)

    # Convert date
    orders["order_date"] = pd.to_datetime(orders["order_date"], errors="coerce")
    invalid_dates = orders["order_date"].isna().sum()
    orders.dropna(subset=["order_date"], inplace=True)

    # Business rule: quantity must be > 0
    invalid_qty = (orders["quantity"] <= 0).sum()
    orders = orders[orders["quantity"] > 0]

    after = len(orders)
    log(f"       Duplicates removed  : {dup_removed}")
    log(f"       Missing quantity    : {missing_qty}")
    log(f"       Invalid dates       : {invalid_dates}")
    log(f"       Zero/negative qty   : {int(invalid_qty)}")
    log(f"       Valid orders kept   : {after}")

    # ── 2D: MERGE ────────────────────────────────────────────
    log("\n  [2D] Merging datasets …")
    merged = orders.merge(customers, on="customer_id", how="inner")
    merged = merged.merge(products,  on="product_id",  how="inner")

    # Records dropped because customer/product ID not found
    orphan_orders = len(orders) - len(merged)
    log(f"       Orders with unknown customer/product (dropped): {orphan_orders}")
    log(f"       Rows after merge: {len(merged)}")

    # ── 2E: ENRICHMENT ───────────────────────────────────────
    log("\n  [2E] Enriching data …")
    merged["quantity"]      = merged["quantity"].astype(int)
    merged["total_amount"]  = merged["quantity"] * merged["price"]
    merged["order_month"]   = merged["order_date"].dt.to_period("M").astype(str)
    merged["order_year"]    = merged["order_date"].dt.year

    # Select and rename final columns
    sales_summary = merged[[
        "order_id", "order_date", "order_month", "order_year",
        "customer_id", "customer_name", "city",
        "product_id", "product_name", "category",
        "quantity", "price", "total_amount"
    ]].copy()

    sales_summary.sort_values("order_date", inplace=True)
    sales_summary.reset_index(drop=True, inplace=True)

    log(f"       total_amount computed for {len(sales_summary)} records")
    log(f"\n  ✔  Transform complete — final dataset: {len(sales_summary)} rows × {len(sales_summary.columns)} columns")
    return sales_summary


# ╔══════════════════════════════════════════════════════════╗
# ║              PHASE 3 — LOAD                              ║
# ╚══════════════════════════════════════════════════════════╝

def load(df: pd.DataFrame):
    """Load the transformed DataFrame into SQLite."""
    log("PHASE 3 — LOAD", section=True)

    conn = sqlite3.connect(DB_PATH)
    df.to_sql("sales_summary", conn, if_exists="replace", index=False)
    conn.commit()

    # Verify
    row_count = pd.read_sql("SELECT COUNT(*) AS cnt FROM sales_summary", conn).iloc[0, 0]
    conn.close()

    log(f"  ✔  Database  : {DB_PATH}")
    log(f"  ✔  Table     : sales_summary")
    log(f"  ✔  Rows saved: {row_count}")


# ╔══════════════════════════════════════════════════════════╗
# ║              PHASE 4 — REPORTING & ANALYSIS              ║
# ╚══════════════════════════════════════════════════════════╝

def report(df: pd.DataFrame):
    """Generate all six business insights."""
    log("PHASE 4 — REPORTING & ANALYSIS", section=True)

    # ── 4.1 Total Revenue ────────────────────────────────────
    total_revenue = df["total_amount"].sum()
    log(f"\n  📊  [1] TOTAL REVENUE GENERATED")
    log(f"       ₦{total_revenue:,.2f}")

    # ── 4.2 Top 5 Customers by Spending ─────────────────────
    top_customers = (
        df.groupby(["customer_id", "customer_name"])["total_amount"]
        .sum()
        .reset_index()
        .sort_values("total_amount", ascending=False)
        .head(5)
        .reset_index(drop=True)
    )
    top_customers.index += 1
    log(f"\n  👑  [2] TOP 5 CUSTOMERS BY SPENDING")
    for _, row in top_customers.iterrows():
        log(f"       {int(row.name)}. {row['customer_name']:<25}  ₦{row['total_amount']:>14,.2f}")

    # ── 4.3 Best-Selling Products ────────────────────────────
    best_products = (
        df.groupby(["product_id", "product_name"])
        .agg(units_sold=("quantity", "sum"), revenue=("total_amount", "sum"))
        .reset_index()
        .sort_values("units_sold", ascending=False)
        .head(5)
        .reset_index(drop=True)
    )
    best_products.index += 1
    log(f"\n  🏆  [3] BEST-SELLING PRODUCTS (by units sold)")
    for _, row in best_products.iterrows():
        log(f"       {int(row.name)}. {row['product_name']:<30}  Units: {int(row['units_sold']):>4}  |  Revenue: ₦{row['revenue']:>13,.2f}")

    # ── 4.4 Revenue by Category ──────────────────────────────
    cat_revenue = (
        df.groupby("category")["total_amount"]
        .sum()
        .reset_index()
        .sort_values("total_amount", ascending=False)
    )
    log(f"\n  📂  [4] REVENUE BY PRODUCT CATEGORY")
    for _, row in cat_revenue.iterrows():
        bar_len = int(row["total_amount"] / cat_revenue["total_amount"].max() * 20)
        bar = "█" * bar_len
        log(f"       {row['category']:<22}  {bar:<20}  ₦{row['total_amount']:>13,.2f}")

    # ── 4.5 Monthly Sales Trend ──────────────────────────────
    monthly = (
        df.groupby("order_month")["total_amount"]
        .sum()
        .reset_index()
        .sort_values("order_month")
    )
    log(f"\n  📅  [5] MONTHLY SALES TREND")
    max_month = monthly["total_amount"].max()
    for _, row in monthly.iterrows():
        bar_len = int(row["total_amount"] / max_month * 25)
        bar = "▓" * bar_len
        log(f"       {row['order_month']}  {bar:<25}  ₦{row['total_amount']:>13,.2f}")

    # ── 4.6 Average Order Value ──────────────────────────────
    aov = df["total_amount"].mean()
    total_orders = df["order_id"].nunique()
    log(f"\n  💡  [6] AVERAGE ORDER VALUE")
    log(f"       Total Orders  : {total_orders}")
    log(f"       Avg Order Value: ₦{aov:,.2f}")

    log("\n" + "═" * 60)
    log("  ✅  ETL Pipeline completed successfully.")
    log("═" * 60)

    return {
        "total_revenue": total_revenue,
        "top_customers": top_customers,
        "best_products": best_products,
        "cat_revenue":   cat_revenue,
        "monthly":       monthly,
        "aov":           aov,
    }


# ╔══════════════════════════════════════════════════════════╗
# ║                        MAIN                              ║
# ╚══════════════════════════════════════════════════════════╝

def main():
    log("╔══════════════════════════════════════════════════════════╗")
    log("║       YTG RETAIL — CUSTOMER ORDERS ETL PIPELINE         ║")
    log(f"║       Run timestamp: {datetime.now().strftime('%Y-%m-%d  %H:%M:%S')}                  ║")
    log("╚══════════════════════════════════════════════════════════╝")

    raw        = extract()
    clean_df   = transform(raw)
    load(clean_df)
    insights   = report(clean_df)

    # Save console output log
    log_path = os.path.join(BASE_DIR, "etl_output.log")
    with open(log_path, "w", encoding="utf-8") as f:
        f.write("\n".join(LOG_LINES))
    print(f"\n  📄  Full log saved to: {log_path}")

    return clean_df, insights


if __name__ == "__main__":
    main()
