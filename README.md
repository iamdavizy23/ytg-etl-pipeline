# 🛒 YTG Retail — Customer Orders ETL Pipeline

A full end-to-end **Extract, Transform, Load (ETL)** pipeline built in Python for YTG Retail Company's customer orders data. Raw data from three CSV source files(Products, Orders, Customers) is extracted, cleaned, merged, loaded into a SQLite database, and analysed to generate six key business insights.

> **Data Engineering Capstone Project** — YTG Data Engineering Programme

---

## 📌 Table of Contents

- [Project Overview](#project-overview)
- [Project Structure](#project-structure)
- [Pipeline Architecture](#pipeline-architecture)
- [Data Sources](#data-sources)
- [ETL Phases](#etl-phases)
- [Business Insights](#business-insights)
- [Data Quality Issues & Fixes](#data-quality-issues--fixes)
- [Technologies Used](#technologies-used)
- [How to Run](#how-to-run)
- [Sample Output](#sample-output)

---

## 📖 Project Overview

A retail company stores its operational data across three separate CSV files — customers, products, and orders. Management needs a clean, consolidated dataset for reporting and analytics.

As the Data Engineer, the task was to build a Python ETL pipeline that:

- ✅ Extracts data from **3 CSV source files** (11,000 rows each)
- ✅ Cleans and validates all records
- ✅ Merges the datasets into one unified table
- ✅ Loads the final output into a **SQLite database**
- ✅ Generates **6 business insights** from the clean data

---

## 📁 Project Structure

```
ytg-etl-pipeline/
│
├── etl_pipeline.py       ← Main ETL script (all 4 phases)
├── generate_data.py      ← Script to generate 11,000-row CSV files
│
├── customers.csv         ← Source: Customer data (11,000 rows)
├── products.csv          ← Source: Product catalogue (11,000 rows)
├── orders.csv            ← Source: Orders data (11,000 rows)
│
├── retail_sales.db       ← Output: SQLite database (auto-generated)
├── etl_output.log        ← Output: Full console log (auto-generated)
│
└── README.md             ← This file
```

---

## 🏗️ Pipeline Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   SOURCE FILES (CSV)                    │
│   customers.csv   products.csv   orders.csv             │
└──────────────────────┬──────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────┐
│              PHASE 1 — EXTRACT                          │
│   pd.read_csv() → Load into Pandas DataFrames          │
└──────────────────────┬──────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────┐
│              PHASE 2 — TRANSFORM                        │
│   • Remove duplicates                                   │
│   • Handle missing values                               │
│   • Fix data types (dates, numbers)                     │
│   • Merge all 3 datasets (inner join)                   │
│   • Calculate total_amount = quantity × price           │
│   • Filter invalid orders (qty ≤ 0)                     │
└──────────────────────┬──────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────┐
│              PHASE 3 — LOAD                             │
│   SQLite → retail_sales.db → table: sales_summary      │
└──────────────────────┬──────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────┐
│              PHASE 4 — REPORTING                        │
│   6 Business Insights generated from clean data        │
└─────────────────────────────────────────────────────────┘
```

---

## 📂 Data Sources

### customers.csv
| Column | Type | Description |
|--------|------|-------------|
| customer_id | String | Unique customer identifier (e.g. C00001) |
| customer_name | String | Full name of the customer |
| city | String | Nigerian city of residence |
| signup_date | Date | Date the customer registered |

### products.csv
| Column | Type | Description |
|--------|------|-------------|
| product_id | String | Unique product identifier (e.g. P00001) |
| product_name | String | Name of the product |
| category | String | Product category (Electronics, Footwear, etc.) |
| price | Float | Unit price in Nigerian Naira (₦) |

### orders.csv
| Column | Type | Description |
|--------|------|-------------|
| order_id | String | Unique order identifier (e.g. ORD000001) |
| customer_id | String | Foreign key → customers.csv |
| product_id | String | Foreign key → products.csv |
| quantity | Integer | Number of units ordered |
| order_date | Date | Date the order was placed |

---

## ⚙️ ETL Phases

### Phase 1 — Extract
- Reads all three CSV files using `pandas.read_csv()`
- Loads each file into a separate Pandas DataFrame
- Logs row and column counts for audit purposes

### Phase 2 — Transform

**Customer Cleaning**
- Removed duplicate `customer_id` records
- Dropped rows with missing `customer_name`
- Parsed `signup_date` to proper `datetime` format
- Normalised city names with `.str.strip().str.title()`

**Product Cleaning**
- Removed duplicate `product_id` records
- Converted `price` to numeric, dropped non-parseable values

**Order Cleaning**
- Removed duplicate `order_id` records
- Dropped rows with null or non-numeric `quantity`
- Parsed `order_date` to proper `datetime` format
- Applied business rule: filtered out orders where `quantity ≤ 0`

**Merge**
- `orders` ⟵ inner join ⟶ `customers` on `customer_id`
- result ⟵ inner join ⟶ `products` on `product_id`
- Orphan orders (invalid foreign keys) dropped automatically

**Enrichment**
- Calculated `total_amount = quantity × price`
- Extracted `order_month` (YYYY-MM) for trend analysis
- Extracted `order_year` for year-level grouping

### Phase 3 — Load
- Connected to `retail_sales.db` via Python's built-in `sqlite3`
- Wrote clean DataFrame to table `sales_summary` using `df.to_sql()`
- Verified row count with `SELECT COUNT(*)` query

### Phase 4 — Reporting & Analysis
Generated 6 business insights using Pandas groupby aggregations:

1. Total revenue generated
2. Top 5 customers by spending
3. Best-selling products by units sold
4. Revenue breakdown by product category
5. Monthly sales trend
6. Average order value

---

## 📊 Business Insights

| # | Insight | Result |
|---|---------|--------|
| 1 | Total Revenue Generated | **₦8,838,504,944.27** |
| 2 | Top Customer | Matthew Park — ₦20,957,345.29 |
| 3 | Best-Selling Product (units) | Adidas Running Shorts — 87 units |
| 4 | Top Revenue Category | Electronics — ₦5,896,105,767.61 |
| 5 | Peak Sales Month | March 2021 — ₦224,889,160.65 |
| 6 | Average Order Value | **₦825,180.18** |

**Key Findings:**
- Electronics dominates revenue at ~67% of total — a concentration risk worth monitoring
- Top 5 customers contribute a disproportionate share of revenue
- No consistent upward growth trend across months — seasonal and promotional drivers likely
- High AOV (₦825k) is pulled up by electronics; category-level AOV would be more actionable

---

## 🧹 Data Quality Issues & Fixes

| Issue | Dataset | Solution |
|-------|---------|----------|
| Duplicate records | All 3 files | `drop_duplicates(subset="<id_col>")` |
| Missing customer names | customers.csv | `dropna(subset=["customer_name"])` |
| Null prices | products.csv | `pd.to_numeric(errors="coerce")` + `dropna()` |
| Null / non-numeric quantity | orders.csv | `pd.to_numeric(errors="coerce")` + `dropna()` |
| Zero or negative quantity | orders.csv | `df[df["quantity"] > 0]` |
| Invalid foreign key IDs | orders.csv | Inner join drops orphan records automatically |
| String dates | All 3 files | `pd.to_datetime(errors="coerce")` |

---

## 🛠️ Technologies Used

| Tool | Version | Purpose |
|------|---------|---------|
| Python | 3.8+ | Core programming language |
| Pandas | Latest | Data extraction, cleaning, transformation |
| SQLite3 | Built-in | Lightweight relational database |
| Faker | Latest | Realistic test data generation |
| CSV | — | Source data format |

---

## 🚀 How to Run

**1. Clone the repository**
```bash
git clone https://github.com/iamdavizy23/ytg-etl-pipeline.git
cd ytg-etl-pipeline
```

**2. Install dependencies**
```bash
pip install pandas faker
```

**3. Generate the data (optional — CSVs already included)**
```bash
python generate_data.py
```

**4. Run the ETL pipeline**
```bash
python etl_pipeline.py
```

**What gets created automatically:**
- `retail_sales.db` — SQLite database with the `sales_summary` table
- `etl_output.log` — Full console output saved to file

**5. Query the database (optional)**
```python
import sqlite3, pandas as pd
conn = sqlite3.connect("retail_sales.db")
df = pd.read_sql("SELECT * FROM sales_summary LIMIT 10", conn)
print(df)
conn.close()
```

---

## 🖥️ Sample Output

```
╔══════════════════════════════════════════════════════════╗
║       YTG RETAIL — CUSTOMER ORDERS ETL PIPELINE         ║
╚══════════════════════════════════════════════════════════╝

  PHASE 1 — EXTRACT
  ✔  Loaded 'customers.csv'  →  11,000 rows, 4 columns
  ✔  Loaded 'products.csv'   →  11,000 rows, 4 columns
  ✔  Loaded 'orders.csv'     →  11,000 rows, 5 columns

  PHASE 2 — TRANSFORM
  [2C] Cleaning Orders …
       Duplicates removed  : 100
       Missing quantity    : 25
       Zero/negative qty   : 50
       Valid orders kept   : 10,825

  ✔  Transform complete — final dataset: 10,711 rows × 13 columns

  PHASE 3 — LOAD
  ✔  Database  : retail_sales.db
  ✔  Table     : sales_summary
  ✔  Rows saved: 10,711

  📊  TOTAL REVENUE GENERATED
       ₦8,838,504,944.27

  ✅  ETL Pipeline completed successfully.
```

---

## 👤 Author

**Dave** — Data Engineering Student, YTG  
GitHub: [@iamdavizy23](https://github.com/iamdavizy23)

---

*YTG Data Engineering Capstone Project | 2026*
