"""
Generate 11,000 rows for each of the three CSV source files.
Includes intentional dirty data (duplicates, nulls, invalid values)
so the ETL pipeline has real cleaning work to do.
"""

import pandas as pd
import random
import os
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()
random.seed(42)
Faker.seed(42)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TARGET   = 11_000

# ── Nigerian cities ───────────────────────────────────────────
NG_CITIES = [
    "Lagos", "Abuja", "Kano", "Ibadan", "Port Harcourt",
    "Enugu", "Kaduna", "Benin City", "Owerri", "Onitsha",
    "Calabar", "Sokoto", "Abeokuta", "Ilorin", "Warri",
    "Maiduguri", "Aba", "Jos", "Zaria", "Uyo",
]

# ── Product catalogue (50 realistic Nigerian products) ────────
PRODUCTS = [
    ("Electronics",     "Samsung Galaxy A54",         320_000),
    ("Electronics",     "iPhone 14 Pro",              850_000),
    ("Electronics",     "HP Laptop 15",               450_000),
    ("Electronics",     "LG 43-inch TV",              280_000),
    ("Electronics",     "Canon DSLR Camera",          550_000),
    ("Electronics",     "USB-C Hub 7-in-1",            15_000),
    ("Electronics",     "Wireless Earbuds",            22_000),
    ("Electronics",     "Portable Power Bank 20000mAh", 18_500),
    ("Electronics",     "Smart Watch",                 75_000),
    ("Electronics",     "Bluetooth Speaker",           35_000),
    ("Footwear",        "Nike Air Max",                65_000),
    ("Footwear",        "Adidas Ultraboost",           78_000),
    ("Footwear",        "Puma Training Shoes",         48_000),
    ("Footwear",        "Timberland Boots",            95_000),
    ("Footwear",        "Clarks Loafers",              55_000),
    ("Apparel",         "Adidas Running Shorts",       18_000),
    ("Apparel",         "Polo Ralph Lauren T-Shirt",   32_000),
    ("Apparel",         "Levi's 501 Jeans",            45_000),
    ("Apparel",         "Zara Casual Shirt",           28_000),
    ("Apparel",         "Nike Tracksuit",              62_000),
    ("Fashion",         "Ankara Fabric (5 yards)",     12_000),
    ("Fashion",         "Women's Handbag",             28_000),
    ("Fashion",         "Men's Leather Wallet",        15_000),
    ("Fashion",         "Wristwatch (Casio)",          40_000),
    ("Fashion",         "Sunglasses (Ray-Ban)",        55_000),
    ("Kitchen",         "Tefal Non-Stick Pan",         25_000),
    ("Kitchen",         "Blender (Binatone)",          32_000),
    ("Kitchen",         "Pressure Cooker 5L",          38_000),
    ("Kitchen",         "Microwave Oven",              95_000),
    ("Kitchen",         "Electric Kettle",             12_500),
    ("Food & Beverage", "Indomie Noodles (Carton)",     7_500),
    ("Food & Beverage", "Golden Penny Semovita 5kg",    4_200),
    ("Food & Beverage", "Jollof Rice Seasoning Pack",   2_500),
    ("Food & Beverage", "Nestle Milo 900g",             8_800),
    ("Food & Beverage", "Ribena Blackcurrant 1L",       3_200),
    ("Health",          "Protein Powder 2kg",          35_000),
    ("Health",          "Multivitamin Tablets (60ct)", 12_000),
    ("Health",          "Blood Pressure Monitor",      45_000),
    ("Health",          "First Aid Kit",                8_500),
    ("Health",          "Glucometer Kit",              28_000),
    ("Beauty",          "Nivea Body Lotion",            4_500),
    ("Beauty",          "Olay Regenerist Cream",       18_500),
    ("Beauty",          "Dove Shampoo 700ml",           6_200),
    ("Beauty",          "Black Opal Foundation",       14_000),
    ("Beauty",          "Perfume (Hugo Boss)",         75_000),
    ("Sports",          "Yoga Mat",                     8_500),
    ("Sports",          "Dumbbells Set 20kg",          35_000),
    ("Sports",          "Football (Adidas)",           18_000),
    ("Furniture",       "Office Chair (Executive)",    85_000),
    ("Accessories",     "Children's Backpack",          9_500),
]

# ────────────────────────────────────────────────────────────
# 1. CUSTOMERS  (11,000 rows)
# ────────────────────────────────────────────────────────────
print("Generating customers.csv …")

clean_customers = TARGET - 200  # reserve 200 for dirty rows
cust_rows = []
for i in range(1, clean_customers + 1):
    cid  = f"C{i:05d}"
    name = fake.name()
    city = random.choice(NG_CITIES)
    date = fake.date_between(start_date="-4y", end_date="today")
    cust_rows.append([cid, name, city, date])

df_cust = pd.DataFrame(cust_rows, columns=["customer_id","customer_name","city","signup_date"])

# Inject 100 duplicates (repeat random existing rows)
dup_sample = df_cust.sample(100, random_state=1)
# Inject 100 rows with missing customer_name
null_rows = df_cust.sample(100, random_state=2).copy()
null_rows["customer_name"] = None

df_cust = pd.concat([df_cust, dup_sample, null_rows], ignore_index=True)
df_cust = df_cust.sample(frac=1, random_state=7).reset_index(drop=True)  # shuffle
df_cust.to_csv(os.path.join(BASE_DIR, "customers.csv"), index=False)
print(f"  customers.csv  → {len(df_cust):,} rows written")

# ────────────────────────────────────────────────────────────
# 2. PRODUCTS  (11,000 rows)
# ────────────────────────────────────────────────────────────
print("Generating products.csv …")

clean_prods = TARGET - 200
prod_rows = []
for i in range(1, clean_prods + 1):
    pid      = f"P{i:05d}"
    template = PRODUCTS[i % len(PRODUCTS)]
    cat, pname, base_price = template
    # Add slight price variation per product ID
    price = round(base_price * random.uniform(0.85, 1.15), 2)
    prod_rows.append([pid, pname, cat, price])

df_prod = pd.DataFrame(prod_rows, columns=["product_id","product_name","category","price"])

# Inject 100 duplicate product rows
dup_prod = df_prod.sample(100, random_state=3)
# Inject 100 rows with null/invalid price
null_price = df_prod.sample(100, random_state=4).copy()
null_price["price"] = None

df_prod = pd.concat([df_prod, dup_prod, null_price], ignore_index=True)
df_prod = df_prod.sample(frac=1, random_state=8).reset_index(drop=True)
df_prod.to_csv(os.path.join(BASE_DIR, "products.csv"), index=False)
print(f"  products.csv   → {len(df_prod):,} rows written")

# ────────────────────────────────────────────────────────────
# 3. ORDERS  (11,000 rows)
# ────────────────────────────────────────────────────────────
print("Generating orders.csv …")

# Use only valid IDs from the clean portion
valid_cust_ids = [f"C{i:05d}" for i in range(1, clean_customers + 1)]
valid_prod_ids = [f"P{i:05d}" for i in range(1, clean_prods + 1)]

clean_orders = TARGET - 300
order_rows = []
start_date = datetime(2021, 1, 1)
end_date   = datetime(2024, 12, 31)
date_range = (end_date - start_date).days

for i in range(1, clean_orders + 1):
    oid      = f"ORD{i:06d}"
    cid      = random.choice(valid_cust_ids)
    pid      = random.choice(valid_prod_ids)
    qty      = random.randint(1, 20)
    odate    = start_date + timedelta(days=random.randint(0, date_range))
    order_rows.append([oid, cid, pid, qty, odate.strftime("%Y-%m-%d")])

df_ord = pd.DataFrame(order_rows, columns=["order_id","customer_id","product_id","quantity","order_date"])

# Inject 100 duplicate orders
dup_ord = df_ord.sample(100, random_state=5)

# Inject 100 invalid orders (qty = 0 or negative, null qty, bad IDs)
invalid_rows = []
for j in range(100):
    oid   = f"ORD_BAD{j:04d}"
    issue = j % 4
    if issue == 0:   # zero quantity
        invalid_rows.append([oid, random.choice(valid_cust_ids), random.choice(valid_prod_ids), 0,    "2023-06-01"])
    elif issue == 1: # negative quantity
        invalid_rows.append([oid, random.choice(valid_cust_ids), random.choice(valid_prod_ids), -5,   "2023-07-15"])
    elif issue == 2: # null quantity
        invalid_rows.append([oid, random.choice(valid_cust_ids), random.choice(valid_prod_ids), None, "2023-08-20"])
    else:            # non-existent customer/product IDs
        invalid_rows.append([oid, "C99999", "P99999", 2, "2023-09-10"])

df_invalid = pd.DataFrame(invalid_rows, columns=["order_id","customer_id","product_id","quantity","order_date"])

df_ord = pd.concat([df_ord, dup_ord, df_invalid], ignore_index=True)
df_ord = df_ord.sample(frac=1, random_state=9).reset_index(drop=True)
df_ord.to_csv(os.path.join(BASE_DIR, "orders.csv"), index=False)
print(f"  orders.csv     → {len(df_ord):,} rows written")

print("\n✅ All three CSV files generated successfully.")
print(f"   customers.csv : {len(df_cust):,} rows")
print(f"   products.csv  : {len(df_prod):,} rows")
print(f"   orders.csv    : {len(df_ord):,} rows")