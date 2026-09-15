"""Quick check: đếm dòng và xem 5 dòng đầu của 2 file CSV thô."""
import duckdb

con = duckdb.connect()

print("=" * 60)
print("TIKTOK ORDERS")
print("=" * 60)
r = con.sql("SELECT count(*) as rows FROM read_csv_auto('data/raw/tiktok_orders.csv')").fetchone()
print(f"Total rows: {r[0]}")
cols = con.sql("SELECT * FROM read_csv_auto('data/raw/tiktok_orders.csv') LIMIT 0")
print(f"Total columns: {len(cols.columns)}")
print(f"Columns: {cols.columns}")
print()
con.sql("SELECT * FROM read_csv_auto('data/raw/tiktok_orders.csv') LIMIT 3").show()

print()
print("=" * 60)
print("SHOPEE ORDERS")
print("=" * 60)
r2 = con.sql("SELECT count(*) as rows FROM read_csv_auto('data/raw/shopee_orders.csv')").fetchone()
print(f"Total rows: {r2[0]}")
cols2 = con.sql("SELECT * FROM read_csv_auto('data/raw/shopee_orders.csv') LIMIT 0")
print(f"Total columns: {len(cols2.columns)}")
print(f"Columns: {cols2.columns}")
print()
con.sql("SELECT * FROM read_csv_auto('data/raw/shopee_orders.csv') LIMIT 3").show()

con.close()
