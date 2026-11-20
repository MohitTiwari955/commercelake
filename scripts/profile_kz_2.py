import pandas as pd

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 20)

path = r"data\raw\electronics\kz.csv"
id_cols = ["order_id", "product_id", "category_id", "user_id"]
df = pd.read_csv(path, dtype={c: "string" for c in id_cols})
df["event_time"] = pd.to_datetime(df["event_time"], utc=True)

start = pd.Timestamp("2020-04-01", tz="UTC")

# 1. The strange dates
early = df[df["event_time"] < start]
print("Rows before 2020-04-01:", len(early))
print(early["event_time"].dt.year.value_counts())
print(early.head(10))
print()

# 2. Rows per month (normal dates only)
ok = df[df["event_time"] >= start]
print("Rows per month:")
print(ok.groupby(ok["event_time"].dt.strftime("%Y-%m")).size())
print()

# 3. Do rows with a missing price also miss category_id?
both = (df["price"].isna() & df["category_id"].isna()).sum()
price_only = (df["price"].isna() & df["category_id"].notna()).sum()
print("Missing price AND missing category_id:", both)
print("Missing price but has category_id:    ", price_only)
print()

# 4. Zero-price rows
print("First zero-price rows:")
print(df[df["price"] == 0].head(10))
print()

# 5. Exact duplicate rows: when do they happen?
dup_rows = df[df.duplicated(keep=False)]
print("Rows involved in a repeat (all copies):", len(dup_rows))
print(dup_rows.groupby(dup_rows["event_time"].dt.strftime("%Y-%m")).size())