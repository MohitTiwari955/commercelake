import pandas as pd

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 20)

path = r"data\raw\electronics\kz.csv"
id_cols = ["order_id", "product_id", "category_id", "user_id"]
df = pd.read_csv(path, dtype={c: "string" for c in id_cols})
df["event_time"] = pd.to_datetime(df["event_time"], utc=True)

# 1. Rows per month, ALL dates
print("Rows per month (all dates):")
print(df.groupby(df["event_time"].dt.strftime("%Y-%m")).size())
print()

# 2. Most common timestamps among rows before April 2020
start = pd.Timestamp("2020-04-01", tz="UTC")
early = df[df["event_time"] < start]
print("Most common timestamps before April 2020:")
print(early["event_time"].value_counts().head(5))
print()

# 3. Do rows with a missing category_id have numbers in category_code?
no_cat = df[df["category_id"].isna()]
looks_numeric = pd.to_numeric(no_cat["category_code"], errors="coerce").notna()
print("Rows with missing category_id:", len(no_cat))
print("...of which category_code looks like a number:", looks_numeric.sum())
print(no_cat[looks_numeric].head(5))
print()

# 4. Which products have price 0?
zero = df[df["price"] == 0]
print("Top products among price-0 rows:")
print(zero["product_id"].value_counts().head(5))