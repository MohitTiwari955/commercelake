import pandas as pd

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 20)

path = r"data\raw\electronics\kz.csv"
df = pd.read_csv(path)

print("Rows:", len(df))
print("Columns:", list(df.columns))
print()

# 1. Time range (convert the text into real timestamps first)
df["event_time"] = pd.to_datetime(df["event_time"], utc=True)
print("Earliest event:", df["event_time"].min())
print("Latest event:  ", df["event_time"].max())
print()

# 2. How many distinct values in each ID column?
for col in ["order_id", "product_id", "category_id", "user_id"]:
    print(col, "distinct:", df[col].nunique())
print()

# 3. Rows per order
rows_per_order = df.groupby("order_id").size()
print("Rows per order - summary:")
print(rows_per_order.describe())
print("Orders with exactly 1 row:", (rows_per_order == 1).sum())
print()

# 4. Exact duplicate rows
dupes = df.duplicated().sum()
print("Rows that exactly repeat an earlier row:", dupes)
print("Share of all rows: %.1f%%" % (100 * dupes / len(df)))
print()

# 5. Missing values
missing = df.isna().sum()
print(pd.DataFrame({"missing": missing, "percent": (100 * missing / len(df)).round(1)}))
print()

# 6. Price
print(df["price"].describe())
print("Rows with price <= 0:", (df["price"] <= 0).sum())