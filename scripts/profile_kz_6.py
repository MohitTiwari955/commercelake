import pandas as pd

path = r"data/raw/electronics/kz.csv"
id_cols = ["order_id", "product_id", "category_id", "user_id"]
df = pd.read_csv(path, dtype={c: "string" for c in id_cols})

shifted = df["category_id"].isna() & (
    pd.to_numeric(df["category_code"], errors="coerce").notna()
)
sh = df[shifted]
other = df[~shifted]

# Do these items sit inside orders that also contain normal products?
with_normal = sh["order_id"].isin(other["order_id"])
print("Shifted rows in orders that ALSO contain a normal row: %.1f%%" % (100 * with_normal.mean()))
print("Shifted rows in orders made ONLY of such rows:         %.1f%%" % (100 * (~with_normal).mean()))
print()

# How big are the orders that are made only of such rows?
only = sh[~with_normal]
sizes = only.groupby("order_id").size()
print("Orders made only of such rows:", len(sizes))
print(sizes.describe())