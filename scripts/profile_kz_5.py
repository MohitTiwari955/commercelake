import pandas as pd

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 20)
pd.set_option("display.float_format", "{:,.2f}".format)

path = r"data\raw\electronics\kz.csv"
id_cols = ["order_id", "product_id", "category_id", "user_id"]
df = pd.read_csv(path, dtype={c: "string" for c in id_cols})

shifted = df["category_id"].isna() & (
    pd.to_numeric(df["category_code"], errors="coerce").notna()
)
sh = df[shifted].copy()
other = df[~shifted]
sh["price_fixed"] = pd.to_numeric(sh["category_code"])

# 1. How concentrated are the shifted rows on a few products?
print("Distinct products in shifted rows:", sh["product_id"].nunique())
print("Distinct products in other rows:  ", other["product_id"].nunique())
top = sh["product_id"].value_counts().head(5)
print(top)
print("Top 5 products = %.1f%% of shifted rows" % (100 * top.sum() / len(sh)))
print()

# 2. Do the shifted rows' products also appear in normal rows?
overlap = sh["product_id"].isin(other["product_id"]).mean()
print("Shifted rows whose product also appears in other rows: %.1f%%" % (100 * overlap))
print()

# 3. Realigned prices of those top products
print(sh[sh["product_id"].isin(top.index)]
      .groupby("product_id")["price_fixed"]
      .agg(["count", "min", "median", "max"]))