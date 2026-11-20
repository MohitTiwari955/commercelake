import pandas as pd

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 20)
pd.set_option("display.float_format", "{:,.2f}".format)

path = r"data\raw\electronics\kz.csv"
id_cols = ["order_id", "product_id", "category_id", "user_id"]
df = pd.read_csv(path, dtype={c: "string" for c in id_cols})
df["event_time"] = pd.to_datetime(df["event_time"], utc=True)


def is_number(s):
    return pd.to_numeric(s, errors="coerce").notna()


def long_digits(s):
    return s.str.fullmatch(r"\d{15,}").fillna(False)


shifted = df["category_id"].isna() & is_number(df["category_code"])
print("Shifted rows:", shifted.sum())
print("Share of file: %.1f%%" % (100 * shifted.mean()))
print()

# In shifted rows: does brand hold a long ID-like number?
sh = df[shifted]
print("Shifted rows where brand looks like a user ID:", long_digits(sh["brand"]).sum())
print("Shifted rows where brand is empty:", sh["brand"].isna().sum())
print()

# In all OTHER rows: any numbers hiding in category_code or brand?
other = df[~shifted]
print("Other rows with a number in category_code:", is_number(other["category_code"]).sum())
print("Other rows with an ID-like number in brand:", long_digits(other["brand"]).sum())
print()

# Which months do shifted rows fall in?
print("Shifted rows per month:")
print(sh.groupby(sh["event_time"].dt.strftime("%Y-%m")).size())
print()

# What would the price be after realignment?
fixed_price = pd.to_numeric(sh["category_code"], errors="coerce")
print("Realigned price - summary:")
print(fixed_price.describe())
print("Most common realigned prices:")
print(fixed_price.value_counts().head(5))