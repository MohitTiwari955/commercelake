import pandas as pd

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 20)

path = r"data\raw\electronics\kz.csv"
df = pd.read_csv(path, nrows=1000)   # read only the first 1000 rows

print("Shape (rows, columns):", df.shape)
print()
print("Column types:")
print(df.dtypes)
print()
print("First 5 rows:")
print(df.head())
print()
print("Empty values per column:")
print(df.isna().sum())
