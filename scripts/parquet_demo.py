import pandas as pd
from pathlib import Path

src = Path(r"data/raw/electronics/kz.csv")
out_dir = Path(r"data/scratch")
out_dir.mkdir(parents=True, exist_ok=True)

# Read the first 200,000 rows, every column as text (no guessing)
df = pd.read_csv(src, nrows=200_000, dtype="string")
print("Rows loaded:", len(df))

csv_out = out_dir / "sample.csv"
parquet_out = out_dir / "sample.parquet"
df.to_csv(csv_out, index=False)
df.to_parquet(parquet_out, index=False)

csv_mb = csv_out.stat().st_size / 1_000_000
pq_mb = parquet_out.stat().st_size / 1_000_000
print("CSV size (MB):    ", round(csv_mb, 2))
print("Parquet size (MB):", round(pq_mb, 2))
print("Parquet is about %.1f times smaller" % (csv_mb / pq_mb))
print()

back = pd.read_parquet(parquet_out)
print("Read back:", back.shape)
print(back.dtypes)
print()

two = pd.read_parquet(parquet_out, columns=["order_id", "price"])
print("Only two columns read:")
print(two.head())