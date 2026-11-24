from pathlib import Path

import pandas as pd

from commercelake.silver import clean_bronze

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 20)

folder = Path(r"data/bronze/electronics_purchases")
first_part = sorted(folder.glob("sha256-*/part-00000.parquet"))[0]
bronze = pd.read_parquet(first_part)
silver, quarantine = clean_bronze(bronze)

print("Bronze rows:     ", len(bronze))
print("Silver rows:     ", len(silver))
print("Quarantine rows: ", len(quarantine))
print()
print("Flags (number of True):")
print(silver[["was_realigned", "event_time_is_placeholder", "price_is_zero", "is_uncatalogued_item"]].sum())
print()
print("Silver column types:")
print(silver.dtypes)
print()
print("First 2 Silver rows, turned sideways:")
print(silver.head(2).T)