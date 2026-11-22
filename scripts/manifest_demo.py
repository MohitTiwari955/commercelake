import csv
import hashlib
import pandas as pd
from pathlib import Path

folder = Path(r"data\scratch")
folder.mkdir(parents=True, exist_ok=True)
tiny = folder / "tiny.csv"

tiny.write_text(
    "event_time,order_id,product_id,category_id,category_code,brand,price,user_id\n"
    "2020-04-24 11:50:39 UTC,100,555,777,electronics.tablet,samsung,162.01,999\n"
    "2020-05-02 11:12:20 UTC,101,556,,0.02,888\n",
    encoding="utf-8",
)

# 1. How many fields does each raw line have?
print("Fields per raw line:")
with open(tiny, newline="", encoding="utf-8") as f:
    for number, row in enumerate(csv.reader(f)):
        print("  line", number, "->", len(row), "fields:", row)
print()

# 2. What pandas shows for the same file
print("What pandas shows:")
print(pd.read_csv(tiny, dtype="string").to_string())
print()


# 3. A fingerprint changes if one character changes
def fingerprint(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


print("Fingerprint before:", fingerprint(tiny))
text = tiny.read_text(encoding="utf-8").replace("162.01", "162.02")
tiny.write_text(text, encoding="utf-8")
print("Fingerprint after: ", fingerprint(tiny))
