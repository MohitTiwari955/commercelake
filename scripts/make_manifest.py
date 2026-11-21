import csv
import hashlib
from collections import Counter
from pathlib import Path

path = Path(r"data\raw\electronics\kz.csv")

# 1. Fingerprint of the file (SHA-256): changes if even one byte changes
sha = hashlib.sha256()
with open(path, "rb") as f:
    for block in iter(lambda: f.read(1024 * 1024), b""):
        sha.update(block)

# 2. Count rows and how many fields each raw line has
field_counts = Counter()
with open(path, newline="", encoding="utf-8") as f:
    reader = csv.reader(f)
    header = next(reader)
    for row in reader:
        field_counts[len(row)] += 1

print("File:", path.name)
print("Size (bytes):", path.stat().st_size)
print("SHA-256:", sha.hexdigest())
print("Header:", header)
print("Data rows:", sum(field_counts.values()))
print("Rows by number of fields:", dict(field_counts))