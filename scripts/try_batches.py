from collections import Counter
from pathlib import Path

from commercelake.bronze import read_batches

path = Path(r"data\raw\electronics\kz.csv")

total = 0
field_counts = Counter()
for batch_number, (first_row, rows) in enumerate(read_batches(path), start=1):
    total += len(rows)
    for row in rows:
        field_counts[len(row)] += 1
    print("Batch", batch_number, "starts at row", first_row, "with", len(rows), "rows")

print()
print("Total rows:", total)
print("Rows by number of fields:", dict(field_counts))