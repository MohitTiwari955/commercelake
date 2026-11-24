from datetime import datetime, timezone
from pathlib import Path

from commercelake.bronze import make_bronze_columns, read_batches

path = Path(r"data/raw/electronics/kz.csv")

# Take only the first chunk of 5,000 rows
first_row, rows = next(read_batches(path, batch_size=5000))
columns = make_bronze_columns(
    first_row_number=first_row,
    rows=rows,
    source_file=path.name,
    sha256="(not computed in this demo)",
    batch_id="demo",
    ingested_at=datetime.now(timezone.utc),
)


def show(index):
    print("--- row", index, "---")
    for name, values in columns.items():
        print("%-20s %r" % (name, values[index]))


short = columns["_field_count"].index(6)
show(0)
show(short)