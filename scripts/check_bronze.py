from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

folder = Path(r"data/bronze/electronics_purchases")
wanted = ["_source_row_number", "_field_count", "_batch_id", "_file_sha256"]

# Read only the four lineage columns we need, from every part file
parts = sorted(folder.glob("sha256-*/part-*.parquet"))
table = pa.concat_tables([pq.read_table(str(part), columns=wanted) for part in parts])
df = table.to_pandas()

print("Part files:", len(parts))
print("Rows in Bronze:", len(df))
print("Distinct row numbers:", df["_source_row_number"].nunique())
print("First and last row number:", df["_source_row_number"].min(), df["_source_row_number"].max())
print("Rows by number of fields:")
print(df["_field_count"].value_counts())
print("Distinct batch IDs:", df["_batch_id"].nunique())
print("Distinct file fingerprints:", df["_file_sha256"].nunique())