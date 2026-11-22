"""Bronze ingestion job: raw CSV -> Parquet files with lineage columns."""
import shutil
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from commercelake.bronze import SOURCE_COLUMNS, file_sha256, make_bronze_columns, read_batches

BRONZE_SCHEMA = pa.schema(
    [(name, pa.string()) for name in SOURCE_COLUMNS]
    + [
        ("_source_file", pa.string()),
        ("_source_row_number", pa.int64()),
        ("_field_count", pa.int32()),
        ("_ingested_at", pa.timestamp("us", tz="UTC")),
        ("_batch_id", pa.string()),
        ("_file_sha256", pa.string()),
    ]
)


def ingest_file(source_path, bronze_dir, batch_size=250_000):
    """Load one raw CSV file into Bronze. Returns a small summary dictionary."""
    source_path = Path(source_path)
    bronze_dir = Path(bronze_dir)
    bronze_dir.mkdir(parents=True, exist_ok=True)

    sha256 = file_sha256(source_path)
    target = bronze_dir / ("sha256-" + sha256)
    if target.exists():
        return {"status": "skipped", "reason": "this exact file is already in Bronze", "sha256": sha256}

    # Write into a temporary folder first, so a crash never leaves half a load behind
    temp = bronze_dir / ("_tmp-" + sha256)
    if temp.exists():
        shutil.rmtree(temp)
    temp.mkdir()

    ingested_at = datetime.now(timezone.utc)
    batch_id = ingested_at.strftime("%Y%m%dT%H%M%SZ") + "-" + sha256[:8]

    rows_written = 0
    parts = 0
    for first_row, rows in read_batches(source_path, batch_size):
        columns = make_bronze_columns(first_row, rows, source_path.name, sha256, batch_id, ingested_at)
        table = pa.Table.from_pydict(columns, schema=BRONZE_SCHEMA)
        pq.write_table(table, str(temp / ("part-%05d.parquet" % parts)))
        rows_written += len(rows)
        parts += 1

    temp.rename(target)  # the load only "appears" once it is complete
    return {
        "status": "loaded",
        "batch_id": batch_id,
        "sha256": sha256,
        "rows": rows_written,
        "parts": parts,
        "folder": str(target),
    }