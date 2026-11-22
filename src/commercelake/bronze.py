"""Bronze layer: read the raw source file safely, exactly as it arrived."""
import csv
import hashlib

SOURCE_COLUMNS = [
    "event_time", "order_id", "product_id", "category_id",
    "category_code", "brand", "price", "user_id",
]


def file_sha256(path):
    """Fingerprint of a file, read in 1 MB pieces."""
    sha = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            sha.update(block)
    return sha.hexdigest()


def read_batches(path, batch_size=250_000):
    """Yield (first_row_number, rows) batches of raw rows from a CSV file.

    Row numbers start at 1 for the first data row (the header is not counted).
    Each row is the list of text fields exactly as found on that line.
    """
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        if header != SOURCE_COLUMNS:
            raise ValueError("Unexpected header: %s" % header)
        rows = []
        first_row_number = 1
        for row_number, row in enumerate(reader, start=1):
            rows.append(row)
            if len(rows) == batch_size:
                yield first_row_number, rows
                rows = []
                first_row_number = row_number + 1
        if rows:
            yield first_row_number, rows
            
LINEAGE_COLUMNS = [
    "_source_file", "_source_row_number", "_field_count",
    "_ingested_at", "_batch_id", "_file_sha256",
]


def make_bronze_columns(first_row_number, rows, source_file, sha256, batch_id, ingested_at):
    """Turn raw rows into columns: the 8 source columns plus lineage columns.

    Nothing is cleaned or fixed. A line with too few fields gets None (null)
    for the missing ones. A line with too many fields raises an error instead
    of silently losing data.
    """
    width = len(SOURCE_COLUMNS)
    data = {name: [] for name in SOURCE_COLUMNS + LINEAGE_COLUMNS}
    for offset, row in enumerate(rows):
        row_number = first_row_number + offset
        if len(row) > width:
            raise ValueError(
                "Row %d has %d fields, expected at most %d" % (row_number, len(row), width)
            )
        for position, name in enumerate(SOURCE_COLUMNS):
            data[name].append(row[position] if position < len(row) else None)
        data["_source_file"].append(source_file)
        data["_source_row_number"].append(row_number)
        data["_field_count"].append(len(row))
        data["_ingested_at"].append(ingested_at)
        data["_batch_id"].append(batch_id)
        data["_file_sha256"].append(sha256)
    return data