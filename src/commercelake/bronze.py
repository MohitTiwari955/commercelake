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