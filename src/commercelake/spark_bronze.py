"""Spark Bronze: raw CSV -> Parquet with lineage. Nothing is cleaned or fixed."""
import hashlib
import shutil
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from commercelake.bronze import SOURCE_COLUMNS


def sha256_of(path):
    """SHA-256 fingerprint of a file, read in 1 MiB blocks."""
    sha = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            sha.update(block)
    return sha.hexdigest()


def build_bronze_spark(spark, source, bronze_dir):
    """Load one raw CSV into Bronze Parquet. Returns a summary dictionary."""
    source = Path(source)
    bronze_dir = Path(bronze_dir)
    sha = sha256_of(source)

    target = bronze_dir / ("sha256-" + sha)
    if target.exists():
        return {"status": "skipped", "reason": "this file was already loaded"}

    bronze_dir.mkdir(parents=True, exist_ok=True)
    temp = bronze_dir / ("_tmp-" + sha)
    if temp.exists():
        shutil.rmtree(temp)

    now = datetime.now(timezone.utc)
    batch_id = now.strftime("%Y%m%dT%H%M%SZ") + "-" + sha[:8]

    # Number every line in file order (line 0 is the header)
    lines = spark.read.text(str(source))
    schema = StructType([StructField("value", StringType()),
                         StructField("line_number", LongType())])
    numbered = spark.createDataFrame(
        lines.rdd.zipWithIndex().map(lambda pair: (pair[0]["value"], pair[1])), schema
    )
    header = numbered.where("line_number = 0").first()["value"]
    if header != ",".join(SOURCE_COLUMNS):
        raise ValueError("Unexpected header: " + header)

    # Split into raw fields; a missing field becomes null (nothing is fixed)
    fields = F.split(F.col("value"), ",", -1)
    bronze = numbered.where("line_number > 0").select(
        *[F.try_element_at(fields, F.lit(i + 1)).alias(name)
          for i, name in enumerate(SOURCE_COLUMNS)],
        F.size(fields).alias("_field_count"),
        F.col("line_number").alias("_source_row_number"),
        F.lit(source.name).alias("_source_file"),
        F.lit(now).alias("_ingested_at"),
        F.lit(batch_id).alias("_batch_id"),
        F.lit(sha).alias("_file_sha256"),
    )

    bronze.write.parquet(str(temp))

    expected = lines.count() - 1
    written = spark.read.parquet(str(temp)).count()
    if written != expected:
        raise RuntimeError("Row counts do not reconcile: %d vs %d" % (written, expected))

    temp.rename(target)
    return {"status": "loaded", "batch_id": batch_id, "rows": written, "folder": str(target)}