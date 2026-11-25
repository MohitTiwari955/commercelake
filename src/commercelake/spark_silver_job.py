"""Spark Silver job: one Bronze load folder -> Silver and Quarantine Parquet."""
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F

from commercelake.spark_silver import add_exact_repeat_flag, clean_bronze_spark

FLAG_COLUMNS = [
    "was_realigned", "event_time_is_placeholder", "price_is_zero",
    "is_uncatalogued_item", "is_exact_repeat",
]


def build_silver_spark(spark, bronze_load_dir, silver_dir):
    """Clean one Bronze load with Spark. Returns a summary dictionary."""
    bronze_load_dir = Path(bronze_load_dir)
    silver_dir = Path(silver_dir)
    silver_dir.mkdir(parents=True, exist_ok=True)

    target = silver_dir / bronze_load_dir.name
    if target.exists():
        return {"status": "skipped", "reason": "Silver already built from this Bronze load"}

    temp = silver_dir / ("_tmp-" + bronze_load_dir.name)
    if temp.exists():
        shutil.rmtree(temp)

    bronze = spark.read.parquet(str(bronze_load_dir / "part-*.parquet"))
    silver, quarantine = clean_bronze_spark(bronze)
    silver = add_exact_repeat_flag(silver).cache()
    quarantine = quarantine.cache()

    bronze_rows = bronze.count()
    silver_rows = silver.count()
    quarantine_rows = quarantine.count()

    # Every Bronze row must appear in exactly one output table
    if silver_rows + quarantine_rows != bronze_rows:
        raise RuntimeError("Row counts do not reconcile")

    silver.write.parquet(str(temp / "silver"))
    if quarantine_rows > 0:
        quarantine.write.parquet(str(temp / "quarantine"))

    # Independent check: count what was actually written
    written = spark.read.parquet(str(temp / "silver")).count()
    if written != silver_rows:
        raise RuntimeError("Written Silver rows differ: %d vs %d" % (written, silver_rows))

    totals = silver.agg(
        *[F.sum(F.col(name).cast("int")).alias(name) for name in FLAG_COLUMNS]
    ).first()
    summary = {
        "status": "built",
        "engine": "spark",
        "bronze_load": bronze_load_dir.name,
        "built_at": datetime.now(timezone.utc).isoformat(),
        "bronze_rows": bronze_rows,
        "silver_rows": silver_rows,
        "quarantine_rows": quarantine_rows,
        "flags": {name: int(totals[name] or 0) for name in FLAG_COLUMNS},
    }
    (temp / "_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    silver.unpersist()
    quarantine.unpersist()
    temp.rename(target)
    summary["folder"] = str(target)
    return summary