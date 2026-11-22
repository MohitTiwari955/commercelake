"""Silver job: one Bronze load folder -> Silver and Quarantine Parquet files."""
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from commercelake.silver import clean_bronze

FLAG_COLUMNS = [
    "was_realigned", "event_time_is_placeholder", "price_is_zero",
    "is_uncatalogued_item", "is_exact_repeat",
]


def write_in_parts(table, folder, rows_per_part):
    folder.mkdir(parents=True)
    for number, start in enumerate(range(0, len(table), rows_per_part)):
        chunk = table.iloc[start:start + rows_per_part]
        chunk.to_parquet(folder / ("part-%05d.parquet" % number), index=False)


def build_silver(bronze_load_dir, silver_dir, rows_per_part=250_000):
    """Clean one Bronze load. Returns a summary dictionary."""
    bronze_load_dir = Path(bronze_load_dir)
    silver_dir = Path(silver_dir)
    silver_dir.mkdir(parents=True, exist_ok=True)

    target = silver_dir / bronze_load_dir.name
    if target.exists():
        return {"status": "skipped", "reason": "Silver already built from this Bronze load"}

    temp = silver_dir / ("_tmp-" + bronze_load_dir.name)
    if temp.exists():
        shutil.rmtree(temp)
    temp.mkdir()

    parts = sorted(bronze_load_dir.glob("part-*.parquet"))
    bronze = pd.concat([pd.read_parquet(part) for part in parts], ignore_index=True)
    silver, quarantine = clean_bronze(bronze)

    # The promise of this layer: every Bronze row is in exactly one table
    if len(silver) + len(quarantine) != len(bronze):
        raise RuntimeError("Row counts do not reconcile")

    write_in_parts(silver, temp / "silver", rows_per_part)
    if len(quarantine) > 0:
        write_in_parts(quarantine, temp / "quarantine", rows_per_part)

    summary = {
        "status": "built",
        "bronze_load": bronze_load_dir.name,
        "built_at": datetime.now(timezone.utc).isoformat(),
        "bronze_rows": len(bronze),
        "silver_rows": len(silver),
        "quarantine_rows": len(quarantine),
        "flags": {name: int(silver[name].sum()) for name in FLAG_COLUMNS},
    }
    (temp / "_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    temp.rename(target)  # the build only "appears" once it is complete
    summary["folder"] = str(target)
    return summary