import json
from pathlib import Path

import pyarrow.parquet as pq

silver_root = Path(r"data\silver\electronics_purchases")
load = sorted(silver_root.glob("sha256-*"))[0]
summary = json.loads((load / "_summary.json").read_text(encoding="utf-8"))

# Count rows straight from the Parquet files, without trusting the summary
silver_files = sorted((load / "silver").glob("part-*.parquet"))
silver_rows = sum(pq.ParquetFile(str(f)).metadata.num_rows for f in silver_files)

# Numbers we measured during profiling (docs/data_findings.md)
expected = {
    "rows": 2633521,
    "was_realigned": 431954,
    "event_time_is_placeholder": 19631,
    "is_exact_repeat": 675,
}
actual = {
    "rows": silver_rows + summary["quarantine_rows"],
    "was_realigned": summary["flags"]["was_realigned"],
    "event_time_is_placeholder": summary["flags"]["event_time_is_placeholder"],
    "is_exact_repeat": summary["flags"]["is_exact_repeat"],
}

for name, want in expected.items():
    got = actual[name]
    print("%-28s expected %9d  got %9d  %s" % (name, want, got, "OK" if got == want else "DIFFERENT"))

print()
print("Silver rows:", silver_rows, "  Quarantine rows:", summary["quarantine_rows"])
print("Other flags:", {k: v for k, v in summary["flags"].items() if k not in expected})