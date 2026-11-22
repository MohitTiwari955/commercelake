from pathlib import Path

from commercelake.silver_job import build_silver

bronze_root = Path(r"data\bronze\electronics_purchases")
bronze_load = sorted(bronze_root.glob("sha256-*"))[0]

summary = build_silver(bronze_load, r"data\silver\electronics_purchases")
for key, value in summary.items():
    print("%-16s %s" % (key, value))