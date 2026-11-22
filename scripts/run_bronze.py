from commercelake.ingest import ingest_file

summary = ingest_file(
    source_path=r"data\raw\electronics\kz.csv",
    bronze_dir=r"data\bronze\electronics_purchases",
    batch_size=250_000,
)
for key, value in summary.items():
    print("%-8s %s" % (key, value))