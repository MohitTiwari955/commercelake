# PySpark migration: Bronze and Silver (electronics purchases)

Date: 25 November 2026. Source: `kz.csv` (REES46 Electronics Purchase History).
Environment: WSL2 Ubuntu 24.04, Python 3.12.3, Java 17, PySpark 4.1.3, local mode (`local[*]`).

Goal: rebuild the Pandas Bronze and Silver in PySpark and prove the results match.
The Pandas pipeline stays unchanged and is the reference ("answer key").

## Results (all observed in terminal output)

| Measure | Pandas (answer key) | Spark |
|---|---:|---:|
| Source data rows | 2,633,521 | 2,633,521 |
| Raw lines with 8 fields | 2,201,567 | 2,201,567 |
| Raw lines with 6 fields | 431,954 | 431,954 |
| Bronze rows | 2,633,521 | 2,633,521 |
| Silver rows | 2,633,521 | 2,633,521 |
| Quarantine rows | 0 | 0 |
| `was_realigned` | 431,954 | 431,954 |
| `event_time_is_placeholder` | 19,631 | 19,631 |
| `price_is_zero` | 3,842 | 3,842 |
| `is_uncatalogued_item` | 431,954 | 431,954 |
| `is_exact_repeat` | 675 | 675 |

Row-by-row comparisons, joined on `_source_row_number`:
- Spark Bronze vs Pandas Bronze: 2,633,521 rows matched, 0 rows differ in any source column.
- Spark Silver vs Pandas Silver: 2,633,521 rows matched, 0 differences in all 13 compared columns
  (price compared after casting the Pandas float to `decimal(18,4)`).

Tests: 38 passed (22 Pandas, 4 Spark Bronze, 12 Spark Silver).

## How to run (Ubuntu, from `/mnt/d/commercelake`, with `.venv-linux` active)

```bash
python scripts/run_bronze_spark.py      # -> data/bronze_spark/electronics_purchases
python scripts/check_bronze_spark.py    # compares with Pandas Bronze
python scripts/run_silver_spark.py      # -> data/silver_spark/electronics_purchases
python scripts/check_silver_spark.py    # compares with Pandas Silver
pytest
```

Both run scripts print `skipped` if the output folder for this file fingerprint already exists.
To rebuild after a rule change, delete the matching folder under `data/bronze_spark` or
`data/silver_spark` first (never the Pandas folders).

## Design decisions

- The Spark CSV reader hides short lines, so Bronze reads the file as plain text and splits each
  line itself. Missing fields stay null; empty fields stay empty text. Nothing is fixed in Bronze.
- Row numbers come from `rdd.zipWithIndex()` so they are consecutive and follow file order
  (header = line 0, first data row = 1), matching Pandas Bronze.
- Silver uses `try_cast` and `try_to_timestamp` because Spark 4 raises errors on bad values in ANSI
  mode. Unreadable values become null and are caught by the quarantine checks.
- The Spark session must use UTC (`spark.sql.session.timeZone = UTC`).
- Price is `decimal(18,4)`, not float.
- Exact repeats use a window over the eight business columns ordered by `_source_row_number`, so
  repeats are found across partitions. A per-partition check would miss them.
- Quarantine keeps the original Bronze values plus `quarantine_reason`.

## Known limitations

- Local mode only. Nothing has run on AWS Glue. Glue 6.0 lists Spark 4.1.1 and Python 3.13, the
  local setup is Spark 4.1.3 and Python 3.12.3, so compatibility must be tested before any claim.
- Output is Parquet. Iceberg tables are not implemented yet.
- The skip rule is tied to the file fingerprint and does not version the Silver rules.
- Both Silver builds work on the one 2.6M-row electronics file. The 285M-row source is not touched.
- A harmless `BrokenPipeError` message appeared once in a Spark preview run; results were correct.