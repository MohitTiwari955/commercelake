# Data findings: kz.csv (electronics purchases)

| # | Finding | Evidence | Status |
|---|---------|----------|--------|
| 1 | 2,633,521 rows, 8 columns | profile_kz.py | confirmed |
| 2 | Dates run Jan 2020 to Nov 2020, not Apr to Nov as Kaggle states (about 904k rows are Jan to Mar) | profile_kz_3.py | confirmed |
| 3 | 19,631 rows have placeholder time 1970-01-01 00:33:40 | profile_kz_3.py | confirmed |
| 4 | 431,954 rows (16.4%) have shifted columns: price sits in category_code, user_id sits in brand. They are exactly the raw lines with 6 fields instead of 8 (empty fields were collapsed when the file was written). | profile_kz_4.py, make_manifest.py | confirmed |
| 5 | 675 exact repeat rows (about 0.03%), none before April 2020 | profile_kz_2.py | confirmed, cause unknown |
| 6 | 121 rows have price 0, concentrated in a few products (counted before realignment) | profile_kz_2.py, profile_kz_3.py | confirmed |
| 7 | Hundreds of rows share one exact timestamp, e.g. 208 at 2020-03-27 16:30:01 | profile_kz_3.py | hypothesis: batch job |
| 8 | Shifted rows occur in every month: about 20% of rows in Jan to Mar and June, only 4.5% in April | profile_kz_4.py | confirmed, cause unknown |
| 9 | Five prices make up 56% of shifted rows (16.18 alone is 72,969 rows) | profile_kz_4.py | hypothesis: a few cheap products |
| 10 | 95.0% of shifted rows sit in orders that also contain a normal row; 19,069 orders (5.0% of shifted rows) consist only of such items. Consistent with add-ons such as fees or services; the real nature is unknown. | profile_kz_6.py | supported, not proven |
| 11 | Silver build on the full file: 2,633,521 Bronze rows become 2,633,521 Silver rows and 0 quarantine rows. Realigned rows 431,954, placeholder times 19,631 and exact repeats 675 all match profiling. | check_silver.py | confirmed |
| 12 | After realignment, 3,842 rows have price 0 (profiling saw 121 before it). About 3,721 of them are realigned rows (calculated: 3,842 minus 121). | run_silver.py | confirmed total, split calculated |
- Size: 298,649,047 bytes
- SHA-256: 5ac5727e6830084806f051d2d09a72570367e2b2ee8d8f3ff9f8096ca72f07fe
- Data rows: 2,633,521 (header excluded)
- Rows by number of raw fields: 8 fields: 2,201,567; 6 fields: 431,954
- Measured with: scripts/make_manifest.py