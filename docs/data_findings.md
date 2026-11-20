# Data findings: kz.csv (electronics purchases)

| # | Finding | Evidence | Status |
|---|---------|----------|--------|
| 1 | 2,633,521 rows, 8 columns | profile_kz.py | confirmed |
| 2 | Dates run Jan 2020 to Nov 2020, not Apr to Nov as Kaggle states (about 904k rows are Jan to Mar) | profile_kz_3.py | confirmed |
| 3 | 19,631 rows have placeholder time 1970-01-01 00:33:40 | profile_kz_3.py | confirmed |
| 4 | 431,954 rows have shifted columns: price sits in category_code, user_id sits in brand | profile_kz_3.py, profile_kz_4.py | verify after script 4 |
| 5 | 675 exact repeat rows (about 0.03%), none before April 2020 | profile_kz_2.py | confirmed, cause unknown |
| 6 | 121 rows have price 0, concentrated in a few products (counted before realignment) | profile_kz_2.py, profile_kz_3.py | confirmed |
| 7 | Hundreds of rows share one exact timestamp, e.g. 208 at 2020-03-27 16:30:01 | profile_kz_3.py | hypothesis: batch job |