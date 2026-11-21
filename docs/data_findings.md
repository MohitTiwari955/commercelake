# Data findings: kz.csv (electronics purchases)

| # | Finding | Evidence | Status |
|---|---------|----------|--------|
| 1 | 2,633,521 rows, 8 columns | profile_kz.py | confirmed |
| 2 | Dates run Jan 2020 to Nov 2020, not Apr to Nov as Kaggle states (about 904k rows are Jan to Mar) | profile_kz_3.py | confirmed |
| 3 | 19,631 rows have placeholder time 1970-01-01 00:33:40 | profile_kz_3.py | confirmed |
| 4 | 431,954 rows (16.4%) have shifted columns: price sits in category_code, user_id sits in brand (only 38,619 of them have a user ID). Rule: category_id empty and category_code numeric. No numbers hiding in category_code or brand in any other row. | profile_kz_4.py | confirmed |
| 5 | 675 exact repeat rows (about 0.03%), none before April 2020 | profile_kz_2.py | confirmed, cause unknown |
| 6 | 121 rows have price 0, concentrated in a few products (counted before realignment) | profile_kz_2.py, profile_kz_3.py | confirmed |
| 7 | Hundreds of rows share one exact timestamp, e.g. 208 at 2020-03-27 16:30:01 | profile_kz_3.py | hypothesis: batch job |
| 8 | Shifted rows occur in every month: about 20% of rows in Jan to Mar and June, only 4.5% in April | profile_kz_4.py | confirmed, cause unknown |
| 9 | Five prices make up 56% of shifted rows (16.18 alone is 72,969 rows) | profile_kz_4.py | hypothesis: a few cheap products |
| 10 | 95.0% of shifted rows sit in orders that also contain a normal row; 19,069 orders (5.0% of shifted rows) consist only of such items. Consistent with add-ons such as fees or services; the real nature is unknown. | profile_kz_6.py | supported, not proven |