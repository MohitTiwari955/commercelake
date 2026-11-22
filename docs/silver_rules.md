# Silver rules: electronics purchases

Input: Bronze load sha256-5ac5727e... (2,633,521 rows).
Output: Silver (2,633,521 rows) and Quarantine (0 rows).
Promise: Silver rows + Quarantine rows = Bronze rows, checked in code on every build.

| Rule | Detail | Rows affected |
|---|---|---|
| Empty text becomes null | Applies to every source column | n/a |
| Realign short lines | Lines with 6 fields: price taken from category_code, user_id from brand, category_code and brand set to null, was_realigned = true | 431,954 |
| Placeholder time | 1970-01-01 00:33:40 UTC: event_time set to null, event_time_is_placeholder = true | 19,631 |
| Zero price | price_is_zero = true | 3,842 |
| No category | is_uncatalogued_item = true | 431,954 |
| Exact repeat | Identical to an earlier row on the 8 business columns. The first copy is not flagged. is_exact_repeat = true | 675 |
| Quarantine | Unexpected field count, odd short-line layout, missing order or product ID, unreadable or negative price, event time outside 2020 (placeholder excepted) | 0 |