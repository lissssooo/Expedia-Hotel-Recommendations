# Distance imputation report

Build timestamp: `2026-08-13T22:11:07+00:00`
Holdout: deterministic 10% of observed distances (`hash(source_dataset, source_row_id) % 10 = 0`)
Estimator: group median
Selected minimum support: **5**

## Coverage

| metric | rows | percent |
|---|---:|---:|
| CORE events | 40,197,567 | 100.000 |
| missing in source | 14,371,504 | 35.752 |
| filled after CORE | 31,064,812 | 77.280 |
| imputed | 5,238,749 | 13.033 |
| final NULL | 9,132,755 | 22.720 |

## Holdout validation at selected support

| level | support | coverage % | MAE | median AE | p90 AE |
|---|---:|---:|---:|---:|---:|
| city_destination | 5 | 84.250 | 29.411 | 0.638 | 7.742 |
| city_market | 5 | 92.860 | 34.434 | 0.962 | 12.275 |
| region_destination | 5 | 96.715 | 73.696 | 13.777 | 141.268 |
| region_market | 5 | 99.403 | 77.366 | 15.483 | 143.004 |
| country_destination | 5 | 99.384 | 473.723 | 270.235 | 1253.243 |
| country_market | 5 | 99.951 | 480.692 | 282.482 | 1263.721 |
| country_hotel_country | 5 | 99.994 | 586.874 | 387.254 | 1480.012 |

At support 1 and 3, sparse groups have worse error; support 5 is the first tested threshold with a stable error/coverage trade-off. Support 10 and above reduce coverage materially, so support 5 is retained as the reproducible technical threshold. The country-level candidates are not applied: their selected-support p90 errors are materially broader than the city/region levels. No global mean or zero imputation is used.

## Applied hierarchy

| level | imputed rows |
|---|---:|
| region_destination | 3,668,813 |
| city_destination | 906,903 |
| city_market | 344,327 |
| region_market | 318,706 |

`distance_raw` is always preserved. `distance_filled` equals it for observed values, contains a validated median for imputed values, and remains NULL when no hierarchy group reaches minimum support. `distance_is_imputed` and `distance_imputation_level` provide provenance.
