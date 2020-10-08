# Changelog

## 1.0.0
  * Major version change of primary `hash_key` fields for `periodic_data_calculated` and `periodic_data_standardized`. Added lookback window and data window overlap due to some changes not replicating.

## 0.0.4
  * Change `sync.py` for `periodic_data_calculated` to use `current_date` and not `latest_date` for `ReportedDate`, based on vendor recommendation.

## 0.0.3
  * Re-work `periodic_data_standardized`, bookmarks. Add `periodic_data_calculated`.

## 0.0.2
  * Major re-factor of code, bookmarking, performance.

## 0.0.1
  * Initial commit
