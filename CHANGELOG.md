# Changelog

## [v1.0.2](https://github.com/singer-io/tap-ilevel/tree/v1.0.2) (2021-04-21)

[Full Changelog](https://github.com/singer-io/tap-ilevel/compare/v1.0.1...v1.0.2)

**Merged pull requests:**

- Fix circle to work with python 3.8 + pylint warnings [\#9](https://github.com/singer-io/tap-ilevel/pull/9) ([asaf-erlich](https://github.com/asaf-erlich))
- Troubleshooting [\#8](https://github.com/singer-io/tap-ilevel/pull/8) ([pbegle](https://github.com/pbegle))

## 1.0.1
  * Adjust `sync.py` to adjust when/how we bookmark endpoint `periodic_data_standardized`. Currently, we are bookmarking too early and filtering out some records.

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
