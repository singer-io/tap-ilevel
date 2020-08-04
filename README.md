# tap-ilevel

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from the [iLevel Portfolio Monitoring Platform](https://ihsmarkit.com/products/ilevel.html) by **IHS Markit** using a SOAP API [WSDL](https://services.ilevelsolutions.com/DataService/Service/2019/Q1/DataService.svc?singleWsdl). Documentation provided in [Web Services Guide](https://github.com/bytecodeio/tap-ilevel/blob/master/web_services_guide-2019Q1.pdf) (PDF).
- Extracts the following resources:
  - assets
  - data_items
  - funds
  - investments
  - scenarios
  - securities
  - investment_transactions
  - periodic_data_standardized
  - periodic_data_calculated
  - relations:
    - asset_to_asset_relations
    - fund_to_asset_relations
    - fund_to_fund_relations
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

## Quick Start

1. Install

    Clone this repository, and then install using setup.py. We recommend using a virtualenv:

    ```bash
    > virtualenv -p python3 venv
    > source venv/bin/activate
    > python setup.py install
    OR
    > cd .../tap-ilevel
    > pip install .
    ```
2. Dependent libraries
    The following dependent libraries were installed.
    ```bash
    > pip install singer-python
    > pip install singer-tools
    > pip install target-stitch
    > pip install suds-jurko
    
    ```
    - [singer-tools](https://github.com/singer-io/singer-tools)
    - [target-stitch](https://github.com/singer-io/target-stitch)

3. Create your tap's `config.json` file. The `subdomain` is everything before `.ilevel.com` in the ilevel instance URL.  For the URL: `https://stitch.sandbox.ilevel.com`, the subdomain would be `stitch.sandbox`.

    ```json
    {
        "username": "YOUR_USERNAME",
        "password": "YOUR_PASSWORD",
        "is_sandbox": "false",
        "wsdl_year": "2019",
        "wsdl_quarter": "Q1",
        "start_date": "2015-01-01T00:00:00Z",
        "user_agent": "tap-ilevel <api_user_email@your_company.com>",
        "period_types": "FiscalQuarter"
    }
    ```
    
    Optionally, also create a `state.json` file. `currently_syncing` is an optional attribute used for identifying the last object to be synced in case the job is interrupted mid-stream. The next run would begin where the last job left off.

    ```json
    {
        "currently_syncing": "tasks",
        "bookmarks": {
            "funds": "2020-06-01",
            "investment_transactions": "2020-06-29",
            "securities": "2020-06-29",
            "data_items": "2020-07-10",
            "periodic_data_standardized": "2020-07-09",
            "investments": "2020-07-09",
            "assets": "2020-07-09"
        }
    }
    ```

4. Run the Tap in Discovery Mode
    This creates a catalog.json for selecting objects/fields to integrate:
    ```bash
    tap-ilevel --config config.json --discover > catalog.json
    ```
   See the Singer docs on discovery mode
   [here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode).

5. Run the Tap in Sync Mode (with catalog) and [write out to state file](https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_DEVELOPING.md#running-a-singer-tap-with-a-singer-target)

    For Sync mode:
    ```bash
    > tap-ilevel --config tap_config.json --catalog catalog.json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To load to json files to verify outputs:
    ```bash
    > tap-ilevel --config tap_config.json --catalog catalog.json | target-json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To pseudo-load to [Stitch Import API](https://github.com/singer-io/target-stitch) with dry run:
    ```bash
    > tap-ilevel --config tap_config.json --catalog catalog.json | target-stitch --config target_config.json --dry-run > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```

6. Test the Tap
    
    While developing the ilevel tap, the following utilities were run in accordance with Singer.io best practices:
    Pylint to improve [code quality](https://github.com/singer-io/getting-started/blob/master/docs/BEST_PRACTICES.md#code-quality):
    ```bash
    > pylint tap_ilevel -d missing-docstring -d logging-format-interpolation -d too-many-locals -d too-many-arguments
    ```
    Pylint test resulted in the following score:
    ```bash
    Your code has been rated at 9.77/10.
    ```

    To [check the tap](https://github.com/singer-io/singer-tools#singer-check-tap) and verify working:
    ```bash
    > tap-ilevel --config tap_config.json --catalog catalog.json | singer-check-tap > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    Check tap resulted in the following:
    ```bash
    The output is valid.
    It contained 226149 messages for 9 streams.

        18 schema messages
    224918 record messages
    1213 state messages

    Details by stream:
    +----------------------------+---------+---------+
    | stream                     | records | schemas |
    +----------------------------+---------+---------+
    | periodic_data_standardized | 286285  | 2       |
    | investment_transactions    | 25856   | 2       |
    | assets                     | 221     | 2       |
    | asset_to_asset_relations   | 0       | 2       |
    | scenarios                  | 3       | 2       |
    | fund_to_asset_relations    | 233     | 2       |
    | funds                      | 5       | 2       |
    | securities                 | 454     | 2       |
    | fund_to_fund_relations     | 0       | 2       |
    | data_items                 | 8359    | 2       |
    | investments                | 215643  | 2       |
    +----------------------------+---------+---------+
    ```
---

Copyright &copy; 2020 Stitch
