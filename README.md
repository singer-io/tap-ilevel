# tap-ilevel

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from the [IPREO iLevel SOAP API]()
- Extracts the following resources:
  - [Branches](https://api.ilevel.com/?http#branches-getAll)

- Outputs the schema for each resource
- Incrementally pulls data based on the input state

## Streams
[**branches (GET v2)**](https://api.ilevel.com/?http#branches-getAll)
- Endpoint: https://instance.sandbox.ilevel.com/api/branches
- Primary keys: id
- Foreign keys: custom_field_set_id, custom_field_id (custom_field_sets)
- Replication strategy: Incremental (query all, filter results)
  - Sort by: lastModifiedDate:ASC
  - Bookmark: last_modified_date (date-time)
- Transformations: Fields camelCase to snake_case, Abstract/generalize custom_field_sets


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
    > pip install target-json
    
    ```
    - [singer-tools](https://github.com/singer-io/singer-tools)
    - [target-stitch](https://github.com/singer-io/target-stitch)

3. Create your tap's `config.json` file. The `subdomain` is everything before `.ilevel.com` in the ilevel instance URL.  For the URL: `https://stitch.sandbox.ilevel.com`, the subdomain would be `stitch.sandbox`.

    ```json
    {
        "username": "YOUR_USERNAME",
        "password": "YOUR_PASSWORD",
        "subdomain": "YOUR_SUBDOMAIN",
        "start_date": "2019-01-01T00:00:00Z",
        "user_agent": "tap-ilevel <api_user_email@your_company.com>"
    }
    ```
    
    Optionally, also create a `state.json` file. `currently_syncing` is an optional attribute used for identifying the last object to be synced in case the job is interrupted mid-stream. The next run would begin where the last job left off.

    ```json
    {
        "currently_syncing": "tasks",
        "bookmarks": {
            "branches": "2019-06-11T13:37:51Z",
            "communications": "2019-06-19T19:48:42Z",
            "centres": "2019-06-18T18:23:53Z",
            "clients": "2019-06-20T00:52:44Z",
            "credit_arrangements": "2019-06-19T19:48:45Z",
            "custom_field_sets": "2019-06-11T13:37:56Z",
            "deposit_accounts": "2019-06-19T19:48:47Z",
            "cards": "2019-06-18T18:23:58Z",
            "deposit_products": "2019-06-20T00:52:49Z",
            "deposit_transactions": "2019-06-20T00:52:40Z",
            "groups": "2019-06-19T19:48:41Z",
            "loan_accounts": "2019-06-11T13:37:52Z",
            "loan_products": "2019-06-20T00:52:43Z",
            "loan_transactions": "2019-06-19T19:48:44Z",
            "tasks": "2019-06-18T18:23:55Z",
            "users": "2019-06-20T00:52:46Z"
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
    Your code has been rated at 9.87/10.
    ```

    To [check the tap](https://github.com/singer-io/singer-tools#singer-check-tap) and verify working:
    ```bash
    > tap-ilevel --config tap_config.json --catalog catalog.json | singer-check-tap > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    Check tap resulted in the following:
    ```bash
    The output is valid.
    It contained 228 messages for 16 streams.

        17 schema messages
        167 record messages
        44 state messages

    Details by stream:
    +----------------------+---------+---------+
    | stream               | records | schemas |
    +----------------------+---------+---------+
    | deposit_transactions | 9       | 1       |
    | cards                | 1       | 2       |
    | clients              | 102     | 1       |
    | loan_products        | 2       | 1       |
    | branches             | 2       | 1       |
    | deposit_products     | 1       | 1       |
    | centres              | 2       | 1       |
    | users                | 3       | 1       |
    | credit_arrangements  | 2       | 1       |
    | communications       | 1       | 1       |
    | deposit_accounts     | 2       | 1       |
    | custom_field_sets    | 19      | 1       |
    | loan_transactions    | 6       | 1       |
    | groups               | 2       | 1       |
    | tasks                | 7       | 1       |
    | loan_accounts        | 6       | 1       |
    +----------------------+---------+---------+
    ```
---

Copyright &copy; 2019 Stitch
