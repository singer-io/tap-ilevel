# Requests to retrieve object details are restricted by a limit, this is standard across calls.
MAX_ID_CHUNK_SIZE = 5000

#Establish stream names that will follow specific publishing paths.
ALL_RECORDS_STREAMS = ["assets", "funds", "investments", "securities", "scenarios", "data_items",\
    "asset_to_asset_relations", "fund_to_asset_relations", "fund_to_fund_relations"]
INCREMENTAL_STREAMS = [] # data_items moved to ALL_ b/c missing fields w/ incremental calls
OTHER_STREAMS = ['investment_transactions']
STANDARDIZED_PERIODIC_DATA_STREAMS = ["periodic_data_standardized"]

#API calls frequently limit request operations to max window periods, define max period here: Note
#API consistently uses same limitation across calls, so single limit is appropriate
MAX_DATE_WINDOW = 14
