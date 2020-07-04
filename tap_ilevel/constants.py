# Requests to retrieve object details are restricted by a limit, this is standard across calls.
MAX_ID_CHUNK_SIZE = 20000

#Establish stream names that will follow specific publishing paths.
OBJECT_TYPE_STREAMS = ["assets", "funds", "securities", "data_items", "scenarios", "securities"]
INVESTMENT_TRANSACTIONS_STREAM = 'investment_transactions'
RELATION_TYPE_STREAM = ["asset_to_asset_relations", "fund_to_asset_relations",
                        "fund_to_fund_relations"]
STANDARDIZED_PERIODIC_DATA_STREAMS = ["periodic_data_standardized"]

#API calls frequently limit request operations to max window periods, define max period here: Note
#API consistently uses same limitation across calls, so single limit is appropriate
MAX_DATE_WINDOW = 25

#Define fields that need to have special date formatting applied during transformation process in
#order to meet requirements of API calls.
ENTITY_DATE_FIELDS = {"LastModified", "LastModifiedDate", "AcquisitionDate", "ExitDate", "AsOf",
                      "TransactionDate", "AcquisitionAsOf", "InitialPeriod", "PeriodEnd",
                      "ReportedDate", "EndOfPeriodValue", "ReportedDateValue"}
