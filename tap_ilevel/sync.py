from datetime import datetime, timedelta
import singer
from singer import metrics, metadata, Transformer, utils
from singer.utils import strptime_to_utc, strftime
from tap_ilevel.transform import transform_json
#from tap_ilevel.streams import STREAMS
from dateutil.relativedelta import *
#import attr
import json
from tap_ilevel.streams import STREAMS
from pytz import timezone
import pytz
import time

LOGGER = singer.get_logger()

MAX_ID_CHUNK_SIZE = 200000 # Requests to retrieve object details are restricted by a limit

"""
 Publish schema to singer
"""
def __write_schema(catalog, stream_name):
    stream = catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()
    try:
        singer.write_schema(stream_name, schema, stream.key_properties)
    except OSError as err:
        LOGGER.info('OS Error writing schema for: %s', stream_name)
        raise err
"""
 Publish individual record...
"""
def __write_record(stream_name, record, time_extracted):
    try:
        singer.messages.write_record(stream_name, record, time_extracted=time_extracted)
        LOGGER.info('write_record to singer successful')
    except OSError as err:
        LOGGER.info('OS Error writing record for: %s', stream_name)
        LOGGER.info('record: %s', record)
        raise err
    except TypeError as err:
        LOGGER.info('Type Error writing record for: %s', stream_name)
        LOGGER.info('record: %s', record)
        raise err

def __get_bookmark(state, stream, default):
    """Retrieve current bookmark."""
    if (state is None) or ('bookmarks' not in state):
        return default
    return (
        state.get('bookmarks', {}).get(stream, default)
    )

def __write_bookmark(state, stream, value):
    """Set current bookmark."""
    if 'bookmarks' not in state:
        state['bookmarks'] = {}
    state['bookmarks'][stream] = value
    LOGGER.info('Write state for stream: %s, value: %s', stream, value)
    singer.write_state(state)

"""
TODO: Remove
def transform_datetime(this_dttm):
    with Transformer() as transformer:
        new_dttm = transformer._transform_datetime(this_dttm)
    return new_dttm
"""

"""
 Sync a specific endpoint (stream)

    According to the documentation: "The Web Services methods can be broken up into six
    categories" (API Call Descriptions section)
        • Entities (Assets, Funds, Securities, Scenarios)
        • Entity Relationships
        • Data Items
        • Cash Transactions (Transactions, etc..)
        • Currency Rates
        • Documents (Currently not importing)

    The retrieval methods for each data source (stream) will be dependant on the object type.
    Data sources/retrieval strategies are as follows. Generally speaking, data retrieval
    methods are fall into two categories; complete table refreshes, or deltas.
    
    Data Sources:
    --------------------------------------------------
        GetUpdatedObjects/GetDeletedObjects:
            Fund (Delta)
            Asset (Delta)
        GetObjects:
            Security (Full refresh)
        GetScenarios:
            Scenario (Full refresh)
        GetDataItems: (Full refresh)
            DataItem (Full refresh)
        GetObjectRelationships:
            ObjectRelationships (Full refresh)
            "This method is similar to GetInvestments but returns all existing investments"  
"""
def __sync_endpoint(client,
                  catalog,
                  state,
                  endpoint_config,
                  start_date,
                  stream,
                  path,
                  static_params,
                  selected_streams=None,
                  base_url=None,
                  bookmark_field=None,
                  bookmark_query_field=None,
                  bookmark_type=None):

    # Top level variables
    endpoint_total = 0
    total_records = 0
    stream_name = stream.stream
    data_key = endpoint_config.get('data_key')
    start = time.time()

    LOGGER.info('syncing stream : %s', stream_name)
    __update_currently_syncing(state, stream_name)

    #Define window period for data load
    start_dt = get_start_date(stream_name, endpoint_config.get('bookmark_type'), state, start_date)
    end_dt = get_end_date()

    #Get last bookmark date for 'incremental load' objects
    """
    last_bookmark_date = None
    if endpoint_config.get('bookmark_type')=='INCREMENTAL':
        LOGGER.info('Determining last bookmark for last stream import')
    """

    # Publish schema to singer
    __write_schema(catalog, stream_name)
    LOGGER.info('Processing date window for stream %s, %s to %s', stream_name, start_dt, end_dt)

    #Define entity types that use a common API for retrieval
    entity_api_streams = [''] #Add data items

    # Delegate processing to the appropriate routine: Certain entities may be retrieved from a
    # common API call, while others are obtained by alternate sources.
    if stream_name == 'currency_rates':
        LOGGER.warn('CurrencyRate data type is not supported')
        return 0
    if stream_name == 'assets':
        endpoint_total = __get_assets(client, stream, data_key, last_bookmark_date)
    if stream_name == 'funds':
        endpoint_total = __get_funds(client, stream, data_key, last_bookmark_date)
    if stream_name == 'data_items':
        endpoint_total = __get_data_items(client, stream, data_key)
    elif stream_name == 'scenarios':
        endpoint_total = __get_scenarios(client, stream, data_key)
    elif stream_name == 'segments':
        endpoint_total = __get_segments(client, stream, data_key)
    elif stream_name == 'securities':
        endpoint_total = __get_securities(client, stream, data_key)
    elif stream_name == 'object_relations':
        endpoint_total = __get_relations(client, stream, data_key)
    elif stream_name == 'investments':
        endpoint_total = __get_investments(client, stream, data_key)
    elif stream_name == 'investment_transactions':
        endpoint_total = __get_investment_transactions(client, stream, data_key, start_dt, end_dt)
    elif stream_name in entity_api_streams:
        endpoint_total = __process_object_stream_type(endpoint_config, state, stream, start_dt,
                                                    end_dt, client, catalog, bookmark_field)

    __update_currently_syncing(state, None)

    end = time.time()
    elapsed_time = end - start
    LOGGER.info('Processed a total of %s records for stream %s in %s', endpoint_total, stream_name, elapsed_time)

    return endpoint_total

"""
 Certain entities (Funds, Assets) are returned via a common API call, which has a limitation on 
 the duration of the overall specified date range. This method will 'chunk' the given date 
 range (if required) into batches that fall within the limit defined by the API call. Actual 
 processing is delegated to other methods. 
"""
def __process_object_stream_type(endpoint_config, state, stream, start_dt, end_dt, client,
                               catalog, bookmark_field):

    stream_name = stream.stream
    endpoint_total = 0
    # Initialization for date related operations:
    # Operations must be performed in 30 day (max) increments
    date_chunks = __get_date_chunks(start_dt, end_dt, 30)
    LOGGER.info('Total number of date periods to process: ' + str(len(date_chunks)))
    cur_start_date = date_chunks[0]
    date_chunks.pop(0)
    cur_end_date = date_chunks[0]
    date_chunks.pop(0)
    cur_date_range_index = 1
    cur_date_criteria_length = len(date_chunks)

    if cur_start_date == cur_end_date:
        LOGGER.info('Last bookmark matches current date, no processing required')
        return 0

    # Main loop: Process records by date chunks
    for cur_date_criteria_index in range(cur_date_criteria_length):
        cur_date_criteria = date_chunks[cur_date_criteria_index]
        LOGGER.info('processing date range: ' + str(cur_start_date) + "' '" +
                    str(cur_end_date) + "', " + str(
                        cur_date_range_index) + " of " + str(cur_date_criteria_length))

        endpoint_total = endpoint_total + __process_date_range(stream, cur_start_date,
                                                             cur_end_date, client, catalog,
                                                             endpoint_config, bookmark_field)

    return endpoint_total

"""
 Process records for a given date range for streams that use a common API call to produce data 
 (Funds, Assets, etc.)

 The GetObjectsByIds(...) API calls used to retrieve certain data types enforce limitations 
 on the date range supplied as a parameter (30 days). This method expects the specified 
 date range to fall within the date limit.

 Records are retrieved (in chunks if required), converted, and produced.
"""
def __process_date_range(stream, cur_start_date, cur_end_date, client, catalog, endpoint_config,
                       bookmark_field):
    update_count = 0
    stream_name = stream.stream
    # Retrieve ids for updated/inserts objects for given date range
    try:
        # Required to establish access to a SOAP alias for a given object type
        object_type = client.factory.create('tns:UpdatedObjectTypes')
        # Make call to retrieve updated objects for max 30 day date range (object details to be
        # retrieved in additional call)
        updated_asset_ids_all = client.service.GetUpdatedObjects(__get_asset_ref(object_type,
                                                                               stream_name),
                                                                 cur_start_date,
                                                                 cur_end_date)
        LOGGER.info('Successfully retrieved ids for recently created/updated objects')
        updated_result_count = len(updated_asset_ids_all)

        # Determine if there are any records to be processed, and if so there is a 20k limitation
        # for retrieving details by ids.
        if updated_result_count == 0:
            LOGGER.info('No inserts/updates available for stream %s', stream_name)
            return 0

        LOGGER.info('Processing %s updated records', updated_result_count)


        id_sets = __split_id_set(updated_asset_ids_all, MAX_ID_CHUNK_SIZE)
        LOGGER.info('Total number of id set to process is %s', len(updated_asset_ids_all))

        # Outer loop: Iterate through each set of object ids to process (within API limits)
        id_set_count = 1

        for cur_id_set in id_sets:
            LOGGER.info('Processing id set %s of %s ', id_set_count, id_sets)

            # Result of id chunking operation will return array based data structure,
            # first we need to convert
            # to data type expected by API
            array_of_int = client.factory.create('ns3:ArrayOfint')
            array_of_int.int = cur_id_set

            # Perform update operations
            data_key = endpoint_config.get('data_key', 'data')
            update_count = update_count + __process_object_set(stream, array_of_int, False,
                                                             client, catalog,
                                                             endpoint_config,
                                                             bookmark_field, data_key)
            id_set_count = id_set_count + 1
    except Exception as err:
        err_msg = 'error: {}, for type: {}'.format(err, stream_name)
        LOGGER.error(err_msg)
        return 0

    # Retrieve ids for deleted objects for given date range
    #TODO: Implement

    return update_count

"""
 Sync data for certain entity types (Assets,Funds, etc...) that are retrieved from an API 
 call that produces ids. Given a set of ids, the corresponding object details are retrieved, 
 and then processed. Note: API used to retrieve object details imposes a limit on the 
 number of records that may be submitted, so this method expects the IDs to be under 
 that limit. 
"""
def __process_object_set(stream, object_ids, is_deleted_refs, client, catalog, endpoint_config,
                       bookmark_field, data_key):

    object_type = client.factory.create('tns:UpdatedObjectTypes')
    stream_name = stream.stream
    call_response = client.service.GetObjectsByIds(__get_asset_ref(object_type, stream_name),
                                                   object_ids)

    stream = catalog.get_stream(stream_name)

    total_update_count = 0
    object_refs = []


    total_record_count = len(object_refs)
    cur_record_count = 1

    records = call_response.NamedEntity

    for record in records:
        LOGGER.info('Processing record '+ str(cur_record_count) +' of '+
                    str(total_record_count) +' total')

        #TODO: update status
        #write_bookmark

        try:
            transformed_record = __transform_record(record, stream, data_key)

            """
            TODO: delete
            if bookmark_field!=None:
                write_bookmark(state, stream_name, max_bookmark_value)

            transformed_record[bookmark_field]
            """

            # Records that have been deleted need additional flag set
            #if is_deleted_refs == True:
                # TODO: Set deleted flag


            __write_record(stream_name, transformed_record, utils.now())

            total_update_count = total_update_count + 1
            LOGGER.info('Updating record count: %s', total_update_count)

        except Exception as err:
            err_msg = 'error during transformation for entity: {}, for type: {}, obj: {}'\
                .format(err, stream_name, transformed_record)
            LOGGER.error(err_msg)

        cur_record_count = cur_record_count + 1

    LOGGER.info('process_object_set: total record count is %s ', total_update_count)
    return total_update_count


"""
 Make data more 'database compliant', i.e. rename columns, convert to UTC timezones, etc. 
 'transform_data' method needs to ensure raw data matches that in the schemas....
"""
def __transform_record(record, stream, data_key):
    obj_dict = __obj_to_dict(record) #Convert SOAP object to dict
    object_json_str = json.dumps(obj_dict)  # Object dict converted to JSON string
    object_json_str = object_json_str.replace('True','true')
    object_json = json.loads(object_json_str) #Parse JSON

    stream_metadata = metadata.to_map(stream.metadata)
    transformed_data = transform_json(object_json)

    # singer validation check
    with Transformer() as transformer:
        transformed_record = transformer.transform(
            transformed_data,
            stream.schema.to_dict(),
            stream_metadata)



    #todo: re-implement
    return transformed_data

"""
    Convert an object to a dictionary object, dates are converted as required.
"""
def __obj_to_dict(obj):

    date_fields = {"LastModified", "LastModifiedDate", "AcquisitionDate", "ExitDate", "AsOf", "TransactionDate", "AcquisitionAsOf", "InitialPeriod"}

    if not  hasattr(obj, "__dict__"):
        return obj
    result = {}
    for key, val in obj.__dict__.items():
        if key.startswith("_"):
            continue
        element = []
        if isinstance(val, list):
            for item in val:
                element.append(__obj_to_dict(item))
        else:
            element = __obj_to_dict(val)
        result[key] = element

        if key in date_fields:
            old_date = result[key]
            utc_date = __est_to_utc_datetime(old_date)
            result[key] = utc_date
    return result


"""
 When calls are performed to retrieve object details by id, we are restricted by a 20k limit, so 
 we need to support the ability to split a given set into chunks of a given size. Note, we are 
 accepting a SOAP data type (ArrayOfInts) and returning an array of arrays which will need to 
 be converted prior to submission to any additional SOAP calls.
"""
def __split_id_set(array_of_ids, max_len):

    result = []
    ids = array_of_ids[0]

    if len(ids) < max_len:
        result.append(ids)
        return result

    chunk_count = len(ids) / max_len
    remaining_records = len(ids) % max_len
    #if (len(ids) % max_len) > 0:
    #    chunk_count = chunk_count + 1
    #count = numpy.array_split(ids, chunk_count)

    cur_chunk_index = 0
    total_index = 0
    while cur_chunk_index < chunk_count:
        f = max_len * cur_chunk_index
        cur_id_set = []
        while f < max_len:
            cur_id_set[f] = array_of_ids[total_index]
            f = f+1
            total_index = total_index + 1
        result[cur_chunk_index] = cur_id_set
        cur_index = cur_index +1

    if remaining_records > 0:
        cur_id_set = []
        cur_chunk_index = cur_chunk_index + 1
        cur_index = 0
        while cur_index < remaining_records:
            total_index = total_index + 1
            cur_id_set[cur_index] = array_of_ids[total_index]
            cur_index = cur_index + 1
        result[cur_chunk_index] = cur_id_set

    return result

"""
 Certain API calls have a limitation of 30 day periods, where the process might be launched 
 with an overall activity window of a greater period of time. Date ranges sorted into 30 
 day chunks in preparation for processing.
 
 Values provided for input dates are in format rerquired by SOAP API (yyyy-mm-dd)

 API calls are performed within a maximum 30 day timeframe, so breaking a period of time 
 between two into limited 'chunks' is required
"""
def __get_date_chunks(start_date, end_date, max_days):

    td = timedelta(days=max_days)
    result = []

    days_dif = __get_num_days_diff(start_date, end_date)
    if days_dif < max_days:
        return result

    working = True
    cur_date = start_date
    result.append(cur_date)
    next_date = cur_date
    while working:

        next_date = (next_date + timedelta(days=max_days))
        if next_date == end_date or next_date > end_date:
            result.append(end_date)
            return result
        else:
            result.append(next_date)


    #TODO: Remove hard coded dates
    """
    result = []
    now = datetime.now()
    result.append(datetime.strptime('2020-04-01', '%Y-%m-%d'))
    result.append(datetime.strptime('2020-05-01', '%Y-%m-%d'))
    result.append(datetime.strptime('2020-06-01', '%Y-%m-%d'))
    """
    return result

"""
 Given stream name, identify the corresponding Soap identifier to send to the API. This is used 
 to identify the type of entity we are retrieving for certain API calls.
"""
def __get_asset_ref(attr, stream_ref):

    if stream_ref == 'assets':
        return attr.Asset
    elif stream_ref == 'currency_rates':
        return attr.CurrencyRate
    elif stream_ref == 'data_items':
        return attr.DataItem
    elif stream_ref == 'funds':
        return attr.Fund
    elif stream_ref == 'investment_transactions':
        return attr.InvestmentTransaction
    elif stream_ref == 'investments':
        return attr.Investment
    elif stream_ref == 'scenarios':
        return attr.Scenario
    elif stream_ref == 'securities':
        return attr.Security
    elif stream_ref == 'segments':
        return attr.SegmentNode
    return None

"""
 Currently syncing sets the stream currently being delivered in the state.
 If the integration is interrupted, this state property is used to identify
  the starting point to continue from.
 Reference: https://github.com/singer-io/singer-python/blob/master/singer/bookmarks.py#L41-L46
"""
def __update_currently_syncing(state, stream_name):
    LOGGER.info('Updating status of current stream processing')
    LOGGER.info(state)
    LOGGER.info(stream_name)
    if (stream_name is None) and ('currently_syncing' in state):
        del state['currently_syncing']
    else:
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)

"""
    Main routine: orchestrates pulling data for selected streams. 
"""
def sync(client, config, catalog, state, base_url):
    LOGGER.info('sync.py: sync()')
    LOGGER.info('state:')

    # Start date may be overridden by command line params
    if 'start_date' in config:
        start_date = config['start_date']

    # Get selected_streams from catalog, based on state last_stream
    #   last_stream = Previous currently synced stream, if the load was interrupted
    last_stream = singer.get_currently_syncing(state) #TODO: Review
    LOGGER.info('last/currently syncing stream: {}'.format(last_stream))
    selected_streams = []
    selected_streams_by_name = {}
    for stream in catalog.get_selected_streams(state):
        selected_streams.append(stream.stream)
        selected_streams_by_name[stream.stream] = stream
    LOGGER.info('selected_streams: {}'.format(selected_streams))

    if not selected_streams or selected_streams == []:
        return

    # Loop through endpoints in selected_streams
    for stream_name, endpoint_config in STREAMS.items():
        if stream_name in selected_streams:
            LOGGER.info('START Syncing: {}'.format(stream_name))
            stream = selected_streams_by_name[stream_name]

            path = endpoint_config.get('path', stream_name)
            bookmark_field = next(iter(endpoint_config.get('replication_keys', [])), None)
            bookmark_query_field = bookmark_query_field = endpoint_config.\
                get('bookmark_query_field')
            bookmark_type = endpoint_config.get('bookmark_type')
            __write_schema(catalog, stream_name)
            total_records = 1

            # Main sync routine
            total_records = __sync_endpoint(
                client=client,
                catalog=catalog,
                state=state,
                endpoint_config=endpoint_config,
                start_date=start_date,
                stream=stream,
                path=path,
                static_params=endpoint_config.get('params', {}),
                selected_streams=selected_streams,
                base_url=base_url,
                bookmark_field=bookmark_field,
                bookmark_query_field=None,
                bookmark_type=None
            )

            __update_currently_syncing(state, None)
            LOGGER.info('FINISHED Syncing: {}, total_records: {}'.format(
                stream_name,
                total_records))

    LOGGER.info('sync.py: sync complete')


"""
 Provides ability to determine number of days between two given dates.
"""
def __get_num_days_diff(start_date, end_date):
    return abs((start_date - end_date).days)

"""
 Retrieve full set of relation entities
"""
def __get_relations(client, stream, data_key):
    updated_record_count = 0
    stream_name = stream.stream
    schema = stream.schema.to_dict()

    try:
        result = client.service.GetObjectRelationships()
        records = result[0]
        total_record_count = len(records)
        LOGGER.info('Preparing to process a total of '+ str(total_record_count)
                    +' object relations')

        for record in records:
            try:
                transformed_record = __transform_record(record, stream, data_key)
                __write_record(stream_name, transformed_record, utils.now())
                updated_record_count = updated_record_count + 1

            except Exception as recordErr:
                err_msg = 'error during transformation for entity: {}, for type: {}, obj: {}'.\
                    format(recordErr, stream_name, transformed_record)
                LOGGER.error(err_msg)

    except Exception as err:
        err_msg = 'API call failed: {}, for type: {}'.format(err, stream_name)
        LOGGER.error(err_msg)
    return updated_record_count

"""
 Retrieve full set of 'data items' via API call.
"""
def __get_data_items(client, stream, data_key):
    updated_record_count = 0
    stream_name = stream.stream
    schema = stream.schema.to_dict()

    try:
        result = client.service.GetDataItems()
        records = result[0]
        total_record_count = len(records)
        LOGGER.info('Preparing to process a total of ' + str(total_record_count) + ' data items')

        for record in records:
            try:
                transformed_record = __transform_record(record, stream, data_key)
                __write_record(stream_name, transformed_record, utils.now())
                updated_record_count = updated_record_count + 1
            except Exception as recordErr:
                err_msg = 'error during transformation for entity: {}, for type: {}, obj: {}'\
        .format(recordErr, stream_name, transformed_record)
                LOGGER.error(err_msg)

    except Exception as err:
        err_msg = 'API call failed: {}, for type: {}'.format(err, stream_name)
        LOGGER.error(err_msg)

    return updated_record_count

def __get_investment_transactions(client, stream, data_key, start_date, end_date):
    """Retrieve investment transactions from a given point in time. Call operations for each date
    between the specified 'start' and 'end' dates will be individually be requested as this particular
    endpoint requires an 'exact match' on the criteria."""
    LOGGER.info('Processing date range %s to %s', start_date, end_date)

    date_criteria = __split_date_range_into_array(start_date, end_date)
    LOGGER.info('A total of %s requests for invest transactions will be required', len(date_criteria))
    result_count = 0
    for as_of_date in date_criteria:
        result_count = result_count + __get_investment_transactions_for_as_of_date(client, stream, data_key, as_of_date)
        #TODO: Write bookmark
    return result_count

def __split_date_range_into_array(start_date, end_date):
    """Support the ability to create an array of individual days between two giiven dates"""
    result = []

    delta = end_date - start_date  # as timedelta

    for i in range(delta.days + 1):
        day = start_date + timedelta(days=i)
        result.append(day)

    return result

def __get_investment_transactions_for_as_of_date(client, stream, data_key, as_of_date):
    """Retrieve investment transactions for a specific 'AsOfDate' criteria """

    LOGGER.info('Retrieving investment transaction for as of date %s', as_of_date)

    updated_record_count = 0
    stream_name = stream.stream
    schema = stream.schema.to_dict()
    try:
        criteria = client.factory.create('InvestmentTransactionsSearchCriteria')
        target_date_str = datetime.strftime(as_of_date, '%Y-%m-%d')
        criteria.AsOfDate = target_date_str

        result = client.service.GetInvestmentTransactions(criteria)
        if isinstance(result, str):
            return 0

        records = result[0]
        total_record_count = len(records)
        LOGGER.info('Preparing to process a total of %s investment transactions', total_record_count)

        for record in records:
            try:
                transformed_record = __transform_record(record, stream, data_key)
                __write_record(stream_name, transformed_record, utils.now())
                updated_record_count = updated_record_count + 1
            except Exception as recordErr:
                err_msg = 'error during transformation for entity: {}, for type: {}, obj: {}'\
                    .format(recordErr, stream_name, transformed_record)
                LOGGER.error(err_msg)

    except Exception as err:
        err_msg = 'API call failed: {}, for type: {}'.format(err, stream_name)
        LOGGER.error(err_msg)

    return updated_record_count

"""
 Retrieve investments via API call, publish results.
"""
def __get_investments(client, stream, data_key):
    updated_record_count = 0
    stream_name = stream.stream
    schema = stream.schema.to_dict()
    try:
        result = client.service.GetInvestments()
        records = result[0]
        total_record_count = len(records)
        LOGGER.info('Preparing to process a total of ' + str(total_record_count) + ' investments')
        transformed_record = None
        for record in records:
            try:
                transformed_record = __transform_record(record, stream, data_key)
                __write_record(stream_name, transformed_record, utils.now())
                updated_record_count = updated_record_count + 1
            except Exception as recordErr:
                err_msg = 'error during transformation for entity: {}, for type: {}, obj: {}'\
                    .format(recordErr, stream_name, transformed_record)
                LOGGER.error(err_msg)

    except Exception as err:
        err_msg = 'API call failed: {}, for type: {}'.format(err, stream_name)
        LOGGER.error(err_msg)

    return updated_record_count

"""
    Retrieve scenarios via API call, publish results.
"""
def __get_scenarios(client, stream, data_key):
    updated_record_count = 0
    stream_name = stream.stream
    schema = stream.schema.to_dict()
    try:
        LOGGER.info('Loading scenarios')
        result = client.service.GetScenarios()
        records = result[0]
        total_record_count = len(records)
        LOGGER.info('Preparing to process a total of ' + str(total_record_count) + ' scenarios')

        for record in records:
            try:
                transformed_record = __transform_record(record, stream, data_key)
                __write_record(stream_name, transformed_record, utils.now())
                updated_record_count = updated_record_count + 1
            except Exception as recordErr:
                err_msg = 'error during transformation for entity: {}, for type: {}, obj: {}'\
                    .format(recordErr, stream_name, transformed_record)
                LOGGER.error(err_msg)

    except Exception as err:
        err_msg = 'API call failed: {}, for type: {}'.format(err, stream_name)
        LOGGER.error(err_msg)

    return updated_record_count

"""
    Retrieve securities via API call, publish results.
"""
def __get_securities(client, stream, data_key):
    updated_record_count = 0
    stream_name = stream.stream
    schema = stream.schema.to_dict()
    try:
        LOGGER.info('Loading scenarios')
        result = client.service.GetSecurities()
        records = result[0]
        total_record_count = len(records)
        LOGGER.info('Preparing to process a total of ' + str(total_record_count) + ' securities')

        for record in records:
            try:
                transformed_record = __transform_record(record, stream, data_key)
                __write_record(stream_name, transformed_record, utils.now())
                updated_record_count = updated_record_count + 1
            except Exception as recordErr:
                err_msg = 'error during transformation for entity: {}, for type: {}, obj: {}'\
                    .format(recordErr, stream_name, transformed_record)
                LOGGER.error(err_msg)

    except Exception as err:
        err_msg = 'API call failed: {}, for type: {}'.format(err, stream_name)
        LOGGER.error(err_msg)

    return updated_record_count

def __get_assets(client, stream, data_key, last_bookmark_date):
    updated_record_count = 0
    stream_name = stream.stream
    schema = stream.schema.to_dict()
    try:
        LOGGER.info('Loading assets')
        result = client.service.GetAssets()
        records = result[0]
        total_record_count = len(records)
        LOGGER.info('Preparing to process a total of %s assets', total_record_count)

        for record in records:
            try:
                transformed_record = __transform_record(record, stream, data_key)
                __write_record(stream_name, transformed_record, utils.now())
                updated_record_count = updated_record_count + 1
            except Exception as recordErr:
                err_msg = 'error during transformation for entity: {}, for type: {}, obj: {}'\
                    .format(recordErr, stream_name, transformed_record)
                LOGGER.error(recordErr)
                LOGGER.error(err_msg)

    except Exception as err:
        err_msg = 'API call failed: {}, for type: {}'.format(err, stream_name)
        LOGGER.error(err_msg)

    return updated_record_count

def __get_funds(client, stream, data_key, last_bookmark_date):
    updated_record_count = 0
    stream_name = stream.stream
    schema = stream.schema.to_dict()
    try:
        LOGGER.info('Loading funds')
        result = client.service.GetFunds()
        records = result[0]
        total_record_count = len(records)
        LOGGER.info('Preparing to process a total of %s funds', total_record_count)

        for record in records:
            try:
                transformed_record = __transform_record(record, stream, data_key)
                __write_record(stream_name, transformed_record, utils.now())
                updated_record_count = updated_record_count + 1
            except Exception as recordErr:
                err_msg = 'error during transformation for entity: {}, for type: {}, obj: {}'\
                    .format(recordErr, stream_name, transformed_record)
                LOGGER.error(recordErr)
                LOGGER.error(err_msg)

    except Exception as err:
        err_msg = 'API call failed: {}, for type: {}'.format(err, stream_name)
        LOGGER.error(err_msg)

    return updated_record_count

"""
 Date object returned from API call needs to be converted to format required by SingerIO 
"""
def __est_to_utc_datetime(date_val):
    date_str = date_val.strftime("%Y-%m-%d %H:%M:%S")
    timezone = pytz.timezone('US/Eastern')
    est_datetime = timezone.localize(datetime.strptime(
        date_str, "%Y-%m-%d %H:%M:%S"))
    utc_datetime = strftime(timezone.normalize(est_datetime).astimezone(
        pytz.utc))
    return utc_datetime

"""
    Retrieve securities via API call, publish results.
    TODO: No data, remove
"""
def __get_segments(client, stream, data_key):
    updated_record_count = 0
    """
    stream_name = stream.stream
    schema = stream.schema.to_dict()
    try:
        LOGGER.info('Loading scenarios')
        result = client.service.GetSecurities()
        records = result[0]
        total_record_count = len(records)
        LOGGER.info('Preparing to process a total of ' + str(total_record_count) + ' securities')

        for record in records:
            try:
                transformed_record = transform_record(record, stream, data_key)
                write_record(stream_name, transformed_record, utils.now())
                updated_record_count = updated_record_count + 1
            except Exception as record_err:
                err_msg = 'error during transformation for entity: {}, for type: {}, obj: {}'\
                    .format(record_err, stream_name, transformed_record)
                LOGGER.error(err_msg)

    except Exception as err:
        err_msg = 'API call failed: {}, for type: {}'.format(err, stream_name)
        LOGGER.error(err_msg)
    """
    #TODO: Implement
    return updated_record_count

def get_start_date(stream_name, bookmark_type, state, start_date):
    """Get start date for a given stream. For streams that are configured to use 'datetime' as a bookmarking
    strategy, the last known bookmark is used (if present). Otherwise, the default start date value is used
    if no bookmark may be located, or in cases where a full table refresh is appropriate."""

    if bookmark_type != 'datetime':
        return datetime.strptime(start_date, '%Y-%m-%dT%H:%M:%SZ')

    #return __get_bookmark(state, stream_name, start_date) #TODO: Remove

    return datetime.now()-timedelta(days=365)

def get_end_date():
    """Obtain reference to end date used for tap processing window."""
    return datetime.now()