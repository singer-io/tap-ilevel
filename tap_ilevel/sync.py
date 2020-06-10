from datetime import datetime, timedelta
import singer
from singer import metrics, metadata, Transformer, utils
from singer.utils import strptime_to_utc, strftime
from tap_ilevel.transform import transform_json
#from tap_ilevel.streams import STREAMS
#from dateutil.relativedelta import *
#import attr
import json
from tap_ilevel.streams import STREAMS
from json import JSONEncoder

#from singer import metrics
import dateutil.parser

from .transform import obj_to_dict, convert_ipush_event_to_obj
from .request_state import RequestState
from .iget_formula import IGetFormula
from .utils import get_date_chunks, strip_record_ids
from .constants import MAX_ID_CHUNK_SIZE, MAX_DATE_WINDOW, OBJECT_TYPE_STREAMS,\
    RELATION_TYPE_STREAM, PERIODIC_DATA_STREAMS, MAX_DATE_WINDOW, STANDARDIZED_PERIODIC_DATA_STREAMS
"""
from .singer_operations import write_record, write_schema, get_bookmark, write_bookmark,\
    update_currently_syncing, get_updated_object_id_sets, write_record, get_deleted_object_id_sets,\
    get_object_details_by_ids, get_object_relation_details_by_ids,\
    get_investment_transaction_details_by_ids, createEntityPath, perform_iget_operation, \
    get_start_date, get_standardized_ids, perform_igetbatch_operation_for_standardized_id_set, \
    get_standardized_data_id_chunks
"""
from .singer_operations import *

"""
 sync.py
 Main routine for syncing data from iLevel data source.
 Data retrieval methodology:
      Data migrated from the API falls into the following categories, for which each
      has a corresponding stream implementation.

      Object types:
            (Top level entities, note: only certain updates will trigger updates: see periodic data)
            Assets 
            Funds
            Securities
            DataItems
            Investments

      Object associations:
            (Joins, relations between objects.)
            AssetToAsset
            FundToAsset
            FundToFundRelations

      Periodic data:
            (Reflects state of entity attributes for a given period in time and scenario)
"""
LOGGER = singer.get_logger()


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
    last_stream = singer.get_currently_syncing(state)
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
            write_schema(catalog, stream_name)
            total_records = 1
            data_key = endpoint_config.get('data_key')
            start_dt = get_start_date(stream_name, bookmark_type, state,
                                      start_date)

            req_state = __get_request_state(client, stream_name, data_key, start_dt,
                                            datetime.now(), state, bookmark_field, stream, path,
                                            catalog)

            # Main sync routine
            total_records = __sync_endpoint(req_state)

            update_currently_syncing(state, None)
            LOGGER.info('FINISHED Syncing: {}, total_records: {}'.format(
                stream_name,
                total_records))

    LOGGER.info('sync.py: sync complete')

def __sync_endpoint(req_state):
    """ Sync a specific endpoint (stream). """

    # Top level variables
    endpoint_total = 0
    total_records = 0

    with metrics.job_timer('http_request_duration'):

        LOGGER.info('syncing stream : %s', req_state.stream_name)
        update_currently_syncing(req_state.state, req_state.stream_name)

        # Publish schema to singer
        write_schema(req_state.catalog, req_state.stream_name)
        LOGGER.info('Processing date window for stream %s, %s to %s', req_state.stream_name,
                    req_state.start_dt, req_state.end_dt)

        if(req_state.stream_name in OBJECT_TYPE_STREAMS or req_state.stream_name in RELATION_TYPE_STREAM):
            #Certain stream types follow a common processing pattern
            endpoint_total = __process_object_stream(req_state)
        elif req_state.stream_name in PERIODIC_DATA_STREAMS:
            endpoint_total = __process_periodic_stream(req_state)
        elif(req_state.stream_name in STANDARDIZED_PERIODIC_DATA_STREAMS):
            endpoint_total = __process_standardized_data_stream(req_state)
        #elif req_state.stream_name=='fund_to_fund_relations':

        #elif req_state.stream_name=='asset_to_asset_relations':

        update_currently_syncing(req_state.state, None)
        LOGGER.info('FINISHED Syncing: {}, total_records: {}'.format(
            req_state.stream_name,
            total_records))


    LOGGER.info('sync.py: sync complete')

    return endpoint_total

def __get_request_state(client, stream_name, data_key, start_dt, end_dt, state, bookmark_field,
                        stream, path, catalog):
    """Given a series of common parameters, combine them into a data structure to minimize
    complexity of passing frequently used data as method parameters."""
    req_state = RequestState()
    req_state.client = client
    req_state.stream_name = stream_name
    req_state.data_key = data_key
    req_state.start_dt = start_dt

    end_dt = end_dt.strftime("%Y %m, %d")
    end_dt = datetime.strptime(end_dt, "%Y %m, %d")

    req_state.end_dt = end_dt
    req_state.state = state
    req_state.bookmark_field = bookmark_field
    req_state.stream = stream
    req_state.path = path
    req_state.catalog = catalog
    return req_state

def __chunk_results_of_get_updated_data_call():
    """When call is performed to translate recently modified records into standardized ids, further
    processing is required to isolate resulting standardized data ids as 20k limited sets in order
    for next call."""

def __process_object_stream(req_state):
    """Top level handler for processing default method of updating data."""
    record_count = 0
    date_chunks = get_date_chunks(req_state.start_dt, req_state.end_dt, MAX_DATE_WINDOW)

    cur_start_date = None
    cur_end_date = None
    cur_date_criteria_length = len(date_chunks)
    cur_date_range_index = 0

    # Loop through date, and id 'chunks' as appropriate, processing each window.
    while cur_date_range_index < cur_date_criteria_length:
        if cur_start_date == None:
            cur_start_date = date_chunks[0]
            cur_end_date = date_chunks[1]
            cur_date_range_index = 2
        else:
            cur_start_date = cur_end_date
            cur_end_date = date_chunks[cur_date_range_index]
            cur_date_range_index = cur_date_range_index + 1

        LOGGER.info('Processing date range %s of % total (%s - %s)', cur_date_range_index,
                     cur_date_criteria_length, cur_start_date, cur_end_date)

        process_count = 0

        #Retrieve updated entities for given date range, and send for processing
        updated_object_id_sets = get_updated_object_id_sets(cur_start_date, cur_end_date,
                                                            req_state.client, req_state.stream_name)

        if len(updated_object_id_sets)>0:
            cur_id_set_index = 0
            for id_set in updated_object_id_sets:
                LOGGER.info('Processing id set %s of %s total sets', cur_id_set_index,
                            len(updated_object_id_sets))
                record_count = record_count + __process_updated_object_stream_id_set(id_set, req_state)


        #Retrieve deleted entities for given date range, and send for processing
        deleted_object_id_sets = get_deleted_object_id_sets(cur_start_date, cur_end_date,
                                                            req_state.client, req_state.stream_name)
        if len(deleted_object_id_sets)>0:
            cur_id_set_index = 0
            for id_set in updated_object_id_sets:
                LOGGER.info('Processing id set %s of %s total sets', cur_id_set_index,
                            len(updated_object_id_sets))
                record_count = record_count + __process_deleted_object_stream_id_set(id_set, req_state)

                cur_id_set_index = cur_id_set_index + 1

        write_bookmark(req_state.state, req_state.stream_name, cur_end_date)

    return record_count

def __process_updated_object_stream_id_set(object_ids, req_state):
    update_count = 0

    if req_state.stream_name in OBJECT_TYPE_STREAMS:
        records = get_object_details_by_ids(object_ids, req_state.stream_name,
                                             req_state.client)
    elif req_state.stream_name in RELATION_TYPE_STREAM:
        records = get_object_relation_details_by_ids(object_ids, req_state.stream_name,
                                             req_state.client)
    else:
        records = get_investment_transaction_details_by_ids(object_ids, req_state.stream_name,
                                              req_state.client)
    if len(records)==0:
            return 0

    update_count = update_count + process_records(records, req_state)
    return update_count

def __process_deleted_object_stream_id_set(object_ids, req_state):
    update_count = 0

    if req_state.stream_name in OBJECT_TYPE_STREAMS:
        records = get_object_details_by_ids(object_ids, req_state.stream_name,
                                             req_state.client)
    elif req_state.stream_name in RELATION_TYPE_STREAM:
        records = get_object_relation_details_by_ids(object_ids, req_state.stream_name,
                                             req_state.client)
    else:
        records = get_investment_transaction_details_by_ids(object_ids, req_state.stream_name,
                                              req_state.client)
    if len(records)==0:
            return 0

    update_count = update_count + process_records(records, req_state, True)
    return update_count

def __set_deletion_flag(entity, is_soft_deleted = False):
    """Set new attribute into entity used to track soft deletion status."""
    entity[''] = is_soft_deleted

def process_records(records, req_state, deletion_flag=None):
    """Handle low level operations to publish records."""
    update_count = 0
    for record in records:
        try:
            transformed_record = __transform_record(record, req_state.stream, req_state.data_key)

            if(deletion_flag!=None):
                __set_deletion_flag(transformed_record, deletion_flag)

            write_record(req_state.stream_name, transformed_record, utils.now())


            LOGGER.info('Updating record count: %s', update_count)
            update_count = update_count + 1
        except Exception as err:
            err_msg = 'Error during transformation for entity: {}, for type: {}, obj: {}'\
                .format(err, req_state.stream_name, transformed_record)
            LOGGER.error(err_msg)

    LOGGER.info('process_object_set: total record count is %s ', update_count)
    return update_count


def __transform_record(record, stream, data_key):
    """ Make data more 'database compliant', i.e. rename columns, convert to UTC timezones, etc.
     'transform_data' method needs to ensure raw data matches that in the schemas. """
    obj_dict = obj_to_dict(record) #Convert SOAP object to dict
    #object_json_str = json.dumps(obj_dict, indent=4, sort_keys=True, default=str)
    object_json_str = json.dumps(obj_dict, indent=4, cls=DateTimeEncoder)
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

    return transformed_data

class DateTimeEncoder(JSONEncoder):
    # Override the default method
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()

def __process_periodic_stream(req_state):
    """Requests that deal with periodic data type data, a different pattern is followed to
    retrieve the appropriate data and publish results."""

    update_count = 0

    date_chunks = get_date_chunks(req_state.start_dt, req_state.end_dt, MAX_DATE_WINDOW)

    cur_start_date = None
    cur_end_date = None
    cur_date_criteria_length = len(date_chunks)
    cur_date_range_index = 0

    while cur_date_range_index < cur_date_criteria_length:

        if cur_start_date == None:
            cur_start_date = date_chunks[0]
            cur_end_date = date_chunks[1]
            cur_date_range_index = 2
        else:
            cur_start_date = cur_end_date
            cur_end_date = date_chunks[cur_date_range_index]
            cur_date_range_index = cur_date_range_index + 1

        LOGGER.info('Processing date range %s of %s total date segments (%s - %s)', cur_date_range_index,
                     cur_date_criteria_length, cur_start_date, cur_end_date)

        #Retrieve appropriate data from date window.
        updated_object_id_sets = get_updated_object_id_sets(cur_start_date, cur_end_date,
                                                            req_state.client, req_state.stream_name)

        if len(updated_object_id_sets)==0:
            continue

        #Execute request. Note this variant of the iGet(...) call provides IDs of objects that were
        #updated, and not the translated 'stanrdized data ids' which are provided in the second
        #variant.
        i_get_results = perform_iget_operation(updated_object_id_sets, req_state.stream_name,
                                               req_state.client)

        #Publish resulting data
        update_count = update_count + process_records(i_get_results, req_state)

        write_bookmark(req_state.state, req_state.stream_name, cur_end_date)

    return update_count

def __process_standardized_data_stream(req_state):
    """Retrieve periodic data. API docs under 'Migrating iLEVEL Data Changes (Deltas) to a Data
    Warehouse' (pp 67). Whereas other streams attempt to reflect updates to entities (Assets,
    Funds, InvestmentTransactions) operations for certain attributes will not be reflected in the
    API calls used to report updates. This call will reflect all attribute updates for the specified
    timeframe. Additionally, this call will reflect the state of an attribute at a given point in
    time (period)."""
    update_count = 0

    #Split date windows: API call restricts date windows based on 30 day periods.
    date_chunks = get_date_chunks(req_state.start_dt, req_state.end_dt, MAX_DATE_WINDOW)

    cur_start_date = None
    cur_end_date = None
    cur_date_criteria_length = len(date_chunks)
    cur_date_range_index = 0
    LOGGER.info('Preparing to process %s date chunks', len(date_chunks))
    date_chunk_index = 0
    while cur_date_range_index < cur_date_criteria_length:
        LOGGER.info('Processing date set %s of %s total', date_chunk_index, len(date_chunks))
        if cur_start_date==None:
            cur_start_date = date_chunks[0]
            cur_end_date = date_chunks[1]
            cur_date_range_index = 2
        else:
            cur_start_date = cur_end_date
            cur_end_date = date_chunks[cur_date_range_index]
            cur_date_range_index = cur_date_range_index + 1

        LOGGER.info('Processing date range '+ str(cur_date_range_index) +' of '+
                    str(cur_date_criteria_length) +' total ('+ str(cur_start_date) +' - '+
                    str(cur_end_date) +')')

        #Get updated records based on date range
        updated_object_id_sets = get_standardized_data_id_chunks(cur_start_date, cur_end_date, req_state.client)
        if len(updated_object_id_sets) == 0:
            continue

        LOGGER.info('Total number of updated sets is %s', len(updated_object_id_sets))

        #Translate to standardized ids
        for id_set in updated_object_id_sets:
            LOGGER.info('Total number of records in current set %s', len(id_set))
            update_count = update_count + process_iget_batch_for_standardized_id_set(id_set, req_state)

        write_bookmark(req_state.state, req_state.stream_name, cur_end_date)
        date_chunk_index = date_chunk_index + 1

    return update_count

def process_iget_batch_for_standardized_id_set(std_id_set, req_state):
    """Given a set of 'stanardized ids', reflecting attributes that have been updated for a given
    time window, perform any additional API requests (iGetBatch) to retrieve associated data,
    and publish results."""
    update_count = 0

    #Retrieve additional details for id criteria.
    std_data_results = perform_igetbatch_operation_for_standardized_id_set(std_id_set, req_state)

    LOGGER.info('Preparing to publish a total of %s records', len(std_data_results))
    # Publish results to Singer.
    for record in std_data_results:
        try:
            transformed_record = __transform_record(record, req_state.stream, 'none')
            write_record(req_state.stream_name, transformed_record, utils.now())
            LOGGER.info(transformed_record)
            update_count = update_count + 1

        except Exception as err:
            err_msg = 'error during transformation for entity (periodic data): ' \
                .format(err, transformed_record)
            LOGGER.error(err_msg)

    return update_count



