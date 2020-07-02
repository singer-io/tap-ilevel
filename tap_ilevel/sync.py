from datetime import datetime
import json
from json import JSONEncoder
import singer
from singer import metrics, metadata, Transformer, utils
from tap_ilevel.transform import transform_json
from tap_ilevel.streams import STREAMS
import tap_ilevel.singer_operations as singer_ops
from .transform import obj_to_dict
from .request_state import RequestState
from .utils import get_date_chunks
from .constants import MAX_DATE_WINDOW, OBJECT_TYPE_STREAMS,\
    RELATION_TYPE_STREAM, INVESTMENT_TRANSACTIONS_STREAM

LOGGER = singer.get_logger()

def sync(client, config, catalog, state):
    """ Main routine: orchestrates pulling data for selected streams. """

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
            bookmark_type = endpoint_config.get('bookmark_type')
            singer_ops.write_schema(catalog, stream_name)
            total_records = 0
            data_key = endpoint_config.get('data_key')
            start_dt = singer_ops.get_start_date(stream_name, bookmark_type, state,
                                                 start_date)

            #Request is made using currrent ddate + 1 as the end period.
            req_state = __get_request_state(client, stream_name, data_key, start_dt,
                                            datetime.now(), state,
                                            bookmark_field, stream, path,
                                            catalog)

            # Main sync routine
            total_records = __sync_endpoint(req_state)


            LOGGER.info('FINISHED Syncing: {}, total_records: {}'.format(
                stream_name,
                total_records))

    LOGGER.info('sync.py: sync complete')

def __sync_endpoint(req_state):
    """ Sync a specific endpoint (stream). """
    # Top level variables
    endpoint_total = 0

    with metrics.job_timer('endpoint_duration'):

        LOGGER.info('syncing stream : %s', req_state.stream_name)
        singer_ops.update_currently_syncing(req_state.state, req_state.stream_name)

        # Publish schema to singer
        singer_ops.write_schema(req_state.catalog, req_state.stream_name)
        LOGGER.info('Processing date window for stream %s, %s to %s', req_state.stream_name,
                    req_state.start_dt, req_state.end_dt)

        if(req_state.stream_name in OBJECT_TYPE_STREAMS or req_state.stream_name in
           RELATION_TYPE_STREAM) or req_state.stream_name in INVESTMENT_TRANSACTIONS_STREAM:
            #Certain stream types follow a common processing pattern
            endpoint_total = __process_object_stream(req_state)
        else:
            endpoint_total = __process_standardized_data_stream(req_state)

        singer_ops.update_currently_syncing(req_state.state, None)
        LOGGER.info('FINISHED Syncing: %s, total_records: %s', req_state.stream_name,
                    endpoint_total)

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
        if cur_start_date is None:
            cur_start_date = date_chunks[0]
            cur_end_date = date_chunks[1]
            cur_date_range_index = 2
        else:
            cur_start_date = cur_end_date
            cur_end_date = date_chunks[cur_date_range_index]
            cur_date_range_index = cur_date_range_index + 1

        LOGGER.info('Processing date range %s of %s total (%s - %s) for stream %s',
                    cur_date_range_index,
                    cur_date_criteria_length, cur_start_date, cur_end_date, req_state.stream_name)

        #Retrieve updated entities for given date range, and send for processing
        updated_object_id_sets = singer_ops.get_updated_object_id_sets(cur_start_date,
                                                                       cur_end_date,
                                                                       req_state.client,
                                                                       req_state.stream_name)

        if len(updated_object_id_sets) > 0:
            cur_id_set_index = 0
            for id_set in updated_object_id_sets:
                LOGGER.info('Processing id set %s of %s total sets', cur_id_set_index,
                            len(updated_object_id_sets))
                record_count = record_count + __process_updated_object_stream_id_set(id_set,
                                                                                     req_state)


        #Retrieve deleted entities for given date range, and send for processing
        deleted_object_id_sets = singer_ops.get_deleted_object_id_sets(cur_start_date,
                                                                       cur_end_date,
                                                                       req_state.client,
                                                                       req_state.stream_name)
        if len(deleted_object_id_sets) > 0:
            cur_id_set_index = 0
            for id_set in deleted_object_id_sets:
                LOGGER.info('Processing id set %s of %s total sets', cur_id_set_index,
                            len(updated_object_id_sets))
                record_count = record_count + __process_deleted_object_stream_id_set(id_set,
                                                                                     req_state)

                cur_id_set_index = cur_id_set_index + 1

        singer_ops.write_bookmark(req_state.state, req_state.stream_name, cur_end_date)

    return record_count

def __process_updated_object_stream_id_set(object_ids, req_state):
    update_count = 0

    if req_state.stream_name in OBJECT_TYPE_STREAMS:
        records = singer_ops.get_object_details_by_ids(object_ids, req_state.stream_name,
                                                       req_state.client)
    elif req_state.stream_name in RELATION_TYPE_STREAM:
        records = singer_ops.get_object_relation_details_by_ids(object_ids, req_state.client)
    else:
        records = singer_ops.get_investment_transaction_details_by_ids(object_ids,
                                                                       req_state.client)
    if len(records) == 0:
        return 0

    update_count = update_count + process_records(records, req_state)
    return update_count

def __process_deleted_object_stream_id_set(object_ids, req_state):
    update_count = 0

    if req_state.stream_name in OBJECT_TYPE_STREAMS:
        records = singer_ops.get_object_details_by_ids(object_ids, req_state.stream_name,
                                                       req_state.client)
    elif req_state.stream_name in RELATION_TYPE_STREAM:
        records = singer_ops.get_object_relation_details_by_ids(object_ids, req_state.client)
    else:
        records = singer_ops.get_investment_transaction_details_by_ids(object_ids,
                                                                       req_state.client)
    if len(records) == 0:
        return 0

    update_count = update_count + process_records(records, req_state, True)
    return update_count

def __set_deletion_flag(entity, is_soft_deleted=False):
    """Set new attribute into entity used to track soft deletion status."""
    if is_soft_deleted is None:
        is_soft_deleted = False
    entity['is_soft_deleted'] = is_soft_deleted

def process_records(records, req_state, deletion_flag=None):
    """Handle low level operations to publish records."""
    update_count = 0

    for record in records:
        try:
            transformed_record = __transform_record(record, req_state.stream)
            __set_deletion_flag(transformed_record, deletion_flag)
            singer_ops.write_record(req_state.stream_name, transformed_record, utils.now())
            update_count = update_count + 1
        except Exception as err: # pylint: disable=broad-except
            err_msg = 'Error during transformation for entity: {}, for type: {}, obj: {}'\
                .format(err, req_state.stream_name, transformed_record)
            LOGGER.error(err_msg)

    LOGGER.info('process_object_set: total record count is %s ', update_count)
    return update_count


def __transform_record(record, stream):
    """ Make data more 'database compliant', i.e. rename columns, convert to UTC timezones, etc.
     'transform_data' method needs to ensure raw data matches that in the schemas. """
    obj_dict = obj_to_dict(record) #Convert SOAP object to dict
    object_json_str = json.dumps(obj_dict, indent=4, cls=DateTimeEncoder)
    object_json_str = object_json_str.replace('True', 'true')
    object_json = json.loads(object_json_str) #Parse JSON

    stream_metadata = metadata.to_map(stream.metadata)
    transformed_data = transform_json(object_json)

    # singer validation check
    with Transformer() as transformer:
        transformed_record = transformer.transform(
            transformed_data,
            stream.schema.to_dict(),
            stream_metadata)

    return transformed_record

class DateTimeEncoder(JSONEncoder):

    def default(self, obj): # pylint: disable=arguments-differ, method-hidden
        if isinstance(obj, datetime):
            return obj.isoformat()
        return obj

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

        if cur_start_date is None:
            cur_start_date = date_chunks[0]
            cur_end_date = date_chunks[1]
            cur_date_range_index = 2
        else:
            cur_start_date = cur_end_date
            cur_end_date = date_chunks[cur_date_range_index]
            cur_date_range_index = cur_date_range_index + 1

        LOGGER.info('Processing date range %s of %s total %s - %s for stream: %s',
                    cur_date_range_index, cur_date_criteria_length, cur_start_date,
                    cur_end_date, req_state.stream_name)



        #Get updated records based on date range
        updated_object_id_sets = singer_ops.get_standardized_data_id_chunks(cur_start_date,
                                                                            cur_end_date,
                                                                            req_state.client)
        if len(updated_object_id_sets) == 0:
            continue

        LOGGER.info('Total number of updated sets is %s', len(updated_object_id_sets))

        #Translate standardized ids to objects
        for id_set in updated_object_id_sets:
            LOGGER.info('Total number of records in current set %s', len(id_set))
            update_count = update_count + process_iget_batch_for_standardized_id_set(id_set,
                                                                                     req_state)

        singer_ops.write_bookmark(req_state.state, req_state.stream_name, cur_end_date)
        date_chunk_index = date_chunk_index + 1

    return update_count

def process_iget_batch_for_standardized_id_set(std_id_set, req_state):
    """Given a set of 'stanardized ids', reflecting attributes that have been updated for a given
    time window, perform any additional API requests (iGetBatch) to retrieve associated data,
    and publish results."""
    update_count = 0

    #Retrieve additional details for id criteria.
    std_data_results = singer_ops.perform_igetbatch_operation_for_standardized_id_set\
        (std_id_set, req_state)

    LOGGER.info('Preparing to publish a total of %s records', len(std_data_results))
    # Publish results to Singer.
    for record in std_data_results:
        try:

            transformed_record = __transform_record(record, req_state.stream)
            singer_ops.write_record(req_state.stream_name, transformed_record, utils.now())
            update_count = update_count + 1

        except Exception as err: # pylint: disable=broad-except
            err_msg = 'error during transformation for entity (periodic data): {} {} ' \
                .format(err, transformed_record)
            LOGGER.error(err_msg)

    return update_count
