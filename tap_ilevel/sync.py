from datetime import datetime

import singer
from singer import metrics, metadata, Transformer, utils

from tap_ilevel.transform import transform_json
from tap_ilevel.streams import STREAMS
import tap_ilevel.singer_operations as singer_ops
import tap_ilevel.ilevel_api as ilevel

from tap_ilevel.constants import ALL_RECORDS_STREAMS, INCREMENTAL_STREAMS, \
    STANDARDIZED_PERIODIC_DATA_STREAMS, MAX_DATE_WINDOW

LOGGER = singer.get_logger()


def transform_datetime(this_dttm):
    with Transformer() as transformer:
        # pylint: disable=protected-access
        new_dttm = transformer._transform_datetime(this_dttm)
    return new_dttm


# Set new attribute into entity used to track soft deletion status.
def __set_deletion_flag(record, is_soft_deleted=False):
    if is_soft_deleted is None:
        is_soft_deleted = False
    record['is_soft_deleted'] = is_soft_deleted


# Handle low level operations to publish records.
def process_records(result_records,
                    req_state,
                    deletion_flag=None,
                    max_bookmark_value=None):

    if not result_records or result_records is None or result_records == []:
        return max_bookmark_value, 0

    stream_name = req_state.stream_name
    bookmark_field = req_state.bookmark_field
    last_date = req_state.last_date

    LOGGER.info('%s: Preparing to publish %s records', stream_name, len(result_records))

    stream = req_state.catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()
    stream_metadata = metadata.to_map(stream.metadata)

    # Transform records
    try:
        transformed_data = transform_json(result_records)
    except Exception as err:
        LOGGER.error(err)
        LOGGER.error('result_records = %s', result_records)
        raise err

    with metrics.record_counter(req_state.stream_name) as counter:
        for record in transformed_data:
            # Add deletion flag to record
            __set_deletion_flag(record, deletion_flag)

            # Singer.io validate/transform vs. JSON schema
            with Transformer() as transformer:
                try:
                    transformed_record = transformer.transform(
                        record,
                        schema,
                        stream_metadata)
                except Exception as err:
                    LOGGER.error(err)
                    LOGGER.error('Error record: %s', record)
                    raise err

            # Reset max_bookmark_value to new value if higher
            if bookmark_field and (bookmark_field in transformed_record):
                bookmark_dt = transformed_record[bookmark_field][:10]

                bookmark_dttm = datetime.strptime(bookmark_dt, "%Y-%m-%d")
                if max_bookmark_value:
                    max_bookmark_value_dttm = datetime.strptime(max_bookmark_value, "%Y-%m-%d")
                    if bookmark_dttm > max_bookmark_value_dttm:
                        max_bookmark_value = bookmark_dt
                else:
                    max_bookmark_value = bookmark_dt

                last_dttm = datetime.strptime(last_date, "%Y-%m-%d")

                # Keep only records whose bookmark is on after the last_date
                if bookmark_dttm >= last_dttm:
                    singer_ops.write_record(
                        req_state.stream_name, transformed_record, utils.now())
                    counter.increment()
            else:
                singer_ops.write_record(
                    req_state.stream_name, transformed_record, utils.now())
                counter.increment()

        LOGGER.info('%s: Published %s records, max_bookmark_value: %s', stream_name,
                    counter.value, max_bookmark_value)
        return max_bookmark_value, counter.value


def __process_all_records_data_stream(req_state):
    max_bookmark_value = req_state.last_date

    record_count = 0
    records = ilevel.get_all_objects(req_state.stream_name, req_state.client)

    if len(records) == 0:
        return 0

    # Process records
    process_record_count = 0
    max_bookmark_value, process_record_count = process_records(
        result_records=records,
        req_state=req_state,
        deletion_flag=False,
        max_bookmark_value=max_bookmark_value)

    record_count = record_count + process_record_count

    # Data not sorted
    # Update the state with the max_bookmark_value for the stream after ALL records
    if req_state.bookmark_field and process_record_count > 0:
        singer_ops.write_bookmark(req_state.state, req_state.stream_name, max_bookmark_value)

    return record_count


def __process_updated_object_stream_id_set(object_ids, req_state, max_bookmark_value):
    update_count = 0

    if object_ids is None or object_ids == []:
        return max_bookmark_value, update_count

    if req_state.stream_name in INCREMENTAL_STREAMS:
        records = ilevel.get_object_details_by_ids(
            object_ids, req_state.stream_name, req_state.client)

    else: # Investment Transactions
        records = ilevel.get_investment_transaction_details_by_ids(
            object_ids, req_state.client)

    if len(records) == 0:
        return max_bookmark_value, update_count

    # Process records
    max_bookmark_value, process_record_count = process_records(
        result_records=records,
        req_state=req_state,
        deletion_flag=False,
        max_bookmark_value=max_bookmark_value)

    update_count = update_count + process_record_count

    return max_bookmark_value, update_count


def __process_deleted_object_stream_id_set(object_ids, req_state, max_bookmark_value):
    update_count = 0

    if object_ids is None or object_ids == []:
        return max_bookmark_value, update_count

    if req_state.stream_name in INCREMENTAL_STREAMS:
        records = ilevel.get_object_details_by_ids(
            object_ids, req_state.stream_name, req_state.client)

    else:
        records = ilevel.get_investment_transaction_details_by_ids(
            object_ids, req_state.client)

    if len(records) == 0:
        return max_bookmark_value, update_count

    # Process Deleted records
    max_bookmark_value, process_record_count = process_records(
        result_records=records,
        req_state=req_state,
        deletion_flag=True,
        max_bookmark_value=max_bookmark_value)

    update_count = update_count + process_record_count

    return max_bookmark_value, update_count


# Top level handler for processing default method of updating data.
def __process_incremental_stream(req_state):
    record_count = 0
    date_chunks = ilevel.get_date_chunks(req_state.last_date, req_state.end_date, MAX_DATE_WINDOW)
    max_bookmark_value_upd = req_state.last_date
    max_bookmark_value_del = req_state.last_date

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

        LOGGER.info('%s: Processing date range %s of %s total (%s - %s)',
                    req_state.stream_name, cur_date_range_index, cur_date_criteria_length,
                    cur_start_date, cur_end_date)

        #Retrieve updated entities for given date range, and send for processing
        updated_object_id_sets = ilevel.get_updated_object_id_sets(
            cur_start_date, cur_end_date, req_state.client, req_state.stream_name)

        update_bookmark = False
        if len(updated_object_id_sets) > 0:
            cur_id_set_index = 0
            for id_set in updated_object_id_sets:
                updated_record_count = 0
                LOGGER.info('%s: Processing id set %s of %s total sets',
                            req_state.stream_name, cur_id_set_index + 1, len(updated_object_id_sets))

                # Process updated object stream id set
                max_bookmark_value_upd, updated_record_count = \
                    __process_updated_object_stream_id_set(
                        object_ids=list(id_set),
                        req_state=req_state,
                        max_bookmark_value=max_bookmark_value_upd)

                record_count = record_count + updated_record_count
                if updated_record_count > 0:
                    update_bookmark = True

        #Retrieve deleted entities for given date range, and send for processing
        deleted_object_id_sets = ilevel.get_deleted_object_id_sets(
            cur_start_date, cur_end_date, req_state.client, req_state.stream_name)

        if len(deleted_object_id_sets) > 0:
            cur_id_set_index = 0
            for id_set in deleted_object_id_sets:
                deleted_record_count = 0
                LOGGER.info('%s: Processing deleted id set %s of %s total sets', req_state.stream_name,
                            cur_id_set_index + 1, len(deleted_object_id_sets))
                # Process deleted records
                max_bookmark_value_del, deleted_record_count = \
                    __process_deleted_object_stream_id_set(
                        object_ids=list(id_set),
                        req_state=req_state,
                        max_bookmark_value=max_bookmark_value_del)

                record_count = record_count + deleted_record_count
                if deleted_record_count > 0:
                    update_bookmark = True

                cur_id_set_index = cur_id_set_index + 1

        # Get max_bookmark_value from update (_1) and delete (_2)
        max_bookmark_value = max(req_state.start_date, req_state.last_date, \
            max_bookmark_value_upd, max_bookmark_value_del)
        max_bookmark_value_dttm = datetime.strptime(max_bookmark_value, "%Y-%m-%d")
        if max_bookmark_value_dttm > cur_end_date:
            max_bookmark_value = cur_end_date.strftime("%Y-%m-%d")

        # Data not sorted
        # Update the state with the max_bookmark_value for the stream after ALL records
        if update_bookmark:
            singer_ops.write_bookmark(req_state.state, req_state.stream_name, max_bookmark_value)

    return record_count


# Given a set of 'stanardized ids', reflecting attributes that have been updated for a given
#  time window, perform any additional API requests (iGetBatch) to retrieve associated data,
#  and publish results.
def process_iget_batch_for_standardized_id_set(std_id_set, req_state, max_bookmark_value):
    update_count = 0

    #Retrieve additional details for id criteria.
    std_data_results = ilevel.perform_igetbatch_operation_for_standardized_id_set(
        std_id_set, req_state)

    # Process records
    max_bookmark_value, process_record_count = process_records(
        result_records=std_data_results,
        req_state=req_state,
        deletion_flag=False,
        max_bookmark_value=max_bookmark_value)

    update_count = update_count + process_record_count

    return max_bookmark_value, update_count


# Retrieve periodic data. API docs under 'Migrating iLEVEL Data Changes (Deltas) to a Data
# Warehouse' (pp 67). Whereas other streams attempt to reflect updates to entities (Assets,
# Funds, InvestmentTransactions) operations for certain attributes will not be reflected in the
# API calls used to report updates. This call will reflect all attribute updates for the specified
# timeframe. Additionally, this call will reflect the state of an attribute at a given point in
# time (period).
def __process_standardized_data_stream(req_state):
    max_bookmark_value = req_state.last_date
    update_count = 0

    #Split date windows: API call restricts date windows based on 30 day periods.
    date_chunks = ilevel.get_date_chunks(req_state.last_date, req_state.end_date, MAX_DATE_WINDOW)

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
        updated_object_id_sets = ilevel.get_standardized_data_id_chunks(
            cur_start_date, cur_end_date, req_state.client)
        if len(updated_object_id_sets) == 0:
            continue

        LOGGER.info('Total number of updated sets is %s', len(updated_object_id_sets))

        #Translate standardized ids to objects
        for id_set in updated_object_id_sets:
            processed_record_count = 0
            LOGGER.info('Total number of records in current set %s', len(id_set))
            max_bookmark_value, processed_record_count = process_iget_batch_for_standardized_id_set(
                id_set, req_state, max_bookmark_value)
            update_count = update_count + processed_record_count

        # Some reported_date_value (bookmark) are in the future?
        max_bookmark_value_dttm = datetime.strptime(max_bookmark_value, "%Y-%m-%d")
        if max_bookmark_value_dttm > cur_end_date:
            max_bookmark_value = cur_end_date.strftime("%Y-%m-%d")

        date_chunk_index = date_chunk_index + 1

        # Data not sorted
        # Update the state with the max_bookmark_value for the stream after ALL records
        if req_state.bookmark_field and processed_record_count > 0:
            singer_ops.write_bookmark(req_state.state, req_state.stream_name, max_bookmark_value)

    return update_count


# Sync a specific endpoint (stream).
def __sync_endpoint(req_state):
    # Top level variables
    endpoint_total = 0

    with metrics.job_timer('endpoint_duration'):

        LOGGER.info('%s: STARTED Syncing stream', req_state.stream_name)
        singer_ops.update_currently_syncing(req_state.state, req_state.stream_name)

        # Publish schema to singer
        singer_ops.write_schema(req_state.catalog, req_state.stream_name)
        LOGGER.info('%s: Processing date window, %s to %s', req_state.stream_name,
                    req_state.last_date, req_state.end_date)

        if req_state.stream_name in ALL_RECORDS_STREAMS:
            endpoint_total = __process_all_records_data_stream(req_state)

        elif req_state.stream_name in STANDARDIZED_PERIODIC_DATA_STREAMS:
            endpoint_total = __process_standardized_data_stream(req_state)

        else:
            # data_items, investment_transactions
            endpoint_total = __process_incremental_stream(req_state)

        singer_ops.update_currently_syncing(req_state.state, None)
        LOGGER.info('%s: FINISHED Syncing Stream, total_records: %s',
                    req_state.stream_name, endpoint_total)

    LOGGER.info('sync.py: sync complete')

    return endpoint_total


# Main routine: orchestrates pulling data for selected streams.
def sync(client, config, catalog, state):
    start_date = config.get('start_date')[:10]

    # Get selected_streams from catalog, based on state last_stream
    #   last_stream = Previous currently synced stream, if the load was interrupted
    last_stream = singer.get_currently_syncing(state)
    LOGGER.info('last/currently syncing stream: %s', last_stream)
    selected_streams = []
    selected_streams_by_name = {}
    for stream in catalog.get_selected_streams(state):
        selected_streams.append(stream.stream)
        selected_streams_by_name[stream.stream] = stream

    if not selected_streams or selected_streams == []:
        return

    # Loop through endpoints in selected_streams
    for stream_name, endpoint_config in STREAMS.items():
        if stream_name in selected_streams:
            LOGGER.info('START Syncing: %s', stream_name)
            stream = selected_streams_by_name[stream_name]

            bookmark_field = next(iter(endpoint_config.get('replication_keys', [])), None)
            id_fields = endpoint_config.get('key_properties')
            singer_ops.write_schema(catalog, stream_name)
            total_records = 0

            last_date = singer_ops.get_bookmark(state, stream_name, start_date)

            #Request is made using currrent ddate + 1 as the end period.
            req_state = singer_ops.get_request_state(
                client=client,
                stream_name=stream_name,
                start_date=start_date,
                last_date=last_date,
                end_date=datetime.now(),
                state=state,
                bookmark_field=bookmark_field,
                id_fields=id_fields,
                stream=stream,
                catalog=catalog)

            # Main sync routine
            total_records = __sync_endpoint(req_state)

            LOGGER.info('FINISHED Syncing: %s, total_records: %s', stream_name, total_records)

    LOGGER.info('sync.py: sync complete')
