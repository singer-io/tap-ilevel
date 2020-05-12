from datetime import timedelta
import singer
from singer import metrics, metadata, Transformer, utils
from singer.utils import strptime_to_utc, strftime
from tap_ilevel.transform import transform_json
from tap_ilevel.streams import STREAMS
from dateutil.relativedelta import *

LOGGER = singer.get_logger()

def write_schema(catalog, stream_name):
    stream = catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()
    try:
        singer.write_schema(stream_name, schema, stream.key_properties)
    except OSError as err:
        LOGGER.info('OS Error writing schema for: {}'.format(stream_name))
        raise err


def write_record(stream_name, record, time_extracted):
    try:
        singer.messages.write_record(stream_name, record, time_extracted=time_extracted)
    except OSError as err:
        LOGGER.info('OS Error writing record for: {}'.format(stream_name))
        LOGGER.info('record: {}'.format(record))
        raise err
    except TypeError as err:
        LOGGER.info('Type Error writing record for: {}'.format(stream_name))
        LOGGER.info('record: {}'.format(record))
        raise err


def get_bookmark(state, stream, default):
    if (state is None) or ('bookmarks' not in state):
        return default
    return (
        state
        .get('bookmarks', {})
        .get(stream, default)
    )


def write_bookmark(state, stream, value):
    if 'bookmarks' not in state:
        state['bookmarks'] = {}
    state['bookmarks'][stream] = value
    LOGGER.info('Write state for stream: {}, value: {}'.format(stream, value))
    singer.write_state(state)


def transform_datetime(this_dttm):
    with Transformer() as transformer:
        new_dttm = transformer._transform_datetime(this_dttm)
    return new_dttm


def process_records(catalog, #pylint: disable=too-many-branches
                    stream_name,
                    records,
                    time_extracted,
                    bookmark_field=None,
                    bookmark_type=None,
                    max_bookmark_value=None,
                    last_datetime=None,
                    last_integer=None,
                    parent=None,
                    parent_id=None):
    stream = catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()
    stream_metadata = metadata.to_map(stream.metadata)

    with metrics.record_counter(stream_name) as counter:
        for record in records:
            # If child object, add parent_id to record
            if parent_id and parent:
                record[parent + '_id'] = parent_id

            # Transform record for Singer.io
            with Transformer() as transformer:
                transformed_record = transformer.transform(
                    record,
                    schema,
                    stream_metadata)

                # Reset max_bookmark_value to new value if higher
                if transformed_record.get(bookmark_field):
                    if max_bookmark_value is None or \
                        transformed_record[bookmark_field] > transform_datetime(max_bookmark_value):
                        max_bookmark_value = transformed_record[bookmark_field]

                if bookmark_field and (bookmark_field in transformed_record):
                    if bookmark_type == 'integer':
                        # Keep only records whose bookmark is after the last_integer
                        if transformed_record[bookmark_field] >= last_integer:
                            write_record(stream_name, transformed_record, \
                                time_extracted=time_extracted)
                            counter.increment()
                    elif bookmark_type == 'datetime':
                        last_dttm = transform_datetime(last_datetime)
                        bookmark_dttm = transform_datetime(transformed_record[bookmark_field])
                        # Keep only records whose bookmark is after the last_datetime
                        if bookmark_dttm:
                            if bookmark_dttm >= last_dttm:
                                write_record(stream_name, transformed_record, \
                                    time_extracted=time_extracted)
                                counter.increment()
                else:
                    write_record(stream_name, transformed_record, time_extracted=time_extracted)
                    counter.increment()

        return max_bookmark_value, counter.value

#
# Sync a specific endpoint (stream)
#
def sync_endpoint(client,
                  catalog,
                  state,
                  start_date,
                  stream_name,
                  path,
                  endpoint_config,
                  static_params,
                  selected_streams=None,
                  parent=None,
                  parent_id=None,
                  base_url=None):


    LOGGER.info('syncing stream :'+ stream_name)

    # Top level variables
    endpoint_total = 0
    total_records = 0
    params = endpoint_config.get('params', {})

    # Progress indicator for current stream
    def update_currently_syncing(state, stream_name)

    # Publish schema
    write_schema(catalog, stream_name)

    # Load bookmark data
    # TODO: Enable call
    last_bookmark_date = "2019-03-15"
    current_bookmark_date = "2019-06-15"

    # Currency rates are handled by a different API endpoint
    if stream_name == 'currency_rates':
        LOGGER.debug('CurrencyRate entities are handled by alternate API call [TODO]')
        return 0

    # Scenarios are handled by a different API endpoint
    if stream_name == 'scenarios':
        LOGGER.debug('Scenario entities are handled by alternate API call [TODO]')
        return 0

    # Initialization for date related operations:
    # Operations must be performed in 30 day (max) increments
    date_chunks = get_date_chunks(last_bookmark_date, current_bookmark_date, 30)
    LOGGER.info('Total number of date periods to process: ' + str(len(date_chunks)))
    cur_start_date = date_chunks[0]
    date_chunks.pop(0)
    cur_end_date = date_chunks[0]
    date_chunks.pop(0)
    cur_date_range_index = 1
    cur_date_criteria_length = len(date_chunks)

    # Main loop: Process records by date chunks
    for cur_date_criteria_index in range(cur_date_criteria_length):
        cur_date_criteria = date_chunks[cur_date_criteria_index]
        LOGGER.info('processing date range: ' + cur_start_date + "' '" + cur_end_date + "', " + str(
            cur_date_range_index) + " of " + str(cur_date_criteria_length))


    # Inner loop: Retrieve objects for ids

    # Convert object

    # Publish record





    return endpoint_total
   


"""
    Certain API calls have a limitation of 30 day periods, where the process might be launched with an overall 
    activity window of a greater period of time. Date ranges sorted into 30 day chunks in preparation
    for processing.

    Values provideed for input dates are in format rerquired by SOAP API (yyyy-mm-dd)
"""
def get_date_chunks(start_date, end_date):
    return ['2019-02-15','2019-03-15','2019-04-15','2019-05-15','2019-06-15','2019-07-15','2019-08-15']

def get_date_chunks(start_date_string, end_date_string, max_days):
    start_date = datetime.strptime(start_date_string, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_string, '%Y-%m-%d')
    td = timedelta(days=max_days)
    result = []

    days_dif = get_num_days_diff(start_date_string, end_date_string)
    if days_dif<max_days:
        return result

    working = True
    cur_date = start_date

    while working:
        next_date = cur_date + td
        if next_date == end_date or next_date > end_date:
            result.append(end_date_string)
            return result
        else:
            result.append(next_date.strftime("%Y-%m-%d"))
        cur_date = next_date

    return result

# Currently syncing sets the stream currently being delivered in the state.
# If the integration is interrupted, this state property is used to identify
#  the starting point to continue from.
# Reference: https://github.com/singer-io/singer-python/blob/master/singer/bookmarks.py#L41-L46
def update_currently_syncing(state, stream_name):
    LOGGER.info('Updating status of current stream processing')
    if (stream_name is None) and ('currently_syncing' in state):
        del state['currently_syncing']
    else:
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)


def sync(client, config, catalog, state, base_url):

    LOGGER.info('sync.py: sync()')

    if 'start_date' in config:
        start_date = config['start_date']

    # Get selected_streams from catalog, based on state last_stream
    #   last_stream = Previous currently synced stream, if the load was interrupted
    last_stream = singer.get_currently_syncing(state)
    LOGGER.info('last/currently syncing stream: {}'.format(last_stream))
    selected_streams = []
    for stream in catalog.get_selected_streams(state):
        selected_streams.append(stream.stream)
    LOGGER.info('selected_streams: {}'.format(selected_streams))

    if not selected_streams or selected_streams == []:
        return

    # Loop through endpoints in selected_streams
    for stream_name, endpoint_config in STREAMS.items():
        if stream_name in selected_streams:
            LOGGER.info('START Syncing: {}'.format(stream_name))
            update_currently_syncing(state, stream_name)
            path = endpoint_config.get('path', stream_name)
            bookmark_field = next(iter(endpoint_config.get('replication_keys', [])), None)
            write_schema(catalog, stream_name)
            total_records = 1

            # Main sync routine
            total_records = sync_endpoint(
                client=client,
                catalog=catalog,
                state=state,
                start_date=start_date,
                stream_name=stream_name,
                path=path,
                endpoint_config=endpoint_config,
                static_params=endpoint_config.get('params', {}),
                selected_streams=selected_streams,
                base_url=base_url)
            
            update_currently_syncing(state, None)
            LOGGER.info('FINISHED Syncing: {}, total_records: {}'.format(
                stream_name,
                total_records))

    LOGGER.info('sync.py: sync complete')

#
# Provides ability to determine number of days between two given dates.
#
def get_num_days_diff(start_date, end_date):
    delta = datetime.strptime(end_date,'%Y-%m-%d') - datetime.strptime(start_date,'%Y-%m-%d')
    return delta.days

#
# Historical data retrieval operation limited to 30 increments, need to support ability to break
# a range of dates into 30 day increments.
#
def get_date_chunks(start_date_string, end_date_string, max_days):
    start_date = datetime.strptime(start_date_string, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_string, '%Y-%m-%d')
    td = timedelta(days=max_days)
    result = []

    days_dif = get_num_days_diff(start_date_string, end_date_string)
    if days_dif<max_days:
        return result

    working = True
    cur_date = start_date

    while working:
        next_date = cur_date + td
        if next_date == end_date or next_date > end_date:
            result.append(end_date_string)
            return result
        else:
            result.append(next_date.strftime("%Y-%m-%d"))
        cur_date = next_date

    return result