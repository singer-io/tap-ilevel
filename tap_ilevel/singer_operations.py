from datetime import datetime
import singer

LOGGER = singer.get_logger()


# pylint: disable=unused-variable
# pylint: disable=too-many-instance-attributes
# pylint: disable=too-few-public-methods
class RequestState:
    def __init__(self):
        client = client = None
        stream_name = stream_name = None
        start_date = start_date = None
        last_date = last_date = None
        end_date = end_date = None
        state = state = None
        id_fields = None
        period_types = None
        bookmark_field = None
        stream = None
        catalog = None


# Given a series of common parameters, combine them into a data structure to minimize
#   complexity of passing frequently used data as method parameters.
def get_request_state(client, stream_name, start_date, last_date, end_date, state, bookmark_field,
                        id_fields, period_types, stream, catalog):
    # pylint: disable=attribute-defined-outside-init
    req_state = RequestState()
    req_state.client = client
    req_state.stream_name = stream_name
    req_state.start_date = start_date
    req_state.last_date = last_date

    end_date = end_date.strftime("%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    req_state.end_date = end_date
    req_state.state = state
    req_state.bookmark_field = bookmark_field
    req_state.id_fields = id_fields
    req_state.period_types = period_types
    req_state.stream = stream
    req_state.catalog = catalog
    return req_state


# Publish schema to singer.
def write_schema(catalog, stream_name):
    stream = catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()
    try:
        singer.write_schema(stream_name, schema, stream.key_properties)
    except OSError as err:
        LOGGER.info('OS Error writing schema for: %s', stream_name)
        raise err

# Publish individual record.
def write_record(stream_name, record, time_extracted):
    try:
        singer.messages.write_record(stream_name, record, time_extracted=time_extracted)
    except OSError as err:
        LOGGER.error('OS Error writing record for: %s', stream_name)
        LOGGER.error('record: %s', record)
        LOGGER.error(err)
        raise err
    except TypeError as err:
        LOGGER.error('Type Error writing record for: %s', stream_name)
        LOGGER.error('record: %s', record)
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
    LOGGER.info('Write state for stream: %s, value: %s', stream, value)
    singer.write_state(state)


# Currently syncing sets the stream currently being delivered in the state.
#   If the integration is interrupted, this state property is used to identify
#   the starting point to continue from.
# Reference: https://github.com/singer-io/singer-python/blob/master/singer/bookmarks.py#L41-L46
def update_currently_syncing(state, stream_name):
    LOGGER.info('Updating status of current stream processing')

    if (stream_name is None) and ('currently_syncing' in state):
        del state['currently_syncing']
    else:
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)
