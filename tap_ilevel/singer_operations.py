from datetime import datetime
from singer import metrics
import singer
from tap_ilevel.constants import MAX_ID_CHUNK_SIZE, MAX_DATE_WINDOW
from tap_ilevel.transform import convert_ipush_event_to_obj, copy_i_get_result
from tap_ilevel.utils import get_num_days_diff
from .i_get_response import IGetResponse
from singer.utils import strptime_to_utc

LOGGER = singer.get_logger()

def write_schema(catalog, stream_name):
    """Publish schema to singer."""
    stream = catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()
    try:
        singer.write_schema(stream_name, schema, stream.key_properties)
    except OSError as err:
        LOGGER.info('OS Error writing schema for: %s', stream_name)
        raise err

def write_record(stream_name, record, time_extracted):
    """ Publish individual record. """
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
    """Retrieve current bookmark."""
    if (state is None) or ('bookmarks' not in state):
        return default
    return (
        state.get('bookmarks', {}).get(stream, default)
    )

def write_bookmark(state, stream, value):

    date_value = value.strftime('%Y-%m-%d')

    if 'bookmarks' not in state:
        state['bookmarks'] = {}
    state['bookmarks'][stream] = date_value
    singer.write_state(state)

def update_currently_syncing(state, stream_name):
    """ Currently syncing sets the stream currently being delivered in the state.
     If the integration is interrupted, this state property is used to identify
      the starting point to continue from.
     Reference: https://github.com/singer-io/singer-python/blob/master/singer/bookmarks.py#L41-L46.
     """

    LOGGER.info('Updating status of current stream processing')

    if (stream_name is None) and ('currently_syncing' in state):
        del state['currently_syncing']
    else:
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)

def get_updated_object_id_sets(start_dt, end_dt, client, stream_name):
    """Retrieve 'chunked' ids of objects that have have been created/updated within the specified
    date windows. Date window must not exceed maximum window period."""

    if get_num_days_diff(start_dt, end_dt) > MAX_DATE_WINDOW:
        fmt = "%Y-%m-%d"
        raise AssertionError('Values supplied for max date window exceed threshold, '+
                             start_dt.strftime(fmt) +' - '+ end_dt.strftime(fmt))
    # pylint: disable=unused-variable
    with metrics.http_request_timer('Retrieve updated object data summary') as timer:
        object_type = client.factory.create('tns:UpdatedObjectTypes')
        call_response = client.service.GetUpdatedObjects(__get_asset_ref(object_type, stream_name),
                                                         start_dt, end_dt)
    if isinstance(call_response, str):
        return []

    updated_asset_ids_all = call_response.int

    if len(updated_asset_ids_all) < 1:
        return []

    return split_ids_into_chunks(updated_asset_ids_all, MAX_ID_CHUNK_SIZE)

def get_deleted_object_id_sets(start_dt, end_dt, client, stream_name):
    """Retrieve 'chunked' ids of objects that have have been deleted within the specified
        date windows."""
    # pylint: disable=unused-variable
    with metrics.http_request_timer('Retrieve deleted object data summary') as timer:
        object_type = client.factory.create('tns:UpdatedObjectTypes')
        call_response = client.service.GetDeletedObjects(__get_asset_ref(object_type,
                                                                         stream_name),
                                                         start_dt, end_dt)

        if isinstance(call_response, str):
            return []

        deleted_asset_ids_all = call_response.int

    if isinstance(deleted_asset_ids_all, str) or len(deleted_asset_ids_all) < 1:
        return []

    return split_ids_into_chunks(deleted_asset_ids_all, MAX_ID_CHUNK_SIZE)

def __get_asset_ref(attr, stream_ref):
    """ Given stream name, identify the corresponding Soap identifier to send to the API. This is
    used  to identify the type of entity we are retrieving for certain API calls,
    GetUpdatedData(...) for example. Both requests to get updated entities and requests to perform
    iGet operations for  these entities make use of the same calls to identify updated objects,
    which in turn rely on this  method for identifying updated records. """

    if stream_ref in 'assets':
        return attr.Asset
    elif stream_ref == 'currency_rates':
        return attr.CurrencyRate
    elif stream_ref == 'data_items':
        return attr.DataItem
    elif stream_ref in 'funds':
        return attr.Fund
    elif stream_ref == 'investment_transactions':
        return attr.InvestmentTransaction
    elif stream_ref == 'scenarios':
        return attr.Scenario
    elif stream_ref == 'securities':
        return attr.Security
    elif stream_ref == 'segments':
        return attr.SegmentNode
    elif stream_ref == 'fund_to_asset_relations':
        return attr.FundToAsset
    elif stream_ref == 'fund_to_fund_relations':
        return attr.FundToFund
    elif stream_ref == 'asset_to_asset_relations':
        return attr.AssetToAsset

    raise AssertionError('Unable to associate stream '+ stream_ref +' with value DataType')

def get_object_details_by_ids(object_ids, stream_name, client):
    """Given a set of object ids, return full details for objects. Calls to return data based on
    date window operations will return subsets of possible available attributes. This method
    provides the ability to take the id's produced by date specific calls and translate them into
    objects with additional attributes."""
    object_type = client.factory.create('tns:UpdatedObjectTypes')
    array_of_int = client.factory.create('ns3:ArrayOfint')
    array_of_int.int = object_ids

    # pylint: disable=unused-variable
    with metrics.http_request_timer('Retrieve detailed info for objects by ids') as timer:
        call_response = client.service.GetObjectsByIds(__get_asset_ref(object_type, stream_name),
                                                       array_of_int)
    #Perform check to ensure that data was actually retruned. Observing instances where alghough
    #Ids identified for a type/ date window criteria set, No details are returned for this call.
    if isinstance(call_response, str):
        return []

    return call_response.NamedEntity

def get_investment_transaction_details_by_ids(object_ids, client):
    """Given a set of object ids, return full details for objects. Calls to return data based on
    date window operations will return subsets of possible available attributes. This method
    provides the ability to take the id's produced by date specific calls and translate them into
    objects with additional attributes."""
    criteria = client.factory.create('InvestmentTransactionsSearchCriteria')
    criteria.TransactionIds.int = object_ids

    # pylint: disable=unused-variable
    with metrics.http_request_timer('Retrieve detailed info for objects by ids') as timer:
        call_response = client.service.GetInvestmentTransactions(criteria)

    return call_response.InvestmentTransaction

def get_object_relation_details_by_ids(ids, client):
    """Given a set of object ids, return full details for objects. Calls to return data based on
    date window operations will return subsets of possible available attributes. This method
    provides the ability to take the id's produced by date specific calls and translate them into
    objects with additional attributes."""

    criteria = client.factory.create('ns3:ArrayOfint')
    criteria.int = ids

    # pylint: disable=unused-variable
    with metrics.http_request_timer('Retrieve detailed info for objects by ids') as timer:
        call_response = client.service.GetObjectRelationshipsByIds(criteria)

    return call_response.ObjectRelationship

def create_entity_path(client_factory, parent_id, child_id=None):
    id_array = client_factory.create('ns3:ArrayOfint')
    id_array.int.append(parent_id)
    if child_id is not None:
        id_array.int.append(child_id)

    entity_path = client_factory.create('EntitiesPath')
    entity_path.Path = id_array

    return entity_path

def get_standardized_data_id_chunks(start_dt, end_dt, client):

    # Perform API call to retrieve 'standardized ids' in preparation for next call
    with metrics.http_request_timer('Retrieve standardized ids') as timer:

        updated_data_ids = client.service.GetUpdatedData(start_dt, end_dt)
        LOGGER.info('Request time %s', timer.elapsed)

    # Validate that there is data to process
    if isinstance(updated_data_ids, str):
        return []

    updated_data_ids_arr = updated_data_ids.int
    return split_ids_into_chunks(updated_data_ids_arr, MAX_ID_CHUNK_SIZE)

def __build_iget_requests(ids, client):
    """Given a set of entity ids, within the context of a stream, build a set of criteria objects
    that are intended to be submitted to a future call. Note: this method will potentially create
    a set criteria elements that may exceed the max possible number of values that may be processed
    by an individual iGet request, so it is the reponsibility of the calling method to interpret the
    results, break them into boundary limits, and process them accordingly."""

    scenario_actual_id = 1
    asset_currency = None

    data_items = get_data_item_ids(client)
    params_list = []

    # Build individual request parameters based on all combinations of selected entity ids and
    # corresponding applicable data_items. For each criteria, we track the generated unique request
    # handler, for mapping resulting value at a later step.
    request_mappings = {}
    req_id = 1

    for cur_id in ids:
        for data_item in data_items:
            entity_path = create_entity_path(cur_id, client)

            # Target object for publishing is constructed, and then tracked via unie request id
            # According to API docs, page 66 'Interpreting iGetBatch Results' the recommended
            # direction is to retain the original request criteria, make requests (in 20k max
            # batches) and then for which records return data, update the original criteria as
            # the response of what will ultimately be published to the data warehouse.
            iget_response = IGetResponse()
            iget_response.id = id
            iget_response.data_item_id = data_item.Id
            iget_response.scenario_id = scenario_actual_id
            request_mappings[str(req_id)] = iget_response

            i_get_for_asset = create_iget_from_input(client, entity_path, data_item.Id,
                                                     asset_currency, scenario_actual_id, req_id)
            params_list.append(i_get_for_asset)
            req_id = req_id + 1

    return params_list, request_mappings

def get_data_item_ids(client):
    """Given a set of category ids, retrieve data items based on supplied criteria. This approach
    is performed based on defined ranges based on stream type (API documentation: pages 57 implies
    that data items fall into specific ranges. Based on those ranges, unique category ids are
    identified, and in turn used to define which ids should be retrieved."""
    criteria = client.factory.create('DataItemsSearchCriteria')
    category_ids = client.factory.create('ns3:ArrayOfint')
    id_arr = []
    id_arr.append(8)
    id_arr.append(11)
    id_arr.append(14)
    category_ids.int = id_arr
    criteria.CategoryIds = category_ids

    result = client.service.GetDataItems(criteria)[0]
    return result

def create_iget_from_input(client, entity_path, data_item_id, currency, scenario_id, req_id):
    """Given a specific combination of criteria, create a Soap request wrapper representing the
    criteria."""

    i_get_params = client.factory.create('AssetAndFundGetRequestParameters')
    reporting_period = client.factory.create('Period')
    period_types = client.factory.create('PeriodTypes')
    reporting_period.Type = period_types.ReportingPeriod

    current_date = client.factory.create('Date')
    date_types = client.factory.create('DateTypes')
    current_date.Type = date_types.Current

    i_get_params.RequestIdentifier = req_id
    i_get_params.DataValueType = getattr(date_types, 'None')
    i_get_params.EntitiesPath = entity_path
    i_get_params.DataItemId = data_item_id
    i_get_params.ScenarioId = scenario_id
    i_get_params.Period = reporting_period
    i_get_params.EndOfPeriod = current_date
    i_get_params.ReportedDate = current_date
    i_get_params.CurrencyCode = currency

    return i_get_params

def split_ids_into_chunks(ids, max_len):
    """
     When calls are performed to retrieve object details by id, we are restricted by a 20k limit, so
     we need to support the ability to split a given set into chunks of a given size. Note, we are
     accepting a SOAP data type (ArrayOfInts) and returning an array of arrays which will need to
     be converted prior to submission to any additional SOAP calls.
    """
    result = []
    if len(ids) < max_len:
        cur_id_set = []
        for cur_id in ids:
            cur_id_set.append(cur_id)
        result.append(cur_id_set)
        return result

    chunk_count = len(ids) // max_len
    remaining_records = len(ids) % max_len

    cur_chunk_index = 0
    total_index = 0
    source_index = 0
    while cur_chunk_index < chunk_count:
        cur_id_set = []
        while source_index < max_len:
            cur_id_set.append(ids[total_index])
            total_index = total_index + 1
            source_index = source_index + 1
        result.append(cur_id_set)
        cur_chunk_index = cur_chunk_index + 1

    if remaining_records > 0:
        source_index = 0
        cur_id_set = []
        cur_chunk_index = cur_chunk_index + 1
        source_index = 0
        while source_index < remaining_records:
            cur_id_set.append(ids[total_index])
            total_index = total_index + 1
            source_index = source_index + 1
        result.append(cur_id_set)

    return result

def get_start_date(stream_name, bookmark_type, state, start_date_str):
    """Get start date for a given stream. For streams that are configured to use 'datetime' as a
    bookmarking strategy, the last known bookmark is used (if present). Otherwise, the default
    start date value is used if no bookmark may be located, or in cases where a full table refresh
    is appropriate."""

    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')

    if bookmark_type != 'datetime':
        #return datetime.strptime(start_date, '%Y-%m-%d')
        return start_date

    bookmark = get_bookmark(state, stream_name, start_date)

    if bookmark is None:
        return start_date

    bookmark = strptime_to_utc(bookmark)

    return bookmark

def get_standardized_ids(id_set, start_date, end_date, req_state):
    """Given a set of ids, translate them into 'standardized ids', which is required for calls
    to iGetBatch operations. Method accepts a collection of ids which are expected to not exceed
    the maximum limit permitted by the API call. The relationship between ids and the corresponding
    'standardized ids' is one to many, so results are also 'chunked' into batches that meet the
    requirements of any iGetBatch operations performed in the future."""
    id_criteria = req_state.client.factory.create('ns3:ArrayOfint')
    id_criteria.int = id_set
    # pylint: disable=unused-variable
    with metrics.http_request_timer('Retrieve standardized ids (getUpdatedData)') as timer:
        updated_data_ids = req_state.client.service.GetUpdatedData(start_date, end_date,
                                                                   id_criteria)

    # Validate that there is data to process
    if isinstance(updated_data_ids, str):
        return []

    updated_data_ids_arr = updated_data_ids.int
    return split_ids_into_chunks(updated_data_ids_arr, MAX_ID_CHUNK_SIZE)

def perform_igetbatch_operation_for_standardized_id_set(id_set, req_state):
    """Perform iGetBatch operations for a given set of 'standardized ids', which will return
    periodic data."""
    # pylint: disable=unused-variable
    with metrics.http_request_timer('Perform iGetBatch for standardized ids') as timer:
        data_value_types = req_state.client.factory.create('DataValueTypes')
        i_get_params_list = req_state.client.factory.create('ArrayOfBaseRequestParameters')
        req_id = 0
        for cur_id in id_set:
            req_id = req_id + 1
            i_get_params = req_state.client.factory.create('AssetAndFundGetRequestParameters')
            i_get_params.StandardizedDataId = cur_id

            i_get_params.RequestIdentifier = req_id
            i_get_params.DataValueType = getattr(data_value_types, 'ObjectId')
            i_get_params_list.BaseRequestParameters.append(i_get_params)

        i_get_request = req_state.client.factory.create('DataServiceRequest')
        i_get_request.IncludeStandardizedDataInfo = True
        i_get_request.ParametersList = i_get_params_list
        # pylint: disable=unused-variable
        with metrics.http_request_timer('Perform iGetBatch request for '+ str(len(id_set))
                                        +' criteria items') as timer:
            data_values = req_state.client.service.iGetBatch(i_get_request)
        if isinstance(data_values, str):
            return []

        period_data_records = data_values.DataValue
        results = []
        for rec in period_data_records:
            if "Error" in rec:
                continue

            if "NoDataAvailable" in rec:
                continue

            if "Value" in rec:
                new_rec = convert_ipush_event_to_obj(rec)

                if len(rec.SDParameters.EntitiesPath.Path.int) > 1:
                    for i in range(len(rec.SDParameters.EntitiesPath.Path.int)):
                        rec_copy = copy_i_get_result(new_rec)
                        rec_copy.EntityPath = rec.SDParameters.EntitiesPath.Path.int[i]
                        results.append(rec_copy)
                else:
                    new_rec.EntityPath = rec.SDParameters.EntitiesPath.Path.int[0]
                    results.append(new_rec)


        return results
