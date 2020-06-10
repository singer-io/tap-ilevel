
import singer
from singer import metrics
from .i_get_response import IGetResponse
from datetime import datetime, timedelta

LOGGER = singer.get_logger()

from .utils import get_num_days_diff
from .constants import MAX_ID_CHUNK_SIZE, MAX_DATE_WINDOW
from .transform import obj_to_dict, convert_ipush_event_to_obj, copy_i_get_result

import logging

"""
 Publish schema to singer
"""
def write_schema(catalog, stream_name):
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
def write_record(stream_name, record, time_extracted):

    try:
        singer.messages.write_record(stream_name, record, time_extracted=time_extracted)
    except OSError as err:
        LOGGER.info('OS Error writing record for: %s', stream_name)
        LOGGER.info('record: %s', record)
        LOGGER.info(err)
        raise err
    except TypeError as err:
        LOGGER.info('Type Error writing record for: %s', stream_name)
        LOGGER.info('record: %s', record)
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

    """Set current bookmark."""
    if 'bookmarks' not in state:
        state['bookmarks'] = {}
    state['bookmarks'][stream] = date_value
    singer.write_state(state)

"""
 Currently syncing sets the stream currently being delivered in the state.
 If the integration is interrupted, this state property is used to identify
  the starting point to continue from.
 Reference: https://github.com/singer-io/singer-python/blob/master/singer/bookmarks.py#L41-L46
"""
def update_currently_syncing(state, stream_name):
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
        raise AssertionError('Values supplied for max date window exceed threshhold, '+
                             start_dt.strftime(fmt) +' - '+ end_dt.strftime(fmt))
    with metrics.http_request_timer('Retrieve updated object data summary') as timer:
        object_type = client.factory.create('tns:UpdatedObjectTypes')
        call_response = client.service.GetUpdatedObjects(__get_asset_ref(object_type,
                                                                                 stream_name),
                                                                                start_dt,
                                                                                end_dt)
    if isinstance(call_response, str):
        return []

    updated_asset_ids_all = call_response.int

    if len(updated_asset_ids_all) < 1:
        return []

    return split_ids_into_chunks(updated_asset_ids_all, MAX_ID_CHUNK_SIZE)

def get_deleted_object_id_sets(start_dt, end_dt, client, stream_name):
    """Retrieve 'chunked' ids of objects that have have been deleted within the specified
        date windows."""
    with metrics.http_request_timer('Retrieve deleted object data summary') as timer:
        object_type = client.factory.create('tns:UpdatedObjectTypes')
        deleted_asset_ids_all = client.service.GetDeletedObjects(__get_asset_ref(object_type,
                                                                                 stream_name),
                                                                                start_dt,
                                                                                end_dt)
    if isinstance(deleted_asset_ids_all, str) or len(deleted_asset_ids_all):
        return []

    return split_ids_into_chunks(deleted_asset_ids_all, MAX_ID_CHUNK_SIZE)

"""
 Given stream name, identify the corresponding Soap identifier to send to the API. This is used 
 to identify the type of entity we are retrieving for certain API calls, GetUpdatedData(...) 
 for example. Both requests to get updated entities and requests to perform iGet operations for 
 these entities make use of the same calls to identify updated objects, which in turn rely on this 
 method for identifying updated records.
"""
def __get_asset_ref(attr, stream_ref):

    asset_types = ['assets', 'asset_periodic_data', 'asset_periodic_data_standardized']
    fund_types = ['funds', 'fund_periodic_data', 'fund_periodic_data_standardized']


    if stream_ref in asset_types:
        return attr.Asset
    elif stream_ref == 'currency_rates':
        return attr.CurrencyRate
    elif stream_ref == 'data_items':
        return attr.DataItem
    elif stream_ref in fund_types:
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
    elif stream_ref == 'fund_to_asset_relations':
        return attr.FundToAsset
    elif stream_ref == 'fund_to_fund_relations':
        return attr.FundToFund
    elif stream_ref == 'asset_to_asset_relations':
        return attr.AssetToAsset

    raise AssertionError('Unable to associate stream '+ stream_ref +' with value DataType')

    return None

def get_object_details_by_ids(object_ids, stream_name, client):
    """Given a set of object ids, return full details for objects. Calls to return data based on
    date window operations will return subsets of possible available attributes. This method
    provides the ability to take the id's produced by date specific calls and translate them into
    objects with additional attributes."""
    object_type = client.factory.create('tns:UpdatedObjectTypes')
    array_of_int = client.factory.create('ns3:ArrayOfint')
    array_of_int.int = object_ids
    with metrics.http_request_timer('Retrieve detailed info for objects by ids') as timer:
        call_response = client.service.GetObjectsByIds(__get_asset_ref(object_type, stream_name),
                                                       array_of_int)

    return call_response.NamedEntity

def get_investment_transaction_details_by_ids(object_ids, stream_name, client):
    """Given a set of object ids, return full details for objects. Calls to return data based on
    date window operations will return subsets of possible available attributes. This method
    provides the ability to take the id's produced by date specific calls and translate them into
    objects with additional attributes."""
    criteria = client.factory.create('InvestmentTransactionsSearchCriteria')
    criteria.TransactionIds.id = object_ids
    with metrics.http_request_timer('Retrieve detailed info for objects by ids') as timer:
        call_response = client.service.GetInvestmentTransactions(criteria)

    return call_response

def get_object_relation_details_by_ids(ids, stream_name, client):
    """Given a set of object ids, return full details for objects. Calls to return data based on
    date window operations will return subsets of possible available attributes. This method
    provides the ability to take the id's produced by date specific calls and translate them into
    objects with additional attributes."""

    criteria = client.factory.create('ns3:ArrayOfint')
    criteria.int = ids

    with metrics.http_request_timer('Retrieve detailed info for objects by ids') as timer:
        call_response = client.service.GetObjectRelationshipsByIds(criteria)

    return call_response.ObjectRelationship

def write_record(stream_name, record, time_extracted):
    """ Publish individual record. """

    try:
        singer.messages.write_record(stream_name, record, time_extracted=time_extracted)
    except OSError as err:
        LOGGER.info('OS Error writing record for: %s', stream_name)
        LOGGER.info('record: %s', record)
        LOGGER.info(err)
        raise err
    except TypeError as err:
        LOGGER.info('Type Error writing record for: %s', stream_name)
        LOGGER.info('record: %s', record)
        raise err

def createEntityPath(clientFactory, parentId, childId = None):
	idArray = clientFactory.create('ns3:ArrayOfint')
	idArray.int.append(parentId)
	if childId is not None:
		idArray.int.append(childId)

	entityPath = clientFactory.create('EntitiesPath')
	entityPath.Path = idArray

	return entityPath

def get_standardized_data_id_chunks(start_dt, end_dt, client):

    # Perform API call to retrieve 'standardized ids' in preparation for next call
    with metrics.http_request_timer('Retrieve standardized ids') as timer:
        LOGGER.info('API call: Translating records from ids to standardized ids for date range %s - %s', start_dt, end_dt)

        updated_data_ids = client.service.GetUpdatedData(start_dt, end_dt)
        LOGGER.info('Request time %s', timer.elapsed)

    # Validate that there is data to process
    if isinstance(updated_data_ids, str):
        return []

    updated_data_ids_arr = updated_data_ids.int

    return split_ids_into_chunks(updated_data_ids_arr, MAX_ID_CHUNK_SIZE)


def perform_iget_operation(obj_ids, stream_name, client):
    """Given a set of ids for a given stream type, perform 'iGetBatch' operations for all
    permutations of variables. This operation essentially provides the ability to track updates
    to top level entities based on a given set of parameters.

    At a high level, a set of ids are provided, for which other potential optional criteria may be
    introduced to create a set of any possible variations of criteria to retrieve attribute related
    data for the entities associated with the supplied set of ids.

    Each possible criteria combination is tracked, and becomes the basis for the resulting record(s)
    produced by this method. Each possible combination is associated with a unique representative
    'request id', a full set of criteria is built, then 'chunked' into 20k record limits to
    satisfy the limitations enforced by the API.

    As results are returned, the resulting value is trimmed 'if present', from the...

    Although the underlying API calls are constrained by record limits, an unlimited number of
    criteria elements may be supplied as parameters, and any related 'chunking' of records is
    handled internally."""
    results = []  # Contains criteria and value mappings for requests which contain an associated value

    i_get_params_req_result = build_iget_requests(obj_ids, stream_name, client)
    iget_bulk_criteria = i_get_params_req_result[0]  # Raw set of criteria that needs to be
                                                     # processed
    req_set_criteria = i_get_params_req_result[1]  # Refs to results that will be returned, and
                                                   # updated with corresponding updated records

    # Now that a set of requests for all possible combinations have been established, split criteria
    # into 20k sets, and then process.
    i_get_params_req_result_chunks = split_ids_into_chunks(iget_bulk_criteria,
                                                           MAX_ID_CHUNK_SIZE)

    iGetRequest = client.factory.create('DataServiceRequest')

    # Process each 20k chunk set, updating a data structure that will associated returned values with
    # the corresponding uniue request identifier
    for cur_criteria_set in i_get_params_req_result_chunks:
        i_get_params_list = client.factory.create('ArrayOfBaseRequestParameters')
        count = 1
        for cur_crtieria in cur_criteria_set:
            if count > 5:
                continue
            count = count + 1
            i_get_params_list.BaseRequestParameters.append(cur_crtieria)
        iGetRequest.ParametersList = i_get_params_list
        iGetRequest.IncludeStandardizedDataInfo = True
        iGetRequest.RetrieveComments = True
        iGetRequest.IncludeExcelFormula = True

        #TODO: Disable verbose logging
        #logging.getLogger('suds.client').setLevel(logging.DEBUG)
        #logging.getLogger('suds').setLevel(logging.DEBUG)

        with metrics.http_request_timer('Perform iGetBatch API call') as timer:
            iget_response = client.service.iGetBatch(
                iGetRequest)



        if isinstance(iget_response, str):
            return []

        reponse_arr = iget_response.DataValue

        """ 
            Process each resulting record. Unlike many operations, we manually create objects 
        and set values based on results returned from the API. This is perfomed in order to 
        'flatten' the results from the data.
        
            Prior to the API call, criteria supplied to the API call is associated with a unique 
        request id. The result returned from the API contains values and the associated values. 
        The following logic will re-map the values returned to the original criteria, create 
        a new object to represent the    
        """
        """
        for cur_response in reponse_arr:
            #req_key = str(cur_data_obj['RequestIdentifier'])
            if "Error" in cur_response:
                continue
            if "FormulaTypeIDsString" in cur_response:
                LOGGER.info(cur_response.FormulaTypeIDsString)
            if "Value" in cur_response:
                req_key = str(cur_response.RequestIdentifier)
                criteria = req_set_criteria[req_key]
                req_wrapper = convert_ipush_event_to_obj(cur_response)
                cur_data_obj = obj_to_dict(req_wrapper)

                #response_val =
                #criteria_val = req_set_criteria[req_key]


                results.append(cur_data_obj)"""
        for rec in reponse_arr:
            if "Error" in rec:
                continue

            if "IsNoDataValue" in rec:
                continue
            new_rec = convert_ipush_event_to_obj(rec)

            if len(rec.SDParameters.EntitiesPath.Path.int)>1:
                for i in range(len(rec.SDParameters.EntitiesPath.int)):
                    rec_copy = copy_i_get_result(new_rec)
                    rec_copy.EntityPath = rec.SDParameters.EntitiesPath.Path.int[i]
                    results.append(rec_copy)
            else:
                new_rec.EntityPath = rec.SDParameters.EntitiesPath.Path.int[0]
                results.append(new_rec)

    return results

def build_iget_requests(ids, stream_name, client):
    """Given a set of entity ids, within the context of a stream, build a set of criteria objects
    that are intended to be submitted to a future call. Note: this method will potentially create
    a set criteria elements that may exceed the max possible number of values that may be processed
    by an individual iGet request, so it is the reponsibility of the calling method to interpret the
    results, break them into boundary limits, and process them accordingly."""

    scenarioActualId = 1 #TODO: Enable other scenarios
    assetCurrency = None

    request_refs = [] #In addition to returning request criteria, we also need to track permutations
                      #of all request parameters.

    data_items = get_data_items_by_stream(stream_name, client)
    params_list = []

    # Build individual request parameters based on all combinations of selected entity ids and
    # corresponding applicable data_items. For each criteria, we track the generated unique request
    # handler, for mapping resulting value at a later step.
    request_mappings = {}
    req_id = 1

    for id in ids:
        for data_item in data_items:
            entity_path = create_entity_path(id, client)

            # Target object for publishing is constructed, and then tracked via unie request id
            # According to API docs, page 66 'Interpreting iGetBatch Results' the recommended
            # direction is to retain the original request criteria, make requests (in 20k max
            # batches) and then for which records return data, update the original criteria as
            # the response of what will ultimately be published to the data warehouse.
            iget_response = IGetResponse()
            iget_response.id = id
            iget_response.data_item_id = data_item.Id
            iget_response.scenario_id = scenarioActualId
            request_mappings[str(req_id)] = iget_response

            i_get_for_asset = create_iGet_from_input(client, entity_path, data_item.Id,
                                              assetCurrency, scenarioActualId, req_id)
            params_list.append(i_get_for_asset)
            req_id = req_id + 1

    return params_list, request_mappings

def get_data_items_by_stream(stream_name, client):
    """Retrieve IDs for data items based on the stream type: IDs of data items associated with the
    currrent stream type are used during the process of building iGet criteria. According to the
    documentation, we """
    data_items = None
    if stream_name == "asset_periodic_data":
        data_items = get_asset_data_items(client)
    elif stream_name == "fund_periodic_data":
        data_items = get_fund_data_items(client)
    #TODO: Implement additional stream types
    return data_items

def get_asset_data_items(client):
    """Return data item categories related to asset type entities"""
    id_arr = []
    id_arr.append(8)
    id_arr.append(14)
    return get_data_items_by_category_ids(id_arr, client)

def get_fund_data_items(client):
    """Return data item categories related to asset type entities"""
    id_arr = []
    id_arr.append(11)
    return get_data_items_by_category_ids(id_arr, client)

def get_data_items_by_category_ids(ids, client):
    """Given a set of category ids, retrieve data items based on supplied criteria. This approach
    is performed based on defined ranges based on stream type (API documentation: pages 57 implies
    that data items fall into specific ranges. Based on those ranges, unique category ids are
    identified, and in turn used to define which ids should be retrieved."""
    criteria = client.factory.create('DataItemsSearchCriteria')
    category_ids = client.factory.create('ns3:ArrayOfint')
    category_ids.int = ids
    criteria.CategoryIds = category_ids

    result = client.service.GetDataItems(criteria)[0]
    return result

def create_iGet_from_input(client, entityPath, dataItemId, currency, scenarioId, reqId):
    """Given a specific combination of criteria, create a Soap request wrapper representing the
    criteria."""

    iGetParams = client.factory.create('AssetAndFundGetRequestParameters')
    reportingPeriod = client.factory.create('Period')
    periodTypes = client.factory.create('PeriodTypes')
    reportingPeriod.Type = periodTypes.ReportingPeriod

    currentDate = client.factory.create('Date')
    dateTypes = client.factory.create('DateTypes')
    currentDate.Type = dateTypes.Current

    dataValueTypes = client.factory.create('DataValueTypes')

    iGetParams.RequestIdentifier = reqId
    iGetParams.DataValueType = getattr(dataValueTypes, 'None')
    iGetParams.EntitiesPath = entityPath
    iGetParams.DataItemId = dataItemId
    iGetParams.ScenarioId = scenarioId
    iGetParams.Period = reportingPeriod
    iGetParams.EndOfPeriod = currentDate
    iGetParams.ReportedDate = currentDate
    iGetParams.CurrencyCode = currency

    return iGetParams

def create_entity_path(id, client):
    idArray = client.factory.create('ns3:ArrayOfint')
    idArray.int.append(id)
    entityPath = client.factory.create('EntitiesPath')
    entityPath.Path = idArray
    return entityPath


"""
 When calls are performed to retrieve object details by id, we are restricted by a 20k limit, so 
 we need to support the ability to split a given set into chunks of a given size. Note, we are 
 accepting a SOAP data type (ArrayOfInts) and returning an array of arrays which will need to 
 be converted prior to submission to any additional SOAP calls.
"""
def split_ids_into_chunks(ids, max_len):
    result = []
    if len(ids) < max_len:
        cur_id_set = []
        for id in ids:
            cur_id_set.append(id)
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
        cur_index = 0
        source_index = 0
        while source_index < remaining_records:
            cur_id_set.append(ids[total_index])
            total_index = total_index + 1
            source_index = source_index + 1
        result.append(cur_id_set)

    return result

def get_start_date(stream_name, bookmark_type, state, start_date_str):
    """Get start date for a given stream. For streams that are configured to use 'datetime' as a bookmarking
    strategy, the last known bookmark is used (if present). Otherwise, the default start date value is used
    if no bookmark may be located, or in cases where a full table refresh is appropriate."""

    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')

    if bookmark_type != 'datetime':
        return datetime.strptime(start_date, '%Y-%m-%d')

    bookmark = get_bookmark(state, stream_name, start_date)

    if bookmark == None:
        return start_date

    return bookmark

def get_standardized_ids(id_set, start_date, end_date, req_state):
    """Given a set of ids, translate them into 'standardized ids', which is required for calls
    to iGetBatch operations. Method accepts a collection of ids which are expected to not exceed
    the maximum limit permitted by the API call. The relationship between ids and the corresponding
    'standardized ids' is one to many, so results are also 'chunked' into batches that meet the
    requirements of any iGetBatch operations performed in the future."""
    id_criteria = req_state.client.factory.create('ns3:ArrayOfint')
    id_criteria.int = id_set
    with metrics.http_request_timer('Retrieve standardized ids (getUpdatedData)') as timer:
        updated_data_ids = req_state.client.service.GetUpdatedData(start_date, end_date, id_criteria)

    # Validate that there is data to process
    if isinstance(updated_data_ids, str):
        return []

    updated_data_ids_arr = updated_data_ids.int
    return split_ids_into_chunks(updated_data_ids_arr, MAX_ID_CHUNK_SIZE)

def perform_igetbatch_operation_for_standardized_id_set(id_set, req_state):
    """Perform iGetBatch operations for a given set of 'stanrdized ids', which will return periodic
    data."""
    with metrics.http_request_timer('Perform iGetBatch for standardized ids') as timer:
        dataValueTypes = req_state.client.factory.create('DataValueTypes')
        iGetParamsList = req_state.client.factory.create('ArrayOfBaseRequestParameters')
        req_id = 1
        for id in id_set:
            req_id = req_id + 1
            iGetParams = req_state.client.factory.create('AssetAndFundGetRequestParameters')
            iGetParams.StandardizedDataId = id
            iGetParams.RequestIdentifier = req_id  # Our own id?
            iGetParams.DataValueType = getattr(dataValueTypes, 'ObjectId')
            iGetParamsList.BaseRequestParameters.append(iGetParams)

        iGetRequest = req_state.client.factory.create('DataServiceRequest')
        iGetRequest.IncludeStandardizedDataInfo = True
        iGetRequest.ParametersList = iGetParamsList
        with metrics.http_request_timer('Perform iGetBatch request for '+ str(len(id_set)) +' criteria items') as timer:
            data_values = req_state.client.service.iGetBatch(iGetRequest)
        if isinstance(data_values, str):
            return []

        period_data_records =  data_values.DataValue
        results = []
        for rec in period_data_records:
            if "Error" in rec:
                continue

            if "IsNoDataValue" in rec:
                continue

            new_rec = convert_ipush_event_to_obj(rec)

            if len(rec.SDParameters.EntitiesPath.Path.int)>1:
                for i in range(len(rec.SDParameters.EntitiesPath.Path.int)):
                    rec_copy = copy_i_get_result(new_rec)
                    rec_copy.EntityPath = rec.SDParameters.EntitiesPath.Path.int[i]
                    results.append(rec_copy)
            else:
                new_rec.EntityPath = rec.SDParameters.EntitiesPath.Path.int[0]
                results.append(new_rec)


        return results
