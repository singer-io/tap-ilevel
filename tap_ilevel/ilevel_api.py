from datetime import time, datetime, timedelta
import dateutil.parser
import json

import singer
from singer import metrics

from tap_ilevel.constants import MAX_ID_CHUNK_SIZE, MAX_DATE_WINDOW
from tap_ilevel.transform import transform_json, hash_data

LOGGER = singer.get_logger()


# Certain API calls have a limitation of 30 day periods, where the process might be launched
#  with an overall activity window of a greater period of time. Date ranges sorted into 30
#  day chunks in preparation for processing.
# Values provided for input dates are in format rerquired by SOAP API (yyyy-mm-dd)
# API calls are performed within a maximum 30 day timeframe, so breaking a period of time
#  between two into limited 'chunks' is required
def get_date_chunks(start_date, end_date, max_days):
    result = []

    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, '%Y-%m-%d')

    days_dif = abs((start_date - end_date).days)
    if days_dif < max_days:
        result.append(start_date)
        result.append(end_date)
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

        next_date = next_date.strftime("%Y-%m-%d")
        next_date = datetime.strptime(next_date, "%Y-%m-%d")
        result.append(next_date)

    return result


# Convert an object to a dictionary object, dates are converted as required.
def obj_to_dict(obj):
    if not  hasattr(obj, "__dict__"):
        if isinstance(obj, (datetime, time)):
            # All iLevel datateimes are UTC (Z) time zone
            dttm = '{}Z'.format(obj.isoformat()).replace('+00:00', '')
            return dttm
        elif type(obj) == type(None):
            return obj
        elif isinstance(obj, (int, float, bool, list, dict)):
            return obj
        else:
            return str(obj)
    result = {}
    for key, val in obj.__dict__.items():
        if key.startswith("_"):
            continue
        element = []
        if isinstance(val, list):
            for item in val:
                element.append(obj_to_dict(item))
        else:
            element = obj_to_dict(val)
        result[key] = element

    return result


# Converts a suds object to a dict.
# :param json_serialize: If set, changes date and time types to iso string.
# :param key_to_lower: If set, changes index key name to lower case.
# :param obj: suds object
# :return: dict object
# Reference: https://stackoverflow.com/questions/17581731/parsing-suds-soap-complex-data-type-into-python-dict
def sobject_to_dict(obj, key_to_lower=False, json_serialize=True):

    if not hasattr(obj, '__keylist__'):
        if json_serialize and isinstance(obj, (datetime, time)):
            # All iLevel datateimes are UTC (Z) time zone
            dttm = '{}Z'.format(obj.isoformat()).replace('+00:00', '')
            return dttm
        elif type(obj) == type(None):
            return obj
        elif isinstance(obj, (int, float, bool, list, dict)):
            return obj
        else:
            return str(obj)
    data = {}
    fields = obj.__keylist__
    for field in fields:
        val = getattr(obj, field)
        if key_to_lower:
            field = field.lower()
        if isinstance(val, list):
            data[field] = []
            for item in val:
                data[field].append(sobject_to_dict(item, json_serialize=json_serialize))
        else:
            data[field] = sobject_to_dict(val, json_serialize=json_serialize)
    return data


# Convert ISO 8601 formatted date string into time zone unaware
def convert_iso_8601_date(date_str):
    if isinstance(date_str, datetime):
        date_str = date_str.strftime("%Y-%m-%d")

    cur_date_ref = dateutil.parser.parse(date_str)
    cur_date_ref = cur_date_ref.replace(tzinfo=None)
    return cur_date_ref


# Given stream name, identify the corresponding Soap identifier to send to the API. This is
#  used  to identify the type of entity we are retrieving for certain API calls,
#  GetUpdatedData(...) for example. Both requests to get updated entities and requests to perform
#  iGet operations for  these entities make use of the same calls to identify updated objects,
#  which in turn rely on this  method for identifying updated records.
def __get_asset_ref(attr, stream_ref):
    if stream_ref in 'assets':
        return attr.Asset, 'NamedEntity'
    elif stream_ref == 'currency_rates':
        return attr.CurrencyRate, 'CurrencyRate'
    elif stream_ref == 'data_items':
        return attr.DataItem, 'NamedEntity'
    elif stream_ref in 'funds':
        return attr.Fund, 'Fund'
    elif stream_ref == 'investments':
        return attr.Investment, 'Investment'
    elif stream_ref == 'investment_transactions':
        return attr.InvestmentTransaction, 'InvestmentTransaction'
    elif stream_ref == 'scenarios':
        return attr.Scenario, 'Scenario'
    elif stream_ref == 'securities':
        return attr.Security, 'Security'
    elif stream_ref == 'segments':
        return attr.SegmentNode, 'SegmentNode'
    elif stream_ref == 'fund_to_asset_relations':
        return attr.FundToAsset, 'ObjectRelationship'
    elif stream_ref == 'fund_to_fund_relations':
        return attr.FundToFund, 'ObjectRelationship'
    elif stream_ref == 'asset_to_asset_relations':
        return attr.AssetToAsset, 'ObjectRelationship'

    raise AssertionError('Unable to associate stream '+ stream_ref +' with value DataType')


# Used for objects with fewer records to get ALL records
def get_all_objects(stream_name, client):
    # pylint: disable=unused-variable
    objectType = client.factory.create('ObjectTypes')
    with metrics.http_request_timer('{}: Retrieve all objects') as timer:
        if stream_name == 'funds':
            call_response = client.service.GetFunds()
            data_key = 'Fund'
        elif stream_name == 'assets':
            call_response = client.service.GetAssets()
            data_key = 'Asset'
        elif stream_name == 'scenarios':
            call_response = client.service.GetScenarios()
            data_key = 'NamedEntity'
        elif stream_name == 'securities':
            call_response = client.service.GetSecurities()
            data_key = 'Security'
        elif stream_name == 'investments':
            call_response = client.service.GetInvestments()
            data_key = 'Investment'
        elif stream_name == 'asset_to_asset_relations':
            relationships = client.service.GetObjectRelationships()
            call_response = (relation for relation in relationships.ObjectRelationship if \
                relation.TypeId == objectType.AssetToAsset)
            data_key = 'ObjectRelationship'
        elif stream_name == 'fund_to_asset_relations':
            relationships = client.service.GetObjectRelationships()
            call_response = (relation for relation in relationships.ObjectRelationship if \
                relation.TypeId == objectType.FundToAsset)
            data_key = 'ObjectRelationship'
        elif stream_name == 'fund_to_fund_relations':
            relationships = client.service.GetObjectRelationships()
            call_response = (relation for relation in relationships.ObjectRelationship if \
                relation.TypeId == objectType.FundToFund)
            data_key = 'ObjectRelationship'
        elif stream_name == 'data_items':
            searchCriteria = client.factory.create('DataItemsSearchCriteria')
            searchCriteria.GetGlobalDataItemsOnly = False
            call_response = client.service.GetDataItems(searchCriteria)
            data_key = 'DataItemObjectEx'

    #Perform check to ensure that data was actually retruned. Observing instances where alghough
    #Ids identified for a type/ date window criteria set, No details are returned for this call.
    if isinstance(call_response, str):
        response = []

    response = []
    if data_key == 'ObjectRelationship':
        for relation in call_response:
            response.append(sobject_to_dict(relation))
    else:
        try:
            response = sobject_to_dict(call_response).get(data_key, [])
        except AttributeError as err:
            LOGGER.info('ERROR call_response = {}'.format(sobject_to_dict(call_response)))
            pass

    return response


# Given a set of object ids, return full details for objects. Calls to return data based on
#  date window operations will return subsets of possible available attributes. This method
#  provides the ability to take the id's produced by date specific calls and translate them into
#  objects with additional attributes.
def get_object_details_by_ids(object_ids, stream_name, client):
    object_type = client.factory.create('tns:UpdatedObjectTypes')
    asset_ref, data_key = __get_asset_ref(object_type, stream_name)
    array_of_int = client.factory.create('ns3:ArrayOfint')
    array_of_int.int = object_ids

    # pylint: disable=unused-variable
    with metrics.http_request_timer('Retrieve detailed info for objects by ids') as timer:
        call_response = client.service.GetObjectsByIds(asset_ref, array_of_int)
    # LOGGER.info('call_response dict = {}'.format(sobject_to_dict(call_response))) # COMMENT OUT

    #Perform check to ensure that data was actually retruned. Observing instances where alghough
    #Ids identified for a type/ date window criteria set, No details are returned for this call.
    if isinstance(call_response, str):
        response = []

    response = []
    try:
        # response = call_response.NamedEntity
        response = sobject_to_dict(call_response).get(data_key, [])
    except AttributeError as err:
        LOGGER.info('ERROR call_response = {}'.format(sobject_to_dict(call_response)))
        pass

    return response


# When calls are performed to retrieve object details by id, we are restricted by a 20k limit, so
#  we need to support the ability to split a given set into chunks of a given size. Note, we are
#  accepting a SOAP data type (ArrayOfInts) and returning an array of arrays which will need to
#  be converted prior to submission to any additional SOAP calls.
def split_ids_into_chunks(data, max_len):
    chunks = [data[x:x+max_len] for x in range(0, len(data), max_len)]
    return chunks


# Retrieve 'chunked' ids of objects that have have been deleted within the specified
#  date windows.
def get_deleted_object_id_sets(start_dt, end_dt, client, stream_name):
    object_type = client.factory.create('tns:UpdatedObjectTypes')
    asset_ref, data_key = __get_asset_ref(object_type, stream_name)

    # pylint: disable=unused-variable
    with metrics.http_request_timer('Retrieve deleted object data summary') as timer:
        call_response = client.service.GetDeletedObjects(asset_ref, start_dt, end_dt)

    if isinstance(call_response, str):
        return []

    try:
        deleted_asset_ids_all = call_response.int
    except AttributeError as err:
        LOGGER.info('ERROR call_response = {}'.format(sobject_to_dict(call_response)))
        pass

    if isinstance(deleted_asset_ids_all, str) or len(deleted_asset_ids_all) < 1:
        return []

    return split_ids_into_chunks(deleted_asset_ids_all, MAX_ID_CHUNK_SIZE)


# Retrieve 'chunked' ids of objects that have have been created/updated within the specified
#  date windows. Date window must not exceed maximum window period.
def get_updated_object_id_sets(start_dt, end_dt, client, stream_name):
    object_type = client.factory.create('tns:UpdatedObjectTypes')
    asset_ref, data_key = __get_asset_ref(object_type, stream_name)

    if abs((start_dt - end_dt).days) > MAX_DATE_WINDOW:
        fmt = "%Y-%m-%d"
        raise AssertionError('Values supplied for max date window exceed threshold, '+
                             start_dt.strftime(fmt) +' - '+ end_dt.strftime(fmt))
    # pylint: disable=unused-variable
    with metrics.http_request_timer('Retrieve updated object data summary') as timer:
        call_response = client.service.GetUpdatedObjects(asset_ref, start_dt, end_dt)
    # LOGGER.info('call_response dict = {}'.format(sobject_to_dict(call_response))) # COMMENT OUT

    if isinstance(call_response, str):
        return []

    updated_asset_ids_all = []
    try:
        updated_asset_ids_all = call_response.int
    except AttributeError as err:
        LOGGER.info('ERROR call_response = {}'.format(sobject_to_dict(call_response)))
        pass

    if isinstance(updated_asset_ids_all, str) or len(updated_asset_ids_all) < 1:
        return []

    return split_ids_into_chunks(updated_asset_ids_all, MAX_ID_CHUNK_SIZE)


# Given a set of object ids, return full details for objects. Calls to return data based on
#  date window operations will return subsets of possible available attributes. This method
#  provides the ability to take the id's produced by date specific calls and translate them into
#  objects with additional attributes.
def get_investment_transaction_details_by_ids(object_ids, client):
    criteria = client.factory.create('InvestmentTransactionsSearchCriteria')
    criteria.TransactionIds.int = object_ids

    # pylint: disable=unused-variable
    with metrics.http_request_timer('Retrieve detailed info for objects by ids') as timer:
        call_response = client.service.GetInvestmentTransactions(criteria)

    # Validate that there is data to process
    if isinstance(call_response, str):
        return []

    # LOGGER.info('call_response dict = {}'.format(sobject_to_dict(call_response))) # COMMENT OUT

    # TODO: Fix issue w/ missing .InvestmentTransaction for last batch
    response = []
    try:
        # response = call_response.InvestmentTransaction
        response = sobject_to_dict(call_response).get('InvestmentTransaction', [])
    except AttributeError as err:
        LOGGER.info('{}'.format(err))
        LOGGER.info('ERROR criteria = {}'.format(criteria))
        LOGGER.info('ERROR call_response dict = {}'.format(sobject_to_dict(call_response)))
        raise err

    return response


def get_standardized_data_id_chunks(start_dt, end_dt, client):
    adj_start_date = start_dt - timedelta(days=2)
    adj_end_date = end_dt + timedelta(days=2)

    # Perform API call to retrieve 'standardized ids' in preparation for next call
    with metrics.http_request_timer('Retrieve standardized ids') as timer:
        updated_data_ids = client.service.GetUpdatedData(adj_start_date, adj_end_date)
        LOGGER.info('Request time %s', timer.elapsed)

    # Validate that there is data to process
    if isinstance(updated_data_ids, str):
        return []

    data = updated_data_ids.int  
    chunks = split_ids_into_chunks(data, MAX_ID_CHUNK_SIZE)
    
    return chunks


# Perform iGetBatch operations for a given set of 'standardized ids', which will return
#  periodic data.
def perform_igetbatch_operation_for_standardized_id_set(id_set, req_state):
    data_value_types = req_state.client.factory.create('DataValueTypes')

    # current_date
    date_types = req_state.client.factory.create('DateTypes')
    current_date = req_state.client.factory.create('Date')
    current_date.Type = date_types.Current
    
    # latest_date
    latest_date = req_state.client.factory.create('Date')
    latest_date.Type = date_types.Latest

    req_id = 1
    id_set_len = len(id_set)
    i_get_params_list = req_state.client.factory.create('ArrayOfBaseRequestParameters')
    for cur_id in id_set:
        req_id = req_id + 1
        i_get_params = req_state.client.factory.create('AssetAndFundGetRequestParameters')
        i_get_params.StandardizedDataId = cur_id

        i_get_params.RequestIdentifier = req_id
        i_get_params.DataValueType = getattr(data_value_types, 'ObjectId')
        i_get_params.EndOfPeriod = latest_date
        i_get_params.ReportedDate = current_date

        i_get_params_list.BaseRequestParameters.append(i_get_params)

    i_get_request = req_state.client.factory.create('DataServiceRequest')
    i_get_request.IncludeStandardizedDataInfo = True
    i_get_request.IncludeExcelFormula = True
    i_get_request.ParametersList = i_get_params_list

    # pylint: disable=unused-variable
    metrics_string = ('Standardized Data Item iGetBatch: {} requests'.format(id_set_len))
    with metrics.http_request_timer(metrics_string) as timer:
        data_values = req_state.client.service.iGetBatch(i_get_request)

    # LOGGER.info('data_values dict = {}'.format(sobject_to_dict(data_values))) # COMMENT OUT

    if isinstance(data_values, str):
        return []

    try:
        periodic_data_records = data_values.DataValue
    except Exception as err:
        LOGGER.error('{}'.format(err))
        LOGGER.error('data_values dict = {}'.format(sobject_to_dict(data_values)))
        raise err

    results = []
    for periodic_data_record in periodic_data_records:
        if "Error" in periodic_data_record:
            continue

        if "NoDataAvailable" in periodic_data_record:
            continue

        periodic_data_record_dict = sobject_to_dict(periodic_data_record)
        # LOGGER.info('period_data_record_dict = {}'.format(periodic_data_record_dict)) # COMMENT OUT

        transformed_record = transform_json(periodic_data_record_dict)
        # LOGGER.info('transformed_record = {}'.format(transformed_record)) # COMMENT OUT

        if 'value' in transformed_record:
            value = transformed_record.get('value')
            value_string = str(value)
            if type(value) in (int, float):
                value_numeric = float(value)
            else:
                value_numeric = None
            if value == 'No Data Available':
                continue

            sd_parameters = transformed_record.get('sd_parameters', {})
            excel_formula = transformed_record.get('excel_formula')
            currency_code = sd_parameters.get('currency_code')
            data_item_id = sd_parameters.get('data_item_id')
            data_value_type = sd_parameters.get('data_value_type')
            detail_id = sd_parameters.get('detail_id')
            scenario_id = sd_parameters.get('scenario_id')
            period_type = sd_parameters.get('period', {}).get('type')
            end_of_period_value = sd_parameters.get('end_of_period', {}).get('value')
            reported_date_value = sd_parameters.get('reported_date', {}).get('value')
            exchange_rate_type = sd_parameters.get('exchange_rate', {}).get('type')
            request_id = sd_parameters.get('request_identifier')
            standardized_data_id = sd_parameters.get('standardized_data_id')

            entity_ids = sd_parameters.get('entities_path', {}).get('path', {}).get('int', [])
            for entity_id in entity_ids:
                # Primary key dimensions, create md5 hash key
                dimensions = {
                    'data_item_id': data_item_id,
                    'entity_id': entity_id,
                    'scenario_id': scenario_id,
                    'period_type': period_type,
                    'end_of_period_value': end_of_period_value,
                    'currency_code': currency_code,
                    'exchange_rate_type': exchange_rate_type,
                    'data_value_type': data_value_type
                }
                hash_key = str(hash_data(json.dumps(dimensions, sort_keys=True)))
                new_record = {
                    'hash_key': hash_key,
                    'excel_formula': excel_formula,
                    'currency_code': currency_code,
                    'data_item_id': data_item_id,
                    'data_value_type': data_value_type,
                    'detail_id': detail_id,
                    'entity_id': entity_id,
                    'scenario_id': scenario_id,
                    'period_type': period_type,
                    'end_of_period_value': end_of_period_value,
                    'reported_date_value': reported_date_value,
                    'exchange_rate_type': exchange_rate_type,
                    'request_id': request_id,
                    'standardized_data_id': standardized_data_id,
                    'value': value,
                    'value_string': value_string,
                    'value_numeric': value_numeric
                }
                
                results.append(new_record)
            # end for rec in periodic_data_records

    # LOGGER.info('results = {}'.format(results)) # COMMENT OUT
    return results


# Creates entity_path object with an Asset id_list (array of id's)
def create_entity_path(req_state, id_list):
    id_array = req_state.client.factory.create('ns3:ArrayOfint')
    for id_val in id_list:
        id_array.int.append(id_val)

    entity_path = req_state.client.factory.create('EntitiesPath')
    entity_path.Path = id_array

    return entity_path


def get_periods(req_state, start_dttm, end_dttm, period_type_name):
    period = req_state.client.factory.create('Period')
    period_types = req_state.client.factory.create('PeriodTypes')

    if period_type_name == 'FiscalYear':
        period.Type = period_types.FiscalYear
        period_diff = 1 + (end_dttm.year - start_dttm.year)
        if period_diff < 2:
            period_diff = 2

    if period_type_name == 'Year':
        period.Type = period_types.Year
        period_diff = 1 + (end_dttm.year - start_dttm.year)
        if period_diff < 2:
            period_diff = 2

    if period_type_name == 'FiscalQuarter':
        period.Type = period_types.FiscalQuarter
        period_diff = 1 + ((end_dttm.year - start_dttm.year) * 4) + ((end_dttm.month - start_dttm.month) // 3)
        if period_diff < 4:
            period_diff = 4

    if period_type_name == 'Quarter':
        period.Type = period_types.Quarter
        period_diff = 1 + ((end_dttm.year - start_dttm.year) * 4) + ((end_dttm.month - start_dttm.month) // 3)
        if period_diff < 4:
            period_diff = 4

    if period_type_name == 'L3M':
        period.Type = period_types.L3M
        period_diff = 1 + ((end_dttm.year - start_dttm.year) * 4) + ((end_dttm.month - start_dttm.month) // 3)
        if period_diff < 4:
            period_diff = 4

    if period_type_name == 'Month':
        period.Type = period_types.Month
        period_diff = 3 + ((end_dttm.year - start_dttm.year) * 12) + (end_dttm.month - start_dttm.month)
        if period_diff < 12:
            period_diff = 12

    return period, period_diff
