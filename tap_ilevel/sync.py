#!/usr/bin/env python3

from datetime import datetime, timedelta
import json
import copy

import singer
from singer import metrics, metadata, Transformer, utils
from singer.utils import strftime, strptime_to_utc

from tap_ilevel.transform import transform_json, hash_data
from tap_ilevel.streams import STREAMS
import tap_ilevel.singer_operations as singer_ops
import tap_ilevel.ilevel_api as ilevel

from tap_ilevel.constants import ALL_RECORDS_STREAMS, INCREMENTAL_STREAMS, \
    STANDARDIZED_PERIODIC_DATA_STREAMS, MAX_DATE_WINDOW, MAX_ID_CHUNK_SIZE

LOGGER = singer.get_logger()


def transform_datetime(this_dttm):
    with Transformer() as transformer:
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

    LOGGER.info('{}: Preparing to publish {} records'.format(
        stream_name, len(result_records)))

    stream = req_state.catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()
    stream_metadata = metadata.to_map(stream.metadata)

    # Transform records
    try:
        transformed_data = transform_json(result_records)
    except Exception as err:
        LOGGER.error(err)
        LOGGER.error('result_records = {}'.format(result_records))
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
                    LOGGER.error('Error record: {}'.format(record))
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

                # Lookback window, always process last 14 days
                last_dttm = datetime.strptime(last_date, "%Y-%m-%d") - timedelta(days=14)

                # Keep only records whose bookmark is on after the last_date
                if bookmark_dttm >= last_dttm:
                    singer_ops.write_record(
                        req_state.stream_name, transformed_record, utils.now())
                    counter.increment()
            else:
                singer_ops.write_record(
                    req_state.stream_name, transformed_record, utils.now())
                counter.increment()

        LOGGER.info('{}: Published {} records, max_bookmark_value: {}'.format(
            stream_name, counter.value, max_bookmark_value))
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

        LOGGER.info('{}: Processing date range {} of {} total ({} - {})'.format(
            req_state.stream_name, cur_date_range_index, cur_date_criteria_length, \
                cur_start_date, cur_end_date))

        #Retrieve updated entities for given date range, and send for processing
        updated_object_id_sets = ilevel.get_updated_object_id_sets(
            cur_start_date, cur_end_date, req_state.client, req_state.stream_name)

        update_bookmark = False
        if len(updated_object_id_sets) > 0:
            cur_id_set_index = 0
            for id_set in updated_object_id_sets:
                updated_record_count = 0
                LOGGER.info('{}: Processing id set {} of {} total sets'.format(
                    req_state.stream_name, cur_id_set_index + 1, len(updated_object_id_sets)))

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
                LOGGER.info('{}: Processing deleted id set {} of {} total sets'.format(
                    req_state.stream_name, cur_id_set_index + 1, len(deleted_object_id_sets)))
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

        LOGGER.info('periodic_data_standardized, {} - {}, Date Range: {} of {}'.format(
                cur_start_date, cur_end_date, cur_date_range_index, cur_date_criteria_length))

        #Get updated records based on date range
        updated_object_id_sets = ilevel.get_standardized_data_id_chunks(
            cur_start_date, cur_end_date, req_state.client)
        if len(updated_object_id_sets) == 0:
            continue

        LOGGER.info('periodic_data_standardized, {} - {}, Updated Sets: {}'.format(
                cur_start_date, cur_end_date, len(updated_object_id_sets)))

        #Translate standardized ids to objects
        batch = 1
        for id_set in updated_object_id_sets:
            processed_record_count = 0
            max_bookmark_value, processed_record_count = process_iget_batch_for_standardized_id_set(
                id_set, req_state, max_bookmark_value)

            LOGGER.info('periodic_data_standardized, {} - {}, Batch #{}, Requests: {}, Results: {}'.format(
                cur_start_date, cur_end_date, batch, len(id_set), processed_record_count))
            update_count = update_count + processed_record_count
            batch = batch + 1

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


def __process_periodic_data_calcs(req_state, scenario_name='Actual', currency_code='USD'):
    entity_types = ['assets'] # Currently: assets only (not funds)
    period_types = req_state.period_types.strip().replace(' ', '').split(',')
    batch_size = 10000
    start_dttm = datetime.strptime(req_state.last_date, '%Y-%m-%d')
    end_dttm = req_state.end_date
    max_bookmark_value = req_state.last_date
    
    # Init params_list and results
    i_get_params_list = req_state.client.factory.create('ArrayOfBaseRequestParameters')
    results = []
    req_id = 1
    batch = 1
    update_count = 0
    
    # Base objects
    data_value_types = req_state.client.factory.create('DataValueTypes')
    
    # scenario_id for scenario_name
    scenarios = req_state.client.service.GetScenarios()
    scenario = [i for i in scenarios.NamedEntity if i.Name == scenario_name][0]
    scenario_id = scenario.Id
    
    # current_date
    date_types = req_state.client.factory.create('DateTypes')
    current_date = req_state.client.factory.create('Date')
    current_date.Type = date_types.Current
    
    # latest_date
    latest_date = req_state.client.factory.create('Date')
    latest_date.Type = date_types.Latest
    
    # Get all calc data items
    data_item_search_criteria = req_state.client.factory.create('DataItemsSearchCriteria')
    data_item_search_criteria.GetGlobalDataItemsOnly = True # Global Data Items ONLY
    data_items = req_state.client.service.GetDataItems(data_item_search_criteria)
    calc_data_items = [i for i in data_items.DataItemObjectEx if i.FormulaTypeIDsString] # TESTING (add): and 'Gross Margin' in i.Name
    calc_data_items_len = len(calc_data_items)
    last_calc_data_item = calc_data_items[-1]
    
    # entity_type loop
    for entity_type in entity_types: # funds, assets
        LOGGER.info('entity_type = {}'.format(entity_type)) # COMMENT OUT
        # entity_ids for funds_or_assets
        if entity_type == 'funds':
            entities = req_state.client.service.GetFunds()
            entity_objs = entities.Fund
            # entity_objs = [i for i in entity_objs if 'IV, L.P.' in i.ExcelName] # COMMENT OUT
        else: # assets
            entities = req_state.client.service.GetAssets()
            entity_objs = entities.Asset
            # entity_objs = [i for i in entity_objs if 'Guild Education' in i.Name] # TESTING: COMMENT OUT
        entity_objs_len = len(entity_objs)
    
        # calc_data_items loop
        cdi = 1
        for data_item in calc_data_items:
            data_item_id = data_item.Id
            data_item_name = data_item.Name
            LOGGER.info('data_item_name = {} ({})'.format(data_item_name, data_item_id)) # COMMENT OUT

            # data_value_type for data_item
            data_value_type_id = data_item.DataValueType
            data_value_type = data_value_types[data_value_type_id]

            # entity loop
            ent = 1
            for entity in entity_objs:
                entity_dict = ilevel.sobject_to_dict(entity)
                entity_id = entity_dict.get('Id')
                entity_name = entity_dict.get('Name')
                # LOGGER.info('entity = {} ({})'.format(entity_name, entity_id)) # COMMENT OUT
                entity_initial_dttm = datetime.strptime(entity_dict.get('InitialPeriod')[:10], '%Y-%m-%d')
                max_dttm = [start_dttm, entity_initial_dttm]
                start_dttm = max(i for i in max_dttm if i is not None)

                # LOGGER.info('periodic_data_calculated: {}, {}: {} ({})'.format(
                #     data_item_name, entity_type, entity_name, entity_id)) # COMMENT OUT
                entity_path = ilevel.create_entity_path(req_state, [entity_id])

                # period_type loop
                last_period_type = period_types[-1]
                for period_type in period_types:
                    period, period_diff = ilevel.get_periods(req_state, start_dttm, end_dttm, period_type)

                    # offset_period loop (0, -1, -2, ...) look-back
                    pd = 0
                    while pd <=  period_diff + 1:
                        # LOGGER.info('{}: periodic_data_calculated: {}, Period Type: {}, Offset: {}'.format(
                        #    req_id, data_item_name, period_type, -pd)) # COMMENT OUT
                        offset_period = copy.copy(period)
                        offset_period.IsOffset = True
                        offset_period.Quantity = int(-1 * pd)

                        i_get_params = req_state.client.factory.create('AssetAndFundGetRequestParameters')
                        i_get_params.RequestIdentifier = req_id
                        i_get_params.DataValueType = data_value_type
                        i_get_params.EntitiesPath = entity_path
                        i_get_params.DataItemId = data_item_id
                        i_get_params.ScenarioId = scenario_id
                        i_get_params.Period = period
                        i_get_params.Offset = offset_period
                        i_get_params.EndOfPeriod = latest_date
                        i_get_params.ReportedDate = current_date
                        i_get_params.CurrencyCode = currency_code

                        i_get_params_list.BaseRequestParameters.append(i_get_params)
                        # LOGGER.info('i_get_params = {}'.format(i_get_params)) # COMMENT OUT

                        # run iGetBatch
                        end_of_batches = False
                        if (pd == (period_diff + 1) and period_type == last_period_type \
                            and ent == entity_objs_len and cdi == calc_data_items_len and entity_type == 'assets'):
                            end_of_batches = True
                            LOGGER.info('xxx END OF BATCHES xxx')
                        if (req_id % batch_size == 0) or end_of_batches:
                            LOGGER.info('xxx BATCH: {} xxx'.format(batch))
                            i_get_count = len(i_get_params_list)
                            i_get_request = req_state.client.factory.create('DataServiceRequest')
                            i_get_request.IncludeStandardizedDataInfo = True
                            i_get_request.IncludeExcelFormula = True
                            i_get_request.ParametersList = i_get_params_list
                            # LOGGER.info('i_get_request = {}'.format(i_get_request)) # COMMENT OUT

                            # pylint: disable=unused-variable
                            metrics_string = ('periodic_data_calculated, iGetBatch #{}: {} requests'.format(
                                batch, i_get_count))
                            with metrics.http_request_timer(metrics_string) as timer:
                                data_values = req_state.client.service.iGetBatch(i_get_request)

                            # LOGGER.info('data_values = {}'.format(data_values)) # COMMENT OUT

                            if isinstance(data_values, str):
                                continue

                            try:
                                periodic_data_records = data_values.DataValue
                            except Exception as err:
                                LOGGER.error('{}'.format(err))
                                LOGGER.error('data_values dict = {}'.format(ilevel.sobject_to_dict(data_values)))
                                raise err

                            for periodic_data_record in periodic_data_records:
                                if "Error" in periodic_data_record:
                                    continue

                                if "NoDataAvailable" in periodic_data_record:
                                    continue
                                
                                periodic_data_record_dict = ilevel.sobject_to_dict(periodic_data_record)
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
                                    entity_id = next(iter(sd_parameters.get('entities_path', {}).get('path', {}).get('int', [])), None)
                                    scenario_id = sd_parameters.get('scenario_id')
                                    period_type = sd_parameters.get('period', {}).get('type')
                                    end_of_period_value = sd_parameters.get('end_of_period', {}).get('value')
                                    reported_date_value = sd_parameters.get('reported_date', {}).get('value')
                                    exchange_rate_type = sd_parameters.get('exchange_rate', {}).get('type')
                                    request_id = sd_parameters.get('request_identifier')
                                    standardized_data_id = sd_parameters.get('standardized_data_id')
                                    
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


                                    # Primary key dimensions, create md5 hash key
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
                                # end for rec in period_data_records

                            # Process batch records
                            max_bookmark_value, process_record_count = process_records(
                                result_records=results,
                                req_state=req_state,
                                deletion_flag=False,
                                max_bookmark_value=max_bookmark_value)

                            update_count = update_count + process_record_count
                            
                            # Init new params_list and results
                            i_get_params_list = req_state.client.factory.create('ArrayOfBaseRequestParameters')
                            results = []

                            batch = batch + 1
                            # end iGetBatch

                        req_id = req_id + 1
                        pd = pd + 1
                        # end offset_period loop

                    # end period_type loop

                ent = ent + 1
                # end entity_id loop
                
            cdi = cdi + 1
            # end calc_data_items loop

        # end entity_type loop

    # Update the state with the max_bookmark_value for the stream after ALL records
    # Always process past year of calculated data (Subtract 365 days from max_bookmark_value)
    max_bookmark_dttm = datetime.strptime(max_bookmark_value[:10], "%Y-%m-%d") - timedelta(days=365)
    max_bookmark_value = max_bookmark_dttm.strftime("%Y-%m-%d")
    singer_ops.write_bookmark(req_state.state, req_state.stream_name, max_bookmark_value)

    return update_count


# Sync a specific endpoint (stream).
def __sync_endpoint(req_state):
    # Top level variables
    endpoint_total = 0

    with metrics.job_timer('endpoint_duration'):

        LOGGER.info('{}: STARTED Syncing stream'.format(req_state.stream_name))
        singer_ops.update_currently_syncing(req_state.state, req_state.stream_name)

        # Publish schema to singer
        singer_ops.write_schema(req_state.catalog, req_state.stream_name)
        LOGGER.info('{}: Processing date window, {} to {}'.format(
            req_state.stream_name, req_state.last_date, req_state.end_date))

        if req_state.stream_name in ALL_RECORDS_STREAMS:
            endpoint_total = __process_all_records_data_stream(req_state)

        elif req_state.stream_name == 'periodic_data_standardized':
            endpoint_total = __process_standardized_data_stream(req_state)

        elif req_state.stream_name == 'periodic_data_calculated':
            endpoint_total = __process_periodic_data_calcs(req_state)

        else:
            # data_items, investment_transactions
            endpoint_total = __process_incremental_stream(req_state)

        singer_ops.update_currently_syncing(req_state.state, None)
        LOGGER.info('{}: FINISHED Syncing Stream, total_records: {}'.format(
            req_state.stream_name, endpoint_total))

    LOGGER.info('sync.py: sync complete')

    return endpoint_total


# Main routine: orchestrates pulling data for selected streams.
def sync(client, config, catalog, state):
    start_date = config.get('start_date')[:10]
    period_types = config.get('period_types', 'FiscalQuarter')

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
                period_types=period_types,
                stream=stream,
                catalog=catalog)

            # Main sync routine
            total_records = __sync_endpoint(req_state)

            LOGGER.info('FINISHED Syncing: {}, total_records: {}'.format(
                stream_name,
                total_records))

    LOGGER.info('sync.py: sync complete')
