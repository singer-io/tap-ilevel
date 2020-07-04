import re
from datetime import datetime
import singer
from singer.utils import strftime
import pytz
import dateutil.parser
from .iget_formula import IGetFormula
LOGGER = singer.get_logger()

def convert(name):
    """ Convert camelCase to snake_case. """
    regsub = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', regsub).lower()

def convert_array(arr):
    """ Convert keys in json array """
    new_arr = []
    for i in arr:
        if isinstance(i, list):
            new_arr.append(convert_array(i))
        elif isinstance(i, dict):
            new_arr.append(convert_json(i))
        else:
            new_arr.append(i)
    return new_arr

def convert_json(this_json):
    """ Convert keys in json """
    out = {}
    for key in this_json:
        new_key = convert(key)
        if isinstance(this_json[key], dict):
            out[new_key] = convert_json(this_json[key])
        elif isinstance(this_json[key], list):
            out[new_key] = convert_array(this_json[key])
        else:
            out[new_key] = this_json[key]
    return out

def __est_to_utc_datetime(date_val):
    """Date object returned from API call needs to be converted to format required by SingerIO."""
    date_str = date_val.strftime("%Y-%m-%d %H:%M:%S")
    t_z = pytz.timezone('US/Eastern')
    est_datetime = t_z.localize(datetime.strptime(
        date_str, "%Y-%m-%d %H:%M:%S"))
    utc_datetime = strftime(t_z.normalize(est_datetime).astimezone(
        pytz.utc))
    return utc_datetime

def remove_custom_nodes(this_json):
    if not isinstance(this_json, (dict, list)):
        return this_json
    if isinstance(this_json, list):
        return [remove_custom_nodes(vv) for vv in this_json]
    return {kk: remove_custom_nodes(vv) for kk, vv in this_json.items() \
        if not kk[:1] == '_'}


def convert_custom_fields(this_json):
    """ Convert custom fields and sets Generalize/Abstract custom fields to key/value pairs """
    new_json = this_json
    i = 0
    for record in this_json:
        cust_field_sets = []
        for key in record:
            if isinstance(record[key], dict):
                if key[:1] == '_':
                    cust_field_set = {}
                    cust_field_set['customFieldSetId'] = key
                    cust_field_set_fields = []
                    for cf_key, cf_value in record[key].items():
                        field = {}
                        field['customFieldId'] = cf_key
                        field['customFieldValue'] = cf_value
                        cust_field_set_fields.append(field)
                    cust_field_set['customFieldValues'] = cust_field_set_fields
                    cust_field_sets.append(cust_field_set)
        new_json[i]['customFieldSets'] = cust_field_sets
        i = i + 1
    return new_json



def transform_json(this_json):
    """ Run all transforms: denests _embedded, removes _embedded/_links, and convert camelCase to
    snake_case for fieldname keys. """
    transformed_json = convert_json(this_json)
    return transformed_json


def obj_to_dict(obj):
    """ Convert an object to a dictionary object, dates are converted as required. """
    if not  hasattr(obj, "__dict__"):
        return obj
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

def convert_ipush_event_to_obj(event):
    """Given an object returned from the SOAP API, convert into simplified object intended for
    publishing to Singer."""
    result = IGetFormula()

    if event.Value is None:
        result.RawValue = "None"
    else:
        result.RawValue = event.Value
    if isinstance(event.Value, (float, int)):
        result.ValueNumeric = event.Value
    else:
        result.ValueString = str(event.Value)

    result.DataItemId = event.SDParameters.DataItemId

    if event.SDParameters.StandardizedDataId == 294814:
        LOGGER.info('hit: standardized data id: 294814 ',)

    result.ScenarioId = event.SDParameters.ScenarioId
    result.DataValueType = event.SDParameters.DataValueType
    result.StandardizedDataId = event.SDParameters.StandardizedDataId
    if "FormulaTypeIDsString" in event:
        result.FormulaTypeIDsString = event.SDParameters.FormulaTypeIDsString
    result.CurrencyCode = event.SDParameters.CurrencyCode

    #Period related
    result.PeriodIsOffset = event.SDParameters.Period.IsOffset
    result.PeriodQuantity = event.SDParameters.Period.Quantity
    result.PeriodType = event.SDParameters.Period.Type

    #Report date related
    result.ReportDateIsFiscal = event.SDParameters.ReportedDate.IsFiscal
    result.ReportDatePeriodsQuantity = event.SDParameters.ReportedDate.PeriodsQuantity
    result.ReportDateType = event.SDParameters.ReportedDate.Type
    result.ReportedDateValue = convert_iso_8601_date(event.SDParameters.ReportedDate.Value)

    #End of period related
    result.EndOfPeriodIsFiscal = event.SDParameters.EndOfPeriod.IsFiscal
    result.EndOfPeriodPeriodsQuantity = event.SDParameters.EndOfPeriod.PeriodsQuantity
    result.EndOfPeriodType = event.SDParameters.EndOfPeriod.Type
    result.EndOfPeriodValue = convert_iso_8601_date(event.SDParameters.EndOfPeriod.Value)

    return result

def convert_iso_8601_date(date_str):
    """Convert ISO 8601 formatted date string into time zone unaware"""
    if isinstance(date_str, datetime):
        date_str = date_str.strftime("%Y-%m-%d")

    cur_date_ref = dateutil.parser.parse(date_str)
    cur_date_ref = cur_date_ref.replace(tzinfo=None)
    return cur_date_ref

def copy_i_get_result(source):
    result = IGetFormula()

    result.RawValue = str(source.RawValue)

    if hasattr(source, "ValueString"):
        result.ValueString = source.ValueString
    else:
        result.ValueNumeric = source.ValueNumeric

    result.DataItemId = source.DataItemId
    result.ScenarioId = source.ScenarioId
    result.DataValueType = source.DataValueType
    result.StandardizedDataId = source.StandardizedDataId

    result.CurrencyCode = source.CurrencyCode

    # Period related
    result.PeriodIsOffset = source.PeriodIsOffset
    result.PeriodQuantity = source.PeriodQuantity
    result.PeriodType = source.PeriodType

    # Report date related
    result.ReportDateIsFiscal = source.ReportDateIsFiscal
    result.ReportDatePeriodsQuantity = source.ReportDatePeriodsQuantity
    result.ReportDateType = source.ReportDateType
    result.ReportedDateValue = source.ReportedDateValue

    # End of period related
    result.EndOfPeriodIsFiscal = source.EndOfPeriodIsFiscal
    result.EndOfPeriodPeriodsQuantity = source.EndOfPeriodPeriodsQuantity
    result.EndOfPeriodType = source.EndOfPeriodType
    result.EndOfPeriodValue = source.EndOfPeriodValue

    return result

def split_data_items(source):
    """References to assets stored in the the Data item entity as an array. This method will return
    a series of records, one per item in the array of asset refs; Basically we build a duplicate
    record per asset ref."""

    results = []

    if 'asset_i_ds_string' not in source:
        results.append(source)
        return results

    asset_ids = source['asset_i_ds_string'].split(",")
    if len(asset_ids) > 1:
        for asset_id in asset_ids:
            results.append(clone_data_item(source, asset_id))
    else:
        source['asset_id'] = int(source['asset_i_ds_string'])
        results.append(source)

    return results

def clone_data_item(source, asset_id):
    copy = {}
    copy['asset_id'] = int(asset_id)

    set_attr('conversion_type_id', source, copy)
    set_attr('data_value_type', source, copy)
    set_attr('enabled_capabilities_string', source, copy)
    set_attr('excel_name', source, copy)
    set_attr('id', source, copy)
    set_attr('is_monetary', source, copy)
    set_attr('is_putable', source, copy)
    set_attr('is_soft_deleted', source, copy)
    set_attr('name', source, copy)
    set_attr('object_type_id', source, copy)
    set_attr('scenario_i_ds_string', source, copy)

    return copy

def set_attr(attr_name, source, target):
    if attr_name in source:
        target[attr_name] = source[attr_name]
