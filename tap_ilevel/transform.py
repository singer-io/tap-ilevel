import re
import pytz
from singer.utils import strptime_to_utc, strftime
from datetime import timezone, datetime, timedelta
from singer.utils import strptime_to_utc, strftime
from pytz import timezone
import pytz
import time
from .iget_formula import IGetFormula
from .utils import est_to_utc_datetime
from .constants import ENTITY_DATE_FIELDS
import dateutil.parser
import singer

LOGGER = singer.get_logger()

# Convert camelCase to snake_case
def convert(name):
    regsub = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', regsub).lower()


# Convert keys in json array
def convert_array(arr):
    new_arr = []
    for i in arr:
        if isinstance(i, list):
            new_arr.append(convert_array(i))
        elif isinstance(i, dict):
            new_arr.append(convert_json(i))
        else:
            new_arr.append(i)
    return new_arr


# Convert keys in json
def convert_json(this_json):
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
    timezone = pytz.timezone('US/Eastern')
    est_datetime = timezone.localize(datetime.strptime(
        date_str, "%Y-%m-%d %H:%M:%S"))
    utc_datetime = strftime(timezone.normalize(est_datetime).astimezone(
        pytz.utc))
    return utc_datetime

"""
def est_to_utc_datetime(data_dict, data_key, datetime_fields):
    timezone = pytz.timezone('US/Eastern')
    new_dict = data_dict
    if datetime_fields:
        i = 0
        for record in data_dict[data_key]:
            for datetime_field in datetime_fields:
                est_datetime_val = record.get(datetime_field)
                if est_datetime_val:
                    if est_datetime_val == '0000-00-00 00:00:00':
                        utc_datetime = None
                    else:
                        try:
                            est_datetime = timezone.localize(datetime.datetime.strptime(
                                est_datetime_val, "%Y-%m-%d %H:%M:%S"))
                            utc_datetime = strftime(timezone.normalize(est_datetime).astimezone(
                                pytz.utc))
                        except ValueError as err:
                            LOGGER.warning('Value Error: {}'.format(err))
                            LOGGER.warning('Invalid Date: {}'.format(est_datetime_val))
                            LOGGER.warning('record: {}'.format(record))
                            utc_datetime = None
                    new_dict[data_key][i][datetime_field] = utc_datetime
            i = i + 1
    return new_dict
"""
def remove_custom_nodes(this_json):
    if not isinstance(this_json, (dict, list)):
        return this_json
    if isinstance(this_json, list):
        return [remove_custom_nodes(vv) for vv in this_json]
    return {kk: remove_custom_nodes(vv) for kk, vv in this_json.items() \
        if not kk[:1] == '_'}


# Convert custom fields and sets
# Generalize/Abstract custom fields to key/value pairs
def convert_custom_fields(this_json):
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


# Run all transforms: denests _embedded, removes _embedded/_links, and
#  converst camelCase to snake_case for fieldname keys.
def transform_json(this_json):
    transformed_json = convert_json(this_json)
    return transformed_json

"""
    Convert an object to a dictionary object, dates are converted as required.
"""
def obj_to_dict(obj):
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

        """
        if key in ENTITY_DATE_FIELDS:
            old_date = result[key]
            utc_date = est_to_utc_datetime(old_date)
            result[key] = utc_date
         """
        """
        if isinstance(result[key], (datetime, date)):
            old_date = result[key]
            utc_date = est_to_utc_datetime(old_date)
            result[key] = utc_date"""

    return result

def convert_ipush_event_to_obj(event):
    """Given an object returned from the SOAP API, convert into simplified object intended for publishing to Singer."""
    result = IGetFormula()

    if isinstance(event.Value, datetime):
        result.ValueString = convert_iso_8601_date(event.Value)
    elif isinstance(event.Value, int) or isinstance(event.Value, float):
        result.ValueNumeric = str(event.Value)
    else:
        result.ValueString = event.Value

    result.DataItemId = event.SDParameters.DataItemId
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

#def

def convert_iso_8601_date(date_str):

    if isinstance(date_str, datetime):
        date_str = date_str.strftime("%Y-%m-%d")

    """Convert ISO 8601 formatted date string into time zone nieve"""
    cur_date_ref = dateutil.parser.parse(date_str)
    cur_date_ref = cur_date_ref.replace(tzinfo=None)
    return cur_date_ref

def copy_i_get_result(source):
    result = IGetFormula()
    result.ValueString = source.ValueString

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