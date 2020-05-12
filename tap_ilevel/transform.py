import re
import pytz
from singer.utils import strptime_to_utc, strftime
from datetime import timezone, datetime, timedelta

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

    """
    if new_key=="last_modified_date":
        old_date = out[new_key]
        est_datetime = timezone.localize(datetime.datetime.strptime(
            old_date, "%Y-%m-%d %H:%M:%S"))
        utc_datetime = strftime(timezone.normalize(est_datetime).astimezone(
            pytz.utc))
        out[new_key] = utc_datetime
"""
    return out

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
    print(transformed_json)
    return transformed_json
