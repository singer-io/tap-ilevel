from datetime import datetime, timedelta
import dateutil.parser
import pytz
import time
from singer.utils import strptime_to_utc, strftime

"""
 Certain API calls have a limitation of 30 day periods, where the process might be launched 
 with an overall activity window of a greater period of time. Date ranges sorted into 30 
 day chunks in preparation for processing.

 Values provided for input dates are in format rerquired by SOAP API (yyyy-mm-dd)

 API calls are performed within a maximum 30 day timeframe, so breaking a period of time 
 between two into limited 'chunks' is required
"""
def get_date_chunks(start_date, end_date, max_days):
    td = timedelta(days=max_days)
    result = []

    days_dif = get_num_days_diff(start_date, end_date)
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
        else:
            next_date = next_date.strftime("%Y %m, %d")
            next_date = datetime.strptime(next_date, "%Y %m, %d")
            result.append(next_date)

    return result

def __convert_iso_8601_date(date_str):

    if isinstance(date_str, datetime):
        date_str = date_str.strftime("%Y-%m-%d")

    """Convert ISO 8601 formatted date string into time zone nieve"""
    cur_date_ref = dateutil.parser.parse(date_str)
    cur_date_ref = cur_date_ref.replace(tzinfo=None)
    return cur_date_ref

"""
 Provides ability to determine number of days between two given dates.
"""
def get_num_days_diff(start_date, end_date):
    return abs((start_date - end_date).days)

"""
 Date object returned from API call needs to be converted to format required by SingerIO 
"""
def est_to_utc_datetime(date_val):
    date_str = date_val.strftime("%Y-%m-%d %H:%M:%S")
    timezone = pytz.timezone('US/Eastern')
    est_datetime = timezone.localize(datetime.strptime(
        date_str, "%Y-%m-%d %H:%M:%S"))
    utc_datetime = strftime(timezone.normalize(est_datetime).astimezone(
        pytz.utc))
    return utc_datetime

def strip_record_ids(records):
	ids = []
	for record in records:
		ids.append(record.Id)
	return ids

def __est_to_utc_datetime(date_val):
    date_str = date_val.strftime("%Y-%m-%d %H:%M:%S")
    timezone = pytz.timezone('US/Eastern')
    est_datetime = timezone.localize(datetime.strptime(
        date_str, "%Y-%m-%d %H:%M:%S"))
    utc_datetime = strftime(timezone.normalize(est_datetime).astimezone(
        pytz.utc))
    return utc_datetime
