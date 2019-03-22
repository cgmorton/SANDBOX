#!/usr/bin/python (python 3!!!)
import datetime as dt
from subprocess import Popen, PIPE
import urllib
import requests


def datetime_to_date_string(date_dt):
    return date_dt.strftime('%Y-%m-%d')

def date_string_to_datetime(date_string):
    try:
        return dt.datetime.strptime(date_string, '%Y-%m-%d')
    except Exception as e:
        raise Exception('\nInvalid date\n{}'.format(str(e)))

def datetime_to_millis(input_dt):
    """Convert a datetime to milliseconds since epoch (Jan 1st, 1970)"""
    try:
        return (input_dt - dt.datetime.utcfromtimestamp(0)) \
            .total_seconds() * 1000.0
    except Exception as e:
        raise Exception('\nInvalid datetime\n{}'.format(str(e)))

def millis_to_datetime(date_int):
    """Convert milliseconds since epoch to datetime"""
    try:
        return dt.datetime.fromtimestamp(float(date_int) / 1000.)
    except Exception as e:
        raise Exception('\nInvalid time (milliseconds)\n{}'.format(str(e)))

def date_string_to_millis(date_string):
    """Convert an ISO format date string to milliseconds since epoch"""
    try:
        return datetime_to_millis(
            dt.datetime.strptime(date_string, '%Y-%m-%d'))
    except Exception as e:
        raise Exception('\nInvalid date string\n{}'.format(str(e)))

def advance_days(num_days, date, date_type, f_or_b):
    """
    Adds num_days days to date.
    Args:
        num_days: int, number of days to be advanced
        date: input date to be advanced
        date_type: date type of input date,
                   this is also the type returned
            Acceptable date_types: date_string, millis, datetime
        f_or_b: forward or backward, direction of advancement
    Returns: date of type date_type
    """
    # Convert date to millis
    if date_type == 'datetime':
        date_dt = date
    elif date_type == 'date_string':
        date_dt = date_string_to_datetime(date)
    elif date_type == 'millis':
        date_dt = millis_to_datetime(date)
    else:
        raise Exception('Invalid date type: ' + str(date_type))
    if f_or_b == 'forward' or f_or_b == 'forwards':
        new_dt = date_dt + dt.timedelta(days=int(num_days))
    elif f_or_b == 'backward' or f_or_b == 'backwards':
        new_dt = date_dt - dt.timedelta(days=int(num_days))
    else:
        raise Exception('Can not advancve date, invalid option for f_or_b.')
    # Convert millis to date_type
    if date_type == 'datetime':
        new_date = new_dt
    elif date_type == 'date_string':
        new_date = datetime_to_date_string(new_dt)
    elif date_type == 'millis':
        new_date = date_string_to_millis(datetime_to_date_string(new_dt))
    return new_date


def make_API_request(base_url, download_location, url_params):
    params_tuple = list(url_params.items())
    url_str =  urllib.urlencode(params_tuple)
    url = base_url + '/?' + url_str
    # FIXME Using this url directly in browser works
    # but in this script I get requests.exceptions.MissingSchema: Invalid URL
    print(url)
    downloadURL = requests.get(url)['downloadURL']
    print(downloadURL)
    ''''
    resp = requests.get(downloadURL)
    print(resp)
    '''

    '''
    args = ['curl','-o %s' % download_location, url]
    # output = Popen(args, stdout=PIPE)
    Popen(args, shell=True)
    # return output
    '''


if __name__ == "__main__":
    base_url = "app.climateengine.org"
    download_location = "/Users/bdaudert/SANDBOX/CE/test.tif"
    # Set start/end date
    spi_months = 6
    end_date = datetime_to_date_string(dt.datetime.today())
    start_date = advance_days(spi_months * 365, end_date, 'date_string', 'backward')
    # set the parameters
    url_params = {
        "toolAction": "downloadRectangleSubset",
        "productType": "MET",
        "product": "G",
        "variable": "spi",
        "statistic": "",
        "calculation": "normprob",
        "dateStart": start_date,
        "dateEnd": end_date,
        "yearStartClim": "1981",
        "yearEndClim": "2010",
        "scale": 4000,
        "downloadFilename": "6m_spi_ce_api_download",
        "NELat": 40.6266,
        "NELong": -106.9043,
        "SWLat": 38.2277,
        "SWLong": -108.7598,
        "downloadMapFormat": "tif",
        "API_key": 1
    }
    make_API_request(base_url, download_location, url_params)
    # output = make_API_request(base_url, download_location, url_params)


