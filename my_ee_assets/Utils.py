
import logging
import datetime as dt
from time import sleep
import numpy as np
import subprocess
from osgeo import gdal, osr
import netCDF4
# Converting UTM to lon, lat
from pyproj import Proj

# FTP
from ftplib import FTP
# GOOGLE SHEETS
from __future__ import print_function
from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools

import ee

from config import ds_settings


def adjust_dates_dt(dataset, start_dt, end_dt):
    '''
    Sanity check on user dates
    Adjusts user start/end date to valid date ranges of dataset if
    user entered invalid dates
    :param dataset:
    :param start_dt: datetime, user start date
    :param end_dt: datetime, user end date
    :return:
    '''
    new_start_dt = start_dt
    new_end_dt = end_dt
    ds_start_dt = dt.datetime.strptime(ds_settings[dataset]['start_date'], "%Y-%m-%d")
    ds_end_dt = dt.datetime.strptime(ds_settings[dataset]['end_date'], "%Y-%m-%d")
    if start_dt < ds_start_dt:
        new_start_dt = ds_start_dt
    if ds_end_dt == 'present':
        if end_dt > dt.datetime.today():
            new_end_dt = dt.datetime.today()
    else:
        if end_dt > ds_end_dt:
            new_end_dt = ds_end_dt
    return new_start_dt, new_end_dt

def date_range(start_dt, end_dt, days=1, skip_leap_days=False):
    """Generate dates within a range (inclusive)

    Parameters
    ----------
    start_dt : datetime
        Start date.
    end_dt : datetime
        End date.
    days : int, optional
        Step size (the defaults is 1).
    skip_leap_days : bool, optional
        If True, skip leap days while incrementing (the default is False).

    Yields
    ------
    datetime

    """
    import copy
    curr_dt = copy.copy(start_dt)
    while curr_dt <= end_dt:
        if not skip_leap_days or curr_dt.month != 2 or curr_dt.day != 29:
            yield curr_dt
        curr_dt += dt.timedelta(days=days)

def get_ee_tasks(states=['RUNNING', 'READY']):
    """Return current active tasks

    Parameters
    ----------
    states : list

    Returns
    -------
    dict : Task descriptions (key) and task IDs (value).

    """

    logging.debug('  Active Tasks')
    tasks = {}
    for i in range(1,10):
        try:
            task_list = ee.data.getTaskList()
            task_list = sorted([
                [t['state'], t['description'], t['id']]
                for t in task_list if t['state'] in states])
            tasks = {t_desc: t_id for t_state, t_desc, t_id in task_list}
            break
        except Exception as e:
            logging.info(
                '  Error getting active task list, retrying ({}/10)\n'
                '  {}'.format(i, e))
            sleep(i ** 2)
    return tasks

def get_ee_assets(asset_id, shell_flag=False):
    """Return assets IDs in a collection

    Parameters
    ----------
    asset_id : str
        A folder or image collection ID.
    shell_flag : bool, optional
        If True, execute the command through the shell (the default is True).

    Returns
    -------
    list : Asset IDs

    """
    asset_id_list = []
    for i in range(1, 10):
        try:
            asset_id_list = subprocess.check_output(
                ['earthengine', 'ls', asset_id], universal_newlines=True,
                shell=shell_flag)
            asset_id_list = [x.strip() for x in asset_id_list.split('\n') if x]
            break
        except Exception as e:
            logging.error(
                '  Error getting asset list, retrying ({}/10)\n'
                '  {}'.format(i, e))
            sleep(i ** 2)
        except ValueError:
            logging.info('  Collection or folder doesn\'t exist')
            raise sys.exit()
    return asset_id_list

def dt_to_days_since_epoch(epoch, date_dt):
    '''
    Converts datetime to days since epoch
    epoch is of format %Y%m%d
    date_dt is datetime
    '''
    yr = int(epoch[0:4])
    m = int(epoch[4:6])
    d = int(epoch[6:8])
    days = (date_dt - dt.datetime(yr,m,d,0,0)).days
    return days

def array_to_geotiff(output_array, output_path, output_shape, output_geo,
                     output_proj, output_nodata=None):
    """
    Parameters
    ----------
    output_array : np.array
    output_path : str
        GeoTIFF file path.
    output_shape : tuple or list of ints
        Image shape (rows, cols).
    output_geo : tuple or list of floats
        Geo-transform (xmin, cs, 0, ymax, 0, -cs).
    output_proj : str
        Projection Well Known Text (WKT) string.
    output_nodata : float, optional
        GeoTIFF nodata value (the default is None).
    Returns
    -------
    None
    """
    output_driver = gdal.GetDriverByName('GTiff')
    output_rows, output_cols = output_shape
    output_ds = output_driver.Create(
        output_path, output_cols, output_rows, 1,
        gdal.GDT_Float32, ['COMPRESS=LZW', 'TILED=YES'])
    output_ds.SetProjection(output_proj)
    output_ds.SetGeoTransform(output_geo)
    output_band = output_ds.GetRasterBand(1)
    output_band.WriteArray(output_array)
    output_band.FlushCache()
    if output_nodata:
        output_band.SetNoDataValue(output_nodata)
    output_ds = None


def raster_to_array(input_raster, band=1):
    """Return a NumPy array from a raster
    Parameters
    ----------
    input_raster : str
        Filepath to the raster for array creation.
    band : int
        Band to convert to array in the input raster.
    Returns
    -------
    output_array: The array of the raster values
    """
    input_raster_ds = gdal.Open(input_raster, 0)
    input_band = input_raster_ds.GetRasterBand(band)
    # input_type = input_band.DataType
    input_nodata = input_band.GetNoDataValue()
    output_array = input_band.ReadAsArray(
        0, 0, input_raster_ds.RasterXSize, input_raster_ds.RasterYSize)
    # For float types, set nodata values to nan
    if (output_array.dtype == np.float32 or
            output_array.dtype == np.float64):
        if input_nodata is not None:
            output_array[output_array == input_nodata] = np.nan
    input_raster_ds = None
    return output_array


class DataUtil(object):
    '''
    Utility ro read data of different format
    '''
    def __init__(self, file_type, dataset, data_file_path, upload_dt):
        self.file_type = file_type
        self.ds_params = ds_settings[dataset]
        self.data_file_path = data_file_path
        self.upload_dt = upload_dt

        if self.file_type == 'netCDF':
            self.args = {
                'data_file_path': self.data_file_path,
                'upload_dt': self.upload_dt
            }


    def create_array_form_data(self):
        if self.file_type == 'netCDF':
            return self.read_netcdf(self.args)

    def read_netcdf(self, **kwargs):
        input_f = netCDF4.Dataset(kwargs['data_file_path'], 'r')
        data_var_name = input_f.variables.keys()[0]
        logging.debug(' Obtaining array_data for date/variable: ' + str(kwargs['upload_dt']) + '/' + data_var_name)
        # Find the time index
        num_days_since_epoch = Utils.dt_to_days_since_epoch(ds_params['epoch'], kwargs['upload_dt'])
        time = input_f.variables['time'][:]
        date_idx = (np.abs(time - num_days_since_epoch)).argmin()
        input_ma = input_f.variables[data_var_name][date_idx, :, :].copy()
        input_array = np.flipud(np.fliplr(input_ma.data.astype(np.float32)))
        input_nodata = float(input_ma.fill_value)
        input_array[input_array == input_nodata] = self.ds_params['asset_nodata']

        return input_array

class DownloadUtil(object):
    '''
    Util to download files via different methods:
        ftp, openDAP, ....
    '''
    def __init__(self, download_method, data_file_path, dataset):
        self.download_method = download_method
        self.data_file_path = data_file_path
        self.ds_params = ds_settings[dataset]


        # Set the inputs for the download method
        if self.download_method == 'ftp':
            self.args = {
                'ftp_url': self.ds_params['ftp_server'],
                'ftp_folder': self.ds_params['ftp_folder'],
                'data_file_path': self.data_file_path,
                'outpath': self.ds_params['outpath']
            }
        if self.download_method == 'gsheet':
            self.args = {
                'scopes': self.ds_params['scopes'],
                'sheet_id': self.ds_params['sheet_id'],
                'sheet_name': self.ds_params['data_sheet_name'],
                'col_range': self.ds_params['data_col_range']
            }

    def download(self):
        if self.download_method == 'ftp':
            self.ftp_download(**self.args)
        if self.download_method == 'gsheet':
            self.gsheet_download(**self.args)

    def gsheet_download(self, **kwargs):
        '''
        Download a google sheet
        :param kwargs:
            scopes: needed for authentication
            sheet_id: google sheet id (obtained from url)
            sheet_name:
            col_range: column range to be downloaded
        :return:
        '''
        store = file.Storage('ghseet_token.json')
        creds = store.get()
        if not creds or creds.invalid:
            flow = client.flow_from_clientsecrets('gsheet_credentials.json', kwargs['scopes'])
            creds = tools.run_flow(flow, store)
        service = build('sheets', 'v4', http=creds.authorize(Http()))
        col_range = kwargs['sheet_name'] + '!' + kwargs['col_range']
        result = service.spreadsheets().values().get(spreadsheetId=kwargs['sheet_id'],
                                                     range=range).execute()
        data = result.get('values', [])
        # FIX ME?? Save to file --> ? .nc
        return data

    def ftp_download(self, **kwargs):
        """
        Downloads one file from ftp server
        :param kwargs:
            site_url: ftp server
            site folder: data folder on ftp server
            file_name
            output_patth: local path to which file will be downloaded
        :return:
        """
        try:
            ftp = FTP()
            ftp.connect(kwargs['ftp_url'])
            ftp.login()
            ftp.cwd('{}'.format(kwargs['ftp_folder']))
            # Uncomment to view files in folder
            # ls = []
            # ftp.retrlines('MLSD', ls.append)
            # for entry in ls:
            #     print(entry)
            ftp.retrbinary('RETR %s' % kwargs['data_file_path'], open(kwargs['outpath'], 'wb').write)
            ftp.quit()
        except Exception as e:
            logging.info('  Unhandled exception: {}'.format(e))
            logging.info('  Removing file')
            try:
                ftp.quit()
            except:
                pass
            try:
                os.remove(output_path)
            except:
                pass
