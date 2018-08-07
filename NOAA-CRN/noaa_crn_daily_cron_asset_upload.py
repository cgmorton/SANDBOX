#--------------------------------
# Name:    daily_cron_asset_upload.py
# Purpose: Ingest dataset into Earth Engine
# Notes:   Data is downloaded to local dir
#          Data is converted to .tif
#          .tif data is stored in
#          Bucket of steel-melody-531 project
#          .tif data is ingested into EE
#--------------------------------

import argparse
# from builtins import input
import datetime
from ftplib import FTP
import netCDF4
import logging
import os
import re
import shutil
import subprocess
import sys
from time import sleep

import ee
import numpy as np
from osgeo import gdal, osr

from config import ds_settings

def main(workspace, start_dt, end_dt, variables, overwrite_flag=False,
         cron_flag=False, composite_flag=True, upload_flag=True,
         ingest_flag=True):
    """Ingest NOAA_CRN data into Earth Engine

    Parameters
    ----------
    workspace : str
        Root folder of dataset data.
    start_dt : datetime
        Start date.
    end_dt : datetime
        End date.
    variables : list
        Variables to process.  Choices depend on dataset.
    overwrite_flag : bool
        If True, overwrite existing files (the default is False).
    cron_flag : bool
        If True, remove any previous intermediate files (the default is False).
    composite_flag : bool, optional
        If True, build multi-band composite images (the default is True).
    upload_flag : bool, optional
        If True, upload images to Cloud Storage bucket (the default is True).
    ingest_flag : bool, optional
        If True, ingest images into Earth Engine (the default is True).

    -------
    python noaa_crn_daily_cron_asset_upload.py --workspace /Volumes/DHS/NOAA_CRN -s 2006-01-01 -e 2006-01-03 -v 'AWC' --no-upload --no-ingest --debug

    Returns
    -------
    None

    Notes
    -----

    """
    # Get the default parameters for the dataset from the config file
    dataset = 'NOAA_CRN'
    ds_params = ds_settings[dataset]

    logging.info('\nIngest ' + dataset + ' data into Earth Engine')

    # infile_re = re.compile('NOAA_CRN_(?P<date>\d{8})')
    infile_fmt = ds_params['infile_fmt']
    infile_dt_fmt = ds_params['infile_dt_fmt']
    dat_nodata = ds_params['dat_nodata']

    # Bucket parameters
    # project_name = 'steel-melody-531'
    bucket_name = ds_params['bucket_name']
    bucket_folder = ds_params['bucket_folder']

    # Asset parameters
    asset_coll = ds_params['asset_coll']
    asset_id_fmt = ds_params['asset_id_fmt']
    asset_dt_fmt = ds_params['asset_dt_fmt']
    asset_id_re = re.compile('{}/(?P<date>\d{{8}})'.format(asset_coll))

    asset_geo = ds_params['asset_geo']
    asset_osr = osr.SpatialReference()
    asset_osr.ImportFromEPSG(4326)
    asset_proj = asset_osr.ExportToWkt()

    asset_nodata = ds_params['asset_nodata']
    asset_shape = ds_params['asset_shape']

    site_url = ds_params['ftp_server']
    site_folder = ds_params['ftp_folder']

    band_name = ds_params['bands']

    if os.name == 'posix':
        shell_flag = False
    else:
        shell_flag = True

    # No data before 2006-01-01 and after 2016-12-31
    epoch = '20060101'
    y = int(epoch[0:4])
    m = int(epoch[4:6])
    d = int(epoch[6:8])
    if start_dt < datetime.datetime(y, m, d):
        start_dt = datetime.datetime(y, m, d)
        logging.info('Adjusting start date to: {}'.format(
            start_dt.strftime('%Y-%m-%d')))
    if end_dt > datetime.datetime(2016, 12, 31):
        end_dt = datetime.datetime(2016, 12, 31)
        logging.info('Adjusting end date to:   {}\n'.format(
            end_dt.strftime('%Y-%m-%d')))

    ee.Initialize()

    # Remove files from previous runs
    infile_ws = os.path.join(workspace, ds_params['infile_ext'])
    if cron_flag and os.path.isdir(infile_ws):
        shutil.rmtree(infile_ws)
    if not os.path.isdir(infile_ws):
        os.makedirs(infile_ws)

    # Each variable will be written to a separate collection
    logging.debug('Image Collection: {}'.format(asset_coll))

    # Start with a list of dates to check
    logging.debug('\nBulding Date List')
    # test_dt_list = [
    #     test_dt for test_dt in date_range(start_dt, end_dt)
    #     if start_dt <= test_dt <= end_dt]
    test_dt_list = list(date_range(start_dt, end_dt))
    if not test_dt_list:
        logging.info('  No test dates, exiting')
        return True
    logging.debug('\nTest dates: {}'.format(
        ', '.join(map(lambda x: x.strftime('%Y-%m-%d'), test_dt_list))))

    # Check if any of the needed dates are currently being ingested
    # Check task list before checking asset list in case a task switches
    #   from running to done before the asset list is retrieved.
    task_id_list = [
        desc.replace('\nAsset ingestion: ', '')
        for desc in get_ee_tasks(states=['RUNNING', 'READY']).keys()]
    task_dt_list = [
        datetime.datetime.strptime(match.group('date'), asset_dt_fmt)
        for asset_id in task_id_list
        for match in [asset_id_re.search(asset_id)] if match]

    # Switch date list to be dates that are missing
    test_dt_list = [
        dt for dt in test_dt_list if dt not in task_dt_list or overwrite_flag]
    if not test_dt_list:
        logging.info('  No missing asset dates, exiting')
        return True
    else:
        logging.debug('\nMissing asset dates: {}'.format(', '.join(
            map(lambda x: x.strftime('%Y-%m-%d'), test_dt_list))))

    # Check if the assets already exist
    # For now, assume the collection exists
    asset_id_list = get_ee_assets(asset_coll, shell_flag)
    asset_dt_list = [
        datetime.datetime.strptime(match.group('date'), asset_dt_fmt)
        for asset_id in asset_id_list
        for match in [asset_id_re.search(asset_id)] if match]

    # Switch date list to be dates that are missing
    test_dt_list = [
        dt for dt in test_dt_list if dt not in asset_dt_list or overwrite_flag]
    if not test_dt_list:
        logging.info('  No missing asset dates, exiting')
        return True
    else:
        logging.debug('\nMissing asset dates: {}'.format(', '.join(
            map(lambda x: x.strftime('%Y-%m-%d'), test_dt_list))))

    logging.info('\nProcessing dates')
    for upload_dt in sorted(test_dt_list, reverse=True):
        logging.info('{}'.format(upload_dt.date()))

        year_ws = os.path.join(workspace, upload_dt.strftime('%Y'))
        date_ws = os.path.join(year_ws, upload_dt.strftime('%Y%m%d'))

        upload_path = os.path.join(
            year_ws, upload_dt.strftime(asset_dt_fmt) + '.tif')
        bucket_path = '{}/{}/{}'.format(
            bucket_name, bucket_folder,
            upload_dt.strftime(asset_dt_fmt) + '.tif')
        asset_id = '{}/{}'.format(
            asset_coll,
            asset_id_fmt.format(date=upload_dt.strftime(asset_dt_fmt)))
        logging.debug('  {}'.format(upload_path))
        logging.debug('  {}'.format(bucket_path))
        logging.debug('  {}'.format(asset_id))

        # The overwrite_flag check may be redundant
        if overwrite_flag and upload_dt in asset_dt_list:
            logging.info('  Removing existing asset')
            try:
                subprocess.check_output(
                    ['earthengine', 'rm', asset_id], shell=shell_flag)
            except Exception as e:
                logging.exception('  Exception: {}'.format(e))
        # if upload_dt in task_dt_list:
        #     # Eventually stop the export task

        # Always overwrite composite if asset doesn't exist
        # if overwrite_flag and os.path.isfile(upload_path):
        if os.path.isfile(upload_path):
            logging.debug('  Removing existing composite GeoTIFF')
            os.remove(upload_path)
        if overwrite_flag and os.path.isdir(date_ws):
            shutil.rmtree(date_ws)
        if not os.path.isdir(date_ws):
            os.makedirs(date_ws)

        logging.debug('  Downloading component images')
        for variable in variables:
            # Each variable has it's own netCDF file containing all dates
            infile_file = infile_fmt.format(var_name=variable)
            # Save all netCDF file in nc dir
            ncfile_path = os.path.join(
                infile_ws, infile_file)
            outfile_path = os.path.join(
                infile_ws, upload_dt.strftime('%Y'), upload_dt.strftime('%m_%b'),
                infile_file)
            logging.debug('  {}'.format(ncfile_path))

            # if overwrite_flag and os.path.isfile(outfile_path):
            #     logging.debug('  Removing netdcf file')
            #     os.remove(ncfile_path)
            if not os.path.isdir(os.path.dirname(ncfile_path)):
                os.makedirs(os.path.dirname(ncfile_path))

            tif_path = os.path.join(date_ws, '{}.tif'.format(variable))
            logging.debug('  {}'.format(tif_path))

            # Remove TIFs but leave netcdf files
            if overwrite_flag and os.path.isfile(tif_path):
                logging.debug('  Removing TIF file')
                os.remove(tif_path)
            elif not overwrite_flag and os.path.isfile(tif_path):
                logging.debug('  TIF file exists, skipping')
                continue

            if not os.path.isfile(ncfile_path) and not os.path.isfile(tif_path):
                logging.debug('  Downloading the file that contains the date')
                ftp_download(
                    site_url,
                    site_folder,
                    infile_file, ncfile_path)

            if os.path.isfile(ncfile_path) and not os.path.isfile(tif_path):
                # Need to read netcdf file
                logging.debug('  Extracting data for date from nc file')
                input_nc_f = netCDF4.Dataset(ncfile_path, 'r')
                nc_var_name = input_nc_f.variables.keys()[0]
                logging.debug(' Obtaining array_data for date/variable: ' + str(upload_dt) + '/' + nc_var_name)
                # Find the time index
                num_days_since_epoch = dt_to_days_since_epoch(epoch, upload_dt)
                time = input_nc_f.variables['time'][:]
                date_idx = (np.abs(time - num_days_since_epoch)).argmin()
                input_ma = input_nc_f.variables[nc_var_name][date_idx,:,:].copy()
                input_array = np.flipud(np.fliplr(input_ma.data.astype(np.float32)))
                input_nodata = float(input_ma.fill_value)
                input_array[input_array == input_nodata] = asset_nodata
                array_to_geotiff(
                    input_array, tif_path, output_shape=asset_shape,
                    output_geo=asset_geo, output_proj=asset_proj,
                    output_nodata=asset_nodata)
                del input_array

        # Build composite image
        # We could also write the arrays directly to the composite image above
        # if composite_flag and not os.path.isfile(upload_path):
        if composite_flag:
            logging.debug('  Building composite image')
            # Only build the composite if all the input images are available
            input_vars = set(
                [os.path.splitext(f)[0] for f in os.listdir(date_ws)])
            if set(variables).issubset(input_vars):
                # Force output files to be 32-bit float GeoTIFFs
                output_driver = gdal.GetDriverByName('GTiff')
                output_rows, output_cols = asset_shape
                output_ds = output_driver.Create(
                    upload_path, output_cols, output_rows, len(variables),
                    gdal.GDT_Float32, ['COMPRESS=LZW', 'TILED=YES'])
                output_ds.SetProjection(asset_proj)
                output_ds.SetGeoTransform(asset_geo)
                for band_i, variable in enumerate(variables):
                    data_array = raster_to_array(
                        os.path.join(date_ws, '{}.tif'.format(variable)))
                    data_array[np.isnan(data_array)] = -9999
                    output_band = output_ds.GetRasterBand(band_i + 1)
                    output_band.WriteArray(data_array)
                    output_band.FlushCache()
                    output_band.SetNoDataValue(-9999)
                    del data_array
                output_ds = None
                del output_ds
            else:
                logging.warning(
                    '  Missing input images for composite\n  '
                    '  {}'.format(
                        ', '.join(list(set(variables) - input_vars))))

        # DEADBEEF - Having this check here makes it impossible to only ingest
        # assets that are already in the bucket.  Moving the file check to the
        # conditionals below doesn't work though because then the ingest call
        # can be made even if the file was never made.
        if not os.path.isfile(upload_path):
            continue

        if upload_flag:
            logging.info('  Uploading to bucket')
            args = ['gsutil', 'cp', upload_path, bucket_path]
            print(upload_path)
            print(bucket_path)
            if not logging.getLogger().isEnabledFor(logging.DEBUG):
                args.insert(1, '-q')
            try:
                subprocess.check_output(args, shell=shell_flag)
                os.remove(upload_path)
            except Exception as e:
                logging.exception(
                    '    Exception: {}\n    Skipping date'.format(e))
                continue

        if ingest_flag:
            logging.info('  Ingesting into Earth Engine')
            # DEADBEEF - For now, assume the file is in the bucket
            try:
                subprocess.check_output(
                    [
                        'earthengine', 'upload', 'image',
                        '--bands', ','.join(band_name[v] for v in variables),
                        '--asset_id', asset_id,
                        '--time_start', upload_dt.date().isoformat(),
                        # '--nodata_value', nodata_value,
                        '--property', '(string)DATE_INGESTED={}'.format(
                            datetime.datetime.today().strftime('%Y-%m-%d')),
                        bucket_path
                    ], shell=shell_flag)
                sleep(1)
            except Exception as e:
                logging.exception('    Exception: {}'.format(e))

        # Removing individual GeoTIFFs in cron mode
        if cron_flag and os.path.isdir(date_ws):
            shutil.rmtree(date_ws)

def dt_to_days_since_epoch(epoch, date_dt):
    '''
    Converts datetime to days since epoch
    epoch is of format %Y%m%d
    date_dt is datetime
    '''
    yr = int(epoch[0:4])
    m = int(epoch[4:6])
    d = int(epoch[6:8])
    days = (date_dt - datetime.datetime(yr,m,d,0,0)).days
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


def date_range(start_dt, end_dt, days=1, skip_leap_days=False):
    """Generate dates within a range (inclusive)

    Parameters
    ----------
    start_dt : datetime
        Start date.
    end_dt : datetime
        End date.
    days : int, optional
        Step size. Defaults to 1.
    skip_leap_days : bool, optional
        If True, skip leap days while incrementing.
        Defaults to True.

    Yields
    ------
    datetime

    """
    import copy
    curr_dt = copy.copy(start_dt)
    while curr_dt <= end_dt:
        if not skip_leap_days or curr_dt.month != 2 or curr_dt.day != 29:
            yield curr_dt
        curr_dt += datetime.timedelta(days=days)


def ftp_download(site_url, site_folder, file_name, output_path):
    """"""
    try:
        ftp = FTP()
        ftp.connect(site_url)
        ftp.login()
        ftp.cwd('{}'.format(site_folder))
        # Uncomment to view files in folder
        # ls = []
        # ftp.retrlines('MLSD', ls.append)
        # for entry in ls:
        #     print(entry)
        ftp.retrbinary('RETR %s' % file_name, open(output_path, 'wb').write)
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


def get_bucket_files(project_name, bucket_name, shell_flag=False):
    """Return Google Cloud Storage buckets associated with project

    Parameters
    ----------
    project_name : str
        AppEngine project name.
    bucket_name : str
        Google Storage bucket name.
    shell_flag : bool, optional
        If True, execute the command through the shell (the default is True).

    Returns
    -------
    list : File names

    """
    try:
        file_list = subprocess.check_output(
            ['gsutil', 'ls', '-r', '-p', project_name, bucket_name],
            universal_newlines=True, shell=shell_flag)
    except Exception as e:
        logging.error(
            '\nERROR: There was a problem getting the bucket file list ' +
            'using gsutil, exiting')
        logging.error('  Exception: {}'.format(e))
        sys.exit()
    return file_list


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
            time.sleep(i ** 2)
        except ValueError:
            logging.info('  Collection or folder doesn\'t exist')
            raise sys.exit()
    return asset_id_list


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


def valid_date(input_date):
    """Check that a date string is ISO format (YYYY-MM-DD)

    This function is used to check the format of dates entered as command
      line arguments.
    DEADBEEF - It would probably make more sense to have this function
      parse the date using dateutil parser (http://labix.org/python-dateutil)
      and return the ISO format string

    Parameters
    ----------
    input_date : string

    Returns
    -------
    datetime

    Raises
    ------
    ArgParse ArgumentTypeError

    """
    try:
        return datetime.datetime.strptime(input_date, "%Y-%m-%d")
    except ValueError:
        msg = "Not a valid date: '{}'.".format(input_date)
        raise argparse.ArgumentTypeError(msg)


def arg_parse():
    """"""
    end_dt = datetime.datetime.today()

    parser = argparse.ArgumentParser(
        description='Ingest NOAA_CRN daily data into Earth Engine',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--workspace', metavar='PATH',
        default=os.path.dirname(os.path.abspath(__file__)),
        help='Set the current working directory')
    parser.add_argument(
        '-s', '--start', type=valid_date, metavar='DATE',
        default=(end_dt - datetime.timedelta(days=365)).strftime('%Y-%m-%d'),
        help='Start date (format YYYY-MM-DD)')
    parser.add_argument(
        '-e', '--end', type=valid_date, metavar='DATE',
        default=end_dt.strftime('%Y-%m-%d'),
        help='End date (format YYYY-MM-DD)')
    parser.add_argument(
        '-v', '--variables', nargs='+',
        default=['AWC', 'Fract_awc', 'Air_temp',
                'Sur_temp_max', 'Sur_temp_min', 'Surface_temp',
                'Temp_max', 'Temp_min', 'Precip',
                'Smois_05cm', 'Smois_10cm', 'Smois_20cm',
                'Smois_50cm', 'Smois_100cm',
                'Soiltemp_05cm', 'Soiltemp_10cm', 'Soiltemp_20cm',
                'Soiltemp_50cm', 'Soiltemp_100cm',
                'Solar', 'Wind', 'rh'],
        choices=['AWC', 'Fract_awc', 'Air_temp',
                'Sur_temp_max', 'Sur_temp_min', 'Surface_temp',
                'Temp_max', 'Temp_min', 'Precip',
                'Smois_05cm', 'Smois_10cm', 'Smois_20cm',
                'Smois_50cm', 'Smois_100cm',
                'Soiltemp_05cm', 'Soiltemp_10cm', 'Soiltemp_20cm',
                'Soiltemp_50cm', 'Soiltemp_100cm',
                'Solar', 'Wind', 'rh'],
        metavar='VAR',
        help='NOAA_CRN daily variables')
    parser.add_argument(
        '-o', '--overwrite', default=False, action='store_true',
        help='Force overwrite of existing files')
    parser.add_argument(
        '--cron', default=False, action='store_true',
        help='Remove previous intermediate files')
    # The default values shows up as True for these which is confusing
    parser.add_argument(
        '--no-composite', action='store_false', dest='composite',
        help='Don\'t build multi-band composites images')
    parser.add_argument(
        '--no-upload', action='store_false', dest='upload',
        help='Don\'t upload images to cloud stroage bucket')
    parser.add_argument(
        '--no-ingest', action='store_false', dest='ingest',
        help='Don\'t ingest images into Earth Engine')
    parser.add_argument(
        '-d', '--debug', default=logging.INFO, const=logging.DEBUG,
        help='Debug level logging', action='store_const', dest='loglevel')
    args = parser.parse_args()

    # Convert relative paths to absolute paths
    if args.workspace and os.path.isdir(os.path.abspath(args.workspace)):
        args.workspace = os.path.abspath(args.workspace)
    return args


if __name__ == '__main__':
    args = arg_parse()
    logging.basicConfig(level=args.loglevel, format='%(message)s')
    main(workspace=args.workspace, start_dt=args.start, end_dt=args.end,
         variables=args.variables, overwrite_flag=args.overwrite,
         cron_flag=args.cron, composite_flag=args.composite,
         upload_flag=args.upload, ingest_flag=args.ingest)
