#--------------------------------
# Name:    attempt_general_daily_cron_asset_upload.py
# Purpose: Download data from ftp server to local dir
#          Convert data to .tif
#          Store data in bucket
# Notes:
#          Data is downloaded to local dir
#          Data is converted to .tif
#          .tif data is stored in
#          Bucket of steel-melody-531 project
#          .tif data is ingested into EE
#--------------------------------

import argparse
#from builtins import input
import datetime as dt
import logging
import os
import re
import shutil
import subprocess
from time import sleep

import ee
import numpy as np
from osgeo import gdal, osr

import Utils



from config import ds_settings

def main(dataset, workspace, start_dt, end_dt, variables, overwrite_flag=False,
         cron_flag=False, composite_flag=True, upload_flag=True,
         ingest_flag=True):
    """
    Parameters
    ----------
    dataset: str
        dataset to be downloaded, see README.md for list of datasets
    workspace : str
        Root folder of data.
    start_dt : datetime
        Start date.
    end_dt : datetime
        End date (Inclusive).
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

    Returns
    -------
    None

    Notes
    -----
    """
    ee.Initialize()

    # Get the default parameters for the dataset from the config file
    ds_params = ds_settings[dataset]
    # FIX ME: CHECK THAT THIS IS OK
    # variables = list(ds_params['bands'].values())
    asset_osr = osr.SpatialReference()
    asset_osr.ImportFromEPSG(ds_params['asset_proj'])
    asset_proj = asset_osr.ExportToWkt()

    # Set the shell flag: What does this do?
    if os.name == 'posix':
        shell_flag = False
    else:
        shell_flag = True

    # Sanity check on user start/end dates
    start_dt, end_dt = Utils.adjust_dates_dt(dataset, start_dt, end_dt)

    # Remove files from previous runs
    infile_ws = os.path.join(workspace, ds_params['infile_ext'])
    if cron_flag and os.path.isdir(infile_ws):
        shutil.rmtree(infile_ws)
    if not os.path.isdir(infile_ws):
        os.makedirs(infile_ws)

    # Each variable will be written to a separate collection
    logging.debug('Image Collection: {}'.format(ds_params['asset_coll']))

    # Start with a list of dates to check
    logging.debug('\nBuilding Date List')
    # test_dt_list = [
    #     test_dt for test_dt in date_range(start_dt, end_dt)
    #     if start_dt <= test_dt <= end_dt]
    test_dt_list = list(Utils.date_range(start_dt, end_dt))
    if not test_dt_list:
        logging.info('  No test dates, exiting')
        return True

    '''
    logging.debug('\nTest dates: {}'.format(
        ', '.join(map(lambda x: x.strftime('%Y-%m-%d'), test_dt_list))))
    '''
    # Check if any of the needed dates are currently being ingested
    # Check task list before checking asset list in case a task switches
    #   from running to done before the asset list is retrieved.
    task_id_list = [
        desc.replace('\nAsset ingestion: ', '')
        for desc in Utils.get_ee_tasks(states=['RUNNING', 'READY']).keys()]

    asset_id_re = re.compile('{}/(?P<date>\d{{8}})'.format(ds_params['asset_coll']))

    task_dt_list = [
        dt.datetime.strptime(match.group('date'), ds_params['asset_dt_fmt'])
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
    asset_id_list = Utils.get_ee_assets(ds_params['asset_coll'], shell_flag)
    asset_dt_list = [
        dt.datetime.strptime(match.group('date'), ds_params['asset_dt_fmt'])
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

    # Line 185!!!
    logging.info('\nProcessing dates')
    for upload_dt in sorted(test_dt_list, reverse=True):
        logging.info('{}'.format(upload_dt.date()))
        year_ws = os.path.join(workspace, upload_dt.strftime('%Y'))
        date_ws = os.path.join(year_ws, upload_dt.strftime('%Y%m%d'))

        upload_path = os.path.join(
            year_ws, upload_dt.strftime(ds_params['asset_dt_fmt']) + '.tif')
        bucket_path = '{}/{}/{}'.format(
            ds_params['bucket_name'], ds_params['bucket_folder'],
            upload_dt.strftime(ds_params['asset_dt_fmt']) + '.tif')
        asset_id = '{}/{}'.format(
            ds_params['asset_coll'],
            ds_params['asset_id_fmt'].format(date=upload_dt.strftime(ds_params['asset_dt_fmt'])))
        logging.debug('  {}'.format(upload_path))
        logging.debug('  {}'.format(bucket_path))
        logging.debug('  {}'.format(asset_id))

        # In cron mode, remove all local files before starting
        if cron_flag and os.path.isdir(date_ws):
            shutil.rmtree(date_ws)

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
            # FIX ME, this might need to be generalized for other datasets
            # Need to check all dataset files for their format, then generalize

            # Each variable has it's own file containing all dates
            data_file = ds_params['infile_fmt'].format(var_name=variable)
            # Save all netCDF file in nc dir
            data_file_path = os.path.join(
                infile_ws, data_file)
            outfile_path = os.path.join(
                infile_ws, upload_dt.strftime('%Y'), upload_dt.strftime('%m_%b'),
                data_file)
            logging.debug('  {}'.format(data_file_path))

            # if overwrite_flag and os.path.isfile(outfile_path):
            #     logging.debug('  Removing data file')
            #     os.remove(data_file_path)
            if not os.path.isdir(os.path.dirname(data_file_path)):
                os.makedirs(os.path.dirname(data_file_path))

            tif_path = os.path.join(date_ws, '{}.tif'.format(variable))
            logging.debug('  {}'.format(tif_path))

            # Remove TIFs but leave data files
            if overwrite_flag and os.path.isfile(tif_path):
                logging.debug('  Removing TIF file')
                os.remove(tif_path)
            elif not overwrite_flag and os.path.isfile(tif_path):
                logging.debug('  TIF file exists, skipping')
                continue

            # Download data
            if not os.path.isfile(data_file_path) and not os.path.isfile(tif_path):
                # FIX ME: this needs to be developed
                DU = Utils.DownloadUtil(ds_params['download_method'], data_file_path, dataset)
                DU.download()

            # Create the geotiff
            if os.path.isfile(data_file_path) and not os.path.isfile(tif_path):
                DATAU = Utils.DataUtil()
                input_array = DATAU.create_array_form_data()
                Utils.array_to_geotiff(
                    input_array, tif_path, output_shape=ds_params['asset_shape'],
                    output_geo=ds_params['asset_geo'], output_proj=asset_proj,
                    output_nodata=ds_params['asset_nodata'])
                del input_array

        # Build composite image
        # We could also write the arrays directly to the composite image above
        # if composite_flag and not os.path.isfile(upload_path):
        input_vars = set(
            [os.path.splitext(f)[0] for f in os.listdir(date_ws)])
        if set(variables).issubset(input_vars):
            # Force output files to be 32-bit float GeoTIFFs
            output_driver = gdal.GetDriverByName('GTiff')
            output_rows, output_cols = ds_params['asset_shape']
            output_ds = output_driver.Create(
                upload_path, output_cols, output_rows, len(variables),
                gdal.GDT_Float32, ['COMPRESS=LZW', 'TILED=YES'])
            output_ds.SetProjection(asset_proj)
            output_ds.SetGeoTransform(ds_params['asset_geo'])
            for band_i, variable in enumerate(variables):
                data_array = Utils.raster_to_array(
                    os.path.join(date_ws, '{}.tif'.format(variable)))
                data_array[np.isnan(data_array)] = ds_params['asset_nodata']
                output_band = output_ds.GetRasterBand(band_i + 1)
                output_band.WriteArray(data_array)
                output_band.FlushCache()
                output_band.SetNoDataValue(ds_params['asset_nodata'])
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
                        '--bands', ','.join(ds_params['band_name'][v] for v in variables),
                        '--asset_id', asset_id,
                        '--time_start', upload_dt.date().isoformat(),
                        # '--nodata_value', nodata_value,
                        '--property', '(string)DATE_INGESTED={}'.format(
                            dt.datetime.today().strftime('%Y-%m-%d')),
                        bucket_path
                    ], shell=shell_flag)
                sleep(1)
            except Exception as e:
                logging.exception('    Exception: {}'.format(e))

        # Removing individual GeoTIFFs in cron mode
        if cron_flag and os.path.isdir(date_ws):
            shutil.rmtree(date_ws)


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
        return dt.datetime.strptime(input_date, "%Y-%m-%d")
    except ValueError:
        msg = "Not a valid date: '{}'.".format(input_date)
        raise argparse.ArgumentTypeError(msg)


def arg_parse():
    """"""

    end_dt = dt.datetime.today()

    parser = argparse.ArgumentParser(
        description='Ingest daily data into Earth Engine',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '-d', '--dataset', type=str, required=True,
        metavar='DATASET', default='NOAA_CRN',
        help='Set the dataset')
    parser.add_argument(
        '-w', '--workspace', type=str, metavar='PATH',
        default=os.path.dirname(os.path.abspath(__file__)),
        help='Set the current working directory')
    parser.add_argument(
        '-s', '--start', type=valid_date, metavar='DATE',
        default=(end_dt - dt.timedelta(days=365)).strftime('%Y-%m-%d'),
        help='Start date (format YYYY-MM-DD)')
    parser.add_argument(
        '-e', '--end', type=valid_date, metavar='DATE',
        default=end_dt.strftime('%Y-%m-%d'),
        help='End date (format YYYY-MM-DD)')
    parser.add_argument(
        '-v', '--variables', nargs='+',
        default=[],
        metavar='VAR',
        help='variables')
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
        '-db', '--debug', default=logging.INFO, const=logging.DEBUG,
        help='Debug level logging', action='store_const', dest='loglevel')

    args = parser.parse_args()

    # Set the variable choices for dataset
    ds_params = ds_settings[args.dataset]
    valid_vars = list(ds_params['bands'].values())
    if not set(args.variables).issubset(set(valid_vars)):
        msg =  'Not a valid variable list. Valid variables for dataset are: {}'.format(valid_vars) 
        raise argparse.ArgumentTypeError(msg)


    # Convert relative paths to absolute paths
    if args.workspace and os.path.isdir(os.path.abspath(args.workspace)):
        args.workspace = os.path.abspath(args.workspace)
    return args


if __name__ == '__main__':
    args = arg_parse()
    logging.basicConfig(level=args.loglevel, format='%(message)s')
    main(dataset=args.dataset, workspace=args.workspace,
         start_dt=args.start, end_dt=args.end, variables=args.variables,
         overwrite_flag=args.overwrite, cron_flag=args.cron,
         composite_flag=args.composite, upload_flag=args.upload,
         ingest_flag=args.ingest)

