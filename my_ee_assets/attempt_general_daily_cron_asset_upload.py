#--------------------------------
# Name:    daily_cron_ftp_download.py
# Purpose: Download data from ftp server to local dir
#          Convert data to .tif
#          Store data in bucket
# Notes:
#--------------------------------
import argparse
from builtins import input
import datetime
from ftplib import FTP
import gzip
import logging
import os
import re
import shutil
import subprocess
import sys
import tarfile
import time

import ee
import numpy as np
from osgeo import gdal, osr



from config import ds_settings

def main(dataset, workspace, start_dt, end_dt, overwrite_flag=False,
         cron_flag=False, composite_flag=True, upload_flag=True,
         ingest_flag=True):
    """Download data from ftp server

    Parameters
    ----------
    dataset: str
        dataset to be downloaded, see README.md for list of datasets
    workspace : str
        Root folder of data.
    start_dt : datetime
        Start date.
    end_dt : datetime
        End date.
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

    # What does this do?
    if os.name == 'posix':
        shell_flag = False
    else:
        shell_flag = True


    # Remove files from previous runs
    tar_ws = os.path.join(workspace, 'tar')
    if cron_flag and os.path.isdir(tar_ws):
        shutil.rmtree(tar_ws)
    if not os.path.isdir(tar_ws):
        os.makedirs(tar_ws)

    # Each variable will be written to a separate collection
    logging.debug('Image Collection: {}'.format(ds_params['asset_coll']))

    '''
    PSEUDO CODE DATES
     set dates
        SNODAS adjust to 2003
     Set dates list for missing data
    '''

    # Start with a list of dates to check
    logging.debug('\nBulding Date List')
    DB = DateBuilder(dataset, start_dt, end_dt)
    DB.adjust_dates_dt()
    test_dt_list = list(DateBuilder.date_range())
    if not test_dt_list:
        logging.info('  No test dates, exiting')
        return True
    logging.debug('\nTest dates: {}'.format(
        ', '.join(map(lambda x: x.strftime('%Y-%m-%d'), test_dt_list))))
    # Check if any of the needed dates are currently being ingested
    # Check task list before checking asset list in case a task switches
    # from running to done before the asset list is retrieved
    logging.debug('Getting Active Tasks')
    task_id_list = DB.get_task_id_list()


    dm = ds_params['download_method']
    variables = ds_params['band_name'].keys()
    DU = DownloadUtil(dm, dataset, workspace, start_dt, end_dt, variables)



class dateBuilder(object):
    '''
    '''
    def __init__(self, dataset, start_dt, end_dt):
        self.dataset = dataset
        self.start_dt = start_dt
        self.end_dt = end_dt

    def adjust_dates_dt(self):
        if self.dataset == 'SNODAS':
            # Limit start date to 2003-09-30
            if self.start_dt < datetime.datetime(2003, 9, 30):
                self.start_dt = datetime.datetime(2003, 9, 30)
                logging.info('\nAdjusting start date to: {}\n'.format(
                    self.start_dt.strftime('%Y-%m-%d')))
            if self.end_dt > datetime.datetime.today():
                self.end_dt = datetime.datetime.today()
                logging.info('Adjusting end date to:   {}\n'.format(
                    self.end_dt.strftime('%Y-%m-%d')))


    def date_range(self, days=1, skip_leap_days=False):
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
        curr_dt = copy.copy(self.start_dt)
        while curr_dt <= self.end_dt:
            if not skip_leap_days or curr_dt.month != 2 or curr_dt.day != 29:
                yield curr_dt
            curr_dt += datetime.timedelta(days=days)

    def get_ee_tasks(self, states=['RUNNING', 'READY']):
        """Return current active tasks
            Parameters
            ----------
            states : list
            Returns
            -------
            dict : Task descriptions (key) and task IDs (value).
        """
        tasks = {}
        for i in range(1, 10):
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
                time.sleep(i ** 2)
        return tasks

    def get_task_id_list(self):
        return [
            desc.replace('\nAsset ingestion: ', '')
            for desc in self.get_ee_tasks(states=['RUNNING', 'READY']).keys()]




class DownloadUtil(object):
    '''
    '''
    def __init__(self, dowmnload_method, dataset, workspace, start_dt, end_dt, variables):
        self.download_method = download_method
        self.dataset = dataset
        self.workspace = workspace
        self.start_dt = start_dt
        self.end_dt = end_dt
        self.variables = variables

    def ftp_download(self, site_url, site_folder, file_name, output_path):
        """
        Downloads one file from ftp server
        :param self:
        :param download_method: options: ftp
        :param site_url: ftp server url
        :param site_folder: folder on ftp server
        :param file_name: file to download
        :param output_path: file will be downloaded in output_path
        :return:
        """
        from ftplib import FTP
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

def arg_parse():
    """"""
    end_dt = datetime.datetime.today()

    parser = argparse.ArgumentParser(
        description='Ingest SNODAS daily data into Earth Engine',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--dataset', metavar='DATASET',
        default='NOAA_CRN',
        help='Set the dataset')
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
    main(dataset=args.dataset, workspace=args.workspace,
         start_dt=args.start, end_dt=args.end,
         overwrite_flag=args.overwrite, cron_flag=args.cron,
         composite_flag=args.composite, upload_flag=args.upload,
         ingest_flag=args.ingest)

