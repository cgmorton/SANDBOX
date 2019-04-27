#!/usr/bin/env python2
# source activate ee-python

import argparse
import logging
import subprocess
import os
import glob
from time import sleep

import ee


def upload_file_to_bucket(data_file_path, bucket_file_path, remove_local=False):
    '''
    :param data_file_path: source file path on local machine
    :param bucket_file_path: bucket name + bucket directory + bucket file name
    :param remove_local: Boolean; if True removes the local data file
    :return:
    '''

    # Set the shell flag: What does this do?
    if os.name == 'posix':
        shell_flag = False
    else:
        shell_flag = True

    args = ['gsutil', 'cp', data_file_path, bucket_file_path]
    if not logging.getLogger().isEnabledFor(logging.DEBUG):
        args.insert(1, '-q')
    try:
        subprocess.check_output(args, shell=shell_flag)
        if remove_local:
            os.remove(data_file_path)
    except Exception as e:
        logging.exception('    Exception: {}\n'.format(e))
        raise Exception(e)

def delete_files_from_bucket(bucket_path):
    '''
    Deletes all files and dirs in bucket_bath directory
    :param bucket_path:
    :return:
    '''
    if os.name == 'posix':
        shell_flag = False
    else:
        shell_flag = True
    if bucket_path[-1] == '/':
        bucket_file_path = bucket_path + '**'
    else:
        bucket_file_path = bucket_path + '/**'
    args = ['gsutil', 'rm', bucket_file_path]

    if not logging.getLogger().isEnabledFor(logging.DEBUG):
        args.insert(1, '-q')
    try:
        subprocess.check_output(args, shell=shell_flag)
    except Exception as e:
        logging.exception('    Exception: {}\n'.format(e))
        raise Exception(e)

def upload_shapefiles_to_ee(bucket_file_path, asset_id):
    '''
    Table upload into ee,
    If asset already exists, it will not be overwritten
    :param bucket_file_path: bucket name + bucket directory + bucket file name
    :param asset_id:
    :return:
    '''
    # Set the shell flag: What does this do?
    if os.name == 'posix':
        shell_flag = False
    else:
        shell_flag = True
    try:
        subprocess.check_output(
            [
                'earthengine', 'upload', 'table',
                '--asset_id', asset_id,
                bucket_file_path
            ], shell=shell_flag)
        sleep(1)
    except Exception as e:
        logging.exception('    Exception: {}'.format(e))


def arg_parse():
    """"""
    parser = argparse.ArgumentParser(
        description='Ingest shapefiles from local dir into Earth Engine (table upload)',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '-ld', '--local-dir', type=str, required=True,
        help='Local directory containing the shapefiles')
    parser.add_argument(
        '-bp', '--bucket-path', type=str,
        help='Bucket path: Bucket Name + Bucket Directory')
    parser.add_argument(
        '-ap', '--asset-path', type=str,
        help='Earth Engine asset directory')
    parser.add_argument(
        '-db', '--debug', default=logging.INFO, const=logging.DEBUG,
        help='Debug level logging', action='store_const', dest='loglevel')
    args = parser.parse_args()
    return args

if __name__ == "__main__":
    '''
    TO RUN
    python upload_shape_to_ee.py -ld /Users/bdaudert/Desktop/CE_Shapefile/CE_shp_orig -bp gs://clim-engine-shapefiles/ -ap projects/climate-engine/featureCollections/shp_orig/ -db
    '''

    EE_ACCOUNT = 'clim-engine-development@appspot.gserviceaccount.com'
    EE_PRIVATE_KEY_FILE = 'clim-engine.json'

    EE_CREDENTIALS = ee.ServiceAccountCredentials(
        EE_ACCOUNT, key_file=EE_PRIVATE_KEY_FILE)
    ee.Initialize(EE_CREDENTIALS)

    args = arg_parse()

    shapefiles = filter(os.path.isfile, glob.glob(args.local_dir + '/*.shp'))
    for shapefile in shapefiles:
        shape_path, shape_file = os.path.split(shapefile)
        file_name = shape_file.split('.shp')[0]
        if args.asset_path[-1] == '/':
            asset_id = args.asset_path + file_name
        else:
            asset_id = args.asset_path + '/' + file_name

        for ext in ['.shp', '.dbf', '.prj', '.shx', '.sbn']:
            file_path = shape_path + '/' + file_name + ext

            if args.bucket_path[-1] == '/':
                bucket_file_path = args.bucket_path +  file_name + ext
            else:
                bucket_file_path = args.bucket_path + '/' + file_name + ext


            logging.info('  Uploading {0} to bucket {1}'.format(file_path, bucket_file_path))
            print('  Uploading {0} to bucket {1}'.format(file_path, bucket_file_path))
            upload_file_to_bucket(shapefile, bucket_file_path, remove_local=False)


        logging.info('  Ingesting into Earth Engine {}'.format(asset_id))
        print('  Ingesting into Earth Engine {}'.format(asset_id))
        # Only .shp needs to be specified in earthengine upload
        bucket_file_path = args.bucket_path +  file_name + '.shp'
        upload_shapefiles_to_ee(bucket_file_path, asset_id)
    # Delete from bucket
    # delete_files_from_bucket(args.bucket_path)



