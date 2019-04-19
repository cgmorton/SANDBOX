#!/usr/bin/env python

import os, time
import datetime as dt
import argparse
from sqlalchemy import create_engine

import db_methods
import config
from sqlalchemy.orm import session as session_module

'''
PROJECT = "NASA-ROSES"
SCHEMA = config.NASA_ROSES_SCHEMA
DB_USER = config.NASA_ROSES_DB_USER
DB_PASSWORD = config.NASA_ROSES_DB_PASSWORD
DB_PORT = config.NASA_ROSES_DB_PORT
DB_HOST = config.NASA_ROSES_DB_HOST
DB_NAME = config.NASA_ROSES_DB_NAME
GEO_BUCKET_URL = config.NASA_ROSES_GEO_BUCKET_URL
DATA_BUCKET_URL = config.NASA_ROSES_DATA_BUCKET_URL
'''

PROJECT = "OPENET"
SCHEMA = config.OPENET_SCHEMA
DB_USER = config.OPENET_DB_USER
DB_PASSWORD = config.OPENET_DB_PASSWORD
DB_PORT = config.OPENET_DB_PORT
DB_HOST = config.OPENET_DB_HOST
DB_NAME = config.OPENET_DB_NAME
DATA_BUCKET_URL = config.OPENET_DATA_BUCKET_URL


def arg_parse():
    """"""
    end_dt = dt.datetime.today()

    parser = argparse.ArgumentParser(
        description='Populate database',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-fi', '--feature-collection-id', type=str, required=True,
        metavar='Earth Engine feature collection ID', default='',
        help='Set the feature collection ID')

    parser.add_argument(
        '-m', '--model', type=str, required=True,
        metavar='Model Name', default='',
        help='Set the model name')

    parser.add_argument(
        '-uid', '--user-id', type=int,
        metavar='User ID', default=0,
        help='User associated with the feature collection, defaults to 0 (public)')

    parser.add_argument(
        '-v', '--variables', nargs='+', default=['et', 'etf', 'etr', 'ndvi'], metavar='VAR',
        help='variables')

    parser.add_argument(
        '-s', '--start', metavar='YEAR',
        default=(end_dt - dt.timedelta(days=365)).strftime('%Y'),
        help='Start Year (format YYYY)')
    parser.add_argument(
        '-e', '--end', metavar='YEAR',
        default=end_dt.strftime('%Y'),
        help='End Year (format YYYY, inclusive)')

    args = parser.parse_args()
    return args


if __name__ == '__main__':

    # Parse user arguments
    args = arg_parse()


    db_string = "postgresql+psycopg2://" + DB_USER + ":" + DB_PASSWORD
    db_string += "@" + DB_HOST +  ":" + str(DB_PORT) + '/' + DB_NAME
    engine = create_engine(db_string, pool_size=20, max_overflow=0)

    '''
    # NOTE: comment this out if you don't want to delete and repopuate everything
    db_methods.Base.metadata.drop_all(engine)
    db_methods.Base.metadata.create_all(engine)
    '''

    start_time = time.time()

    # Set up the db session
    Session = session_module.sessionmaker()
    # Session = scoped_session(sessionmaker())
    Session.configure(bind=engine)

    # print(Base.metadata.sorted_tables)
    feature_collection_name = args.feature_collection_id

    features_change_by_year = False
    '''
    if feat_coll in config.statics['feature_collections_changing_by_year']:
        features_change_by_year = True
    '''
    bucket_path = DATA_BUCKET_URL
    # FIXME: is it a goog idea to demand the file names to be of certain format??
    data_file_name = args.model + '_'
    data_file_name += config.statics['feature_collections'][args.feature_collection_id]['data_file_name']


    s_year = int(args.start[0:4])
    e_year = int(args.end[0:4])
    years = range(s_year, e_year + 1)
    for year_int in years:
        # FIXME: support general file format for zonal stats files?
        zonal_stats_geojson = data_file_name + '_' + str(year) + '.geojson'
        year = str(year_int)
        DB_Util = db_methods.database_Util(
            args.model, year, args.variables, engine,
            args.user_id, args.feature_collection_id,
            zonal_stats_geojson, bucket_path, features_change_by_year = False
        )
        if PROJECT == "OPENET":
            # Could not  read from nasa-roses bucket, but that might be fixable
            local_data_file = config.OPENET_LOCAL_DATA_PATH + zonal_stats_geojson
            geojson_data = DB_Util.read_data_from_local()
        else:
            # Data is read from bucket inside add_data_to_db
            geojson_data = None

        session = Session()
        session.execute("SET search_path TO " + SCHEMA + ', public')
        DB_Util.add_data_to_db(session, user_id=args.user_id, geojson_data=geojson_data)
        session.close()

    print("--- %s seconds ---" % (str(time.time() - start_time)))
    print("--- %s minutes ---" % (str((time.time() - start_time) / 60.0)))
    print("--- %s hours ---" % (str((time.time() - start_time) / 3600.0)))

