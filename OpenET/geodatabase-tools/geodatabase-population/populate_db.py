import os, time
import datetime as dt
import argparse
from sqlalchemy import create_engine

import db_methods
import config
from sqlalchemy.orm import session as session_module

SCHEMA = config.NASA_ROSES_SCHEMA
DB_USER = config.NASA_ROSES_DB_USER
DB_PASSWORD = config.NASA_ROSES_DB_PASSWORD
DB_PORT = config.NASA_ROSES_DB_PORT
DB_HOST = config.NASA_ROSES_DB_HOST
DB_NAME = config.NASA_ROSES_DB_NAME
GEO_BUCKET_URL = config.NASA_ROSES_GEO_BUCKET_URL
DATA_BUCKET_URL = config.NASA_ROSES_DATA_BUCKET_URL

def arg_parse():
    """"""
    end_dt = dt.datetime.today()

    parser = argparse.ArgumentParser(
        description='Populate database',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-fcollid', '--feature-collection-id', type=str, required=True,
        metavar='Earth Engine feature collection ID', default='',
        help='Set the feature collection ID')

    parser.add_argument(
        '-m', '--model', type=str, required=True,
        metavar='Model Name', default='',
        help='Set the model name')

    parser.add_argument(
        '-v', '--variables', nargs='+', default=['et', 'eto', 'etr', 'ndvi'], metavar='VAR',
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
    user_id = 0
    feat_coll = args.feature_collection_id
    model =  args.model

    geom_change_by_year = False
    if feat_coll in config.statics['feature_collections_changing_by_year']:
        geom_change_by_year = True

    s_year = int(args.start[0:4])
    e_year = int(args.end[0:4])
    years = range(s_year, e_year + 1)
    for year_int in years:
        year = str(year_int)
        DB_Util = db_methods.database_Util(feat_coll, model, year, user_id, geom_change_by_year, engine)
        etdata = DB_Util.read_etdata_from_bucket()
        geojson_data = DB_Util.read_geodata_from_bucket()
        session = Session()
        session.execute("SET search_path TO " + SCHEMA + ', public')
        DB_Util.add_data_to_db(session, user_id=user_id, etdata=etdata, geojson_data=geojson_data)
        session.close()

    print("--- %s seconds ---" % (str(time.time() - start_time)))
    print("--- %s minutes ---" % (str((time.time() - start_time) / 60.0)))
    print("--- %s hours ---" % (str((time.time() - start_time) / 3600.0)))

