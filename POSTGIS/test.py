import os, subprocess
import time



from sqlalchemy import create_engine

'''
from sqlalchemy.orm import session as session_module
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import event
from sqlalchemy import DDL
'''


from osgeo import ogr
from osgeo import gdal
gdal.SetConfigOption('CPL_DEBUG','ON')

import config
import db_methods

def testLoad(serverDS, table, sourceFile):
    ogr.RegisterAll()
    shapeDS = ogr.Open(sourceFile)
    sourceLayer = shapeDS.GetLayerByIndex(0)
    options = []
    newLayer = serverDS.CreateLayer(table,sourceLayer.GetSpatialRef(),ogr.wkbUnknown,options)
    for x in xrange(sourceLayer.GetLayerDefn().GetFieldCount()):
        newLayer.CreateField(sourceLayer.GetLayerDefn().GetFieldDefn(x))

    newLayer.StartTransaction()
    for x in xrange(sourceLayer.GetFeatureCount()):
        newFeature = sourceLayer.GetNextFeature()
        newFeature.SetFID(-1)
        newLayer.CreateFeature(newFeature)
        if x % 500 == 0:
            newLayer.CommitTransaction()
            newLayer.StartTransaction()
    newLayer.CommitTransaction()
    return newLayer.GetName()


#######################################
# END OpenET database tables
######################################

if __name__ == '__main__':

    start_time = time.time()
    '''
    shapefile = '/Users/bdaudert/SANDBOX/OpenET/test_files/base15_ca_poly_170616_DATA_ALL.shp'
    schema = 'shape_test'
    table = schema + '.data_all'
    connectionString = "PG:dbname='%s' host='%s' port='%s' user='%s' password = '%s'" % (DB_NAME,DB_HOST,DB_PORT,DB_USER,DB_PASSWORD)
    ogrds = ogr.Open(connectionString)
    name = testLoad(ogrds, table, shapefile)
    print(name)
    #    ogrds.DeleteLayer(table)
    '''

    '''
       # Set up the db session
       Session = session_module.sessionmaker()
       Session.configure(bind=engine)
       session = Session()
       session.execute("SET search_path TO " + schema + ', public')
       session.close()
   '''

    DB_USER = config.DRI_DB_USER
    DB_PASSWORD = config.DRI_DB_PASSWORD
    DB_PORT = config.DRI_DB_PORT
    DB_HOST = config.DRI_DB_HOST
    DB_NAME = config.DRI_DB_NAME
    schema = config.schema

    model = 'ssebop'
    variable = 'et'
    user_id = 0
    temporal_resolution = 'monthly'


    db_string = "postgresql+psycopg2://" + DB_USER + ":" + DB_PASSWORD
    db_string += "@" + DB_HOST + ":" + str(DB_PORT) + '/' + DB_NAME
    engine = create_engine(db_string, pool_size=20, max_overflow=0)
    # db_methods.Base.metadata.bind = engine

    QU = db_methods.new_query_Util(model, variable, user_id, temporal_resolution, engine)
    # QU.test()



    '''
    # 1 API call example
    Request monthly time series for a single field that is not associated 
    with a user using the feature_id (unique primary key) directly 
    '''

    '''
    data = QU.api_ex_1(1, '2003-01-01', '2003-12-31', temporal_summary='raw')
    print(data)
    '''

    '''
    # 2 API call example
    Request mean monthly values for each feature  in a featureCollection
    Note: no spatial summary
    '''

    params = {
        'feature_collection_name': '/projects/nasa-roses/BRC_Combined_subset_2009',
        'start_date': '2003-01-01',
        'end_date': '2003-06-30',
        'temporal_summary': 'sum'
    }
    data = QU.api_ex2(**params)
    print(data)


    '''
    # 3 API call example
    Request monthly time series for a single field from a featureCollection that is selected by metadata;
    feature_metadata_name (feature_id)/feature_metadata_value
    Note: no spatial summary
    '''

    '''
    feat_coll_name = '/projects/nasa-roses/BRC_Combined_subset_2009'
    feature_id = '2645'
    sd = '2003-01-01'
    ed = '2003-06-30'
    data = QU.api_ex3(feat_coll_name, 'OBJECTID', feature_id, sd, ed, temporal_summary="mean")
    print(data)
    '''
    print("--- %s seconds ---" % (str(time.time() - start_time)))
    print("--- %s minutes ---" % (str((time.time() - start_time) / 60.0)))
    print("--- %s hours ---" % (str((time.time() - start_time) / 3600.0)))
