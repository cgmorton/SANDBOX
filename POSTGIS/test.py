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
    data = QU.get_data_for_feature_id(1, '2003-01-01', '2003-12-31', temporal_summary='raw')
    print(data)
    '''

    '''
    # 2 PAI call example
    Request mean monthly values for each feature  in a featureCollection
    '''

    feat_coll_name = '/projects/nasa-roses/BRC_Combined_subset_2009'
    sd = '2003-01-01'
    ed = '2003-06-30'
    data = QU.get_data_for_features_in_collection(feat_coll_name, sd, ed, temporal_summary='max', spatial_summary='mean')
    print(data)


    print("--- %s seconds ---" % (str(time.time() - start_time)))
    print("--- %s minutes ---" % (str((time.time() - start_time) / 60.0)))
    print("--- %s hours ---" % (str((time.time() - start_time) / 3600.0)))
