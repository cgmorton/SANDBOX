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

    QU = db_methods.query_Util(model, variable, user_id, temporal_resolution, engine)
    # QU.test()



    '''
    # 1 API call example 0.277secs
    Request monthly time series for a single field that is not associated 
    with a user using the feature_id (unique primary key) directly 
    '''
    '''
    params = {
        'start_date': '2003-01-01',
        'end_date': '2003-12-31',
        'feature_id': 4,
        'temporal_summary': 'raw'
    }
    data = QU.api_ex1(**params)
    print(data)
    '''

    '''
    # 2 API call example 1.45secs
    Request mean monthly values for each feature  in a featureCollection
    Note: no spatial summary
    '''
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

    '''
    # 3 API call example 0.44 seconds
    Request monthly time series for a single field from a featureCollection that is selected by metadata;
    feature_metadata_name (feature_id)/feature_metadata_value
    Note: no spatial summary
    '''

    '''
    params = {
        'feature_collection_name': '/projects/nasa-roses/BRC_Combined_subset_2009',
        'feature_metadata_name': 'OBJECTID',
        'feature_metadata_properties': '2645',
        'start_date': '2003-01-01',
        'end_date': '2003-06-30',
        'temporal_summary': 'mean'
    }
    data = QU.api_ex3(**params)
    print(data)
    '''

    '''
    # 4 API call example 2.189 seconds
    Request area averaged max monthly values for all features in a featureCollection for a user 
    '''
    '''
    params = {
        'feature_collection_name': '/projects/nasa-roses/BRC_Combined_subset_2009',
        'start_date': '2003-01-01',
        'end_date': '2003-06-30',
        'temporal_summary': 'max',
        'spatial_summary': 'mean'
    }
    data = QU.api_ex4(**params)
    print(data)
    '''

    '''
    # 5 API call example  seconds 0.428
    Request monthly time series for a subset of features in collection defined by list of property values
    '''

    '''
    params = {
        'feature_collection_name': '/projects/nasa-roses/BRC_Combined_subset_2009',
        'feature_metadata_name': 'OBJECTID',
        'feature_metadata_properties': ('2708', '2640', '2706'),
        'start_date': '2003-01-01',
        'end_date': '2003-12-31',
        'temporal_summary': 'mean'
    }
    data = QU.api_ex5(**params)
    print(data)
    '''

    '''
    # 6 API call example  0.7411 seconds
    Request time series for subset of features in collection defined by a separate geometry (like bbox or polygon) 
    '''

    params = {
        'feature_collection_name': '/projects/nasa-roses/BRC_Combined_subset_2009',
        'selection_geometry': 'POLYGON((-111.5 42, -111.5 43, -111.4 43, -111.4 42, -111.5 42))',
        'start_date': '2003-01-01',
        'end_date': '2003-12-31',
        'temporal_summary': 'mean'
    }
    data = QU.api_ex6(**params)
    print(data)


    print("--- %s seconds ---" % (str(time.time() - start_time)))
    print("--- %s minutes ---" % (str((time.time() - start_time) / 60.0)))
    print("--- %s hours ---" % (str((time.time() - start_time) / 3600.0)))
