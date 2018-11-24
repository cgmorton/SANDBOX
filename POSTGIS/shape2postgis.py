import os, subprocess
import time

'''
import sqlalchemy as db
from sqlalchemy import create_engine
from sqlalchemy.orm import session as session_module
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import event
from sqlalchemy import DDL
'''

from osgeo import ogr
from osgeo import gdal
gdal.SetConfigOption('CPL_DEBUG','ON')


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
    DB_USER = os.environ['DB_USER']
    DB_PASSWORD = os.environ['DB_PASSWORD']
    DB_PORT = os.environ['DB_PORT']
    DB_HOST = os.environ['DB_HOST']
    DB_NAME = os.environ['DB_NAME']
    connectionString = "PG:dbname='%s' host='%s' port='%s' user='%s' password = '%s'" % (DB_NAME, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD)

    base_dir = '/Users/bdaudert/SANDBOX/POSTGIS/test_files'
    full_dir = os.walk(base_dir)
    schema = 'shape_test'

    start_time = time.time()
    # Find shapefiles
    shapefile_list = []
    for source, dirs, files in full_dir:
        for file_ in files:
            if file_[-3:] == 'shp':
                print("Found Shapefile " + str(file_))
                shapefile_path = base_dir + '/' + file_
                shapefile_list.append(shapefile_path)

    ogrds = ogr.Open(connectionString)
    for shapefile in shapefile_list:
        table_name = schema  + '.' + os.path.splitext(os.path.basename(shapefile))[0]
        print('LOOOOK')
        print(table_name)
        # table_name = schema + '.data_all'

        name = testLoad(ogrds, table_name, shapefile)
        print(name)

    # ogrds.DeleteLayer(table)

    print("--- %s seconds ---" % (str(time.time() - start_time)))
    print("--- %s minutes ---" % (str((time.time() - start_time) / 60.0)))
    print("--- %s hours ---" % (str((time.time() - start_time) / 3600.0)))
