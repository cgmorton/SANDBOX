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

    start_time = time.time()

    shapefile = '/Users/bdaudert/SANDBOX/OpenET/test_files/base15_ca_poly_170616_DATA_ALL.shp'
    schema = 'shape_test'
    table = schema + '.data_all'

    DB_USER = os.environ['DB_USER']
    DB_PASSWORD = os.environ['DB_PASSWORD']
    DB_PORT = os.environ['DB_PORT']
    DB_HOST = os.environ['DB_HOST']
    DB_NAME = os.environ['DB_NAME']

    connectionString = "PG:dbname='%s' host='%s' port='%s' user='%s' password = '%s'" % (DB_NAME,DB_HOST,DB_PORT,DB_USER,DB_PASSWORD)

    ogrds = ogr.Open(connectionString)
    name = testLoad(ogrds, table, shapefile)
    print(name)
    #    ogrds.DeleteLayer(table)

    print("--- %s seconds ---" % (str(time.time() - start_time)))
    print("--- %s minutes ---" % (str((time.time() - start_time) / 60.0)))
    print("--- %s hours ---" % (str((time.time() - start_time) / 3600.0)))
