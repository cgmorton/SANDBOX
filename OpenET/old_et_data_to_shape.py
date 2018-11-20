#!/Users/bdaudert/anaconda/envs/assets/bin/python
import sys
import time
from osgeo import gdal, osr, ogr
import ee

def reduceRegions(ee_img, featColl, scale, proj):
    ee_reducedFeatColl = ee_img.reduceRegions(
        collection=featColl,
        reducer=ee.Reducer.mean(),
        scale=scale,
        tileScale=1,
        crs=proj
    )
    aa_data= ee_reducedFeatColl.aggregate_array('mean').getInfo()
    return aa_data

def add_to_shapefile(infile, feat_names, feat_data):
    # open the shapefile
    driver = ogr.GetDriverByName('ESRI Shapefile')
    dataSource = driver.Open(infile, 1) # open for rw
    if dataSource is None:
        print "ERROR: could not open '%s' as shapefile!" % (infile)
        sys.exit(1)

    layer = dataSource.GetLayer()
    print(len(layer))
    print(len(feat_data[0]))
    for i, feat_name in enumerate(feat_names):
        layer.CreateField(ogr.FieldDefn(feat_name, ogr.OFTReal))
        for j, feat in enumerate(layer):
            feat.SetField(feat_name, feat_data[i][j])
    dataSource = None

if __name__ == '__main__':
    ee.Initialize()
    year = '2017'
    start = year + '-01-01'
    end = year + '-12-31'
    proj = 'EPSG:4326'
    scale = 30
    var_name = 'et_actual'

    coll_name = 'projects/usgs-ssebop/et/conus/monthly/v0'
    ee_img = ee.ImageCollection(coll_name).\
        filterDate(start, end).select(var_name).sum().unmask()
    featColl = ee.FeatureCollection('users/bdaudert/base15_ca_poly_170616')
    shapefile = '/Users/bdaudert/DATA/OpenET/Central_Valley/shapefiles/base15_ca_poly_170616.shp'
    start_time = time.time()
    aadata = reduceRegions(ee_img, featColl, scale, proj)
    add_to_shapefile(shapefile, ['et_' + year], [aadata])

    # Add Annual Data
    coll_name = 'projects/usgs-ssebop/et/conus/annual/v1'
    print("--- %s seconds ---" % (str(time.time() - start_time)))
    print("--- %s minutes ---" % (str((time.time() - start_time) / 600.0)))
    print("--- %s hours ---" % (str((time.time() - start_time) / 3600.0)))
