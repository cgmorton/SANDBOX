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
    # aa_data= ee_reducedFeatColl.getInfo()
    return aa_data

def add_to_shapefile(infile, feat_names, new_feat_data):
    # open the shapefile
    driver = ogr.GetDriverByName('ESRI Shapefile')
    dataSource = driver.Open(infile, 1) # open for rw
    if dataSource is None:
        print "ERROR: could not open '%s' as shapefile!" % (infile)
        sys.exit(1)

    layer = dataSource.GetLayer()
    for feat_name in feat_names:
        layer.CreateField(ogr.FieldDefn(feat_name, ogr.OFTReal))

    for i in range(layer.GetFeatureCount()):
        feature = layer.GetFeature(i)
        for j in range(len(feat_names)):
            feat_name = feat_names[j]
            val = new_feat_data[j][i]
            if abs(val) < 0.0001:
                feature.SetField(feat_name, -9999.0)
            else:
                feature.SetField(feat_name, val)
        layer.setFeature(feature)
        feat = None
    dataSource = None


if __name__ == '__main__':
    ee.Initialize()
    year = '2017'
    start = year + '-01-01'
    end = year + '-12-31'
    proj = 'EPSG:4326'
    scale = 30
    var_name = 'et_actual'
    featColl = ee.FeatureCollection('users/bdaudert/base15_ca_poly_170616')
    shapefile = 'test_files/base15_ca_poly_170616.shp'
    feat_names = []
    feat_data = []

    start_time = time.time()

    coll_name = 'projects/usgs-ssebop/et/conus/monthly/v0'
    ee_coll = ee.ImageCollection(coll_name).\
        filterDate(start, end).select(var_name)
    '''
    print('Getting monthly data')
    for m_int in range(1,13):
        m_str = str(m_int)
        if len(m_str) < 10:
            m_str = '0' + m_str
        feat_names.append('et_' + year + '_' + m_str)
        ee_img = ee.Image(ee_coll.filter(ee.Filter.calendarRange(m_int, m_int, 'month')).sum().unmask())
        feat_data.append(reduceRegions(ee_img, featColl, scale, proj)) 
    '''


    print('Getting annual data')
    feat_names.append('et_' + year)
    coll_name = 'projects/usgs-ssebop/et/conus/annual/v1'
    ee_img = ee.Image(ee.ImageCollection(coll_name).\
        filterDate(start, end).select(var_name).sum().unmask())
    feat_data.append(reduceRegions(ee_img, featColl, scale, proj))

    print('Adding to shapefile')
    add_to_shapefile(shapefile, feat_names, feat_data)

    print("--- %s seconds ---" % (str(time.time() - start_time)))
    print("--- %s minutes ---" % (str((time.time() - start_time) / 600.0)))
    print("--- %s hours ---" % (str((time.time() - start_time) / 3600.0)))
