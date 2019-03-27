import sys, time
import datetime as dt
import argparse
import ee
import pprint

import config

def set_start_end_date_str(yr_int, m_int):
    '''
    Set start/end date
    Note: ee dates are ot inclusive so we need to set end date
    to the beginning of next months rather than endof this month
    '''
    if len(str(m_int)) == 1:
        m_str = '0' + str(m_int)
    else:
        m_str = str(m_int)
    sd = str(yr_int) + '-' + m_str + '-01'
    ed = str(yr_int) + '-' + m_str + '-' + str(config.statics['mon_len'][m_int - 1])
    return sd, ed

def compute_zonal_stats(ee_img, feat_coll):
    '''
    Apply a reducer over the area of each feature in the given feature collection.
    e.g. fromFT = ee.FeatureCollection('ft:1tdSwUL7MVpOauSgRzqVTOwdfy17KDbw-1d9omPw')
    :param ee_img:
    :param feat_col:
    :return: dict zonal_stats
    '''
    def add_img_properties(feature):
        return feature.copyProperties(ee_img)


    '''
    # FIXME: crs/transform should match the crs of the img
    proj = ee_img.projection()
    crs = proj.crs()
    transform = ee.List(ee.Dictionary(ee.Algorithms.Describe(proj)).get('transform'))
    '''
    crs = 'EPSG:32610'
    transform = [30, 0, 15, 0, -30, 15]
    try:
        ee_reducedFeatColl = ee.Image(ee_img).reduceRegions(
            reducer=ee.Reducer.mean(),
            collection=feat_coll,
            tileScale=1,
            crs=crs,
            crsTransform=transform
        )
    except Exception as e:
        raise Exception(e)

    #FIXME: Copy the image properties to each feature
    ee_reducedFeatColl = ee_reducedFeatColl.map(add_img_properties)
    return ee_reducedFeatColl


def arg_parse():
    """"""
    end_dt = dt.datetime.today()

    parser = argparse.ArgumentParser(
        description='Compute Zonal Statistics',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-ai', '--asset-id', type=str, required=True,
        metavar='Earth Engine asset ID', default='',
        help='Set the asset ID')

    parser.add_argument(
        '-fi', '--feature-collection-id', type=str, required=True,
        metavar='Earth Engine feature collection ID', default='',
        help='Set the feature collection ID')

    parser.add_argument(
        '-y', '--year', type=str, metavar='YEAR',
        default=(end_dt - dt.timedelta(days=365)).strftime('%Y'),
        help='Start date (format YYYY)')

    parser.add_argument(
        '-v', '--variables', nargs='+', default=['et', 'etr', 'ndvi'], metavar='VAR',
        help='variables')

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    '''
    # One landsat scene ssebop
    python compute_zonal_stats.py -ai projects/openet/test2/ssebop/monthly_wrs2 -fi users/bdaudert/nasa-roses/usgs_central_valley_mod_base15_ca_poly_170616_wgs84 -y 2017

    # One landsat scene ndvi_et
    python compute_zonal_stats.py -ai projects/openet/test2/ndvi_et/monthly_wrs2 -fi users/bdaudert/nasa-roses/usgs_central_valley_mod_base15_ca_poly_170616_wgs84 -y 2017

    # All of CA ssebop
    python compute_zonal_stats.py -ai projects/openet/test2/ssebop/monthly_mgrs -fi users/bdaudert/nasa-roses/usgs_central_valley_mod_base15_ca_poly_170616_wgs84 -y 2017

    # All of CA ndvi_et
    python compute_zonal_stats.py -ai projects/openet/test2/ndvi_et/monthly_mgrs -fi users/bdaudert/nasa-roses/usgs_central_valley_mod_base15_ca_poly_170616_wgs84 -y 2017
    '''

    start_time = time.time()
    args = arg_parse()
    month_ints = range(1, 13)
    feat_coll_name = ''
    # Get the feature collection name
    feat_colls = config.statics['feature_collections'].keys()
    for feat_coll in feat_colls:
        if config.statics['feature_collections'][feat_coll]['feature_collection_name'] == args.feature_collection_id:
                feat_coll_name = feat_coll
    if not feat_coll_name:
        raise Exception('Feature Collection not found in statics.feature_collections')
        sys.exit(1)

    year = int(args.year)
    ee_feat_coll = ee.FeatureCollection(args.feature_collection_id)
    ee_img = ee.Image()
    ee_coll = ee.ImageCollection(args.asset_id)
    # FIXME: There will be another loop here over tiles(or utm zones) that deals with crs and img properties
    # The featuremetadata table needs to be updated with that info
    ee_img_list = []
    for m_int in month_ints:
        m_str = str(m_int)
        if len(m_str) == 1:
            m_str = '0' + m_str
        for var in args.variables:
            # Set start/end date strings for year and month
            sd, ed = set_start_end_date_str(year, m_int)
            # Filter collections by dates, and rename the band
            coll = ee_coll.filterDate(sd, ed).select([var], [var + '_m' + m_str])
            ee_img = coll.mosaic()
            # ee_img = coll.first()
            ee_img_list.append(ee_img)

        # Combine images into one multi-band image
        ee_img = ee.Image.cat(ee_img_list).copyProperties(coll.first())
        # Zonal Stats
        reducedFeatColl = compute_zonal_stats(ee_img, ee_feat_coll)

        file_name = ('.').join(args.asset_id.split('/')) + '-' + ('.').join(args.feature_collection_id.split('/'))
        file_name = file_name + '-' + str(year) + '-' + m_str,
        # Upload to bucket as geojson
        task = ee.batch.Export.table.toCloudStorage(
            collection=reducedFeatColl,
            bucket='roses-geojson',
            fileNamePrefix = file_name,
            fileFormat='GeoJSON'
        )
        task.start()
        print(task.status())
        print(file_name)

        '''
        # Uncomment if you want to see the data
        for var in args.variables:
            sd, ed = set_start_end_date_str(year, m_int)
            m_str = str(m_int)
            if len(m_str) == 1:
                m_str = '0' + m_str
            print('VAR/MONTH ' + var + '/' + m_str)
            print(sd, ed)
            print(reducedFeatColl.aggregate_array(var + '_m' + m_str).getInfo())
        '''
    print("--- %s seconds ---" % (str(time.time() - start_time)))
    print("--- %s minutes ---" % (str((time.time() - start_time) / 60.0)))
    print("--- %s hours ---" % (str((time.time() - start_time) / 3600.0)))
