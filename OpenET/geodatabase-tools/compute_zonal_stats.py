import sys, time
import datetime as dt
import argparse
import ee

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


def compute_temporal_summary(ee_coll, temporal_summary):
    if temporal_summary == 'mean':
        ee_img = ee_coll.mean()
    if temporal_summary == 'sum':
        ee_img = ee_coll.sum()
    if temporal_summary == 'min':
        ee_img = ee_coll.min()
    if temporal_summary == 'max':
        ee_img = ee_coll.max()
    if temporal_summary == 'median':
        ee_img = ee_coll.median()
    return ee_img

def compute_zonal_stats(ee_img, ee_coll_name, feat_coll):
    '''
    Apply a reducer over the area of each feature in the given feature collection.
    e.g. fromFT = ee.FeatureCollection('ft:1tdSwUL7MVpOauSgRzqVTOwdfy17KDbw-1d9omPw')
    :param ee_img:
    :param feat_col:
    :return: dict zonal_stats
    '''

    def set_aadata(feature):
        if config.statics['feature_collections'][ee_coll_name]['metadata']:
            unique_col = config.statics['feature_collections'][ee_coll_name]['metadata'][0]
            name = feature.get(unique_col)
        else:
            name = 'NO NAME'
        area = feature.area()
        mean = feature.get('mean')
        feature = feature.set({'aa_data': [name, area, mean]})
        return feature

    # Reduce img over the regions of feat_coll
    '''
    # FIXME crs should match the crs of the img
    # proj = ee_img.projection()
    # crs = proj.crs()
    # transform = ee.List(ee.Dictionary(ee.Algorithms.Describe(proj)).get('transform'));  
    '''
    '''
    try:
        ee_reducedFeatColl = ee_img.reduceRegions(
            reducer=ee.Reducer.mean(),
            collection=feat_coll,
            tileScale=1,
            crs=crs,
            crsTransform=transform
        )
    except:
        ee_reducedFeatColl = ee.FeatureCollection([])
    '''

    ee_reducedFeatColl = ee_img.reduceRegions(
        collection=feat_coll,
        reducer=ee.Reducer.mean(),
        scale=30,
        tileScale=1,
        crs='EPSG:32610' # only valid in CA, based on img, feat projections should match the img projections
    )

    # ee_reducedFeatColl = ee_reducedFeatColl.map(set_aadata)
    # aa_datas = ee_reducedFeatColl.aggregate_array('aa_data').getInfo()
    # aa_datas = ee_reducedFeatColl.aggregate_array('mean').getInfo()
    # return aa_datas
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
    python compute_zonal_stats.py -ai projects/openet/test2/ssebop/monthly_wrs2 -fi users/bdaudert/nasa-roses/usgs_central_valley_mod_base15_ca_poly_170616_wgs84 -y 2017
    '''

    start_time = time.time()
    # ee.Initialize()

    args = arg_parse()
    coll_name = ''
    # Get the feature collection name
    feat_colls = config.statics['feature_collections'].keys()
    for feat_coll in feat_colls:
        if config.statics['feature_collections'][feat_coll]['feature_collection_name'] == args.feature_collection_id:
                coll_name = feat_coll

    if not coll_name:
        raise Exception('Feature Collection not found in statics.feature_collections')
        sys.exit(1)

    year = int(args.year)
    ee_feat_coll = ee.FeatureCollection(args.feature_collection_id)
    ee_img = ee.Image()
    ee_coll = ee.ImageCollection(args.asset_id)
    ee_img_list = []
    ee_img_properties = {}
    # FIXME: There will be another loop here over tiles(or utm zones) that deals with crs and img properties
    # The featuremetadta table needs to be updated with that info
    for var in args.variables:
        for m_int in range(1, 13):
            m_str = str(m_int)
            if len(m_str) == 1:
                m_str = '0' + m_str
            # Set start/end date strings for year and month
            sd, ed = set_start_end_date_str(year, m_int)
            # Filter collections by dates, and rename the band
            coll = ee_coll.filterDate(sd, ed).select([var], [var + '_m' + m_str])
            # FIXME not needed sincew monthly data
            # Temporal Summary
            if var == 'ndvi':
                temporal_summary = 'mean'
            else:
                temporal_summary = 'sum'
            ee_img = compute_temporal_summary(coll, temporal_summary).copyProperties(coll.first())
            # print(ee_img.getInfo()['properties']);
            #FIXME: attach these properties to each feature in the collection as properties
            ee_img_list.append(ee_img)
    # Combine images into one multi-band image
    ee_img = ee.Image.cat(ee_img_list)


    # Zonal Stats
    reducedFeatColl = compute_zonal_stats(ee_img, coll_name, ee_feat_coll)
    # data = [[d[0], round(d[1], 4), round(d[2], 4)] for d in data if d[2] is not None]

    for var in args.variables:
        for m_int in range(1, 13):
            sd, ed = set_start_end_date_str(year, m_int)
            m_str = str(m_int)
            if len(m_str) == 1:
                m_str = '0' + m_str
            print('VAR/MONTH ' + var + '/' + m_str)
            print(sd, ed)
            print(len(reducedFeatColl.aggregate_array(var + '_m' + m_str).getInfo()))

    # reducedFeatColl.getInfo() returns dict --> convert to geojson
    ''''
    task = ee.batch.Export.table.toCloudStorage(
        collection=reducedFeatColl,
        bucket='roses-geojson',
        fileNamePrefix= args.feature_collection_id + '_' + str(year),
        fileFormat='GeoJSON'
    )
    task.start()
    print(task.status())
    '''

    print("--- %s seconds ---" % (str(time.time() - start_time)))
    print("--- %s minutes ---" % (str((time.time() - start_time) / 60.0)))
    print("--- %s hours ---" % (str((time.time() - start_time) / 3600.0)))