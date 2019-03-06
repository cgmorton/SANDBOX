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
    if len(str(m_int)) < 2:
        m_str = '0' + str(m_int)
    else:
        m_str = str(m_int)
    sd = str(yr_int) + '-' + m_str + '-01'
    ed = str(yr_int) + '-' + m_str + '-' + str(config.statics['mon_len'][m_int - 1])
    '''
    if m_int < 10:
        ed = str(yr_int) + '-' + '0' + str(m_int + 1) + '-01'
    elif m_int >= 10 and m_int < 12:
        ed = str(yr_int) + '-' + str(m_int + 1) + '-01'
    else:
        ed = str(yr_int + 1) + '-01-01'
    '''
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
        feature = feature.set({'aa_data': [name, area, mean, 4]})
        return feature

    # Reduce img over the regions of feat_coll
    '''
    try:
        ee_reducedFeatColl = ee_img.reduceRegions(
            reducer=ee.Reducer.mean(),
            collection=feat_coll,
            scale=self.tv['scale'],
            tileScale=1,
            crs='EPSG:4326'
        )
    except:
        ee_reducedFeatColl = ee.FeatureCollection([])
    '''

    ee_reducedFeatColl = ee_img.reduceRegions(
        collection=feat_coll,
        reducer=ee.Reducer.mean(),
        scale=250,
        tileScale=1,
        crs='EPSG:4326'
    )

    ee_reducedFeatColl = ee_reducedFeatColl.map(set_aadata)
    aa_datas = ee_reducedFeatColl.aggregate_array('aa_data').getInfo()
    return aa_datas


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
        '-s', '--start', type=str, metavar='YEAR',
        default=(end_dt - dt.timedelta(days=365)).strftime('%Y'),
        help='Start date (format YYYY)')

    parser.add_argument(
        '-e', '--end', type=str, metavar='YEAR',
        default=end_dt.strftime('%Y'),
        help='End date (format YYYY)')

    parser.add_argument(
        '-v', '--variables', nargs='+', default=['et', 'eto', 'etr', 'ndvi'], metavar='VAR',
        help='variables')

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    '''
    python compute_zonal_stats.py -ai projects/openet/test2/ssebop/monthly_wrs2 -fi users/bdaudert/nasa-roses/usgs_central_valley_mod_base15_ca_poly_170616_wgs84 -v et -s 2017 -e 2017
    '''
    start_time = time.time()
    ee.Initialize()
    args = arg_parse()
    coll_name = ''
    feat_colls = config.statics['feature_collections'].keys()
    for feat_coll in feat_colls:
        if config.statics['feature_collections'][feat_coll]['feature_collection_name'] == args.feature_collection_id:
                coll_name = feat_coll

    if not coll_name:
        raise Exception('Feature Collection not found in statics.feature_collections')
        sys.exit(1)

    year_start_int = int(args.start[0:4])
    year_end_int = int(args.end[0:4])
    # Loop over months in each year
    for yr_int in range(year_start_int, year_end_int + 1):
        for m_int in range(1, 13):
            # Set start/end date strings for year and month
            sd, ed = set_start_end_date_str(yr_int, m_int)
            # Filter collections by dates
            ee_coll = ee.ImageCollection(args.asset_id).filterDate(sd, ed)
            ee_feat_coll = ee.FeatureCollection(args.feature_collection_id)
            for var in args.variables:
                # Filetr collection by variable
                ee_coll = ee_coll.select(var)
                if var == 'ndvi':
                    temporal_summary = 'mean'
                else:
                    temporal_summary = 'sum'

                ee_img = compute_temporal_summary(ee_coll, temporal_summary)
                data = compute_zonal_stats(ee_img, coll_name, ee_feat_coll)
                data = [[d[0], round(d[1], 4), round(d[2], 4)] for d in data if d[2] is not None]
                print(data)
                print(sd, ed)
    print("--- %s seconds ---" % (str(time.time() - start_time)))
    print("--- %s minutes ---" % (str((time.time() - start_time) / 60.0)))
    print("--- %s hours ---" % (str((time.time() - start_time) / 3600.0)))