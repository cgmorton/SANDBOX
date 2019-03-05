import datetime as dt
import argparse
import ee

import config

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

def compute_zonal_stats(self, ee_img, feat_coll, add_to_db=False):
    '''
    Apply a reducer over the area of each feature in the given feature collection.
    e.g. fromFT = ee.FeatureCollection('ft:1tdSwUL7MVpOauSgRzqVTOwdfy17KDbw-1d9omPw')
    :param ee_img:
    :param feat_col:
    :return: dict zonal_stats
    '''

    def set_aadata(feature):
        unique_col = config.statics.feature_collections['unique_column']
        name = feature.get(unique_col)
        area = feature.area()
        mean = feature.get('mean')
        feature = feature.set({'aa_data': [name, area, mean]})
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
        scale=int(self.tv['scale']),
        tileScale=1,
        crs='EPSG:4326'
    )

    ee_reducedFeatColl = ee_reducedFeatColl.map(set_aadata)
    aa_datas = ee_reducedFeatColl.aggregate_array('aa_data').getInfo()

    if add_to_db:
        # FIXME: call populate_db with arguments
        msg = 'Succesfullay added data to datastore: ' + str(aa_datas)
    return aa_datas


def arg_parse():
    """"""
    end_dt = dt.datetime.today()

    parser = argparse.ArgumentParser(
        description='Compute Zonal Statistics',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-assetid', '--asset-id', type=str, required=True,
        metavar='Earth Engine asset ID', default='',
        help='Set the asset ID')

    parser.add_argument(
        '-fcollid', '--feature-collection-id', type=str, required=True,
        metavar='Earth Engine feature collection ID', default='',
        help='Set the feature collection ID')

    parser.add_argument(
        '-s', '--start', metavar='YEAR',
        default=(end_dt - dt.timedelta(days=365)).strftime('%Y'),
        help='Start date (format YYYY)')
    parser.add_argument(
        '-e', '--end', metavar='YEAR',
        default=end_dt.strftime('%Y'),
        help='End date (format YYYY)')

    parser.add_argument(
        '-ts', '--temporal-summary', type=str, required=True,
        metavar='Temporal Summary', default='',
        help='Set the temporal summary')

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    args = arg_parse()

    # FIXME:
    ee_coll = ee.ImageCollection(args.asset_id).filterDate(args.start, args.end)
    ee_img = compute_temporal_summary(ee_coll)
    feat_coll = ee.FeatureCollection('ft:' + args.feature_collection_id)
    data = compute_zonal_stats(ee_img, feat_coll, add_to_db=False)
    print(data)