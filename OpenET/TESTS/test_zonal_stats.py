import datetime as dt
import os

import ee

import config
import utils


def zonal_stats(feature_collection_name, feature_collection_asset_dir, mgrs_tiles, model, variables,
           start_year, end_year, start_month, end_month, data_bucket, data_bucket_sub_dir):
    """
    ET data precomputation of zonal statistics
    and export to Google Cloud bucket as geojson files
    :param feature_collection_name:
    :param feature_collection_asset_dir:
    :param mgrs_tile:
    :param model:
    :param model_asset_id:
    :param variables:
    :param start_year:
    :param end_year:
    :param start_month: string format mm
    :param end_month: string format mm
    :param data_bucket:
    :param data_bucket_sub_dir:
    :return:
    """
    # Only keep these properties from the original feat coll
    props_to_keep = ["OPENET_ID", "MGRS_TILE", "SOURCECODE"]

    def clean_properties(feat):
        return feat.select(ee.List(props_to_keep))

    start_year = int(start_year)
    end_year = int(end_year)
    # Some tiles are in multiple states
    feature_collection_names = utils.get_valid_feature_collections(feature_collection_name, mgrs_tiles)

    for fcn in feature_collection_names:
        ready_task_count = len(utils.get_ee_tasks(states=["READY"]).keys())
        # Set the img, feature and tile collections
        feat_coll_path = os.path.join(feature_collection_asset_dir, fcn)
        feat_coll = ee.FeatureCollection(feat_coll_path)
        # find the appropriate tile list
        valid_tile_list = utils.get_valid_tile_list(fcn, mgrs_tiles)
        # Year loop
        for year in range(int(start_year), int(end_year) + 1):
            if year == int(start_year):
                s_month = int(start_month)
                s_month_str = str(start_month).zfill(2)
            else:
                s_month = 1
                s_month_str = "01"
            if year == int(end_year):
                e_month = int(end_month)
                e_month_str = str(end_month).zfill(2)
            else:
                e_month = 12
                e_month_str = "12"

            print("Processing year {}".format(year))
            # Iterate over the MGRS tiles
            # All of the debug getInfo calls here are not great
            # They should be commented out or removed in the final code
            for tile in valid_tile_list:
                print("TILE: {}".format(tile))
                if tile != "NOTILE":
                    # Set crs and tranform
                    # The UTM zone should be the first two characters (digits) of the MGRS tile
                    feat_coll = ee.FeatureCollection(feat_coll.map(clean_properties))
                    tile_zone = tile[:2]
                    tile_crs = "EPSG:326" + tile_zone
                    zoned_feat_coll = feat_coll.filter(ee.Filter.eq("MGRS_TILE", tile))
                else:
                    try:
                        tile_crs = feat_coll.first().geometry().projection().getInfo()["crs"]
                    except KeyError:
                        raise Exception("Projection of {} can not be determined!".format(feat_coll_path))
                    zoned_feat_coll = feat_coll

                # Set the concatenated image (one image per variable/month)
                img = utils.set_ee_etdata_img(model, variables, tile, tile_crs, year, feature_collection_name,
                                        start_month=s_month, end_month=e_month)
                # Compute the zonal stats
                reduced_feat_coll = utils.ee_compute_etdata(img, zoned_feat_coll, tile_crs)

                # Set the properties from the image for the target tile (not the mosaic) as
                # well as any other desired properties
                extra_props = {
                    model.upper() + "_ZONAL_STATS_LOADED": dt.datetime.now().strftime("%Y-%m-%d")
                }
                reduced_feat_coll = utils.set_feature_properties(reduced_feat_coll, extra_props=extra_props)

                # Upload to bucket as geojson
                file_name = "_meta_".join([fcn, str(year), str(s_month_str), str(e_month_str), tile])
                task_name = "zonal_et_{}_{}_{}_{}".format(model, fcn, tile, str(year))
                utils.ee_export_feat_coll_to_bucket(data_bucket, data_bucket_sub_dir, file_name, reduced_feat_coll,
                                                      task_name, ready_task_count)

                print("ETDATA PRECOMPUTED for {} TILE {}!".format(feature_collection_name, tile))


if __name__ == '__main__':

    ee_credentials = ee.ServiceAccountCredentials(config.ee_account, config.ee_private_key_file_name)
    ee.Initialize(ee_credentials)

    feature_collection_names = ["TX"]
    mgrs_tiles = []
    model = "disalexi"
    variables = ["et", "et_reference", "et_fraction", "ndvi", "precipitation", "count"]
    start_year = "2019"
    end_year = "2019"
    start_month = "01"
    end_month = "12"

    for feature_collection_name in feature_collection_names:
        print("PROCESSING {}".format(' '.join([feature_collection_name, model, start_year, 'to', end_year])))
        data_bucket_sub_dir = os.path.join(config.data_bucket_sub_dir, model, feature_collection_name)
        zonal_stats(feature_collection_name, config.feature_collection_asset_dir, mgrs_tiles, model, variables,
            start_year, end_year, start_month, end_month, config.data_bucket, data_bucket_sub_dir)
