from calendar import monthrange
import datetime as dt
import os
import pprint
from time import sleep

import ee

from tiles_by_state import fbt as tiles_by_state


def get_ee_tasks(states=['RUNNING', 'READY']):
    """Return current active user tasks in ee
    Parameters
    ----------
    states : list
    Returns
    -------
    dict : Task descriptions (key) and task IDs (value).
    """

    print('  Active Tasks')
    tasks = {}
    for i in range(1, 10):
        try:
            task_list = ee.data.getTaskList()
            task_list = sorted([
                [t['state'], t['description'], t['id']]
                for t in task_list if t['state'] in states])
            tasks = {t_desc: t_id for t_state, t_desc, t_id in task_list}
            break
        except Exception as e:
            print(' Error getting active task list, retrying ({}/10)\n {}'.format(i, e))
            sleep(i ** 2)
    return tasks


# # CGM - This is more confusing as a utility function than just having this one line in the main
# def ee_compute_etdata(img, featColl, tile_crs, tile_transform=[30, 0, 15, 0, -30, 15]):
#     '''
#     Apply a reducer over the area of each feature in the given feature collection.
#     :param img: Earth Engine Imgage
#     :param featColl: Earth Engine FeatureCollection
#     :return: Earth Engine FeatureCollection
#     '''
#     ee_reducedFeatColl = ee.Image(img).reduceRegions(
#         reducer=ee.Reducer.mean(),
#         collection=featColl,
#         crs=tile_crs,
#         crsTransform=tile_transform,
#         # scale=30
#     )
#     return ee_reducedFeatColl


def delay_task(delay_time=0, task_max=-1, task_count=0):
    """Delay script execution based on number of READY tasks
    Parameters
    ----------
    delay_time : float, int
        Delay time in seconds between starting export tasks or checking the
        number of queued tasks if "ready_task_max" is > 0.  The default is 0.
        The delay time will be set to a minimum of 10 seconds if
        ready_task_max > 0.
    task_max : int, optional
        Maximum number of queued "READY" tasks.
    task_count : int
        The current/previous/assumed number of ready tasks.
        Value will only be updated if greater than or equal to ready_task_max.
    Returns
    -------
    int : ready_task_count
    """
    if task_max > 3000:
        raise ValueError('The maximum number of queued tasks must be less than 3000')

    # Force delay time to be a positive value since the parameter used to
    #   support negative values
    if delay_time < 0:
        delay_time = abs(delay_time)

    if ((task_max is None or task_max <= 0) and (delay_time >= 0)):
        # Assume task_max was not set and just wait the delay time
        print('Pausing {} seconds, not checking task list'.format(delay_time))
        sleep(delay_time)
        return 0
    elif task_max and (task_count < task_max):
        # Skip waiting or checking tasks if a maximum number of tasks was set
        #   and the current task count is below the max
        print('Ready tasks: {}'.format(task_count))
        return task_count

    # If checking tasks, force delay_time to be at least 10 seconds if
    #   ready_task_max is set to avoid excessive EE calls
    delay_time = max(delay_time, 10)

    # Make an initial pause before checking tasks lists to allow
    #   for previous export to start up
    # CGM - I'm not sure what a good default first pause time should be,
    #   but capping it at 30 seconds is probably fine for now
    print(f'  Pausing {min(delay_time, 30)} seconds for tasks to start')
    sleep(delay_time)

    # If checking tasks, don't continue to the next export until the number
    #   of READY tasks is greater than or equal to "ready_task_max"
    while True:
        ready_task_count = len(get_ee_tasks(states=['READY']).keys())
        print(f'  Ready tasks: {ready_task_count}')
        if ready_task_count >= task_max:
            print(f'  Pausing {delay_time} seconds')
            sleep(delay_time)
        else:
            print(f'  {task_max - ready_task_count} open task '
                          f'slots, continuing processing')
            break

    return ready_task_count


def ee_export_feat_coll_to_bucket(bucket, bucket_sub_dir, file_name, featColl,
                                  task_name, current_task_count):
    '''
    Uploads featureColl to Google Cloud storage bucket as geojson
    :param bucket_dir:
    :param bucket_sub_dir:
    :param file_name:
    :param featColl:
    :return:
    '''
    prefix = os.path.join(bucket_sub_dir, file_name)
    print('EE Export {}/{}'.format(bucket, prefix))
    task = ee.batch.Export.table.toCloudStorage(
        description=task_name,
        collection=featColl,
        bucket=bucket,
        fileNamePrefix=prefix,
        fileFormat='GeoJSON',
    )
    ready_task_count = current_task_count
    while ready_task_count >= 2995:
        ready_task_count = delay_task(delay_time=0, task_max=3000, task_count=ready_task_count)

    """Make an exponential backoff Earth Engine request"""
    for i in range(1, 10):
        try:
            task.start()
            break
        except Exception as e:
            print('    Resending query ({}/10)'.format(i))
            print('    {}'.format(e))
            sleep(i ** 2)

    task_status = dict(task.status())
    task_state = task_status['state']
    print('Upload task submitted. Task state is ' + task_state)


def get_valid_feature_collections(feature_collection_name, mgrs_tiles):
    """
    # Note: a mgrs_tile can be in more than one feature_collection
    # Find the feature collections that contain features intersecting the tile
    :param feature_collection_name:
    :param mgrs_tiles:
    :return: List
    """
    feature_collection_names = []
    if feature_collection_name and not  mgrs_tiles:
        feature_collection_names.append(feature_collection_name)
    elif not feature_collection_name and mgrs_tiles:
        feature_collection_names = []
        for tile in mgrs_tiles:
            feature_collection_names += [
                fcn for fcn in tiles_by_state.keys()
                if tile in tiles_by_state[fcn]]
    elif feature_collection_name and mgrs_tiles:
        tiles_in_fcn = [t for t in mgrs_tiles
                        if t in tiles_by_state[feature_collection_name]]
        if tiles_in_fcn:
            feature_collection_names.append(feature_collection_name)
    return feature_collection_names


def get_valid_tile_list(feature_collection_name, mgrs_tiles):
    """
    :param feature_collection_name: String: state name
    :param mgrs_tiles: List of mgrs tiles or NOne
    :return:
    """
    if not feature_collection_name and not mgrs_tiles:
        return []
    elif feature_collection_name and not mgrs_tiles:
        if feature_collection_name in tiles_by_state.keys():
            valid_tile_list = tiles_by_state[feature_collection_name]
        else:
            valid_tile_list = ["NOTILE"]
    elif not feature_collection_name and mgrs_tiles:
        return list(mgrs_tiles)
    else:
        if feature_collection_name in tiles_by_state.keys():
            valid_tile_list = [t for t in mgrs_tiles
                               if t in tiles_by_state[feature_collection_name]]
        else:
            valid_tile_list = ["NOTILE"]
    return valid_tile_list


def set_feature_properties(featColl, extra_props=[], props_to_delete=[]):
    """
    Maps over features in featColl to set and/or delete properties
    :param featColl: Earth Engine FeatureCollection
    :param extra_props: dict, properties to add
    :param props_to_delete: dict, properties to delete
    :return:
    """
    def remove_properties(feat, properties):
        all_properties = feat.propertyNames()
        p_filter = ee.Filter.inList("item", properties).Not()
        selectProperties = all_properties.filter(p_filter)
        return feat.select(selectProperties)

    def set_properties(feature):
        feat = feature.transform("EPSG:4326")
        # Remove unwanted properties:
        if props_to_delete:
            feat = remove_properties(feat, props_to_delete)

        # Set additional properties
        props = {}
        if extra_props:
            props.update(extra_props)
        if props:
            return feat.set(props)
        else:
            return feat

    if extra_props or props_to_delete:
        return ee.FeatureCollection(featColl.map(set_properties))
    else:
        return featColl


# CGM - This ends up recursively calling set_model_collection, is that bad?
def set_et_fraction_coll(model, feature_collection_name, start_date, end_date, tile_ftr):
    et_reference_coll = set_model_collection(
        None, "et_reference", feature_collection_name, start_date, end_date, tile_ftr)
    et_coll = set_model_collection(
        model, "et", feature_collection_name, start_date, end_date, tile_ftr)

    def add_et_fraction(img):
        img_date = ee.Date(img.get('system:time_start'))
        next_date = img_date.advance(1, 'day')
        et_reference_img = et_reference_coll.filterDate(img_date, next_date).first()
        return img.select(['et'], ['et_fraction']).divide(et_reference_img)\
            .copyProperties(img, ["system:time_start"]).copyProperties(img)

    # Copmpute et_fraction
    et_fraction_coll = ee.ImageCollection(et_coll.map(add_et_fraction).select("et_fraction"))
    return et_fraction_coll


def set_model_collection(model, variable, feature_collection_name,
                         start_date, end_date, tile_ftr, tile_buffer=10000):
    # mgrs_feat_coll_name = "projects/openet/mgrs/mgrs_region"
    et_reference_coll_name = "projects/openet/reference_et/gridmet/monthly"
    et_reference_band_name = 'eto'
    ndvi_coll_name = "projects/openet/ndvi/conus_gridmet/monthly_provisional"
    precipitation_coll_name = "IDAHO_EPSCOR/GRIDMET"
    precipitation_band_name = 'pr'
    if feature_collection_name == "CA":
        et_coll_name = "projects/openet/{}/california_cimis/monthly_provisional".format(model)
    else:
        et_coll_name = "projects/openet/{}/conus_gridmet/monthly_provisional".format(model)

    # Set a bounding geometry based on the tile
    tile_buffer_geom = tile_ftr.geometry().buffer(tile_buffer)

    def scale_collection(collection, variable):
        sf = "scale_factor_{}".format(variable)

        def scale_func(img):
            scale_factor = ee.Number(img.get(sf))
            return img.multiply(scale_factor)\
                .select([0], [variable])\
                .copyProperties(img, ["system:time_start"])\
                .copyProperties(img)

        return collection.map(scale_func)

    if not model and variable in ["et", "et_fraction", "count"]:
        error = "Variable {} is model dependent!"
        print(error)
        raise Exception(error)

    # NOTE: ndvi, precipitation, et_reference are model independent
    if variable in ["et", "count"]:
        ee_coll = ee.ImageCollection(et_coll_name)\
            .filterDate(start_date, end_date)\
            .filterBounds(tile_buffer_geom).select(variable)
    elif variable == "et_reference":
        ee_coll = ee.ImageCollection(et_reference_coll_name)\
            .filterDate(start_date, end_date) \
            .select([et_reference_band_name], [variable])
    elif variable == "et_fraction":
        # Compute et_fraction: et / et_reference from  monthly data
        ee_coll = set_et_fraction_coll(
            model, feature_collection_name,  start_date, end_date, tile_ftr)
    elif variable == "ndvi":
        ee_coll = ee.ImageCollection(ndvi_coll_name)\
            .filterDate(start_date, end_date)\
            .filterBounds(tile_buffer_geom)
    elif variable == "precipitation":
        ee_coll = ee.ImageCollection(precipitation_coll_name)\
            .filterDate(start_date, end_date)\
            .select([precipitation_band_name], [variable])

    # Apply scale factor
    if variable not in ["precipitation", "et_fraction", "et_reference"]:
        ee_coll = scale_collection(ee_coll, variable)

    return ee_coll


def set_ee_etdata_img(model, variables, tile_ftr, tile_crs, year,
                      feature_collection_name, start_month=1, end_month=12,
                      tile_transform=[30, 0, 15, 0, -30, 15], tile_buffer=10000):
    """
    Filters model collection by variable and by monthly and annual time steps in year and applies statistic
    to create an Image where each band corresponds to one time step
    :param model_asset_id: EE ImageCollection for the model
    :param variables: list
    :param tile_crs: ee.Feature
    :param tile_crs: string
    :param year: int
    :param start_month: int
    :param end_month: int
    :param tile_transform: list
    :param tile_buffer: int
    :return: Earth Engine Image
    """

    tile_buffer_geom = tile_ftr.geometry().buffer(tile_buffer)

    # TODO: Simplify this section using the calendar range module or EE date objects
    last_month_idx = end_month + 1
    now = dt.datetime.now()
    current_year = int(now.year)
    current_month = int(now.month)
    current_day = int(now.day)
    current_month_length = monthrange(current_year, current_month)[1]

    # Make sure we do not extend into the future
    if str(current_year) == str(year):
        if last_month_idx > current_month or (last_month_idx == current_month and current_day != current_month_length):
            # We are in future
            if current_day < int(current_month_length) and current_month > 1:
                last_month_idx = current_month - 1
            else:
                # Current month is Jan or current_day is end of month
                last_month_idx = current_month

    # Build multi band image
    ee_img_list = []
    for variable in variables:
        # Monthly data
        for m_int in range(start_month, last_month_idx):
            band = "{}_m{}".format(variable, str(m_int).zfill(2))
            sd = "{}-{}-01".format(year, str(m_int).zfill(2))
            month_length = str()
            ed = "{}-{}-{}".format(year, str(m_int).zfill(2), monthrange(int(year), m_int)[1])

            ee_coll = set_model_collection(
                model, variable, feature_collection_name, sd, ed, tile_ftr)

            # CGM - Is this just a .rename()?
            coll = ee_coll.select([variable], [band])

            # CGM - Test out reprojecting all of the calls to the target MGRS transform
            if variable == "precipitation":
                ee_img = ee.Image(coll.sum())
                ee_img = ee_img.resample("bilinear")
                # ee_img = ee_img.resample("bilinear").reproject(crs=tile_crs, scale=30)
            else:
                ee_img = coll.mosaic()
                # ee_img = coll.mosaic().reproject(crs=tile_crs, scale=30)
            ee_img = ee_img.reproject(crs=tile_crs, crsTransform=tile_transform)

            # FIXME: requested by charles but is it needed?
            ee_img = ee_img.clip(tile_buffer_geom)

            ee_img_list.append(ee_img)

        # Annual data
        if start_month == 1 and end_month == 12:
            band = "{}_annual".format(variable)
            sd = str(year) + "{}-01-01".format(year)
            ed = str(year) + "{}-12-31".format(year)
            ee_coll = set_model_collection(
                model, variable, feature_collection_name, sd, ed, tile_ftr)
            coll = ee_coll.select([variable], [band])

            if variable in ["ndvi", "et_fraction"]:
                ee_img = ee.Image(coll.mean())
            else:
                ee_img = ee.Image(coll.sum())

            if variable == "precipitation":
                ee_img = ee_img.resample("bilinear")
                # ee_img = ee_img.resample("bilinear").reproject(crs=tile_crs, scale=30)

            # CGM - Test out reprojecting all of the calls to the target MGRS transform
            ee_img = ee_img.reproject(crs=tile_crs, crsTransform=tile_transform)

            ee_img_list.append(ee_img)

    # Combine images into one multi-band image
    ee_img = ee.Image.cat(ee_img_list)

    return ee_img
