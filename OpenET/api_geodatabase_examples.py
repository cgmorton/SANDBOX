import time
from sqlalchemy import create_engine

import config
from db_methods import query_Util




if __name__ == "__main__":

    '''
    Note: WHAT IS IN THE DATABASES RIGHT NOW:
    etdata.dri.edu, db britta_test, schema test (all fake data)
        feature_collection_name: projects/openet/featureCollections/ca_clu_public
        data years: 2014 - 2017
        vars: et, etr, etf, ndvi
    etdata.dri.edu, db britta_test, schema openet_feb26
        feature_collection_name: '/projects/nasa-roses/WBDHU4_Truckee_Carson_Walker_METRIC_NV_apoly_subset
        data years: 2003
        vars: et, etr, eto, ndvi

    '''

    SCHEMA = config.SCHEMA
    DB_USER = config.OPENET_DB_USER
    DB_PASSWORD = config.OPENET_DB_PASSWORD
    DB_PORT = config.OPENET_DB_PORT
    DB_HOST = config.OPENET_DB_HOST
    DB_NAME = config.OPENET_DB_NAME


    db_string = "postgresql+psycopg2://" + DB_USER + ":" + DB_PASSWORD
    db_string += "@" + DB_HOST + ":" + str(DB_PORT) + '/' + DB_NAME
    engine = create_engine(db_string, pool_size=20, max_overflow=0,
                           connect_args={'options': '-csearch_path={}'.format(SCHEMA + ',public')})

    # Set test vars
    model = "ssebop"
    variable = "et"
    user_id = 0
    temporal_resolution = "monthly"

    # LATEST SCHEMA test
    year = '2017'
    fc_name = 'projects/openet/featureCollections/ca_clu_public'
    unique_feat_identifier = 'FID'

    # SCHEMA openet_feb26
    # year = '20013'
    # fc_name = '/projects/nasa-roses/WBDHU4_Truckee_Carson_Walker_METRIC_NV_apoly_subset'
    # unique_feat_identifier = 'OBJECTID'

    QU = query_Util(model, variable, user_id, temporal_resolution, engine, SCHEMA)



    # FIXME: figure out how to run the same function for temporal_summary = raw and others
    '''
    # 1 API call example
    Request monthly time series for a single field that is not associated
    with a user using the feature_id (unique primary key) directly
    '''
    start_time = time.time()
    params = {
        'start_date': year + '-01-01',
        'end_date': year + '-12-31',
        'feature_id': 4,
        'temporal_summary': 'raw'
    }
    if params['temporal_summary'] == 'raw':
        data = QU.api_ex1_raw(**params)
    else:
        data = QU.api_ex1(**params)
    print('EXAMPLE 1')
    print(data)
    print("--- %s seconds ---" % (str(time.time() - start_time)))
    print("--- %s minutes ---" % (str((time.time() - start_time) / 60.0)))
    print("--- %s hours ---" % (str((time.time() - start_time) / 3600.0)))

    '''
    # 2 API call example
    Request mean monthly values for each feature  in a featureCollection
    Note: no spatial summary
    '''

    start_time = time.time()
    params = {
        'feature_collection_name': fc_name,
        'start_date': year + '-01-01',
        'end_date': year + '-3-31',
        'temporal_summary': 'mean'
    }
    if params['temporal_summary'] == 'raw':
        data = QU.api_ex2_raw(**params)
    else:
        data = QU.api_ex2(**params)
    data = QU.api_ex2(**params)
    print('EXAMPLE 2')
    print(data)
    print("--- %s seconds ---" % (str(time.time() - start_time)))
    print("--- %s minutes ---" % (str((time.time() - start_time) / 60.0)))
    print("--- %s hours ---" % (str((time.time() - start_time) / 3600.0)))


    '''
    # 3 API call example 0.44 seconds
    Request monthly time series for a single field from a featureCollection that is selected by metadata;
    feature_metadata_name (feature_id)/feature_metadata_value
    Note: no spatial summary
    '''

    start_time = time.time()
    params = {
        'feature_collection_name': fc_name,
        'feature_metadata_name': unique_feat_identifier,
        'feature_metadata_properties': '2645',
        'start_date': year + '-01-01',
        'end_date': year + '-06-30',
        'temporal_summary': 'raw'
    }
    if params['temporal_summary'] == 'raw':
        data = QU.api_ex3_raw(**params)
    else:
        data = QU.api_ex3(**params)
    print('EXAMPLE 3')
    print(data)
    print("--- %s seconds ---" % (str(time.time() - start_time)))
    print("--- %s minutes ---" % (str((time.time() - start_time) / 60.0)))
    print("--- %s hours ---" % (str((time.time() - start_time) / 3600.0)))


    '''
    # 4 API call example 2.189 seconds, 95.12 seconds for temporal_summary = 'raw'
    Request area averaged max monthly values for all features in a featureCollection for a user
    '''

    start_time = time.time()
    params = {
        'feature_collection_name': fc_name,
        'feature_metadata_name': unique_feat_identifier,
        'start_date': year + '-01-01',
        'end_date': year + '-06-30',
        'temporal_summary': 'sum',
        'spatial_summary': 'mean'
    }

    if params['temporal_summary'] == 'raw':
        data = QU.api_ex4_raw(**params)
    else:
        data = QU.api_ex4(**params)
    print('EXAMPLE 4')
    print(data)
    print("--- %s seconds ---" % (str(time.time() - start_time)))
    print("--- %s minutes ---" % (str((time.time() - start_time) / 60.0)))
    print("--- %s hours ---" % (str((time.time() - start_time) / 3600.0)))


    '''
    # 5 API call example  seconds 0.428
    Request monthly time series for a subset of features in collection defined by list of property values
    '''

    start_time = time.time()
    params = {
        'feature_collection_name': fc_name,
        'feature_metadata_name': unique_feat_identifier,
        'feature_metadata_properties': ('2708', '2640', '2706'),
        'start_date': year + '-01-01',
        'end_date': year + '-12-31',
        'temporal_summary': 'mean'
    }
    if params['temporal_summary'] == 'raw':
        data = QU.api_ex5_raw(**params)
    else:
        data = QU.api_ex5(**params)
    print('EXAMPLE 5')
    print(data)
    print("--- %s seconds ---" % (str(time.time() - start_time)))
    print("--- %s minutes ---" % (str((time.time() - start_time) / 60.0)))
    print("--- %s hours ---" % (str((time.time() - start_time) / 3600.0)))


    '''
    # 6 API call example  0.7411 seconds
    Request time series for subset of features in collection defined by a separate geometry (like bbox or polygon)
    '''

    start_time = time.time()
    params = {
        'feature_collection_name': fc_name,
        'selection_geometry': 'POLYGON((-111.5 42, -111.5 43, -111.4 43, -111.4 42, -111.5 42))',
        'start_date': year + '-01-01',
        'end_date': year + '-12-31',
        'temporal_summary': 'mean'
    }
    if params['temporal_summary'] == 'raw':
        data = QU.api_ex6_raw(**params)
    else:
        data = QU.api_ex6(**params)
    print('EXAMPLE 6')
    print(data)
    print("--- %s seconds ---" % (str(time.time() - start_time)))
    print("--- %s minutes ---" % (str((time.time() - start_time) / 60.0)))
    print("--- %s hours ---" % (str((time.time() - start_time) / 3600.0)))





