#!/usr/bin/python

from netCDF4 import Dataset
import matplotlib.dates as mdates
from matplotlib.mlab import PCA
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import numpy as np
from sklearn.decomposition import PCA
import json
import datetime as dt
from osgeo import gdal, osr, ogr
from scipy.stats.stats import pearsonr

def array_to_raster(lats, lons, data_array, output_path, nodata=-9999):
    '''
    Args:
        lats: list of latitudes
        lons: list of longitues
        data_array: array of values, one per lat, lon combination
        output_path-path: path to output file
        nodata: nodata value
    '''
    ## set spatial res and size of lat, lon grids
    yres = abs(lats[1] - lats[0])
    xres = abs(lons[1] - lons[0])
    output_cols = len(lons)
    output_rows = len(lats)
    ulx = min(lons) - (xres / 2.)
    uly = max(lats) + (yres / 2.)

    ## Build the output raster file
    driver = gdal.GetDriverByName('GTiff')
    output_ds = driver.Create(
        output_path, output_cols, output_rows, 1, gdal.GDT_Float32)

    ## Convert array to float32 and set nodata value
    data_array = data_array.reshape((output_rows,output_cols)).astype(np.float32)
    data_array[data_array == nodata] = np.nan

    ## This assumes the projection is Geographic lat/lon WGS 84
    output_osr = osr.SpatialReference()
    output_osr.ImportFromEPSG(4326)
    output_ds.SetProjection(output_osr.ExportToWkt())
    output_ds.SetGeoTransform([ulx, xres, 0, uly, 0, yres])
    output_band = output_ds.GetRasterBand(1)
    output_band.WriteArray(data_array.reshape((output_rows,output_cols)))
    output_band.SetNoDataValue(float(np.finfo(np.float32).min))
    output_ds = None

def ortho_rotation(lam, method='varimax',gamma=None,
                   eps=1e-6, itermax=100):
    """
    Return orthogal rotation matrix
    TODO: - other types beyond
    """
    if gamma == None:
        if method == 'varimax':
            gamma = 1.0
        if method == 'quartimax':
            gamma = 0.0

    nrow, ncol = lam.shape
    R = np.eye(ncol)
    var = 0

    for i in range(itermax):
        lam_rot = np.dot(lam, R)
        tmp = np.diag(np.sum(lam_rot ** 2, axis=0)) / nrow * gamma
        u, s, v = np.linalg.svd(np.dot(lam.T, lam_rot ** 3 - np.dot(lam_rot, tmp)))
        R = np.dot(u, v)
        var_new = np.sum(s)
        if var_new < var * (1 + eps):
            break
        var = var_new

    return R

def varimax(Phi, gamma=1.0, q=20, tol=1e-6):
    from numpy import eye, asarray, dot, sum, diag
    from numpy.linalg import svd
    p, k = Phi.shape
    R = eye(k)
    d = 0
    for i in xrange(q):
        d_old = d
        Lambda = dot(Phi, R)
        u, s, vh = svd(dot(Phi.T, asarray(Lambda) ** 3 - (gamma / p) *
                       dot(Lambda, diag(diag(dot(Lambda.T, Lambda))))))
        R = dot(u, vh)
        d = sum(s)
        if d / d_old < tol:
            break
    return dot(Phi, R)

def datetime_to_date(dtime, seperator):
    '''
    yyyy-mm-dd
    yyyy/mm/dd
    yyyy:mm:dd
    yyyymmdd
    '''
    if type(dtime) != dt.datetime:
        return '0000' + str(seperator) + '00' + str(seperator) + '00'
    try:y = str(dtime.year)
    except:y = '0000'

    try:m =str(dtime.month)
    except:m = '00'
    if len(m) == 1:m = '0' + m

    try:d =str(dtime.day)
    except:d = '00'
    if len(d) == 1:d = '0' + d
    return y + str(seperator) + m + str(seperator) + d

def advance_date(date_dt, days, back_or_forward):
    if back_or_forward == 'forward':
        d_dt_new = date_dt + dt.timedelta(days=int(days))
    if back_or_forward == 'back':
        d_dt_new = date_dt - dt.timedelta(days=int(days))
    return d_dt_new

def correlate_ts(ts_1, ts_2):
    #corr_coeff, p_value
    return pearsonr(ts_1, ts_2)

def get_LIVNEH_PCA_data(var_name, years, data_dir):
    '''
    Row are time steps, columns are locations.
    For our domain
    PCA_data.shape = (5490, 103968)
    where 5490 = 61 years * 90 days
    and 103968 is number of locations in domain
    '''
    lons = np.array([])
    lats = np.array([])
    PCA_data = []
    for year in years:
        DS = Dataset(data_dir + var_name +  '_5th_Indices_WUSA_' + str(year) + '.nc', 'r')
        if lons.shape[0] == 0:
            lons = np.array([l -360 for l in DS.variables['lon'][:]])
            lats = DS.variables['lat'][:]
            num_lons = lons.shape[0]
            num_lats = lats.shape[0]
        indices = DS.variables['index'][:,:,:]
        doys = DS.variables['doy'][:]
        for doy_idx in range(doys.shape[0]):
            d_list = np.reshape(indices[doy_idx,:,:], num_lons * num_lats)
            d_list[d_list == -9999] = 0
            PCA_data.append(d_list)
    return np.array(PCA_data), lons, lats

def get_LOCA_PCA_data(rcp, var_name, years, data_dir):
    '''
    Row are time steps, columns are locations.
    For our domain
    PCA_data.shape = (5490, 103968)
    where 5490 = 61 years * 90 days
    and 103968 is number of locations in domain
    '''
    lons = np.array([])
    lats = np.array([])
    PCA_data = []
    for year in years:
        DS = Dataset(data_dir + var_name + '_' + rcp +   '_5th_Indices_WUSA_' + str(year) + '.nc', 'r')
        if lons.shape[0] == 0:
            lons = np.array([l -360 for l in DS.variables['lon'][0:-7]])
            lats = DS.variables['lat'][:]
            num_lons = lons.shape[0]
            num_lats = lats.shape[0]
        indices = DS.variables['index'][:,0:num_lats,0:num_lons]
        doys = DS.variables['doy'][:]
        for doy_idx in range(doys.shape[0]):
            d_list = np.reshape(indices[doy_idx,:,:], num_lons * num_lats)
            d_list[d_list == -9999] = 0
            PCA_data.append(d_list)
    return np.array(PCA_data), lons, lats

def scale_linear_bycolumn(rawpoints, high=1.0, low=0.0):
    mins = np.min(rawpoints, axis=0)
    maxs = np.max(rawpoints, axis=0)
    rng = maxs - mins
    print('RNG ' + str(rng))
    print(maxs - rawpoints)
    return high - (((high - low) * (maxs - rawpoints)) / rng)

########
# M A I N
########


if __name__ == '__main__':
    start_time = dt.datetime.now()
    print('Start Time: ' + str(start_time))
    LOCA_CMIP5_MODELS = {
        'CNRM-CM5':[1950,2100],
        'HadGEM2-CC':[1950,2100],
        'HadGEM2-ES':[1950,2100],
        'GFDL-CM3':[1950,2100],
        'CanESM2':[1950,2100],
        'MICRO5':[1950,2100],
        'CESM1-BGC':[1950,2100],
        #'CMCC-CMS':[1950,2100], # missing data
        'ACCESS1-0':[1950,2100],
        'CCSM4':[1950,2100]
    }
    # LOCA_CMIP5_MODELS = {'CNRM-CM5':[1950,2100]}
    rcps = ['rcp45', 'rcp85']
    livneh_years = range(1951, 2012)
    loca_years = range(2006, 2100)
    livneh_data_dir = '/media/DataSets/livneh/'
    for model in LOCA_CMIP5_MODELS.keys():
        for rcp in rcps:
            loca_data_dir = '/media/DataSets/loca/' + model + '/' + rcp + '/'
            for var_name in ['tmin', 'tmax']:
                print('PROCESSSING VARIABLE ' + var_name)
                print('Extracting PCA data from index files')

                LIVNEH_PCA_data, LIVNEH_lons, LIVNEH_lats = get_LIVNEH_PCA_data(var_name, livneh_years, livneh_data_dir)
                LOCA_PCA_data, LOCA_lons, LOCA_lats = get_LOCA_PCA_data(rcp, var_name, loca_years, loca_data_dir)
                num_lons = LIVNEH_lons.shape[0]
                num_lats = LIVNEH_lats.shape[0]
                print('LIVNEH DATA MATRIX ' + str(LIVNEH_PCA_data.shape))
                print('LOCA DATA MATRIX ' + str(LOCA_PCA_data.shape))

                #print('Minutes elapsed: ' + str((dt.datetime.now() - start_time).total_seconds() / 60.0))
                print('Computing component matrix')
                num_comps = 6
                comp_indices = []
                pca = PCA(n_components=num_comps)
                X_pca = pca.fit_transform(LIVNEH_PCA_data) #(5490, 3)
                # Projection of original data onto component space
                corr_array = pca.inverse_transform(X_pca) #(5490, 103968)
                components = pca.components_.transpose() # (103968, 3)
                print('VARIANCE EXPLAINED: ')
                print (pca.explained_variance_ratio_)
                rotated_components = varimax(components).transpose() #(3, 103968)
                dates_dt = []
                dates_ts = []
                for year in loca_years:
                    dates_dt.append(dt.datetime(year,12,1))
                    dates_ts.append(datetime_to_date(dates_dt[-1], '-'))
                    for doy_idx in range(1,90):
                        dates_dt.append(advance_date(dates_dt[-1],1, 'forward'))
                        dates_ts.append(datetime_to_date(dates_dt[-1], '-'))

                rotated_components_list = []
                for c_idx in range(num_comps):
                    print('Working component ' + str(c_idx + 1))
                    print('Finding time series data')
                    # Project LIVNEH PCA components onto the LOCA data
                    ts_data_comp = rotated_components[c_idx].dot(LOCA_PCA_data.T)
                    std_ts_comp =  np.std(ts_data_comp)
                    ts_data_comp /= std_ts_comp
                    corr_coeffs = []
                    corr_p_values= []
                    for loc_idx  in range(LOCA_PCA_data.shape[1]):
                        corr = correlate_ts(LOCA_PCA_data[:,loc_idx], ts_data_comp)
                        corr_coeffs.append(corr[0])
                        corr_p_values.append(corr[1])
                    corr_coeffs = np.array(corr_coeffs)
                    corr_coeffs[np.isnan(corr_coeffs)] = 0
                    # Make all time series positive
                    if np.sum(ts_data_comp) < 0:
                        ts_data_comp = np.multiply(ts_data_comp, -1)
                        corr_coeffs = np.multiply(corr_coeffs, -1)
                    ts_data_corr = corr_coeffs.dot(LOCA_PCA_data.T)
                    std_ts_corr = np.std(ts_data_corr)
                    ts_data_corr /= std_ts_corr
                    hc_ts_data_comp = []
                    hc_ts_data_corr = []
                    for i in range(ts_data_comp.shape[0]):
                        hc_ts_data_comp.append([dates_ts[i], round(ts_data_comp[i], 4)])
                        hc_ts_data_corr.append([dates_ts[i], round(ts_data_corr[i], 4)])
                    # Save time series data
                    with open(loca_data_dir + var_name + '_' + rcp + '_2006_2099_pca_component_' + str(c_idx+1) + '_ts.json', 'w') as outfile:
                        json.dump(hc_ts_data_comp, outfile)
                    with open(loca_data_dir + var_name + '_' + rcp + '_2006_2099_pca_correlation_' + str(c_idx+1) + '_ts.json', 'w') as outfile:
                        json.dump(hc_ts_data_corr, outfile)
                    print('Saving tif')
                    #Generate tif for component
                    out_file = loca_data_dir + var_name + '_' + rcp + '_2006_2099_pca_component_' + str(c_idx + 1) + '.tif'
                    array_to_raster(LOCA_lats, LOCA_lons, rotated_components[c_idx] * std_ts_comp, out_file, nodata=-9999)
                    out_file = loca_data_dir + var_name + '_' + rcp + '_2006_2099_pca_correlation_' + str(c_idx + 1) + '.tif'
                    array_to_raster(LOCA_lats, LOCA_lons, corr_coeffs, out_file, nodata=-9999)
