import sys
import numpy as np
import json
#GEOSPATIAL STUFF
# from osgeo import gdal, osr, ogr
import h5py
from netCDF4 import Dataset
from scipy.stats import linregress
from scipy.stats.stats import pearsonr

def get_lls():
    f_name = 'LOCA_lls.nc'
    ds = Dataset(f_name, 'r')
    lats = ds.variables['lat'][:]
    lons = ds.variables['lon'][:]
    return lats, lons

def correlate_two(var_name, rcp, data_dir):
    with open(data_dir + var_name + '_' + rcp + '_ts_years.json') as f:
        var_ts_data = json.load(f)
    with open(data_dir + var_name + '_' + rcp + '_ind_sums_years.json') as f:
        index_ts_data = json.load(f)
    #return pearsonr(var_ts_data, index_ts_data)
    #  slope, intercept, r_value, p_value, std_err
    reg = linregress(var_ts_data, index_ts_data)
    #p_val, r_val, intercept, slope
    return reg[3], reg[2], reg[1], reg[0]

def correlate_one(data):
    x_data = data[0]
    y_data = data[1]
    reg = linregress(x_data, y_data)
    #p_val, r_val, intercept, slope
    return reg[3], reg[2], reg[1], reg[0]

def get_index_ts(var_name, rcp, years, data_dir):
    year_sums_all = []
    year_sums_nz = []
    data_all = np.array([])
    hist_data = np.array([])
    # fort box plots
    ll_data = {}
    count = 0
    for yr_idx, year in enumerate(years):
        net_file = data_dir + var_name + '_' + rcp + '_5th_Indices_WUSA_' + str(year) + '.nc'
        print net_file
        try:
            ds = Dataset(net_file, 'r')
        except:
            year_sums_all.append(None)
            continue
        lats = ds.variables['lat'][:]
        lons = ds.variables['lon'][:]
        doys = ds.variables['doy'][:]
        num_doys = doys.shape[0]
        num_lons = lons.shape[0]
        num_lats = lats.shape[0]
        if lats.size != 0  and lons.size != 0 and hist_data.size == 0:
            data_all = np.empty([len(years), num_doys, lats.shape[0], lons.shape[0]])
            hist_data = np.empty([len(years), lats.shape[0], lons.shape[0]])
        data_all[yr_idx] = ds.variables['index'][:,:,:]
        hist_data[yr_idx] = np.sum(data_all[yr_idx], axis=0)

    # HIST DATA
    # Average over all years
    hist_data = np.mean(hist_data, axis=0)
    d = np.reshape(hist_data, num_lats * num_lons)
    d_nz = d[np.where(d > 0)]
    hist_data_all = [round(v,4) for v in list(d)]
    hist_data_nz = [round(v,4) for v in list(d_nz)]
    # SUMS TIME SERIES
    for yr_idx, year in enumerate(years):
        # Sum over season
        s = np.sum(data_all[yr_idx], axis=0)
        # Ave over grid points
        s = np.reshape(s, num_lats * num_lons)
        s_nz = s[s > 0]
        year_sums_all.append(round(np.mean(s), 4))
        year_sums_nz.append(round(np.mean(s_nz), 4))
    return year_sums_all

def get_temp_ts(var_name,rcp, num_days, start_doy, latbounds, lonbounds, years, data_dir):
    ts_data = []
    year_change = False
    if start_doy + num_days > 365:
        end_doy = num_days - (365 - start_doy)
        doys = np.concatenate((np.arange(start_doy,365), np.arange(0,end_doy)))
        year_change = True
    else:
        end_doy = start_doy + num_days
        doys = np.arange(start_doy, end_doy)

    all_lats, all_lons = get_lls()
    # latitude lower and upper index
    latli = np.argmin( np.abs( all_lats - latbounds[0] ) )
    latui = np.argmin( np.abs( all_lats - latbounds[1] ) )
    # longitude lower and upper index
    lonli = np.argmin( np.abs( all_lons - lonbounds[0] ) )
    lonui = np.argmin( np.abs( all_lons - lonbounds[1] ) )
    lats = np.array([])
    lons = np.array([])
    num_lats = 0
    num_lons = 0
    if var_name == 'tmin':
        loca_var_name = 'tasmin'
    if var_name == 'tmax':
        loca_var_name = 'tasmax'
    for year_idx, year in enumerate(years):
        p_year = year - 1
        c_year = year
        print('PROCESSING YEAR ' + str(year))
        if not year_change:
            f_name = data_dir + str(c_year) + '.h5'
            year_data = np.array(h5py.File(f_name, 'r')[loca_var_name])
            year_data = year_data[start_doy:end_doy, latli:latui, lonli:lonui]
            if lats.size == 0:
                lats = all_lats[latli:latui]
                num_lats = lats.shape[0]
            if lons.size == 0:
                lons = all_lons[lonli:lonui]
                num_lons = lons.shape[0]
        else:
            f_name = data_dir + str(p_year) + '.h5'
            last_year_data = np.array(h5py.File(f_name, 'r')[loca_var_name])
            last_year_data = last_year_data[start_doy:365, latli:latui, lonli:lonui]
            # get the valid lats
            if lats.size == 0:
                lats = all_lats[latli:latui]
                num_lats = lats.shape[0]
            if lons.size == 0:
                lons = all_lons[lonli:lonui]
                num_lons = lons.shape[0]
            f_name = data_dir + str(c_year) + '.h5'
            this_year_data = np.array(h5py.File(f_name, 'r')[loca_var_name])
            this_year_data = this_year_data[0:end_doy, latli:latui, lonli:lonui]
            year_data = np.concatenate((last_year_data, this_year_data), axis=0)

            year_data = np.concatenate((last_year_data, this_year_data), axis=0)
            del this_year_data, last_year_data
        #NOTE: loca temps are stored as Celcius * 100
        '''
        nan_idx = np.where(year_data == -32768)
        year_data = np.divide(year_data, 100.0)
        year_data[nan_idx] = -32768
        nan_idx = np.where(year_data == -32768)
        non_nan_idx = np.where(year_data != -32768)
        mask = year_data
        mask[nan_idx] = 1
        mask[non_nan_idx] = 0
        mx = np.ma.masked_array(year_data, mask)
        mean_temps = np.apply_along_axis(np.mean, 0, mx)
        '''
        year_data = year_data.astype(float)
        # LOCA temps stored as Celcius * 100
        year_data = np.divide(year_data, 100.0)
        year_data[np.absolute(year_data + 32768 / 100.0) < 0.0001] = np.nan
        #Ave over season
        mean_temps = np.apply_along_axis(np.nanmean, 0, year_data)
        #Ave over Lon/Lats
        mean_temps = np.reshape(mean_temps,(num_lats*num_lons,))
        # meat_temps = np.mean(np.ma.compressed(mean_temps))
        ts_data.append(round(np.nanmean(mean_temps), 4))
    return ts_data



########
#M A I N
########
if __name__ == '__main__' :
    LOCA_CMIP5_MODELS = {
        'CNRM-CM5':[1950,2100],
        'HadGEM2-CC':[1950,2100],
        'HadGEM2-ES':[1950,2100],
        'GFDL-CM3':[1950,2100],
        'CanESM2':[1950,2100],
        'MICRO5':[1950,2100],
        'CESM1-BGC':[1950,2100],
        # 'CMCC-CMS':[1950,2100],
        'ACCESS1-0':[1950,2100],
        'CCSM4':[1950,2100]
    }
    rcps = ['rcp45', 'rcp85']
    # years = range(1951,2012)
    years = range(2006, 2100)
    num_days = 90
    start_doy = 334
    var_name = 'tmin'
    latbounds = [31, 49]
    lonbounds = [235, 258]
    # for model in LOCA_CMIP5_MODELS.keys():
    for model in LOCA_CMIP5_MODELS:
        print('PROCESSING MODEL ' + model)
        for rcp in rcps:
            data_dir = '/media/DataSets/loca/' + model + '/' + rcp + '/'
            print('PROCESSING RCP ' + rcp)
            for var_name in ['tmin', 'tmax']:
                json_data = {}
                print('PROCESSING VAR ' + var_name)
                # Get index and variable data
                json_data['index_ts_data'] = get_index_ts(var_name, rcp,years, data_dir)
                json_data['var_ts_data'] = get_temp_ts(var_name,rcp, num_days, start_doy, latbounds, lonbounds, years, data_dir)
                # Get regression data
                ind_reg_data = [range(len(json_data['index_ts_data'])), json_data['index_ts_data']]
                ind_p_val, ind_r_val, ind_intercept, ind_slope = correlate_one(ind_reg_data)
                json_data['index_reg_data'] = [ind_p_val, ind_r_val, ind_intercept, ind_slope]
                var_reg_data = [range(len(json_data['var_ts_data'])), json_data['var_ts_data']]
                var_p_val, var_r_val, var_intercept, var_slope = correlate_one(var_reg_data)
                json_data['var_reg_data'] = [var_p_val, var_r_val, var_intercept, var_slope]
                with open(data_dir + var_name + '_' + rcp + '_2006_2100_timeseries.json', 'w') as outfile:
                    json.dump(json_data, outfile)
