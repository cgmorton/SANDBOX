#!/usr/bin/env python
import numpy as np
from netCDF4 import Dataset
from netcdftime import utime
import json
import h5py

def get_lls():
    f_name = 'LOCA_lls.nc'
    ds = Dataset(f_name, 'r')
    lats = ds.variables['lat'][:]
    lons = ds.variables['lon'][:]
    return lats, lons

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
        #'CNRM-CM5':[1950,2100],
        'HadGEM2-CC':[1950,2100],
        'HadGEM2-ES':[1950,2100],
        'GFDL-CM3':[1950,2100],
        'CanESM2':[1950,2100],
        'MICRO5':[1950,2100],
        'CESM1-BGC':[1950,2100],
        'CMCC-CMS':[1950,2100],
        'ACCESS1-0':[1950,2100],
        'CCSM4':[1950,2100]
    }
    data_dir = '/media/DataSets/loca/'
    years = range(1951, 2012)
    #years = range(1951, 1953)
    num_days = 90
    start_doy = 334
    var_name = 'tmin'
    latbounds = [31, 49]
    lonbounds = [235, 258]
    rcps = ['rcp45', 'rcp85']
    for model in LOCA_CMIP5_MODELS.keys():
        print('PROCESSING MODEL ' + model)
        out_dir = '/media/DataSets/loca/' + model + '/'
        for rcp in rcps:
            data_dir = '/media/DataSets/loca/' + model + '/' + rcp + '/'
            print('PROCESSING RCP ' + rcp)
            for var_name in ['tmin', 'tmax']:
                print('PROCESSING VAR ' + var_name)
                ts_data = get_temp_ts(var_name,rcp, num_days, start_doy, latbounds, lonbounds, years, data_dir)
                print ts_data
                with open(out_dir + var_name + '_' + rcp + '_ts_years.json', 'w') as outfile:
                    json.dump(ts_data, outfile)
