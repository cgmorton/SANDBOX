#!/usr/bin/env python
import numpy as np
from netCDF4 import Dataset
from netcdftime import utime
import json

def get_lls(data_dir, var_name, year):
    f_name = data_dir + var_name + '.' + str(year) + '.nc'
    ds = Dataset(f_name, 'r')
    lats = ds.variables['lat'][:]
    lons = ds.variables['lon'][:]
    return lats, lons

def get_temp_ts(var_name, num_days, start_doy, latbounds, lonbounds, years, data_dir):
    ts_data = []
    year_change = False
    if start_doy + num_days > 365:
        end_doy = num_days - (365 - start_doy)
        doys = np.concatenate((np.arange(start_doy,365), np.arange(0,end_doy)))
        year_change = True
    else:
        end_doy = start_doy + num_days
        doys = np.arange(start_doy, end_doy)

    all_lats, all_lons = get_lls(data_dir, 'tmin', years[0])
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
    for year_idx, year in enumerate(years):
        p_year = year - 1
        c_year = year
        print('PROCESSING YEAR ' + str(year))
        if not year_change:
            f_name = data_dir + var_name + '.' + str(c_year) + '.nc'
            year_data = Dataset(f_name, 'r').variables[var_name][start_doy:end_doy, latli:latui, lonli:lonui]
            if lats.size == 0:
                lats = Dataset(f_name, 'r').variables['lat'][latli:latui]
                num_lats = lats.shape[0]
            if lons.size == 0:
                lons = Dataset(f_name, 'r').variables['lon'][lonli:lonui]
                num_lons = lons.shape[0]
        else:
            # get December data from previous year
            f_name = data_dir + var_name + '.' + str(p_year) + '.nc'
            # Last Year Data
            # FIX ME: How to deal with fill values
            last_year_data = Dataset(f_name, 'r').variables[var_name][start_doy:365, latli:latui, lonli:lonui]
            # get the valid lats
            if lats.size == 0:
                lats = Dataset(f_name, 'r').variables['lat'][latli:latui]
                num_lats = lats.shape[0]
            if lons.size == 0:
                lons = Dataset(f_name, 'r').variables['lon'][lonli:lonui]
                num_lons = lons.shape[0]
            f_name = data_dir + var_name + '.' + str(c_year) + '.nc'
            this_year_data = Dataset(f_name, 'r').variables[var_name][0:end_doy, latli:latui, lonli:lonui]


        year_data = np.concatenate((last_year_data, this_year_data), axis=0)
        del this_year_data, last_year_data
        #Mask the array
        #year_data =  np.ma.masked_where(year_data == 1e20, year_data)
        year_data[year_data == 1e20] = np.nan
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
    # read_vars('tmin', 2011)
    data_dir = '/media/DataSets/livneh/'
    out_dir = 'RESULTS/livneh/'
    years = range(1951, 2012)
    #years = range(1951, 1953)
    num_days = 90
    start_doy = 334
    var_name = 'tmin'
    latbounds = [31, 49]
    lonbounds = [235, 258]
    for var_name in ['tmin', 'tmax']:
        print('PROCESSING VAR ' + var_name)
        ts_data = get_temp_ts(var_name, num_days, start_doy, latbounds, lonbounds, years, data_dir)
        print ts_data
        with open(out_dir + var_name + '_ts_years.json', 'w') as outfile:
            json.dump(ts_data, outfile)
