#! /anaconda3/envs/assets/bin/python
import logging
import os
import json
import numpy as np
import datetime as dt


import netCDF4

import config

def get_data_index(lons, lats, lon, lat):
    '''
    find indices of lon, lat in lon, lat arrays
    '''
    lon_idx = np.argmin(np.abs(lons - lon))
    lat_idx = np.argmin(np.abs(lats - lat))
    return lon_idx, lat_idx

def set_nc_vars(var_name, nc_var_keys):
    if var_name != 'precip':
        nc_var = list(nc_var_keys)[0]
    else:
        nc_var = list(nc_var_keys)[-1]
    if var_name not in ['Tmin', 'Tmax', 'SoilMoist']:
        nc_lat = 'Lat'
        nc_lon = 'Lon'
    else:
        nc_lat = 'lat'
        nc_lon = 'lon'
    return nc_var, nc_lon, nc_lat

def set_year_data(doy, point_data):
    doy_idx = doy - 1
    year_data = []
    while doy_idx < point_data.shape[0]:
        year_data.append(point_data[doy_idx])
        doy_idx += 365
    return np.array(year_data)

def get_stats(year_data):
    # NOTE npfloat32 is not json serializable
    stats = {
        'min': round(np.float64(np.nanmin(year_data)), 4),
        'max': round(np.float64(np.nanmax(year_data)), 4),
        'mean': round(np.float64(np.nanmean(year_data)), 4),
        'median': round(np.float64(np.nanmedian(year_data)), 4),
        'std': round(np.float64(np.nanstd(year_data)), 4)
    }
    return stats


if __name__ == '__main__':
    # Loop over sample points
    for p_name in list(config.sample_points.keys()):
        p_info = config.sample_points[p_name]
        ll = str(p_info['lon']) + ', ' + str(p_info['lat'])
        print('Processing Point: ' + p_info['name'] + ': ' + ll)
        outfile_name = p_name + '_doy_data.json'
        json_data = {}
        # Loop over variables
        for var_name in list(config.variables.keys()):
            var_info = config.variables[var_name]
            print('Processing Variable: ' + var_info['name'])
            start_year_str = str(var_info['start'])
            end_year_str = str(var_info['end'])
            in_file_name = config.infile_format.format(
                variable=var_name, start=start_year_str, end=end_year_str)
            infile = os.path.join(config.livneh_data_dir, in_file_name)
            ds = netCDF4.Dataset(infile, 'r')
            nc_var, nc_lon, nc_lat = set_nc_vars(var_name, ds.variables.keys())
            lon_idx, lat_idx = get_data_index(
                ds.variables[nc_lon][:], ds.variables[nc_lat][:],
                p_info['lon'], p_info['lat'])
            if var_name != 'SoilMoist':
                json_data[var_name] = []
                point_data = ds.variables[nc_var][:, lat_idx, lon_idx]
                point_data[point_data == config.indata_fill_value] = np.nan
                for doy_idx in range(365):
                    doy = doy_idx + 1
                    year_data = set_year_data(doy, point_data)
                    stats = get_stats(year_data)
                    json_data[var_name].append(stats)
            else:
                # Soil Moisture data at multiple levels
                # Loop over levels
                for lev_idx in range(ds.variables['lev'].shape[0]):
                    point_data = ds.variables[nc_var][:, lev_idx, lat_idx, lon_idx]
                    new_var_name = var_name + '_' + str(ds.variables['lev'][lev_idx])
                    json_data[new_var_name] = []
                    for doy_idx in range(365):
                        doy = doy_idx + 1
                        year_data = set_year_data(doy, point_data)
                        stats = get_stats(year_data)
                        json_data[new_var_name].append(stats)
        outfile = os.path.join(config.out_dir, outfile_name)
        with open(outfile, 'w') as outfile:
            json.dump(json_data, outfile)


