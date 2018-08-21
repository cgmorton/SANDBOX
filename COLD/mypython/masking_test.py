#!/usr/local/pythonenv/csc/bin/python
import numpy as np
from netCDF4 import Dataset
import json


def get_lls(data_dir, var_name, year):
    f_name = data_dir + var_name + '.' + str(year) + '.nc'
    ds = Dataset(f_name, 'r')
    lats = ds.variables['lat'][:]
    lons = ds.variables['lon'][:]
    return lats, lons

def write_netcdf(PCTLS, lons, lats, out_file):
    print 'Writing netcdf file'
    DS =  Dataset(out_file, 'w', format='NETCDF3_64BIT')
    DS.description = '''
        At each livneh gridpoint in the Western United States
        (BBOX: : -125, 31, -102, 49.1) the 5th percentile of tmin
        is computed. The base period used was 1951 - 2005
        '''
    #Define the dimensions
    nlons = lons.shape[0] #number of stations
    nlats = lats.shape[0]
    DS.createDimension('latitude', nlats)
    DS.createDimension('longitude', nlons)

    #Define the variables
    '''
    lat = DS.createVariable('lat', 'f4', ('latitude',), fill_value=-9999)
    lon = DS.createVariable('lon', 'f4', ('longitude',), fill_value=-9999)
    pctl = DS.createVariable('percentile', 'i4', ('latitude', 'longitude'), fill_value=-9999)
    pctl.units = 'Deg Celsius'
    '''
    lat = DS.createVariable('lat', 'f4', ('latitude',))
    lon = DS.createVariable('lon', 'f4', ('longitude',))
    pctl = DS.createVariable('percentile', 'i4', ('latitude', 'longitude'))
    pctl.units = 'Deg Celsius'

    #Populate variable
    lat[:] = lats
    lon[:] = lons
    pctl[:,:] = PCTLS
    DS.close()

def compute_percentile(a):
    b = a[np.where(a != 1e20)]
    if b.size == 0:
        return -9999
    else:
        p = int(round(np.percentile(b, 5)))
        return p

def get_percentiles(num_days, start_doy, latbounds, lonbounds, var_name, years, data_dir, out_file):
    year_change = False
    if start_doy + num_days > 365:
        end_doy = num_days - (365 - start_doy)
        doys = np.concatenate((np.arange(start_doy,365), np.arange(0,end_doy)))
        year_change = True
    else:
        end_doy = start_doy + num_days
        doys = np.arange(start_doy, end_doy)

    all_lats, all_lons = get_lls(data_dir, 'tmin', years[0])
    all_lats_rev = all_lats[::1]
    all_lons_rev = all_lons[::-1]
    print all_lats[0], all_lats[-1]
    print all_lons[0], all_lons[-1]
    # latitude lower and upper index
    latli = np.argmin( np.abs( all_lats - latbounds[0] ) )
    latui = np.argmin( np.abs( all_lats_rev - latbounds[1] ) )
    # longitude lower and upper index
    lonli = np.argmin( np.abs( all_lons - lonbounds[0] ) )
    lonui = np.argmin( np.abs( all_lons_rev - lonbounds[1] ) )
    print all_lats[latli], all_lats[latui]
    print all_lons[lonli], all_lons[lonui]
    print('LATS INDICES')
    print latli, latui
    print('LONS INDICES')
    print lonli, lonui
    fh = open(out_file, 'w+')
    fh.close()
    lats = np.array([])
    lons = np.array([])
    DOY_DATA = np.array([])
    for year_idx, year in enumerate(years):
        c_year = year
        p_year = year - 1
        print('PROCESSING YEAR ' + str(year))
        if not year_change:
            f_name = data_dir + var_name + '.' + str(c_year) + '.nc'
            try:
                year_data = Dataset(f_name, 'r').variables[var_name][start_doy:end_doy, latli:latui, lonli:lonui]
                if lats.size == 0:
                    lats = Dataset(f_name, 'r').variables['lat'][latli:latui]
                if lons.size == 0:
                    lons = Dataset(f_name, 'r').variables['lon'][lonli:lonui]
            except:
                # Last year reached
                break
        else:
            # get December data from previous year
            f_name = data_dir + var_name + '.' + str(p_year) + '.nc'
            # Last Year Data
            try:
                # FIX ME: How to deal with fill values
                last_year_data = Dataset(f_name, 'r').variables[var_name][start_doy:365, latli:latui, lonli:lonui]
                #last_year_data = list(Dataset(f_name, 'r').variables[var_name][333:365,lat_idx,lon_idx])
                #last_year_data = [float(v) for v in last_year_data]
                # get the valid lats
                if lats.size == 0:
                    lats = Dataset(f_name, 'r').variables['lat'][latli:latui]
                if lons.size == 0:
                    lons = Dataset(f_name, 'r').variables['lon'][lonli:lonui]
            except:
                # On to next year
                continue

            f_name = data_dir + var_name + '.' + str(c_year) + '.nc'
            try:
                this_year_data = Dataset(f_name, 'r').variables[var_name][0:end_doy, latli:latui, lonli:lonui]
            except:
                # Last year reached
                break


            year_data = np.concatenate((last_year_data, this_year_data), axis=0)
            del this_year_data, last_year_data
            if DOY_DATA.size == 0:
                DOY_DATA = year_data
            else:
                DOY_DATA = np.concatenate((DOY_DATA, year_data), axis=0)
    print('COMPUTING PERCENTILES')

    # NOTE: we need to use np.nanpercentile to deal with invalid data
    # First mask out the fill values
    #print np.ma.is_masked(DOY_DATA)
    '''
    dmiss = DOY_DATA[np.where(DOY_DATA == 1e20)]
    print('MISSING')
    print dmiss.shape
    d =  DOY_DATA[np.where(DOY_DATA != 1e20)]
    print('NOT MISSING')
    print d.shape
    '''

    '''
    DOY_DATA = np.ma.masked_where(DOY_DATA == 1e20, DOY_DATA)
    # Fill masked values with nan values
    DOY_DATA = DOY_DATA.filled(np.nan)

    print np.where(np.isnan(DOY_DATA))
    # PCTLS = np.ma.MaskedArray(np.nanpercentile(DOY_DATA, 5, axis=0, keepdims=True), fill_value=1e20)
    PCTLS = np.ma.array(np.nanpercentile(DOY_DATA, 5, axis=0, keepdims=True))
    del DOY_DATA
    print PCTLS.mask
    PCTLS = PCTLS.filled(1e20)
    print np.where(PCTLS == 1e20)
    write_netcdf(PCTLS, lons, lats, out_file)
    '''

    '''
    PCTLS = np.percentile(DOY_DATA, 5, axis=0, keepdims=True)
    del DOY_DATA
    PCTLS = PCTLS[0,:,:]
    PCTLS = np.ma.masked_where(np.ma.is_masked(PCTLS[:,:]), PCTLS)
    print np.ma.is_masked(PCTLS)
    write_netcdf(PCTLS, lons, lats, out_file)
    '''

    PCTLS = np.apply_along_axis(compute_percentile, 0, DOY_DATA)
    del DOY_DATA
    write_netcdf(PCTLS, lons, lats, out_file)
    return lats, lons, PCTLS

########
#M A I N
########
if __name__ == '__main__' :
    # read_vars('tmin', 2011)
    out_file = 'test_percentiles.nc'
    data_dir = '/media/DataSets/livneh/'
    out_dir = 'RESULTS/livneh/'
    #years = range(1951, 2006)
    years = range(1951, 1954)
    num_days = 90
    start_doy = 334
    var_name = 'tmax'
    latbounds = [31, 49]
    lonbounds = [235, 258]
    lats, lons, PCTLS = get_percentiles(num_days, start_doy, latbounds, lonbounds, var_name, years, data_dir, out_dir + out_file)
    # check_percentiles(out_file)
    print("RESULTS ")
    print PCTLS.shape
    print('MISSING INDICES')
    miss_idx = np.where(PCTLS == -9999)
    lats_miss = lats[miss_idx[0]]
    lons_miss = lons[miss_idx[1]]
    print('MISSING')
    print('LATS')
    print lats_miss.shape
    print(np.min(lats_miss), np.max(lats_miss))
    print('LONS')
    print lons_miss.shape
    print(np.min(lons_miss), np.max(lons_miss))
    print PCTLS[np.where(PCTLS == -9999)].shape
    print('VALID')
    print PCTLS[np.where(PCTLS != -9999)].shape
