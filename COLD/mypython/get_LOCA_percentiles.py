#!/usr/local/pythonenv/csc/bin/python
import numpy as np
from netCDF4 import Dataset
import h5py
import json


def get_lls():
    f_name = 'LOCA_lls.nc'
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
    b = a[np.where(a!=-32768)]
    if b.size == 0:
        return -9999
    else:
        return int(round(np.percentile(b, 5)))

def get_percentiles(num_days, start_doy, latbounds, lonbounds, var_name, years, data_dir, out_file):
    if var_name == 'tmin':
        loca_var_name = 'tasmin'
    if var_name == 'tmax':
        loca_var_name = 'tasmax'
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
    fh = open(out_file, 'w+')
    fh.close()
    lats = np.array([])
    lons = np.array([])
    DOY_DATA = np.array([])
    for year_idx, year in enumerate(years):
        p_year = year - 1
        c_year = year
        print('PROCESSING YEAR ' + str(c_year))
        if not year_change:
            f_name = data_dir + str(c_year) + '.h5'
            year_data = np.divide(np.array(h5py.File(f_name, 'r')[loca_var_name]))
            year_data = year_data[start_doy:end_doy, latli:latui, lonli:lonui]
            if lats.size == 0:
                lats = all_lats[latli:latui]
            if lons.size == 0:
                lons = all_lons[lonli:lonui]
        else:
            # get December data from previous year
            f_name = data_dir + str(p_year) + '.h5'
            last_year_data = np.array(h5py.File(f_name, 'r')[loca_var_name])
            last_year_data = last_year_data[start_doy:365, latli:latui, lonli:lonui]
            # get the valid lats
            if lats.size == 0:
                lats = all_lats[latli:latui]
            if lons.size == 0:
                lons = all_lons[lonli:lonui]
            # this year data
            f_name = data_dir + str(c_year) + '.h5'
            this_year_data = np.array(h5py.File(f_name, 'r')[loca_var_name])
            this_year_data = this_year_data[0:end_doy, latli:latui, lonli:lonui]
            year_data = np.concatenate((last_year_data, this_year_data), axis=0)
        # NOTE: loca temp data stored as Celcius * 100
        nan_idx = np.where(year_data == -32768)
        year_data = np.divide(year_data, 100.0)
        year_data[nan_idx] = -32768
        if DOY_DATA.size == 0:
            DOY_DATA = year_data
        else:
            DOY_DATA = np.concatenate((DOY_DATA, year_data), axis=0)
    print('COMPUTING PERCENTILES')
    PCTLS = np.apply_along_axis(compute_percentile, 0, DOY_DATA)
    print PCTLS.shape
    del DOY_DATA
    write_netcdf(PCTLS, lons, lats, out_file)

########
#M A I N
########
if __name__ == '__main__' :

    LOCA_CMIP5_MODELS = {
        'CNRM-CM5':[1950,2100],
        'HadGEM2-CC':[1950,2100],
        'HadGEM2-ES':[1950,2100],
        #'GFDL-CM3':[1950,2100],
        #'CanESM2':[1950,2100],
        #'MICRO5':[1950,2100],
        #'CESM1-BGC':[1950,2100],
        #'CMCC-CMS':[1950,2100], # missing data e.g. 1950.h5
        'ACCESS1-0':[1950,2100],
        'CCSM4':[1950,2100]
    }
    rcps = ['rcp45', 'rcp85']
    # read_vars('tmin', 2011)
    loca_dir = '/media/DataSets/loca/'
    res_dir = '/RESULTS/loca/'
    years = range(1951, 2006)
    #years = range(1951, 1952)
    num_days = 90
    start_doy = 334
    '''
    latbounds = [39 , 39.2 ]
    lonbounds = [ -119 + 360 , -118.8 + 360] # degrees east ?
    '''
    latbounds = [31, 49]
    lonbounds = [235, 258]

    for model in LOCA_CMIP5_MODELS.keys():
        print('PROCESSING MODEL ' +model)
        for rcp in rcps:
            print('PROCESSING RCP ' + rcp)
            data_dir = loca_dir + model + '/' + rcp + '/'
            for var_name in ['tmin', 'tmax']:
                print('PROCESSING VAR ' + var_name)
                out_file = var_name + '_' + rcp + '_percentiles.nc'
                out_dir = res_dir + model + '/' + rcp + '/'
                get_percentiles(num_days, start_doy, latbounds, lonbounds, var_name, years, data_dir, out_dir + out_file)

