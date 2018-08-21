import sys
import numpy as np
import json
#GEOSPATIAL STUFF
# from osgeo import gdal, osr, ogr
from netCDF4 import Dataset

def get_plotting_data(var_name, rcp, years, data_dir):
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
    return year_sums_all, year_sums_nz, hist_data_all, hist_data_nz
########
#M A I N
########
if __name__ == '__main__' :
    LOCA_CMIP5_MODELS = {
        #'CNRM-CM5':[1950,2100],
        #'HadGEM2-CC':[1950,2100],
        #'HadGEM2-ES':[1950,2100],
        'GFDL-CM3':[1950,2100],
        'CanESM2':[1950,2100],
        'MICRO5':[1950,2100],
        'CESM1-BGC':[1950,2100],
        # 'CMCC-CMS':[1950,2100],
        # 'ACCESS1-0':[1950,2100],
        # 'CCSM4':[1950,2100]
    }
    rcps = ['rcp45', 'rcp85']
    years = range(1951,2012)
    for model in LOCA_CMIP5_MODELS.keys():
        print('PROCESSING MODEL ' + model)
        data_dir = '/media/DataSets/loca/' + model + '/'
        for rcp in rcps:
            print('PROCESSING RCP ' + rcp)
            for var_name in ['tmin', 'tmax']:
                print('PROCESSING VAR ' + var_name)
                year_sums_all, year_sums_nz, hist_data_all, hist_data_nz = get_plotting_data(var_name, rcp,years, data_dir)
                with open(data_dir + var_name + '_' + rcp + '_ind_sums_years.json', 'w') as outfile:
                    json.dump(year_sums_all, outfile)
                '''
                with open(data_dir + var_name + '_' + rcp + '_ind_sums_years_nz.json', 'w') as outfile:
                    json.dump(year_sums_nz, outfile)
                with open(data_dir + var_name + '_' + rcp + '_hist_sum_all_years_and_locs.json', 'w') as outfile:
                    json.dump(hist_data_all, outfile)
                with open(data_dir + var_name + '_' + rcp + '_hist_sum_all_years_and_locs_nz.json', 'w') as outfile:
                    json.dump(hist_data_nz, outfile)
                '''
