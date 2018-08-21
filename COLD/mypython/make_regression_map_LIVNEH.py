from netCDF4 import Dataset
import numpy as np
import json
from osgeo import gdal, osr, ogr
from scipy.stats import linregress

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
    output_band.WriteArray(data_array)
    output_band.SetNoDataValue(float(np.finfo(np.float32).min))
    output_ds = None


def line_regress(data):
    d = data[np.absolute(data + 9999.0) > 0.001]
    reg_data = [range(len(d)), d]
    reg = linregress(reg_data)
    p_val = reg[3]
    if float(p_val) > 0.05:
        slope = 0.0
    else:
        slope = float(reg[0])
    return slope

def line_regress_two_slopes(data):
    temp_data = data[0:61]
    index_sums = data[61:122]
    t_data = temp_data
    i_data = index_sums
    '''
    index_idx = np.where(np.absolute(index_sums + 9999.0) > 0.001)
    temp_idx = np.where(np.absolute(temp_data - 1e20) > 0.001)
    idx = np.intersect1d(index_idx[0], temp_idx[0])
    t_data = temp_data[idx]
    i_data = index_sums[idx]
    '''
    if t_data.size == 0 or i_data.size == 0:
        return 0.0
    reg_data = np.array([[t_data[i], i_data[i]] for i in range(len(list(t_data)))])
    reg = linregress(reg_data)
    p_val = reg[3]
    if p_val > 0.05 or np.isnan(reg[3]):
        slope = 0.0
    else:
        slope = round(reg[0], 4)
    return slope

def line_regress_two_coeffs(data):
    # Returns the SLOPES
    temp_data = data[0:61]
    index_sums = data[61:122]
    t_data = temp_data
    i_data = index_sums
    if t_data.size == 0 or i_data.size == 0:
        return 0.0
    reg_data = [[t_data[i], i_data[i]] for i in range(len(list(t_data)))]
    reg = linregress(reg_data)
    coeff = round(reg[2], 4)
    return coeff

def get_array_data(var_name, years, data_dir):
    all_sums = np.array([])
    all_temps = np.array([])
    lons = np.array([])
    lats = np.array([])
    for year_idx, year in enumerate(years):
        DS = Dataset(data_dir + var_name +  '_5th_Indices_WUSA_' + str(year) + '.nc', 'r')
        if lons.shape[0] == 0:
            #lons = np.array([l - 360 for l in DS.variables['lon'][:]])
            lons = np.array([l for l in DS.variables['lon'][:]])
            lats = DS.variables['lat'][:]
            lat_min = lats[np.argmin(lats)]
            lat_max = lats[np.argmax(lats)]
            latbounds = [lat_min, lat_max]
            lon_min = lons[np.argmin(lons)]
            lon_max = lons[np.argmax(lons)]
            lonbounds = [lon_min, lon_max]
            all_sums = np.empty([len(years), lats.shape[0], lons.shape[0]], dtype=float)
            all_temps = np.empty([len(years), lats.shape[0], lons.shape[0]], dtype=float)
        indices = DS.variables['index'][:,:,:]
        indices[indices == -9999] = 0
        # Sum /Ave over season
        all_sums[year_idx] = np.sum(indices, axis=0)
        # NEW
        start_doy = 334
        num_days = 90
        end_doy = num_days - (365 - start_doy)
        DS = Dataset('/media/DataSets/livneh/' + var_name + '.' + str(year) + '.nc', 'r')
        all_lats = DS.variables['lat'][:]
        all_lons = DS.variables['lon'][:]
        # latitude lower and upper index
        latli = np.argmin(np.abs(all_lats - latbounds[0]))
        latui = np.argmin(np.abs(all_lats - latbounds[1]))
        # longitude lower and upper index
        lonli = np.argmin(np.abs(all_lons - lonbounds[0]))
        lonui = np.argmin(np.abs(all_lons - lonbounds[1]))
        this_year_data = DS.variables[var_name][0:end_doy, latli:latui+1, lonli:lonui+1]
        DS = Dataset('/media/DataSets/livneh/' + var_name + '.' + str(year - 1) + '.nc', 'r')
        last_year_data = DS.variables[var_name][start_doy:365, latli:latui+1, lonli:lonui+1]
        year_data = np.concatenate((last_year_data, this_year_data), axis=0)
        #year_data[np.absolute(year_data - 1e20) < 0.0001] = np.nan
        year_data[year_data == 1e20] = np.nan
        all_temps[year_idx] = np.nanmean(year_data, axis=0)
        # END NEW
    #get the slopes
    #slopes = np.apply_along_axis(line_regress, 0, all_sums)
    slopes = np.apply_along_axis(line_regress_two_slopes, 0, np.concatenate((all_temps, all_sums), axis=0))
    corr_coeffs = np.apply_along_axis(line_regress_two_coeffs, 0, np.concatenate((all_temps, all_sums), axis=0))
    lons = np.array([l - 360 for l in lons])
    return slopes, corr_coeffs, lons, lats

if __name__ == '__main__':
    years = range(1951, 2012)
    data_dir = 'RESULTS/livneh/'
    for var_name in ['tmin', 'tmax']:
        print('VAR ' + var_name)
        slopes, corr_coeffs, lons, lats = get_array_data(var_name, years, data_dir)
        out_file = data_dir + var_name + '_index_sum_slopes.tif'
        array_to_raster(lats, lons, slopes, out_file, nodata=-9999)
        out_file = data_dir + var_name + '_index_sum_correlations.tif'
        array_to_raster(lats, lons, corr_coeffs, out_file, nodata=-9999)
