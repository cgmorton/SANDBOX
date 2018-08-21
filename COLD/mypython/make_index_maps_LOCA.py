#!/usr/bin/env python

from netCDF4 import Dataset
import numpy as np
import json
from osgeo import gdal, osr, ogr

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

def get_array_data(var_name, rcp, years, data_dir):
    all_sums = np.array([])
    all_aves = np.array([])
    lons = np.array([])
    lats = np.array([])
    for year_idx, year in enumerate(years):
        DS = Dataset(data_dir + var_name + '_' + rcp + '_5th_Indices_WUSA_' + str(year) + '.nc', 'r')
        if lons.shape[0] == 0:
            lons = np.array([l - 360 for l in DS.variables['lon'][:]])
            lats = DS.variables['lat'][:]
            all_sums = np.empty([len(years), lats.shape[0], lons.shape[0]], dtype=float)
            all_aves = np.empty([len(years), lats.shape[0], lons.shape[0]], dtype=float)
        indices = DS.variables['index'][:,:,:]
        # Sum /Ave over season
        all_sums[year_idx] = np.sum(indices, axis=0)
        all_aves[year_idx] = np.mean(indices, axis=0)
    # Average over years
    index_sums = np.apply_along_axis(np.mean, 0, all_sums)
    index_aves = np.apply_along_axis(np.mean, 0, all_aves)
    return index_sums, index_aves, lons, lats

if __name__ == '__main__':
    LOCA_CMIP5_MODELS = {
        'CNRM-CM5':[1950,2100],
        'HadGEM2-CC':[1950,2100],
        'HadGEM2-ES':[1950,2100],
        'GFDL-CM3':[1950,2100],
        'CanESM2':[1950,2100],
        'MICRO5':[1950,2100],
        'CESM1-BGC':[1950,2100],
        #'CMCC-CMS':[1950,2100],
        'ACCESS1-0':[1950,2100],
        'CCSM4':[1950,2100]
    }
    years = range(2006, 2100)
    for model in LOCA_CMIP5_MODELS.keys():
        for rcp in ['rcp45', 'rcp85']:
            data_dir = '/media/DataSets/loca/' + model + '/' + rcp + '/'
            for var_name in ['tmin', 'tmax']:
                index_sums, index_aves, lons, lats = get_array_data(var_name, rcp, years, data_dir)
                # out_file = data_dir + var_name + '_' + rcp + '_index_sums.tif'
                # array_to_raster(lats, lons, index_sums, out_file, nodata=-9999)
                out_file = data_dir + var_name + '_' + rcp + '_2006_2099_index_aves.tif'
                array_to_raster(lats, lons, index_aves, out_file, nodata=-9999)
