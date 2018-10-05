import os
import logging
from ftplib import FTP
from netCDF4 import Dataset
import datetime as dt
import numpy as np
from osgeo import gdal, osr

from config import ds_settings


def days_since_epoch_to_date_str(epoch, numdays):
    '''
    computes date of numdays after epoch
    epoch is of format yyyy-mm-dd
    numdays; number of days from epoch
    returns date string
    '''
    yr = int(epoch[0:4])
    m = int(epoch[4:6])
    d = int(epoch[6:8])
    new_date = dt.datetime(yr,m,d,0,0) + dt.timedelta(numdays)
    return new_date.strftime("%Y%m%d")

def days_since_epoch_to_date_dt(epoch, numdays):
    '''
    computes date of numdays after epoch
    epoch is of format yyyy-mm-dd
    numdays; number of days from epoch
    returns date string
    '''
    yr = int(epoch[0:4])
    m = int(epoch[4:6])
    d = int(epoch[6:8])
    new_date = dt.datetime(yr,m,d,0,0) + dt.timedelta(numdays)
    return new_date


def dt_to_days_since_epoch(epoch, date_dt):
    '''

    '''
    yr = int(epoch[0:4])
    m = int(epoch[4:6])
    d = int(epoch[6:8])
    days = (date_dt - dt.datetime(yr,m,d,0,0)).days
    return days

def get_geo(lons, lats):
    '''
    [upper left lon, lon_res, 0, upper left lat, 0, lat_res]
    '''
    yres = abs(lats[1] - lats[0])
    xres = abs(lons[1] - lons[0])
    ulx = min(lons) - (xres / 2.)
    uly = max(lats) + (yres / 2.)
    return [ulx, xres, 0, uly, 0, -yres]

def ftp_download(site_url, site_folder, file_name, output_path):
    """"""
    ftp = FTP()
    ftp.connect(site_url)
    ftp.login()
    ftp.cwd('{}'.format(site_folder))
    ftp.retrbinary('RETR %s' % file_name, open(output_path, 'wb').write)
    ftp.quit()

def array_to_geotiff(output_array, output_path, output_shape, output_geo,
                     output_proj, output_nodata=None):
    """
    Parameters
    ----------
    output_array : np.array
    output_path : str
        GeoTIFF file path.
    output_shape : tuple or list of ints
        Image shape (rows, cols).
    output_geo : tuple or list of floats
        Geo-transform (xmin, cs, 0, ymax, 0, -cs).
    output_proj : str
        Projection Well Known Text (WKT) string.
    output_nodata : float, optional
        GeoTIFF nodata value (the default is None).
    Returns
    -------
    None
    """
    output_driver = gdal.GetDriverByName('GTiff')
    output_rows, output_cols = output_shape
    output_ds = output_driver.Create(
        output_path, output_cols, output_rows, 1,
        gdal.GDT_Float32, ['COMPRESS=LZW', 'TILED=YES'])
    output_ds.SetProjection(output_proj)
    output_ds.SetGeoTransform(output_geo)
    output_band = output_ds.GetRasterBand(1)
    output_band.WriteArray(output_array)
    output_band.FlushCache()
    if output_nodata:
        output_band.SetNoDataValue(output_nodata)
    output_ds = None

if __name__ == '__main__':
    ds_params = ds_settings['NOAA_CRN']
    site_url = ds_params['ftp_server']
    site_folder = ds_params['ftp_folder']
    var_name = 'Sur_temp_min'
    #var_name = 'Precip'
    infile = var_name + '_all.nc'
    infile_path = '/Volumes/DHS/TEST_NOAA_CRN/nc/' + infile
    '''
    outfile = os.path.join(ds_params['outpath'], infile)
    if not os.path.isfile(outfile):
        print('  Downloading the file ' + infile)
        ftp_download(site_url, site_folder, infile, outfile)
    '''
    # read file
    ds = Dataset(infile_path, 'r')
    lons = ds.variables['lon'][:]
    lats = ds.variables['lat'][:]
    time = ds.variables['time'][:]
    epoch = '20060101'
    shape = (lats.shape[0], lons.shape[0])
    geo = get_geo(lons, lats)
    print(ds.variables)
    print('GEO ' + str(geo))
    asset_osr = osr.SpatialReference()
    asset_osr.ImportFromEPSG(4326)
    proj = asset_osr.ExportToWkt()
    print(ds.variables.keys())
    '''
    nc_var_name = list(ds.variables.keys())[0]
    for day_idx in range(5):
        outfile_name =  var_name + '_' + str(day_idx) + '.tif'
        tif_path = os.path.join(os.getcwd(), outfile_name)
        input_ma = ds.variables[nc_var_name][day_idx,:,:].copy()
        input_array = np.fliplr(np.flipud(input_ma.data.astype(np.float32)))
        input_nodata = float(input_ma.fill_value)
        input_array[input_array == input_nodata] = -9999
        # print(input_array[input_array != -9999].shape)
        # print(input_array[input_array == -9999].shape)
        #array_to_geotiff(input_array,tif_path,shape,geo,proj,output_nodata=-9999)

    '''
    for t_idx, t in enumerate(time):
        print(t)
        print(days_since_epoch_to_date_str(epoch, t_idx))
        date_dt = days_since_epoch_to_date_dt(epoch,t_idx)
        num_days_since_epoch = dt_to_days_since_epoch(epoch, date_dt)
        idx = (np.abs(time - num_days_since_epoch)).argmin()
        # print(idx)
