#!/usr/bin/env python
'''Config file for custom ee assets'''
import re

ds_settings = {
    'NOAA_CRN':  {
        'download_method': 'ftp',
        'ftp_server': 'ftp.atdd.noaa.gov',
        'ftp_folder': '/CI/crn/gridded/netCDF/archive/alldays/',
        'infile_ext': 'nc',
        'infile_fmt': '{var_name}_all.nc',
        'infile_dt_fmt': '%Y%m%d',
        'dat_nodata': 9.96921e+36,
        'bucket_name': 'gs://climate-engine',
        'bucket_folder': 'noaa_crn/daily',
        'asset_coll': 'projects/climate-engine/noaa_crn/daily',
        'asset_id_fmt': '{date}',
        'asset_dt_fmt': '%Y%m%d',
        'asset_proj': 4326,
        'asset_geo': [-125.10030746459961, 0.20061493, 0, 50.10067081451416, 0, 0.20134163],
        'asset_nodata': -9999,
        'asset_shape': (150, 325),
        'bands': {
            'AWC': 'Available Water Capacity',
            'Fract_awc': 'Fractional Available Water Capacity',
            'Air_temp': 'Air Temperature',
            'Sur_temp_max': 'Maximum Surface Temperature',
            'Sur_temp_min': 'Minimum Surface Temperature',
            'Surface_temp': 'Surface Temperature',
            'Temp_max': 'Maximum Temperature',
            'Temp_min': 'Minimum Temperature',
            'Precip': 'Precipitation',
            'Smois_05cm': 'Soil Moisture at 5cm',
            'Smois_10cm': 'Soil Moisture at 10cm',
            'Smois_20cm': 'Soil Moisture at 20cm',
            'Smois_50cm': 'Soil Moisture at 50cm',
            'Smois_100cm': 'Soil Moisture at 100cm',
            'Soiltemp_05cm': 'Soil Temperature at 5cm',
            'Soiltemp_10cm': 'Soil Temperature at 10cm',
            'Soiltemp_20cm': 'Soil Temperature at 20cm',
            'Soiltemp_50cm': 'Soil Temperature at 50cm',
            'Soiltemp_100cm': 'Soil Temperature at 100cm',
            'Solar': 'Solar Radiation',
            'Wind': 'Wind',
            'rh': 'Relative Humidity'
        }
    }
}
