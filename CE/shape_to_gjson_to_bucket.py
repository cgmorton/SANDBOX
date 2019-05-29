#!/usr/bin/env python2
# source activate ee-python

import os
import subprocess
import json
import glob
import shapefile

import ee

def upload_file_to_bucket(upload_path, bucket_fl_path):
    if os.name == 'posix':
        shell_flag = False
    else:
        shell_flag = True

    args = ['gsutil', 'cp', upload_path, bucket_fl_path]
    try:
        subprocess.check_output(args, shell=shell_flag)
        # os.remove(upload_path)
        print('Successfully uploaded to bucket: ' + bucket_fl_path)
    except Exception as e:
        print('    Exception: {}\n'.format(e))

def delete_from_local(fl_path):
    try:
        os.remove(fl_path)
        print('Remnoved ' + fl_path)
    except:
        print('ERROR: Could not remove file from local: ' + fl_path)

def make_file_public(bucket_file_path):
    if os.name == 'posix':
        shell_flag = False
    else:
        shell_flag = True
    args = ['gsutil', 'acl', 'ch', '-u', 'AllUsers:R', bucket_file_path]
    try:
        subprocess.check_output(args, shell=shell_flag)
    except Exception as e:
        print('    Exception: {}\n'.format(e))


if __name__ == "__main__":
    EE_ACCOUNT = 'clim-engine-development@appspot.gserviceaccount.com'
    EE_PRIVATE_KEY_FILE = 'clim-engine.json'

    EE_CREDENTIALS = ee.ServiceAccountCredentials(
        EE_ACCOUNT, key_file=EE_PRIVATE_KEY_FILE)
    ee.Initialize(EE_CREDENTIALS)

    geojson_dir = 'GEOJSON/'
    local_dir = '/Users/bdaudert/DATA/CE/shapefiles/shp_simplified/'
    shape_files = filter(os.path.isfile, glob.glob(local_dir + '*.shp'))
    print(shape_files)
    for shape_file in shape_files:
        s_path, s_file = os.path.split(str(shape_file))

        file_name = s_file.split('.shp')[0]
        if file_name != 'ClimateEngine_FEWS_Admin2':
            continue

        '''
        if file_name in ['ClimateEngine_Countries','ClimateEngine_US_Counties', 'ClimateEngine_US_States',\
                        'ClimateEngine_US_HUC6', 'ClimateEngine_Navajo_Nation_Chapters', 'ClimateEngine_Navajo_Nation_Agencies',\
                        'ClimateEngine_Sierra_Meadows', 'ClimateEngine_Predictive_Service_Areas', 'ClimateEngine_FEWS_Admin1']:
            continue
        '''

        print('Processing: ' + file_name)
        geojson_file_name = file_name + '.geojson'
        geojson_file_path = geojson_dir + geojson_file_name

        # Check if geojson exists, if yes, do not regenerate
        if not os.path.isfile(geojson_file_path) or  os.stat(geojson_file_path).st_size == 0:
            # Read the shapefile
            reader = shapefile.Reader(shape_file)
            fields = reader.fields[1:]
            field_names = [field[0] for field in fields]
            # Read the features
            features = []
            try:
                rd = reader.shapeRecords()
            except Exception as e:
                print('Unable to read shapefile: ' + str(e))
                continue

            for sr in rd:
                try:
                    geom = sr.shape.__geo_interface__
                except:
                    continue
                properties = dict(zip(field_names, sr.record))
                for key, val in properties.iteritems():
                    if not isinstance(val, basestring) or not isinstance(val, float) or not isinstance(val, int):
                        try:
                            properties[key] = str(val)
                        except:
                            pass
                features.append(dict(type="Feature", geometry=geom, properties=properties))

            # write the GeoJSON file
            geojson = open(geojson_file_path, "w")
            geojson.write(json.dumps({"type": "FeatureCollection", "features": features}, indent=2) + "\n")
            geojson.close()
        else:
            print('Geojson exists ' + geojson_file_path)

        # Uplaod to bucket
        bucket_file_path = 'gs://clim-engine-geojson/shp_simplified/' + geojson_file_name
        upload_file_to_bucket(geojson_file_path, bucket_file_path)
        # This throws error right now:
        # Failed to set acl for gs... Please ensure you have OWNER-role access to this resource.
        # make_file_public(bucket_file_path)


        # delete_from_local(geojson_file_path)
