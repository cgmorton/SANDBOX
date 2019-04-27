#!/usr/bin/env python2
# source activate ee-python

import os
import subprocess
import json
import glob
import shapefile

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
    geojson_dir = './'
    local_dir = '/Users/bdaudert/Desktop/CE_Shapefile/CE_shp_orig/'
    shape_files = filter(os.path.isfile, glob.glob(local_dir + '*.shp'))
    for shape_file in shape_files:
        shape_path, shape_file = os.path.split(shapefile)
        file_name = shape_file.split('.shp')[0]
        reader = shapefile.Reader(shape_file)
        fields = reader.fields[1:]
        field_names = [field[0] for field in fields]
        geojson_file_name = file_name + '.geojson'
        geojson_file_path = geojson_dir + geojson_file_name
        # Read the features
        buffer = []
        for sr in reader.shapeRecords():
            try:
                geom = sr.shape.__geo_interface__
            except:
                continue
            properties = dict(zip(field_names, sr.record))
            buffer.append(dict(type="Feature", \
                               geometry=geom, properties=properties))

        # write the GeoJSON file

        geojson = open(geojson_file_path, "w")
        geojson.write(json.dumps({"type": "FeatureCollection", "features": buffer}, indent=2) + "\n")
        geojson.close()


        bucket_file_path = 'gs://clim-engine-development/clim-engine-geojson/shp_orig/' + geojson_file_name
        upload_file_to_bucket(geojson_file_path, bucket_file_path)
        make_file_public(bucket_file_path)
        delete_from_local(geojson_file_path)
