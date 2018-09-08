#!/Users/bdaudert/anaconda/bin/python
import csv
import json

stn_names = '/Users/bdaudert/DATA/NOAA-CRN/station_names.txt'
stn_locs = '/Users/bdaudert/DATA/NOAA-CRN/stationID_lat_lon_2012.txt'
out_file_name = '/Users/bdaudert/DATA/NOAA-CRN/NOAA_CRN_stations_2012_GEOM.geojson'

if __name__ == '__main__':
    geojson_data = {
        'type': 'FeatureCollection',
        'features': []
    }
    with open(stn_locs, 'r') as loc_file:
       loc_lines = loc_file.read().split('\n')

    with open(stn_names, 'r') as in_file:
        rdr = csv.reader(in_file, delimiter=' ')
        for row_idx, row in enumerate(rdr):
            #t = ','.join(row[0].split('\t'))
            l = row[0].split('\t')
            stn_id = l[0]; stn_state = l[1]; stn_name = l[2]
            # Grab station lon/lats
            # '96404.0\t 62.7400\t -141.210\r'
            stn_loc_line = loc_lines[row_idx].replace('\r', '').split('\t ')
            if int(float(stn_loc_line[0])) != int(float(stn_id)):
                print('Something is up!')
            stn_lon = round(float(stn_loc_line[2]),4)
            stn_lat = round(float(stn_loc_line[1]),4)
            feat_data = {
                'type': 'Feature',
                'properties': {
                    'Name': stn_name,
                    'State': stn_state,
                    'ID': stn_id,
                    'Lon': str(stn_lon),
                    'Lat': str(stn_lat)
                },
                'geometry': {'type': 'Point', 'coordinates': [stn_lon, stn_lat]}
            }
            print feat_data
            geojson_data['features'].append(feat_data)

    with open(out_file_name, 'w') as outfile:
        json.dump(geojson_data, outfile)
