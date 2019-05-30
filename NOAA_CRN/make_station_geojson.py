#!/Users/bdaudert/anaconda/bin/python
import csv
import json

# stn_names = '/Users/bdaudert/DATA/NOAA-CRN/station_names.txt'
stn_names = '/Users/bdaudert/DATA/NOAA-CRN/new_station_names.txt'
stn_locs = '/Users/bdaudert/DATA/NOAA-CRN/stationID_lat_lon_2012.txt'
out_file_name = '/Users/bdaudert/DATA/NOAA-CRN/NOAA_CRN_stations_2019_GEOM.geojson'

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
            stn_lon = None; stn_lat = None
            #t = ','.join(row[0].split('\t'))
            l = row[0].split('\t')
            '''
            # for station_names.txt
            stn_id = l[0]; stn_state = l[1];
            stn_name = l[2]
            try:
                stn_name += ' ' + row[1]
            except:
                pass
            stn_id_int = None
            try:
                stn_id_int = int(float(stn_id))
            except:
                continue
            print stn_id
            '''
            # for new_station_names.txt
            stn_id = l[0]
            stn_id_int = int(stn_id)
            rest = l[1].split('.txt')[0].split('-2019-')[1].split('_')
            stn_state = rest[0]
            stn_name = ' '.join(rest[1:-2]) + ', ' + stn_state + ', ' + ''.join(rest[-2:])
            # Grab station lon/lats
            # '96404.0\t 62.7400\t -141.210\r'
            #stn_loc_line = loc_lines[row_idx].replace('\r', '').split('\t ')
            for loc in loc_lines:
                stn_loc_line = loc.replace('\r', '').split('\t ')
                stn_loc_line_id_int = None
                try:
                    stn_loc_line_id_int = int(float(stn_loc_line[0]))
                except:
                    continue
                if stn_id_int != stn_loc_line_id_int:
                    continue
                else:
                    stn_lon = round(float(stn_loc_line[2]),4)
                    stn_lat = round(float(stn_loc_line[1]),4)
                    break
            if not stn_lat or not stn_lon:
                print('Can not find station in stn_lon_lat file: ' + stn_id)
                continue

            feat_data = {
                'type': 'Feature',
                'properties': {
                    'Name': stn_name,
                    'State': stn_state,
                    ''
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
