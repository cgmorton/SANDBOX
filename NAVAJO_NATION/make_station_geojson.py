from __future__ import print_function
from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools
import json

from pyproj import Proj
import config


def make_geojson(service, out_file_name):
    geojson_data = {
        'type': 'FeatureCollection',
        'features': []
    }
    # Call the Sheets API
    col_range = config.statics['metadata_sheet'] + '!' + config.statics['metadata_col_range']
    result = service.spreadsheets().values().get(spreadsheetId=config.statics['spreadsheet_id'],
                                                 range=col_range).execute()
    values = result.get('values', [])

    if not values:
        print('No data found.')
        return
    # Header
    print(', '.join(config.statics['metadata_cols']) + ':')
    for row in values:
        print(row)
        print(len(row))
        feat_data = {
            'type': 'Feature',
            'properties': {},
            'geometry': {'type': 'Point', 'coordinates': []}
            }
        for prop_idx, prop in enumerate(config.statics['metadata_cols']):
            if prop == 'UTM EASTING':
                idx_east = prop_idx
            if prop == 'UTM NORTHING':
                idx_north = prop_idx
            try:
                feat_data['properties'][prop] = row[prop_idx]
            except:
                feat_data['properties'][prop] = 'Not Found!'
        # Convert UTM to lon, lat
        myProj = Proj("+proj=utm +zone=12K, +north +ellps=WGS84 +datum=WGS84 +units=m +no_defs")
        lon, lat = myProj(row[idx_east], row[idx_north], inverse=True)
        print(lon, lat)
        feat_data['geometry']['coordinates'] = [lon, lat]
        feat_data['properties']['Lon'] = str(round(lon, 4))
        feat_data['properties']['Lat'] = str(round(lat, 4))
        geojson_data['features'].append(feat_data)
        print(', '.join(row))
        '''
        # Print columns A and E, which correspond to indices 0 and 4.
        print('%s, %s' % (row[0], row[4]))
        '''
    with open(out_file_name, 'w') as outfile:
        json.dump(geojson_data, outfile)

def read_station_metadata(service, station_id):
    pass
    # FIX ME: don't know how to pick metadata row by station ID
    # Without looping
    col_range = config.statics['metadata_sheet'] + '!' + config.statics['metadata_col_range']
    result = service.spreadsheets().values().get(spreadsheetId=config.statics['spreadsheet_id'],
                                                 range=col_range).execute()
    values = result.get('values', [])

    if not values:
        print('No data found.')
        return

    # Header
    print(', '.join(config.statics['metadata_cols']) + ':')
    for row in values:
        if str(row[0]) != str(station_id):
            continue
        print(', '.join(row))



def main():
    """Shows basic usage of the Sheets API.
    Prints values from a sample spreadsheet.
    """
    store = file.Storage('token.json')
    creds = store.get()
    if not creds or creds.invalid:
        flow = client.flow_from_clientsecrets('credentials.json', config.statics['scopes'])
        creds = tools.run_flow(flow, store)
    service = build('sheets', 'v4', http=creds.authorize(Http()))

    # Read the metadata
    out_file_name = '/Users/bdaudert/DATA/NAVAJO_NATION/NN_stations_GEOM.geojson'
    make_geojson(service, out_file_name)

    '''
    # read_station_data(service, 85)
    read_station_metadata(service, 85)
    '''
if __name__ == '__main__':
    main()

