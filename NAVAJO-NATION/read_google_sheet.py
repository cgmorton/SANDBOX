from __future__ import print_function
from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools

import config


def read_metadata(service):
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
        print(', '.join(row))
        '''
        # Print columns A and E, which correspond to indices 0 and 4.
        print('%s, %s' % (row[0], row[4]))
        '''

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

def read_station_data(service, station_id):
    # Call the Sheets API
    col_range = str(station_id) + '!' + config.statics['data_col_range']
    result = service.spreadsheets().values().get(spreadsheetId=config.statics['spreadsheet_id'],
                                                 range=col_range).execute()
    values = result.get('values', [])

    if not values:
        print('No data found.')
        return
    # Header
    print(', '.join(config.statics['data_cols']) + ':')
    for row in values:
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
    # read_metadata(service)
    # read_station_data(service, 85)
    read_station_metadata(service, 85)

if __name__ == '__main__':
    main()

