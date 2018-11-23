#!/Users/bdaudert/anaconda/envs/assets/bin/python
import json
from math import cos, asin, sqrt

def distance(lat1, lon1, lat2, lon2):
    p = 0.017453292519943295
    a = 0.5 - cos((lat2-lat1)*p)/2 + cos(lat1*p)*cos(lat2*p) * (1-cos((lon2-lon1)*p)) / 2
    return 12742 * asin(sqrt(a))

if __name__ == '__main__':
    stn_lls = []
    with open('/Users/bdaudert/DATA/NAVAJO_NATION/NN_stations_GEOM.geojson') as f:
        stn_meta = json.load(f)
    for feat in stn_meta['features']:
        lat = feat['geometry']['coordinates'][1]
        lon = feat['geometry']['coordinates'][0]
        stn_lls.append([lat, lon])
    mn = 9999999
    for p1 in stn_lls:
        for p2 in stn_lls:
            if str(p1) == str(p2):
                continue
            dist = distance(p1[0], p1[1], p2[0], p2[1])
            if dist < mn:
                mn = dist
    print(mn)

