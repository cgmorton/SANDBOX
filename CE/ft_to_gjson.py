#!/usr/bin/python

import urllib2, requests
import json

client_id = "1044763978609-19ispnqukug04fcgvrki7ca7h4rmbj3b.apps.googleusercontent.com"
client_secret = "EsCHFgQa6eppzARS0f0HvRE2"
redirect_uri = "http://test.com/oauth2callback"
api_key = "AIzaSyAShBVlyAW_9i6WsnIRnfsUwVbqvRxa9s0"
query_url = 'https://www.googleapis.com/fusiontables/v1/query'
tableids = ["1MC-LjlHxy04aBcxeSB_aVaDo2DlP_y2MVg3p2sxb"]
# https://www.googleapis.com/fusiontables/v1/query?sql=SELECT%20*%20FROM%201YAAEPdkYw0GybLrLlCr2xs_6lXZMi6w4HCE_JCaQ&key=AIzaSyAShBVlyAW_9i6WsnIRnfsUwVbqvRxa9s0

def ft_to_gjson(query_url, tableid):
    url_params = '?sql=SELECT%20*%20FROM%20' + tableid + '&key=' + api_key
    url = query_url + url_params
    data = requests.get(url).json()
    print(data)

    '''
    data_json = json.load(urllib2.urlopen(url))
    print(data_json)
    '''
if __name__ == "__main__":
    for tableid in tableids:
        ft_to_gjson(query_url, tableid)


