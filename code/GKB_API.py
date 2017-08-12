"""Example of Python client calling Knowledge Graph Search API."""
import json
import urllib

#api_key = open('../API_key.txt').read()
api_key = 'AIzaSyAhokgs_ncsIxGetz64lcXqBun5cBamPVA'
query = 'DONALD TRUMP'
service_url = 'https://kgsearch.googleapis.com/v1/entities:search'
params = {
    'query': query,
    'limit': 1,
    'indent': True,
    'key': api_key,
}
url = service_url + '?' + urllib.urlencode(params)
response = json.loads(urllib.urlopen(url).read())
print response

# for element in response['itemListElement']:
#     print element['result']
#     print '*******************************************'
#
