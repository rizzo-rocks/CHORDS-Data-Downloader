"""
This is a barebones example of the underlying logic behind chords_data_download.py
"""

import requests
from json import dumps
from json import loads

portal = 'URL'
inst_id='1'
user_email='REDACTED'
api_key='REDACTED'
start='2024-10-31T00:00:00'
end='2024-10-31T23:59:59'

url = f"{portal}/api/v1/data/{inst_id}?start={start}&end={end}&email={user_email}&api_key={api_key}"

response = requests.get(url=url)
all_fields = loads(dumps(response.json()))
data = all_fields['features'][0]['properties']['data']
print(data)