import requests
from os import getenv
from asset import Asset

URL = 'https://dolphin.jump-technology.com:8443/api/v1'
# Get credentials
USERNAME = getenv('DOLPHIN_USERNAME', None)
PASSWORD = getenv('DOLPHIN_PASSWORD', None)

AUTH = (USERNAME, PASSWORD)

def get_assets():
    return requests.get(URL + '/asset?columns=ASSET_DATABASE_ID&columns=LABEL&columns=TYPE&columns=LAST_CLOSE_VALUE_IN_CURR', verify=False, auth=AUTH)


res = get_assets()
assets = []
for asset in res.json():
    assets.append(Asset(asset))

print(f'There are {len(assets)} assets')
for a in assets:
    print(a)
