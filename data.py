import requests
from os import getenv
from asset import Asset
from quote import Quote
from ratio import Ratio

URL = 'https://dolphin.jump-technology.com:8443/api/v1'
# Get credentials
USERNAME = getenv('DOLPHIN_USERNAME', None)
PASSWORD = getenv('DOLPHIN_PASSWORD', None)

AUTH = (USERNAME, PASSWORD)


def get_assets(date: str) -> [Asset]:
    res = requests.get(URL + f'/asset?columns=ASSET_DATABASE_ID&columns=LABEL&columns=LAST_CLOSE_VALUE_IN_CURR&columns=TYPE&date={date}', verify=False, auth=AUTH)
    return [Asset(x, date) for x in res.json()]


def get_quote(start: str, end: str, asset: Asset) -> [Quote]:
    res = requests.get(URL + f'/asset/{asset.id}/quote?start_date={start}&end_date={end}', auth=AUTH)
    return [Quote(x) for x in res.json()]


def get_ratios():
    res = requests.get(URL + '/ratio', auth=AUTH)
    return [Ratio(x) for x in res.json()]


def get_porfolio(asset: Asset):
    res = requests.get(URL + f'/portfolio/{asset.id}/dyn_amount_compo', auth=AUTH)
    return res.json()
