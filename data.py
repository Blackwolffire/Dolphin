import json
from os import getenv

import requests

from asset import Asset
from portfolio import Portfolio
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


def get_quote(start_date: str, end_date: str, asset: Asset) -> [Quote]:
    res = requests.get(URL + f'/asset/{asset.id}/quote?start_date={start_date}&end_date={end_date}', auth=AUTH)
    return [Quote(x) for x in res.json()]


def get_data_dataframe(start_date: str, end_date: str):
    assets = get_assets(start_date)
    data = []
    for asset in assets:
        if asset.currency:
            for quote in get_quote(start_date, end_date, asset):
                data.append((quote.asset, quote.date, quote.real_close_price))

    return data


def get_ratios():
    res = requests.get(URL + '/ratio', auth=AUTH)
    return [Ratio(x) for x in res.json()]


def get_portfolio(asset: Asset) -> Portfolio:
    res = requests.get(URL + f'/portfolio/{asset.id}/dyn_amount_compo', auth=AUTH)
    return Portfolio(res.json(), asset)


def update_portfolio(portfolio: Portfolio) -> bool:
    res = requests.put(URL + f'/portfolio/{portfolio.asset.id}/dyn_amount_compo', auth=AUTH, data=portfolio.json())
    return res.status_code == 200


def calculate_sharpe_ratio(asset: Asset, start_date: str, end_date: str):
    payload = json.dumps({
        '_ratio': [9, 12, 10],
        '_asset': [asset.id],
        '_bench': None,
        '_startDate': start_date,
        '_endDate': end_date,
        '_frequency': None
    })
    res = requests.post(URL + '/ratio/invoke', data=payload, auth=AUTH)
    return res