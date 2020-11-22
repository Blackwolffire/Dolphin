import json
from typing import Union

from algo.asset import Asset


class Portfolio:

    def __init__(self, data: dict = {}, asset: Asset=None):
        self.label = data.get('label', 'EPITA_PTF_12')
        self.currency = data.get('currency', data).get('code', 'EUR')
        self.type = data.get('type', 'front')
        self.values = data.get('values', {})
        self.asset = asset
        self.date = list(self.values.keys())[0] if len(list(self.values.keys())) > 0 else '2016-06-01'
        self.id = None

    def add_asset(self, asset: Union[Asset, int], quantity: int):
        if self.date not in self.values:
            self.values[self.date] = []
        asset_id = asset if isinstance(asset, int) else asset.id
        self.values[self.date].append({
            'asset': {'asset': asset_id, 'quantity': quantity}
        })

    def json(self):
        return json.dumps({
            'label': self.label,
            'currency': {'code': self.currency},
            'type': self.type,
            'values': self.values
        })

    def get_assets(self):
        return [(x['asset']['asset'], x['asset']['quantity']) for x in self.values[self.date]]
