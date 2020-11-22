import json

from algo.asset import Asset


class Portfolio:
    @classmethod
    def from_dict(cls, data: dict):
        pass

    def __init__(self, data: dict, asset: Asset):
        self.label = data['label']
        self.currency = data['currency']['code']
        self.type = data['type']
        self.values = data['values']
        self.asset = asset
        self.date = '2016-06-01'
        self.id = None

    def add_asset(self, asset: Asset, quantity: int):
        if self.date not in self.values:
            self.values[self.date] = []

        self.values[self.date].append({
            'asset': {'asset': asset.id, 'quantity': quantity}
        })

    def json(self):
        return json.dumps({
            'label': self.label,
            'currency': {'code': self.currency},
            'type': self.type,
            'values': self.values
        })

    def get_assets(self):
        return self.values[self.date]
