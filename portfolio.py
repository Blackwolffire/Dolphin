import json

from asset import Asset


class Portfolio:
    def __init__(self, data: dict, asset: Asset):
        self.label = data['label']
        self.currency = data['currency']['code']
        self.type = data['type']
        self.values = data['values']
        self.asset = asset

    def add_asset(self, asset: Asset, quantity: int):
        if '2016-06-01' not in self.values:
            self.values['2016-06-01'] = []

        self.values['2013-06-14'].append({
            'asset': {'asset': asset.id, 'quantity': quantity}
        })

    def json(self):
        return json.dumps({
            'label': self.label,
            'currency': {'code': self.currency},
            'type': self.type,
            'values': self.values
        })