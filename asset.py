import json

class Asset:
    def __init__(self, data: dict):
        self.label = data['LABEL']['value']
        self.id = data['ASSET_DATABASE_ID']['value']
        self.type = data['TYPE']['value']
        self.close = data['LAST_CLOSE_VALUE_IN_CURR']['value']

    def __str__(self):
        return f'{self.label} | {self.type} | {self.id} | {self.close}'