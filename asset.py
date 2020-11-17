class Asset:
    def __init__(self, data: dict, date: str):
        self.label = data['LABEL']['value']
        self.id = int(data['ASSET_DATABASE_ID']['value'])
        self.type = data['TYPE']['value']
        self.date = date
        if 'LAST_CLOSE_VALUE_IN_CURR' in data:
            close = data['LAST_CLOSE_VALUE_IN_CURR']['value']
            self.currency = close.split(' ')[1]
            self.close = float(close.split(' ')[0].replace(',', '.'))
        else:
            self.currency = None
            self.close = None

    def __str__(self):
        return f'{self.label} | {self.type} | {self.id} | {self.close} | {self.currency} | {self.date}'