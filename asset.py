from typing import Union

from sqlalchemy.engine import RowProxy


class Asset:
    def __init__(self, data: Union[dict, RowProxy, int], date: str = None):
        if isinstance(data, dict):
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
        elif isinstance(data, RowProxy):
            self.id = data[0]
            self.label = data[1]
            self.type = data[2]
            self.currency = data[3]
        elif isinstance(data, int):
            self.id = data


    def __str__(self):
        return f'{self.label} | {self.type} | {self.id} | {self.currency}'