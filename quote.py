import datetime
from typing import Union

from sqlalchemy.engine import RowProxy

def get_float_value(data: dict, key: str):
    value = data.get(key, data).get('value', None)
    if value is not None and len(value) > 0:
        return float(value.replace(',', '.'))
    else:
        return None

class Quote:
    def __init__(self, data: Union[dict, RowProxy]):
        if isinstance(data, dict):
            self.date = data.get('date', data).get('value', None)
            self.nav = get_float_value(data, 'nav')
            self.gross = get_float_value(data, 'gross')
            self.real_close_price = get_float_value(data, 'real_close_price')
            self.feed_source = data.get('feed_source', data).get('value', None)
            self.asset = int(data.get('asset', data).get('value', -1))
            self.pl = get_float_value(data, 'pl')
            self.close = get_float_value(data, 'close')
            self.return_value = get_float_value(data, 'return')

            # Convert date to datetime
            self.date = datetime.datetime.strptime(self.date, '%Y-%m-%d')
        elif isinstance(data, RowProxy):
            self.asset = data[0]
            self.date = data[1]
            self.close = data[2]

