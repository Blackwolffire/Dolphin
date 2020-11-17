import datetime
from typing import Union

from sqlalchemy.engine import RowProxy


class Quote:
    def __init__(self, data: Union[dict, RowProxy]):
        if isinstance(data, dict):
            self.date = data.get('date', data).get('value', None)
            self.nav = data.get('nav', data).get('value', None)
            self.gross = data.get('gross', data).get('value', None)
            self.real_close_price = data.get('real_close_price', data).get('value', None)
            self.feed_source = data.get('feed_source', data).get('value', None)
            self.asset = int(data.get('asset', data).get('value', -1))
            self.pl = float(data.get('pl', data).get('value', -1))
            self.close = float(data.get('close', data).get('value', -1))
            self.return_value = float(data.get('return_value', data).get('value', -1))

            # Convert date to datetime
            self.date = datetime.datetime.strptime(self.date, '%Y-%m-%d')
        elif isinstance(data, RowProxy):
            self.asset = data[0]
            self.date = data[1]
            self.close = data[2]

