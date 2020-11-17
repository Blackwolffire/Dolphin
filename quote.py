class Quote:
    def __init__(self, data: dict):
        self.date = data['date']['value'] if 'date' in data else None
        self.nav = data['nav']['value'] if 'nav' in data else None
        self.gross = data['gross']['value'] if 'gross' in data else None
        self.real_close_price = data['real_close_price']['value'] if 'real_close_price' in data else None
        self.feed_source = data['feed_source']['value'] if 'feed_source' in data else None
        self.asset = int(data['asset']['value']) if 'asset' in data else None
        self.pl = data['pl']['value'] if 'pl' in data else None
        self.close = data['close']['value'] if 'close' in data else None
        self.return_value = data['return']['value']if 'return' in data else None

