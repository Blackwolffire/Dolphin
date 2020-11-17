class Quote:
    def __init__(self, data: dict):
        self.date = data['date']['value']
        self.nav = data['nav']['value']
        self.gross = data['gross']['value']
        self.real_close_price = data['real_close_price']['value']
        self.feed_source = data['feed_source']['value']
        self.asset = data['asset']['value']
        self.pl = data['pl']['value']
        self.close = data['close']['value']
        self.return_value = data['return']['value']