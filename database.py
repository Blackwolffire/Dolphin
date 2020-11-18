import datetime

from pandas import DataFrame
from sqlalchemy import create_engine, MetaData, Table, Column, Float, Date, Integer, String, ForeignKey, select

from asset import Asset
from quote import Quote


class Database:
    def __init__(self, path: str):
        engine_url = f'sqlite:///{path}'
        self.db_engine = create_engine(engine_url)
        self.quote_table = None
        self.asset_table = None
        self.currency_table = None
        self.create_tables()

    def create_tables(self):
        metadata = MetaData()
        self.asset_table = Table('asset', metadata,
                                 Column('id', Integer, primary_key=True),
                                 Column('label', String, nullable=True),
                                 Column('type', String, nullable=True),
                                 Column('currency', String, nullable=True)
                                 )

        self.quote_table = Table('quote', metadata,
                                Column('asset_id', None, ForeignKey("asset.id")),
                                Column('date', Date),
                                Column('nav', Float),
                                Column('gross', Float),
                                Column('real_close_price', Float),
                                Column('pl', Float),
                                Column('feed_source', Float),
                                Column('return_value', Float),
                                Column('close', Float)
                                )
        self.currency_table = Table('currency', metadata,
                                    Column('currency_src', String),
                                    Column('currency_dst', String),
                                    Column('value', Float),
                                    Column('date', Date)
                                    )

        try:
            metadata.create_all(self.db_engine)
        except Exception as e:
            print(f"Cannot create table: {e}")

    def add_asset(self, asset: Asset):
        cmd = self.asset_table.insert().values(id=asset.id, label=asset.label, type=asset.type, currency=asset.currency)
        self.db_engine.execute(cmd)

    def add_currency(self, src: str, dst: str, date, value: float):
        if src == dst:
            print('Src cannot be dst!')
            return
        cmd = self.currency_table.insert().values(currency_src=src,
                                                  currency_dst=dst,
                                                  date=date,
                                                  value=value)
        self.db_engine.execute(cmd)

    def get_rate(self, src: str, dst: str, date: str):
        cmd = self.currency_table.select().where(
            (self.currency_table.c.currency_src == src) &
            (self.currency_table.c.currency_dst == dst) &
            (self.currency_table.c.date == date)
        )
        res = self.db_engine.execute(cmd)
        rate = res.fetchall()
        return rate[3]

    def add_quote(self, quote: Quote):
        cmd = self.quote_table.insert().values(
            asset_id=quote.asset,
            date=quote.date,
            close=quote.close,
            nav=quote.nav,
            pl=quote.pl,
            gross=quote.gross,
            real_close_price=quote.real_close_price,
            feed_source=quote.feed_source,
            return_value=quote.return_value)
        self.db_engine.execute(cmd)

    def get_assets(self):
        cmd = self.asset_table.select()
        res = self.db_engine.execute(cmd)
        assets = []
        for asset in res.fetchall():
            assets.append(Asset(data=asset))
        return assets

    def get_portfolio_asset(self) -> Asset:
        name = 'EPITA_PTF_12'
        cmd = self.asset_table.select().where(self.asset_table.c.label == name)
        res = self.db_engine.execute(cmd)
        asset = res.fetchall()
        return Asset(data=asset[0])

    def get_quotes(self, assets: [Asset], start_date: str, end_date: str, data_frame=False):
        asset_ids = [x.id for x in assets]
        columns = [self.quote_table.c.asset_id if not data_frame else self.asset_table.c.label]
        for column in self.quote_table.columns:
            if column.name != 'asset_id':
                columns.append(column)
        cmd = select(columns).where(self.quote_table.c.asset_id.in_(asset_ids))
        if start_date:
            cmd = cmd.where(self.quote_table.c.date >= start_date)
        if end_date:
            cmd = cmd.where(self.quote_table.c.date <= end_date)
        if data_frame:
            join = self.quote_table.join(self.asset_table)
            cmd = cmd.select_from(join)
        res = self.db_engine.execute(cmd)
        quotes = []
        for quote in res.fetchall():
            quotes.append(Quote(data=quote) if not data_frame else quote)
        if data_frame:
            df = DataFrame(quotes)
            df = df.rename(columns={0: 'asset', 1: 'date', 2: 'nav', 3: 'gross', 4: 'real_close_price', 5: 'pl', 6: 'feed_source', 7: 'return', 8: 'close'})
            df = df.set_index('date')
            df = df.sort_index()
            return df
        return quotes
