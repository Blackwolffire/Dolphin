from pandas import DataFrame
from sqlalchemy import create_engine, MetaData, Table, Column, Float, Date, Integer, String, ForeignKey

from asset import Asset
from quote import Quote


class Database:
    def __init__(self, path: str):
        engine_url = f'sqlite:///{path}'
        self.db_engine = create_engine(engine_url)
        self.quote_table = None
        self.asset_table = None
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
                                Column('close', Float)
                                )

        try:
            metadata.create_all(self.db_engine)
            print("Tables created")
        except Exception as e:
            print(f"Cannot create table: {e}")

    def add_asset(self, asset: Asset):
        cmd = self.asset_table.insert().values(id=asset.id, label=asset.label, type=asset.type, currency=asset.currency)
        self.db_engine.execute(cmd)

    def add_quote(self, quote: Quote):
        cmd = self.quote_table.insert().values(asset_id=quote.asset, date=quote.date, close=quote.close)
        self.db_engine.execute(cmd)

    def get_assets(self):
        cmd = self.asset_table.select()
        res = self.db_engine.execute(cmd)
        assets = []
        for asset in res.fetchall():
            assets.append(Asset(data=asset))
        return assets

    def get_quotes(self, assets: list, start_date: str, end_date: str):
        asset_ids = [x.id for x in assets]
        cmd = self.quote_table.select().where(self.quote_table.c.asset_id.in_(asset_ids))
        if start_date:
            cmd = cmd.where(self.quote_table.c.date >= start_date)
        if end_date:
            cmd = cmd.where(self.quote_table.c.date <= end_date)
        print(cmd)
        res = self.db_engine.execute(cmd)
        quotes = []
        for quote in res.fetchall():
            print(quote)
            quotes.append(Quote(data=quote))
        return quotes
