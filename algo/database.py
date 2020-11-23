from typing import Union

from pandas import DataFrame
from sqlalchemy import create_engine, MetaData, Table, Column, Float, Date, Integer, String, ForeignKey, select

from algo import data
from algo.asset import Asset
from algo.quote import Quote


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
                                 Column('currency', String, nullable=True),
                                 Column('sharpe', Float),
                                 Column('sharpe_custom', Float),
                                 Column('return_', Float),
                                 Column('ann_return', Float),
                                 Column('close', Float)
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

        self.correlation_matrix_table = Table('correlation_matrix', metadata,
                                              Column('asset_left', Integer),
                                              Column('asset_right', Integer),
                                              Column('correlation', Float))

        try:
            metadata.create_all(self.db_engine)
        except Exception as e:
            print(f"Cannot create table: {e}")

    def add_asset(self, asset: Asset):
        cmd = self.asset_table.insert().values(id=asset.id, label=asset.label, type=asset.type, currency=asset.currency)
        self.db_engine.execute(cmd)

    def add_close(self, asset: Asset, close: float):
        cmd = self.asset_table.update().where(self.asset_table.c.id == asset.id).values(close=close)
        self.db_engine.execute(cmd)

    def add_correlation(self, asset1: Asset, asset2: Asset, correlation: float):
        cmd = self.correlation_matrix_table.insert().values(asset_left=asset1.id, asset_right=asset2.id, correlation=correlation)
        self.db_engine.execute(cmd)

    def get_correlation(self, asset1: Union[Asset, int], asset2: Union[Asset, int]) -> float:
        a1 = asset1 if isinstance(asset1, int) else asset1.id
        a2 = asset2 if isinstance(asset2, int) else asset2.id
        cmd = self.correlation_matrix_table.select(self.correlation_matrix_table.c.correlation).where(
            (self.correlation_matrix_table.c.asset_left == a1) &
            (self.correlation_matrix_table.c.asset_right == a2)
        )
        res = self.db_engine.execute(cmd)
        corr = res.fetchall()
        return corr[0][2]

    def get_correlated(self, asset: Union[Asset, int], inverse=True):
        asset_id = asset if isinstance(asset, int) else asset.id
        cmd = self.correlation_matrix_table.select().where(
            (self.correlation_matrix_table.c.asset_left == asset_id) |
            (self.correlation_matrix_table.c.asset_right == asset_id)
        )
        if inverse:
            cmd = cmd.where(self.correlation_matrix_table.c.correlation < 0)
        else:
            cmd = cmd.where(self.correlation_matrix_table.c.correlation >= 0)

        res = self.db_engine.execute(cmd)
        # return res.fetchall()
        return [x[0] if x[1] == asset_id else x[1] for x in res.fetchall()]

    def update_asset_sharpe(self, asset: Asset, sharpe: float, custom=False):
        if not custom:
            cmd = self.asset_table.update().where(self.asset_table.c.id == asset.id).values(sharpe=sharpe)
        else:
            cmd = self.asset_table.update().where(self.asset_table.c.id == asset.id).values(sharpe_custom=sharpe)
        self.db_engine.execute(cmd)

    def update_asset_returns(self, asset: Asset, return_: float, ann_return: float):
        cmd = self.asset_table.update().where(self.asset_table.c.id == asset.id).values(return_=return_,
                                                                                        ann_return=ann_return)
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
        return rate[0][2]

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

    def add_custom_quote(self, quote, date):
        cmd = self.quote_table.insert().values(
            asset_id=int(quote.asset),
            date=date,
            close=quote.close,
            nav=quote.nav,
            pl=quote.pl,
            gross=quote.gross,
            real_close_price=quote.real_close_price,
            feed_source=quote.feed_source,
            return_value=quote['return'])
        self.db_engine.execute(cmd)

    def get_assets(self, data_frame=False, type:Union[str, list] = None, assets: list =None):
        cmd = self.asset_table.select().order_by(self.asset_table.c.sharpe.desc())
        if type:
            cmd = cmd.where(self.asset_table.c.type.in_(type if isinstance(type, list) else [type]))
        if assets:
            cmd = cmd.where(self.asset_table.c.id.in_(assets))

        print(cmd)
        res = self.db_engine.execute(cmd)
        assets = []
        for asset in res.fetchall():
            if data_frame:
                a = asset
            else:
                a = Asset(data=asset)
            assets.append(a)
        if data_frame:
            df = DataFrame(assets)
            df = df.rename(columns={0: 'id', 1: 'label', 2: 'type', 3: 'currency', 4: 'sharpe', 5: 'custom_sharpe', 6: 'return', 7: 'ann_return', 8: 'close'})
            df = df.set_index('id')
            return df

        return assets

    def get_portfolio_asset(self) -> Asset:
        name = 'EPITA_PTF_12'
        cmd = self.asset_table.select().where(self.asset_table.c.label == name)
        res = self.db_engine.execute(cmd)
        asset = res.fetchall()
        return Asset(data=asset[0])

    def get_test_portfolio(self) -> Asset:
        name = 'CUSTOM_PORTFOLIO'
        cmd = self.asset_table.select().where(self.asset_table.c.label == name)
        res = self.db_engine.execute(cmd)
        asset = res.fetchall()
        return Asset(data=asset[0])

    def get_quotes(self, assets: list, start_date: str, end_date: str, data_frame=False):
        asset_ids = assets if isinstance(assets[0], int) else [x.id for x in assets]
        columns = [x for x in self.quote_table.columns]
        if data_frame:
            columns.append(self.asset_table.c.currency)
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
        if data_frame and len(quotes) > 0:
            df = DataFrame(quotes)
            df = df.rename(
                columns={0: 'asset', 1: 'date', 2: 'nav', 3: 'gross', 4: 'real_close_price', 5: 'pl', 6: 'feed_source',
                         7: 'return', 8: 'close', 9: 'currency'})
            df = df.set_index('date')
            df = df.sort_index()
            return df
        return quotes

    def fill_empty_days(self):
        assets = self.get_assets()
        ref_asset = self.get_portfolio_asset()
        df = self.get_quotes([ref_asset], data.START_DATE, data.END_DATE, data_frame=True)  # This is the reference for the days
        missing = 0
        # Insert for the first date
        # for asset in assets:
        #     asset_df = self.get_quotes([asset], data.START_DATE, data.END_DATE, data_frame=True)
        #     if asset_df.index[0] != df.index[0]:
        #         print(f'First price is {asset_df.iloc[0].close} for asset {asset_df.iloc[0].asset}')
        #         good_asset = asset_df.iloc[0]
        #         self.add_custom_quote(good_asset, date=df.index[0])
        #         missing += 1

        for asset in assets:
            asset_df = self.get_quotes([asset], data.START_DATE, data.END_DATE, data_frame=True)
            for date in df.index:
                if date not in asset_df.index:
                    print(asset)
                    quotes = self.get_quotes([asset], data.START_DATE, date, data_frame=True)
                    good_asset = quotes.iloc[-1]
                    self.add_custom_quote(good_asset, date=date)
                    missing += 1

        print(f'Missing {missing} assets')