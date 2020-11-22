from datetime import datetime

from sqlalchemy import create_engine, MetaData, Table, Column, Float, Date, Integer, String, ForeignKey, select, func
import os

from sqlalchemy.orm import Query

from algo.portfolio import Portfolio


class Database:
    def __init__(self):
        username = os.getenv('PSQL_USERNAME')
        password = os.getenv('PSQL_PASSWORD')
        engine_url = f'postgresql://{username}:{password}@posgtres:5432/portfolios'
        self.db_engine = create_engine(engine_url)
        self.portfolio_table = None
        self.asset_table = None
        self.create_tables()

    def create_tables(self):
        metadata = MetaData()
        self.portfolio_table = Table('portfolio', metadata,
                                 Column('id', Integer, primary_key=True),
                                 Column('created', Date, nullable=False),
                                 Column('finished', String, nullable=True),
                                 Column('sharpe', Float, nullable=True),
                                 Column('return', Float, nullable=True),
                                 )

        self.asset_table = Table('asset', metadata,
                                 Column('portfolio_id', None, ForeignKey("portfolio.id")),
                                 Column('asset_id', Integer, nullable=False),
                                 Column('quantity', Float, nullable=False),
                                 )

        try:
            metadata.create_all(self.db_engine)
        except Exception as e:
            print(f"Cannot create table: {e}")

    def store_portfolio(self, portfolio: Portfolio):
        created = datetime.now()
        cmd = self.portfolio_table.insert().values(created=created)
        res = self.db_engine.execute(cmd)
        id = res.fetchall()[0]
        for asset in portfolio.get_assets():
            a = asset['asset']
            self.store_asset(id, a['asset'], a['quantity'])

        return id

    def store_asset(self, pf_id: int, asset_id: int, quantity: float):
        cmd = self.asset_table.insert().values(portfolio_id=pf_id, asset_id=asset_id, quantity=quantity)
        self.db_engine.execute(cmd)

    def update_portfolio(self, pf_id: int, sharpe: float):
        finished = datetime.now()
        cmd = self.portfolio_table.update().where(id == pf_id).values(finished=finished, sharpe=sharpe)
        self.db_engine.execute(cmd)

    def get_best_portfolios(self):
        best_sharpe = Query(func.max(self.portfolio_table.c.sharpe))
        cmd = self.portfolio_table.select().where(self.portfolio_table.c.sharpe == best_sharpe)
        res = self.db_engine.execute(cmd)
        return res.fetchall()
