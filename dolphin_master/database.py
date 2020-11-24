import time
from sqlalchemy import create_engine, MetaData, Table, Column, Float, Date, Integer, String, ForeignKey, select, func
import os

from algo.portfolio import Portfolio


class Database:
    def __init__(self):
        engine_url = f'sqlite:///portfolios.sqlite'
        self.db_engine = create_engine(engine_url)
        self.portfolio_table = None
        self.asset_table = None
        self.create_tables()

    def create_tables(self):
        metadata = MetaData()
        self.portfolio_table = Table('portfolio', metadata,
                                 Column('id', Integer, primary_key=True),
                                 Column('created', Float, nullable=False),
                                 Column('finished', Float, nullable=True),
                                 Column('sharpe', Float, nullable=True),
                                 Column('custom_sharpe', Float, nullable=True),
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
        created = time.time()
        cmd = self.portfolio_table.insert().values(created=created)
        res = self.db_engine.execute(cmd)
        id = res.lastrowid
        for asset in portfolio.get_assets():
            self.store_asset(id, asset[0], asset[1])

        return id

    def get_portfolio(self, pf_id: int, assets=True):
        pf = Portfolio()
        pf.id = pf_id
        cmd = self.asset_table.select().where(self.asset_table.c.portfolio_id == pf_id)
        res = self.db_engine.execute(cmd)
        if assets:
            for asset in res.fetchall():
                pf.add_asset(asset[1], asset[2])
        return pf

    def get_portfolio_start_time(self, pf_id):
        cmd = self.portfolio_table.select().where(self.portfolio_table.c.id == pf_id)
        res = self.db_engine.execute(cmd)
        pf = res.fetchall()
        return pf[0][1]

    def store_asset(self, pf_id: int, asset_id: int, quantity: float):
        cmd = self.asset_table.insert().values(portfolio_id=pf_id, asset_id=asset_id, quantity=quantity)
        self.db_engine.execute(cmd)

    def update_portfolio(self, pf_id: int, custom_sharpe: float):
        finished = time.time()
        cmd = self.portfolio_table.update().where(self.portfolio_table.c.id == pf_id).values(finished=finished, custom_sharpe=custom_sharpe)
        self.db_engine.execute(cmd)
        return finished - self.get_portfolio_start_time(pf_id)

    def add_jump_sharpe(self, pf_id, sharpe):
        cmd = self.portfolio_table.update().where(self.portfolio_table.c.id == pf_id).values(sharpe=sharpe)
        self.db_engine.execute(cmd)

    def get_best_custom_portfolios(self, n=10):
        cmd = self.portfolio_table.select().where(self.portfolio_table.c.sharpe == None).order_by(self.portfolio_table.c.custom_sharpe).limit(n)
        res = self.db_engine.execute(cmd)
        return [x for x in res.fetchall()]

    def get_best_portfolio(self):
        cmd = self.portfolio_table.select().order_by(self.portfolio_table.c.sharpe.desc()).limit(1)
        res = self.db_engine.execute(cmd)
        best = [x for x in res.fetchall()]

        if len(best) > 0:
            return best[0]
        else:
            return None
