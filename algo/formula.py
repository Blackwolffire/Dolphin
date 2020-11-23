import datetime
from math import sqrt
from numpy import ndarray

from pandas import DataFrame, Series

from algo.asset import Asset
from algo.database import Database
from algo.portfolio import Portfolio


def compute_volatility(df: DataFrame, annualized=True) -> float:
    mean = df['return'].mean()
    df['variance'] = (df['return'] - mean).pow(2) / (df['return'].count() - 1)
    if annualized:
        vol = sqrt(df.variance.sum()) * sqrt(252)
    else:
        vol = sqrt(df.variance.sum())
    return vol


def compute_covariance(df1: DataFrame, df2: DataFrame) -> float:
    mean1 = df1['return'].mean()
    mean2 = df2['return'].mean()

    df1['covariance'] = ((df1['return'] - mean1) * (df2['return'] - mean2)) / (df2['return'].count() - 1)
    cov = df1['covariance'].sum()
    return cov


def compute_sharp_ratio(df: DataFrame) -> float:
    xdeb = df.close.iloc[0]
    xfin = df.close.iloc[-1]

    days = (df.index.max() - df.index.min()).days
    roi = (1 + (xfin - xdeb) / xdeb) ** (365 / days) - 1

    to = compute_volatility(df)

    return (roi - 0.0005)/to


def compute_nav_return(portfolio: Portfolio, date: str, db: Database, quantity: ndarray):
    assets = [x[0] for x in portfolio.get_assets()]
    df = db.get_quotes(assets, start_date=date, end_date=date, data_frame=True)

    if len(df) == 0:
        return None, None

    df['real_nav'] = df['nav'] * quantity
    daily_nav = df['real_nav'].sum()
    df['weighted_return'] = (df['return'] * df['real_nav']) / daily_nav
    return daily_nav, df['weighted_return'].sum()


def compute_portfolio_sharpe_ratio(portfolio: Portfolio, start_date: str, end_date: str, db: Database) -> float:
    start = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    delta = datetime.timedelta(days=15)

    def get_quantity(asset_id):
        return next((x[1] for x in portfolio.get_assets() if x[0] == asset_id))

    df_1 = db.get_quotes([x[0] for x in portfolio.get_assets()], start_date, start_date, data_frame=True)
    df_1['quantity'] = df_1['asset'].apply(get_quantity)

    def rate(curr: str):
        if curr != portfolio.currency:
            return db.get_rate(curr, portfolio.currency, start_date)
        else:
            return 1
    df_1['quantity'] = df_1['currency'].apply(rate) * df_1['quantity']

    pf_data = []
    while start < end:
        daily_nav, daily_return = compute_nav_return(portfolio, start.strftime('%Y-%m-%d'), db, df_1['quantity'].values)
        if start.weekday() in [5, 6] or (daily_nav is None and daily_nav is None):
            start += delta
            continue
        pf_data.append((start, daily_nav, daily_return))
        start += delta

    df = DataFrame(pf_data)
    df = df.rename(columns={0: 'date', 1: 'close', 2: 'return'})
    df = df.set_index('date')
    return compute_sharp_ratio(df)


def compute_correlation(asset1: Asset, asset2: Asset, start_date: str, end_date: str, db: Database):
    df1 = db.get_quotes([asset1], start_date, end_date, data_frame=True)
    df2 = db.get_quotes([asset2], start_date, end_date, data_frame=True)

    vol1 = compute_volatility(df1, annualized=False)
    vol2 = compute_volatility(df2, annualized=False)

    cov = compute_covariance(df1, df2)
    correlation = cov / (vol1 * vol2)
    return correlation

