import datetime
from math import sqrt

from pandas import DataFrame

from asset import Asset
from database import Database
from portfolio import Portfolio
from quote import Quote


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


def compute_sharp_ratio(asset: Asset, start: str, end: str, db: Database) -> float:
    df = db.get_quotes([asset], start, end, data_frame=True)
    xdeb = df.close.iloc[0]
    xfin = df.close.iloc[-1]

    days = (df.index.max() - df.index.min()).days
    roi = (1 + (xfin - xdeb) / xdeb) ** (365 / days) - 1

    to = compute_volatility(df)

    return (roi - 0.0005)/to


def compute_nav(portfolio: Portfolio, date: str, db: Database):
    assets = [Asset(x['asset']['asset']) for x in portfolio.values['2013-06-14']]
    df = db.get_quotes(assets, start_date=date, end_date=date, data_frame=True)  # TODO: fetch previous value if 0
    if len(df) == 0:
        return None, None
    df.sort_values(by='asset')

    def get_quantity(asset_id):
        q = [x['asset']['quantity'] for x in portfolio.values['2013-06-14'] if int(x['asset']['asset']) == asset_id]
        return q[0]

    df['quantity'] = df['asset'].apply(get_quantity)

    def rate(curr: str):
        if curr != portfolio.currency:
            return db.get_rate(curr, portfolio.currency, date)
        else:
            return 1

    df['real_nav'] = df['nav'] * df['quantity'] * df['currency'].apply(rate)  # TODO: check without currency?
    df['weighted_return'] = (df['return'] * df['quantity']) / df['quantity'].sum()  # TODO: check
    return df['real_nav'].sum(), df['weighted_return'].sum()


def compute_portfolio_sharpe_ratio(portfolio: Portfolio, start_date: str, end_date: str, db: Database) -> float:
    # TODO: delete all previous data in db
    start = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    delta = datetime.timedelta(days=1)

    asset = db.get_test_portfolio()

    while start != end:
        print(start)
        daily_nav, daily_return = compute_nav(portfolio, start.strftime('%Y-%m-%d'), db)
        if start.weekday() in [5, 6] or (daily_nav is None and daily_nav is None):
            start += delta
            continue
        quote = Quote(None)
        quote.create_custom(start, daily_nav, daily_return, asset.id)
        db.add_quote(quote)
        start += delta

    return compute_sharp_ratio(asset, start_date, end_date, db)


def compute_correlation(asset1: Asset, asset2: Asset, start_date: str, end_date: str, db: Database):
    df1 = db.get_quotes([asset1], start_date, end_date, data_frame=True)
    df2 = db.get_quotes([asset2], start_date, end_date, data_frame=True)

    vol1 = compute_volatility(df1, annualized=False)
    vol2 = compute_volatility(df2, annualized=False)

    cov = compute_covariance(df1, df2)
    correlation = cov / (vol1 * vol2)
    return correlation

