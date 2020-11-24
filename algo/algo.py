import heapq
import json
from time import sleep

import requests
from pandas import DataFrame

from algo import data
from algo.data import START_DATE
from algo.database import Database
from algo.portfolio import Portfolio

db = Database('data.sqlite')


def generate_portfolio():
    stocks = db.get_assets(type='STOCK')  # Select stocks, ordered by sharpe
    best_stocks = stocks[0:15]  # Select the 15 best

    correl_assets = []
    correl_asset_id = set()
    list_correlation = []
    # Get negatively correlated stocks
    for asset in best_stocks:  # For each asset in the best stocks, get correlated
        correlated = db.get_correlated(asset.id, inverse=True, threshold=-0.05)  # Get assets with negative correlation
        correlated_assets = db.get_assets(assets=[x[0] for x in correlated],
                                          type=['FUND', 'INDEX', 'PORTFOLIO', 'ETF_FUND'])  # elect the corresponding assets
        for c in correlated_assets:
            if c.id not in correl_asset_id:
                correl_asset_id.add(c.id)
                correl_assets.append(c)
        list_correlation += [(x[0], x[1], asset) for x in correlated]

    sorted_assets = correl_assets
    heapq._heapify_max(sorted_assets)
    # print(list_correlation)

    portfolio = Portfolio(asset=db.get_assets(assets=[1831])[0])
    for a in best_stocks:
        portfolio.add_asset(a, a.to_quantity(10000, START_DATE, db))
    yield portfolio

    print(f'correl asset list size: {len(sorted_assets)}')
    for j in range(15):
        popi = heapq._heappop_max(sorted_assets)
        best_stocks.append(popi)
        print(f'{popi} : sharpe {popi.sharpe}')
        print('correlation :')
        for poco in [(x[1], x[2].label) for x in list_correlation if x[0] == popi.id]:
            print(f'{poco[0]} with {poco[1]}')
        portfolio = Portfolio()
        for a in best_stocks:
            portfolio.add_asset(a, a.to_quantity(10000, START_DATE, db))
        yield portfolio


def generate_portfolio_1(size: int):
    stocks = db.get_assets(type='STOCK', data_frame=True)  # Select stocks, ordered by sharpe
    non_stocks = db.get_assets(type=['FUND', 'INDEX', 'PORTFOLIO', 'ETF_FUND'], data_frame=True)
    best_stocks = stocks.head(size)  # Select the size best stocks

    weights = [0.01 for _ in range(size)]
    aw = [(a, w) for a, w in zip(best_stocks.index, weights)]
    submit_portfolio(build_portfolio(aw))
    for i in range(size, 100):
        aw.append((int(stocks.index[i]), 0.01))
        submit_portfolio(build_portfolio(aw))


def build_portfolio(assets: list) -> Portfolio:
    # assets = [(4354, 0.005), ...]
    amount = 1000000
    pf = Portfolio()

    for a in assets:
        asset = db.get_assets(assets=[a[0]], data_frame=True)
        price = asset.close.iloc[0]
        currency = asset.currency.iloc[0]
        if currency != 'EUR':
            price = price * db.get_rate('EUR', currency, data.START_DATE)
        pf.add_asset(a[0], int((amount * a[1]) / price))

    return pf


def submit_portfolio(pf: Portfolio):
    payload = json.dumps({'assets': pf.get_assets()})
    print(payload)
    res = requests.post('http://api.finance.debuisne.fr/new_pf', data=payload)
    sleep(0.05)
    return res.status_code


def generate_portfolio_2(size: int):
    stocks = db.get_assets(type='STOCK')  # Select stocks, ordered by sharpe
    non_stocks = db.get_assets(type=['FUND', 'INDEX', 'PORTFOLIO', 'ETF_FUND'], data_frame=True)
    best_stocks = stocks[0:size]

    weights = [0.01 for _ in range(size)]
    aw = [(a.id, w) for a, w in zip(best_stocks, weights)]

    correl_assets = []
    correl_asset_id = set()
    list_correlation = []
    # Get negatively correlated stocks
    for asset in best_stocks:  # For each asset in the best stocks, get correlated
        correlated = db.get_correlated(asset.id, inverse=True, threshold=-0.05)  # Get assets with negative correlation
        correlated_assets = db.get_assets(assets=[x[0] for x in correlated],
                                          type=['FUND', 'INDEX', 'PORTFOLIO',
                                                'ETF_FUND'])  # elect the corresponding assets
        for c in correlated_assets:
            if c.id not in correl_asset_id:
                correl_asset_id.add(c.id)
                correl_assets.append(c)
        list_correlation += [(x[0], x[1], asset) for x in correlated]

    sorted_assets = correl_assets
    heapq._heapify_max(sorted_assets)

    print(f'correl asset list size: {len(sorted_assets)}')
    for i in range(size, size + len(sorted_assets)):
        asset = heapq._heappop_max(sorted_assets)
        aw.append((int(asset.id), 0.1))
        submit_portfolio(build_portfolio(aw))

######## SOS CODE DUPLICATED #########

def generate_portfolio_3(size: int):
    stocks = db.get_assets(type='STOCK', threshold=0.0)  # Select stocks, ordered by sharpe
    non_stocks = db.get_assets(type=['FUND', 'INDEX', 'PORTFOLIO', 'ETF_FUND'], data_frame=True, threshold=0.0)
    best_stocks = stocks[0:size]

    sharpes = [a.sharpe for a in best_stocks]
    sharpe_sum = sum(sharpes)
    weights = [s/sharpe_sum for s in sharpes]
    gap = 0.0
    for i in range(weights):
        if weights[i] > 0.1:
            gap += weights - 0.1
            weights[i] = 0.1
        if weights[i] < 0.01:
            weights[i] = 0.01
    while gap != 0:
        maxi = 0
        for i in range(1, weights):
            if weights[i] < 0.1 and (weights[i] > weights[maxi] or weights[maxi] > 0.1):
                maxi = i
        if gap > 0.1 - weights[maxi]:
            gap -= 0.1 - weights[maxi]
            weights[maxi] = 0.1
        else:
            weights[maxi] += gap
            gap = 0.0

    aw = [(a.id, w) for (a, w) in zip(best_stocks, weights)]

    correl_assets = []
    correl_asset_id = set()
    list_correlation = []
    # Get negatively correlated stocks
    for asset in best_stocks:  # For each asset in the best stocks, get correlated
        correlated = db.get_correlated(asset.id, inverse=True, threshold=0)  # Get assets with negative correlation
        correlated_assets = db.get_assets(assets=[x[0] for x in correlated],
                                          type=['FUND', 'INDEX', 'PORTFOLIO',
                                                'ETF_FUND'], threshold=1.0)  # elect the corresponding assets
        for c in correlated_assets:
            if c.id not in correl_asset_id:
                correl_asset_id.add(c.id)
                correl_assets.append(c)
        list_correlation += [(x[0], x[1], asset) for x in correlated]

    sorted_assets = correl_assets
    heapq._heapify_max(sorted_assets)

    print(f'correl asset list size: {len(sorted_assets)}')
    for i in range(size, size + len(sorted_assets)):
        asset = heapq._heappop_max(sorted_assets)
        best_stocks.append(asset)
        sharpes.append(asset.sharpe)
        sharpe_sum = sum(sharpes)
        weights = [s/sharpe_sum for s in sharpes]
        print(f'weights : {weights}')
        aw = [(a.id, w) for (a, w) in zip(best_stocks, weights)]
        submit_portfolio(build_portfolio(aw))

    for (i, a) in zip(range(len(aw)), aw):
        if i % 2:
            a[1] *= 2
        else:
            a[1] = a[1] / 2
    submit_portfolio(build_portfolio(aw))

    """
    for (i, a) in zip(range(len(aw)), aw):
        if i % 2:
            a[1] *= 2
        else:
            a[1] /= 2
    """
