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
    for i in range(size, 40):
        aw.append((int(stocks.index[i]), 0.01))
        submit_portfolio(build_portfolio(aw))


def build_portfolio(aw: list) -> Portfolio:
    # assets = [(4354, 0.005), ...]
    amount = data.BUDGET
    pf = Portfolio()
    for a in aw:
        asset = db.get_assets(assets=[a[0]], data_frame=True)
        price = asset.close.iloc[0]
        currency = asset.currency.iloc[0]
        if currency != 'EUR':
            price = price * db.get_rate('EUR', currency, data.START_DATE)
        quantity = int((amount * a[1]) / price)
        pf.add_asset(a[0], quantity)
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


def compute_weights(sharpes):
    sharpe_sum = sum(sharpes)
    weights = [s / sharpe_sum for s in sharpes]
    gap = 0.0
    for i in range(len(weights)):
        if weights[i] > 0.1:
            gap += weights[i] - 0.1
            weights[i] = 0.1
        if weights[i] < 0.01:
            weights[i] = 0.01
    while gap > 0.0:
        maxi = 0
        for i in range(1, len(weights)):
            if weights[i] < 0.1 and (weights[i] > weights[maxi] or weights[maxi] >= 0.1):
                maxi = i
        if gap > 0.1 - weights[maxi]:
            gap -= 0.1 - weights[maxi]
            weights[maxi] = 0.1
        else:
            weights[maxi] += gap
            gap = 0.0
    return weights


def generate_portfolio_3(size: int):
    stocks = db.get_assets(type='STOCK', threshold=0.0)  # Select stocks, ordered by sharpe
    non_stocks = db.get_assets(type=['FUND', 'INDEX', 'PORTFOLIO', 'ETF_FUND'], data_frame=True, threshold=0.0)
    best_stocks = stocks[0:size]

    sharpes = [a.sharpe for a in best_stocks]
    weights = compute_weights(sharpes)
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
        weights = compute_weights(sharpes)
        print(f'weights : {weights}')
        aw = [(a.id, w) for (a, w) in zip(best_stocks, weights)]
        submit_portfolio(build_portfolio(aw))


def generate_portfolio_4(size: int):
    stocks = db.get_assets(type='STOCK', threshold=0.0)  # Select stocks, ordered by sharpe
    all_assets = db.get_assets(type=['STOCK', 'FUND', 'INDEX', 'PORTFOLIO', 'ETF_FUND'], threshold=0.0)
    best_stocks = stocks[0:size]

    sharpes = [a.sharpe for a in best_stocks]

    for i in range(size, 40):
        asset = all_assets.pop(0)
        best_stocks.append(asset)
        sharpes.append(asset.sharpe)
        weights = compute_weights(sharpes)
        print(f'weights : {weights}')
        aw = [(a.id, w) for (a, w) in zip(best_stocks, weights)]
        submit_portfolio(build_portfolio(aw))


def generate_portfolio_5(size: int):
    decorrel = [1455, 1519, 1521, 1745, 2128, 2189, 2190, 2191]
    dasset = db.get_assets(assets=decorrel)
    stocks = db.get_assets(type='STOCK', threshold=0.0)  # Select stocks, ordered by sharpe
    all_assets = db.get_assets(type=['STOCK', 'FUND', 'INDEX', 'PORTFOLIO', 'ETF_FUND'], threshold=0.0)
    best_stocks = stocks[0:size]
    best_stocks = best_stocks + dasset
    sharpes = [a.sharpe for a in best_stocks]
    weights = compute_weights(sharpes)
    aw = [(a.id, w) for (a, w) in zip(best_stocks, weights)]
    submit_portfolio(build_portfolio(aw))

    for i in range(size, 40):
        asset = all_assets.pop(0)
        best_stocks.append(asset)
        sharpes.append(asset.sharpe)
        weights = compute_weights(sharpes)
        print(f'weights : {weights}')
        aw = [(a.id, w) for (a, w) in zip(best_stocks, weights)]
        submit_portfolio(build_portfolio(aw))


def generate_portfolio_6(size: int = 20):
    stocks = db.get_assets(type='STOCK', threshold=0.0)  # Select stocks, ordered by sharpe
    all_assets = db.get_assets(type=['STOCK', 'FUND', 'INDEX', 'PORTFOLIO', 'ETF_FUND'], threshold=0.0)
    best_stocks = stocks[0:size]
    other_b_stocks = all_assets[0:size]

    sharpes = [a.sharpe for a in best_stocks]
    for ob in other_b_stocks:
        sharpes.append(ob.sharpe)
    weights = compute_weights(sharpes[0:int(size/2)])
    weights += compute_weights(sharpes[int(size/2):])
    for i in range(len(weights)):
        weights[i] /= 2
    aw = [(a.id, w) for (a, w) in zip(best_stocks + other_b_stocks, weights)]
    submit_portfolio(build_portfolio(aw))

    for i in range(size):
        weights[i] = 0.1
        for j in range(size):
            if j != i:
                weights[j] = 0.4/(size-1)
        for j in range(size):
            weights[j+size] = 0.1
            for k in range(size, size * 2):
                if k != j + size:
                    weights[k] = 0.4/(size-1)

            print(f'weights : {weights}')
            aw = [(a.id, w) for (a, w) in zip(best_stocks + other_b_stocks, weights)]
            submit_portfolio(build_portfolio(aw))
"""
    for i in range(size):
        for ii in range(size):
            if i == ii:
                continue
            weights[i] = 0.1
            weights[ii] = 0.1
            for j in range(size):
                if j != i and j != ii:
                    weights[j] = 0.3/(size-2)
            for j in range(size):
                for jj in range(size):
                    if j == jj:
                        continue
                    weights[j+size] = 0.1
                    weights[jj+size] = 0.1
                    for k in range(size, size * 2):
                        if k != j + size:
                            weights[k] = 0.3/(size-2)

                    print(f'weights : {weights}')
                    aw = [(a.id, w) for (a, w) in zip(best_stocks + other_b_stocks, weights)]
                    submit_portfolio(build_portfolio(aw))
    """


def generate_portfolio_7(size: int = 20):
    stocks = db.get_assets(type='STOCK', threshold=0.0)  # Select stocks, ordered by sharpe
    best_stocks = stocks[0:size]
    correl_assets = []
    correl_asset_id = set()
    list_correlation = []
    # Get negatively correlated stocks
    for asset in best_stocks:  # For each asset in the best stocks, get correlated
        correlated = db.get_correlated(asset.id, inverse=True, threshold=0.2)  # Get assets with negative correlation
        correlated_assets = db.get_assets(assets=[x[0] for x in correlated],
                                          type=['STOCK', 'FUND', 'INDEX', 'PORTFOLIO',
                                                'ETF_FUND'], threshold=0)  # elect the corresponding assets
        for c in correlated_assets:
            if c.id not in correl_asset_id:
                correl_asset_id.add(c.id)
                correl_assets.append(c)
        list_correlation += [(x[0], x[1], asset) for x in correlated]

    sharpes = [a.sharpe for a in best_stocks]
    sorted_assets = correl_assets
    heapq._heapify_max(sorted_assets)

    print(f'correl asset list size: {len(sorted_assets)}')
    for i in range(size, min(size + len(sorted_assets), 40)):
        asset = heapq._heappop_max(sorted_assets)
        best_stocks.append(asset)
        sharpes.append(asset.sharpe)
        weights = compute_weights(sharpes)
        print(f'weights : {weights}')
        aw = [(a.id, w) for (a, w) in zip(best_stocks, weights)]
        submit_portfolio(build_portfolio(aw))


def generate_portfolio_8(size: int = 20):
    stocks = db.get_assets(type='STOCK', threshold=0.0)  # Select stocks, ordered by sharpe
    best_stocks = stocks[0:size]
    correl_assets = []
    correl_asset_id = set()
    list_correlation = []
    # Get negatively correlated stocks
    for asset in best_stocks:  # For each asset in the best stocks, get correlated
        correlated = db.get_correlated(asset.id, inverse=True,
                                       threshold=0.2)  # Get assets with negative correlation
        correlated_assets = db.get_assets(assets=[x[0] for x in correlated],
                                          type=['STOCK', 'FUND', 'INDEX', 'PORTFOLIO',
                                                'ETF_FUND'], threshold=0)  # elect the corresponding assets
        for c in correlated_assets:
            if c.id not in correl_asset_id:
                correl_asset_id.add(c.id)
                correl_assets.append(c)
        list_correlation += [(x[0], x[1], asset) for x in correlated]
    sharpes = [a.sharpe for a in best_stocks]

    correl_assets = sorted(correl_assets, reverse=True)
    sorted_assets = correl_assets[0: size]
    for s in sorted_assets:
        sharpes.append(s.sharpe)
    weights = compute_weights(sharpes[0:int(size/2)])
    weights += compute_weights(sharpes[int(size/2):])
    for i in range(len(weights)):
        weights[i] /= 2
    aw = [(a.id, w) for (a, w) in zip(best_stocks + sorted_assets, weights)]
    submit_portfolio(build_portfolio(aw))

    for i in range(size):
        weights[i] = 0.1
        for j in range(size):
            if j != i:
                weights[j] = 0.4/(size-1)
        for j in range(size):
            weights[j+size] = 0.1
            for k in range(size, size * 2):
                if k != j + size:
                    weights[k] = 0.4/(size-1)

            print(f'weights : {weights}')
            aw = [(a.id, w) for (a, w) in zip(best_stocks + sorted_assets, weights)]
            submit_portfolio(build_portfolio(aw))














