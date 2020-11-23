import heapq

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
    print(f'correl asset list size: {len(sorted_assets)}')
    for j in range(15):
        popi = heapq._heappop_max(sorted_assets)
        best_stocks.append(popi)
        print(f'{popi} : sharpe {popi.sharpe}')
        print('correlation :')
        for poco in [(x[1], x[2].label) for x in list_correlation if x[0] == popi.id]:
            print(f'{poco[0]} with {poco[1]}')
        yield best_stocks  # TODO: yield a Portfolio
