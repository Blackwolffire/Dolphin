from algo.database import Database
from algo.portfolio import Portfolio

db = Database('data.sqlite')


def generate_portfolio() -> Portfolio:
    pairs = []
    stocks = db.get_assets(data_frame=True, type='STOCK') # Select stocks, ordered by sharpe
    best_stocks = stocks.head(15) # Select the 15 best

    # Get negatively correlated stocks
    for asset_id in best_stocks.index: # For each asset in the best stocks, get correlated
        correlated = db.get_correlated(asset_id, inverse=True)  # Get assets with negative correlation
        correlated_assets = db.get_assets(data_frame=True, assets=correlated,
                                          type=['FUND', 'INDEX', 'PORTFOLIO', 'ETF_FUND'])  # select the corresponding assets
        best_associated_stock = correlated_assets.index[0]  # Get the asset with the highest sharpe
        pairs.append((asset_id, best_associated_stock))  # Append the pair
