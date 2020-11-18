import database
import data
from asset import Asset
import matplotlib.pyplot as plt

db = database.Database('new.sqlite')


def get_portfolio():
    return data.get_portfolio(db.get_portfolio_asset())


def get_assets():
    return db.get_assets()


def get_df(asset: Asset, plot=False):
    df = db.get_quotes([asset], start_date=data.START_DATE, end_date=data.END_DATE, data_frame=True)
    if plot:
        df.plot(y='close')
        plt.show()
    else:
        return df


