from datetime import datetime
from math import sqrt

from asset import Asset
from database import Database


def compute_sharp_ratio(asset: Asset, start: datetime, end: datetime) -> float:
    db = Database('new.sqlite')
    df = db.get_quotes([asset], start, end, data_frame=True)
    xdeb = df['close'].head(1).values[0]
    xfin = df['close'].tail(1).values[0]
    roi = (1 + (xfin - xdeb) / xdeb) ** (365 / (df.close.count()-1)) - 1
    df["square"] = df['close'].pow(2)
    to = sqrt(df.square.sum() / (df.close.count() - 1)) * sqrt(252)
    return roi/to