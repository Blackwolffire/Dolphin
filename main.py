from data import get_assets, get_quote
from database import Database

start_date = '2016-06-01'
end_date = '2020-10-23'

db = Database('test.sqlite')
assets = get_assets(date='')
for asset in assets:
    print(asset)
    for quote in get_quote(start_date, end_date, asset):
        db.add_quote(quote)