import datetime

import database
import data
db = database.Database('new.sqlite')
currencies = ['EUR', 'USD', 'JPY']

start = datetime.datetime.strptime('2018-10-10', '%Y-%m-%d')
end = datetime.datetime.strptime(data.END_DATE, '%Y-%m-%d')
delta = datetime.timedelta(days=1)

while start != end:
    print(start)
    for c1 in currencies:
        for c2 in currencies:
            if c1 != c2:
                rate = data.get_currency_rate(c1, c2, start.strftime('%Y-%m-%d'))
                db.add_currency(c1, c2, start, rate)

    start += delta