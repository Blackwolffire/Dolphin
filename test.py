from algo.database import Database
import algo.formula as formula
import algo.data as data
import time
import matplotlib.pyplot as plt
from pandas import DataFrame

db = Database('data.sqlite')
pf = data.get_portfolio(db.get_portfolio_asset())
print(pf.get_assets())
def get_sh():
        start = time.time()
        s = formula.compute_portfolio_sharpe_ratio(pf, data.START_DATE, data.END_DATE, db)
        print(f'{time.time() - start} seconds')
        return s

df = get_sh()
df.plot(y='close')
quotes = data.get_quote(data.START_DATE, data.END_DATE, pf.asset)
l = [x.__dict__ for x in quotes]
ref_df = DataFrame(l)
ref_df = ref_df.set_index('date')
print(ref_df['close'] - df['close'])
ref_df['return'] = ref_df['return_value']
print(f'estimaed sharpe = {formula.compute_sharp_ratio(ref_df)}')
ref_df.plot(y='close')
print(f'custom sharpe is {formula.compute_sharp_ratio(df)}')
print(data.calculate_ratio([pf.asset], [data.get_sharpe_ratio()], data.START_DATE, data.END_DATE))
plt.show()
