import os

import requests
from algo import formula, database, data
from algo.portfolio import Portfolio

from influxdb import InfluxDBClient
db = database.Database('../data.sqlite')


def get_new_job() -> dict:
    res = requests.get('http://127.0.0.1:8000/new_pf')
    if res.status_code != 200:
        return {}
    return res.json()  # {'pf_id', 'assets': [{'asset_id': qty}, {...}]}


def compute_sharpe(pf: dict) -> float:
    portfolio = Portfolio()
    portfolio.id = pf['id']
    for a in pf['assets']:
        portfolio.add_asset(a[0], a[1])
    return formula.compute_portfolio_sharpe_ratio(portfolio, data.START_DATE, data.END_DATE, db)


def upload_result(pf_id: int, sharpe: float):
    res = requests.post(f'http://127.0.0.1:8000/pf_sharpe/{pf_id}', data={'sharpe': sharpe})
    if res.status_code != 200:
        return False
    return True


def work_once():
    new_pf = get_new_job()
    if not new_pf:
        return
    sharpe = compute_sharpe(new_pf)
    upload_result(new_pf['pf_id'], sharpe)
    # Log new sharpe to influx


def main():
    while True:
        work_once()
        # Log new work to influx


if __name__ == '__main__':
    main()
