import json
import os
from time import sleep
import requests
import logging
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from algo import formula, database, data
from algo.portfolio import Portfolio

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)
db = database.Database('../data.sqlite')

SERVER = os.getenv('SERVER', 'localhost')


def get_new_job():
    try:
        res = requests.get(f'{SERVER}/new_pf')
        if res.status_code != 200:
            return {}
        return res.json()  # {'pf_id', 'assets': [{'asset_id': qty}, {...}]}
    except Exception:
        return 'empty'


def compute_sharpe(pf: dict) -> float:
    portfolio = Portfolio()
    portfolio.id = pf['id']
    for a in pf['assets']:
        portfolio.add_asset(a[0], a[1])
    return formula.compute_portfolio_sharpe_ratio(portfolio, data.START_DATE, data.END_DATE, db)


def upload_result(pf_id: int, sharpe: float):
    res = requests.post(f'{SERVER}/pf_sharpe/{pf_id}', data=json.dumps({'sharpe': sharpe}))
    if res.status_code != 200:
        return False
    return True


def work_once(pf: dict):
    sharpe = compute_sharpe(pf)
    upload_result(pf['id'], sharpe)


def main():
    while True:
        new_pf = get_new_job()
        if new_pf == 'empty':
            logging.info('No work to do, waiting 10s...')
            sleep(10)
            continue
        try:
            work_once(new_pf)
        except Exception as e:
            logging.error(f'Could not compute portfolio sharpe: {e}')
            sleep(5)
            continue


if __name__ == '__main__':
    main()
