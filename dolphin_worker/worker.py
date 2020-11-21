import requests
import database
import formula
import data
from portfolio import Portfolio

db = database.Database('new.sqlite')


def get_new_job() -> dict:
    res = requests.get('http://manager/new_pf')
    if res.status_code != 200:
        return {}
    return res.json()  # {'pf_id', 'assets': {'asset_id': qty, ...}}


def compute_sharpe(pf: Portfolio) -> float:
    return formula.compute_portfolio_sharpe_ratio(pf, data.START_DATE, data.END_DATE, db)


def upload_result(pf_id: int, sharpe: float):
    res = requests.post(f'http://manager/pf_sharpe/{pf_id}', data={'sharpe': sharpe})
    if res.status_code != 200:
        return False
    return True


def work_once():
    new_pf = get_new_job()
    if not new_pf:
        return
    sharpe = compute_sharpe(new_pf)
    upload_result(new_pf.id, sharpe)


while True:
    work_once()
