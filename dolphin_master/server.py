import sys
import time
from threading import Thread

import uvicorn
from fastapi import FastAPI, Request
import os
from starlette.middleware.cors import CORSMiddleware
from influxdb import InfluxDBClient
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import json

from algo.portfolio import Portfolio
from dolphin_master.database import Database
import algo.data as data
from algo.database import Database as MarketDatabase


db = Database()
market_db = MarketDatabase('../data.sqlite')
new_pfs = set()
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

influxdb_host = os.getenv('INFLUX_HOST', 'localhost')
client = InfluxDBClient(influxdb_host,
                        8086,
                        os.environ.get("INFLUXDB_USER", "worker"),
                        os.environ.get("INFLUXDB_USER_PASSWORD", "worker"),
                        os.environ.get("INFLUXDB_DB", "worker"))


def get_metrics():
    return {}


def get_info() -> dict:
    metrics = get_metrics()
    count = client.query('select count(*) from "worker"."autogen"."sharpe";')
    try:
        total_calculated = [x for x in count][0][0]['count_custom_sharpe']
    except Exception as e:
        print(e)
        total_calculated = 0
    best_pf = db.get_best_portfolio()
    print(best_pf)
    info = {
        'best_sharpe': str(best_pf[3]),
        'best_portfolio': db.get_portfolio(best_pf[0]).get_assets() if best_pf[0] != 0 else [[0,0]],
        'current_rate': metrics.get('current_rate', 0),
        'total_calculated': str(total_calculated),
        'mean': metrics.get('mean', 0),
        'queue': len(new_pfs)
    }
    return info


@app.get('/info')
async def return_info():
    return json.dumps(get_info())


@app.get('/new_pf')
def dispatch_portfolio():
    if len(new_pfs) > 0:
        new_pf = new_pfs.pop()
        id = db.store_portfolio(new_pf)
        # Log new dispatch to influxdb
        client.write_points([{
            "measurement": "dispatch",
            "fields": {
                "value": 1
            }
        }])
        return {'id': id, 'assets': new_pf.get_assets()}
    else:
        return 'empty'


@app.post('/pf_sharpe/{pf_id}')
async def store_new_sharpe(pf_id: int, request: Request):
    data = await request.json()
    sharpe = data['sharpe'] if 'sharpe' in data else 0
    compute_time = db.update_portfolio(pf_id, sharpe)
    # Log new dispatch to influxdb
    client.write_points([{
        "measurement": "sharpe",
        "fields": {
            "custom_sharpe": sharpe,
            "compute_time": compute_time
        }
    }])
    return 'OK'


@app.post('/new_pf')
async def receive_new_portfolio(request: Request):
    data = await request.json()
    pf = Portfolio()
    for asset in data['assets']:
        pf.add_asset(asset[0], asset[1])
    new_pfs.add(pf)
    return 'OK'

@app.get('/check_jump/{size}')
def check_jump(size: int):
    jump_check_thread(size)

def jump_check_thread(size: int):
    # Get best portfolios
    try:
        pfs = db.get_best_custom_portfolios(size)
        pf_asset = market_db.get_portfolio_asset()
        for pf in pfs:
            print(pf)
            portfolio = db.get_portfolio(pf[0])
            portfolio.asset = pf_asset
            data.update_portfolio(portfolio)
            data.update_portfolio(portfolio) # Update twice for jump
            sharpe = data.get_pf_sharpe(portfolio.asset)
            db.add_jump_sharpe(pf[0], sharpe) # Not thread safe here
            client.write_points([{
                "measurement": "sharpe",
                "fields": {
                    "jump_sharpe": sharpe,
                }
            }])
        # Fetch the best portfolio and send it to JUMP
        best_pf = db.get_best_portfolio()
        best_portfolio = db.get_portfolio(best_pf[0])
        best_portfolio.asset = pf_asset
        data.update_portfolio(best_portfolio)
        data.update_portfolio(best_portfolio)
    except Exception as e:
        print(e)

def worker_thread():
    while True:
        jump_check_thread(50)
        time.sleep(30)

if __name__ == '__main__':
    thread = Thread(target=worker_thread)
    thread.start()
    uvicorn.run(app, host="0.0.0.0", port=8000)
