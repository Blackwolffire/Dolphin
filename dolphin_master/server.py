import sys
import uvicorn
from fastapi import FastAPI, Request
import os
from starlette.middleware.cors import CORSMiddleware
from influxdb import InfluxDBClient
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import json

from algo.portfolio import Portfolio
from dolphin_master.database import Database


db = Database()
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

def get_best_portfolios():
    return db.get_best_portfolios()


def get_metrics():
    return {}


def get_info() -> dict:
    metrics = get_metrics()
    best_pf = get_best_portfolios()[0] if len(get_best_portfolios()) > 0 else [0, 0, 0, 0]
    info = {
        'best_sharpe': best_pf[3],
        'best_portfolio': db.get_portfolio(best_pf[0]).get_assets() if best_pf[0] != 0 else [[0,0]],
        'current_rate': metrics.get('current_rate', 0),
        'total_calculated': metrics.get('total', 0),
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
    pf = db.get_portfolio(pf_id)
    compute_time = db.update_portfolio(pf_id, sharpe, sharpe)
    # Log new dispatch to influxdb
    client.write_points([{
        "measurement": "sharpe",
        "fields": {
            "custom_sharpe": sharpe,
            "jump_sharpe": sharpe,
            "compute_time": compute_time
        }
    }])
    return 'OK'


@app.post('/new_pf')
async def receive_new_portfolio(request: Request):
    data = await request.json()
    pf = Portfolio()
    # TODO: check if exists
    for asset in data['assets']:
        pf.add_asset(asset[0], asset[1])
    new_pfs.add(pf)
    return 'OK'

if __name__ == '__main__':
    uvicorn.run(app, host="0.0.0.0", port=8000)
