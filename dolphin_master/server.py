from threading import Thread
from typing import List
import uvicorn
from fastapi import FastAPI, WebSocket, Request
import time
import os

from influxdb import InfluxDBClient
from starlette.websockets import WebSocketDisconnect

from algo.portfolio import Portfolio
from dolphin_master.database import Database

db = Database()
new_pfs = set()
app = FastAPI()

client = InfluxDBClient('localhost',
                        8086,
                        os.environ.get("INFLUXDB_USER", "worker"),
                        os.environ.get("INFLUXDB_USER_PASSWORD", "worker"),
                        os.environ.get("INFLUXDB_DB", "worker"))


class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    def send(self, message: dict, websocket: WebSocket):
        websocket.send_json(message)

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            await connection.send_json(message)


manager = ConnectionManager()


def get_best_portfolios():
    return db.get_best_portfolios()


def get_metrics():
    return {}


def get_info() -> dict:
    metrics = get_metrics()
    best_pf = get_best_portfolios()[0]
    info = {
        'best_sharpe': best_pf[3],
        'best_portfolio': db.get_portfolio(best_pf[0]).get_assets(),
        'current_rate': metrics.get('current_rate', 0),
        'total_calculated': metrics.get('total', 0),
        'mean': metrics.get('mean', 0),
        'queue': len(new_pfs)
    }
    return info


@app.websocket('/info/{ts}')
async def return_info(websocket: WebSocket, ts: float):
    await manager.connect(websocket)
    try:
        while True:
            await manager.broadcast(get_info())
            time.sleep(5)
    except WebSocketDisconnect:
        manager.disconnect(websocket)


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
    db.update_portfolio(pf_id, sharpe, sharpe)
    # Log new dispatch to influxdb
    client.write_points([{
        "measurement": "sharpe",
        "fields": {
            "custom_sharpe": sharpe,
            "jump_sharpe": sharpe
        }
    }])
    return 'OK'


def portfolio_thread():
    global new_pfs
    # for pf in generate_portfolios():
    #     new_pfs.add(pf)
    while True:
        pf = Portfolio()
        pf.add_asset(1, 1)
        pf.add_asset(2, 2)
        pf.add_asset(3, 3)
        new_pfs.add(pf)
        time.sleep(5)


def main():
    thread = Thread(target=portfolio_thread)
    thread.start()
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == '__main__':
    main()