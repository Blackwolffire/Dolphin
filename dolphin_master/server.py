from typing import List

from fastapi import FastAPI, WebSocket, Request
import time

from starlette.websockets import WebSocketDisconnect

from dolphin_master.database import Database

db = Database()
new_pfs = set()
app = FastAPI()


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
    best_pfs = db.get_best_portfolios()
    return best_pfs[0]['sharpe']


def get_metrics():
    return {}


def get_info() -> dict:
    metrics = get_metrics()
    best_pfs = get_best_portfolios()
    info = {
        'best_sharpe': best_pfs[0]['sharpe'],
        'best_portfolios': best_pfs,
        'current_rate': metrics.get('current_rate', 0),
        'total_calculated': metrics.get('total', 0),
        'mean': metrics.get('mean', 0),
        'queue': len(new_pfs)
    }
    return info


@app.websocket('/info')
async def return_info(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await manager.broadcast(get_info())
            time.sleep(5)
    except WebSocketDisconnect:
        manager.disconnect(websocket)


@app.get('/new_pf')
def dispatch_portfolio():
    new_pf = new_pfs.pop()
    id = db.store_portfolio(new_pf)  # Store current time
    return {'id': id, 'assets': new_pf.assets()}


@app.post('/pf_sharpe/{pf_id}')
def store_new_sharpe(pf_id: int, request: Request):
    data = request.json()
    sharpe = data['sharpe'] if 'sharpe' in data else 0
    db.update_portfolio(pf_id, sharpe)
    return 'OK'
