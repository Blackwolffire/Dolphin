from fastapi import FastAPI, WebSocket, Request
import time

new_pfs = set()
app = FastAPI()


def get_best_sharpe():
    return 0


def get_best_portfolio():
    return []


def get_metrics():
    return {}


def get_info() -> dict:
    metrics = get_metrics()
    info = {
        'best_sharpe': get_best_sharpe(),
        'best_portfolio': get_best_portfolio(),
        'current_rate': metrics.get('current_rate', 0),
        'total_calculated': metrics.get('total', 0),
        'mean': metrics.get('mean', 0),
        'queue': len(new_pfs)
    }
    return info


def store_portfolio() -> int:
    pass


def update_portfolio():
    pass


@app.websocket('/info')
async def return_info(websocket: WebSocket):
    await websocket.accept()
    while True:
        await websocket.send_json(get_info())
        time.sleep(5)


@app.get('/new_pf')
def dispatch_portfolio():
    new_pf = new_pfs.pop()
    id = store_portfolio(new_pf)  # Store current time
    return {'id': id, 'assets': new_pf.assets()}


@app.post('/pf_sharpe/{pf_id}')
def store_new_sharpe(pf_id: int, request: Request):
    data = request.json()
    sharpe = data['sharpe'] if 'sharpe' in data else 0
    update_portfolio(pf_id, sharpe)
    return 'OK'
