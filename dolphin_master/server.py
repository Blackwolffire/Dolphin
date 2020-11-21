from fastapi import FastAPI, WebSocket
import time


def get_info() -> str:
    return str(time.time())


app = FastAPI()


@app.websocket('/info')
async def return_info(websocket: WebSocket):
    await websocket.accept()
    while True:
        await websocket.send_text(get_info())
        time.sleep(5)


@app.get('/new_pf')
def dispatch_portfolio():
    return 'Test'


@app.post('/pf_sharpe/{pf_id}')
def store_new_sharpe(pf_id: int):
    return pf_id
