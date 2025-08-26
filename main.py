
import os
import sqlite3
import asyncio
from typing import List
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Query, WebSocket
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi import Response
from starlette.websockets import WebSocketState, WebSocketDisconnect
from dotenv import load_dotenv

from api.dart_api import DartAPI
from api.kiwoom_api import KiwoomAPI
from core.database import fetch_all_companies, fetch_kospi, fetch_stock_day, fetch_stock_year
from tools.update import init_stock, update_day

load_dotenv()

# Global state for websocket clients and shutdown flag
clients: List[WebSocket] = []
shutdown_flag = False

# FastAPI lifespan for graceful shutdown
@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        yield
    finally:
        # On shutdown
        global shutdown_flag
        shutdown_flag = True
        for ws in clients[:]:
            try:
                if (
                    getattr(ws, 'application_state', WebSocketState.CONNECTED) == WebSocketState.CONNECTED
                    and ws.client_state == WebSocketState.CONNECTED
                ):
                    await ws.close()
            except Exception:
                pass
        clients.clear()


app = FastAPI(lifespan=lifespan)

# Instantiate API classes and attach to app
app.dart_api = DartAPI()
app.kiwoom_api = KiwoomAPI()

# Mount static files (for CSS/JS if needed)
if not os.path.exists('webui/static'):
    os.makedirs('webui/static')
app.mount('/static', StaticFiles(directory='webui/static'), name='static')

# Set up templates
if not os.path.exists('webui/templates'):
    os.makedirs('webui/templates')
templates = Jinja2Templates(directory='webui/templates')

@app.get('/', response_class=HTMLResponse)
def index(request: Request):
    nav_functions = [
        {'name': 'View Database Tables', 'endpoint': '/db'}
    ]
    action_functions = [
        {'name': 'Update Today', 'endpoint': '/update_today'}
    ]
    return templates.TemplateResponse('index.html', {
        'request': request,
        'nav_functions': nav_functions,
        'action_functions': action_functions
    })


# WebSocket endpoint for live log streaming
async def tail_log(websocket: WebSocket, log_path: str):
    await websocket.accept()
    clients.append(websocket)
    last_size = 0
    try:
        while not shutdown_flag:
            # If the client is no longer connected, stop the loop
            if websocket.client_state != WebSocketState.CONNECTED:
                break

            # Handle log file being truncated/rotated
            try:
                if os.path.exists(log_path):
                    current_size = os.path.getsize(log_path)
                    if current_size < last_size:
                        # File was truncated; restart from beginning
                        last_size = 0
                else:
                    # If the file doesn't exist yet, wait and retry
                    await asyncio.sleep(0.5)
                    continue
            except Exception:
                # Ignore filesystem race conditions
                await asyncio.sleep(0.5)
                continue

            try:
                with open(log_path, 'r', encoding='utf-8') as f:
                    f.seek(last_size)
                    new_data = f.read()
                    last_size = f.tell()
                if new_data:
                    await websocket.send_text(new_data)
            except WebSocketDisconnect:
                # Client initiated close
                break
            except (OSError, IOError) as e:
                # File read error; try to report once if still connected
                try:
                    if websocket.client_state == WebSocketState.CONNECTED:
                        await websocket.send_text(f'Error reading log: {e}\n')
                except Exception:
                    # If we can't send, just break the loop
                    break
            except RuntimeError:
                # Likely attempting to send on a closed websocket
                break

            await asyncio.sleep(0.5)
    except asyncio.CancelledError:
        # Graceful task cancellation
        pass
    finally:
        if websocket in clients:
            clients.remove(websocket)
        # Only attempt to close if still connected; guard against double-close
        try:
            if (
                getattr(websocket, 'application_state', WebSocketState.CONNECTED) == WebSocketState.CONNECTED
                and websocket.client_state == WebSocketState.CONNECTED
            ):
                await websocket.close()
        except Exception:
            # Ignore any errors during close
            pass

@app.websocket('/ws/logs')
async def websocket_logs(websocket: WebSocket):
    log_path = os.path.join('log', 'console.log')
    # Clear the log file on new websocket connection
    try:
        os.makedirs('log', exist_ok=True)
        with open(log_path, 'w', encoding='utf-8') as f:
            f.truncate(0)
    except Exception:
        pass
    await tail_log(websocket, log_path)


@app.get('/update_today')
def update_today():
    conn = sqlite3.connect('data/database.db')
    update_day(conn, 'pykrx')
    conn.close()
    return Response(status_code=200)

# Add endpoint to reset DB
@app.get('/reset')
def reset_db(source: str = Query(...)):
    conn = sqlite3.connect('data/database.db')
    init_stock(conn, source, app.dart_api, app.kiwoom_api)
    conn.close()

# Database table viewer
@app.get('/db', response_class=HTMLResponse)
def db(
    request: Request,
    table: str = Query(None),
    start: str = Query(None),
    end: str = Query(None),
    date: str = Query(None),
    stock_code: str = Query(None),
    year: int = Query(None),
    page: int = Query(1),
    page_size: int = Query(25)
):
    tables = ['companies', 'kospi', 'stock_daily', 'stock_year']
    rows = []
    columns = []
    total_rows = 0
    conn = sqlite3.connect('data/database.db')
    if table == 'companies':
        data = fetch_all_companies(conn)
        if data:
            columns = data[0].__dataclass_fields__.keys()
            rows = [c.__dict__ for c in data]
    elif table == 'kospi':
        s = start if start else '0'
        e = end if end else '99999999'
        data = fetch_kospi(conn, s, e)
        if data:
            columns = data[0].__dataclass_fields__.keys()
            rows = [c.__dict__ for c in data]
    elif table == 'stock_daily':
        s = start if start else '0'
        e = end if end else '99999999'
        data = fetch_stock_day(conn, s, e, stock_code, date)
        if data:
            columns = data[0].__dataclass_fields__.keys()
            rows = [c.__dict__ for c in data]
    elif table == 'stock_year':
        data = fetch_stock_year(conn, year, stock_code)
        if data:
            columns = data[0].__dataclass_fields__.keys()
            rows = [c.__dict__ for c in data]
    conn.close()
    # Pagination
    total_rows = len(rows)
    start_idx = (page - 1) * page_size
    end_idx = start_idx + page_size
    paged_rows = rows[start_idx:end_idx]
    total_pages = (total_rows + page_size - 1) // page_size if page_size else 1
    return templates.TemplateResponse('db.html', {
        'request': request,
        'tables': tables,
        'selected_table': table,
        'start': start or '',
        'end': end or '',
        'date': date or '',
        'stock_code': stock_code or '',
        'year': year or '',
        'columns': columns,
        'rows': paged_rows,
        'page': page,
        'page_size': page_size,
        'total_rows': total_rows,
        'total_pages': total_pages
    })

