
import os
import sqlite3
import asyncio
from datetime import datetime
from contextlib import asynccontextmanager
from typing import List

from dotenv import load_dotenv
from fastapi import FastAPI, Request, Query, WebSocket
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi import Response
from starlette.websockets import WebSocketState, WebSocketDisconnect
from pathlib import Path

from api.dart_api import DartAPI
from api.kiwoom_api import KiwoomAPI, parse_account_info
from core.database import fetch_all_companies, fetch_kospi, fetch_stock_day, fetch_stock_year, init_db
from tools import undervalued, portfolio
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
        app.kiwoom_api.revoke_access_token()
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
app.kiwoom_api = KiwoomAPI(api_url='https://mockapi.kiwoom.com')  # TEST
init_db()

# Mount static files (for CSS/JS if needed)
if not os.path.exists('webui/static'):
    os.makedirs('webui/static')
app.mount('/static', StaticFiles(directory='webui/static'), name='static')

# Mount results directory to serve generated images
if not os.path.exists('results'):
    os.makedirs('results', exist_ok=True)
app.mount('/results', StaticFiles(directory='results'), name='results')

# Set up templates
if not os.path.exists('webui/templates'):
    os.makedirs('webui/templates')
templates = Jinja2Templates(directory='webui/templates')

@app.get('/', response_class=HTMLResponse)
def index(request: Request):
    nav_functions = [
        {'name': 'View Database Tables', 'endpoint': '/db'},
        {'name': 'Portfolio Images', 'endpoint': '/portfolio_page'},
        {'name': 'View Undervalued', 'endpoint': '/undervalued'},
        {'name': 'Account Info', 'endpoint': '/account_info'}
    ]
    action_functions = [
        {'name': 'Update Today', 'endpoint': '/update_today'},
        {'name': 'Save Portfolio', 'endpoint': '/portfolio'},
    ]
    return templates.TemplateResponse('index.html', {
        'request': request,
        'nav_functions': nav_functions,
        'action_functions': action_functions
    })

@app.get('/undervalued', response_class=HTMLResponse)
def undervalued_view(
    request: Request,
    page: int = Query(1),
    page_size: int = Query(25)
):
    rows = []
    columns = []
    error_message = None
    try:
        conn = sqlite3.connect('data/database.db')
        companies = fetch_all_companies(conn)
        end_date = datetime.today()
        start_date = end_date.replace(year=end_date.year - 3)

        df = undervalued.find_undervalued_assets(conn, companies, start_date, end_date)
        conn.close()
        uv = df[df['undervalued'] == True]
        if not uv.empty:
            uv = uv.reset_index()  # bring stock_code into columns
            columns = list(uv.columns)
            rows = uv.to_dict(orient='records')
    except Exception as e:
        error_message = str(e)

    total_rows = len(rows)
    start_idx = (page - 1) * page_size
    end_idx = start_idx + page_size
    paged_rows = rows[start_idx:end_idx]
    total_pages = (total_rows + page_size - 1) // page_size if page_size else 1

    return templates.TemplateResponse('undervalued.html', {
        'request': request,
        'columns': columns,
        'rows': paged_rows,
        'page': page,
        'page_size': page_size,
        'total_rows': total_rows,
        'total_pages': total_pages,
        'error_message': error_message
    })

@app.get('/portfolio_page', response_class=HTMLResponse)
def portfolio_page(request: Request):
    base = Path('results')
    pngs = []
    try:
        if base.exists():
            for p in base.rglob('*.png'):
                # Build URL path using /results mount
                rel = p.relative_to(base).as_posix()
                pngs.append({
                    'name': rel,
                    'url': f'/results/{rel}'
                })
    except Exception:
        pass
    # Sort by name for consistency
    pngs.sort(key=lambda x: x['name'])
    return templates.TemplateResponse('portfolio.html', {
        'request': request,
        'images': pngs
    })


# WebSocket endpoint for live log streaming
async def tail_log(websocket: WebSocket, log_path: str):
    await websocket.accept()
    clients.append(websocket)
    last_size = 0
    # On connect, send entire existing log content (show all lines)
    try:
        if os.path.exists(log_path):
            with open(log_path, 'r', encoding='utf-8') as f:
                data = f.read()
                last_size = f.tell()
            if data:
                await websocket.send_text(data)
    except Exception:
        # If initial full send fails, continue with streaming
        pass
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
    # Clear the log file on new websocket connection (temporary session log)
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

@app.get('/portfolio')
def save_portfolio():
    conn = sqlite3.connect('data/database.db')
    companies = fetch_all_companies(conn)
    end_date = datetime.today()
    start_date = end_date.replace(year=end_date.year - 3)

    undervalued_assets = undervalued.find_undervalued_assets(conn, companies, start_date, end_date)
    undervalued_true = undervalued_assets[undervalued_assets['undervalued'] == True]
    undervalued_assets = undervalued_true.index.tolist()

    result = portfolio.optimize_portfolio(conn, undervalued_assets, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], start_date, end_date, rf=0.03)
    portfolio.graph_lambda(conn, result['lambda_results'], undervalued_assets)
    portfolio.graph_sharpe(conn, result['sharpe'], undervalued_assets)
    conn.close()
    return Response(status_code=200)

@app.get('/account_info', response_class=HTMLResponse)
def acc_info(request: Request):
    error_message = None
    parsed = None
    try:
        info = app.kiwoom_api.get_account_info()
        parsed = parse_account_info(info)
    except Exception as e:
        error_message = str(e)
    return templates.TemplateResponse('account_info.html', {
        'request': request,
        'parsed': parsed,
        'error_message': error_message
    })


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
