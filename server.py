import time
import uuid
import asyncio
import numpy as np
import math
from typing import Dict, List, Optional, Any
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, RootModel
from starlette.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pathlib import Path
import json
from concurrent.futures import ThreadPoolExecutor
import random
import hashlib
import os
from pathlib import Path

# -----------------------------------------------------------------------------
# Pydantic Моделі
# -----------------------------------------------------------------------------

class TaskState(BaseModel):
    """Модель стану асинхронного завдання."""
    task_id: str
    status: str = "PENDING"
    stage: str = "Initializing"
    progress: float = 0.0
    start_time: float = time.time()
    end_time: Optional[float] = None
    result: Optional[Dict[str, Any]] = None
    owner: Optional[str] = None

class TaskInput(BaseModel):
    """Модель вхідних даних для завдання."""
    nodes: int
    iterations: int

class TaskHistory(RootModel):
    """Модель для відображення історії (список TaskState)."""
    root: List[TaskState]

# -----------------------------------------------------------------------------
# Менеджер Завдань (Task Manager) та Воркер
# -----------------------------------------------------------------------------

class TaskManager:
    """Керує життєвим циклом, станом та WebSockets для асинхронних завдань."""
    
    tasks: Dict[str, TaskState] = {}
    active_connections: Dict[str, set] = {}
    cancel_flags: Dict[str, bool] = {}
    pause_flags: Dict[str, bool] = {}
    
    executor = ThreadPoolExecutor(max_workers=4)
    MAX_NODES = 100000

    def get_status(self, task_id: str) -> Optional[TaskState]:
        return self.tasks.get(task_id)

    def set_status(self, task_id: str, status: str, stage: str, progress: Optional[float] = None, result: Optional[Dict[str, Any]] = None):
        """Оновлює стан завдання та сповіщає клієнтів через WS."""
        task = self.tasks.get(task_id)
        if not task: return

        task.status = status
        task.stage = stage
        
        # Only update progress if provided and not decreasing (keeps monotonic progress)
        if progress is not None:
            try:
                new_progress = float(progress)
            except Exception:
                new_progress = task.progress
            # clamp and ensure non-decreasing
            new_progress = max(task.progress, min(100.0, new_progress))
            task.progress = new_progress

        if result is not None: task.result = result
        
        if status in ['COMPLETED', 'FAILED', 'CANCELLED']:
            task.end_time = time.time()
            self.cancel_flags.pop(task_id, None)

        self.send_status(task_id, task.status, task.stage, task.progress, task.result)

    def send_status(self, task_id: str, status: str, stage: str, progress: float, result: Optional[Dict[str, Any]] = None):
        """Надсилає оновлення статусу всім підключеним клієнтам WS.

        Підтримує виклики як з основного event loop (через create_task),
        так і з фонового потоку (через asyncio.run).
        """
        
        message = {"status": status, "stage": stage, "progress": float(progress), "result": result}
        data = json.dumps(message)
        
        if task_id in self.active_connections:
            connections_to_remove = set()
            for connection in list(self.active_connections[task_id]):
                try:
                    # Якщо є запущений цикл у поточному потоці — використовуємо create_task
                    try:
                        loop = asyncio.get_running_loop()
                        # schedule non-blocking send in that loop
                        loop.call_soon_threadsafe(asyncio.create_task, connection.send_text(data))
                    except RuntimeError:
                        # Немає запущеного циклу в цьому потоці (ймовірно фонова нитка) — виконуємо send у новому циклі
                        asyncio.run(connection.send_text(data))
                except (WebSocketDisconnect, RuntimeError):
                    connections_to_remove.add(connection)
                except Exception:
                    connections_to_remove.add(connection)
            
            self.active_connections[task_id].difference_update(connections_to_remove)
            
            if status in ['COMPLETED', 'FAILED', 'CANCELLED'] and task_id in self.active_connections:
                 for connection in list(self.active_connections[task_id]):
                     try:
                         try:
                             loop = asyncio.get_running_loop()
                             loop.call_soon_threadsafe(asyncio.create_task, connection.close())
                         except RuntimeError:
                             asyncio.run(connection.close())
                     except Exception:
                         pass
                 self.active_connections.pop(task_id, None)

    def start_new_task(self, nodes: int, iterations: int) -> TaskState:
        """Створює нове завдання та запускає обчислення у фоновому потоці."""
        task_id = str(uuid.uuid4())
        new_task = TaskState(task_id=task_id, start_time=time.time())
        # owner may be set by caller after creation
        self.tasks[task_id] = new_task
        self.cancel_flags[task_id] = False
        
        print(f"Task {task_id} added to the queue.")

        # Запускаємо трудомістке завдання у фоновому потоці
        loop = asyncio.get_event_loop()
        loop.run_in_executor(
            self.executor,
            # Використовуємо self, оскільки heavy_computation інтегрована
            lambda: self.heavy_computation(task_id, nodes, iterations) 
        )
        
        return new_task

    def check_cancel(self, task_id: str) -> bool:
        return self.cancel_flags.get(task_id, False)

    def request_cancel(self, task_id: str) -> bool:
        task = self.tasks.get(task_id)
        if not task:
            return False
        # allow cancelling if task is not already finished
        if task.status in ['COMPLETED', 'FAILED', 'CANCELLED']:
            return False
        # set cancellation flag so worker or queue honors it
        self.cancel_flags[task_id] = True
        task.status = 'CANCELLATION_REQUESTED'
        task.stage = 'Cancellation requested by user'
        self.send_status(task_id, task.status, task.stage, task.progress, task.result)
        return True

    def request_pause(self, task_id: str) -> bool:
        task = self.tasks.get(task_id)
        if task and task.status == 'RUNNING':
            # set pause flag; worker will observe and set status to PAUSED
            self.pause_flags[task_id] = True
            # notify clients immediately
            task.status = 'PAUSED'
            task.stage = 'Paused by user'
            self.send_status(task_id, task.status, task.stage, task.progress, task.result)
            return True
        return False

    def request_resume(self, task_id: str) -> bool:
        task = self.tasks.get(task_id)
        if task and task.status == 'PAUSED':
            # clear pause flag; worker will resume
            self.pause_flags.pop(task_id, None)
            task.status = 'RUNNING'
            task.stage = 'Resumed'
            self.send_status(task_id, task.status, task.stage, task.progress, task.result)
            return True
        return False
    
    async def connect_ws(self, websocket: WebSocket, task_id: str):
        await websocket.accept()
        if task_id not in self.active_connections: self.active_connections[task_id] = set()
        self.active_connections[task_id].add(websocket)
        
        initial_status = self.get_status(task_id)
        if initial_status:
            self.send_status(task_id, initial_status.status, initial_status.stage, initial_status.progress, initial_status.result)

    def disconnect_ws(self, websocket: WebSocket, task_id: str):
        if task_id in self.active_connections:
            self.active_connections[task_id].discard(websocket)

    # -------------------------------------------------------------------------
    # ТРУДОМІСТКА ФУНКЦІЯ (ІНТЕГРОВАНА)
    # -------------------------------------------------------------------------
    def heavy_computation(self, task_id: str, nodes: int, iterations: int):
        """
        Імітує обчислення МСЕ та оновлює статус через менеджер.
        """
        try:
            start_time = time.time()
            matrix_size = int(math.sqrt(nodes))
            time_series = []
            # starting temperature baseline
            temp_baseline = random.uniform(50.0, 120.0)
            
            # 1. Початкове оновлення статусу
            self.set_status(task_id, 'RUNNING', "0. Started", 0.0)

            # Етап 1-2: Ініціалізація та Збірка матриці (блокуючі операції)
            self.set_status(task_id, 'RUNNING', "2. Stiffness Matrix Assembly", 20.0)
            
            # Імітація NumPy-роботи
            A = np.random.rand(matrix_size, matrix_size) 
            B = np.random.rand(matrix_size)
            time.sleep(1) 
            
            # Етап 3: Ітеративний Розв'язувач (75% роботи)
            for i in range(1, iterations + 1):
                # Перевірка на скасування
                if self.check_cancel(task_id):
                    self.set_status(task_id, 'CANCELLED', 'Cancelled by user', self.tasks[task_id].progress)
                    return
                # Перевірка на паузу
                if self.pause_flags.get(task_id):
                    # signal PAUSED status and then wait until resume or cancel
                    self.set_status(task_id, 'PAUSED', 'Paused by user', self.tasks[task_id].progress)
                    # busy-wait with small sleep to allow resume/cancel
                    while self.pause_flags.get(task_id):
                        # allow cancellation during pause
                        if self.check_cancel(task_id):
                            self.set_status(task_id, 'CANCELLED', 'Cancelled by user', self.tasks[task_id].progress)
                            return
                        time.sleep(0.2)
                    # resumed, mark running
                    self.set_status(task_id, 'RUNNING', current_stage, self.tasks[task_id].progress)

                # Симуляція розв'язку СЛАР
                if matrix_size > 0 and matrix_size < 1000: # Обмежуємо складність для демонстрації
                     _ = np.linalg.solve(A, B) 

                # simulate temperature climb and cpu load sample
                temp_now = temp_baseline + (i/iterations) * random.uniform(100.0, 220.0)
                cpu_now = min(100.0, random.uniform(20.0, 80.0) + (i/iterations)*20.0)
                time_series.append({
                    'step': i,
                    'progress': round(25.0 + (75.0 * i / max(1, iterations)), 2),
                    'temperature_c': round(temp_now, 2),
                    'cpu_percent': round(cpu_now, 2),
                    'timestamp': round(time.time() - start_time, 2)
                })

                # Оновлення прогресу (використовуємо float та заокруглення)
                current_progress = 25.0 + (75.0 * i / max(1, iterations))
                current_progress = min(100.0, current_progress)
                current_stage = f"3. Iterative Solver ({i}/{iterations})"
                self.set_status(task_id, 'RUNNING', current_stage, round(current_progress, 2))
                
                # Затримка для демонстрації
                time.sleep(0.001 * (nodes / 50000))

            # 4. Фіналізація та Збереження Результату
            end_time = time.time()
            
            # derive final stats from the time_series
            temps = [s['temperature_c'] for s in time_series] if time_series else [temp_baseline]
            final_result = {
                "max_temperature_c": round(max(temps), 2),
                "average_temperature_c": round(sum(temps)/len(temps), 2),
                "execution_time_seconds": round(end_time - start_time, 2),
                "grid_dimensions": f"{matrix_size}x{matrix_size}",
                "time_series": time_series
            }
            
            self.set_status(task_id, 'COMPLETED', '4. Done. Result Ready.', 100.0, final_result)

            # persist result to owner's account if present
            task = self.tasks.get(task_id)
            if task and task.owner:
                try:
                    accounts = load_accounts()
                    user = accounts.get(task.owner, {})
                    user_tasks = user.get('tasks', [])
                    entry = {
                        'task_id': task_id,
                        'start_time': task.start_time,
                        'end_time': time.time(),
                        'status': 'COMPLETED',
                        'result': final_result
                    }
                    user_tasks.append(entry)
                    user['tasks'] = user_tasks
                    accounts[task.owner] = user
                    save_accounts(accounts)
                except Exception:
                    pass

        except Exception as e:
            self.set_status(task_id, 'FAILED', f'Fatal Error: {str(e)}', 100.0)

# -----------------------------------------------------------------------------
# Ініціалізація FastAPI та Маршрути
# -----------------------------------------------------------------------------

task_manager = TaskManager()

app = FastAPI(title="HeatFlow Solver Async API")

# Serve frontend static files (styles, images, scripts) under /static
# Resolve frontend directory relative to this file so uvicorn can run from backend/
FRONTEND_DIR = str(Path(__file__).resolve().parent.parent.joinpath('frontend'))
app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Simple accounts persistence (file-based). Keeps username -> {password_hash, created_at}
DATA_DIR = str(Path(__file__).resolve().parent.joinpath('data'))
os.makedirs(DATA_DIR, exist_ok=True)
ACCOUNTS_FILE = Path(DATA_DIR).joinpath('accounts.json')

def load_accounts():
    if not ACCOUNTS_FILE.exists():
        return {}
    try:
        return json.loads(ACCOUNTS_FILE.read_text(encoding='utf-8'))
    except Exception:
        return {}

def save_accounts(d):
    ACCOUNTS_FILE.write_text(json.dumps(d, indent=2), encoding='utf-8')

# in-memory token map token -> username
app.state.token_map = {}

@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def get_index():
    """Serve the login page (frontend/login.html)."""
    try:
        p = Path(FRONTEND_DIR).joinpath('login.html')
        with open(p, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    except FileNotFoundError:
        return HTMLResponse("<h1>Error 404: Frontend file not found.</h1><p>Ensure frontend/login.html and backend/server.py are in the correct relative paths.</p>", status_code=404)

@app.get("/app", response_class=HTMLResponse, include_in_schema=False)
async def get_app():
    """Serve the main app page (frontend/index.html)."""
    try:
        p = Path(FRONTEND_DIR).joinpath('index.html')
        with open(p, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    except FileNotFoundError:
        return HTMLResponse("<h1>Error 404: App file not found.</h1><p>Create frontend/index.html.</p>", status_code=404)


@app.get("/profile", response_class=HTMLResponse, include_in_schema=False)
async def get_profile_page():
    """Serve the standalone profile page (frontend/profile.html)."""
    try:
        p = Path(FRONTEND_DIR).joinpath('profile.html')
        with open(p, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    except FileNotFoundError:
        return HTMLResponse("<h1>Error 404: Profile file not found.</h1>", status_code=404)


@app.get("/health")
async def health_check():
    """Simple health endpoint used by load-balancers or containers."""
    return {"status": "ok"}


from fastapi import Request


@app.post("/api/auth/register")
async def register_account(creds: Dict[str, str]):
    username = creds.get('username')
    password = creds.get('password')
    if not username or not password:
        raise HTTPException(status_code=400, detail='username and password required')
    accounts = load_accounts()
    if username in accounts:
        raise HTTPException(status_code=400, detail='username already exists')
    pwd_hash = hashlib.sha256(password.encode('utf-8')).hexdigest()
    accounts[username] = {'password_hash': pwd_hash, 'created_at': time.time()}
    save_accounts(accounts)
    return {'username': username}


@app.post("/api/auth/login")
async def login_account(creds: Dict[str, str]):
    username = creds.get('username')
    password = creds.get('password')
    if not username or not password:
        raise HTTPException(status_code=400, detail='username and password required')
    accounts = load_accounts()
    entry = accounts.get(username)
    if not entry:
        raise HTTPException(status_code=401, detail='invalid credentials')
    pwd_hash = hashlib.sha256(password.encode('utf-8')).hexdigest()
    if pwd_hash != entry.get('password_hash'):
        raise HTTPException(status_code=401, detail='invalid credentials')
    token = str(uuid.uuid4())
    app.state.token_map[token] = username
    return {'token': token, 'username': username}


@app.get('/api/account/profile')
async def get_profile(request: Request):
    auth = request.headers.get('Authorization','')
    token = None
    if auth.startswith('Bearer '):
        token = auth.split(' ',1)[1]
    username = app.state.token_map.get(token)
    if not username:
        raise HTTPException(status_code=401, detail='unauthorized')
    accounts = load_accounts()
    user = accounts.get(username, {})
    # compute record from persisted tasks
    tasks = user.get('tasks', [])
    record = None
    if tasks:
        record = max((t.get('result', {}).get('max_temperature_c', 0) for t in tasks))
    return {'username': username, 'record_max_temperature_c': record, 'created_at': user.get('created_at')}


@app.get('/api/account/history')
async def account_history(request: Request):
    auth = request.headers.get('Authorization','')
    token = None
    if auth.startswith('Bearer '):
        token = auth.split(' ',1)[1]
    username = app.state.token_map.get(token)
    if not username:
        raise HTTPException(status_code=401, detail='unauthorized')
    accounts = load_accounts()
    user = accounts.get(username, {})
    return user.get('tasks', [])


@app.post("/api/tasks/start", response_model=TaskState)
async def start_task(task_input: TaskInput, request: Request):
    """Запускає нове завдання з валідацією складності (Вимога #1)."""
    if task_input.nodes > task_manager.MAX_NODES:
        raise HTTPException(
            status_code=400, 
            detail=f"Кількість вузлів (nodes) перевищує максимальне значення {task_manager.MAX_NODES}."
        )
    if task_input.iterations <= 0:
        raise HTTPException(status_code=400, detail="Кількість ітерацій має бути більшою за 0.")
    # determine owner from token (if any)
    auth = request.headers.get('Authorization','')
    token = None
    if auth.startswith('Bearer '):
        token = auth.split(' ',1)[1]
    username = app.state.token_map.get(token)

    new_task = task_manager.start_new_task(task_input.nodes, task_input.iterations)
    if username:
        new_task.owner = username
    return new_task

@app.post("/api/tasks/{task_id}/cancel")
async def cancel_task(task_id: str):
    """POST-маршрут для скасування активного завдання."""
    if task_manager.request_cancel(task_id):
        return {"task_id": task_id, "status": "CANCELLATION_REQUESTED", "message": f"Cancellation requested for task {task_id}."}
    
    status = task_manager.get_status(task_id)
    if status and status.status not in ['RUNNING', 'CANCELLATION_REQUESTED']:
        raise HTTPException(status_code=400, detail=f"Cannot cancel task {task_id}. Current status is {status.status}.")

    raise HTTPException(status_code=404, detail=f"Task {task_id} not found.")


@app.post("/api/tasks/{task_id}/pause")
async def pause_task(task_id: str):
    """Request to pause a running task."""
    if task_manager.request_pause(task_id):
        return {"task_id": task_id, "status": "PAUSE_REQUESTED", "message": f"Pause requested for task {task_id}."}
    status = task_manager.get_status(task_id)
    if status and status.status != 'RUNNING':
        raise HTTPException(status_code=400, detail=f"Cannot pause task {task_id}. Current status is {status.status}.")
    raise HTTPException(status_code=404, detail=f"Task {task_id} not found.")


@app.post("/api/tasks/{task_id}/resume")
async def resume_task(task_id: str):
    """Request to resume a paused task."""
    if task_manager.request_resume(task_id):
        return {"task_id": task_id, "status": "RESUMED", "message": f"Resume requested for task {task_id}."}
    status = task_manager.get_status(task_id)
    if status and status.status != 'PAUSED':
        raise HTTPException(status_code=400, detail=f"Cannot resume task {task_id}. Current status is {status.status}.")
    raise HTTPException(status_code=404, detail=f"Task {task_id} not found.")

@app.get("/api/tasks/history", response_model=TaskHistory)
async def get_history():
    """GET-маршрут для отримання історії всіх завдань (вимога #3)."""
    sorted_tasks = sorted(
        task_manager.tasks.values(), 
        key=lambda task: task.start_time, 
        reverse=True
    )
    return TaskHistory.model_validate(sorted_tasks) 

@app.websocket("/ws/{task_id}")
async def websocket_endpoint(websocket: WebSocket, task_id: str):
    """Обробляє WS-з'єднання для моніторингу прогресу (Вимога #2)."""
    task = task_manager.get_status(task_id)
    if not task:
        await websocket.close(code=1008, reason="Task ID not found")
        return

    await task_manager.connect_ws(websocket, task_id)
    
    try:
        while True:
            await websocket.receive_text()
            
    except Exception as e:
        pass 
        
    finally:
        task_manager.disconnect_ws(websocket, task_id)