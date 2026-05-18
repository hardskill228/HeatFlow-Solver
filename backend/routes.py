import hashlib
import time
import asyncio
import re
from pathlib import Path
import json

from fastapi import Request, HTTPException, WebSocket
from fastapi.responses import HTMLResponse

from .app import app, FRONTEND_DIR, task_manager
from .models import TaskInput, TaskState, TaskHistory
try:
    from .database import (
        create_user,
        get_user,
        update_user_profile,
        get_nickname_availability,
        reserve_nickname,
        activate_reserved_nickname,
        get_username_availability,
        reserve_username,
        activate_reserved_username,
        get_leaderboard,
        add_task_to_history,
        get_user_history,
        get_user_active_task_ids,
    )
    USE_DATABASE = True
except ImportError:
    USE_DATABASE = False

from .data import load_accounts, save_accounts
NICKNAME_RE = re.compile(r"^[A-Za-z0-9_]{3,20}$")
USERNAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{3,23}$")


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def get_index():
    try:
        p = Path(FRONTEND_DIR).joinpath('login.html')
        with open(p, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    except FileNotFoundError:
        return HTMLResponse("<h1>Error 404: Frontend file not found.</h1>", status_code=404)


@app.get("/app", response_class=HTMLResponse, include_in_schema=False)
async def get_app():
    try:
        p = Path(FRONTEND_DIR).joinpath('index.html')
        with open(p, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    except FileNotFoundError:
        return HTMLResponse("<h1>Error 404: App file not found.</h1>", status_code=404)


@app.get("/profile", response_class=HTMLResponse, include_in_schema=False)
async def get_profile_page():
    try:
        p = Path(FRONTEND_DIR).joinpath('profile.html')
        with open(p, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    except FileNotFoundError:
        return HTMLResponse("<h1>Error 404: Profile file not found.</h1>", status_code=404)


@app.get("/leaderboard", response_class=HTMLResponse, include_in_schema=False)
async def get_leaderboard_page():
    try:
        p = Path(FRONTEND_DIR).joinpath('leaderboard.html')
        with open(p, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    except FileNotFoundError:
        return HTMLResponse("<h1>Error 404: Leaderboard file not found.</h1>", status_code=404)


@app.get("/health")
async def health_check():
    return {"status": "ok"}


@app.get("/api/tasks/stats")
async def get_task_stats():
    
    total = len(task_manager.tasks)
    running = sum(1 for t in task_manager.tasks.values() if t.status == 'RUNNING')
    pending = sum(1 for t in task_manager.tasks.values() if t.status == 'PENDING')
    queued = sum(1 for t in task_manager.tasks.values() if t.status == 'QUEUED')
    completed = sum(1 for t in task_manager.tasks.values() if t.status == 'COMPLETED')
    
    estimated_wait = 0.0
    if running >= task_manager.MAX_CONCURRENT_TASKS:
        estimated_wait = task_manager._estimate_queue_wait_time()
    
    return {
        "total_tasks": total,
        "max_tasks": task_manager.MAX_TASKS,
        "available_slots": max(0, task_manager.MAX_TASKS - total),
        "running": running,
        "pending": pending,
        "queued": queued,
        "completed": completed,
        "max_concurrent": task_manager.MAX_CONCURRENT_TASKS,
        "queue_length": queued,
        "estimated_wait_seconds": round(estimated_wait, 1) if estimated_wait > 0 else 0
    }


@app.post("/api/auth/register")
async def register_account(creds: dict):
    username = creds.get('username')
    password = creds.get('password')
    name = creds.get('name')
    avatar_url = creds.get('avatar_url')
    email = creds.get('email')
    address = creds.get('address')
    city = creds.get('city')
    phone = creds.get('phone')
    if not username or not password:
        raise HTTPException(status_code=400, detail='username and password required')
    
    pwd_hash = hashlib.sha256(password.encode('utf-8')).hexdigest()
    
    if USE_DATABASE:
        success = create_user(username, pwd_hash, name, avatar_url, email, address, city, phone)
        if not success:
            raise HTTPException(status_code=400, detail='username already exists')
    else:
        accounts = load_accounts()
        if username in accounts:
            raise HTTPException(status_code=400, detail='username already exists')
        accounts[username] = {
            'password_hash': pwd_hash,
            'created_at': time.time(),
            'name': name,
            'avatar_url': avatar_url,
            'email': email,
            'address': address,
            'city': city,
            'phone': phone,
        }
        save_accounts(accounts)
    
    return {'username': username}


@app.post("/api/auth/login")
async def login_account(creds: dict):
    username = creds.get('username')
    password = creds.get('password')
    if not username or not password:
        raise HTTPException(status_code=400, detail='username and password required')
    
    pwd_hash = hashlib.sha256(password.encode('utf-8')).hexdigest()
    
    if USE_DATABASE:
        user = get_user(username)
        if not user or user['password_hash'] != pwd_hash:
            raise HTTPException(status_code=401, detail='invalid credentials')
    else:
        accounts = load_accounts()
        entry = accounts.get(username)
        if not entry or pwd_hash != entry.get('password_hash'):
            raise HTTPException(status_code=401, detail='invalid credentials')
    
    token = str(time.time()) + username
    token = hashlib.sha256(token.encode('utf-8')).hexdigest()
    app.state.token_map[token] = username
    
    # Зберігаємо токен в базі даних для спільного доступу між серверами
    if USE_DATABASE:
        from .database import save_token
        save_token(token, username)
    
    return {'token': token, 'username': username}


def _get_username_from_request(request: Request):
    auth = request.headers.get('Authorization','')
    token = None
    if auth.startswith('Bearer '):
        token = auth.split(' ',1)[1]
    
    # Спочатку перевіряємо локальний кеш
    username = app.state.token_map.get(token)
    if username:
        return username
    
    # Якщо не знайдено локально, перевіряємо в базі даних
    if USE_DATABASE and token:
        from .database import get_username_by_token
        username = get_username_by_token(token)
        if username:
            # Кешуємо локально для швидкості
            app.state.token_map[token] = username
            return username
    
    return None


def _get_token_from_request(request: Request):
    auth = request.headers.get('Authorization', '')
    if auth.startswith('Bearer '):
        return auth.split(' ', 1)[1]
    return None


def _verify_current_password(username: str, current_password: str) -> bool:
    if not current_password:
        return False
    pwd_hash = hashlib.sha256(current_password.encode('utf-8')).hexdigest()
    if USE_DATABASE:
        user = get_user(username)
        return bool(user and user.get('password_hash') == pwd_hash)
    accounts = load_accounts()
    entry = accounts.get(username)
    return bool(entry and entry.get('password_hash') == pwd_hash)


@app.get('/api/account/profile')
async def get_profile(request: Request):
    username = _get_username_from_request(request)
    if not username:
        raise HTTPException(status_code=401, detail='unauthorized')
    
    if USE_DATABASE:
        user = get_user(username)
        if not user:
            raise HTTPException(status_code=404, detail='user not found')
        
        history = get_user_history(username, limit=100)
        record = None
        if history:
            record = None
        
        return {
            'username': user['username'],
            'name': user.get('name'),
            'avatar_url': user.get('avatar_url'),
            'nickname': user.get('nickname'),
            'pending_nickname': user.get('pending_nickname'),
            'nickname_reserved_until': str(user['nickname_reserved_until']) if user.get('nickname_reserved_until') else None,
            'pending_username': user.get('pending_username'),
            'username_reserved_until': str(user['username_reserved_until']) if user.get('username_reserved_until') else None,
            'email': user.get('email'),
            'address': user.get('address'),
            'city': user.get('city'),
            'phone': user.get('phone'),
            'record_max_temperature_c': record, 
            'created_at': str(user['created_at'])
        }
    else:
        accounts = load_accounts()
        user = accounts.get(username, {})
        tasks = user.get('tasks', [])
        record = None
        if tasks:
            record = max((t.get('result', {}).get('max_temperature_c', 0) for t in tasks))
        return {
            'username': username,
            'name': user.get('name'),
            'avatar_url': user.get('avatar_url'),
            'nickname': user.get('nickname'),
            'pending_nickname': user.get('pending_nickname'),
            'nickname_reserved_until': user.get('nickname_reserved_until'),
            'pending_username': user.get('pending_username'),
            'username_reserved_until': user.get('username_reserved_until'),
            'email': user.get('email'),
            'address': user.get('address'),
            'city': user.get('city'),
            'phone': user.get('phone'),
            'record_max_temperature_c': record,
            'created_at': user.get('created_at')
        }


@app.get('/api/account/nickname/check')
async def check_nickname(request: Request):
    username = _get_username_from_request(request)
    if not username:
        raise HTTPException(status_code=401, detail='unauthorized')

    candidate = (request.query_params.get('value') or '').strip()
    if USE_DATABASE:
        return get_nickname_availability(candidate, exclude_username=username)

    if not candidate:
        return {'available': False, 'reason': 'Nickname is required.'}
    if not NICKNAME_RE.fullmatch(candidate):
        return {'available': False, 'reason': 'Use 3-20 letters, numbers or underscore.'}
    accounts = load_accounts()
    for account_username, account in accounts.items():
        if account_username == username:
            continue
        if (account.get('nickname') or '').lower() == candidate.lower():
            return {'available': False, 'reason': 'Nickname is already reserved or active.'}
    return {'available': True, 'reason': 'Nickname is available.'}


@app.post('/api/account/nickname/reserve')
async def reserve_account_nickname(request: Request, payload: dict):
    username = _get_username_from_request(request)
    if not username:
        raise HTTPException(status_code=401, detail='unauthorized')

    candidate = (payload.get('nickname') or '').strip()
    if USE_DATABASE:
        result = reserve_nickname(username, candidate)
        if not result.get('available'):
            raise HTTPException(status_code=400, detail=result.get('reason', 'Could not reserve nickname.'))
        user = get_user(username)
        return {
            'message': result.get('reason'),
            'nickname': user.get('nickname'),
            'pending_nickname': user.get('pending_nickname'),
            'nickname_reserved_until': str(user['nickname_reserved_until']) if user.get('nickname_reserved_until') else None,
        }

    if not candidate:
        raise HTTPException(status_code=400, detail='Nickname is required.')
    if not NICKNAME_RE.fullmatch(candidate):
        raise HTTPException(status_code=400, detail='Use 3-20 letters, numbers or underscore.')
    accounts = load_accounts()
    user = accounts.get(username)
    if not user:
        raise HTTPException(status_code=404, detail='user not found')
    user['pending_nickname'] = candidate
    user['nickname_reserved_until'] = time.time() + 15 * 60
    save_accounts(accounts)
    return {
        'message': 'Nickname reserved.',
        'nickname': user.get('nickname'),
        'pending_nickname': user.get('pending_nickname'),
        'nickname_reserved_until': user.get('nickname_reserved_until'),
    }


@app.post('/api/account/nickname/activate')
async def activate_account_nickname(request: Request):
    username = _get_username_from_request(request)
    if not username:
        raise HTTPException(status_code=401, detail='unauthorized')

    if USE_DATABASE:
        result = activate_reserved_nickname(username)
        if not result.get('success'):
            raise HTTPException(status_code=400, detail=result.get('reason', 'Could not activate nickname.'))
        user = result['user']
        return {
            'message': 'Nickname activated.',
            'nickname': user.get('nickname'),
            'pending_nickname': user.get('pending_nickname'),
            'nickname_reserved_until': str(user['nickname_reserved_until']) if user.get('nickname_reserved_until') else None,
        }

    accounts = load_accounts()
    user = accounts.get(username)
    if not user:
        raise HTTPException(status_code=404, detail='user not found')
    if not user.get('pending_nickname'):
        raise HTTPException(status_code=400, detail='No active nickname reservation found.')
    reserved_until = user.get('nickname_reserved_until')
    if not reserved_until or reserved_until <= time.time():
        raise HTTPException(status_code=400, detail='Nickname reservation expired.')
    user['nickname'] = user.get('pending_nickname')
    user['pending_nickname'] = None
    user['nickname_reserved_until'] = None
    save_accounts(accounts)
    return {
        'message': 'Nickname activated.',
        'nickname': user.get('nickname'),
        'pending_nickname': user.get('pending_nickname'),
        'nickname_reserved_until': user.get('nickname_reserved_until'),
    }


@app.get('/api/account/username/check')
async def check_username(request: Request):
    username = _get_username_from_request(request)
    if not username:
        raise HTTPException(status_code=401, detail='unauthorized')

    candidate = (request.query_params.get('value') or '').strip()
    if USE_DATABASE:
        return get_username_availability(candidate, exclude_username=username)

    if not candidate:
        return {'available': False, 'reason': 'Username is required.'}
    if not USERNAME_RE.fullmatch(candidate):
        return {'available': False, 'reason': 'Use 4-24 chars, start with a letter, only letters, numbers or underscore.'}
    accounts = load_accounts()
    for account_username, account in accounts.items():
        if account_username == username:
            continue
        if account_username.lower() == candidate.lower():
            return {'available': False, 'reason': 'Username is already reserved or active.'}
        if (account.get('pending_username') or '').lower() == candidate.lower():
            reserved_until = account.get('username_reserved_until') or 0
            if reserved_until > time.time():
                return {'available': False, 'reason': 'Username is already reserved or active.'}
    return {'available': True, 'reason': 'Username is available.'}


@app.post('/api/account/username/reserve')
async def reserve_account_username(request: Request, payload: dict):
    username = _get_username_from_request(request)
    if not username:
        raise HTTPException(status_code=401, detail='unauthorized')

    current_password = payload.get('current_password') or ''
    desired_username = (payload.get('username') or '').strip()
    if not _verify_current_password(username, current_password):
        raise HTTPException(status_code=401, detail='Current password is incorrect.')

    if USE_DATABASE:
        result = reserve_username(username, desired_username)
        if not result.get('available'):
            raise HTTPException(status_code=400, detail=result.get('reason', 'Could not reserve username.'))
        user = get_user(username)
        return {
            'message': result.get('reason'),
            'username': user.get('username'),
            'pending_username': user.get('pending_username'),
            'username_reserved_until': str(user['username_reserved_until']) if user.get('username_reserved_until') else None,
        }

    accounts = load_accounts()
    user = accounts.get(username)
    if not user:
        raise HTTPException(status_code=404, detail='user not found')
    if not USERNAME_RE.fullmatch(desired_username):
        raise HTTPException(status_code=400, detail='Use 4-24 chars, start with a letter, only letters, numbers or underscore.')
    user['pending_username'] = desired_username
    user['username_reserved_until'] = time.time() + 15 * 60
    save_accounts(accounts)
    return {
        'message': 'Username reserved.',
        'username': username,
        'pending_username': user.get('pending_username'),
        'username_reserved_until': user.get('username_reserved_until'),
    }


@app.post('/api/account/username/activate')
async def activate_account_username(request: Request, payload: dict):
    username = _get_username_from_request(request)
    token = _get_token_from_request(request)
    if not username:
        raise HTTPException(status_code=401, detail='unauthorized')

    current_password = payload.get('current_password') or ''
    if not _verify_current_password(username, current_password):
        raise HTTPException(status_code=401, detail='Current password is incorrect.')

    if USE_DATABASE:
        result = activate_reserved_username(username)
        if not result.get('success'):
            raise HTTPException(status_code=400, detail=result.get('reason', 'Could not activate username.'))
        new_username = result['new_username']
        if token:
            app.state.token_map[token] = new_username
        user = result['user']
        return {
            'message': 'Username activated.',
            'old_username': result.get('old_username'),
            'username': user.get('username'),
            'pending_username': user.get('pending_username'),
            'username_reserved_until': str(user['username_reserved_until']) if user.get('username_reserved_until') else None,
        }

    accounts = load_accounts()
    user = accounts.get(username)
    if not user:
        raise HTTPException(status_code=404, detail='user not found')
    if not user.get('pending_username'):
        raise HTTPException(status_code=400, detail='No active username reservation found.')
    reserved_until = user.get('username_reserved_until')
    if not reserved_until or reserved_until <= time.time():
        raise HTTPException(status_code=400, detail='Username reservation expired.')
    new_username = user['pending_username']
    accounts[new_username] = dict(user)
    accounts[new_username]['pending_username'] = None
    accounts[new_username]['username_reserved_until'] = None
    del accounts[username]
    save_accounts(accounts)
    if token:
        app.state.token_map[token] = new_username
    return {
        'message': 'Username activated.',
        'old_username': username,
        'username': new_username,
        'pending_username': None,
        'username_reserved_until': None,
    }


@app.put('/api/account/profile')
async def update_profile(request: Request, payload: dict):
    username = _get_username_from_request(request)
    if not username:
        raise HTTPException(status_code=401, detail='unauthorized')

    profile_data = {
        'name': (payload.get('name') or '').strip() or None,
        'avatar_url': (payload.get('avatar_url') or '').strip() or None,
        'email': (payload.get('email') or '').strip() or None,
        'address': (payload.get('address') or '').strip() or None,
        'city': (payload.get('city') or '').strip() or None,
        'phone': (payload.get('phone') or '').strip() or None,
    }

    if USE_DATABASE:
        success = update_user_profile(username, **profile_data)
        if not success:
            raise HTTPException(status_code=404, detail='user not found')
        user = get_user(username)
        return {
            'username': user['username'],
            'name': user.get('name'),
            'avatar_url': user.get('avatar_url'),
            'email': user.get('email'),
            'address': user.get('address'),
            'city': user.get('city'),
            'phone': user.get('phone'),
            'created_at': str(user['created_at'])
        }

    accounts = load_accounts()
    user = accounts.get(username)
    if not user:
        raise HTTPException(status_code=404, detail='user not found')
    user.update(profile_data)
    save_accounts(accounts)
    return {
        'username': username,
        'name': user.get('name'),
        'avatar_url': user.get('avatar_url'),
        'email': user.get('email'),
        'address': user.get('address'),
        'city': user.get('city'),
        'phone': user.get('phone'),
        'created_at': user.get('created_at')
    }


@app.get('/api/leaderboard')
async def leaderboard():
    if USE_DATABASE:
        return get_leaderboard(limit=10)

    accounts = load_accounts()
    rows = []
    for username, account in accounts.items():
        tasks = account.get('tasks', [])
        if not tasks:
            continue
        completed = [t for t in tasks if t.get('result')]
        if not completed:
            continue
        best = max((t.get('result', {}).get('max_temperature_c', 0) for t in completed), default=0)
        durations = [t.get('result', {}).get('execution_time_seconds', 0) for t in completed]
        rows.append({
            'username': username,
            'display_name': username,
            'nickname': account.get('nickname'),
            'sessions_count': len(completed),
            'best_temperature': best,
            'avg_duration': sum(durations) / len(durations) if durations else 0,
        })
    rows.sort(key=lambda row: (-row['best_temperature'], -row['sessions_count'], row['username']))
    return rows[:10]


@app.get('/api/account/history')
async def account_history(request: Request):
    username = _get_username_from_request(request)
    if not username:
        raise HTTPException(status_code=401, detail='unauthorized')
    
    if USE_DATABASE:
        history = get_user_history(username, limit=50)
        
        formatted_history = []
        for task in history:
            result_data = task.get('result_data', {})
            
            formatted_history.append({
                'task_id': task['task_id'],
                'status': 'COMPLETED',
                'start_time': task['timestamp'].timestamp() if hasattr(task['timestamp'], 'timestamp') else 0,
                'end_time': task['timestamp'].timestamp() + task['computation_time'] if hasattr(task['timestamp'], 'timestamp') else task['computation_time'],
                'result': result_data if result_data else {
                    'execution_time_seconds': task['computation_time'],
                    'max_temperature_c': task.get('final_avg_temp', 0),
                    'nodes': task['nodes'],
                    'iterations': task['iterations'],
                    'time_series': []
                }
            })
        return formatted_history
    else:
        accounts = load_accounts()
        user = accounts.get(username, {})
        return user.get('tasks', [])


@app.get('/api/account/active-tasks', response_model=TaskHistory)
async def account_active_tasks(request: Request):
    username = _get_username_from_request(request)
    if not username:
        raise HTTPException(status_code=401, detail='unauthorized')

    task_ids = []
    seen = set()

    for task_id, task in task_manager.tasks.items():
        if task.owner == username and task.status not in ['COMPLETED', 'FAILED', 'CANCELLED']:
            seen.add(task_id)
            task_ids.append(task_id)

    if USE_DATABASE:
        for task_id in get_user_active_task_ids(username, limit=20):
            if task_id not in seen:
                seen.add(task_id)
                task_ids.append(task_id)

    active_tasks = []
    for task_id in task_ids:
        task = task_manager.get_status(task_id)
        if not task or task.status in ['COMPLETED', 'FAILED', 'CANCELLED']:
            continue
        if not task.owner:
            task.owner = username
        active_tasks.append(task)

    active_tasks.sort(key=lambda task: task.start_time or 0, reverse=True)
    return TaskHistory.model_validate(active_tasks)


@app.post("/api/tasks/start", response_model=TaskState)
async def start_task(task_input: TaskInput, request: Request):
    if task_input.nodes > task_manager.MAX_NODES:
        raise HTTPException(status_code=400, detail=f"Кількість вузлів (nodes) перевищує максимальне значення {task_manager.MAX_NODES}.")
    if task_input.iterations <= 0:
        raise HTTPException(status_code=400, detail="Кількість ітерацій має бути більшою за 0.")
    username = _get_username_from_request(request)
    simulation_parameters = {
        "rod_length_m": task_input.rod_length_m,
        "total_time_s": task_input.total_time_s,
        "thermal_diffusivity": task_input.thermal_diffusivity,
        "initial_temperature_c": task_input.initial_temperature_c,
        "left_boundary_c": task_input.left_boundary_c,
        "right_boundary_c": task_input.right_boundary_c,
    }
    
    try:
        new_task = task_manager.start_new_task(
            task_input.nodes,
            task_input.iterations,
            username=username,
            simulation_parameters=simulation_parameters
        )
    except ValueError as e:
        raise HTTPException(status_code=429, detail=str(e))
    
    if username:
        new_task.owner = username
    return new_task


@app.post("/api/tasks/{task_id}/cancel")
async def cancel_task(task_id: str):
    if task_manager.request_cancel(task_id):
        return {"task_id": task_id, "status": "CANCELLATION_REQUESTED", "message": f"Надіслано запит на скасування задачі {task_id}."}
    status = task_manager.get_status(task_id)
    if status and status.status not in ['RUNNING', 'CANCELLATION_REQUESTED']:
        raise HTTPException(status_code=400, detail=f"Неможливо скасувати задачу {task_id}. Поточний статус: {status.status}.")
    raise HTTPException(status_code=404, detail=f"Задачу {task_id} не знайдено.")


@app.post("/api/tasks/{task_id}/pause")
async def pause_task(task_id: str):
    if task_manager.request_pause(task_id):
        return {"task_id": task_id, "status": "PAUSE_REQUESTED", "message": f"Pause requested for task {task_id}."}
    status = task_manager.get_status(task_id)
    if status and status.status != 'RUNNING':
        raise HTTPException(status_code=400, detail=f"Неможливо призупинити задачу {task_id}. Поточний статус: {status.status}.")
    raise HTTPException(status_code=404, detail=f"Задачу {task_id} не знайдено.")


@app.post("/api/tasks/{task_id}/resume")
async def resume_task(task_id: str):
    if task_manager.request_resume(task_id):
        return {"task_id": task_id, "status": "RESUMED", "message": f"Задачу {task_id} відновлено."}
    status = task_manager.get_status(task_id)
    if status and status.status != 'PAUSED':
        raise HTTPException(status_code=400, detail=f"Неможливо відновити задачу {task_id}. Поточний статус: {status.status}.")
    raise HTTPException(status_code=404, detail=f"Задачу {task_id} не знайдено.")


@app.get("/api/tasks/history", response_model=TaskHistory)
async def get_history():
    sorted_tasks = sorted(task_manager.tasks.values(), key=lambda task: task.start_time, reverse=True)
    return TaskHistory.model_validate(sorted_tasks)


@app.get("/api/tasks/{task_id}", response_model=TaskState)
async def get_task(task_id: str):
    task = task_manager.get_status(task_id)
    if not task:
        raise HTTPException(status_code=404, detail=f"Задачу {task_id} не знайдено.")
    return task


@app.websocket("/ws/{task_id}")
async def websocket_endpoint(websocket: WebSocket, task_id: str):
    await websocket.accept()
    
    if task_manager._event_loop is None:
        task_manager._event_loop = asyncio.get_running_loop()
    
    task = task_manager.get_status(task_id)
    if not task:
        await websocket.close(code=1008, reason="Task ID not found")
        return

    if task_id not in task_manager.active_connections:
        task_manager.active_connections[task_id] = set()
    task_manager.active_connections[task_id].add(websocket)
    
    initial_status = task_manager.get_status(task_id)
    if initial_status:
        task_manager.send_status(task_id, initial_status.status, initial_status.stage, initial_status.progress, initial_status.result)
    
    try:
        # Періодично оновлюємо статус з БД (для cross-server tasks)
        while True:
            try:
                # Чекаємо повідомлення або timeout (0.5 секунди для частішого оновлення)
                await asyncio.wait_for(websocket.receive_text(), timeout=0.5)
            except asyncio.TimeoutError:
                # Timeout - оновлюємо статус з БД
                current_status = task_manager.get_status(task_id)
                if current_status:
                    await websocket.send_text(json.dumps({
                        'status': current_status.status,
                        'stage': current_status.stage,
                        'progress': current_status.progress or 0.0,
                        'result': current_status.result,
                        'worker_id': current_status.worker_id
                    }))
                    # Якщо задача завершена - виходимо
                    if current_status.status in ['COMPLETED', 'FAILED', 'CANCELLED']:
                        break
    except Exception:
        pass
    finally:
        task_manager.disconnect_ws(websocket, task_id)
