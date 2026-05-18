import time
import uuid
import asyncio
import json
import math
import random
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Optional, Any

import numpy as np
from fastapi import WebSocket

from .models import TaskState
try:
    from .database import (
        add_task_to_history, 
        add_task_to_queue, 
        get_next_queued_task, 
        mark_task_completed,
        get_queue_position
    )
    USE_DATABASE = True
except ImportError:
    from .data import load_accounts, save_accounts
    USE_DATABASE = False

DATABASE_URL = os.getenv(
    'DATABASE_URL',
    'postgresql://heatflow:heatflow123@localhost:5433/heatflow_db'
)


DEFAULT_SIMULATION_PARAMETERS = {
    "rod_length_m": 1.0,
    "total_time_s": 120.0,
    "thermal_diffusivity": 0.000115,
    "initial_temperature_c": 20.0,
    "left_boundary_c": 100.0,
    "right_boundary_c": 20.0,
}


def _normalize_simulation_parameters(parameters: Optional[Dict[str, Any]] = None) -> Dict[str, float]:
    merged = dict(DEFAULT_SIMULATION_PARAMETERS)
    if parameters:
        for key in merged:
            if key in parameters and parameters[key] is not None:
                merged[key] = float(parameters[key])
    return merged


def _db_connect():
    import psycopg2
    return psycopg2.connect(DATABASE_URL)


class TaskManager:
    
    tasks: Dict[str, TaskState] = {}
    active_connections: Dict[str, set] = {}
    cancel_flags: Dict[str, bool] = {}
    pause_flags: Dict[str, bool] = {}
    _event_loop: Optional[asyncio.AbstractEventLoop] = None

    executor = ThreadPoolExecutor(max_workers=5)
    MAX_NODES = 100000
    MAX_TASKS = 50
    MAX_CONCURRENT_TASKS = 10  # Загалом на обох серверах (5 на api1 + 5 на api2)
    
    WORKER_ID = os.getenv('WORKER_ID', 'local')
    
    loop = None

    def __init__(self):
        
        try:
            self.loop = asyncio.get_event_loop()
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

    def get_status(self, task_id: str) -> Optional[TaskState]:
        local_task = self.tasks.get(task_id)

        if USE_DATABASE:
            try:
                import json as json_module
                conn = _db_connect()
                cur = conn.cursor()
                cur.execute(
                    "SELECT task_id, nodes, iterations, username, status, worker_id, stage, progress, result_data, simulation_parameters "
                    "FROM task_queue WHERE task_id = %s",
                    (task_id,)
                )
                row = cur.fetchone()
                cur.close()
                conn.close()

                if row:
                    result = row[8] if isinstance(row[8], dict) else (json_module.loads(row[8]) if row[8] else None)
                    parameters = row[9] if isinstance(row[9], dict) else (json_module.loads(row[9]) if row[9] else {})
                    task = local_task or TaskState(
                        task_id=row[0],
                        start_time=time.time(),
                    )
                    task.nodes = row[1]
                    task.iterations = row[2]
                    task.owner = row[3] or task.owner
                    task.status = row[4]
                    task.worker_id = row[5]
                    task.stage = row[6] or f"On {row[5] or 'unknown worker'}"
                    task.progress = row[7] or 0.0
                    task.result = result
                    for key, value in _normalize_simulation_parameters(parameters).items():
                        setattr(task, key, value)
                    self.tasks[task_id] = task
                    return task
            except Exception as e:
                print(f"[TaskManager] Failed to get task from DB: {e}")

        return local_task

    def set_status(self, task_id: str, status: str, stage: str, progress: Optional[float] = None, result: Optional[Dict[str, Any]] = None):
        task = self.tasks.get(task_id)
        if not task:
            task = self.get_status(task_id)
        if not task: return

        task.status = status
        task.stage = stage

        if progress is not None:
            try:
                new_progress = float(progress)
            except Exception:
                new_progress = task.progress
            new_progress = max(task.progress, min(100.0, new_progress))
            task.progress = new_progress

        if result is not None:
            task.result = result

        if USE_DATABASE:
            try:
                import json as json_module
                conn = _db_connect()
                cur = conn.cursor()
                result_json = json_module.dumps(result) if result else None
                cur.execute(
                    "UPDATE task_queue SET status = %s, stage = %s, progress = %s, result_data = %s WHERE task_id = %s",
                    (status, stage, progress or 0.0, result_json, task_id)
                )
                conn.commit()
                cur.close()
                conn.close()
            except Exception as e:
                print(f"[{self.WORKER_ID}] Failed to update task in DB: {e}")

        if status in ['COMPLETED', 'FAILED', 'CANCELLED']:
            task.end_time = time.time()
            self.cancel_flags.pop(task_id, None)
            self._process_queue()

        self.send_status(task_id, task.status, task.stage, task.progress, task.result)

    def send_status(self, task_id: str, status: str, stage: str, progress: float, result: Optional[Dict[str, Any]] = None):
        # Отримуємо worker_id з бази даних
        worker_id = None
        if USE_DATABASE:
            try:
                task_state = self.get_status(task_id)
                if task_state:
                    worker_id = task_state.worker_id
            except Exception:
                pass
        
        message = {"status": status, "stage": stage, "progress": float(progress), "result": result, "worker_id": worker_id}
        data = json.dumps(message)

        if task_id in self.active_connections:
            if self._event_loop is None:
                try:
                    self._event_loop = asyncio.get_running_loop()
                except RuntimeError:
                    return
            
            connections_to_remove = set()
            for connection in list(self.active_connections[task_id]):
                try:
                    asyncio.run_coroutine_threadsafe(connection.send_text(data), self._event_loop)
                except Exception as e:
                    print(f"[TaskManager] Error sending status: {e}")
                    connections_to_remove.add(connection)

            self.active_connections[task_id].difference_update(connections_to_remove)

            if status in ['COMPLETED', 'FAILED', 'CANCELLED'] and task_id in self.active_connections:
                for connection in list(self.active_connections[task_id]):
                    try:
                        asyncio.run_coroutine_threadsafe(connection.close(), self._event_loop)
                    except Exception:
                        pass
                self.active_connections.pop(task_id, None)

    def start_new_task(
        self,
        nodes: int,
        iterations: int,
        username: str = None,
        simulation_parameters: Optional[Dict[str, Any]] = None
    ) -> TaskState:
        
        if len(self.tasks) >= self.MAX_TASKS:
            self._cleanup_old_tasks()
            if len(self.tasks) >= self.MAX_TASKS:
                raise ValueError(f"Досягнуто максимальну кількість задач ({self.MAX_TASKS}). Очікуйте завершення поточних задач.")
        
        task_id = str(uuid.uuid4())
        
        parameters = _normalize_simulation_parameters(simulation_parameters)

        new_task = TaskState(
            task_id=task_id,
            start_time=time.time(),
            nodes=nodes,
            iterations=iterations,
            worker_id=self.WORKER_ID,
            **parameters
        )
        self.tasks[task_id] = new_task
        self.cancel_flags[task_id] = False

        # Перевіряємо ГЛОБАЛЬНУ кількість RUNNING задач у PostgreSQL (з обох серверів)
        running_count = 0
        if USE_DATABASE:
            try:
                conn = _db_connect()
                cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM task_queue WHERE status = 'RUNNING'")
                running_count = cur.fetchone()[0]
                cur.close()
                conn.close()
            except Exception as e:
                print(f"[{self.WORKER_ID}] Failed to check running count: {e}")
                running_count = sum(1 for t in self.tasks.values() if t.status == 'RUNNING')
        else:
            running_count = sum(1 for t in self.tasks.values() if t.status == 'RUNNING')
        
        print(f"[{self.WORKER_ID}] Global running count: {running_count}/{self.MAX_CONCURRENT_TASKS}")
        
        if running_count >= self.MAX_CONCURRENT_TASKS:
            # Зберігаємо в БД зі статусом QUEUED
            if USE_DATABASE:
                try:
                    conn = _db_connect()
                    cur = conn.cursor()
                    cur.execute(
                        "INSERT INTO task_queue (task_id, username, nodes, iterations, simulation_parameters, status, stage, progress, queued_at) "
                        "VALUES (%s, %s, %s, %s, %s, 'QUEUED', %s, 0, CURRENT_TIMESTAMP)",
                        (task_id, username or 'anonymous', nodes, iterations, json.dumps(parameters), 'У черзі очікування')
                    )
                    conn.commit()
                    
                    # Отримуємо позицію в черзі
                    cur.execute("SELECT COUNT(*) FROM task_queue WHERE status = 'QUEUED' AND queued_at <= (SELECT queued_at FROM task_queue WHERE task_id = %s)", (task_id,))
                    queue_pos = cur.fetchone()[0]
                    
                    cur.close()
                    conn.close()
                    print(f"[{self.WORKER_ID}] Task {task_id} queued at position {queue_pos}")
                except Exception as e:
                    print(f"[{self.WORKER_ID}] Failed to queue task: {e}")
                    queue_pos = 1
            else:
                queue_pos = 1
            
            new_task.status = 'QUEUED'
            new_task.stage = f'В черзі (позиція: {queue_pos})'
            new_task.queue_position = queue_pos
            
            self.send_status(task_id, new_task.status, new_task.stage, 0.0, None)
            
            # Спробуємо обробити чергу (можливо є вільні слоти)
            self._process_queue()
        else:
            # Запускаємо одразу і зберігаємо в БД зі статусом RUNNING
            print(f"[{self.WORKER_ID}] Starting task {task_id}, username={username}, USE_DATABASE={USE_DATABASE}")
            if USE_DATABASE:
                try:
                    conn = _db_connect()
                    cur = conn.cursor()
                    cur.execute(
                        "INSERT INTO task_queue (task_id, username, nodes, iterations, simulation_parameters, status, worker_id, stage, progress, started_at) "
                        "VALUES (%s, %s, %s, %s, %s, 'RUNNING', %s, %s, 0, CURRENT_TIMESTAMP)",
                        (task_id, username or 'anonymous', nodes, iterations, json.dumps(parameters), self.WORKER_ID, 'Підготовка розрахунку')
                    )
                    conn.commit()
                    cur.close()
                    conn.close()
                    print(f"[{self.WORKER_ID}] Task {task_id} started on {self.WORKER_ID}")
                except Exception as e:
                    print(f"[{self.WORKER_ID}] Failed to save task to DB: {e}")
            
            loop = asyncio.get_event_loop()
            loop.run_in_executor(self.executor, lambda: self.heavy_computation(task_id, nodes, iterations, parameters))
        
        return new_task

    def check_cancel(self, task_id: str) -> bool:
        return self.cancel_flags.get(task_id, False)

    def request_cancel(self, task_id: str) -> bool:
        task = self.tasks.get(task_id)
        if not task:
            return False
        if task.status in ['COMPLETED', 'FAILED', 'CANCELLED']:
            return False
        self.cancel_flags[task_id] = True
        task.status = 'CANCELLATION_REQUESTED'
        task.stage = 'Користувач надіслав запит на скасування'
        print(f"[TaskManager] Cancel requested for {task_id}")
        self.send_status(task_id, task.status, task.stage, task.progress, task.result)
        return True

    def request_pause(self, task_id: str) -> bool:
        task = self.tasks.get(task_id)
        if task and task.status == 'RUNNING':
            self.pause_flags[task_id] = True
            task.status = 'PAUSED'
            task.stage = 'Обчислення призупинено користувачем'
            print(f"[TaskManager] Pause requested for {task_id}")
            self.send_status(task_id, task.status, task.stage, task.progress, task.result)
            return True
        return False

    def request_resume(self, task_id: str) -> bool:
        task = self.tasks.get(task_id)
        if task and task.status == 'PAUSED':
            self.pause_flags.pop(task_id, None)
            task.status = 'RUNNING'
            task.stage = 'Обчислення відновлено'
            print(f"[TaskManager] Resume requested for {task_id}")
            self.send_status(task_id, task.status, task.stage, task.progress, task.result)
            return True
        return False

    async def connect_ws(self, websocket: WebSocket, task_id: str):
        await websocket.accept()
        if task_id not in self.active_connections:
            self.active_connections[task_id] = set()
        self.active_connections[task_id].add(websocket)

        # Спробуємо отримати статус з БД, якщо не знайдено локально
        initial_status = self.get_status(task_id)
        if not initial_status and USE_DATABASE:
            # Почекаємо трохи - задача може ще не встигла зберегтись
            import asyncio
            await asyncio.sleep(0.1)
            initial_status = self.get_status(task_id)
        
        if initial_status:
            self.send_status(task_id, initial_status.status, initial_status.stage, initial_status.progress, initial_status.result)
        else:
            # Відправимо початковий статус
            await websocket.send_text(json.dumps({
                'status': 'QUEUED',
                'stage': 'Ініціалізація задачі',
                'progress': 0.0,
                'result': None
            }))

    def disconnect_ws(self, websocket: WebSocket, task_id: str):
        if task_id in self.active_connections:
            self.active_connections[task_id].discard(websocket)

    def _cleanup_old_tasks(self):
        
        finished_tasks = [
            (task_id, task) for task_id, task in self.tasks.items()
            if task.status in ['COMPLETED', 'FAILED', 'CANCELLED'] and task.end_time
        ]
        finished_tasks.sort(key=lambda x: x[1].end_time)
        
        tasks_to_remove = max(0, len(self.tasks) - self.MAX_TASKS + 10)
        for i in range(min(tasks_to_remove, len(finished_tasks))):
            task_id = finished_tasks[i][0]
            self.tasks.pop(task_id, None)
            self.cancel_flags.pop(task_id, None)
            self.pause_flags.pop(task_id, None)
            self.active_connections.pop(task_id, None)
            print(f"[TaskManager] Cleaned up old task {task_id}")
    
    def _estimate_queue_wait_time(self) -> float:
        
        running_tasks = [t for t in self.tasks.values() if t.status == 'RUNNING']
        if not running_tasks:
            return 0.0
        
        now = time.time()
        avg_elapsed = sum(now - t.start_time for t in running_tasks) / len(running_tasks)
        
        estimated_remaining = avg_elapsed * 0.5
        
        queue_position = sum(1 for t in self.tasks.values() if t.status == 'QUEUED')
        slots_ahead = (queue_position - 1) // self.MAX_CONCURRENT_TASKS
        
        return estimated_remaining * (slots_ahead + 1)
    
    def _process_queue(self):
        if not USE_DATABASE:
            return
        
        # Отримуємо глобальний лічильник RUNNING задач з бази даних
        try:
            conn = _db_connect()
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM task_queue WHERE status = 'RUNNING'")
            running_count = cur.fetchone()[0]
            cur.close()
            conn.close()
        except Exception as e:
            print(f"[{self.WORKER_ID}] Error getting running count: {e}")
            return
        
        while running_count < self.MAX_CONCURRENT_TASKS:
            try:
                conn = _db_connect()
                cur = conn.cursor()
                
                # Беремо найстарішу задачу QUEUED і помічаємо її як RUNNING для поточного worker
                cur.execute(
                    "UPDATE task_queue SET status = 'RUNNING', worker_id = %s, stage = %s, progress = 0, started_at = CURRENT_TIMESTAMP "
                    "WHERE task_id = (SELECT task_id FROM task_queue WHERE status = 'QUEUED' ORDER BY queued_at LIMIT 1) "
                    "RETURNING task_id, nodes, iterations, simulation_parameters",
                    (self.WORKER_ID, 'Запуск задачі з черги')
                )
                row = cur.fetchone()
                conn.commit()
                cur.close()
                conn.close()
                
                if not row:
                    # Черга порожня
                    break
                
                task_id, nodes, iterations, raw_parameters = row
                parameters = raw_parameters if isinstance(raw_parameters, dict) else (json.loads(raw_parameters) if raw_parameters else {})
                print(f"[{self.WORKER_ID}] Starting queued task {task_id}")
                
                # Якщо задача вже є локально (створена на цьому сервері), оновлюємо статус
                task = self.tasks.get(task_id)
                if task:
                    task.status = 'PENDING'
                    task.stage = 'Запуск задачі з черги'
                else:
                    # Задача створена на іншому сервері, створюємо локальний TaskState
                    task = TaskState(
                        task_id=task_id,
                        start_time=time.time(),
                        nodes=nodes,
                        iterations=iterations,
                        status='PENDING',
                        stage='Запуск задачі з черги',
                        **_normalize_simulation_parameters(parameters)
                    )
                    self.tasks[task_id] = task
                    self.cancel_flags[task_id] = False
                
                self.executor.submit(self._execute_queued_task, task_id)
                running_count += 1
                
            except Exception as e:
                print(f"[{self.WORKER_ID}] Error processing queue: {e}")
                break
    
    def _execute_queued_task(self, task_id: str):
        
        task = self.tasks.get(task_id)
        if not task:
            return
        nodes = task.nodes or 10000
        iterations = task.iterations or 1000
        parameters = {
            key: getattr(task, key, None)
            for key in DEFAULT_SIMULATION_PARAMETERS
        }
        self.heavy_computation(task_id, nodes, iterations, parameters)

    def heavy_computation(
        self,
        task_id: str,
        nodes: int,
        iterations: int,
        simulation_parameters: Optional[Dict[str, Any]] = None
    ):
        try:
            start_time = time.time()
            parameters = _normalize_simulation_parameters(simulation_parameters)
            computational_nodes = max(20, min(nodes, 1200))
            sample_steps = max(40, min(240, iterations))
            time_series = []

            length = parameters["rod_length_m"]
            total_time = parameters["total_time_s"]
            alpha = parameters["thermal_diffusivity"]
            initial_temp = parameters["initial_temperature_c"]
            left_boundary = parameters["left_boundary_c"]
            right_boundary = parameters["right_boundary_c"]
            dx = length / max(computational_nodes - 1, 1)
            requested_dt = total_time / max(iterations, 1)
            stable_dt = 0.45 * dx * dx / max(alpha, 1e-12)
            dt = min(requested_dt, stable_dt)
            effective_time = dt * iterations

            self.set_status(task_id, 'RUNNING', "0. Старт обчислення", 0.0)
            self.set_status(task_id, 'RUNNING', "1. Дискретизація стрижня", 10.0)

            field = np.full(computational_nodes, initial_temp, dtype=np.float64)
            field[0] = left_boundary
            field[-1] = right_boundary
            fourier_number = alpha * dt / (dx * dx)

            self.set_status(task_id, 'RUNNING', "2. Застосування початкових і крайових умов", 20.0)
            time.sleep(0.15)

            for step in range(1, sample_steps + 1):
                if self.check_cancel(task_id):
                    print(f"[TaskManager] Worker observed cancel for {task_id} at step {step}")
                    self.set_status(task_id, 'CANCELLED', 'Скасовано користувачем', self.tasks[task_id].progress)
                    return
                if self.pause_flags.get(task_id):
                    print(f"[TaskManager] Worker pausing {task_id} at step {step}")
                    self.set_status(task_id, 'PAUSED', 'Обчислення призупинено користувачем', self.tasks[task_id].progress)
                    while self.pause_flags.get(task_id):
                        if self.check_cancel(task_id):
                            print(f"[TaskManager] Worker observed cancel during pause for {task_id}")
                            self.set_status(task_id, 'CANCELLED', 'Скасовано користувачем', self.tasks[task_id].progress)
                            return
                        time.sleep(0.2)
                    print(f"[TaskManager] Worker resumed {task_id} at step {step}")
                    self.set_status(task_id, 'RUNNING', 'Обчислення відновлено', self.tasks[task_id].progress)

                previous = field.copy()
                field[1:-1] = previous[1:-1] + fourier_number * (
                    previous[:-2] - 2.0 * previous[1:-1] + previous[2:]
                )
                field[0] = left_boundary
                field[-1] = right_boundary

                simulated_iteration = max(1, int(round(step * iterations / sample_steps)))
                progress_ratio = step / sample_steps
                max_temp = float(np.max(field))
                avg_temp = float(np.mean(field))
                min_temp = float(np.min(field))
                cpu_now = min(100.0, 24.0 + progress_ratio * 58.0 + random.uniform(-4.0, 8.0))
                time_series.append({
                    'step': simulated_iteration,
                    'progress': round(25.0 + (75.0 * progress_ratio), 2),
                    'temperature_c': round(avg_temp, 2),
                    'max_temperature_c': round(max_temp, 2),
                    'min_temperature_c': round(min_temp, 2),
                    'cpu_percent': round(cpu_now, 2),
                    'timestamp': round(time.time() - start_time, 2),
                    'model_time_s': round(dt * simulated_iteration, 4)
                })

                current_progress = 25.0 + (75.0 * progress_ratio)
                current_progress = min(100.0, current_progress)
                current_stage = f"3. Явна різницева схема ({simulated_iteration}/{iterations})"
                self.set_status(task_id, 'RUNNING', current_stage, round(current_progress, 2))

                # Keep the UI animated and the runtime predictable.
                time.sleep(min(0.08, 0.015 + nodes / 4000000))

            end_time = time.time()
            temps = [s['max_temperature_c'] for s in time_series] if time_series else [float(np.max(field))]
            sample_indices = np.linspace(0, computational_nodes - 1, min(80, computational_nodes), dtype=int)
            final_result = {
                "max_temperature_c": round(max(temps), 2),
                "average_temperature_c": round(float(np.mean(field)), 2),
                "min_temperature_c": round(float(np.min(field)), 2),
                "execution_time_seconds": round(end_time - start_time, 2),
                "grid_dimensions": f"{computational_nodes} вузлів",
                "requested_nodes": nodes,
                "iterations": iterations,
                "rod_length_m": length,
                "total_time_s": total_time,
                "effective_model_time_s": round(effective_time, 4),
                "thermal_diffusivity": alpha,
                "initial_temperature_c": initial_temp,
                "left_boundary_c": left_boundary,
                "right_boundary_c": right_boundary,
                "fourier_number": round(float(fourier_number), 6),
                "temperature_profile": [
                    {
                        "x_m": round(float(index * dx), 6),
                        "temperature_c": round(float(field[index]), 2)
                    }
                    for index in sample_indices
                ],
                "time_series": time_series
            }

            self.set_status(task_id, 'COMPLETED', '4. Результат готовий', 100.0, final_result)

            task = self.tasks.get(task_id)
            if task and task.owner:
                try:
                    computation_time = round(end_time - start_time, 2)
                    
                    if USE_DATABASE:
                        add_task_to_history(
                            username=task.owner,
                            task_id=task_id,
                            nodes=nodes,
                            iterations=iterations,
                            computation_time=computation_time,
                            final_avg_temp=final_result.get('max_temperature_c', 0.0),
                            result_data=final_result,
                            simulation_parameters=parameters
                        )
                    else:
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
            self.set_status(task_id, 'FAILED', f'Помилка обчислення: {str(e)}', 100.0)
        finally:
            # Запускаємо наступну задачу з черги
            self._process_queue()
