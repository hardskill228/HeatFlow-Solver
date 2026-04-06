import os
import re
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Optional, List, Dict
import logging

logger = logging.getLogger(__name__)
NICKNAME_RE = re.compile(r"^[A-Za-z0-9_]{3,20}$")
USERNAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{3,23}$")

DATABASE_URL = os.getenv(
    'DATABASE_URL', 
    'postgresql://heatflow:heatflow123@localhost:5433/heatflow_db'
)


def get_connection():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise


def ensure_user_profile_columns() -> None:
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS name VARCHAR(100)")
        cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS avatar_url TEXT")
        cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS nickname VARCHAR(100)")
        cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS pending_nickname VARCHAR(100)")
        cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS nickname_reserved_until TIMESTAMP")
        cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS pending_username VARCHAR(100)")
        cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS username_reserved_until TIMESTAMP")
        cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS email VARCHAR(255)")
        cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS address TEXT")
        cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS city VARCHAR(100)")
        cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS phone VARCHAR(50)")
        cursor.execute(
            """
            UPDATE users
            SET name = nickname
            WHERE (name IS NULL OR name = '')
              AND nickname IS NOT NULL
              AND nickname <> ''
            """
        )
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_email ON users(email)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_nickname_lower ON users (LOWER(nickname))")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_pending_nickname_lower ON users (LOWER(pending_nickname))")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_pending_username_lower ON users (LOWER(pending_username))")
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"Error ensuring user profile columns: {e}")


def create_user(
    username: str,
    password_hash: str,
    name: Optional[str] = None,
    avatar_url: Optional[str] = None,
    email: Optional[str] = None,
    address: Optional[str] = None,
    city: Optional[str] = None,
    phone: Optional[str] = None
) -> bool:

    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            """
            INSERT INTO users (username, password_hash, name, avatar_url, email, address, city, phone)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (username, password_hash, name, avatar_url, email, address, city, phone)
        )
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"User '{username}' created successfully")
        return True
        
    except psycopg2.IntegrityError:
        logger.warning(f"User '{username}' already exists")
        return False
    except Exception as e:
        logger.error(f"Error creating user: {e}")
        return False


def get_user(username: str) -> Optional[Dict]:

    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute(
            """
            SELECT id, username, password_hash, name, avatar_url, nickname, pending_nickname, nickname_reserved_until,
                   pending_username, username_reserved_until, email, address, city, phone, created_at
            FROM users WHERE username = %s
            """,
            (username,)
        )
        
        user = cursor.fetchone()
        cursor.close()
        conn.close()
        
        return dict(user) if user else None
        
    except Exception as e:
        logger.error(f"Error getting user: {e}")
        return None


def _nickname_taken(cursor, nickname: str, exclude_username: Optional[str] = None) -> bool:
    params = [nickname, nickname]
    query = """
        SELECT 1
        FROM users
        WHERE (
            LOWER(COALESCE(nickname, '')) = LOWER(%s)
            OR (
                LOWER(COALESCE(pending_nickname, '')) = LOWER(%s)
                AND nickname_reserved_until IS NOT NULL
                AND nickname_reserved_until > CURRENT_TIMESTAMP
            )
        )
    """
    if exclude_username:
        query += " AND username <> %s"
        params.append(exclude_username)
    query += " LIMIT 1"
    cursor.execute(query, tuple(params))
    return cursor.fetchone() is not None


def _username_taken(cursor, username: str, exclude_username: Optional[str] = None) -> bool:
    params = [username, username]
    query = """
        SELECT 1
        FROM users
        WHERE (
            LOWER(COALESCE(username, '')) = LOWER(%s)
            OR (
                LOWER(COALESCE(pending_username, '')) = LOWER(%s)
                AND username_reserved_until IS NOT NULL
                AND username_reserved_until > CURRENT_TIMESTAMP
            )
        )
    """
    if exclude_username:
        query += " AND username <> %s"
        params.append(exclude_username)
    query += " LIMIT 1"
    cursor.execute(query, tuple(params))
    return cursor.fetchone() is not None


def get_nickname_availability(nickname: str, exclude_username: Optional[str] = None) -> Dict:
    candidate = (nickname or "").strip()
    if not candidate:
        return {"available": False, "reason": "Nickname is required."}
    if not NICKNAME_RE.fullmatch(candidate):
        return {"available": False, "reason": "Use 3-20 letters, numbers or underscore."}

    try:
        conn = get_connection()
        cursor = conn.cursor()
        taken = _nickname_taken(cursor, candidate, exclude_username=exclude_username)
        cursor.close()
        conn.close()
        if taken:
            return {"available": False, "reason": "Nickname is already reserved or active."}
        return {"available": True, "reason": "Nickname is available."}
    except Exception as e:
        logger.error(f"Error checking nickname availability: {e}")
        return {"available": False, "reason": "Could not check nickname availability."}


def get_username_availability(username: str, exclude_username: Optional[str] = None) -> Dict:
    candidate = (username or "").strip()
    if not candidate:
        return {"available": False, "reason": "Username is required."}
    if not USERNAME_RE.fullmatch(candidate):
        return {"available": False, "reason": "Use 4-24 chars, start with a letter, only letters, numbers or underscore."}

    try:
        conn = get_connection()
        cursor = conn.cursor()
        taken = _username_taken(cursor, candidate, exclude_username=exclude_username)
        cursor.close()
        conn.close()
        if taken:
            return {"available": False, "reason": "Username is already reserved or active."}
        return {"available": True, "reason": "Username is available."}
    except Exception as e:
        logger.error(f"Error checking username availability: {e}")
        return {"available": False, "reason": "Could not check username availability."}


def reserve_nickname(username: str, nickname: str, reservation_minutes: int = 15) -> Dict:
    candidate = (nickname or "").strip()
    availability = get_nickname_availability(candidate, exclude_username=username)
    if not availability["available"]:
        return availability

    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE users
            SET pending_nickname = %s,
                nickname_reserved_until = CURRENT_TIMESTAMP + (%s || ' minutes')::interval
            WHERE username = %s
            """,
            (candidate, reservation_minutes, username)
        )
        updated = cursor.rowcount > 0
        conn.commit()
        cursor.close()
        conn.close()
        if not updated:
            return {"available": False, "reason": "User not found."}
        return {"available": True, "reason": "Nickname reserved.", "nickname": candidate}
    except Exception as e:
        logger.error(f"Error reserving nickname: {e}")
        return {"available": False, "reason": "Could not reserve nickname."}


def activate_reserved_nickname(username: str) -> Dict:
    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(
            """
            UPDATE users
            SET nickname = pending_nickname,
                pending_nickname = NULL,
                nickname_reserved_until = NULL
            WHERE username = %s
              AND pending_nickname IS NOT NULL
              AND nickname_reserved_until IS NOT NULL
              AND nickname_reserved_until > CURRENT_TIMESTAMP
            RETURNING username, name, nickname, pending_nickname, nickname_reserved_until, email, address, city, phone, created_at
            """,
            (username,)
        )
        user = cursor.fetchone()
        conn.commit()
        cursor.close()
        conn.close()
        if not user:
            return {"success": False, "reason": "No active nickname reservation found."}
        return {"success": True, "user": dict(user)}
    except Exception as e:
        logger.error(f"Error activating nickname: {e}")
        return {"success": False, "reason": "Could not activate nickname."}


def reserve_username(username: str, desired_username: str, reservation_minutes: int = 15) -> Dict:
    candidate = (desired_username or "").strip()
    availability = get_username_availability(candidate, exclude_username=username)
    if not availability["available"]:
        return availability

    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE users
            SET pending_username = %s,
                username_reserved_until = CURRENT_TIMESTAMP + (%s || ' minutes')::interval
            WHERE username = %s
            """,
            (candidate, reservation_minutes, username)
        )
        updated = cursor.rowcount > 0
        conn.commit()
        cursor.close()
        conn.close()
        if not updated:
            return {"available": False, "reason": "User not found."}
        return {"available": True, "reason": "Username reserved.", "username": candidate}
    except Exception as e:
        logger.error(f"Error reserving username: {e}")
        return {"available": False, "reason": "Could not reserve username."}


def activate_reserved_username(current_username: str) -> Dict:
    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        cursor.execute(
            """
            SELECT pending_username
            FROM users
            WHERE username = %s
              AND pending_username IS NOT NULL
              AND username_reserved_until IS NOT NULL
              AND username_reserved_until > CURRENT_TIMESTAMP
            """,
            (current_username,)
        )
        row = cursor.fetchone()
        if not row:
            cursor.close()
            conn.close()
            return {"success": False, "reason": "No active username reservation found."}

        new_username = row["pending_username"]

        if _username_taken(cursor, new_username, exclude_username=current_username):
            cursor.close()
            conn.close()
            return {"success": False, "reason": "Username is already reserved or active."}

        cursor.execute(
            """
            UPDATE users
            SET username = pending_username,
                pending_username = NULL,
                username_reserved_until = NULL
            WHERE username = %s
            RETURNING username, name, nickname, pending_nickname, nickname_reserved_until,
                      pending_username, username_reserved_until, email, address, city, phone, created_at
            """,
            (current_username,)
        )
        user = cursor.fetchone()
        if not user:
            conn.rollback()
            cursor.close()
            conn.close()
            return {"success": False, "reason": "Could not activate username."}

        cursor.execute(
            "UPDATE user_tokens SET username = %s WHERE username = %s",
            (new_username, current_username)
        )
        cursor.execute(
            "UPDATE task_queue SET username = %s WHERE username = %s AND status IN ('QUEUED', 'RUNNING')",
            (new_username, current_username)
        )

        conn.commit()
        cursor.close()
        conn.close()
        return {"success": True, "user": dict(user), "old_username": current_username, "new_username": new_username}
    except Exception as e:
        logger.error(f"Error activating username: {e}")
        return {"success": False, "reason": "Could not activate username."}


def update_user_profile(
    username: str,
    name: Optional[str] = None,
    avatar_url: Optional[str] = None,
    email: Optional[str] = None,
    address: Optional[str] = None,
    city: Optional[str] = None,
    phone: Optional[str] = None
) -> bool:
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE users
            SET name = %s,
                avatar_url = %s,
                email = %s,
                address = %s,
                city = %s,
                phone = %s
            WHERE username = %s
            """,
            (name, avatar_url, email, address, city, phone, username)
        )
        updated = cursor.rowcount > 0
        conn.commit()
        cursor.close()
        conn.close()
        return updated
    except Exception as e:
        logger.error(f"Error updating user profile: {e}")
        return False


def get_leaderboard(limit: int = 10) -> List[Dict]:
    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(
            """
            SELECT
                u.username,
                u.username AS display_name,
                u.nickname,
                COUNT(th.id) AS sessions_count,
                MAX(COALESCE((th.result_data ->> 'max_temperature_c')::float, th.final_avg_temp)) AS best_temperature,
                AVG(th.computation_time) AS avg_duration
            FROM users u
            JOIN task_history th ON th.user_id = u.id
            GROUP BY u.id, u.username, u.nickname, u.name
            ORDER BY MAX(COALESCE((th.result_data ->> 'max_temperature_c')::float, th.final_avg_temp)) DESC, COUNT(th.id) DESC, u.username ASC
            LIMIT %s
            """,
            (limit,)
        )
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return [dict(row) for row in rows]
    except Exception as e:
        logger.error(f"Error getting leaderboard: {e}")
        return []


def get_all_users() -> List[Dict]:
    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(
            """
            SELECT id, username, COALESCE(name, nickname) AS name, email, address, city, phone, created_at
            FROM users
            ORDER BY created_at DESC
            """
        )
        users = cursor.fetchall()
        cursor.close()
        conn.close()
        return [dict(user) for user in users]
    except Exception as e:
        logger.error(f"Error getting all users: {e}")
        return []


def add_task_to_history(
    username: str,
    task_id: str,
    nodes: int,
    iterations: int,
    computation_time: float,
    final_avg_temp: float,
    result_data: dict = None
) -> bool:
 
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT id FROM users WHERE username = %s", (username,))
        result = cursor.fetchone()
        
        if not result:
            logger.warning(f"User '{username}' not found")
            return False
        
        user_id = result[0]
        
        import json
        result_json = json.dumps(result_data) if result_data else None
        
        cursor.execute(
            """
            INSERT INTO task_history 
            (task_id, user_id, nodes, iterations, computation_time, final_avg_temp, result_data)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (task_id, user_id, nodes, iterations, computation_time, final_avg_temp, result_json)
        )
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"Task {task_id} added to history for user '{username}'")
        return True
        
    except Exception as e:
        logger.error(f"Error adding task to history: {e}")
        return False


def get_user_history(username: str, limit: int = 50) -> List[Dict]:

    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute(
            """
            SELECT 
                th.task_id,
                th.nodes,
                th.iterations,
                th.computation_time,
                th.final_avg_temp,
                th.result_data,
                th.created_at as timestamp
            FROM task_history th
            JOIN users u ON th.user_id = u.id
            WHERE u.username = %s
            ORDER BY th.created_at DESC
            LIMIT %s
            """,
            (username, limit)
        )
        
        history = cursor.fetchall()
        cursor.close()
        conn.close()
        
        result = []
        for row in history:
            result_data = row['result_data'] if row['result_data'] else {}
            result.append({
                'task_id': row['task_id'],
                'nodes': row['nodes'],
                'iterations': row['iterations'],
                'computation_time': row['computation_time'],
                'final_avg_temp': row['final_avg_temp'],
                'result_data': result_data,
                'timestamp': row['timestamp']
            })
        
        return result
        
    except Exception as e:
        logger.error(f"Error getting user history: {e}")
        return []


def get_user_active_task_ids(username: str, limit: int = 20) -> List[str]:
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT task_id
            FROM task_queue
            WHERE username = %s
              AND status NOT IN ('COMPLETED', 'FAILED', 'CANCELLED')
            ORDER BY COALESCE(started_at, created_at) DESC
            LIMIT %s
            """,
            (username, limit)
        )
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return [row[0] for row in rows]
    except Exception as e:
        logger.error(f"Error getting active tasks for user '{username}': {e}")
        return []


def get_all_users_count() -> int:

    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM users")
        count = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        return count
        
    except Exception as e:
        logger.error(f"Error counting users: {e}")
        return 0


def get_all_tasks_count() -> int:
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM task_history")
        count = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        return count
        
    except Exception as e:
        logger.error(f"Error counting tasks: {e}")
        return 0



def add_task_to_queue(task_id: str, username: str, nodes: int, iterations: int) -> bool:
    """
    Додає задачу в спільну чергу PostgreSQL для round-robin балансування
    
    Args:
        task_id: UUID задачі
        username: Ім'я користувача
        nodes: Кількість вузлів
        iterations: Кількість ітерацій
    
    Returns:
        bool: True якщо успішно додано
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            """
            INSERT INTO task_queue (task_id, username, nodes, iterations, status)
            VALUES (%s, %s, %s, %s, 'QUEUED')
            """,
            (task_id, username, nodes, iterations)
        )
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"Task {task_id} added to shared queue")
        return True
        
    except Exception as e:
        logger.error(f"Error adding task to queue: {e}")
        return False


def get_next_queued_task(worker_id: str) -> Optional[Dict]:
    """
    Отримує наступну задачу з черги для воркера (round-robin)
    Використовує SELECT FOR UPDATE SKIP LOCKED для безпечної конкуренції
    
    Args:
        worker_id: Ідентифікатор воркера (api1 або api2)
    
    Returns:
        dict: {'task_id', 'username', 'nodes', 'iterations'} або None
    """
    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute(
            """
            UPDATE task_queue
            SET status = 'RUNNING', worker_id = %s, started_at = CURRENT_TIMESTAMP
            WHERE id = (
                SELECT id FROM task_queue
                WHERE status = 'QUEUED'
                ORDER BY created_at ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            RETURNING task_id, username, nodes, iterations
            """,
            (worker_id,)
        )
        
        task = cursor.fetchone()
        conn.commit()
        cursor.close()
        conn.close()
        
        if task:
            logger.info(f"Worker {worker_id} claimed task {task['task_id']}")
            return dict(task)
        return None
        
    except Exception as e:
        logger.error(f"Error getting next task: {e}")
        return None


def mark_task_completed(task_id: str) -> bool:
    """
    Позначає задачу як завершену в черзі
    
    Args:
        task_id: UUID задачі
    
    Returns:
        bool: True якщо успішно
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            """
            UPDATE task_queue
            SET status = 'COMPLETED', completed_at = CURRENT_TIMESTAMP
            WHERE task_id = %s
            """,
            (task_id,)
        )
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"Task {task_id} marked as completed")
        return True
        
    except Exception as e:
        logger.error(f"Error marking task completed: {e}")
        return False


def get_queue_position(task_id: str) -> int:
    """
    Отримує позицію задачі в черзі
    
    Args:
        task_id: UUID задачі
    
    Returns:
        int: Позиція в черзі (1-based) або 0 якщо не в черзі
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            """
            SELECT COUNT(*) + 1 as position
            FROM task_queue
            WHERE status = 'QUEUED' 
            AND created_at < (SELECT created_at FROM task_queue WHERE task_id = %s)
            """,
            (task_id,)
        )
        
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        return result[0] if result else 0
        
    except Exception as e:
        logger.error(f"Error getting queue position: {e}")
        return 0


def save_token(token: str, username: str) -> bool:
    """Зберігає токен в базі даних для спільного доступу між серверами"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            "INSERT INTO user_tokens (token, username) VALUES (%s, %s) ON CONFLICT (token) DO UPDATE SET username = %s",
            (token, username, username)
        )
        
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Error saving token: {e}")
        return False


def get_username_by_token(token: str) -> Optional[str]:
    """Отримує username за токеном з бази даних"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            "SELECT username FROM user_tokens WHERE token = %s AND expires_at > CURRENT_TIMESTAMP",
            (token,)
        )
        
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        return result[0] if result else None
    except Exception as e:
        logger.error(f"Error getting username by token: {e}")
        return None


def delete_token(token: str) -> bool:
    """Видаляє токен з бази даних при logout"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute("DELETE FROM user_tokens WHERE token = %s", (token,))
        
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Error deleting token: {e}")
        return False
