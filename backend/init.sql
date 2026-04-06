-- Ініціалізація бази даних HeatFlow Solver
-- Створюється автоматично при першому запуску PostgreSQL контейнера

-- Таблиця користувачів
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(100) UNIQUE NOT NULL,
    password_hash VARCHAR(64) NOT NULL, 
    name VARCHAR(100),
    avatar_url TEXT,
    nickname VARCHAR(100),
    pending_nickname VARCHAR(100),
    nickname_reserved_until TIMESTAMP,
    pending_username VARCHAR(100),
    username_reserved_until TIMESTAMP,
    email VARCHAR(255),
    address TEXT,
    city VARCHAR(100),
    phone VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблиця історії задач
CREATE TABLE IF NOT EXISTS task_history (
    id SERIAL PRIMARY KEY,
    task_id VARCHAR(36) NOT NULL,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    nodes INTEGER NOT NULL,
    iterations INTEGER NOT NULL,
    computation_time FLOAT NOT NULL,
    final_avg_temp FLOAT NOT NULL,
    result_data JSONB,  -- Повний result включаючи time_series
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Індекси для task_history
CREATE INDEX IF NOT EXISTS idx_user_id ON task_history(user_id);
CREATE INDEX IF NOT EXISTS idx_task_id ON task_history(task_id);
CREATE INDEX IF NOT EXISTS idx_created_at ON task_history(created_at);

-- Вставка тестового користувача (username: admin, password: admin)
-- SHA-256("admin") = 8c6976e5b5410415bde908bd4dee15dfb167a9c873fc4bb8a81f6f2ab448a918
INSERT INTO users (username, password_hash) 
VALUES ('admin', '8c6976e5b5410415bde908bd4dee15dfb167a9c873fc4bb8a81f6f2ab448a918')
ON CONFLICT (username) DO NOTHING;

-- Додаткові індекси для оптимізації
CREATE INDEX IF NOT EXISTS idx_username ON users(username);
CREATE INDEX IF NOT EXISTS idx_email ON users(email);
CREATE INDEX IF NOT EXISTS idx_nickname_lower ON users (LOWER(nickname));
CREATE INDEX IF NOT EXISTS idx_pending_nickname_lower ON users (LOWER(pending_nickname));
CREATE INDEX IF NOT EXISTS idx_pending_username_lower ON users (LOWER(pending_username));
CREATE INDEX IF NOT EXISTS idx_user_history ON task_history(user_id, created_at DESC);

-- Таблиця для спільної черги задач між api1 та api2 (round-robin)
CREATE TABLE IF NOT EXISTS task_queue (
    id SERIAL PRIMARY KEY,
    task_id VARCHAR(36) UNIQUE NOT NULL,
    username VARCHAR(100) NOT NULL,
    nodes INTEGER NOT NULL,
    iterations INTEGER NOT NULL,
    status VARCHAR(20) DEFAULT 'QUEUED', -- QUEUED, RUNNING, COMPLETED
    worker_id VARCHAR(50), -- api1 або api2
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP
);

-- Індекси для швидкого пошуку
CREATE INDEX IF NOT EXISTS idx_queue_status ON task_queue(status, created_at);
CREATE INDEX IF NOT EXISTS idx_queue_worker ON task_queue(worker_id);

-- Таблиця для зберігання токенів (спільна для api1 та api2)
CREATE TABLE IF NOT EXISTS user_tokens (
    id SERIAL PRIMARY KEY,
    token VARCHAR(64) UNIQUE NOT NULL,
    username VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP + INTERVAL '7 days'
);

CREATE INDEX IF NOT EXISTS idx_token ON user_tokens(token);
CREATE INDEX IF NOT EXISTS idx_token_username ON user_tokens(username);
