# HeatFlow Solver

Веб-застосунок для симуляції теплопровідності з авторизацією, історією запусків, WebSocket-прогресом і PostgreSQL.

## Основне

- FastAPI бекенд
- HTML/CSS/Vanilla JS фронтенд
- PostgreSQL для користувачів, токенів та історії
- WebSocket для live-оновлення прогресу
- Docker-конфіг для багатосервісного запуску

## Локальний запуск

```bash
docker start heatflow_postgres
source venv/bin/activate
uvicorn backend.server:app --host 0.0.0.0 --port 8000
```

Відкрити: `http://localhost:8000`

## Docker-запуск

```bash
docker-compose up --build
```

Відкрити: `http://localhost:8080`

## Структура

```text
heatflow-solver/
├── backend/
│   ├── app.py
│   ├── server.py
│   ├── routes.py
│   ├── task_manager.py
│   ├── database.py
│   ├── models.py
│   ├── data.py
│   ├── init.sql
│   ├── Dockerfile
│   └── requirements.txt
├── frontend/
│   ├── index.html
│   ├── login.html
│   ├── profile.html
│   └── styles.css
├── nginx/
│   └── nginx.conf
├── docker-compose.yml
├── LICENSE
└── README.md
```

## База даних

Приклад перегляду користувачів:

```bash
docker exec heatflow_postgres psql -U heatflow -d heatflow_db -c "
SELECT id, username, name, email, address, city, phone, created_at
FROM users
ORDER BY created_at DESC;
"
```
