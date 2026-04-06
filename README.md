# HeatFlow Solver

`HeatFlow Solver` — курсовий вебпроєкт для моделювання задачі теплопровідності з авторизацією користувачів, асинхронним виконанням обчислень, відстеженням прогресу в реальному часі та Docker-розгортанням.

## Можливості

- реєстрація та вхід користувачів
- запуск обчислювальних задач із параметрами `nodes` та `iterations`
- відображення прогресу через WebSocket
- збереження історії задач у PostgreSQL
- сторінка профілю з даними користувача та результатами запусків
- leaderboard-сторінка
- запуск у Docker Compose з `NGINX + 2 API servers + PostgreSQL`

## Технології

- `Python`
- `FastAPI`
- `Uvicorn`
- `PostgreSQL`
- `HTML / CSS / Vanilla JavaScript`
- `WebSocket`
- `Docker`
- `Docker Compose`
- `NGINX`

## Архітектура

Система складається з таких компонентів:

- `frontend` — клієнтський інтерфейс
- `backend` — API, автентифікація, менеджер задач, інтеграція з БД
- `postgres` — збереження користувачів, токенів, черги та історії задач
- `nginx` — reverse proxy і балансування навантаження між двома API-серверами

Схема роботи:

```text
Browser
   -> NGINX
      -> api1
      -> api2
          -> PostgreSQL
```

## Швидкий старт

### Варіант 1. Docker

Основний спосіб запуску:

```bash
docker-compose up -d --build
```

Після запуску застосунок доступний за адресою:

```text
http://localhost:8080
```

Перевірка контейнерів:

```bash
docker-compose ps
```

Зупинка:

```bash
docker-compose down
```

### Варіант 2. Локальний запуск backend

Якщо потрібно підняти застосунок без повного Docker-стеку:

```bash
docker start heatflow_postgres
source venv/bin/activate
uvicorn backend.server:app --host 0.0.0.0 --port 8000 --reload
```

Адреса:

```text
http://localhost:8000
```

## Docker-сервіси

У `docker-compose.yml` описані:

- `postgres` — база даних `PostgreSQL 15`
- `api1` — перший екземпляр FastAPI
- `api2` — другий екземпляр FastAPI
- `nginx` — вхідна точка на порті `8080`

Порти:

- `8080` -> вебзастосунок через `nginx`
- `5433` -> локальний доступ до `PostgreSQL`

## Структура проєкту

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
│   ├── leaderboard.html
│   └── styles.css
├── nginx/
│   └── nginx.conf
├── docker-compose.yml
├── COURSEWORK_BRIEF.md
├── GITHUB_PUBLISH.md
├── LICENSE
└── README.md
```

## Основні сторінки

- `/` — сторінка входу
- `/app` — головна сторінка запуску задач
- `/profile` — профіль користувача та історія
- `/leaderboard` — рейтинг користувачів

## Дані, що зберігаються в PostgreSQL

У базі даних зберігаються:

- користувачі
- токени автентифікації
- активні задачі
- черга задач
- історія завершених задач
- результати обчислень

Приклад перегляду користувачів:

```bash
docker exec heatflow_postgres psql -U heatflow -d heatflow_db -c "
SELECT id, username, name, email, address, city, phone, created_at
FROM users
ORDER BY created_at DESC;
"
```

## Призначення проєкту

Проєкт створений як курсова робота з акцентом на:

- розробку вебзастосунку
- інтеграцію обчислювального модуля у web-інтерфейс
- роботу з базою даних
- асинхронну обробку задач
- використання Docker-інфраструктури

## Додаткові матеріали

- [COURSEWORK_BRIEF.md](COURSEWORK_BRIEF.md) — бриф для генерації тексту курсової
- [GITHUB_PUBLISH.md](GITHUB_PUBLISH.md) — коротка інструкція для публікації проєкту на GitHub
