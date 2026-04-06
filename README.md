<div align="center">

# HeatFlow Solver

Вебзастосунок для моделювання задач теплопровідності з асинхронним виконанням обчислень, відстеженням прогресу в реальному часі, збереженням даних у PostgreSQL та Docker-розгортанням.

![Python](https://img.shields.io/badge/Python-3.x-3776AB?style=for-the-badge&logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-API-009688?style=for-the-badge&logo=fastapi&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-База_даних-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![NGINX](https://img.shields.io/badge/NGINX-Reverse_Proxy-009639?style=for-the-badge&logo=nginx&logoColor=white)

</div>

## Огляд

`HeatFlow Solver` — це курсова робота у форматі вебпроєкту, яка поєднує браузерний інтерфейс, FastAPI-бекенд, PostgreSQL і контейнеризовану інфраструктуру для запуску обчислювальних задач, пов’язаних із моделюванням теплопровідності.

Система підтримує:

- реєстрацію та вхід користувачів
- створення задач із параметрами
- відображення прогресу через WebSocket
- збереження історії задач у PostgreSQL
- сторінку профілю та сторінку рейтингу
- багатосервісне розгортання через `NGINX + 2 API-сервери + PostgreSQL`

## Технології

- `Python`
- `FastAPI`
- `Uvicorn`
- `PostgreSQL`
- `HTML`
- `CSS`
- `Vanilla JavaScript`
- `WebSocket`
- `Docker`
- `Docker Compose`
- `NGINX`

## Архітектура

Застосунок побудований як невелика розподілена вебсистема:

```text
Браузер
   -> NGINX
      -> api1
      -> api2
          -> PostgreSQL
```

### Основні компоненти

- `frontend` — клієнтський інтерфейс для входу, створення задач, моніторингу, профілю та рейтингу
- `backend` — API-маршрути, автентифікація, керування задачами та інтеграція з базою даних
- `postgres` — постійне збереження користувачів, токенів, активних задач, черги та історії
- `nginx` — reverse proxy і балансувальник навантаження

## Основні можливості

- асинхронне виконання обчислювальних задач
- відображення прогресу виконання в реальному часі
- збереження історії запусків
- керування задачами: пауза, відновлення, скасування
- робота з кількома API-екземплярами через балансувальник
- локальне розгортання через Docker

## Швидкий старт

### Docker-розгортання

Основний спосіб запуску проєкту:

```bash
docker-compose up -d --build
```

Після запуску застосунок доступний за адресою:

```text
http://localhost:8080
```

Перевірка стану контейнерів:

```bash
docker-compose ps
```

Зупинка:

```bash
docker-compose down
```

### Локальний запуск бекенду

Якщо потрібно запустити бекенд без повного Docker-стеку:

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

- `postgres` — `PostgreSQL 15`
- `api1` — перший екземпляр FastAPI
- `api2` — другий екземпляр FastAPI
- `nginx` — вхідна точка на порті `8080`

### Порти

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
├── LICENSE
└── README.md
```

## Основні сторінки

- `/` — сторінка входу
- `/app` — головна сторінка створення задач і моніторингу
- `/profile` — профіль користувача та історія задач
- `/leaderboard` — сторінка рейтингу

## Використання бази даних

PostgreSQL зберігає:

- облікові записи користувачів
- токени автентифікації
- активні задачі
- задачі в черзі
- історію завершених задач
- результати обчислень

Приклад запиту:

```bash
docker exec heatflow_postgres psql -U heatflow -d heatflow_db -c "
SELECT id, username, name, email, address, city, phone, created_at
FROM users
ORDER BY created_at DESC;
"
```

## Контекст курсової роботи

Репозиторій створений як курсова робота з фокусом на:

- розробку вебзастосунку
- інтеграцію обчислювального модуля у вебінтерфейс
- керування життєвим циклом задач через базу даних
- асинхронну взаємодію між клієнтом і сервером
- контейнеризоване розгортання

## Ліцензія

У корені проєкту присутній файл `LICENSE`.
