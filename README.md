<div align="center">

# HeatFlow Solver

Web application for heat conduction simulation with asynchronous task execution, real-time progress tracking, PostgreSQL persistence, and Docker-based deployment.

![Python](https://img.shields.io/badge/Python-3.x-3776AB?style=for-the-badge&logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-API-009688?style=for-the-badge&logo=fastapi&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Database-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![NGINX](https://img.shields.io/badge/NGINX-Reverse_Proxy-009639?style=for-the-badge&logo=nginx&logoColor=white)

</div>

## Overview

`HeatFlow Solver` is a coursework project that combines a browser-based interface, a FastAPI backend, a PostgreSQL database, and containerized infrastructure for running computational heat-transfer tasks.

The system supports:

- user registration and login
- task creation with configurable parameters
- real-time progress updates through WebSocket
- task history persistence in PostgreSQL
- profile and leaderboard pages
- multi-service deployment with `NGINX + 2 API instances + PostgreSQL`

## Tech Stack

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

## Architecture

The application is built as a small distributed web system:

```text
Browser
   -> NGINX
      -> api1
      -> api2
          -> PostgreSQL
```

### Components

- `frontend` — browser UI for authentication, task creation, monitoring, and profile pages
- `backend` — API routes, authentication, task orchestration, and database integration
- `postgres` — persistent storage for users, tokens, active tasks, queue state, and task history
- `nginx` — reverse proxy and load balancer for incoming traffic

## Key Features

- asynchronous execution of computational tasks
- live task progress updates
- persistent task history
- pause, resume, and cancel task controls
- multiple API instances behind a load balancer
- Docker-based local deployment

## Quick Start

### Docker Deployment

Primary way to run the project:

```bash
docker-compose up -d --build
```

Application URL:

```text
http://localhost:8080
```

Check running services:

```bash
docker-compose ps
```

Stop the stack:

```bash
docker-compose down
```

### Local Backend Run

If you want to run the backend outside the full Docker stack:

```bash
docker start heatflow_postgres
source venv/bin/activate
uvicorn backend.server:app --host 0.0.0.0 --port 8000 --reload
```

Application URL:

```text
http://localhost:8000
```

## Docker Services

Defined in `docker-compose.yml`:

- `postgres` — `PostgreSQL 15`
- `api1` — first FastAPI instance
- `api2` — second FastAPI instance
- `nginx` — public entry point on port `8080`

### Exposed Ports

- `8080` -> web application through `nginx`
- `5433` -> local PostgreSQL access

## Project Structure

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

## Main Pages

- `/` — login page
- `/app` — task creation and monitoring
- `/profile` — user profile and task history
- `/leaderboard` — leaderboard page

## Database Usage

PostgreSQL stores:

- user accounts
- authentication tokens
- active tasks
- queued tasks
- completed task history
- computation results

Example query:

```bash
docker exec heatflow_postgres psql -U heatflow -d heatflow_db -c "
SELECT id, username, name, email, address, city, phone, created_at
FROM users
ORDER BY created_at DESC;
"
```

## Coursework Context

This repository was created as a coursework project focused on:

- web application development
- integration of a computational module into a web interface
- database-backed task lifecycle management
- asynchronous communication
- containerized deployment

## License

This repository includes a `LICENSE` file in the project root.
