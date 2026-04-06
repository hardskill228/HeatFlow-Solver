# GitHub Publish Guide

## Local Preparation

This project is ready to be versioned with Git and published to GitHub.

Ignored by default:

- `venv/`
- `__pycache__/`
- IDE folders
- local data file `backend/data/accounts.json`

## Recommended Repository Name

`heatflow-solver`

## Recommended Description

Coursework project: web application for heat conduction simulation with FastAPI, PostgreSQL, WebSocket progress tracking, and Docker deployment.

## Local Commands

Initialize repository:

```bash
git init
git branch -M main
git add .
git commit -m "Initial commit: HeatFlow Solver coursework project"
```

If Git asks for identity:

```bash
git config user.name "Your Name"
git config user.email "you@example.com"
```

## Publish With GitHub CLI

If `gh` is installed and authenticated:

```bash
gh repo create heatflow-solver --public --source=. --remote=origin --push
```

## Publish Manually

1. Create an empty repository on GitHub named `heatflow-solver`
2. Copy its remote URL
3. Run:

```bash
git remote add origin <YOUR_GITHUB_REPO_URL>
git push -u origin main
```

## Suggested Repository Topics

- `fastapi`
- `postgresql`
- `docker`
- `websocket`
- `simulation`
- `heat-transfer`
- `coursework`

