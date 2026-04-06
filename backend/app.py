from pathlib import Path
import os
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from starlette.middleware.cors import CORSMiddleware

from .task_manager import TaskManager
from .database import ensure_user_profile_columns


app = FastAPI(title="HeatFlow Solver Async API")

FRONTEND_DIR = str(Path(__file__).resolve().parent.parent.joinpath('frontend'))
app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.state.token_map = {}
ensure_user_profile_columns()

task_manager = TaskManager()

from . import routes
