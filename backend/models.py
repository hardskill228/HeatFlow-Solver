import time
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, RootModel


class TaskState(BaseModel):
    
    task_id: str
    status: str = "PENDING"
    stage: str = "Initializing"
    progress: float = 0.0
    start_time: float = time.time()
    end_time: Optional[float] = None
    result: Optional[Dict[str, Any]] = None
    owner: Optional[str] = None
    nodes: Optional[int] = None
    iterations: Optional[int] = None
    queue_position: Optional[int] = None
    estimated_wait_seconds: Optional[float] = None
    worker_id: Optional[str] = None  # api1, api2, або None


class TaskInput(BaseModel):
    
    nodes: int
    iterations: int


class TaskHistory(RootModel):
    
    root: List[TaskState]
