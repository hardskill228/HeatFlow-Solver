import time
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field, RootModel


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
    rod_length_m: Optional[float] = None
    total_time_s: Optional[float] = None
    thermal_diffusivity: Optional[float] = None
    initial_temperature_c: Optional[float] = None
    left_boundary_c: Optional[float] = None
    right_boundary_c: Optional[float] = None
    queue_position: Optional[int] = None
    estimated_wait_seconds: Optional[float] = None
    worker_id: Optional[str] = None  # api1, api2, або None


class TaskInput(BaseModel):
    
    nodes: int = Field(200, ge=20, le=100000)
    iterations: int = Field(6000, ge=10, le=1000000)
    rod_length_m: float = Field(1.0, gt=0, le=1000)
    total_time_s: float = Field(120.0, gt=0, le=100000)
    thermal_diffusivity: float = Field(0.000115, gt=0, le=1)
    initial_temperature_c: float = Field(20.0, ge=-273.15, le=5000)
    left_boundary_c: float = Field(100.0, ge=-273.15, le=5000)
    right_boundary_c: float = Field(20.0, ge=-273.15, le=5000)


class TaskHistory(RootModel):
    
    root: List[TaskState]
