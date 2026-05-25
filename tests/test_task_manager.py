from backend.models import TaskState
from backend.task_manager import _normalize_simulation_parameters, TaskManager


def test_normalize_simulation_parameters_merges_defaults_and_casts_values():
    params = _normalize_simulation_parameters(
        {
            "rod_length_m": "2.5",
            "total_time_s": 10,
            "left_boundary_c": "80",
            "unknown": 123,
        }
    )

    assert params["rod_length_m"] == 2.5
    assert params["total_time_s"] == 10.0
    assert params["left_boundary_c"] == 80.0
    assert params["initial_temperature_c"] == 20.0
    assert "unknown" not in params


def test_task_manager_pause_resume_and_cancel_state_transitions(monkeypatch):
    monkeypatch.setattr("backend.task_manager.USE_DATABASE", False)
    manager = TaskManager()
    manager.tasks.clear()
    manager.active_connections.clear()
    manager.cancel_flags.clear()
    manager.pause_flags.clear()

    task = TaskState(task_id="task-1", status="RUNNING", stage="Виконується", progress=15)
    manager.tasks[task.task_id] = task

    assert manager.request_pause(task.task_id) is True
    assert manager.tasks[task.task_id].status == "PAUSED"
    assert manager.pause_flags[task.task_id] is True

    assert manager.request_resume(task.task_id) is True
    assert manager.tasks[task.task_id].status == "RUNNING"
    assert task.task_id not in manager.pause_flags

    assert manager.request_cancel(task.task_id) is True
    assert manager.tasks[task.task_id].status == "CANCELLATION_REQUESTED"
    assert manager.cancel_flags[task.task_id] is True


def test_request_cancel_rejects_completed_task(monkeypatch):
    monkeypatch.setattr("backend.task_manager.USE_DATABASE", False)
    manager = TaskManager()
    manager.tasks.clear()
    manager.tasks["done"] = TaskState(task_id="done", status="COMPLETED", stage="Готово")

    assert manager.request_cancel("done") is False
