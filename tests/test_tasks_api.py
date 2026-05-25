from backend.models import TaskState
from backend import routes


class FakeTaskManager:
    MAX_NODES = 100000
    MAX_TASKS = 50
    MAX_CONCURRENT_TASKS = 10

    def __init__(self):
        self.tasks = {}
        self.started_payloads = []
        self.cancel_requested = False

    def start_new_task(self, nodes, iterations, username=None, simulation_parameters=None):
        self.started_payloads.append(
            {
                "nodes": nodes,
                "iterations": iterations,
                "username": username,
                "simulation_parameters": simulation_parameters,
            }
        )
        task = TaskState(
            task_id=f"task-{len(self.started_payloads)}",
            status="RUNNING",
            stage="Запуск задачі",
            progress=0,
            owner=username,
            nodes=nodes,
            iterations=iterations,
            worker_id="api-test",
            **simulation_parameters,
        )
        self.tasks[task.task_id] = task
        return task

    def request_cancel(self, task_id):
        self.cancel_requested = True
        task = self.tasks.get(task_id)
        if task:
            task.status = "CANCELLATION_REQUESTED"
            return True
        return False

    def get_status(self, task_id):
        return self.tasks.get(task_id)

    def request_pause(self, task_id):
        task = self.tasks.get(task_id)
        if task and task.status == "RUNNING":
            task.status = "PAUSED"
            return True
        return False

    def request_resume(self, task_id):
        task = self.tasks.get(task_id)
        if task and task.status == "PAUSED":
            task.status = "RUNNING"
            return True
        return False


def test_start_task_passes_model_parameters_to_manager(client, monkeypatch):
    manager = FakeTaskManager()
    monkeypatch.setattr(routes, "task_manager", manager)

    response = client.post(
        "/api/tasks/start",
        json={
            "nodes": 30,
            "iterations": 20,
            "rod_length_m": 2,
            "total_time_s": 5,
            "thermal_diffusivity": 0.0002,
            "initial_temperature_c": 15,
            "left_boundary_c": 120,
            "right_boundary_c": 25,
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["task_id"] == "task-1"
    assert body["worker_id"] == "api-test"
    assert body["rod_length_m"] == 2
    assert manager.started_payloads[0]["nodes"] == 30
    assert manager.started_payloads[0]["simulation_parameters"]["left_boundary_c"] == 120


def test_start_task_rejects_too_many_nodes(client, monkeypatch):
    manager = FakeTaskManager()
    manager.MAX_NODES = 25
    monkeypatch.setattr(routes, "task_manager", manager)

    response = client.post(
        "/api/tasks/start",
        json={
            "nodes": 30,
            "iterations": 20,
            "rod_length_m": 1,
            "total_time_s": 5,
            "thermal_diffusivity": 0.0002,
            "initial_temperature_c": 15,
            "left_boundary_c": 120,
            "right_boundary_c": 25,
        },
    )

    assert response.status_code == 400
    assert "Кількість вузлів" in response.json()["detail"]


def test_cancel_task_returns_ukrainian_success_message(client, monkeypatch):
    manager = FakeTaskManager()
    task = manager.start_new_task(
        20,
        10,
        simulation_parameters={
            "rod_length_m": 1,
            "total_time_s": 1,
            "thermal_diffusivity": 0.000115,
            "initial_temperature_c": 20,
            "left_boundary_c": 100,
            "right_boundary_c": 20,
        },
    )
    monkeypatch.setattr(routes, "task_manager", manager)

    response = client.post(f"/api/tasks/{task.task_id}/cancel")

    assert response.status_code == 200
    assert response.json()["status"] == "CANCELLATION_REQUESTED"
    assert "Надіслано запит на скасування" in response.json()["message"]


def test_pause_and_resume_task(client, monkeypatch):
    manager = FakeTaskManager()
    task = manager.start_new_task(
        20,
        10,
        simulation_parameters={
            "rod_length_m": 1,
            "total_time_s": 1,
            "thermal_diffusivity": 0.000115,
            "initial_temperature_c": 20,
            "left_boundary_c": 100,
            "right_boundary_c": 20,
        },
    )
    monkeypatch.setattr(routes, "task_manager", manager)

    pause = client.post(f"/api/tasks/{task.task_id}/pause")
    resume = client.post(f"/api/tasks/{task.task_id}/resume")

    assert pause.status_code == 200
    assert pause.json()["status"] == "PAUSE_REQUESTED"
    assert resume.status_code == 200
    assert resume.json()["status"] == "RESUMED"
