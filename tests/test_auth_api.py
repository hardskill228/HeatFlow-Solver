import hashlib

from backend.app import app


def test_register_creates_account_with_profile_fields(client, saved_accounts):
    response = client.post(
        "/api/auth/register",
        json={
            "username": "thermal_user",
            "password": "secret123",
            "name": "Тестовий Користувач",
            "email": "thermal@example.com",
            "address": "Вулиця 1",
            "city": "Київ",
            "phone": "+380001112233",
        },
    )

    assert response.status_code == 200
    assert response.json() == {"username": "thermal_user"}

    account = saved_accounts()["thermal_user"]
    assert account["password_hash"] == hashlib.sha256(b"secret123").hexdigest()
    assert account["name"] == "Тестовий Користувач"
    assert account["city"] == "Київ"


def test_register_rejects_duplicate_username(client):
    payload = {"username": "same_user", "password": "secret123"}

    first = client.post("/api/auth/register", json=payload)
    second = client.post("/api/auth/register", json=payload)

    assert first.status_code == 200
    assert second.status_code == 400
    assert second.json()["detail"] == "username already exists"


def test_login_returns_token_and_enables_profile_access(client):
    client.post("/api/auth/register", json={"username": "login_user", "password": "secret123"})

    response = client.post(
        "/api/auth/login",
        json={"username": "login_user", "password": "secret123"},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["username"] == "login_user"
    assert body["token"]
    assert app.state.token_map[body["token"]] == "login_user"

    profile = client.get(
        "/api/account/profile",
        headers={"Authorization": f"Bearer {body['token']}"},
    )

    assert profile.status_code == 200
    assert profile.json()["username"] == "login_user"


def test_login_rejects_wrong_password(client):
    client.post("/api/auth/register", json={"username": "login_user", "password": "secret123"})

    response = client.post(
        "/api/auth/login",
        json={"username": "login_user", "password": "wrong"},
    )

    assert response.status_code == 401
    assert response.json()["detail"] == "invalid credentials"


def test_profile_requires_authorization(client):
    response = client.get("/api/account/profile")

    assert response.status_code == 401
    assert response.json()["detail"] == "unauthorized"
