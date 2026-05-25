from pathlib import Path


FRONTEND_DIR = Path(__file__).resolve().parents[1] / "frontend"


def test_auth_page_no_demo_credentials_text():
    login_html = (FRONTEND_DIR / "login.html").read_text(encoding="utf-8")

    assert "Для демонстрації" not in login_html
    assert "admin з паролем admin" not in login_html


def test_frontend_pages_declare_ukrainian_language():
    for page_name in ["index.html", "login.html", "leaderboard.html"]:
        html = (FRONTEND_DIR / page_name).read_text(encoding="utf-8")

        assert '<html lang="uk">' in html


def test_main_app_contains_core_ukrainian_controls():
    index_html = (FRONTEND_DIR / "index.html").read_text(encoding="utf-8")

    assert "Запустити розрахунок" in index_html
    assert "Скасувати задачу" in index_html
    assert "Профіль" in index_html
