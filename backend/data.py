import json
import os
from pathlib import Path

DATA_DIR = str(Path(__file__).resolve().parent.joinpath('data'))
os.makedirs(DATA_DIR, exist_ok=True)
ACCOUNTS_FILE = Path(DATA_DIR).joinpath('accounts.json')


def load_accounts():
    if not ACCOUNTS_FILE.exists():
        return {}
    try:
        return json.loads(ACCOUNTS_FILE.read_text(encoding='utf-8'))
    except Exception:
        return {}


def save_accounts(d):
    ACCOUNTS_FILE.write_text(json.dumps(d, indent=2), encoding='utf-8')
