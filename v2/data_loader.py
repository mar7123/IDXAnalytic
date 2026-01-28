import json
from sqlalchemy import create_engine
from config import DB_CONFIG_PATH


def load_db_config(path=DB_CONFIG_PATH):
    with open(path, "r") as f:
        return json.load(f)


def get_engine(config_path=DB_CONFIG_PATH):
    cfg = load_db_config(config_path)
    return create_engine(
        f"mysql+pymysql://{cfg['user']}:{cfg['password']}@{cfg['host']}/{cfg['database']}"
    )
