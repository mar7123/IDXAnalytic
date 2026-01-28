from datetime import datetime
from config import LOG_NAME


def write_log(msg):
    with open(LOG_NAME, "a") as fw:
        fw.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {msg}\n")
