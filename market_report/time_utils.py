from datetime import datetime
from zoneinfo import ZoneInfo


TAIPEI_TZ = ZoneInfo("Asia/Taipei")


def now_taipei() -> datetime:
    return datetime.now(TAIPEI_TZ)


def timestamp_taipei() -> str:
    return now_taipei().strftime("%Y-%m-%d %H:%M:%S")


def today_taipei() -> str:
    return now_taipei().strftime("%Y-%m-%d")
