INDEX_CODE = "COMPOSITE"

WINDOW = 60

NUM_FEATURES = [
    "ret_1d", "ret_5d",
    "vol_20", "vol_60",
    "trend", "drawdown",
    "volume", "volume_change"
]

CYCLE_FEATURES = [
    "dow_sin", "dow_cos",
    "woy_sin", "woy_cos",
    "month_sin", "month_cos"
]

ALL_FEATURES = NUM_FEATURES + CYCLE_FEATURES

REGIME_TARGET = "regime"
VOLATILITY_TARGET = "future_vol"
CRASH_TARGET = "crash"

PREP_SQL = "v2/risk_engine/db/prep.sql"
CLEAN_SQL = "v2/risk_engine/db/clean.sql"
