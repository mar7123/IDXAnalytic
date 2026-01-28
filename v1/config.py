RANDOM_SEED = 42

RISK_FEATURES = [
    "ret_1d",
    "ret_3d",
    "ret_5d",
    "vol_5d",
    "vol_10d",
    "volume_ratio_10d",
]

RISK_LABEL_COL = "risk_on_label"

RISK_MIN_SCORE = 0.2
RISK_OFF_THRESHOLD = 0.4
RISK_ON_THRESHOLD = 0.6

CRASH_RET_5D_THRESHOLD = -0.03
CRASH_VOL_QUANTILE = 0.95

RISK_LGB_PARAMS = {
    "objective": "binary",
    "metric": "auc",
    "learning_rate": 0.05,
    "num_leaves": 7,
    "max_depth": 3,
    "min_data_in_leaf": 40,
    "feature_fraction": 0.7,
    "bagging_fraction": 0.7,
    "bagging_freq": 5,
    "verbosity": -1,
    "seed": RANDOM_SEED,
}

RISK_NUM_BOOST_ROUND = 300
RISK_EARLY_STOPPING = 30

ALPHA_FEATURES = [
    "ret_1d", "ret_3d", "ret_5d",
    "volatility_5d", "volatility_10d",
    "volume_ratio_10d",
    "bid_offer_imbalance",
    "foreign_flow_ratio",
    "non_regular_vol_ratio",
    "hl_range", "gap_open",
]

ALPHA_LABEL_COL = "label"
ALPHA_GROUP_COL = "date"

ALPHA_LGB_PARAMS = {
    "objective": "lambdarank",
    "metric": "ndcg",
    "ndcg_eval_at": [3, 5],
    "learning_rate": 0.05,
    "num_leaves": 31,
    "min_data_in_leaf": 50,
    "feature_fraction": 0.8,
    "bagging_fraction": 0.8,
    "bagging_freq": 5,
    "verbosity": -1,
    "seed": RANDOM_SEED,
}

ALPHA_NUM_BOOST_ROUND = 500
ALPHA_EARLY_STOPPING = 50

MAX_POSITIONS = 5
MAX_WEIGHT_PER_STOCK = 0.30

EXPOSURE_BY_RISK = {
    "RISK_OFF": 0.2,
    "NEUTRAL": 1.0,
    "RISK_ON": 1.0,
}

INITIAL_CAPITAL = 1.0
HOLD_DAYS = 3
TRANSACTION_COST = 0.0002

DB_CONFIG_PATH = "v1/db/db_config.json"
MODEL_OUTPUT_PATH = "v1/outputs/stock_forecast_model.keras"
OUTPUT_PATH = "v1/outputs/output.xlsx"
LOG_NAME = "v1/log.txt"
