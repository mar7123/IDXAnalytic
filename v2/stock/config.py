RETURN_FEATURE_COLS = [
    "ret_1d_n", "ret_5d_n", "ret_20d_n",
    "gap_n", "intraday_range_n", "close_position",
    "vol_20_n", "drawdown_n",
    "turnover_n", "foreign_flow_n",
    "order_imbalance_n", "spread_n",
    "dow_sin", "dow_cos", "woy_sin", "woy_cos",
    "month_sin", "month_cos"
]

VOL_FEATURES = [
    "ret_1d_n",
    "ret_5d_n",
    "ret_20d_n",
    "vol_20_n",
    "drawdown_n",
    "turnover_n",
    "foreign_flow_n",
    "order_imbalance_n",
    "spread_n",
]

DD_FEATURES = [
    "ret_1d_n",
    "ret_5d_n",
    "vol_20_n",
    "drawdown_n",
    "turnover_n",
    "foreign_flow_n",
    "order_imbalance_n",
    "spread_n",
]


WINDOW = 60
PREP_SQL = "v2/stock/db/prep.sql"
CLEAN_SQL = "v2/stock/db/clean.sql"
