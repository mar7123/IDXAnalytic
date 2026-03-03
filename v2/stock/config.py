INDEX_FEATURE_COLS = [
    "idx_ret_1d_n",
    "idx_ret_5d_n",
    "idx_close_pos_n",
    "idx_range_n",
    "idx_vol_20_n",
    "idx_value_z_n_n",
]

CURRENCY_EXCHANGE_RATE_FEATURE_COLS = [
    "currency_exchange_rate_daily_return_n",
    "currency_exchange_rate_log_return_n",
    "currency_exchange_rate_ma_7_n",
    "currency_exchange_rate_ma_30_n",
    "currency_exchange_rate_volatility_7_n",
    "currency_exchange_rate_dist_from_ma30_n",
]

RETURN_FEATURE_COLS = [
    "ret_1d_n", "ret_5d_n", "ret_20d_n",
    "gap_n", "intraday_range_n", "close_position",
    "vol_20_n", "drawdown_n",
    "turnover_n", "foreign_flow_n",
    "order_imbalance_n", "spread_proxy_n",
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
    "spread_proxy_n",
]

DD_FEATURES = [
    "ret_1d_n",
    "ret_5d_n",
    "vol_20_n",
    "drawdown_n",
    "turnover_n",
    "foreign_flow_n",
    "order_imbalance_n",
    "spread_proxy_n",
]

CRASH_FEATURES = [
    "vol_20",
    "drawdown",
    "turnover",
    "foreign_flow",
    "order_imbalance",
    "spread_proxy",
]


WINDOW = 60
PREP_SQL = "v2/stock/db/prep.sql"
CLEAN_SQL = "v2/stock/db/clean.sql"
