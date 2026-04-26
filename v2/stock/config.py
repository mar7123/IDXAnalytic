from sqlalchemy import Engine


INDEX_FEATURE_COLS = [
    "idx_close_pos",
    "idx_range_n",
    "idx_ret_1d_n",
    "idx_ret_5d_n",
    "idx_ret_20d_n",
    "idx_ret_60d_n",
    "idx_vol_20d_n",
    "idx_vol_60d_n",
    "idx_drawdown_20d_n",
    "idx_drawdown_60d_n",
]

CURRENCY_EXCHANGE_RATE_FEATURE_COLS = [
    "currency_exchange_rate_ret_1d_n",
    "currency_exchange_rate_ret_5d_n",
    "currency_exchange_rate_ret_20d_n",
    "currency_exchange_rate_ret_60d_n",
    "currency_exchange_rate_vol_20d_n",
    "currency_exchange_rate_vol_60d_n",
    "currency_exchange_rate_mr_20d_n",
    "currency_exchange_rate_mr_60d_n",
]

STOCK_FEATURE_COLS = [
    "is_active",
    "is_active_5d",
    "is_active_20d",
    "is_active_60d",
    "turnover_n",
    "foreign_flow_n",
    "order_imbalance_n",
    "relative_spread_n",
    "non_regular_activity_n",
    "spread_proxy_n",
    "ret_1d_n",
    "ret_5d_n",
    "ret_20d_n",
    "ret_60d_n",
    "gap_n",
    "intraday_range_n",
    "close_position",
    "drawdown_20d_n",
    "drawdown_60d_n",
    "vol_20d_n",
    "vol_60d_n",
    "dow_sin",
    "dow_cos",
    "woy_sin",
    "woy_cos",
    "month_sin",
    "month_cos",
    "excess_ret_1d_n",
    "excess_ret_5d_n",
    "excess_ret_20d_n",
    "excess_ret_60d_n",
]


WINDOW = 60
PREP_SQL = "v2/stock/db/prep.sql"
CLEAN_SQL = "v2/stock/db/clean.sql"
OUTPUT_PATH = "output.xlsx"


class ConfigManager:
    def __init__(self):
        self.stock_profile_mapper = dict[int, str]()
        self.return_delta = 1.0
        self.vol_delta = 1.0
        self.drawdown_delta = 1.0


config_manager = ConfigManager()
