from stock.config import CURRENCY_EXCHANGE_RATE_FEATURE_COLS, INDEX_FEATURE_COLS, STOCK_FEATURE_COLS, WINDOW, config_manager
import numpy as np
import pandas as pd


def make_return_sequences(df: pd.DataFrame):
    X, X_id, y = [], [], []
    stock_profile_mapper = config_manager.stock_profile_mapper

    for stock_profile, g in df.groupby("stock_profile"):
        stock_id = stock_profile_mapper[stock_profile]
        g.sort_values("timestamp", inplace=True)
        values = g[STOCK_FEATURE_COLS + INDEX_FEATURE_COLS +
                   CURRENCY_EXCHANGE_RATE_FEATURE_COLS].values
        targets = g["future_return"].values

        for i in range(len(g) - WINDOW):
            X.append(values[i:i+WINDOW])
            X_id.append(stock_id)
            y.append(targets[i+WINDOW])

    return np.array(X), np.array(X_id), np.array(y)


def make_vol_sequences(df: pd.DataFrame):
    X, X_id, y = [], [], []
    stock_profile_mapper = config_manager.stock_profile_mapper

    for stock_profile, g in df.groupby("stock_profile"):
        stock_id = stock_profile_mapper[stock_profile]
        g.sort_values("timestamp", inplace=True)
        values = g[STOCK_FEATURE_COLS + INDEX_FEATURE_COLS +
                   CURRENCY_EXCHANGE_RATE_FEATURE_COLS].values
        targets = g["future_vol"].values

        for i in range(len(g) - WINDOW):
            X.append(values[i:i+WINDOW])
            X_id.append(stock_id)
            y.append(targets[i+WINDOW])

    return np.array(X), np.array(X_id), np.array(y)


def make_drawdown_sequences(df: pd.DataFrame):
    X, X_id, y = [], [], []
    stock_profile_mapper = config_manager.stock_profile_mapper
    for stock_profile, g in df.groupby("stock_profile"):
        stock_id = stock_profile_mapper[stock_profile]
        g.sort_values("timestamp", inplace=True)
        values = g[STOCK_FEATURE_COLS + INDEX_FEATURE_COLS +
                   CURRENCY_EXCHANGE_RATE_FEATURE_COLS].values
        targets = g["future_drawdown"].values

        for i in range(len(g) - WINDOW):
            X.append(values[i:i+WINDOW])
            X_id.append(stock_id)
            y.append(targets[i+WINDOW])

    return np.array(X), np.array(X_id), np.array(y)


def make_regime_sequences(df: pd.DataFrame, features: list[str]):
    X,  y = [], []
    X = df[features].values
    y = df["future_regime"].values
    return np.array(X), np.array(y)


def make_inference_sequences(df: pd.DataFrame):
    X, X_id, X_latest = [], [], []
    features = STOCK_FEATURE_COLS + INDEX_FEATURE_COLS + \
        CURRENCY_EXCHANGE_RATE_FEATURE_COLS
    latest_ts = df['timestamp'].max()
    stock_profile_mapper = config_manager.stock_profile_mapper
    for stock_profile, g in df.groupby("stock_profile"):
        stock_id = stock_profile_mapper[stock_profile]
        g.sort_values("timestamp", inplace=True)
        if len(g) < WINDOW:
            continue

        values = g[features].values

        latest_df = g[g['timestamp'] == latest_ts]
        X_latest.append(latest_df[features].values[0])
        X.append(values[-WINDOW:])
        X_id.append(stock_id)

    return np.array(X), np.array(X_id), np.array(X_latest)
