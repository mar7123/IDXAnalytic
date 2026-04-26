from stock.config import CURRENCY_EXCHANGE_RATE_FEATURE_COLS, INDEX_FEATURE_COLS, STOCK_FEATURE_COLS, WINDOW, config_manager
import numpy as np
import pandas as pd


def make_return_sequences(df: pd.DataFrame, features: list[str]):
    X, X_id, y, X_tree, y_tree = [], [], [], [], []
    X_tree = df[features].values
    y_tree = df["future_return"].values
    stock_profile_mapper = config_manager.stock_profile_mapper

    for stock_profile, g in df.groupby("stock_profile"):
        stock_id = stock_profile_mapper[stock_profile]
        g.sort_values("timestamp", inplace=True)
        values = g[features].values
        targets = g["future_return"].values

        for i in range(len(g) - WINDOW):
            X.append(values[i:i+WINDOW])
            X_id.append(stock_id)
            y.append(targets[i+WINDOW])

    return np.array(X), np.array(X_id), np.array(y), np.array(X_tree), np.array(y_tree)


def make_vol_sequences(df: pd.DataFrame, features: list[str]):
    X, X_id, y, X_tree, y_tree = [], [], [], [], []
    X_tree = df[features].values
    y_tree = df["future_vol"].values
    stock_profile_mapper = config_manager.stock_profile_mapper

    for stock_profile, g in df.groupby("stock_profile"):
        stock_id = stock_profile_mapper[stock_profile]
        g.sort_values("timestamp", inplace=True)
        values = g[features].values
        targets = g["future_vol"].values

        for i in range(len(g) - WINDOW):
            X.append(values[i:i+WINDOW])
            X_id.append(stock_id)
            y.append(targets[i+WINDOW])

    return np.array(X), np.array(X_id), np.array(y), np.array(X_tree), np.array(y_tree)


def make_drawdown_sequences(df: pd.DataFrame, features: list[str]):
    X, X_id, y, X_tree, y_tree = [], [], [], [], []
    X_tree = df[features].values
    y_tree = df["future_drawdown"].values
    stock_profile_mapper = config_manager.stock_profile_mapper
    for stock_profile, g in df.groupby("stock_profile"):
        stock_id = stock_profile_mapper[stock_profile]
        g.sort_values("timestamp", inplace=True)
        values = g[features].values
        targets = g["future_drawdown"].values

        for i in range(len(g) - WINDOW):
            X.append(values[i:i+WINDOW])
            X_id.append(stock_id)
            y.append(targets[i+WINDOW])

    return np.array(X), np.array(X_id), np.array(y), np.array(X_tree), np.array(y_tree)


def make_inference_sequences(df: pd.DataFrame):
    X, X_id, X_tree = [], [], []
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
        X_tree.append(latest_df[features].values[0])
        X.append(values[-WINDOW:])
        X_id.append(stock_id)

    return np.array(X), np.array(X_id), np.array(X_tree)
