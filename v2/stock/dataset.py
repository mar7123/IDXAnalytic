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
        zero_volume_counts = g["zero_volume_count"].values
        targets = g["future_return_5d"].values
        future_vols = g["future_volume_5d"].values

        for i in range(len(g) - WINDOW):
            zero_volume_count = zero_volume_counts[i:i+WINDOW]
            future_vol = future_vols[i+WINDOW]
            if future_vol == 0 or any(x > 0 for x in zero_volume_count):
                continue
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
        zero_volume_counts = g["zero_volume_count"].values
        targets = g["future_vol_20d"].values
        future_vols = g["min_future_volume_20d"].values

        for i in range(len(g) - WINDOW):
            zero_volume_count = zero_volume_counts[i:i+WINDOW]
            future_vol = future_vols[i+WINDOW]
            if future_vol == 0 or any(x > 0 for x in zero_volume_count):
                continue
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
        zero_volume_counts = g["zero_volume_count"].values
        future_vols = g["min_future_volume_20d"].values
        targets = g["future_drawdown_20d"].values

        for i in range(len(g) - WINDOW):
            zero_volume_count = zero_volume_counts[i:i+WINDOW]
            future_vol = future_vols[i+WINDOW]
            if future_vol == 0 or any(x > 0 for x in zero_volume_count):
                continue
            X.append(values[i:i+WINDOW])
            X_id.append(stock_id)
            y.append(targets[i+WINDOW])

    return np.array(X), np.array(X_id), np.array(y)


def make_inference_sequences(df: pd.DataFrame):
    X, X_id = [], []
    stock_profile_mapper = config_manager.stock_profile_mapper
    for stock_profile, g in df.groupby("stock_profile"):
        stock_id = stock_profile_mapper[stock_profile]
        g.sort_values("timestamp", inplace=True)
        if len(g) < WINDOW:
            continue

        values = g[STOCK_FEATURE_COLS + INDEX_FEATURE_COLS +
                   CURRENCY_EXCHANGE_RATE_FEATURE_COLS].values

        zero_volume_counts = g["zero_volume_count"].values[-WINDOW:]

        if any(x > 0 for x in zero_volume_counts):
            continue

        X.append(values[-WINDOW:])
        X_id.append(stock_id)

    return np.array(X), np.array(X_id)
