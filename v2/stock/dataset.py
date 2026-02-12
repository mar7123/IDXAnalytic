from sklearn.calibration import LabelEncoder
from stock.config import DD_FEATURES, RETURN_FEATURE_COLS, VOL_FEATURES, WINDOW
import numpy as np
import pandas as pd


def make_return_sequences(df: pd.DataFrame):
    X, X_id, y = [], [], []

    X_id_count = 0
    df['stock_id'] = LabelEncoder().fit_transform(df['stock_profile'])

    for stock_id, g in df.groupby("stock_id"):
        X_id_count += 1
        g = g.sort_values("timestamp")
        values = g[RETURN_FEATURE_COLS].values
        volumes = g["volume"].values
        targets = g["future_return_5d"].values
        future_vols = g["future_vol_5d"].values

        for i in range(len(g) - WINDOW):
            current_volume = volumes[i:i+WINDOW]
            future_vol = future_vols[i+WINDOW]
            if future_vol == 0 or 0 in current_volume:
                continue
            X.append(values[i:i+WINDOW])
            X_id.append(stock_id)
            y.append(targets[i+WINDOW])

    return np.array(X), np.array(X_id), X_id_count, np.array(y)


def make_vol_sequences(df: pd.DataFrame):
    X, y = [], []

    for _, g in df.groupby("stock_profile"):
        g = g.sort_values("timestamp")
        values = g[VOL_FEATURES].values
        targets = g["future_vol_20d"].values

        for i in range(len(g) - WINDOW):
            X.append(values[i:i+WINDOW])
            y.append(targets[i+WINDOW])

    return np.array(X), np.array(y)


def make_drawdown_sequences(df: pd.DataFrame):
    X, X_id, y = [], [], []
    X_id_count = 0
    df['stock_id'] = LabelEncoder().fit_transform(df['stock_profile'])

    for stock_id, g in df.groupby("stock_id"):
        X_id_count += 1
        g = g.sort_values("timestamp")
        values = g[DD_FEATURES].values
        targets = g["future_drawdown_20d"].values

        for i in range(len(g) - WINDOW):
            X.append(values[i:i+WINDOW])
            X_id.append(stock_id)
            y.append(targets[i+WINDOW])

    return np.array(X), np.array(X_id), X_id_count, np.array(y)
