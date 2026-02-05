from stock.config import DD_FEATURES, RETURN_FEATURE_COLS, VOL_FEATURES, WINDOW
import numpy as np
import pandas as pd


def make_return_sequences(df: pd.DataFrame):
    X, y = [], []

    for _, g in df.groupby("stock_profile"):
        g = g.sort_values("timestamp")
        values = g[RETURN_FEATURE_COLS].values
        targets = g["future_return_5d"].values

        for i in range(len(g) - WINDOW):
            X.append(values[i:i+WINDOW])
            y.append(targets[i+WINDOW])

    return np.array(X), np.array(y)


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
    X, y = [], []

    for _, g in df.groupby("stock_profile"):
        g = g.sort_values("timestamp")
        values = g[DD_FEATURES].values
        targets = g["future_drawdown_20d"].values

        for i in range(len(g) - WINDOW):
            X.append(values[i:i+WINDOW])
            y.append(targets[i+WINDOW])

    return np.array(X), np.array(y)
