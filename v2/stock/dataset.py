from stock.config import FEATURE_COLS, WINDOW
import numpy as np
import pandas as pd

from config import OUTPUT_PATH


def make_sequences(df: pd.DataFrame):
    X, y = [], []
    print(df.head())
    for _, g in df.groupby("stock_profile"):
        g = g.sort_values("timestamp")
        values = g[FEATURE_COLS].values
        targets = g["future_return_5d"].values

        for i in range(len(g) - WINDOW):
            X.append(values[i:i+WINDOW])
            y.append(targets[i+WINDOW])

    return np.array(X), np.array(y)
