import numpy as np
import pandas as pd
from risk_engine.config import WINDOW


def make_sequences(df: pd.DataFrame, feature_cols: list[str], target_col: str):
    X, y = [], []

    values = df[feature_cols].values
    targets = df[target_col].values

    for i in range(len(df) - WINDOW):
        X.append(values[i:i+WINDOW])
        y.append(targets[i+WINDOW])

    return np.array(X), np.array(y)
