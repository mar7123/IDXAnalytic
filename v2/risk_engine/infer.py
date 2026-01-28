from risk_engine.config import ALL_FEATURES, WINDOW
from typing import Dict
import numpy as np
import pandas as pd
from xgboost import XGBClassifier

from keras.models import Sequential

from risk_engine.config import ALL_FEATURES


def infer_risk(regime_model: Sequential, vol_model: Sequential, crash_model: XGBClassifier, latest_df: pd.DataFrame):
    seq = np.expand_dims(
        latest_df[ALL_FEATURES].values[-WINDOW:],
        axis=0
    )

    flat_features = latest_df[ALL_FEATURES].values[-1].reshape(1, -1)

    regime_probs = regime_model.predict(seq)[0]

    bull_p, neutral_p, bear_p = regime_probs

    total = bull_p + neutral_p + bear_p
    bull_p, neutral_p, bear_p = bull_p / total, neutral_p / total, bear_p / total

    raw_vol_pred = float(vol_model.predict(seq)[0][0])

    realized_vol = latest_df["vol_60"].iloc[-1]

    vol_ratio = raw_vol_pred / (realized_vol + 1e-8)
    vol_ratio = float(np.clip(vol_ratio, 0.5, 3.0))

    crash_prob = float(crash_model.predict_proba(flat_features)[0][1])
    crash_prob = float(np.clip(crash_prob, 0.0, 1.0))

    exposure = 1.0

    exposure *= (1.0 - 0.7 * crash_prob)

    exposure *= (1.0 - 0.4 * bear_p)

    exposure *= np.clip(1.0 / vol_ratio, 0.3, 1.2)

    exposure = float(np.clip(exposure, 0.1, 1.0))

    return {
        "regime_probs": {
            "bull": bull_p,
            "neutral": neutral_p,
            "bear": bear_p,
        },
        "volatility_ratio": vol_ratio,
        "crash_probability": crash_prob,
        "recommended_exposure": exposure,
    }
