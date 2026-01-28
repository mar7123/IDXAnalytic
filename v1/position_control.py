import pandas as pd
import numpy as np

from config import (
    MAX_POSITIONS,
    MAX_WEIGHT_PER_STOCK,
    EXPOSURE_BY_RISK,
)


def build_portfolio(
    alpha_df: pd.DataFrame,
    risk_state: str,
    risk_score: float,
) -> pd.DataFrame:

    if alpha_df.empty:
        return pd.DataFrame(
            columns=["stock_profile", "alpha_score", "target_weight"]
        )

    if risk_state not in EXPOSURE_BY_RISK:
        raise ValueError(f"Unknown risk_state: {risk_state}")

    if risk_state == "RISK_OFF" or risk_score <= 0.0:
        return pd.DataFrame(
            columns=["stock_profile", "alpha_score", "target_weight"]
        )

    df = (
        alpha_df
        .sort_values("alpha_score", ascending=False)
        .head(MAX_POSITIONS)
        .copy()
    )

    base_exposure = EXPOSURE_BY_RISK[risk_state]
    total_exposure = base_exposure * float(risk_score)
    total_exposure = float(np.clip(total_exposure, 0.0, base_exposure))

    if total_exposure <= 0:
        return pd.DataFrame(
            columns=["stock_profile", "alpha_score", "target_weight"]
        )

    df["rank"] = np.arange(len(df), 0, -1)
    signal = df["rank"].astype(float)

    signal_sum = signal.sum()
    raw_weights = signal / signal_sum

    df["target_weight"] = raw_weights * total_exposure

    df["capped"] = False

    while True:
        over_cap = df["target_weight"] > MAX_WEIGHT_PER_STOCK
        if not over_cap.any():
            break

        df.loc[over_cap, "target_weight"] = MAX_WEIGHT_PER_STOCK
        df.loc[over_cap, "capped"] = True

        used = df["target_weight"].sum()
        leftover = total_exposure - used

        if leftover <= 1e-8:
            break

        free = df[~df["capped"]]
        if free.empty:
            break

        free_signal = free["rank"].sum()
        df.loc[~df["capped"], "target_weight"] += (
            free["rank"] / free_signal * leftover
        )

    df["target_weight"] = df["target_weight"].clip(lower=0.0)

    assert df["target_weight"].sum() <= total_exposure + 1e-6
    assert df["target_weight"].max() <= MAX_WEIGHT_PER_STOCK + 1e-6

    return (
        df[["stock_profile", "alpha_score", "target_weight"]]
        .sort_values("target_weight", ascending=False)
        .reset_index(drop=True)
    )
