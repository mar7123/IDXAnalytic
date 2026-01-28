import pandas as pd
import lightgbm as lgb
import numpy as np

from config import (
    RISK_FEATURES,
    RISK_LABEL_COL,

    RISK_OFF_THRESHOLD,
    RISK_ON_THRESHOLD,

    CRASH_RET_5D_THRESHOLD,
    CRASH_VOL_QUANTILE,

    RISK_LGB_PARAMS,
    RISK_NUM_BOOST_ROUND,
    RISK_EARLY_STOPPING,
)


def load_data(db_connection) -> pd.DataFrame:
    query = f"""
        SELECT
            date,
            {", ".join(RISK_FEATURES)},
            {RISK_LABEL_COL}
        FROM index_risk_labeled
        ORDER BY date
    """
    df = pd.read_sql(query, con=db_connection)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df


def train_model(df: pd.DataFrame) -> lgb.Booster:
    dates = sorted(df["date"].unique())
    split_idx = int(len(dates) * 0.8)
    split_date = dates[split_idx]

    train_df = df[df["date"] <= split_date]
    valid_df = df[df["date"] > split_date]

    train_set = lgb.Dataset(
        train_df[RISK_FEATURES],
        label=train_df[RISK_LABEL_COL],
        free_raw_data=False,
    )

    valid_set = lgb.Dataset(
        valid_df[RISK_FEATURES],
        label=valid_df[RISK_LABEL_COL],
        free_raw_data=False,
    )

    model = lgb.train(
        params=RISK_LGB_PARAMS,
        train_set=train_set,
        num_boost_round=RISK_NUM_BOOST_ROUND,
        valid_sets=[valid_set],
        callbacks=[
            lgb.early_stopping(RISK_EARLY_STOPPING),
            lgb.log_evaluation(50),
        ],
    )

    return model


def infer_risk(df: pd.DataFrame, model: lgb.Booster) -> pd.DataFrame:
    df = df.copy()

    df["risk_prob"] = model.predict(
        df[RISK_FEATURES],
        num_iteration=model.best_iteration,
    )

    rolling_vol_q = (
        df["vol_10d"]
        .rolling(window=252, min_periods=60)
        .quantile(CRASH_VOL_QUANTILE)
    )

    df["crash_flag"] = (
        (df["ret_5d"] <= CRASH_RET_5D_THRESHOLD) |
        (df["vol_10d"] >= rolling_vol_q)
    ).fillna(False)

    df["risk_exposure"] = df["risk_prob"]

    df.loc[df["crash_flag"], "risk_exposure"] = 0.0

    df["risk_exposure"] = df["risk_exposure"].clip(0.0, 1.0)

    df["risk_state"] = "NEUTRAL"
    df.loc[df["risk_exposure"] <= RISK_OFF_THRESHOLD, "risk_state"] = "RISK_OFF"
    df.loc[df["risk_exposure"] >= RISK_ON_THRESHOLD, "risk_state"] = "RISK_ON"

    return df[[
        "date",
        "risk_state",
        "risk_exposure",
        "risk_prob",
        "crash_flag",
    ]]


def run_market_risk(db_connection) -> pd.DataFrame:
    df = load_data(db_connection)

    assert not df.empty, "Market risk dataset is empty"
    assert df[RISK_LABEL_COL].isin([0, 1]).all(), "Invalid risk labels"

    model = train_model(df)
    result = infer_risk(df, model)

    assert result["risk_exposure"].between(0, 1).all()
    assert result["risk_state"].isin(
        ["RISK_OFF", "NEUTRAL", "RISK_ON"]
    ).all()

    return result
