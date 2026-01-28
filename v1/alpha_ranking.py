import pandas as pd
import lightgbm as lgb

from config import (
    ALPHA_FEATURES,
    ALPHA_LABEL_COL,
    ALPHA_GROUP_COL,
    ALPHA_LGB_PARAMS,
    ALPHA_NUM_BOOST_ROUND,
    ALPHA_EARLY_STOPPING,
)


def load_data(db_connection) -> pd.DataFrame:
    query = f"""
        SELECT
            date,
            stock_profile,
            {ALPHA_LABEL_COL},
            {", ".join(ALPHA_FEATURES)}
        FROM alpha_rank_dataset
        ORDER BY date, stock_profile
    """
    df = pd.read_sql(query, con=db_connection)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df


def build_groups(df: pd.DataFrame) -> list[int]:
    """
    Build LambdaRank group sizes.
    REQUIRES df sorted by date.
    """
    assert df[ALPHA_GROUP_COL].is_monotonic_increasing
    return df.groupby(ALPHA_GROUP_COL, sort=False).size().tolist()


def train_ranker(df: pd.DataFrame) -> lgb.Booster:
    df = df.copy()

    assert df[ALPHA_LABEL_COL].between(0, 4).all(), "Label must be 0..4"
    assert df.groupby("date").size().min(
    ) >= 2, "Each date must have ≥2 stocks"

    dates = sorted(df["date"].unique())
    split_idx = int(len(dates) * 0.8)
    split_date = dates[split_idx]

    train_df = df[df["date"] <= split_date].copy()
    valid_df = df[df["date"] > split_date].copy()

    for d in (train_df, valid_df):
        d.sort_values(["date", "stock_profile"], inplace=True)
        assert d["date"].is_monotonic_increasing

    train_set = lgb.Dataset(
        train_df[ALPHA_FEATURES],
        label=train_df[ALPHA_LABEL_COL],
        group=build_groups(train_df),
        free_raw_data=False,
    )

    valid_set = lgb.Dataset(
        valid_df[ALPHA_FEATURES],
        label=valid_df[ALPHA_LABEL_COL],
        group=build_groups(valid_df),
        free_raw_data=False,
    )

    model = lgb.train(
        params=ALPHA_LGB_PARAMS,
        train_set=train_set,
        num_boost_round=ALPHA_NUM_BOOST_ROUND,
        valid_sets=[valid_set],
        callbacks=[
            lgb.early_stopping(ALPHA_EARLY_STOPPING),
            lgb.log_evaluation(50),
        ],
    )

    return model


def infer_all_days(df: pd.DataFrame, model: lgb.Booster) -> pd.DataFrame:
    df = df.copy()
    df.sort_values(["date", "stock_profile"], inplace=True)

    scores = model.predict(
        df[ALPHA_FEATURES],
        num_iteration=model.best_iteration,
    )

    out = df[["date", "stock_profile"]].copy()
    out["alpha_score"] = scores

    return out


def run_alpha_ranking(db_connection) -> pd.DataFrame:
    df = load_data(db_connection)

    assert not df.empty, "Alpha ranking dataset is empty"
    assert ALPHA_LABEL_COL in df.columns, "Missing label column"

    model = train_ranker(df)
    result = infer_all_days(df, model)

    assert result["alpha_score"].notna().all()
    assert result.groupby("date").size().min() >= 2

    return result
