from sqlalchemy import Engine
from xgboost import XGBClassifier
from xgboost.callback import EarlyStopping
import pandas as pd

from stock.config import CURRENCY_EXCHANGE_RATE_FEATURE_COLS, INDEX_FEATURE_COLS, STOCK_FEATURE_COLS


def load_crash_training(db_engine: Engine) -> pd.DataFrame:
    with db_engine.connect() as connection:
        pd_query = f"""
        SELECT
            *
        FROM stock_crash_train
        """
        df = pd.read_sql(pd_query, connection)
    return df


def load_crash_test(db_engine: Engine) -> pd.DataFrame:
    with db_engine.connect() as connection:
        pd_query = f"""
        SELECT
            *
        FROM stock_crash_val
        """
        df = pd.read_sql(pd_query, connection)
    return df


def build_crash_model(engine: Engine):
    train_df = load_crash_training(engine)
    train_df.dropna(inplace=True)
    val_df = load_crash_test(engine)
    val_df.dropna(inplace=True)
    X_train = train_df[STOCK_FEATURE_COLS + INDEX_FEATURE_COLS +
                       CURRENCY_EXCHANGE_RATE_FEATURE_COLS].values
    y_train = train_df["crash"].values

    X_val = val_df[STOCK_FEATURE_COLS + INDEX_FEATURE_COLS +
                   CURRENCY_EXCHANGE_RATE_FEATURE_COLS].values
    y_val = val_df["crash"].values

    scale_pos_weight = (y_train == 0).sum() / max((y_train == 1).sum(), 1)

    print("TRAIN CRASH --------------------")
    model = XGBClassifier(
        n_estimators=5000,
        max_depth=4,
        learning_rate=0.03,
        subsample=0.8,
        colsample_bytree=0.8,
        min_child_weight=10,
        gamma=0.1,
        reg_alpha=0.0,
        reg_lambda=1.0,
        objective="binary:logistic",
        eval_metric="logloss",
        scale_pos_weight=scale_pos_weight,
        callbacks=[
            EarlyStopping(
                rounds=100,
                min_delta=1e-3,
                save_best=True,
                maximize=False,
                data_name="validation_0",
                metric_name="logloss",
            )
        ],
        random_state=42
    )

    model.fit(
        X_train, y_train,
        eval_set=[(X_val, y_val)],
        verbose=True
    )

    return model
