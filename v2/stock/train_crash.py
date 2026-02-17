from sqlalchemy import Engine
from xgboost import XGBClassifier
import pandas as pd
from sklearn.calibration import CalibratedClassifierCV

from stock.config import CRASH_FEATURES


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
    val_df = load_crash_test(engine)
    X_train = train_df[CRASH_FEATURES].values
    y_train = train_df["crash"].values

    X_val = val_df[CRASH_FEATURES].values
    y_val = val_df["crash"].values

    scale_pos_weight = (y_train == 0).sum() / max((y_train == 1).sum(), 1)

    model = XGBClassifier(
        n_estimators=500,
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
        random_state=42
    )

    model.fit(
        X_train, y_train,
        eval_set=[(X_val, y_val)],
        verbose=True
    )
    calibrated_model = CalibratedClassifierCV(
        estimator=model,
        method="isotonic",
        ensemble=False
    )

    calibrated_model.fit(X_val, y_val)

    return model
