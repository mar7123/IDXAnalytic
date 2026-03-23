from sqlalchemy import Engine
from xgboost import XGBClassifier
from xgboost.callback import EarlyStopping
import pandas as pd
from stock.dataset import make_crash_sequences
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


def build_crash_model(engine: Engine, rand: int):
    train_df = load_crash_training(engine)
    train_df.dropna(inplace=True)
    val_df = load_crash_test(engine)
    val_df.dropna(inplace=True)
    features = STOCK_FEATURE_COLS + INDEX_FEATURE_COLS + \
        CURRENCY_EXCHANGE_RATE_FEATURE_COLS
    X_train, y_train = make_crash_sequences(df=train_df, features=features)
    X_val, y_val = make_crash_sequences(df=val_df, features=features)

    scale_pos_weight = (y_train == 0).sum() / max((y_train == 1).sum(), 1)

    print("TRAIN CRASH --------------------")
    model = XGBClassifier(
        n_estimators=5000,
        max_depth=7,
        learning_rate=0.03,
        subsample=0.8,
        colsample_bytree=0.8,
        min_child_weight=4,
        gamma=0.1,
        reg_alpha=0.1,
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
        random_state=rand
    )

    model.fit(
        X_train, y_train,
        eval_set=[(X_val, y_val)],
        verbose=True
    )
    feature_importances = pd.DataFrame({
        "features": features,
        "scores": model.feature_importances_,
    })
    print(feature_importances)
    return model
