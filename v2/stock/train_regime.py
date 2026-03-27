from sqlalchemy import Engine
import lightgbm as lgb
import pandas as pd
from stock.dataset import make_regime_sequences
from stock.config import CURRENCY_EXCHANGE_RATE_FEATURE_COLS, INDEX_FEATURE_COLS, STOCK_FEATURE_COLS


def load_regime_training(db_engine: Engine) -> pd.DataFrame:
    with db_engine.connect() as connection:
        pd_query = f"""
        SELECT
            *
        FROM stock_regime_train
        """
        df = pd.read_sql(pd_query, connection)
    return df


def load_regime_test(db_engine: Engine) -> pd.DataFrame:
    with db_engine.connect() as connection:
        pd_query = f"""
        SELECT
            *
        FROM stock_regime_val
        """
        df = pd.read_sql(pd_query, connection)
    return df


def build_regime_model(engine: Engine, rand: int):
    train_df = load_regime_training(engine)
    train_df.dropna(inplace=True)
    val_df = load_regime_test(engine)
    val_df.dropna(inplace=True)
    features = STOCK_FEATURE_COLS + INDEX_FEATURE_COLS + \
        CURRENCY_EXCHANGE_RATE_FEATURE_COLS
    X_train, y_train = make_regime_sequences(df=train_df, features=features)
    X_val, y_val = make_regime_sequences(df=val_df, features=features)

    print("TRAIN regime --------------------")

    model = lgb.LGBMClassifier(
        objective='multiclass',
        num_class=3,
        num_leaves=127,
        min_child_samples=10,
        colsample_bytree=0.8,
        subsample=0.8,
        subsample_freq=5,
        metric='multi_logloss',
        learning_rate=0.03,
        n_estimators=10000,
        importance_type='gain',
        class_weight='balanced',
        random_state=rand,
    )

    model.fit(
        X_train, y_train,
        eval_set=[(X_val, y_val)],
        callbacks=[
            lgb.early_stopping(
                stopping_rounds=100,
                min_delta=1e-3,
            ),
            lgb.log_evaluation(period=100),
        ],
    )
    feature_importances = pd.DataFrame({
        "features": features,
        "scores": model.feature_importances_,
    })
    print(feature_importances)
    return model
