from keras.layers import Input, LSTM, Dense, Dropout, LayerNormalization, Embedding, Flatten, Concatenate, RepeatVector
from keras.losses import Huber
from keras.metrics import MeanAbsoluteError
from keras.models import Model
from keras.optimizers import Adam
import numpy as np
import pandas as pd
from sqlalchemy import Engine
from keras.callbacks import EarlyStopping, ReduceLROnPlateau
from stock.config import CURRENCY_EXCHANGE_RATE_FEATURE_COLS, INDEX_FEATURE_COLS, STOCK_FEATURE_COLS, WINDOW, config_manager
from stock.dataset import make_drawdown_sequences
import lightgbm as lgb


def load_drawdown_training(db_engine: Engine) -> pd.DataFrame:
    with db_engine.connect() as connection:
        pd_query = f"""
        SELECT
            *
        FROM stock_drawdown_train
        """
        df = pd.read_sql(pd_query, connection)
    return df


def load_drawdown_test(db_engine: Engine) -> pd.DataFrame:
    with db_engine.connect() as connection:
        pd_query = f"""
        SELECT
            *
        FROM stock_drawdown_val
        """
        df = pd.read_sql(pd_query, connection)
    return df


def build_drawdown_model(engine: Engine, rand: int):
    train_df = load_drawdown_training(engine)
    val_df = load_drawdown_test(engine)

    features = STOCK_FEATURE_COLS + INDEX_FEATURE_COLS + \
        CURRENCY_EXCHANGE_RATE_FEATURE_COLS

    stock_profile_mapper = config_manager.stock_profile_mapper
    X_train, X_train_id, y_train, X_train_tree, y_train_tree = make_drawdown_sequences(
        df=train_df, features=features)
    X_val, X_val_id, y_val, X_val_tree, y_val_tree = make_drawdown_sequences(
        df=val_df, features=features)

    ts_input = Input(shape=(WINDOW, X_train.shape[-1]), name="ts_input")

    stock_id_input = Input(shape=(1,), name="stock_id_input")
    emb = Embedding(input_dim=len(stock_profile_mapper),
                    output_dim=16)(stock_id_input)
    emb = Flatten()(emb)
    emb_seq = RepeatVector(WINDOW)(emb)

    merged_input = Concatenate(axis=-1)([ts_input, emb_seq])

    x = LSTM(
        64,
        return_sequences=True,
    )(merged_input)
    x = LayerNormalization()(x)
    x = Dropout(0.1)(x)

    x = LSTM(32)(x)
    x = LayerNormalization()(x)
    x = Dropout(0.1)(x)

    z = Dense(16, activation="relu")(x)
    output = Dense(1, activation="linear")(z)

    model = Model(inputs=[ts_input, stock_id_input], outputs=output)

    for _ in range(5):
        print("TRAIN DRAWDOWN --------------------")
        model.compile(
            optimizer=Adam(learning_rate=1e-5),
            loss=Huber(delta=config_manager.drawdown_delta),
            metrics=[MeanAbsoluteError()],
        )

        history = model.fit(
            x={
                "ts_input": X_train,
                "stock_id_input": X_train_id
            },
            y=y_train,
            validation_data=({
                "ts_input": X_val,
                "stock_id_input": X_val_id
            }, y_val),
            epochs=150,
            batch_size=256,
            callbacks=[
                EarlyStopping(
                    monitor='val_loss',
                    patience=15,
                    restore_best_weights=True
                ),
                ReduceLROnPlateau(patience=10, verbose=1,),
            ],
            verbose=2,
        )
        best_val_loss = min(history.history['val_mean_absolute_error'])
        y_pred = model.predict(
            x={
                "ts_input": X_val,
                "stock_id_input": X_val_id
            },
            verbose=1,
        )
        residuals = y_val - y_pred
        mad = np.median(np.abs(residuals - np.median(residuals)))
        sigma = 1.4826 * mad
        new_delta = 1.345 * sigma
        print(f"DELTA {new_delta} ------------------")
        if abs(config_manager.drawdown_delta-new_delta) < 0.2:
            break
        config_manager.drawdown_delta = new_delta

    tree_model = lgb.LGBMRegressor(
        objective='huber',
        alpha=config_manager.drawdown_delta,
        num_leaves=128,
        min_child_samples=100,
        colsample_bytree=0.8,
        subsample=0.8,
        subsample_freq=5,
        learning_rate=1e-4,
        n_estimators=10000,
        importance_type='gain',
        random_state=rand,
    )

    tree_model.fit(
        X_train_tree, y_train_tree,
        eval_set=[(X_val_tree, y_val_tree)],
        eval_metric=['mae'],
        callbacks=[
            lgb.early_stopping(
                stopping_rounds=100,
                min_delta=1e-5,
            ),
            lgb.log_evaluation(period=100),
        ],
    )
    tree_results = tree_model.evals_result_

    tree_best_iter = tree_model.best_iteration_
    tree_best_val_loss = tree_results['valid_0']['l1'][tree_best_iter - 1]

    feature_importances = pd.DataFrame({
        "features": features,
        "scores": tree_model.feature_importances_,
    })
    print(feature_importances)

    return model, best_val_loss, tree_model, tree_best_val_loss
