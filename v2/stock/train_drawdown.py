from keras.models import Sequential
from keras.layers import Input, LSTM, Dense, Dropout, LayerNormalization
from keras.optimizers import Adam
import numpy as np
import pandas as pd
from sqlalchemy import Engine
from keras.callbacks import EarlyStopping, ReduceLROnPlateau
import tensorflow as tf
from stock.config import WINDOW
from stock.dataset import make_drawdown_sequences


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


def quantile_loss(q):
    def loss(y_true, y_pred):
        e = y_true - y_pred
        return tf.reduce_mean(tf.maximum(q * e, (q - 1) * e))
    return loss


def build_drawdown_model(engine: Engine, q: float = 0.95):
    X_train, y_train = make_drawdown_sequences(load_drawdown_training(engine))
    X_val, y_val = make_drawdown_sequences(load_drawdown_test(engine))

    model = Sequential([
        Input(shape=(WINDOW, X_train.shape[-1])),
        LSTM(
            64,
            return_sequences=True,
        ),
        LayerNormalization(),
        Dropout(0.1),

        LSTM(32),
        LayerNormalization(),
        Dropout(0.1),

        Dense(16, activation="relu"),
        Dropout(0.1),

        Dense(1, activation="linear")
    ])

    model.compile(
        optimizer=Adam(learning_rate=1e-3),
        loss=quantile_loss(q)
    )

    model.fit(
        X_train, y_train,
        validation_data=(X_val, y_val),
        epochs=150,
        batch_size=256,
        shuffle=True,
        callbacks=[
            EarlyStopping(
                monitor='val_loss',
                patience=10,
                restore_best_weights=True
            ),
            ReduceLROnPlateau(patience=5, verbose=1,),
        ],
    )

    pred = model.predict(X_val)
    coverage_check = y_val <= pred.flatten()

    coverage_ratio = np.mean(coverage_check)

    print(f"Empirical Coverage: {coverage_ratio:.4f}")
    print(f"Target Coverage: 0.95")
    return model
