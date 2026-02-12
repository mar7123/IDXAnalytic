from keras.models import Sequential
from keras.layers import Input, LSTM, Dense, Dropout, LayerNormalization, Embedding, Flatten, Concatenate
from keras.models import Model
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
    X_train, X_train_id, X_train_id_count, y_train = make_drawdown_sequences(
        load_drawdown_training(engine))
    X_val, X_val_id, X_val_id_count, y_val = make_drawdown_sequences(
        load_drawdown_test(engine))

    ts_input = Input(shape=(WINDOW, X_train.shape[-1]), name="ts_input")
    x = LSTM(
        64,
        return_sequences=True,
    )(ts_input)
    x = LayerNormalization()(x)
    x = Dropout(0.1)(x)

    x = LSTM(32)(x)
    x = LayerNormalization()(x)
    x = Dropout(0.1)(x)

    stock_id_input = Input(shape=(1,), name="stock_id_input")
    emb = Embedding(input_dim=X_train_id_count, output_dim=16)(stock_id_input)
    emb = Flatten()(emb)

    merged = Concatenate()([x, emb])

    z = Dense(16, activation="relu")(merged)
    output = Dense(1, activation="linear")(z)

    model = Model(inputs=[ts_input, stock_id_input], outputs=output)

    model.compile(
        optimizer=Adam(learning_rate=1e-3),
        loss=quantile_loss(q)
    )

    model.fit(
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

    pred = model.predict({
        "ts_input": X_val,
        "stock_id_input": X_val_id
    })
    coverage_check = y_val <= pred.flatten()

    coverage_ratio = np.mean(coverage_check)

    print(f"Empirical Coverage: {coverage_ratio:.4f}")
    print(f"Target Coverage: 0.95")
    return model
