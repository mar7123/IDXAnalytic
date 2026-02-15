from keras.models import Sequential
from keras.layers import Input, LSTM, Dense, Dropout, LayerNormalization, Embedding, Flatten, Concatenate, RepeatVector
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
    train_df = load_drawdown_training(engine)
    val_df = load_drawdown_test(engine)
    stock_profile_set: set[str] = set()
    for stock_profile in train_df["stock_profile"].values:
        stock_profile_set.add(stock_profile)
    for stock_profile in val_df["stock_profile"].values:
        stock_profile_set.add(stock_profile)
    stock_profile_mapper = {label: i for i,
                            label in enumerate(sorted(stock_profile_set))}

    X_train, X_train_id, y_train = make_drawdown_sequences(
        train_df, stock_profile_mapper)
    X_val, X_val_id, y_val = make_drawdown_sequences(
        val_df, stock_profile_mapper)

    ts_input = Input(shape=(WINDOW, X_train.shape[-1]), name="ts_input")

    stock_id_input = Input(shape=(1,), name="stock_id_input")
    emb = Embedding(input_dim=len(stock_profile_mapper), output_dim=16)(stock_id_input)
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

    model.compile(
        optimizer=Adam(learning_rate=1e-4),
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
