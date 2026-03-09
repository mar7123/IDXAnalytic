from keras.layers import Input, LSTM, Dense, Dropout, LayerNormalization, Embedding, Flatten, Concatenate, RepeatVector
from keras.metrics import MeanAbsoluteError
from keras.losses import MeanSquaredError
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


def build_drawdown_model(engine: Engine, stock_profile_mapper: dict[str, int]):
    train_df = load_drawdown_training(engine)
    val_df = load_drawdown_test(engine)

    X_train, X_train_id, y_train = make_drawdown_sequences(
        train_df, stock_profile_mapper)
    X_val, X_val_id, y_val = make_drawdown_sequences(
        val_df, stock_profile_mapper)

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
    output = Dense(1, activation="sigmoid")(z)

    model = Model(inputs=[ts_input, stock_id_input], outputs=output)

    print("TRAIN DRAWDOWN --------------------")
    model.compile(
        optimizer=Adam(learning_rate=1e-5),
        loss=MeanSquaredError(),
        metrics=[MeanAbsoluteError()],
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

    return model
