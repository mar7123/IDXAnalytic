from keras.models import Model
from keras.layers import LSTM, Dense, Dropout, LayerNormalization, Input, Embedding, Flatten, Concatenate
from keras.optimizers import Adam
from keras.losses import Huber
from keras.metrics import MeanAbsoluteError
import numpy as np
import pandas as pd
from sqlalchemy import Engine
from keras.callbacks import EarlyStopping, ReduceLROnPlateau
from stock.config import WINDOW, config_manager
from stock.dataset import make_vol_sequences


def load_vol_training(db_engine: Engine) -> pd.DataFrame:
    with db_engine.connect() as connection:
        pd_query = f"""
        SELECT
            *
        FROM stock_vol_train
        """
        df = pd.read_sql(pd_query, connection)
    return df


def load_vol_test(db_engine: Engine) -> pd.DataFrame:
    with db_engine.connect() as connection:
        pd_query = f"""
        SELECT
            *
        FROM stock_vol_val
        """
        df = pd.read_sql(pd_query, connection)
    return df


def build_vol_model(engine: Engine):
    train_df = load_vol_training(engine)
    val_df = load_vol_test(engine)

    X_train, X_train_id, y_train = make_vol_sequences(train_df)
    X_val, X_val_id, y_val = make_vol_sequences(val_df)

    ts_input = Input(shape=(WINDOW, X_train.shape[-1]), name="ts_input")
    stock_id_input = Input(shape=(1,), name="stock_id_input")

    emb = Embedding(input_dim=len(config_manager.stock_profile_mapper),
                    output_dim=18)(stock_id_input)
    emb_flat = Flatten()(emb)

    x = LSTM(
        64,
        return_sequences=True,
    )(ts_input)
    x = LayerNormalization()(x)
    x = Dropout(0.1)(x)

    x = LSTM(
        32,
    )(ts_input)
    x = LayerNormalization()(x)
    x = Dropout(0.1)(x)

    merged = Concatenate()([x, emb_flat])

    x = Dense(16, activation="relu")(merged)
    x = Dropout(0.1)(x)

    output = Dense(1, activation="linear")(x)

    model = Model(inputs=[ts_input, stock_id_input], outputs=output)

    for _ in range(5):
        model.compile(
            optimizer=Adam(learning_rate=1e-5),
            loss=Huber(delta=config_manager.vol_delta),
            metrics=[MeanAbsoluteError()]
        )
        print("TRAIN VOL --------------------")
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
        if abs(config_manager.vol_delta-new_delta) < 0.2:
            break
        config_manager.vol_delta = new_delta
    return model
