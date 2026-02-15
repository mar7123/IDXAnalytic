from keras.models import Sequential
from keras.layers import LSTM, Dense, Dropout, LayerNormalization
from keras.optimizers import Adam
from keras.losses import Huber
from keras.metrics import MeanAbsoluteError
import pandas as pd
from sqlalchemy import Engine
from keras.callbacks import EarlyStopping, ReduceLROnPlateau
from stock.config import WINDOW
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
    X_train, y_train = make_vol_sequences(load_vol_training(engine))
    X_val, y_val = make_vol_sequences(load_vol_test(engine))

    model = Sequential([
        LSTM(
            64,
            return_sequences=True,
        ),
        LayerNormalization(),
        Dropout(0.3),

        LSTM(32),
        LayerNormalization(),
        Dropout(0.2),

        Dense(16, activation="relu"),
        Dropout(0.2),

        Dense(1, activation="linear")
    ])

    model.compile(
        optimizer=Adam(learning_rate=1e-3),
        loss=Huber(delta=1),
        metrics=[MeanAbsoluteError()]
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
        verbose=2,
    )
    return model
