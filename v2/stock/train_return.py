from keras.models import Model
from keras.layers import Input, Dense, Dropout, LayerNormalization
from keras.optimizers import Adam
from keras.losses import Huber
import pandas as pd
from sqlalchemy import Engine
from tcn import TCN
from keras.callbacks import EarlyStopping, ReduceLROnPlateau
from stock.config import WINDOW
from stock.dataset import make_return_sequences


def load_return_training(db_engine: Engine) -> pd.DataFrame:
    with db_engine.connect() as connection:
        pd_query = f"""
        SELECT
            *
        FROM stock_return_train
        """
        df = pd.read_sql(pd_query, connection)
    return df


def load_return_test(db_engine: Engine) -> pd.DataFrame:
    with db_engine.connect() as connection:
        pd_query = f"""
        SELECT
            *
        FROM stock_return_val
        """
        df = pd.read_sql(pd_query, connection)
    return df


def build_return_model(engine: Engine):
    X_train, y_train = make_return_sequences(load_return_training(engine))
    X_val, y_val = make_return_sequences(load_return_test(engine))

    inputs = Input(shape=(WINDOW, X_train.shape[-1]))

    x = TCN(
        nb_filters=32,
        kernel_size=3,
        dilations=[1, 2, 4, 8, 16],
        padding="causal",
        dropout_rate=0.05,
        return_sequences=False
    )(inputs)

    x = LayerNormalization()(x)
    x = Dense(32, activation="relu")(x)
    x = Dropout(0.2)(x)

    outputs = Dense(1, activation="linear")(x)

    model = Model(inputs, outputs)
    model.compile(
        optimizer=Adam(learning_rate=1e-4),
        loss=Huber(delta=1.0)
    )

    model.fit(
        X_train, y_train,
        validation_data=(X_val, y_val),
        epochs=150,
        batch_size=256,
        callbacks=[
            EarlyStopping(
                monitor='val_loss',
                patience=10,
                restore_best_weights=True
            ),
            ReduceLROnPlateau(patience=5, verbose=1,),
        ],
    )
    return model
