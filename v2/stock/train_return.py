from keras.models import Model
from keras.layers import Input, Dense, Dropout, LayerNormalization, Embedding, Flatten, Concatenate, RepeatVector
from keras.optimizers import Adam
from keras.losses import Huber
from keras.metrics import MeanAbsoluteError, R2Score
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

# TODO add market return diff


def build_return_model(engine: Engine):
    X_train, X_train_id, X_train_id_count, y_train = make_return_sequences(
        load_return_training(engine))
    X_val, X_val_id, X_val_id_count, y_val = make_return_sequences(
        load_return_test(engine))

    ts_input = Input(shape=(WINDOW, X_train.shape[-1]), name="ts_input")

    stock_id_input = Input(shape=(1,), name="stock_id_input")
    emb = Embedding(input_dim=X_train_id_count,
                    output_dim=18)(stock_id_input)
    emb = Flatten()(emb)
    emb_seq = RepeatVector(WINDOW)(emb)

    merged_input = Concatenate(axis=-1)([ts_input, emb_seq])

    x = TCN(
        nb_filters=64,
        kernel_size=5,
        dilations=[1, 2, 4, 8, 16, 32],
        padding="causal",
        dropout_rate=0.1,
        return_sequences=False
    )(merged_input)
    x = LayerNormalization()(x)

    x = Dense(64, activation="relu")(x)
    x = Dropout(0.1)(x)

    z = Dense(32, activation="relu")(x)
    z = Dropout(0.1)(z)

    output = Dense(1, activation="linear")(z)

    model = Model(inputs=[ts_input, stock_id_input], outputs=output)

    model.compile(
        optimizer=Adam(learning_rate=1e-4),
        loss=Huber(delta=1),
        metrics=[MeanAbsoluteError(), R2Score()]
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
    # preds = model.predict([X_val, X_val_id])
    return model
