from sqlalchemy import Engine, text
import pandas as pd
from keras.callbacks import EarlyStopping, ReduceLROnPlateau
from stock.config import CLEAN_SQL, PREP_SQL
from stock.dataset import make_sequences
from stock.train_return import build_tcn_return_model


def prep_data(db_engine: Engine):
    with db_engine.connect() as connection:
        with open(PREP_SQL, "r") as fw:
            content = fw.read()
            for query in content.split(";"):
                if len(query) == 0:
                    continue
                connection.execute(text(query))
        connection.commit()
        connection.close()


def load_training(db_engine: Engine) -> pd.DataFrame:
    with db_engine.connect() as connection:
        pd_query = f"""
        SELECT
            *
        FROM stock_return_train
        """
        df = pd.read_sql(pd_query, connection)
    return df


def load_test(db_engine: Engine) -> pd.DataFrame:
    with db_engine.connect() as connection:
        pd_query = f"""
        SELECT
            *
        FROM stock_return_val
        """
        df = pd.read_sql(pd_query, connection)
    return df


def clean_data(db_engine: Engine):
    with db_engine.connect() as connection:
        with open(CLEAN_SQL, "r") as fw:
            content = fw.read()
            for query in content.split(";"):
                if len(query) == 0:
                    continue
                connection.execute(text(query))
        connection.commit()
        connection.close()


def stock_main(engine: Engine):
    prep_data(engine)
    X_train, y_train = make_sequences(load_training(engine))
    X_val, y_val = make_sequences(load_test(engine))

    return_model = build_tcn_return_model(X_train.shape[-1])

    return_model.fit(
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

    clean_data(engine)
