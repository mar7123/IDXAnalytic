import pandas as pd
from sqlalchemy import Engine, text

from risk_engine.config import ALL_FEATURES, CRASH_TARGET, INDEX_CODE, NUM_FEATURES, PREP_SQL, CLEAN_SQL
from risk_engine.train_crash import train_crash_model
from risk_engine.train_regime import train_regime_model
from risk_engine.train_volatility import train_vol_model
from risk_engine.infer import infer_risk


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
        FROM risk_engine_normalized_training_data
        """
        df = pd.read_sql(pd_query, connection)
    return df


def load_test(db_engine: Engine) -> pd.DataFrame:
    with db_engine.connect() as connection:
        pd_query = f"""
        SELECT
            *
        FROM risk_engine_normalized_test_data
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


def risk_engine_main(engine: Engine):
    prep_data(engine)

    train_df = load_training(engine)
    test_df = load_test(engine)
    concat = pd.concat([train_df, test_df])

    crash_X = train_df[ALL_FEATURES].values
    crash_y = train_df[CRASH_TARGET].values

    trial = 5
    for i in range(trial):
        regime_model = train_regime_model(train_df, test_df)
        if regime_model.history.history["val_loss"][-1] < 0.4:
            break
    for i in range(trial):
        vol_model = train_vol_model(train_df, test_df)
        if vol_model.history.history["val_loss"][-1] < 0.3:
            break
    crash_model = train_crash_model(crash_X, crash_y)

    print(infer_risk(regime_model=regime_model, vol_model=vol_model,
          crash_model=crash_model, latest_df=concat))

    clean_data(engine)
