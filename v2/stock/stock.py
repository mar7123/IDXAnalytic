from sqlalchemy import Engine, text
from stock.config import CLEAN_SQL, PREP_SQL
from stock.train_return import build_return_model
from stock.train_vol import build_vol_model
from stock.train_drawdown import build_drawdown_model
from stock.train_crash import build_crash_model


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

    return_model = build_return_model(engine)
    vol_model = build_vol_model(engine)
    drawdown_model = build_drawdown_model(engine=engine)
    crash_model = build_crash_model(engine=engine)

    clean_data(engine)
