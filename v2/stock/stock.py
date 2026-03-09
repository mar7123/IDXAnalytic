import pandas as pd
from sqlalchemy import Engine, text
from stock.config import CLEAN_SQL, PREP_SQL
from stock.train_return import build_return_model
from stock.train_vol import build_vol_model
from stock.train_drawdown import build_drawdown_model
from stock.train_crash import build_crash_model
from stock.dataset import make_inference_sequences
from config import OUTPUT_PATH


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


def load_inference(db_engine: Engine) -> pd.DataFrame:
    with db_engine.connect() as connection:
        pd_query = f"""
        SELECT
            *
        FROM stock_inference
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
    inference_df = load_inference(engine)
    stock_profile_set: set[str] = set()
    for stock_profile in inference_df["stock_profile"].values:
        stock_profile_set.add(stock_profile)
    stock_profile_mapper = {label: i for i,
                            label in enumerate(sorted(stock_profile_set))}
    stock_profile_mapper_reversed = {
        v: k for k, v in stock_profile_mapper.items()}

    X_infer, X_infer_id = make_inference_sequences(
        inference_df, stock_profile_mapper)

    return_model = build_return_model(
        engine=engine, stock_profile_mapper=stock_profile_mapper)
    vol_model = build_vol_model(
        engine=engine, stock_profile_mapper=stock_profile_mapper)
    drawdown_model = build_drawdown_model(
        engine=engine, stock_profile_mapper=stock_profile_mapper)
    # crash_model = build_crash_model(engine=engine)

    return_pred = return_model.predict(
        x={
            "ts_input": X_infer,
            "stock_id_input": X_infer_id
        },
        verbose=1,
    )
    vol_pred = vol_model.predict(
        x={
            "ts_input": X_infer,
            "stock_id_input": X_infer_id
        },
        verbose=1,
    )
    drawdown_pred = drawdown_model.predict(
        x={
            "ts_input": X_infer,
            "stock_id_input": X_infer_id
        },
        verbose=1,
    )
    result_df = pd.DataFrame({
        "stock_id": X_infer_id.flatten(),
        "return_pred": return_pred.flatten(),
        "vol_pred": vol_pred.flatten(),
        "drawdown_pred": drawdown_pred.flatten(),
    })
    result_df["stock_profile"] = result_df["stock_id"].map(
        stock_profile_mapper_reversed)
    result_df["result_score"] = (
        result_df["return_pred"] / result_df["vol_pred"])/result_df["drawdown_pred"]

    with pd.ExcelWriter(OUTPUT_PATH, engine="openpyxl") as writer:
        result_df.to_excel(
            writer, sheet_name="result_df", index=False)

    clean_data(engine)
