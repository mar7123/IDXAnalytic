from datetime import datetime
import os
import pandas as pd
from sqlalchemy import Engine, text
import tensorflow as tf
import random
import numpy as np
from stock.config import CLEAN_SQL, PREP_SQL
from stock.train_return import build_return_model
from stock.train_vol import build_vol_model
from stock.train_drawdown import build_drawdown_model
from stock.train_regime import build_regime_model
from stock.dataset import make_inference_sequences
from stock.config import OUTPUT_PATH, config_manager


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
    start_time = datetime.now()
    # prep_data(engine)
    inference_df = load_inference(engine)
    stock_profile_set: set[str] = set()
    for stock_profile in inference_df["stock_profile"].values:
        stock_profile_set.add(stock_profile)
    config_manager.stock_profile_mapper = {label: i for i,
                                           label in enumerate(sorted(stock_profile_set))}
    stock_profile_mapper_reversed = {
        v: k for k, v in config_manager.stock_profile_mapper.items()}
    X_infer, X_infer_id, X_regime = make_inference_sequences(inference_df)
    result_df = pd.DataFrame()
    num_train = 2
    for i in range(num_train):
        rand = random.randint(1, 1000)
        random.seed(rand)
        tf.random.set_seed(rand)
        np.random.seed(rand)
        # return_model = build_return_model(engine=engine)
        # vol_model = build_vol_model(engine=engine)
        # drawdown_model = build_drawdown_model(engine=engine)
        regime_model = build_regime_model(engine=engine, rand=rand)

        # return_pred = return_model.predict(
        #     x={
        #         "ts_input": X_infer,
        #         "stock_id_input": X_infer_id
        #     },
        #     verbose=1,
        # )
        # vol_pred = vol_model.predict(
        #     x={
        #         "ts_input": X_infer,
        #         "stock_id_input": X_infer_id
        #     },
        #     verbose=1,
        # )
        # drawdown_pred = drawdown_model.predict(
        #     x={
        #         "ts_input": X_infer,
        #         "stock_id_input": X_infer_id
        #     },
        #     verbose=1,
        # )
        regime_pred = regime_model.predict_proba(
            X_regime,
        )
        regime_prob = pd.DataFrame(regime_pred, columns=["0_prob", "1_prob", "2_prob"])
        temp_df = pd.DataFrame({
            "stock_id": X_infer_id.flatten(),
            # "return_pred": return_pred.flatten(),
            # "vol_pred": vol_pred.flatten(),
            # "drawdown_pred": drawdown_pred.flatten(),
        })
        temp_df["stock_profile"] = temp_df["stock_id"].map(
            stock_profile_mapper_reversed)
        # temp_df["result_score"] = (
        #     temp_df["return_pred"] + (-0.5 * temp_df["vol_pred"]) + (-0.5 * temp_df["drawdown_pred"]))/3
        if i == 0:
            result_df["stock_profile"] = temp_df["stock_profile"]
        # result_df[f"result_score{i}"] = temp_df["result_score"]
        result_df[f"regime_prob{i}"] = regime_prob["2_prob"] - regime_prob["1_prob"]

    # result_df["score"] = result_df[[
    #     f'result_score{i}' for i in range(num_train)]].mean(axis=1)
    result_df["regime"] = result_df[[
        f'regime_prob{i}' for i in range(num_train)]].mean(axis=1)

    with pd.ExcelWriter(OUTPUT_PATH, engine="openpyxl") as writer:
        result_df.to_excel(
            writer, sheet_name="result_df", index=False)

    # clean_data(engine)
    end_time = datetime.now()
    duration = end_time-start_time
    print(duration)
