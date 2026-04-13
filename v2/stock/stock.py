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


def stock_short_term(engine: Engine):
    start_time = datetime.now()
    prep_data(engine)
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
    val_loss_df = pd.DataFrame()
    num_train = 8
    prob_lis = ["0_prob", "1_prob", "2_prob"]
    for i in range(num_train):
        rand = random.randint(1, 1000)
        random.seed(rand)
        tf.random.set_seed(rand)
        np.random.seed(rand)
        regime_model, regime_val_loss = build_regime_model(
            engine=engine, rand=rand)
        regime_pred = regime_model.predict_proba(
            X_regime,
        )
        regime_prob = pd.DataFrame(regime_pred, columns=prob_lis)
        val_loss_df[f"regime_model_val{i}"] = [regime_val_loss]
        temp_df = pd.DataFrame({
            "stock_id": X_infer_id.flatten(),
        })
        temp_df["stock_profile"] = temp_df["stock_id"].map(
            stock_profile_mapper_reversed)
        if i == 0:
            result_df["stock_profile"] = temp_df["stock_profile"]
        for prob in prob_lis:
            result_df[f"{prob}_{i}"] = regime_prob[prob]
    for prob in prob_lis:
        result_df[f"regime_{prob}"] = result_df[[
            f'{prob}_{i}' for i in range(num_train)]].mean(axis=1)

    with pd.ExcelWriter(OUTPUT_PATH, engine="openpyxl") as writer:
        result_df.to_excel(
            writer, sheet_name="result_df", index=False)
        val_loss_df.to_excel(
            writer, sheet_name="val_loss_df", index=False)

    clean_data(engine)
    end_time = datetime.now()
    duration = end_time-start_time
    print(duration)


def stock_main(engine: Engine):
    start_time = datetime.now()
    prep_data(engine)
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
    val_loss_df = pd.DataFrame()
    num_train = 2
    for i in range(num_train):
        rand = random.randint(1, 1000)
        random.seed(rand)
        tf.random.set_seed(rand)
        np.random.seed(rand)
        return_model, return_val_loss = build_return_model(engine=engine)
        vol_model, vol_val_loss = build_vol_model(engine=engine)
        drawdown_model, drawdown_val_loss = build_drawdown_model(engine=engine)
        regime_model, regime_val_loss = build_regime_model(
            engine=engine, rand=rand)

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
        regime_pred = regime_model.predict(
            X_regime,
        )
        val_loss_df[f"return_model_val{i}"] = [return_val_loss]
        val_loss_df[f"vol_model_val{i}"] = [vol_val_loss]
        val_loss_df[f"drawdown_model_val{i}"] = [drawdown_val_loss]
        val_loss_df[f"regime_model_val{i}"] = [regime_val_loss]

        temp_df = pd.DataFrame({
            "stock_id": X_infer_id.flatten(),
            "return_pred": return_pred.flatten(),
            "vol_pred": vol_pred.flatten(),
            "drawdown_pred": drawdown_pred.flatten(),
            "regime_pred": regime_pred,
        })

        temp_df["return_pred"] = temp_df["return_pred"].rank(pct=True)
        temp_df["vol_pred"] = temp_df["vol_pred"].rank(
            pct=True, ascending=False)
        temp_df["drawdown_pred"] = temp_df["drawdown_pred"].rank(
            pct=True, ascending=False)

        conditions = [
            (temp_df['regime_pred'] == 0),
            (temp_df['regime_pred'] == 1),
            (temp_df['regime_pred'] == 2),
        ]
        return_weights = [0.1, 0.4, 0.7]
        vol_weights = [0.6, 0.3, 0.1]
        drawdown_weights = [0.6, 0.3, 0.1]
        temp_df["w_r"] = np.select(conditions, return_weights)
        temp_df["w_v"] = np.select(conditions, vol_weights)
        temp_df["w_d"] = np.select(conditions, drawdown_weights)

        temp_df["stock_profile"] = temp_df["stock_id"].map(
            stock_profile_mapper_reversed)
        temp_df["result_score"] = (
            (temp_df["w_r"] * temp_df["return_pred"]) +
            (temp_df["w_v"] * temp_df["vol_pred"]) +
            (temp_df["w_d"] * temp_df["drawdown_pred"])
        )

        if i == 0:
            result_df["stock_profile"] = temp_df["stock_profile"]
        result_df[f"result_score{i}"] = temp_df["result_score"]
        result_df[f"regime_pred{i}"] = temp_df["regime_pred"]

    result_df["score"] = result_df[[
        f'result_score{i}' for i in range(num_train)]].mean(axis=1)
    result_df["score_std"] = result_df[[
        f'result_score{i}' for i in range(num_train)]].std(axis=1)

    with pd.ExcelWriter(OUTPUT_PATH, engine="openpyxl") as writer:
        result_df.to_excel(
            writer, sheet_name="result_df", index=False)
        val_loss_df.to_excel(
            writer, sheet_name="val_loss_df", index=False)

    clean_data(engine)
    end_time = datetime.now()
    duration = end_time-start_time
    print(duration)
