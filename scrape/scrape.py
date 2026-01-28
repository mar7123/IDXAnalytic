from sqlalchemy import Engine, create_engine, text
import curl_cffi
import json
from datetime import datetime, timedelta
import time
import random

import os

from response_model import *

DAY_WINDOW = 800
LOG_NAME = "scrape/scrape.txt"
DB_CONFIG_PATH = "db/db_config.json"


def load_db_config(path=DB_CONFIG_PATH):
    with open(path, "r") as f:
        return json.load(f)


def get_engine(config_path=DB_CONFIG_PATH):
    cfg = load_db_config(config_path)
    return create_engine(
        f"mysql+pymysql://{cfg['user']}:{cfg['password']}@{cfg['host']}/{cfg['database']}"
    )


def create_db_connection(engine: Engine):
    return engine.connect()


engine = get_engine()

with open(LOG_NAME, "w") as fw:
    fw.write("start\n")
now = datetime.now()
end = (now if now.hour >= 17 else now - timedelta(days=1)).replace(
    hour=0, minute=0, second=0, microsecond=0
)
start = end - timedelta(days=DAY_WINDOW)

all_dates = {start + timedelta(days=i) for i in range((end - start).days + 1)}
dates = list()
first = datetime(year=2020, month=8, day=24)
with create_db_connection(engine) as connection:
    connection.begin()
    queried_timestamps_query = "SELECT * from time_dimensions"
    with open(LOG_NAME, "a") as fw:
        fw.write(f"Query {queried_timestamps_query}\n")
    result = connection.execute(text(queried_timestamps_query))
    queried_dates = set()
    for row in result:
        tm = row[0]
        if isinstance(tm, datetime):
            queried_dates.add(tm)
    dates = sorted(all_dates - queried_dates)
    connection.commit()
    connection.close()

for timestamp in dates:
    date_query = timestamp.strftime("%Y%m%d")
    index_url = "https://www.idx.co.id/primary/TradingSummary/GetIndexSummary"
    stock_url = "https://www.idx.co.id/primary/TradingSummary/GetStockSummary"
    params = {
        "length": "9999",
        "start": "0",
        "date": date_query,
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Host": "www.idx.co.id",
        "Referer": "www.idx.co.id",
    }
    # session = requests.Session(impersonate="chrome")
    with open(LOG_NAME, "a") as fw:
        fw.write(f"{date_query}\n")

    index_response = curl_cffi.get(
        index_url, params=params, headers=headers, impersonate="chrome"
    )
    if index_response.status_code != 200:
        with open(LOG_NAME, "a") as fw:
            fw.write(
                f"{index_response.status_code}\n{index_response.content}\n")
        num = random.randint(200, 500)
        time.sleep(num / 100)
        continue
    stock_response = curl_cffi.get(
        stock_url, params=params, headers=headers, impersonate="chrome"
    )
    if stock_response.status_code != 200:
        with open(LOG_NAME, "a") as fw:
            fw.write(
                f"{stock_response.status_code}\n{stock_response.content}\n")
        num = random.randint(200, 500)
        time.sleep(num / 100)
        continue
    index_content_json = json.loads(index_response.content)
    index_summary = IndexSummaryResponse.from_json(index_content_json)
    stock_content_json = json.loads(stock_response.content)
    stock_summary = StockSummaryResponse.from_json(stock_content_json)
    with create_db_connection(engine) as connection:
        time_dimensions_insert_query = f"INSERT IGNORE INTO time_dimensions VALUES (:tm)"
        time_res = connection.execute(
            text(time_dimensions_insert_query), {"tm": timestamp})
        with open(LOG_NAME, "a") as fw:
            fw.write(
                f"{time_dimensions_insert_query} Affected rows {time_res.rowcount}\n"
            )
        connection.commit()
        connection.close()

    if len(stock_summary.data) != 0 and len(index_summary.data) != 0:
        index_profiles = []
        index_data = []
        stock_profiles = []
        stock_data = []
        for i in index_summary.data:
            index_profiles.append({"index_code": i.IndexCode})
            index_data.append(
                {
                    "index_profile": i.IndexCode,
                    "timestamp": i.Date,
                    "previous": i.Previous,
                    "highest": i.Highest,
                    "lowest": i.Lowest,
                    "close": i.Close,
                    "number_of_stock": i.NumberOfStock,
                    "change": i.Change,
                    "volume": i.Volume,
                    "value": i.Value,
                    "frequency": i.Frequency,
                    "market_capital": i.MarketCapital,
                }
            )
        for i in stock_summary.data:
            stock_profiles.append(
                {
                    "stock_code": i.StockCode,
                    "stock_name": i.StockName,
                    "remarks": i.Remarks,
                    "delisting_date": (None if i is None else None if len(i.DelistingDate) == 0 else i.DelistingDate),
                }
            )
            stock_data.append(
                {
                    "stock_profile": i.StockCode,
                    "timestamp": i.Date,
                    "previous": i.Previous,
                    "open_price": i.OpenPrice,
                    "first_trade": i.FirstTrade,
                    "high": i.High,
                    "low": i.Low,
                    "close": i.Close,
                    "change": i.Change,
                    "volume": i.Volume,
                    "value": i.Value,
                    "frequency": i.Frequency,
                    "index_individual": i.IndexIndividual,
                    "offer": i.Offer,
                    "offer_volume": i.OfferVolume,
                    "bid": i.Bid,
                    "bid_volume": i.BidVolume,
                    "listed_shares": i.ListedShares,
                    "tradeble_shares": i.TradebleShares,
                    "weight_for_index": i.WeightForIndex,
                    "foreign_sell": i.ForeignSell,
                    "foreign_buy": i.ForeignBuy,
                    "non_regular_volume": i.NonRegularVolume,
                    "non_regular_value": i.NonRegularValue,
                    "non_regular_frequency": i.NonRegularFrequency,
                    "persen": i.persen,
                    "percentage": i.percentage,
                }
            )
        with create_db_connection(engine) as connection:
            index_profiles_insert_query = (
                "INSERT IGNORE INTO index_profiles (index_code) VALUE (:index_code)"
            )
            idx_prof_res = connection.execute(
                text(index_profiles_insert_query), index_profiles)
            with open(LOG_NAME, "a") as fw:
                fw.write(
                    f"{index_profiles_insert_query} Affected rows {idx_prof_res.rowcount}\n"
                )
            index_data_insert_query = "INSERT IGNORE INTO index_data VALUE (:index_profile, :timestamp, :previous, :highest, :lowest, :close, :number_of_stock, :change, :volume, :value, :frequency, :market_capital)"
            idx_data_res = connection.execute(
                text(index_data_insert_query), index_data)
            with open(LOG_NAME, "a") as fw:
                fw.write(
                    f"{index_data_insert_query} Affected rows {idx_data_res.rowcount}\n")
            stock_profiles_insert_query = "INSERT INTO stock_profiles VALUE (:stock_code, :stock_name, :remarks, :delisting_date) ON DUPLICATE KEY UPDATE delisting_date = delisting_date"
            stock_prof_res = connection.execute(
                text(stock_profiles_insert_query), stock_profiles)
            with open(LOG_NAME, "a") as fw:
                fw.write(
                    f"{stock_profiles_insert_query} Affected rows {stock_prof_res.rowcount}\n"
                )
            stock_data_insert_query = "INSERT IGNORE INTO stock_data VALUE (:stock_profile, :timestamp, :previous, :open_price, :first_trade, :high, :low, :close, :change, :volume, :value, :frequency, :index_individual, :offer, :offer_volume, :bid, :bid_volume, :listed_shares, :tradeble_shares, :weight_for_index, :foreign_sell, :foreign_buy, :non_regular_volume, :non_regular_value, :non_regular_frequency, :persen, :percentage)"
            stock_data_res = connection.execute(
                text(stock_data_insert_query), stock_data)
            with open(LOG_NAME, "a") as fw:
                fw.write(
                    f"{stock_data_insert_query} Affected rows {stock_data_res.rowcount}\n")
            connection.commit()
            connection.close()
    else:
        with open(LOG_NAME, "a") as fw:
            fw.write("data empty\n")
    num = random.randint(200, 500)
    time.sleep(num / 100)

# os.system("shutdown /s /t 0")
