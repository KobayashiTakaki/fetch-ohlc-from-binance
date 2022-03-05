import os
from os import path
import time
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
from urllib.parse import urljoin
import zipfile
import requests
import pandas as pd


BASE_URL = "https://data.binance.vision"
KLINES_PATH = "/data/spot/monthly/klines"
SYMBOL = "BTCUSDT"
INTERVAL = "1m"

def main():
    out_file_path = path.join(path.dirname(__file__), "tmp", "ohlc_all_{}.csv".format(INTERVAL))
    init_output_file(out_file_path)
    now = datetime.now(timezone.utc)
    time_to = datetime(now.year, now.month, 1, 0, 0, 0, 0, timezone.utc)
    time_target = datetime(2019, 1, 1, 0, 0, 0, 0, timezone.utc)
    while time_target <= time_to:
        file_path = fetch_file(time_target.year, time_target.month)
        time_target += relativedelta(months=1)
        if not file_path:
            continue
        zip_file = zipfile.ZipFile(file_path)
        filename = zip_file.namelist()[0]
        with open(out_file_path, mode="a") as out_f:
            with zip_file.open(filename, mode="r") as zip_f:
                out_f.write(zip_f.read().decode())
        time.sleep(1)
    format_csv(out_file_path)

def fetch_file(year, month):
    filename = "{}-{}-{}-{:02}.zip".format(SYMBOL, INTERVAL, year, month)
    reqest_path = KLINES_PATH + "/{}/{}/{}".format(SYMBOL, INTERVAL, filename)
    url = urljoin(BASE_URL, reqest_path)
    print("fetch file ", filename)
    res = requests.get(url, stream=True)
    if res.status_code != requests.codes.ok:
        return False
    out_file_path = path.join(path.dirname(__file__), "tmp", filename)
    with open(out_file_path, "wb") as f:
        for chunk in res.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)
                f.flush()
    return out_file_path

def init_output_file(output_path):
    if os.path.isfile(output_path):
        os.remove(output_path)
    with open(output_path, mode='w') as f:
        columns = [
            "open_time",
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "volume",
            "close_time",
            "quote_asset_volume",
            "number_of_trades",
            "taker_buy_base_asset_volume",
            "taker_buy_quote_asset_volume",
            "ignore",
        ]
        for i, column in enumerate(columns):
            f.write(column)
            if i != len(columns) - 1:
                f.write(',')
        f.write('\n')

def format_csv(file_path):
    df = pd.read_csv(file_path)
    df["close_time"] = ((df["open_time"] + 1) / 1000).astype("int")
    df = df[[
        "close_time",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "volume",
    ]]
    out_file_path = path.join(path.dirname(__file__), "tmp", "ohlc_{}.csv".format(INTERVAL))
    df.to_csv(out_file_path, index=False)

if __name__ == "__main__":
    main()
