import sys
import os

wd = os.getcwd()
sys.path[0] = wd + "/dags"

import obtainPrices_dag as op

import pytest
from pandas.testing import assert_frame_equal
import traceback
import logging
from io import BytesIO
from unittest.mock import patch
import pandas as pd
import sys


@pytest.fixture(name="raw_json")  # Is writen like this to avoid 'outer scope'
def df_raw_json():
    json = {
        "Meta Data": {
            "1. Information": "Daily Prices (open, high, low, close) and Volumes",
            "2. Symbol": "GOOG",
            "3. Last Refreshed": "2021-11-23",
            "4. Output Size": "Compact",
            "5. Time Zone": "US/Eastern",
        },
        "Time Series (Daily)": {
            "2021-11-23": {
                "1. open": "2942.2600",
                "2. high": "2953.8800",
                "3. low": "2897.7900",
                "4. close": "2935.1400",
                "5. volume": "906657",
            },
            "2021-11-22": {
                "1. open": "3002.8352",
                "2. high": "3014.8900",
                "3. low": "2940.1100",
                "4. close": "2941.5700",
                "5. volume": "1231385",
            },
            "2021-11-19": {
                "1. open": "3020.0000",
                "2. high": "3037.0000",
                "3. low": "2997.7500",
                "4. close": "2999.0500",
                "5. volume": "989148",
            },
        },
    }
    return json


def test_transform_json_df(raw_json):
    # try:
    df_expected = pd.DataFrame(
        [
            ["2021-11-23", 2942.2600, 2953.8800, 2897.7900, 2935.1400, 906657],
            ["2021-11-22", 3002.8352, 3014.8900, 2940.1100, 2941.5700, 1231385],
            ["2021-11-19", 3020.0000, 3037.0000, 2997.7500, 2999.0500, 989148],
        ],
        columns=["date", "1. open", "2. high", "3. low", "4. close", "5. volume"],
    )
    df_actual = op.transform_json_df(raw_json)
    # assert df_actual.iloc[1][2] == 3
    assert_frame_equal(df_actual, df_expected)


# except Exception as e:
#     logging.error(traceback.format_exc())
# Logs the error appropriately


@pytest.fixture(name="raw")  # Is writen like this to avoid 'outer scope'
def df_raw():
    df = pd.DataFrame(
        [
            ["2021-11-23", 2942.2600, 2953.8800, 2897.7900, 2935.1400, 906657],
        ],
        columns=["date", "1. open", "2. high", "3. low", "4. close", "5. volume"],
    )
    return df


def test_transform_df(raw):
    # try:
    df_expected = pd.DataFrame(
        [
            [
                "2021-11-23",
                2942.2600,
                2953.8800,
                2897.7900,
                2935.1400,
                906657,
                2925.835,
                629.622917,
            ]
        ],
        columns=[
            "date",
            "1. open",
            "2. high",
            "3. low",
            "4. close",
            "5. volume",
            "avg_price",
            "avg_num_trades",
        ],
    )

    date = "2021-11-23"
    df_actual = op.transform_df(raw, date)

    assert df_actual.iloc[0][5] == df_expected.iloc[0][5]
    assert_frame_equal(df_actual, df_expected)


# except Exception as e:
#    logging.error(traceback.format_exc())
# Logs the error appropriately
