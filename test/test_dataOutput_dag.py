import sys
import os
import obtainPrices_dag as op
import pytest
from pandas.testing import assert_frame_equal
import traceback
import logging
from io import BytesIO
from unittest.mock import patch
import pandas as pd
import sys


@pytest.fixture(name="raw")  # Is writen like this to avoid 'outer scope'
def df_raw():
    df = pd.DataFrame(
        [
            ["2021-11-23", "GOOG", 1777.9653, 3566.2976],
        ],
        columns=["date", "symbol", "avg_price", "avg_num_trades"],
    )
    df["date"] = pd.to_datetime(df["date"])
    return df


def test_transform2(raw):
   # try:
        df_expected = pd.DataFrame(
            [["2021-11-23", "GOOG", 1777.9653, 3566.2976]],
            columns=[
                "date",
                "symbol",
                "avg_price",
                "avg_num_trades",
            ],
        )
        df_expected["avg_price"] = df_expected["avg_price"].astype(float)
        df_actual = op.transform2(raw)

        assert df_actual.iloc[0][2] == df_expected.iloc[0][2]
        assert_frame_equal(df_actual, df_expected)
   # except Exception as e:
   #     logging.error(traceback.format_exc())
        # Logs the error appropriately
