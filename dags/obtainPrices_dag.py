"""Stocks dag extended."""
import json
from datetime import datetime
from time import sleep

import numpy as np
import pandas as pd
import requests  # type: ignore
import sqlalchemy
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers import postgres
from sqlPostgresCli import SqlPostgresClient
from decouple import config
import matplotlib.pyplot as plt
from airflow.models import DAG


BASE_URL = "https://www.alphavantage.co/query"
API_KEY = "TFHNYCWBD71JBSON"
STOCK_FN = "TIME_SERIES_DAILY"
SQL_TABLE = "stocks_daily"
SCHEMA = config("DB_SCHEMA")

SQL_CREATE = f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.{SQL_TABLE} (
date date,
symbol TEXT,
avg_num_trades REAL,
avg_price REAL,
UNIQUE(date,symbol)
)
"""

STOCK = {"Google": "GOOG", "Microsoft": "MSFT", "Amazon": "AMZN"}


def transform_json_df(data):
    """Transforms a json into a Pandas dataFrame, convert columns into numbers

    Args:
        data ([Json]): Json File from Time Series (Daily)

    Returns:
        [DataFrame]: [date,"1. open","2. high","3. low","4. close","5. volume"]
    """
    df = (
        pd.DataFrame(data["Time Series (Daily)"])
        .T.reset_index()
        .rename(columns={"index": "date"})
    )
    df["1. open"] = pd.to_numeric(df["1. open"])
    df["2. high"] = pd.to_numeric(df["2. high"])
    df["3. low"] = pd.to_numeric(df["3. low"])
    df["4. close"] = pd.to_numeric(df["4. close"])
    df["5. volume"] = pd.to_numeric(df["5. volume"])
    return df


def transform_df(df, date):
    """Form a DataFrame generates a now one with the average number of trades and the average price of the day

    Args:
        df ([DataFrame]): [date,"1. open","2. high","3. low","4. close","5. volume"]
        date ([str]): date to process

    Returns:
        [dataFrame]: ["date", "avg_num_trades", "avg_price"]
    """
    if not df.empty:
        #for c in df.columns:
            #if c != "date":
               # df[c] = df[c].astype(float)
        df["avg_price"] = (df["2. high"] + df["3. low"]) / 2
        df["avg_num_trades"] = df["5. volume"] / 1440
    else:
        df = pd.DataFrame(
            [[date, np.nan, np.nan]], columns=["date", "avg_num_trades", "avg_price"]
        )
    return df


def transform2(df):
    df["date"] = df["date"].astype(str)
    df["avg_price"] = df["avg_price"].astype(float)
    return df


def _get_stock_data(stock_symbol, **context):
    f"""Gets Time Series (Daily) from {BASE_URL}

    Args:
        stock_symbol ([str]): {STOCK}

    Returns:
        [json]: containing "date", "symbol", "avg_num_trades", "avg_price"
    """

    date = f"{context['execution_date']:%Y-%m-%d}"  # read execution date from context
    end_point = (
        f"{BASE_URL}?function={STOCK_FN}&symbol={stock_symbol}"
        f"&apikey={API_KEY}&datatype=json"
    )
    print(f"Getting data from {end_point}...")
    r = requests.get(end_point)
    sleep(61)  # To avoid api limits

    data = json.loads(r.content)
    with open(f"dags/files/{stock_symbol}{date}Raw.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

    df = transform_json_df(data)

    df = df[df["date"] == date]  # Obtain only records for the definied date
    df = transform_df(
        df, date
    )  # Transform df in a new one as (date, avg_num_trades,avg_price)

    df["symbol"] = stock_symbol  # add new column
    df = df[["date", "symbol", "avg_num_trades", "avg_price"]]
    df.to_json(f"dags/files/{stock_symbol}{date}.json")
    return df.to_json()


def _insert_daily_data(**context):
    f"""Gets daily data, transform into df and save it in {SCHEMA}.{SQL_TABLE}
    """
    task_instance = context["ti"]
    # Get xcom for each upstream task
    dfs = []
    for ticker in STOCK:
        stock_df = pd.read_json(
            task_instance.xcom_pull(task_ids=f"get_daily_data_{ticker}"),
            orient="index",
        ).T
        stock_df = stock_df[["date", "symbol", "avg_num_trades", "avg_price"]]
        dfs.append(stock_df)
    df = pd.concat(dfs, axis=0)
    sql_cli = SqlPostgresClient()
    try:
        sql_cli.insert_from_frame(df, SQL_TABLE)
        print(f"Inserted {len(df)} records")
    except sqlalchemy.exc.IntegrityError:
        # You can avoid doing this by setting a trigger rule in the reports operator
        print("Data already exists! Nothing to do...")


def _perform_weekly_report(**context):
    """ Performs a weekly report of the stocks in pdf format """

    date = context["execution_date"]
    date = date.date()
    dateStr = date.strftime("%Y%m%d")  # read execution date from context

    SQL_REPORT = f"""
        SELECT date, symbol, avg_price
        FROM {SCHEMA}.{SQL_TABLE}
        where  date >= (to_date('{dateStr}','yyyymmdd') - INTERVAL '7 day') 
        and date <= to_date('{dateStr}','yyyymmdd')
        ORDER BY symbol, date
        """
    sql_cli = SqlPostgresClient()
    sql = SQL_REPORT.format()
    df = sql_cli.to_frame(sql)
    print(df)
    df = transform2(df)

    fig, ax = plt.subplots()
    for key in STOCK:
        ax.plot(
            df[df.symbol == STOCK[key]]["date"],
            df[df.symbol == STOCK[key]]["avg_price"],
            label=key,
        )
    ax.set_xlabel("Date")
    ax.set_ylabel("Prices")
    ax.legend(loc="best")
    plt.title(
        f"Prices during last week (actual day: {date.strftime('%Y-%m-%d')})",
        fontsize=12,
    )
    plt.savefig(f"dags/graphs/{date.strftime('%Y-%m-%d')}.pdf")
    msg = ()
    #   f"Most traded action in {date} was {df['symbol']} with "
    #    f"an avg of {df['avg_num_trades']} trades per minute."
    # )
    return msg


default_args = {
    "owner": "flor",
    "retries": 0,
    "start_date": datetime(2021, 11, 1),
}
with DAG(
    "price_solution", default_args=default_args, schedule_interval="0 4 * * 1-5"
) as dag:

    create_table_if_not_exists = PostgresOperator(
        task_id="create_table_if_not_exists",
        sql=SQL_CREATE,
        postgres_conn_id=config("CONN_ID"),
    )
    # Create several task in loop
    get_data_task = {}
    for company, symbol in STOCK.items():
        get_data_task[company] = PythonOperator(
            task_id=f"get_daily_data_{company}",
            python_callable=_get_stock_data,
            op_args=[symbol],
        )

    insert_daily_data = PythonOperator(
        task_id="insert_daily_data", python_callable=_insert_daily_data
    )

    do_weekly_report = PythonOperator(
        task_id="do_weekly_prices_report", python_callable=_perform_weekly_report
    )

    for company in STOCK:
        upstream_task = create_table_if_not_exists
        task = get_data_task[company]
        upstream_task.set_downstream(task)
        task.set_downstream(insert_daily_data)
    insert_daily_data.set_downstream(do_weekly_report)
