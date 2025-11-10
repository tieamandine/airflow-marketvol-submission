# dags/marketvol.py
from datetime import datetime, timedelta, date
from pathlib import Path
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import yfinance as yf
import pandas as pd

LOCAL_TZ = pendulum.timezone("America/New_York")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# -------- Python callables --------
def download_symbol(symbol: str, ds: str, **_):
    """
    Download 1-min bars for the execution date [ds, ds+1) and save to /tmp/{symbol_lower}_{ds}.csv
    """
    start_date = datetime.strptime(ds, "%Y-%m-%d").date()
    end_date = start_date + timedelta(days=1)

    df = yf.download(symbol, start=start_date, end=end_date, interval="1m")
    out_path = Path(f"/tmp/{symbol.lower()}_{ds}.csv")
    df.to_csv(out_path, header=False)

def run_query(ds: str, **_):
    """
    Load both CSVs from /tmp/data/{ds} and print a tiny summary (rows, min/max time, avg volume).
    This is just a placeholder for your custom analytics.
    """
    base = Path(f"/tmp/data/{ds}")
    files = [base / f"aapl_{ds}.csv", base / f"tsla_{ds}.csv"]

    # Read as no-header CSV then assign columns
    cols = ["Datetime","Open","High","Low","Close","Adj Close","Volume"]
    for fp in files:
        df = pd.read_csv(fp, header=None, names=cols)
        df["Datetime"] = pd.to_datetime(df["Datetime"])
        print(f"\n=== {fp.name} ===")
        print(f"Rows: {len(df)}")
        if len(df):
            print(f"Time range: {df['Datetime'].min()} -> {df['Datetime'].max()}")
            print(f"Avg Volume: {df['Volume'].mean():,.2f}")
            print(df.head(5).to_string(index=False))

# -------- DAG --------
with DAG(
    dag_id="marketvol",
    description="Download AAPL & TSLA minute bars and run a simple query",
    default_args=default_args,
    # 6:00 PM Mon–Fri (New York time)
    schedule_interval="0 18 * * 1-5",
    start_date=pendulum.datetime(2025, 11, 1, 18, 0, tz=LOCAL_TZ),
    catchup=True,           # keep on for repeatable backfills if desired
    tags=["mini-project", "yfinance", "stocks"],
) as dag:

    # t0 — make target dir for this run (use templated {{ ds }})
    t0 = BashOperator(
        task_id="t0",
        bash_command="mkdir -p /tmp/data/{{ ds }}"
    )

    # t1/t2 — download to /tmp
    t1 = PythonOperator(
        task_id="t1_download_aapl",
        python_callable=download_symbol,
        op_kwargs={"symbol": "AAPL", "ds": "{{ ds }}"},
    )

    t2 = PythonOperator(
        task_id="t2_download_tsla",
        python_callable=download_symbol,
        op_kwargs={"symbol": "TSLA", "ds": "{{ ds }}"},
    )

    # t3/t4 — move files into /tmp/data/{{ ds }}
    t3 = BashOperator(
        task_id="t3_move_aapl",
        bash_command="cp /tmp/aapl_{{ ds }}.csv /tmp/data/{{ ds }}/"
    )

    t4 = BashOperator(
        task_id="t4_move_tsla",
        bash_command="cp /tmp/tsla_{{ ds }}.csv /tmp/data/{{ ds }}/"
    )

    # t5 — Python query over both files in the target dir
    t5 = PythonOperator(
        task_id="t5_query_both",
        python_callable=run_query,
        op_kwargs={"ds": "{{ ds }}"},
    )

    # dependencies
    t0 >> [t1, t2]
    t1 >> t3
    t2 >> t4
    [t3, t4] >> t5
