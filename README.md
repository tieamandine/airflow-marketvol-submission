# Apache Airflow Mini-Project: MarketVol DAG

This mini-project demonstrates how to orchestrate a data pipeline using Apache Airflow to fetch one-minute stock market data for **AAPL** and **TSLA** from Yahoo Finance.

The DAG automates downloading, storing, and analyzing the data using PythonOperator and BashOperator, running on a CeleryExecutor Airflow setup.

---

## Files Included

| File | Description |
|------|--------------|
| `marketvol_dag.py` | The complete DAG code used to orchestrate the workflow. |
| `Dag_Success.png` | Screenshot showing a successful DAG run in Airflow. |
| `README.md` | Instructions for running and verifying the project. |

---

## How to Run This DAG

### 1. Prerequisites
- Apache Airflow installed (Docker or local setup)
- `yfinance` and `pandas` available in your Airflow environment

### 2. Place the DAG
Copy `marketvol_dag.py` into your local Airflow `dags/` 
cp marketvol_dag.py ~/airflow/dags/

### 3. Start Airflow
- airflow webserver & airflow scheduler
- http://localhost:8080

### 4. Trigger the DAG
Manually trigger two runs
Dag flow t0 → (t1, t2) → (t3, t4) → t5