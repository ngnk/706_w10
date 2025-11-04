from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


# ---- Config (from .env) ----
DAG_SCHEDULE = os.getenv("DAG_SCHEDULE", "@daily")
YELLOW_PATH = os.getenv("YELLOW_PATH", "/opt/airflow/dags/data/yellow_tripdata_2025-01.parquet")
GREEN_PATH  = os.getenv("GREEN_PATH",  "/opt/airflow/dags/data/green_tripdata_2025-01.parquet")

BASE_DIR = Path("/opt/airflow")
DATA_DIR = BASE_DIR / "dags" / "data"
TMP_DIR  = DATA_DIR / "tmp"
OUT_DIR  = BASE_DIR / "output"
TMP_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)


default_args = {"owner": "week10", "retries": 0}

with DAG(
    dag_id="week10_taxi_pipeline",
    description="Parallel ingest/transform/merge/load/analysis/cleanup using local Parquet files",
    start_date=datetime(2025, 10, 1),
    schedule=DAG_SCHEDULE,
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["week10", "taxi", "postgres"],
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    # ---------- Setup ----------
    @task
    def init_warehouse():
        sql = """
        CREATE SCHEMA IF NOT EXISTS warehouse;

        CREATE TABLE IF NOT EXISTS warehouse.taxi_trips_clean (
            source TEXT,
            pickup_ts TIMESTAMP,
            dropoff_ts TIMESTAMP,
            passenger_count INT,
            trip_distance DOUBLE PRECISION,
            pu_location_id INT,
            do_location_id INT,
            fare_amount DOUBLE PRECISION,
            tip_amount DOUBLE PRECISION,
            total_amount DOUBLE PRECISION,
            trip_minutes DOUBLE PRECISION,
            tip_rate DOUBLE PRECISION
        );

        CREATE TABLE IF NOT EXISTS warehouse.zone_daily_agg (
            pickup_date DATE,
            source TEXT,
            pu_location_id INT,
            trips INT,
            avg_distance DOUBLE PRECISION,
            avg_tip_rate DOUBLE PRECISION
        );
        """
        hook = PostgresHook(postgres_conn_id="warehouse_postgres")
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute(sql)
            conn.commit()
        return "ok"

    # ---------- Ingestion (parallel) ----------
    @task
    def locate_yellow() -> str:
        p = Path(YELLOW_PATH)
        if not p.exists():
            raise FileNotFoundError(f"Yellow parquet not found: {p}")
        return str(p)

    @task
    def locate_green() -> str:
        p = Path(GREEN_PATH)
        if not p.exists():
            raise FileNotFoundError(f"Green parquet not found: {p}")
        return str(p)

    # ---------- Transform (parallel) ----------
    def _clean_common(df: pd.DataFrame, source_name: str) -> pd.DataFrame:
        pickup_col = "tpep_pickup_datetime" if "tpep_pickup_datetime" in df.columns else "lpep_pickup_datetime"
        drop_col   = "tpep_dropoff_datetime" if "tpep_dropoff_datetime" in df.columns else "lpep_dropoff_datetime"

        df["pickup_ts"] = pd.to_datetime(df[pickup_col])
        df["dropoff_ts"] = pd.to_datetime(df[drop_col])
        df["trip_minutes"] = (df["dropoff_ts"] - df["pickup_ts"]).dt.total_seconds() / 60.0

        if "fare_amount" not in df.columns: df["fare_amount"] = 0.0
        if "tip_amount"  not in df.columns: df["tip_amount"]  = 0.0
        if "total_amount" not in df.columns: df["total_amount"] = df["fare_amount"] + df["tip_amount"]

        fare = df["fare_amount"].where(df["fare_amount"] > 0, other=1.0)
        df["tip_rate"] = (df["tip_amount"] / fare).clip(lower=0, upper=5)

        df["pu_location_id"] = df["PULocationID"].astype("Int64")
        df["do_location_id"] = df["DOLocationID"].astype("Int64")

        keep = [
            "pickup_ts","dropoff_ts","passenger_count","trip_distance",
            "pu_location_id","do_location_id","fare_amount","tip_amount","total_amount",
            "trip_minutes","tip_rate"
        ]
        out = df[keep].copy()
        out.insert(0, "source", source_name)
        out = out[(out["trip_minutes"] > 0) & (out["trip_minutes"] < 12*60)]
        out = out[(out["trip_distance"] >= 0) & (out["trip_distance"] < 200)]
        return out

    @task
    def transform_yellow(yellow_path: str) -> str:
        df = pd.read_parquet(yellow_path)
        clean = _clean_common(df, "yellow")
        outp = TMP_DIR / "yellow_clean.parquet"
        clean.to_parquet(outp, index=False)
        return str(outp)

    @task
    def transform_green(green_path: str) -> str:
        df = pd.read_parquet(green_path)
        clean = _clean_common(df, "green")
        outp = TMP_DIR / "green_clean.parquet"
        clean.to_parquet(outp, index=False)
        return str(outp)

    # ---------- Load ----------
    @task
    def merge_and_load(yellow_clean: str, green_clean: str) -> dict:
        y = pd.read_parquet(yellow_clean)
        g = pd.read_parquet(green_clean)
        trips = pd.concat([y, g], ignore_index=True)

        hook = PostgresHook(postgres_conn_id="warehouse_postgres")
        engine = hook.get_sqlalchemy_engine()
        trips.to_sql("taxi_trips_clean", engine, schema="warehouse", if_exists="append", index=False)

        df = trips.copy()
        df["pickup_date"] = pd.to_datetime(df["pickup_ts"]).dt.date
        agg = df.groupby(["pickup_date","source","pu_location_id"], dropna=False).agg(
            trips=("pu_location_id","size"),
            avg_distance=("trip_distance","mean"),
            avg_tip_rate=("tip_rate","mean"),
        ).reset_index()
        agg.to_sql("zone_daily_agg", engine, schema="warehouse", if_exists="append", index=False)

        return {"rows_trips": len(trips), "rows_agg": len(agg)}

    # ---------- Analysis (parallel) ----------
    @task
    def plot_top_zones() -> str:
        import matplotlib.pyplot as plt
        hook = PostgresHook(postgres_conn_id="warehouse_postgres")
        q = """
            SELECT pu_location_id, SUM(trips) AS trips
            FROM warehouse.zone_daily_agg
            GROUP BY pu_location_id
            ORDER BY trips DESC NULLS LAST
            LIMIT 15
        """
        df = pd.read_sql(q, hook.get_conn())
        outp = OUT_DIR / "top_zones.png"
        if df.empty:
            outp.write_text("No data")
            return str(outp)
        plt.figure(figsize=(10,6))
        df.plot(kind="bar", x="pu_location_id", y="trips", legend=False, rot=45)
        plt.title("Top Pickup Location IDs by Trips")
        plt.tight_layout(); plt.savefig(outp)
        return str(outp)

    @task
    def tiny_ml_report() -> str:
        from sklearn.linear_model import LinearRegression
        from sklearn.metrics import r2_score
        hook = PostgresHook(postgres_conn_id="warehouse_postgres")
        q = """
            SELECT trip_distance, trip_minutes, tip_rate
            FROM warehouse.taxi_trips_clean
            WHERE trip_distance IS NOT NULL AND trip_minutes IS NOT NULL AND tip_rate IS NOT NULL
            LIMIT 50000
        """
        df = pd.read_sql(q, hook.get_conn())
        p = OUT_DIR / "ml_report.txt"
        if df.empty:
            p.write_text("No rows available for ML.")
            return str(p)
        X = df[["trip_distance","trip_minutes"]].values
        y = df["tip_rate"].values
        model = LinearRegression().fit(X, y)
        r2 = r2_score(y, model.predict(X))
        p.write_text(
            "Linear Regression: tip_rate ~ trip_distance + trip_minutes\n"
            f"R^2 (in-sample): {r2:.4f}\n"
            f"coef_distance: {model.coef_[0]:.4f}\n"
            f"coef_minutes:  {model.coef_[1]:.4f}\n"
            f"intercept:     {model.intercept_:.4f}\n"
        )
        return str(p)

    # ---------- Cleanup ----------
    @task
    def cleanup(yellow_clean: str, green_clean: str) -> str:
        for p in [yellow_clean, green_clean]:
            if p and os.path.exists(p):
                try: os.remove(p)
                except Exception: pass
        return "cleaned"


    # ===== Wiring (parallel where possible) =====
    init = init_warehouse()

    # ingest in parallel
    y_path = locate_yellow()
    g_path = locate_green()

    # transform in parallel
    y_clean = transform_yellow(y_path)
    g_clean = transform_green(g_path)

    load = merge_and_load(y_clean, g_clean)

    # analysis in parallel after load
    plot = plot_top_zones()
    ml   = tiny_ml_report()

    cln  = cleanup(y_clean, g_clean)

    # dependency graph
    start >> init >> [y_path, g_path]
    [y_clean, g_clean] >> load >> [plot, ml] >> cln >> end