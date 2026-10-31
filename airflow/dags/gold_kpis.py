from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

L = "/opt/shopflow/lakehouse/"

def compute_kpis(**ctx):
    import pandas as pd
    from deltalake import DeltaTable, write_deltalake
    g = DeltaTable(L + "gold_order_scores").to_pandas()
    now = pd.Timestamp.now()
    by_state = (g.groupby("customer_state")
                .agg(orders=("order_id", "count"),
                     at_risk=("risk_flag", "sum"),
                     avg_score=("risk_score", "mean"),
                     avg_price=("total_price", "mean"))
                .reset_index())
    by_state["risk_rate"] = by_state["at_risk"] / by_state["orders"]
    by_state["computed_at"] = now
    by_state = by_state.reset_index(drop=True)
    import pyarrow as pa
    write_deltalake(L + "gold_kpis_by_state", pa.Table.from_pandas(by_state, preserve_index=False),
                    mode="overwrite", schema_mode="overwrite")
    print(f"{len(by_state)} États, {len(g)} commandes")

with DAG("gold_kpis", start_date=datetime(2026, 10, 1),
         schedule=timedelta(hours=1), catchup=False,
         default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
         tags=["shopflow"]) as dag:
    PythonOperator(task_id="compute_kpis", python_callable=compute_kpis)
