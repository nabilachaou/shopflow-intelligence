from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

L = "/opt/shopflow/lakehouse/"

def count_layers(**ctx):
    from deltalake import DeltaTable
    res = {}
    for n in ["bronze_orders_raw", "silver_orders_features", "gold_order_scores"]:
        df = DeltaTable(L + n).to_pandas()
        res[n] = {"rows": len(df), "dups": len(df) - df["order_id"].nunique()}
    print(res)
    ctx["ti"].xcom_push(key="res", value=res)

def check_rules(**ctx):
    res = ctx["ti"].xcom_pull(key="res", task_ids="count_layers")
    b, s, g = (res[k]["rows"] for k in ["bronze_orders_raw", "silver_orders_features", "gold_order_scores"])
    errors = []
    for k, v in res.items():
        if v["dups"] > 0:
            errors.append(f"{k}: {v['dups']} doublons")
    if b > 0 and (b - g) / b > 0.05:
        errors.append(f"Gold en retard de {b - g} commandes sur Bronze (>5 %)")
    if errors:
        raise ValueError("; ".join(errors))
    print("Contrôles OK :", res)

with DAG("data_quality", start_date=datetime(2026, 10, 1),
         schedule=timedelta(minutes=15), catchup=False,
         default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
         tags=["shopflow"]) as dag:
    c = PythonOperator(task_id="count_layers", python_callable=count_layers)
    r = PythonOperator(task_id="check_rules", python_callable=check_rules)
    c >> r
