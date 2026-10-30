import json, os, time
import joblib
import pandas as pd
from deltalake import DeltaTable, write_deltalake

FEATURES = [
    "purchase_day_of_week", "purchase_hour",
    "item_count", "total_price", "total_freight",
    "seller_count", "seller_state_count", "same_state_seller_ratio",
    "avg_product_weight_g", "max_product_weight_g",
    "avg_product_length_cm", "avg_product_height_cm", "avg_product_width_cm",
    "customer_state", "estimated_delivery_days",
    "avg_distance_km", "distance_missing"]
NUM = [c for c in FEATURES if c != "customer_state"]

SILVER = "lakehouse/silver_orders_features"
GOLD = "lakehouse/gold_order_scores"
THR_FILE = "models/threshold.json"

model = joblib.load("models/model_v2.joblib")

def load_silver():
    return DeltaTable(SILVER).to_pandas().drop_duplicates("order_id")

def scored_ids():
    if not os.path.exists(GOLD + "/_delta_log"):
        return set()
    return set(DeltaTable(GOLD).to_pandas()["order_id"])

def score(df):
    X = df[FEATURES].copy()
    X[NUM] = X[NUM].astype("float64")
    return model.predict_proba(X)[:, 1]

def get_threshold():
    """Seuil = 80e centile des premiers scores (calibration), puis figé."""
    if os.path.exists(THR_FILE):
        return json.load(open(THR_FILE))["p80"]
    s = load_silver()
    if len(s) < 100:
        return None
    thr = float(pd.Series(score(s)).quantile(0.80))
    json.dump({"p80": thr, "n_calibration": len(s)}, open(THR_FILE, "w"))
    print(f"Seuil calibré sur {len(s)} commandes : {thr:.4f}", flush=True)
    return thr

print("Scoring continu démarré", flush=True)
while True:
    try:
        if not os.path.exists(SILVER + "/_delta_log"):
            time.sleep(5); continue
        thr = get_threshold()
        if thr is None:
            time.sleep(5); continue
        new = load_silver()
        new = new[~new["order_id"].isin(scored_ids())]
        if len(new):
            new = new.copy()
            new["risk_score"] = score(new)
            new["risk_flag"] = (new["risk_score"] >= thr).astype(int)
            new["scored_at"] = pd.Timestamp.now()
            out = new[["order_id", "purchase_ts", "customer_state",
                       "estimated_delivery_days", "avg_distance_km", "total_price",
                       "risk_score", "risk_flag", "scored_at"]]
            out = out.reset_index(drop=True)
            write_deltalake(GOLD, out, mode="append")
            print(f"{len(out)} commandes scorées | à risque : {int(out.risk_flag.sum())}", flush=True)
    except Exception as e:
        print("Erreur (nouvelle tentative) :", repr(e), flush=True)
    time.sleep(10)
