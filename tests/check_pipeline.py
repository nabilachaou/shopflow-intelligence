import joblib
import numpy as np
import pandas as pd
from deltalake import DeltaTable

FEATURES = [
    "purchase_day_of_week", "purchase_hour",
    "item_count", "total_price", "total_freight",
    "seller_count", "seller_state_count", "same_state_seller_ratio",
    "avg_product_weight_g", "max_product_weight_g",
    "avg_product_length_cm", "avg_product_height_cm", "avg_product_width_cm",
    "customer_state", "estimated_delivery_days",
    "avg_distance_km", "distance_missing"]
num_cols = [c for c in FEATURES if c != "customer_state"]

model = joblib.load("models/model_v2.joblib")

# Référence : CSV d'origine
ref = pd.read_csv("data/test_replay.csv")
Xr = ref[FEATURES].copy(); Xr[num_cols] = Xr[num_cols].astype("float64")
ref["p_ref"] = model.predict_proba(Xr)[:, 1]

# Pipeline : scores écrits dans la table Gold
gold = DeltaTable("lakehouse/gold_order_scores").to_pandas()
m = gold.merge(ref[["order_id", "p_ref"]], on="order_id", how="left")

print("Commandes comparées :", len(m), "| sans correspondance :", int(m.p_ref.isna().sum()))
print("Écart max |score pipeline - score référence| :", float((m.risk_score - m.p_ref).abs().max()))
