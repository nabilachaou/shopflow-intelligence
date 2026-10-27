import joblib
import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score, roc_auc_score

model = joblib.load("models/model_v2.joblib")
df = pd.read_csv("data/test_replay.csv")

FEATURES = [
    "purchase_day_of_week", "purchase_hour",
    "item_count", "total_price", "total_freight",
    "seller_count", "seller_state_count", "same_state_seller_ratio",
    "avg_product_weight_g", "max_product_weight_g",
    "avg_product_length_cm", "avg_product_height_cm", "avg_product_width_cm",
    "customer_state", "estimated_delivery_days",
    "avg_distance_km", "distance_missing"]

print("Colonnes attendues par le modèle :",
      list(getattr(model, "feature_names_in_", [])) or "non disponible")

num_cols = [c for c in FEATURES if c != "customer_state"]
X = df[FEATURES].copy()
X[num_cols] = X[num_cols].astype("float64")

p = model.predict_proba(X)[:, 1]
y = df["is_late"].values

print("Lignes :", len(df), "(attendu 18603)")
print("PR-AUC :", round(average_precision_score(y, p), 4), "(attendu 0.2198)")
print("ROC-AUC :", round(roc_auc_score(y, p), 4), "(attendu 0.7877)")
