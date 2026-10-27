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
num_cols = [c for c in FEATURES if c != "customer_state"]

model = joblib.load("models/model_v2.joblib")
df = DeltaTable("lakehouse/silver_orders_features").to_pandas().drop_duplicates("order_id")
print("Commandes lues :", len(df))

X = df[FEATURES].copy()
X[num_cols] = X[num_cols].astype("float64")

df["risk_score"] = model.predict_proba(X)[:, 1]
df["risk_rank_pct"] = df["risk_score"].rank(pct=True)
df["risk_flag"] = (df["risk_rank_pct"] >= 0.80).astype(int)
df["scored_at"] = pd.Timestamp.now()

out = df[["order_id", "purchase_ts", "customer_state",
          "estimated_delivery_days", "avg_distance_km", "total_price",
          "risk_score", "risk_rank_pct", "risk_flag", "scored_at"]]
write_deltalake(
    "lakehouse/gold_order_scores",
    out,
    mode="overwrite",
    schema_mode="overwrite"
)
print("Score moyen :", round(out.risk_score.mean(), 4))
print("Part signalée :", round(out.risk_flag.mean() * 100, 1), "%")
print(out.sort_values("risk_score", ascending=False).head(5)
      [["order_id", "customer_state", "estimated_delivery_days", "avg_distance_km", "risk_score"]]
      .to_string(index=False))
