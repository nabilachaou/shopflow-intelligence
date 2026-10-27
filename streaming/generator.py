import argparse, json, time, uuid
from datetime import datetime, timedelta
import numpy as np
import pandas as pd

p = argparse.ArgumentParser()
p.add_argument("--servers", default="localhost:9092")
p.add_argument("--topic", default="orders_raw")
p.add_argument("--rate", type=float, default=2.0, help="commandes/seconde (moyenne)")
p.add_argument("--limit", type=int, default=0, help="0 = infini")
p.add_argument("--burst-factor", type=float, default=1.0, help="multiplicateur de débit pendant un pic")
p.add_argument("--burst-every", type=float, default=60.0, help="secondes entre deux pics")
p.add_argument("--burst-duration", type=float, default=10.0, help="durée d'un pic en secondes")
p.add_argument("--seed", type=int, default=None)
p.add_argument("--dry-run", action="store_true", help="affiche sans envoyer à Kafka")
args = p.parse_args()

rng = np.random.default_rng(args.seed)

# --- Données de référence (exportées depuis Databricks) ---
cust = pd.read_csv("data/ref_customers.csv", dtype={"customer_zip": str})
sell = pd.read_csv("data/ref_sellers.csv", dtype={"seller_zip": str})
prod = pd.read_csv("data/ref_products.csv").dropna(subset=["avg_price"])
zips = pd.read_csv("data/zip_coords.csv", dtype={"zip": str}).set_index("zip")

# --- Hypothèses (NON calibrées sur Olist, à remplacer par des valeurs mesurées) ---
STATE_SHARE = {"SP": 42, "RJ": 13, "MG": 12, "RS": 5.5, "PR": 5, "SC": 3.7, "BA": 3.4}
ITEM_COUNTS, ITEM_PROBS = [1, 2, 3, 4], [0.88, 0.08, 0.03, 0.01]
SELLER_SP_WEIGHT = 3.0   # les vendeurs de SP sont sur-représentés dans Olist

# Pondération des clients : part de l'État répartie sur ses zips
n_zip_state = cust.groupby("customer_state")["customer_zip"].transform("count")
w_c = cust["customer_state"].map(STATE_SHARE).fillna(1.0) / n_zip_state
w_c = (w_c / w_c.sum()).values

w_s = np.where(sell["seller_state"] == "SP", SELLER_SP_WEIGHT, 1.0)
w_s = w_s / w_s.sum()

def haversine(lat1, lng1, lat2, lng2):
    r = 6371.0
    a = np.radians
    dlat, dlng = a(lat2 - lat1), a(lng2 - lng1)
    h = np.sin(dlat / 2) ** 2 + np.cos(a(lat1)) * np.cos(a(lat2)) * np.sin(dlng / 2) ** 2
    return 2 * r * np.arcsin(np.sqrt(h))

def estimate_days(c_zip, s_zips):
    """Délai estimé fictif : base + distance + bruit. Hypothèse, pas la formule d'Olist."""
    dists = []
    for sz in s_zips:
        if c_zip in zips.index and sz in zips.index:
            c, s = zips.loc[c_zip], zips.loc[sz]
            dists.append(haversine(c.lat, c.lng, s.lat, s.lng))
    d = max(dists) if dists else None
    days = (14 + d / 60) if d is not None else 24
    return int(np.clip(round(days + rng.normal(0, 3)), 3, 100))

def make_order(now):
    c = cust.iloc[rng.choice(len(cust), p=w_c)]
    n = int(rng.choice(ITEM_COUNTS, p=ITEM_PROBS))
    items, s_zips = [], []
    for i in range(1, n + 1):
        pr = prod.iloc[rng.integers(len(prod))]
        se = sell.iloc[rng.choice(len(sell), p=w_s)]
        s_zips.append(se["seller_zip"])
        freight = pr["avg_freight"] if pd.notna(pr["avg_freight"]) else 15.0
        items.append({
            "order_item_id": i,
            "product_id": pr["product_id"],
            "seller_id": se["seller_id"],
            "price": round(float(pr["avg_price"]) * float(rng.uniform(0.9, 1.1)), 2),
            "freight_value": round(float(freight) * float(rng.uniform(0.9, 1.1)), 2)})
    days = estimate_days(c["customer_zip"], s_zips)
    return {
        "order_id": uuid.uuid4().hex,
        "purchase_ts": now.strftime("%Y-%m-%d %H:%M:%S"),
        "customer_zip": c["customer_zip"],
        "customer_state": c["customer_state"],
        "estimated_delivery_date": (now + timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S"),
        "items": items}

producer = None
if not args.dry_run:
    from kafka import KafkaProducer
    producer = KafkaProducer(
        bootstrap_servers=args.servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"), acks="all")

start, sent = time.time(), 0
while args.limit == 0 or sent < args.limit:
    elapsed = time.time() - start
    in_burst = (elapsed % args.burst_every) < args.burst_duration and args.burst_factor > 1
    rate = args.rate * (args.burst_factor if in_burst else 1.0)

    order = make_order(datetime.now())
    if producer:
        producer.send(args.topic, key=order["order_id"], value=order)
    else:
        print(json.dumps(order, ensure_ascii=False))
    sent += 1
    if sent % 50 == 0:
        print(f"{sent} commandes | débit {rate:.1f}/s {'(PIC)' if in_burst else ''}", flush=True)
    time.sleep(rng.exponential(1.0 / rate))     # arrivées de type Poisson

if producer:
    producer.flush()
print(f"Terminé : {sent} commandes")
