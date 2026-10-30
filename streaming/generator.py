import argparse, json, time, uuid
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
try:
    import orjson
    dumps = orjson.dumps
except ImportError:
    dumps = lambda o: json.dumps(o).encode("utf-8")

p = argparse.ArgumentParser()
p.add_argument("--servers", default="localhost:9092")
p.add_argument("--topic", default="orders_raw")
p.add_argument("--rate", type=float, default=2000.0, help="commandes/s (0 = maximum)")
p.add_argument("--limit", type=int, default=0, help="0 = infini")
p.add_argument("--batch", type=int, default=500)
p.add_argument("--burst-factor", type=float, default=1.0)
p.add_argument("--burst-every", type=float, default=60.0)
p.add_argument("--burst-duration", type=float, default=10.0)
p.add_argument("--seed", type=int, default=None)
p.add_argument("--dry-run", action="store_true", help="génère sans envoyer, mesure le débit")
args = p.parse_args()
rng = np.random.default_rng(args.seed)

# --- Références (exportées depuis Databricks) ---
cust = pd.read_csv("data/ref_customers.csv", dtype={"customer_zip": str})
sell = pd.read_csv("data/ref_sellers.csv", dtype={"seller_zip": str})
prod = pd.read_csv("data/ref_products.csv").dropna(subset=["avg_price"])
zips = pd.read_csv("data/zip_coords.csv", dtype={"zip": str}).set_index("zip")

# --- Hypothèses (NON calibrées sur Olist) ---
STATE_SHARE = {"SP": 42, "RJ": 13, "MG": 12, "RS": 5.5, "PR": 5, "SC": 3.7, "BA": 3.4}
ITEM_COUNTS, ITEM_PROBS = [1, 2, 3, 4], [0.88, 0.08, 0.03, 0.01]
SELLER_SP_WEIGHT = 3.0

n_zip = cust.groupby("customer_state")["customer_zip"].transform("count")
w_c = cust["customer_state"].map(STATE_SHARE).fillna(1.0) / n_zip
w_c = (w_c / w_c.sum()).values
w_s = np.where(sell["seller_state"] == "SP", SELLER_SP_WEIGHT, 1.0)
w_s = w_s / w_s.sum()

# Tableaux numpy pré-calculés (une seule fois)
c_zip, c_state = cust["customer_zip"].values, cust["customer_state"].values
c_lat = zips["lat"].reindex(c_zip).values
c_lng = zips["lng"].reindex(c_zip).values
s_id, s_lat = sell["seller_id"].values, zips["lat"].reindex(sell["seller_zip"]).values
s_lng = zips["lng"].reindex(sell["seller_zip"]).values
p_id = prod["product_id"].values
p_price = prod["avg_price"].values.astype(float)
p_fr = prod["avg_freight"].fillna(15.0).values.astype(float)

def haversine(lat1, lng1, lat2, lng2):
    r, a = 6371.0, np.radians
    h = (np.sin(a(lat2 - lat1) / 2) ** 2 +
         np.cos(a(lat1)) * np.cos(a(lat2)) * np.sin(a(lng2 - lng1) / 2) ** 2)
    return 2 * r * np.arcsin(np.sqrt(h))

FMT = "%Y-%m-%d %H:%M:%S"

def make_batch(B, now):
    ci = rng.choice(len(cust), size=B, p=w_c)
    n = rng.choice(ITEM_COUNTS, size=B, p=ITEM_PROBS)
    T = int(n.sum())
    oi = np.repeat(np.arange(B), n)
    starts = np.cumsum(n) - n
    pi = rng.integers(len(prod), size=T)
    si = rng.choice(len(sell), size=T, p=w_s)
    price = np.round(p_price[pi] * rng.uniform(0.9, 1.1, T), 2).tolist()
    freight = np.round(p_fr[pi] * rng.uniform(0.9, 1.1, T), 2).tolist()
    d = haversine(c_lat[ci][oi], c_lng[ci][oi], s_lat[si], s_lng[si])
    dmax = np.maximum.reduceat(np.nan_to_num(d, nan=-1.0), starts)
    days = np.where(dmax >= 0, 14 + dmax / 60, 24) + rng.normal(0, 3, B)
    days = np.clip(np.round(days), 3, 100).astype(int).tolist()
    pid, sid = p_id[pi].tolist(), s_id[si].tolist()
    zl, stl = c_zip[ci].tolist(), c_state[ci].tolist()
    nl, sl = n.tolist(), starts.tolist()
    ts, dates, out = now.strftime(FMT), {}, []
    for k in range(B):
        dd = days[k]
        if dd not in dates:
            dates[dd] = (now + timedelta(days=dd)).strftime(FMT)
        a = sl[k]
        out.append({
            "order_id": uuid.uuid4().hex, "purchase_ts": ts,
            "customer_zip": zl[k], "customer_state": stl[k],
            "estimated_delivery_date": dates[dd],
            "items": [{"order_item_id": j - a + 1, "product_id": pid[j], "seller_id": sid[j],
                       "price": price[j], "freight_value": freight[j]}
                      for j in range(a, a + nl[k])]})
    return out

producer = None
if not args.dry_run:
    from kafka import KafkaProducer
    producer = KafkaProducer(bootstrap_servers=args.servers, acks=1,
                             linger_ms=20, batch_size=262144,
                             buffer_memory=134217728)

start, sent, next_t = time.time(), 0, time.time()
while args.limit == 0 or sent < args.limit:
    B = args.batch if args.limit == 0 else min(args.batch, args.limit - sent)
    elapsed = time.time() - start
    burst = args.burst_factor > 1 and (elapsed % args.burst_every) < args.burst_duration
    rate = args.rate * (args.burst_factor if burst else 1.0)
    orders = make_batch(B, datetime.now())
    for o in orders:
        payload = dumps(o)
        if producer:
            producer.send(args.topic, key=o["order_id"].encode(), value=payload)
    if args.dry_run and sent == 0:
        print(dumps(orders[0]).decode())
    sent += B
    if rate > 0:
        next_t += B / rate
        wait = next_t - time.time()
        if wait > 0:
            time.sleep(wait)
    if sent % (args.batch * 20) < B:
        print(f"{sent:,} commandes | {sent / (time.time() - start):.0f}/s mesuré"
              f"{' (PIC)' if burst else ''}", flush=True)

if producer:
    producer.flush()
dur = time.time() - start
print(f"Terminé : {sent:,} commandes en {dur:.1f} s = {sent / dur:.0f} commandes/s mesurées")
