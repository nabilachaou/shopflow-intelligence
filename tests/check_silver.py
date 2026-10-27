import numpy as np, pandas as pd
from deltalake import DeltaTable

s = DeltaTable("lakehouse/silver_orders_features").to_pandas()
b = DeltaTable("lakehouse/bronze_orders_raw").to_pandas().drop_duplicates("order_id")
prod = pd.read_csv("data/ref_products.csv").set_index("product_id")
sell = pd.read_csv("data/ref_sellers.csv", dtype={"seller_zip": str}).set_index("seller_id")
zips = pd.read_csv("data/zip_coords.csv", dtype={"zip": str}).set_index("zip")

def hav(a, b_):
    r = 6371.0; f = np.radians
    dl, dg = f(b_[0]-a[0]), f(b_[1]-a[1])
    h = np.sin(dl/2)**2 + np.cos(f(a[0]))*np.cos(f(b_[0]))*np.sin(dg/2)**2
    return 2*r*np.arcsin(np.sqrt(h))

rows = []
for _, o in b.iterrows():
    its = list(o["items"])
    d = []
    for it in its:
        sz = sell.loc[it["seller_id"], "seller_zip"]
        if o["customer_zip"] in zips.index and sz in zips.index:
            c, s_ = zips.loc[o["customer_zip"]], zips.loc[sz]
            d.append(hav((c.lat, c.lng), (s_.lat, s_.lng)))
    w = [prod.loc[i["product_id"], "weight_g"] for i in its]
    rows.append({"order_id": o["order_id"],
                 "p_items": len(its),
                 "p_price": round(sum(i["price"] for i in its), 2),
                 "p_sellers": len({i["seller_id"] for i in its}),
                 "p_dist": round(float(np.mean(d)), 1) if d else np.nan,
                 "p_weight": round(float(np.nanmean(w)), 2)})
ref = pd.DataFrame(rows)
m = s.merge(ref, on="order_id")
print("Commandes comparées :", len(m))
print("item_count écart max :", (m.item_count - m.p_items).abs().max())
print("total_price écart max :", (m.total_price - m.p_price).abs().max())
print("seller_count écart max :", (m.seller_count - m.p_sellers).abs().max())
print("distance écart max (km) :", (m.avg_distance_km - m.p_dist).abs().max())
print("poids écart max (g) :", (m.avg_product_weight_g - m.p_weight).abs().max())
