import altair as alt
import pandas as pd
import streamlit as st
from deltalake import DeltaTable
import theme

st.set_page_config(page_title="ShopFlow Intelligence", page_icon="📦", layout="wide")
theme.apply()
theme.header()

L = "lakehouse/"
BLUE, CYAN, GREEN, ORANGE, RED = "#3B82F6", "#22D3EE", "#10B981", "#F59E0B", "#EF4444"

def read(name):
    try:
        return DeltaTable(L + name).to_pandas()
    except Exception:
        return None

def utc(s):
    return pd.to_datetime(s, utc=True, errors="coerce")

tabs = st.tabs(["Pipeline", "Risque de retard", "Qualité des données", "Modèle"])

@st.fragment(run_every=10)
def live_pipeline():
    b, s, g = read("bronze_orders_raw"), read("silver_orders_features"), read("gold_order_scores")
    if b is None:
        st.info("En attente du flux de commandes.")
        return
    nb, ns, ng = len(b), len(s) if s is not None else 0, len(g) if g is not None else 0
    c = st.columns(4)
    theme.kpi(c[0], "Bronze", f"{nb:,}", BLUE)
    theme.kpi(c[1], "Silver", f"{ns:,}", CYAN)
    theme.kpi(c[2], "Gold (scorées)", f"{ng:,}", GREEN)
    t = utc(b["kafka_ts"]).dropna()
    last = int((t > t.max() - pd.Timedelta(minutes=1)).sum()) if len(t) else 0
    theme.kpi(c[3], "Commandes / dernière minute", f"{last:,}", ORANGE)
    left, right = st.columns([3, 2])
    with left:
        theme.section("Débit d'arrivée (commandes par minute)")
        if len(t):
            st.area_chart(t.dt.floor("min").value_counts().sort_index().tail(30), color=CYAN)
    with right:
        theme.section("Latences (secondes)")
        rows = []
        d = (utc(b["ingested_at"]) - utc(b["kafka_ts"])).dt.total_seconds().clip(lower=0).dropna()
        if len(d):
            rows.append({"étape": "Kafka → Bronze", "médiane": d.median(), "p95": d.quantile(0.95)})
        if g is not None and len(g):
            d = (utc(g["scored_at"]) - utc(g["purchase_ts"])).dt.total_seconds().clip(lower=0).dropna()
            if len(d):
                rows.append({"étape": "Génération → score", "médiane": d.median(), "p95": d.quantile(0.95)})
        if rows:
            st.dataframe(pd.DataFrame(rows).round(1), hide_index=True)

@st.fragment(run_every=10)
def live_risk():
    g = read("gold_order_scores")
    if g is None or not len(g):
        st.info("Aucune commande scorée pour l'instant.")
        return
    g = g.sort_values("scored_at", ascending=False)
    r = g.head(500)
    c = st.columns(4)
    theme.kpi(c[0], "Commandes scorées", f"{len(g):,}", BLUE)
    theme.kpi(c[1], "Risque élevé", f"{int(g.risk_flag.sum()):,}", RED)
    theme.kpi(c[2], "Part à risque (500 dernières)", f"{r.risk_flag.mean() * 100:.1f} %", ORANGE)
    theme.kpi(c[3], "Score moyen", f"{r.risk_score.mean():.3f}", CYAN)
    left, right = st.columns(2)
    with left:
        theme.section("Part à risque par État")
        by = (r.groupby("customer_state").agg(n=("order_id", "count"), part=("risk_flag", "mean"))
              .query("n >= 5").sort_values("part", ascending=False))
        st.bar_chart(by["part"], color=ORANGE)
    with right:
        theme.section("Score moyen selon le délai estimé (jours)")
        labels = ["0-12", "12-18", "18-24", "24-30", "30-40", "40+"]
        bins = pd.cut(r["estimated_delivery_days"], [0, 12, 18, 24, 30, 40, 200], labels=labels)
        st.bar_chart(r.groupby(bins, observed=True)["risk_score"].mean(), color=BLUE)
    theme.section("Commandes à risque récentes")
    st.dataframe(r[r.risk_flag == 1][["order_id", "customer_state", "estimated_delivery_days",
                                      "avg_distance_km", "total_price", "risk_score"]].head(12),
                 hide_index=True, width="stretch")

OLIST = {"estimated_delivery_days": 25.2, "avg_distance_km": 612.8, "total_price": 136.1}

@st.fragment(run_every=15)
def live_quality():
    b, s, g = read("bronze_orders_raw"), read("silver_orders_features"), read("gold_order_scores")
    if s is None or not len(s):
        st.info("Silver n'existe pas encore.")
        return
    theme.section("Intégrité des couches")
    st.dataframe(pd.DataFrame([{"couche": n, "lignes": len(d), "doublons": len(d) - d["order_id"].nunique()}
                               for n, d in [("Bronze", b), ("Silver", s), ("Gold", g)] if d is not None]),
                 hide_index=True, width="stretch")
    left, right = st.columns(2)
    with left:
        theme.section("Valeurs manquantes (%)")
        nulls = (s.isna().mean() * 100).round(2)
        nulls = nulls[nulls > 0]
        if len(nulls):
            st.dataframe(nulls.rename("% nul"), width="stretch")
        else:
            st.success("Aucune valeur manquante")
    with right:
        theme.section("Flux généré vs Olist")
        st.dataframe(pd.DataFrame([{"variable": k, "flux": round(float(s[k].mean()), 1), "Olist": v,
                                    "écart %": round((float(s[k].mean()) / v - 1) * 100, 1)}
                                   for k, v in OLIST.items()]), hide_index=True, width="stretch")

with tabs[0]:
    live_pipeline()
with tabs[1]:
    live_risk()
with tabs[2]:
    live_quality()

with tabs[3]:
    theme.section("Performance sur le test Olist")
    c = st.columns(4)
    theme.kpi(c[0], "PR-AUC", "0,220", BLUE)
    theme.kpi(c[1], "ROC-AUC", "0,788", CYAN)
    theme.kpi(c[2], "Top 10 % → retards captés", "38,8 %", ORANGE)
    theme.kpi(c[3], "Top 20 % → retards captés", "59,1 %", GREEN)
    left, right = st.columns(2)
    with left:
        theme.section("Comparaison des modèles (validation)")
        st.dataframe(pd.DataFrame({"modèle": ["Régression logistique", "Random Forest", "LightGBM"],
                                   "PR-AUC": [0.1736, 0.1784, 0.1469],
                                   "ROC-AUC": [0.7073, 0.7016, 0.6900]}),
                     hide_index=True, width="stretch")
    with right:
        theme.section("Poids du délai estimé (PR-AUC, validation)")
        abl = pd.DataFrame({"variante": ["Toutes", "Sans purchase_month", "Sans délai estimé", "Sans les deux"],
                            "PR-AUC": [0.1736, 0.1741, 0.1155, 0.1160]})
        st.bar_chart(abl.set_index("variante"), color=CYAN)
    theme.section("Dérive : taux de retard (%) selon le délai estimé")
    drift = pd.DataFrame({"délai": ["0-12", "12-18", "18-24", "24-30", "30-40", "40+"],
                          "train": [7.1, 7.4, 10.4, 9.2, 7.6, 3.5],
                          "validation": [7.2, 11.7, 11.6, 6.0, 3.2, 0.7],
                          "test": [16.5, 3.7, 3.3, 1.8, 0.9, 0.6]})
    long = drift.melt("délai", var_name="jeu", value_name="taux (%)")
    st.altair_chart(alt.Chart(long).mark_line(point=True, strokeWidth=3).encode(
        x=alt.X("délai:N", sort=list(drift["délai"]), title=None),
        y=alt.Y("taux (%):Q", title=None),
        color=alt.Color("jeu:N", scale=alt.Scale(domain=["train", "validation", "test"],
                                                 range=[BLUE, ORANGE, RED]), title=None)
    ).properties(height=300), width="stretch")
