import streamlit as st

LOGO = ('<svg width="54" height="54" viewBox="0 0 48 48" xmlns="http://www.w3.org/2000/svg">'
        '<defs><linearGradient id="g" x1="0" y1="0" x2="1" y2="1">'
        '<stop offset="0" stop-color="#2563EB"/><stop offset="1" stop-color="#22D3EE"/>'
        '</linearGradient></defs>'
        '<rect width="48" height="48" rx="12" fill="url(#g)"/>'
        '<g fill="none" stroke="#fff" stroke-width="2.2" stroke-linejoin="round" stroke-linecap="round">'
        '<path d="M24 10 L37 17 V31 L24 38 L11 31 V17 Z"/>'
        '<path d="M11 17 L24 24 L37 17"/><path d="M24 24 V38"/></g></svg>')

CSS = """<style>
#MainMenu, footer, header [data-testid="stToolbar"] {visibility: hidden;}
.block-container {padding-top: 1.5rem; max-width: 1400px;}
.hero {display:flex; align-items:center; gap:18px; padding:18px 26px; border-radius:16px;
  background: linear-gradient(120deg, #0F1D3A 0%, #12305A 55%, #0E4A63 100%);
  border:1px solid #1F3A66; margin-bottom:18px;}
.hero .t {font-size:1.7rem; font-weight:700; color:#F4F8FF; line-height:1.1;}
.hero .s {color:#9FC2E8; font-size:0.95rem; margin-top:4px;}
.live {margin-left:auto; color:#34D399; font-weight:600; font-size:0.85rem;
  border:1px solid #1E6B54; background:#0F2E27; padding:5px 12px; border-radius:20px;}
.kpi {background:#111A2E; border:1px solid #1E2A44; border-radius:12px; padding:14px 18px;}
.kpi .kl {color:#8CA0C0; font-size:0.78rem; text-transform:uppercase; letter-spacing:.06em;}
.kpi .kv {color:#F4F8FF; font-size:1.9rem; font-weight:700; margin-top:2px;}
.sec {color:#CFE0F7; font-weight:600; font-size:1.05rem; margin:18px 0 8px 0;
  padding-left:10px; border-left:3px solid #22D3EE;}
.stTabs [data-baseweb="tab-list"] {gap:6px;}
.stTabs [data-baseweb="tab"] {background:#111A2E; border-radius:10px 10px 0 0; padding:10px 20px;}
.stTabs [aria-selected="true"] {background:#16305A; color:#fff;}
</style>"""

def apply():
    st.markdown(CSS, unsafe_allow_html=True)

def header():
    st.markdown(
        f'<div class="hero">{LOGO}<div><div class="t">ShopFlow Intelligence</div>'
        '<div class="s">Risque de retard de livraison</div></div>'
        '<div class="live">● EN DIRECT</div></div>', unsafe_allow_html=True)

def kpi(col, label, value, accent="#3B82F6"):
    col.markdown(f'<div class="kpi" style="border-top:3px solid {accent}">'
                 f'<div class="kl">{label}</div><div class="kv">{value}</div></div>',
                 unsafe_allow_html=True)

def section(title):
    st.markdown(f'<div class="sec">{title}</div>', unsafe_allow_html=True)
