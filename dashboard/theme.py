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
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
html, body, [class*="css"], .stApp {font-family:'Inter', sans-serif;}
.stApp {background: radial-gradient(1200px 500px at 85% -10%, #12305A55 0%, transparent 60%), #0B1220;}
#MainMenu, footer {visibility:hidden;}
header[data-testid="stHeader"] {background:transparent;}
.block-container {padding-top: 3.5rem; max-width: 1400px;}
.hero {display:flex; align-items:center; gap:18px; padding:20px 28px; border-radius:18px;
  background: linear-gradient(120deg, #0F1D3A 0%, #12305A 55%, #0E4A63 100%);
  border:1px solid #1F3A66; margin-bottom:22px; box-shadow:0 8px 30px rgba(0,0,0,.35);}
.hero .t {font-size:1.8rem; font-weight:700; color:#F4F8FF; line-height:1.1; letter-spacing:-.01em;}
.hero .s {color:#9FC2E8; font-size:.95rem; margin-top:5px;}
.live {margin-left:auto; color:#34D399; font-weight:600; font-size:.82rem; letter-spacing:.05em;
  border:1px solid #1E6B54; background:#0F2E27; padding:6px 14px; border-radius:20px;}
.live::before {content:""; display:inline-block; width:8px; height:8px; border-radius:50%;
  background:#34D399; margin-right:8px; animation:pulse 1.6s infinite;}
@keyframes pulse {0%{box-shadow:0 0 0 0 rgba(52,211,153,.6);} 70%{box-shadow:0 0 0 8px rgba(52,211,153,0);} 100%{box-shadow:0 0 0 0 rgba(52,211,153,0);}}
.kpi {background:linear-gradient(180deg,#131E36 0%,#0F1829 100%); border:1px solid #1E2A44;
  border-radius:14px; padding:16px 20px; transition:transform .15s, border-color .15s;}
.kpi:hover {transform:translateY(-2px); border-color:#2F4A7A;}
.kpi .kl {color:#8CA0C0; font-size:.74rem; text-transform:uppercase; letter-spacing:.08em; font-weight:500;}
.kpi .kv {color:#F4F8FF; font-size:2rem; font-weight:700; margin-top:4px; letter-spacing:-.02em;}
.kpi .ks {color:#6F86AA; font-size:.78rem; margin-top:2px;}
.sec {color:#E3EEFF; font-weight:600; font-size:1.02rem; margin:26px 0 10px 0;
  padding-left:12px; border-left:3px solid #22D3EE;}
.stTabs [data-baseweb="tab-list"] {gap:8px; border-bottom:1px solid #1E2A44; padding-bottom:0;}
.stTabs [data-baseweb="tab"] {background:transparent; border-radius:10px 10px 0 0; padding:10px 22px;
  color:#8CA0C0; font-weight:500;}
.stTabs [aria-selected="true"] {background:#16305A; color:#fff;}
[data-testid="stDataFrame"] {border:1px solid #1E2A44; border-radius:12px; overflow:hidden;}
</style>"""

def apply():
    st.markdown(CSS, unsafe_allow_html=True)

def header():
    st.markdown(
        f'<div class="hero">{LOGO}<div><div class="t">ShopFlow Intelligence</div>'
        '<div class="s">Risque de retard de livraison</div></div>'
        '<div class="live">EN DIRECT</div></div>', unsafe_allow_html=True)

def kpi(col, label, value, accent="#3B82F6", sub=""):
    s = f'<div class="ks">{sub}</div>' if sub else ""
    col.markdown(f'<div class="kpi" style="border-top:3px solid {accent}">'
                 f'<div class="kl">{label}</div><div class="kv">{value}</div>{s}</div>',
                 unsafe_allow_html=True)

def section(title):
    st.markdown(f'<div class="sec">{title}</div>', unsafe_allow_html=True)
