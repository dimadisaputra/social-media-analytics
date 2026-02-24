import streamlit as st

# ═══════════════════════════════════════════════════════════════════════════════
#  CHART THEME AND COLORS
# ═══════════════════════════════════════════════════════════════════════════════
BG      = "#FFFFFF"
GRID    = "#F3F4F6"
FC      = "#9CA3AF"    # font colour

# Platform Colors (Updated per user request)
TIKTOK  = "#070008"
IG      = "#f6379c"

# Generic Colors
BLUE    = "#2563EB"
RED     = "#EF4444"
GREEN   = "#10B981"
AMBER   = "#F59E0B"

# Sentiment Colors
POS_C   = "#10B981"
NEG_C   = "#EF4444"
NEU_C   = "#F59E0B"

COLOR_MAP = {"tiktok": TIKTOK, "instagram": IG}

LAYOUT = dict(
    paper_bgcolor=BG, plot_bgcolor=BG,
    font=dict(family="Inter", color="#374151", size=12), # Darker font for clear labels
    margin=dict(l=0, r=0, t=20, b=20), # increased margin to prevent cropping
    xaxis=dict(gridcolor=GRID, zeroline=False, showline=False, color="#374151"), # Darker label text
    yaxis=dict(gridcolor=GRID, zeroline=False, showline=False, color="#374151"),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
                font=dict(color="#374151", size=11)),
    hoverlabel=dict(bgcolor="#1E293B", bordercolor="#334155",
                    font=dict(family="Inter", color="#F9FAFB", size=12)),
)

def L(**overrides):
    """Return a copy of LAYOUT with keys overridden."""
    return {**LAYOUT, **overrides}

# ═══════════════════════════════════════════════════════════════════════════════
#  GLOBAL CSS
# ═══════════════════════════════════════════════════════════════════════════════
def apply_theme():
    # Provide the CSS that forces light theme to avoid dark-on-dark. Includes fixes for Streamlit rendering.
    st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

html, body, [class*="css"] { font-family: 'Inter', sans-serif; color: #111827 !important; }
[data-testid="stAppViewContainer"] { background: #F4F6FA !important; }
[data-testid="stHeader"] { background: #FFFFFF !important; border-bottom: 1px solid #E5E7EB; }
section[data-testid="stSidebar"] { display: none; }
/* ── Page header ── */
.block-container { padding-top: 64px !important; padding-bottom: 56px !important; max-width: 1300px; }
.page-header { margin-bottom: 22px; }
.page-title  { font-size: 22px; font-weight: 700; color: #111827; letter-spacing: -0.02em; margin: 0; }
.page-title span { color: #2563EB; }
.page-sub    { font-size: 12px; color: #9CA3AF; margin-top: 3px; }

/* ── KPI cards ── */
.kpi-row { display: grid; grid-template-columns: repeat(4,1fr); gap: 14px; margin-bottom: 22px; }
.kpi-card {
    background: #FFFFFF; border: 1px solid #E5E7EB; border-radius: 12px;
    padding: 18px 20px; display: flex; align-items: flex-start; gap: 14px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.05);
}
.kpi-icon {
    width: 40px; height: 40px; border-radius: 10px; flex-shrink: 0;
    display: flex; align-items: center; justify-content: center; font-size: 18px;
}
.ic-blue   { background: #EFF6FF; }
.ic-rose   { background: #FFF1F2; }
.ic-amber  { background: #FFFBEB; }
.ic-emerald{ background: #ECFDF5; }
.kpi-label { font-size: 11px; font-weight: 500; color: #6B7280; text-transform: uppercase; letter-spacing: .06em; margin-bottom: 5px; }
.kpi-value { font-size: 26px; font-weight: 700; color: #111827; line-height: 1; margin-bottom: 3px; }
.kpi-sub   { font-size: 11px; color: #9CA3AF; }

/* ── Generic white card ── */
.card { background: #FFFFFF; border: 1px solid #E5E7EB; border-radius: 12px; padding: 20px 20px 14px; margin-bottom: 14px; box-shadow: 0 1px 3px rgba(0,0,0,0.05); }
.card-title { font-size: 13px; font-weight: 600; color: #111827; margin-bottom: 14px; }
.divider { height: 1px; background: #E5E7EB; margin: 24px 0; }
.section-title { font-size: 18px; font-weight: 700; color: #111827; margin: 32px 0 16px; display: flex; align-items: center; gap: 8px;}

/* ── Sentiment summary pill row ── */
.sent-row { display: flex; gap: 10px; margin-bottom: 16px; flex-wrap: wrap; }
.sent-pill {
    display: flex; align-items: center; gap: 6px; padding: 6px 14px;
    border-radius: 30px; font-size: 12px; font-weight: 600; border: 1px solid transparent;
}
.sp-pos { background: #F0FDF4; color: #16A34A; border-color: #BBF7D0; }
.sp-neu { background: #FFFBEB; color: #D97706; border-color: #FDE68A; }
.sp-neg { background: #FEF2F2; color: #DC2626; border-color: #FECACA; }
.sp-dot { width: 8px; height: 8px; border-radius: 50%; }
.sp-pos .sp-dot { background: #16A34A; }
.sp-neu .sp-dot { background: #D97706; }
.sp-neg .sp-dot { background: #DC2626; }

/* ── Post list item ── */
.post-list-item {
    background: #FFFFFF; border: 1px solid #E5E7EB; border-radius: 12px;
    padding: 16px; margin-bottom: 12px; display: flex; gap: 16px;
    text-decoration: none !important; color: inherit; transition: all .18s;
    box-shadow: 0 1px 3px rgba(0,0,0,0.05); align-items: center;
}
.post-list-item:hover { transform: translateX(4px); box-shadow: 0 4px 12px rgba(37,99,235,.08); border-color: #BFDBFE; }
.post-rank { font-size: 24px; font-weight: 800; color: #D1D5DB; min-width: 32px; text-align: right; }
.post-thumb { width: 80px; height: 100px; object-fit: cover; border-radius: 8px; background: #F3F4F6; flex-shrink: 0; }
.post-thumb-ph { width: 80px; height: 100px; border-radius: 8px; background: linear-gradient(135deg,#EFF6FF 0%,#F4F6FA 100%); display: flex; align-items: center; justify-content: center; font-size: 28px; flex-shrink: 0; }
.post-meta-content { flex: 1; display: flex; flex-direction: column; justify-content: center;}
.badge { display: inline-block; font-size: 9px; font-weight: 600; letter-spacing: .08em; text-transform: uppercase; padding: 2px 7px; border-radius: 4px; margin-bottom: 6px; }
.badge-tiktok    { background: #E0F7FA; color: #00838F; } /* Changed to black color in app if needed, but styling stays neat */
.badge-instagram { background: #FCE4EC; color: #AD1457; }
.post-desc { font-size: 13px; color: #4B5563; line-height: 1.5; margin-bottom: 8px; display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; }
.post-stats { display: flex; gap: 16px; flex-wrap: wrap; }
.ps { font-size: 12px; color: #6B7280; }
.ps strong { color: #111827; font-weight: 600; }
.sb { display: inline-block; font-size: 10px; font-weight: 600; padding: 2px 8px; border-radius: 10px; margin-left: 8px; vertical-align: middle; }
.sb-positive { background: #D1FAE5; color: #065F46; }
.sb-neutral  { background: #FEF3C7; color: #92400E; }
.sb-negative { background: #FEE2E2; color: #991B1B; }

/* ── Streamlit widget overrides ── */
p, span, div, label, input, textarea, select { color: #111827 !important; }
[data-testid="stAppViewBlockContainer"], [data-testid="block-container"], .main { background: #F4F6FA !important; }

/* Widget backgrounds */
div[data-baseweb="select"] > div, div[data-baseweb="select"] div[class*="ValueContainer"], div[data-baseweb="select"] div[class*="control"], input[type="date"], [data-testid="stDateInput"] input { background: #FFFFFF !important; border-color: #E5E7EB !important; color: #111827 !important; }
ul[data-baseweb="menu"], div[data-baseweb="popover"] > div, div[role="listbox"] { background: #FFFFFF !important; border: 1px solid #E5E7EB !important; box-shadow: 0 4px 16px rgba(0,0,0,0.08) !important; }
li[role="option"], div[role="option"] { background: #FFFFFF !important; color: #111827 !important; }
li[role="option"]:hover, div[role="option"]:hover, li[aria-selected="true"], div[aria-selected="true"] { background: #EFF6FF !important; color: #2563EB !important; }

/* Text colors */
div[data-baseweb="select"] span, div[data-baseweb="select"] div { color: #111827 !important; }
[data-testid="stMultiSelect"] span[data-baseweb="tag"] { background: #EFF6FF !important; color: #2563EB !important; border: 1px solid #BFDBFE !important; }
[data-testid="stRadio"] label, [data-testid="stRadio"] div[role="radiogroup"] { color: #374151 !important; }

/* Labels */
label[data-testid="stWidgetLabel"] p, [data-testid="stWidgetLabel"] p { font-size: 11px !important; color: #6B7280 !important; font-weight: 600 !important; text-transform: uppercase !important; letter-spacing: .04em !important; }
[data-testid="stCaptionContainer"] p, small { color: #6B7280 !important; }

/* Plotly chart container background */
[data-testid="stPlotlyChart"] { background: transparent !important; }
</style>
""", unsafe_allow_html=True)
