"""
Roll Yield & Roll Cost Monitor — All Commodities
"""

import pandas as pd
import numpy as np
import plotly.graph_objects as go
import streamlit as st
from pathlib import Path

st.set_page_config(page_title="Roll Yield Monitor", layout="wide", initial_sidebar_state="collapsed")
st.markdown("""
<style>
[data-testid="stAppViewContainer"] {
    background:
        radial-gradient(ellipse 70% 60% at 20% 80%, rgba(26,74,90,.50) 0%, transparent 65%),
        radial-gradient(ellipse 60% 50% at 80% 20%, rgba(42,85,104,.45) 0%, transparent 60%),
        radial-gradient(ellipse 80% 70% at 50% 30%, rgba(79,176,200,.10) 0%, transparent 55%),
        #0D1620;
    background-attachment: fixed;
}
[data-testid="stHeader"] {
    background: rgba(13,22,32,.85) !important;
    backdrop-filter: saturate(180%) blur(16px);
    -webkit-backdrop-filter: saturate(180%) blur(16px);
    border-bottom: 1px solid rgba(188,212,222,.14);
}
.block-container { padding-top: 2rem !important; padding-bottom: 1.5rem; max-width: 1500px; }
</style>
""", unsafe_allow_html=True)

# ── Constants ─────────────────────────────────────────────────────────────────
NAVY  = "#0a2463"
BLACK = "#1d1d1f"
_BASE = Path(__file__).parent.parent / "Database"

COMM_CONFIG = {
    "KC":  {"name": "Arabica",      "color": "#0a2463", "lot_mult": 375,  "rolls_yr": 5},
    "RC":  {"name": "Robusta",      "color": "#8b1a00", "lot_mult": 10,   "rolls_yr": 5},
    "CC":  {"name": "NYC Cocoa",    "color": "#e8a020", "lot_mult": 10,   "rolls_yr": 5},
    "LCC": {"name": "LDN Cocoa",    "color": "#4a7fb5", "lot_mult": 10,   "rolls_yr": 5},
    "SB":  {"name": "Sugar",        "color": "#1a6b1a", "lot_mult": 1120, "rolls_yr": 4},
    "CT":  {"name": "Cotton",       "color": "#7b2d8b", "lot_mult": 500,  "rolls_yr": 5},
    "W":   {"name": "White Sugar",  "color": "#c0392b", "lot_mult": 50,   "rolls_yr": 6},
    "ZC":  {"name": "Corn",         "color": "#f39c12", "lot_mult": 50,   "rolls_yr": 5},
    "ZW":  {"name": "Wheat",        "color": "#d35400", "lot_mult": 50,   "rolls_yr": 5},
    "KE":  {"name": "KC Wheat",     "color": "#795548", "lot_mult": 50,   "rolls_yr": 5},
    "JO":  {"name": "Orange Juice", "color": "#e67e22", "lot_mult": 150,  "rolls_yr": 5},
}

COMMS  = list(COMM_CONFIG.keys())
NAMES  = {k: v["name"] for k, v in COMM_CONFIG.items()}
COLORS = {k: v["color"] for k, v in COMM_CONFIG.items()}
MONTHS = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]

_D = dict(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="-apple-system,Helvetica Neue,sans-serif", color=BLACK, size=10),
)

def lbl(text):
    return (f"<div style='background:{NAVY};padding:5px 13px;border-radius:5px;"
            f"margin-bottom:8px'><span style='font-size:.78rem;font-weight:500;"
            f"letter-spacing:.07em;text-transform:uppercase;color:#dde4f0'>{text}</span></div>")

# ── Demo data generator ───────────────────────────────────────────────────────
def _generate_demo():
    dates = pd.bdate_range("2020-01-01", pd.Timestamp.today())
    np.random.seed(42)
    rows = []
    for comm, cfg in COMM_CONFIG.items():
        spot0   = {"KC": 150, "RC": 2000, "CC": 2500, "LCC": 1800,
                   "SB": 15,  "CT": 80,   "W": 400,   "ZC": 450,
                   "ZW": 550, "KE": 570,  "JO": 130}[comm]
        spot    = spot0 * np.exp(np.cumsum(np.random.normal(0, 0.008, len(dates))))
        base_ry = np.random.uniform(0.03, 0.15)
        ry      = base_ry + np.random.normal(0, 0.02, len(dates))
        ry      = pd.Series(ry).rolling(20).mean().fillna(base_ry).values
        yr1     = spot / (1 + ry)
        for i, d in enumerate(dates):
            spread = yr1[i] - spot[i]
            curve  = [spot[i] + spread * (j / 7) for j in range(8)]
            rows.append({
                "Date": d, "Commodity": comm,
                "Spot": round(spot[i], 2), "OneYr": round(yr1[i], 2),
                "Roll_Yield_1yr": round(ry[i], 6),
                **{f"c{j+1}": round(curve[j], 2) for j in range(8)},
            })
    return pd.DataFrame(rows)

# ── Load data ─────────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def load_data():
    pq = _BASE / "roll_yield_data.parquet"
    if pq.exists():
        df = pd.read_parquet(pq)
        df["Date"] = pd.to_datetime(df["Date"])
        return df, False
    return _generate_demo(), True

df, is_demo = load_data()

# ── Header ────────────────────────────────────────────────────────────────────
st.markdown(
    "<h2 style='font-family:\"Playfair Display\",Georgia,serif;color:#0a2463;"
    "font-weight:400;letter-spacing:-.01em;margin-bottom:2px'>Roll Yield & Roll Cost Monitor</h2>",
    unsafe_allow_html=True,
)
if is_demo:
    st.info("Demo mode — synthetic data. Run roll_yield_ingest.py to load live data.")
st.markdown("<hr>", unsafe_allow_html=True)

# ── Shared date filter ────────────────────────────────────────────────────────
min_d         = df["Date"].min().date()
max_d         = df["Date"].max().date()
default_start = (df["Date"].max() - pd.DateOffset(years=3)).date()

with st.expander("Controls", expanded=True):
    ca, cb = st.columns([3, 5])
    with ca:
        sel_comms = st.multiselect(
            "Commodities",
            options=COMMS,
            default=["KC", "RC", "CC"],
            format_func=lambda x: f"{x} — {NAMES[x]}",
            key="ms_comms",
        )
    with cb:
        date_range = st.slider(
            "Date range", min_value=min_d, max_value=max_d,
            value=(default_start, max_d), key="sl_dates",
        )

start_d, end_d = date_range
df_fil = df[(df["Date"].dt.date >= start_d) & (df["Date"].dt.date <= end_d)]

# ── Tabs ──────────────────────────────────────────────────────────────────────
ry_tab, rc_tab = st.tabs(["Roll Yield", "Roll Cost"])


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1 — ROLL YIELD (existing content unchanged)
# ═══════════════════════════════════════════════════════════════════════════════
with ry_tab:

    # SECTION 1 — Roll Yield Line Chart
    st.markdown(lbl("1-Year Roll Yield (%) — Spot / 1yr − 1"), unsafe_allow_html=True)
    fig_line = go.Figure()
    for comm in sel_comms:
        s = df_fil[df_fil["Commodity"] == comm].sort_values("Date")
        fig_line.add_trace(go.Scatter(
            x=s["Date"], y=(s["Roll_Yield_1yr"] * 100).round(2),
            name=NAMES[comm], mode="lines",
            line=dict(color=COLORS[comm], width=1.8),
            hovertemplate=f"<b>{NAMES[comm]}</b>  %{{x|%d %b %Y}}  %{{y:.1f}}%<extra></extra>",
        ))
    fig_line.add_hline(y=0, line_dash="dot", line_color="#aaaaaa", line_width=1)
    fig_line.update_layout(
        height=370,
        xaxis=dict(showgrid=False, tickfont=dict(size=9, color=BLACK)),
        yaxis=dict(showgrid=True, gridcolor="#f0f0f0", tickfont=dict(size=9, color=BLACK),
                   ticksuffix="%", title="Roll Yield (%)"),
        legend=dict(orientation="h", y=1.02, x=0, font=dict(size=8, color=BLACK), bgcolor="rgba(255,255,255,0.7)"),
        margin=dict(t=10, b=10, l=4, r=4), **_D,
    )
    st.plotly_chart(fig_line, use_container_width=True)
    st.markdown("<hr>", unsafe_allow_html=True)

    # SECTION 2 — Ranking + Percentile
    col_rank, col_pct = st.columns(2)
    latest_date = df_fil["Date"].max()
    df_latest   = df_fil[df_fil["Date"] == latest_date].set_index("Commodity")

    with col_rank:
        st.markdown(lbl(f"Roll Yield Ranking · {latest_date.strftime('%d/%m/%Y')}"), unsafe_allow_html=True)
        rank_rows = []
        for comm in COMMS:
            if comm in df_latest.index:
                ry = df_latest.loc[comm, "Roll_Yield_1yr"] * 100
                rank_rows.append({"Rank": 0, "Commodity": NAMES[comm], "Roll Yield (1yr)": f"{ry:+.1f}%", "_ry": ry})
        rank_df = pd.DataFrame(rank_rows).sort_values("_ry", ascending=False).reset_index(drop=True)
        rank_df["Rank"] = rank_df.index + 1
        fig_rank = go.Figure(go.Table(
            columnwidth=[30, 100, 80],
            header=dict(
                values=["Rank", "Commodity", "Roll Yield (1yr)"],
                fill_color=NAVY, font=dict(color="white", size=10),
                align="center", height=28,
            ),
            cells=dict(
                values=[rank_df["Rank"], rank_df["Commodity"], rank_df["Roll Yield (1yr)"]],
                fill_color=[["white" if i % 2 == 0 else "#f5f5f7" for i in range(len(rank_df))]],
                font=dict(color=[
                    ["white"]*len(rank_df),
                    [BLACK]*len(rank_df),
                    [("#1a6b1a" if r > 0 else "#8b0000") for r in rank_df["_ry"]],
                ], size=10),
                align="center", height=24,
            ),
        ))
        fig_rank.update_layout(height=340, margin=dict(t=0, b=0, l=0, r=0), **_D)
        st.plotly_chart(fig_rank, use_container_width=True)

    with col_pct:
        st.markdown(lbl("Roll Yield Percentile vs Full History"), unsafe_allow_html=True)
        pct_rows = []
        for comm in COMMS:
            hist = df[df["Commodity"] == comm]["Roll_Yield_1yr"].dropna()
            if hist.empty:
                continue
            cur = df_latest.loc[comm, "Roll_Yield_1yr"] if comm in df_latest.index else np.nan
            if np.isnan(cur):
                continue
            pct = float((hist < cur).mean() * 100)
            pct_rows.append({"Commodity": NAMES[comm], "Percentile": round(pct, 1), "color": COLORS[comm]})
        pct_df = pd.DataFrame(pct_rows).sort_values("Percentile", ascending=True)
        fig_pct = go.Figure(go.Bar(
            x=pct_df["Percentile"], y=pct_df["Commodity"],
            orientation="h", marker_color=pct_df["color"],
            text=pct_df["Percentile"].map(lambda x: f"{x:.0f}th"),
            textposition="outside", textfont=dict(size=9, color=BLACK),
        ))
        fig_pct.add_vline(x=50, line_dash="dot", line_color="#aaaaaa", line_width=1)
        fig_pct.add_vline(x=80, line_dash="dot", line_color="#e07b39", line_width=1)
        fig_pct.update_layout(
            height=340,
            xaxis=dict(range=[0, 115], showgrid=False, tickfont=dict(size=9, color=BLACK), ticksuffix="%"),
            yaxis=dict(showgrid=False, tickfont=dict(size=9, color=BLACK)),
            margin=dict(t=0, b=0, l=4, r=60), **_D,
        )
        st.plotly_chart(fig_pct, use_container_width=True)

    st.markdown("<hr>", unsafe_allow_html=True)

    # SECTION 3 — Forward Curves
    curve_comm  = st.selectbox("Commodity for curves", COMMS, format_func=lambda x: f"{x} — {NAMES[x]}", key="curve_comm")
    curve_color = COLORS[curve_comm]
    curve_cols  = [f"c{i}" for i in range(1, 9)]
    curve_labels= [f"c{i}" for i in range(1, 9)]
    df_comm     = df_fil[df_fil["Commodity"] == curve_comm].sort_values("Date")
    all_dates_sorted = df_comm["Date"].drop_duplicates().sort_values()
    latest_4d   = all_dates_sorted.iloc[-4:].tolist() if len(all_dates_sorted) >= 4 else all_dates_sorted.tolist()
    weekly_idx  = list(range(-1, -len(all_dates_sorted), -5))[:4]
    latest_4w   = [all_dates_sorted.iloc[i] for i in sorted(weekly_idx)]
    day_colors  = ["#1d1d1f", "#c0392b", "#82c982", "#aaaaaa"]

    st.markdown(lbl(f"Forward Curves · {NAMES[curve_comm]}"), unsafe_allow_html=True)
    fc1, fc2, fc3 = st.columns(3)

    def _curve_fig(dates, colors, title):
        fig = go.Figure()
        for d, col in zip(dates, colors):
            row = df_comm[df_comm["Date"] == d]
            if row.empty:
                continue
            y = [row.iloc[0][c] for c in curve_cols]
            fig.add_trace(go.Scatter(
                x=curve_labels, y=y, mode="lines+markers",
                name=d.strftime("%d/%m/%Y"),
                line=dict(color=col, width=2), marker=dict(size=5),
                hovertemplate="%{x}  %{y:.2f}<extra></extra>",
            ))
        fig.update_layout(
            title=dict(text=title, font=dict(size=11, color=BLACK), x=0.5, xanchor="center"),
            height=320,
            xaxis=dict(showgrid=False, tickfont=dict(size=9, color=BLACK)),
            yaxis=dict(showgrid=True, gridcolor="#f0f0f0", tickfont=dict(size=9, color=BLACK)),
            legend=dict(font=dict(size=8, color=BLACK), bgcolor="rgba(255,255,255,0.7)"),
            margin=dict(t=35, b=10, l=4, r=4), **_D,
        )
        return fig

    with fc1:
        latest_row = df_comm[df_comm["Date"] == all_dates_sorted.iloc[-1]]
        y_latest   = [latest_row.iloc[0][c] for c in curve_cols]
        fig_latest = go.Figure(go.Scatter(
            x=curve_labels, y=y_latest, mode="lines+markers",
            line=dict(color=curve_color, width=2.5), marker=dict(size=6),
            hovertemplate="%{x}  %{y:.2f}<extra></extra>",
        ))
        fig_latest.update_layout(
            title=dict(text=f"Latest · {all_dates_sorted.iloc[-1].strftime('%d/%m/%Y')}",
                       font=dict(size=11, color=BLACK), x=0.5, xanchor="center"),
            height=320,
            xaxis=dict(showgrid=False, tickfont=dict(size=9, color=BLACK)),
            yaxis=dict(showgrid=True, gridcolor="#f0f0f0", tickfont=dict(size=9, color=BLACK)),
            margin=dict(t=35, b=10, l=4, r=4), **_D,
        )
        st.plotly_chart(fig_latest, use_container_width=True)
    with fc2:
        st.plotly_chart(_curve_fig(latest_4d, day_colors, "Last 4 Days"), use_container_width=True)
    with fc3:
        st.plotly_chart(_curve_fig(latest_4w, day_colors, "Last 4 Weeks"), use_container_width=True)

    st.markdown("<hr>", unsafe_allow_html=True)

    # SECTION 4 — Roll Yield Heatmap
    hm_comm = st.selectbox("Commodity for Heatmap", COMMS, format_func=lambda x: f"{x} — {NAMES[x]}", key="hm_comm")
    st.markdown(lbl(f"Roll Yield Heatmap · {NAMES[hm_comm]} · Monthly Avg"), unsafe_allow_html=True)
    hm_s = df_fil[df_fil["Commodity"] == hm_comm].copy()
    hm_s["Roll_Yield_1yr"] = pd.to_numeric(hm_s["Roll_Yield_1yr"], errors="coerce")
    hm_s["Year"]  = hm_s["Date"].dt.year
    hm_s["Month"] = hm_s["Date"].dt.month
    pivot = (
        hm_s.groupby(["Year", "Month"])["Roll_Yield_1yr"]
        .mean().reset_index()
        .pivot(index="Year", columns="Month", values="Roll_Yield_1yr")
    )
    if not pivot.empty:
        pivot.columns = [MONTHS[int(m) - 1] for m in pivot.columns]
        pivot = pivot.sort_index(ascending=False)
        z       = (pivot.to_numpy(dtype='float64', na_value=np.nan) * 100).round(2)
        years   = [str(y) for y in pivot.index]
        months  = list(pivot.columns)
        text_mat= [[f"{v:.1f}%" if not np.isnan(v) else "" for v in row] for row in z]
        fig_hm  = go.Figure(go.Heatmap(
            z=z, x=months, y=years, text=text_mat, texttemplate="%{text}",
            textfont=dict(size=8, color=BLACK),
            colorscale=[[0.0,"#8b0000"],[0.4,"#f5c6cb"],[0.5,"#ffffff"],[0.6,"#d4edda"],[1.0,"#1a6b1a"]],
            zmid=0,
            colorbar=dict(title=dict(text="Roll Yield %", font=dict(size=9, color=BLACK)),
                          tickfont=dict(size=8, color=BLACK), ticksuffix="%", thickness=12, len=0.8),
            hoverongaps=False,
            hovertemplate="<b>%{y} · %{x}</b><br>Avg Roll Yield: %{z:.1f}%<extra></extra>",
        ))
        fig_hm.update_layout(
            height=max(300, len(years) * 28),
            xaxis=dict(side="top", tickfont=dict(size=9, color=BLACK), showgrid=False),
            yaxis=dict(tickfont=dict(size=9, color=BLACK), showgrid=False),
            margin=dict(t=40, b=10, l=60, r=10), **_D,
        )
        st.plotly_chart(fig_hm, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2 — ROLL COST
# ═══════════════════════════════════════════════════════════════════════════════
with rc_tab:

    # Compute roll spread (c2 - c1) for all rows
    df_rc = df_fil.copy()
    df_rc["roll_spread"] = df_rc["c2"] - df_rc["c1"]
    df_rc["roll_pct"]    = (df_rc["roll_spread"] / df_rc["c1"] * 100).round(3)

    latest_rc   = df_rc[df_rc["Date"] == df_rc["Date"].max()].set_index("Commodity")

    # ── RC SECTION 1 — Roll Spread over time ──────────────────────────────────
    st.markdown(lbl("Roll Spread (c2 − c1) Over Time — Positive = Contango, Negative = Backwardation"), unsafe_allow_html=True)

    fig_rc_line = go.Figure()
    for comm in sel_comms:
        s = df_rc[df_rc["Commodity"] == comm].sort_values("Date")
        fig_rc_line.add_trace(go.Scatter(
            x=s["Date"], y=s["roll_spread"].round(2),
            name=NAMES[comm], mode="lines",
            line=dict(color=COLORS[comm], width=1.8),
            hovertemplate=f"<b>{NAMES[comm]}</b>  %{{x|%d %b %Y}}  %{{y:.2f}}<extra></extra>",
        ))
    fig_rc_line.add_hline(y=0, line_dash="dot", line_color="#aaaaaa", line_width=1)
    fig_rc_line.update_layout(
        height=360,
        xaxis=dict(showgrid=False, tickfont=dict(size=9, color=BLACK)),
        yaxis=dict(showgrid=True, gridcolor="#f0f0f0", tickfont=dict(size=9, color=BLACK), title="c2 − c1"),
        legend=dict(orientation="h", y=1.02, x=0, font=dict(size=8, color=BLACK), bgcolor="rgba(255,255,255,0.7)"),
        margin=dict(t=10, b=10, l=4, r=4), **_D,
    )
    st.plotly_chart(fig_rc_line, use_container_width=True)
    st.markdown("<hr>", unsafe_allow_html=True)

    # ── RC SECTION 2 — Snapshot table + Seasonal bar ──────────────────────────
    rc_col1, rc_col2 = st.columns(2)

    with rc_col1:
        st.markdown(lbl(f"Current Roll Cost Snapshot · {df_rc['Date'].max().strftime('%d/%m/%Y')}"), unsafe_allow_html=True)
        snap_rows = []
        for comm in COMMS:
            if comm not in latest_rc.index:
                continue
            spread  = latest_rc.loc[comm, "roll_spread"]
            pct     = latest_rc.loc[comm, "roll_pct"]
            mult    = COMM_CONFIG[comm]["lot_mult"]
            rolls   = COMM_CONFIG[comm]["rolls_yr"]
            dol_lot = spread * mult
            ann_lot = dol_lot * rolls
            regime  = "Contango" if spread > 0 else "Backwardation"
            snap_rows.append({
                "Commodity": NAMES[comm],
                "Spread":    f"{spread:+.2f}",
                "Spread %":  f"{pct:+.2f}%",
                "$/Lot":     f"${dol_lot:+,.0f}",
                "Ann $/Lot": f"${ann_lot:+,.0f}",
                "Regime":    regime,
                "_spread":   spread,
            })
        snap_df = pd.DataFrame(snap_rows).sort_values("_spread", ascending=False).reset_index(drop=True)

        regime_colors = [("#8b0000" if r == "Contango" else "#1a6b1a") for r in snap_df["Regime"]]

        fig_snap = go.Figure(go.Table(
            columnwidth=[90, 55, 60, 65, 75, 80],
            header=dict(
                values=["Commodity", "Spread", "Spread %", "$/Lot", "Ann $/Lot", "Regime"],
                fill_color=NAVY, font=dict(color="white", size=9),
                align="center", height=28,
            ),
            cells=dict(
                values=[
                    snap_df["Commodity"], snap_df["Spread"], snap_df["Spread %"],
                    snap_df["$/Lot"], snap_df["Ann $/Lot"], snap_df["Regime"],
                ],
                fill_color=[["white" if i % 2 == 0 else "#f5f5f7" for i in range(len(snap_df))]],
                font=dict(
                    color=[
                        [BLACK]*len(snap_df),
                        [("#8b0000" if v > 0 else "#1a6b1a") for v in snap_df["_spread"]],
                        [("#8b0000" if v > 0 else "#1a6b1a") for v in snap_df["_spread"]],
                        [("#8b0000" if v > 0 else "#1a6b1a") for v in snap_df["_spread"]],
                        [("#8b0000" if v > 0 else "#1a6b1a") for v in snap_df["_spread"]],
                        regime_colors,
                    ], size=9,
                ),
                align="center", height=24,
            ),
        ))
        fig_snap.update_layout(height=380, margin=dict(t=0, b=0, l=0, r=0), **_D)
        st.plotly_chart(fig_snap, use_container_width=True)

    with rc_col2:
        st.markdown(lbl("Seasonal Roll Spread — Avg by Month Across All Years"), unsafe_allow_html=True)
        rc_seas_comm = st.selectbox(
            "Commodity", COMMS,
            format_func=lambda x: f"{x} — {NAMES[x]}", key="rc_seas_comm",
        )
        seas = df_rc[df_rc["Commodity"] == rc_seas_comm].copy()
        seas["Month"] = seas["Date"].dt.month
        seas_avg = seas.groupby("Month")["roll_spread"].mean().reindex(range(1, 13))
        colors_seas = ["#8b0000" if v > 0 else "#1a6b1a" for v in seas_avg.fillna(0)]
        fig_seas = go.Figure(go.Bar(
            x=MONTHS, y=seas_avg.values.round(2),
            marker_color=colors_seas,
            text=[f"{v:.2f}" if not np.isnan(v) else "" for v in seas_avg.values],
            textposition="outside", textfont=dict(size=8, color=BLACK),
            hovertemplate="<b>%{x}</b><br>Avg spread: %{y:.2f}<extra></extra>",
        ))
        fig_seas.add_hline(y=0, line_dash="dot", line_color="#aaaaaa", line_width=1)
        fig_seas.update_layout(
            height=340,
            xaxis=dict(showgrid=False, tickfont=dict(size=9, color=BLACK)),
            yaxis=dict(showgrid=True, gridcolor="#f0f0f0", tickfont=dict(size=9, color=BLACK), title="Avg c2−c1"),
            margin=dict(t=10, b=10, l=4, r=4), **_D,
        )
        st.plotly_chart(fig_seas, use_container_width=True)

    st.markdown("<hr>", unsafe_allow_html=True)

    # ── RC SECTION 3 — Roll Cost Heatmap ──────────────────────────────────────
    rc_hm_comm = st.selectbox(
        "Commodity for Heatmap", COMMS,
        format_func=lambda x: f"{x} — {NAMES[x]}", key="rc_hm_comm",
    )
    st.markdown(lbl(f"Roll Spread Heatmap · {NAMES[rc_hm_comm]} · Monthly Avg (c2−c1)"), unsafe_allow_html=True)

    hm_rc = df_rc[df_rc["Commodity"] == rc_hm_comm].copy()
    hm_rc["Year"]  = hm_rc["Date"].dt.year
    hm_rc["Month"] = hm_rc["Date"].dt.month
    rc_pivot = (
        hm_rc.groupby(["Year", "Month"])["roll_spread"]
        .mean().reset_index()
        .pivot(index="Year", columns="Month", values="roll_spread")
    )

    if not rc_pivot.empty:
        rc_pivot.columns = [MONTHS[int(m) - 1] for m in rc_pivot.columns]
        rc_pivot = rc_pivot.sort_index(ascending=False)
        z_rc      = rc_pivot.to_numpy(dtype='float64', na_value=np.nan).round(2)
        years_rc  = [str(y) for y in rc_pivot.index]
        months_rc = list(rc_pivot.columns)
        text_rc   = [[f"{v:.2f}" if not np.isnan(v) else "" for v in row] for row in z_rc]

        fig_rc_hm = go.Figure(go.Heatmap(
            z=z_rc, x=months_rc, y=years_rc,
            text=text_rc, texttemplate="%{text}",
            textfont=dict(size=8, color=BLACK),
            colorscale=[[0.0,"#1a6b1a"],[0.4,"#d4edda"],[0.5,"#ffffff"],[0.6,"#f5c6cb"],[1.0,"#8b0000"]],
            zmid=0,
            colorbar=dict(
                title=dict(text="c2−c1", font=dict(size=9, color=BLACK)),
                tickfont=dict(size=8, color=BLACK), thickness=12, len=0.8,
            ),
            hoverongaps=False,
            hovertemplate="<b>%{y} · %{x}</b><br>Avg Spread: %{z:.2f}<extra></extra>",
        ))
        fig_rc_hm.update_layout(
            height=max(300, len(years_rc) * 28),
            xaxis=dict(side="top", tickfont=dict(size=9, color=BLACK), showgrid=False),
            yaxis=dict(tickfont=dict(size=9, color=BLACK), showgrid=False),
            margin=dict(t=40, b=10, l=60, r=10), **_D,
        )
        st.plotly_chart(fig_rc_hm, use_container_width=True)

    st.markdown("<hr>", unsafe_allow_html=True)

    # ── RC SECTION 4 — Cumulative Annual Roll Cost ─────────────────────────────
    st.markdown(lbl("Cumulative Annual Roll Cost ($/Lot) — Estimated from Daily Spread"), unsafe_allow_html=True)

    rc_cum_comm = st.selectbox(
        "Commodity", COMMS,
        format_func=lambda x: f"{x} — {NAMES[x]}", key="rc_cum_comm",
    )
    mult_cum  = COMM_CONFIG[rc_cum_comm]["lot_mult"]
    rolls_cum = COMM_CONFIG[rc_cum_comm]["rolls_yr"]

    cum_s = df_rc[df_rc["Commodity"] == rc_cum_comm].sort_values("Date").copy()
    cum_s["dol_spread"] = cum_s["roll_spread"] * mult_cum
    cum_s["Year"] = cum_s["Date"].dt.year

    fig_cum = go.Figure()
    for yr, grp in cum_s.groupby("Year"):
        # Approximate annual cost: avg daily spread × lot_mult × rolls_per_year
        avg_spread = grp["roll_spread"].mean()
        ann_cost   = avg_spread * mult_cum * rolls_cum
        # Daily cumulative within year (shows trajectory)
        grp = grp.copy()
        grp["cum_dol"] = grp["dol_spread"].cumsum() / len(grp) * rolls_cum
        opacity = 0.4 if yr < cum_s["Year"].max() else 1.0
        fig_cum.add_trace(go.Scatter(
            x=grp["Date"].dt.dayofyear, y=grp["cum_dol"].round(0),
            name=str(yr), mode="lines",
            line=dict(color=COLORS[rc_cum_comm], width=1.5 if yr < cum_s["Year"].max() else 2.5),
            opacity=opacity,
            hovertemplate=f"<b>{yr}</b>  Day %{{x}}  $%{{y:,.0f}}/lot<extra></extra>",
        ))

    fig_cum.add_hline(y=0, line_dash="dot", line_color="#aaaaaa", line_width=1)
    fig_cum.update_layout(
        height=380,
        xaxis=dict(showgrid=False, tickfont=dict(size=9, color=BLACK), title="Day of Year"),
        yaxis=dict(showgrid=True, gridcolor="#f0f0f0", tickfont=dict(size=9, color=BLACK),
                   title="Est. Cumulative Roll Cost ($/lot)", tickprefix="$"),
        legend=dict(orientation="h", y=1.02, x=0, font=dict(size=8, color=BLACK), bgcolor="rgba(255,255,255,0.7)"),
        margin=dict(t=10, b=10, l=4, r=4), **_D,
    )
    st.plotly_chart(fig_cum, use_container_width=True)
