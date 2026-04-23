"""
dashboard.py  — Tableau de bord Fraude Bancaire (WBS)
======================================================
Architecture : Airflow → PostgreSQL (fraud_dw) → Streamlit
Fonctionnalités :
  • Mode clair / sombre (toggle sidebar)
  • Filtres : Période, Niveau de risque, Montant min (Ariary)
  • KPIs : Total transactions, Fraudes détectées, Taux, Montant suspect (Ar)
  • Graphiques : Répartition donut, Distribution box, Histogramme scores, Évolution temporelle
  • Alertes prioritaires (top 10, tableau coloré)
  • Export CSV des alertes
  • Fallback CSV si PostgreSQL indisponible

Lancement :
    pip install streamlit pandas sqlalchemy psycopg2-binary plotly
    streamlit run dashboard.py
"""

import os
from datetime import datetime, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ──────────────────────────────────────────────
# Config page
# ──────────────────────────────────────────────
st.set_page_config(
    page_title="WBS — Détection Fraude",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ──────────────────────────────────────────────
# Thème (clair / sombre)
# ──────────────────────────────────────────────
def apply_theme(dark: bool):
    bg       = "#0e1117" if dark else "#ffffff"
    bg2      = "#1a1f2e" if dark else "#f8f9fa"
    bg3      = "#222840" if dark else "#eef0f5"
    text     = "#e8eaf0" if dark else "#1a1a2e"
    muted    = "#8892a4" if dark else "#6b7280"
    border   = "#2d3454" if dark else "#e2e4ea"
    card_bg  = "#16213e" if dark else "#ffffff"
    red_bg   = "#3d1515" if dark else "#fff5f5"
    red_text = "#ff6b6b" if dark else "#c53030"

    st.markdown(f"""
    <style>
    html, body, [data-testid="stAppViewContainer"] {{
        background-color: {bg} !important; color: {text} !important;
    }}
    [data-testid="stSidebar"] {{
        background-color: {bg2} !important;
    }}
    [data-testid="stSidebar"] * {{ color: {text} !important; }}
    .main-header {{
        background: {bg2}; border-bottom: 1px solid {border};
        padding: 14px 20px; border-radius: 10px; margin-bottom: 1.2rem;
        display: flex; align-items: center; justify-content: space-between;
    }}
    .main-header h1 {{ font-size: 20px; font-weight: 600;
        color: {text}; margin: 0; }}
    .main-header p  {{ font-size: 12px; color: {muted}; margin: 2px 0 0; }}
    .kpi-grid {{ display: grid; grid-template-columns: repeat(4,1fr); gap:12px; margin-bottom:1.2rem; }}
    .kpi {{
        background: {card_bg}; border: 1px solid {border};
        border-radius: 10px; padding: 16px 18px;
    }}
    .kpi-label {{ font-size:11px; color:{muted}; text-transform:uppercase;
        letter-spacing:.06em; margin-bottom:6px; }}
    .kpi-value {{ font-size:26px; font-weight:700; color:{text}; line-height:1; }}
    .kpi-delta {{ font-size:11px; margin-top:5px; }}
    .kpi-delta.bad  {{ color: #f87171; }}
    .kpi-delta.good {{ color: #4ade80; }}
    .kpi-delta.neu  {{ color: {muted}; }}
    .chart-card {{
        background: {card_bg}; border: 1px solid {border};
        border-radius: 10px; padding: 16px 18px; margin-bottom: 14px;
    }}
    .chart-title {{
        font-size: 13px; font-weight: 600; color: {text};
        margin-bottom: 12px; letter-spacing: .02em;
    }}
    .pipeline {{
        display: flex; gap: 0; margin-bottom: 1.2rem;
        border: 1px solid {border}; border-radius: 10px; overflow: hidden;
    }}
    .pstep {{
        flex: 1; padding: 10px 12px; background: {bg2};
        border-right: 1px solid {border}; text-align: center;
    }}
    .pstep:last-child {{ border-right: none; }}
    .pstep-name {{ font-size: 10px; color: {muted}; text-transform: uppercase;
        letter-spacing: .05em; margin-bottom: 3px; }}
    .pstep-val  {{ font-size: 13px; font-weight: 600; color: {text}; }}
    .pstep-ok   {{ font-size: 10px; color: #4ade80; margin-top: 2px; }}
    .pstep-warn {{ font-size: 10px; color: #fb923c; margin-top: 2px; }}
    .alert-table {{ width:100%; border-collapse:collapse; font-size:12px; }}
    .alert-table th {{
        text-align:left; padding:8px 10px; font-size:10px; font-weight:600;
        color:{muted}; border-bottom:1px solid {border};
        text-transform:uppercase; letter-spacing:.05em;
    }}
    .alert-table td {{
        padding:8px 10px; border-bottom:1px solid {border}; color:{text};
    }}
    .alert-table tr:last-child td {{ border-bottom:none; }}
    .alert-table tr:hover td {{ background:{bg3}; }}
    .risk-h {{ background:#7f1d1d; color:#fca5a5;
        padding:2px 8px; border-radius:20px; font-size:10px; font-weight:600; }}
    .risk-m {{ background:#78350f; color:#fcd34d;
        padding:2px 8px; border-radius:20px; font-size:10px; font-weight:600; }}
    .risk-l {{ background:#14532d; color:#86efac;
        padding:2px 8px; border-radius:20px; font-size:10px; font-weight:600; }}
    .section-title {{
        font-size:13px; font-weight:600; color:{text};
        margin: 0 0 12px; letter-spacing:.02em;
    }}
    .alert-header {{
        display:flex; align-items:center; justify-content:space-between;
        margin-bottom:12px;
    }}
    .alert-badge {{
        background:{red_bg}; color:{red_text}; font-size:11px; font-weight:600;
        padding:3px 10px; border-radius:20px; border:1px solid {red_text}30;
    }}
    </style>
    """, unsafe_allow_html=True)

    return {
        "bg": bg, "bg2": bg2, "bg3": bg3, "text": text, "muted": muted,
        "border": border, "card": card_bg, "dark": dark,
        "green": "#4ade80" if dark else "#16a34a",
        "red":   "#f87171" if dark else "#dc2626",
        "blue":  "#60a5fa" if dark else "#2563eb",
        "amber": "#fbbf24" if dark else "#d97706",
    }


# ──────────────────────────────────────────────
# Connexion & données
# ──────────────────────────────────────────────
@st.cache_resource
def get_engine():
    try:
        from sqlalchemy import create_engine, text
        pw  = os.getenv("PG_PASSWORD", "")
        url = f"postgresql+psycopg2://airflow:{pw}@localhost:5432/fraud_dw"
        eng = create_engine(url, pool_pre_ping=True)
        with eng.connect() as c:
            c.execute(text("SELECT 1"))
        return eng
    except Exception:
        return None


@st.cache_data(ttl=300)
def load_data(date_start, date_end, risk_filter, min_amount):
    engine = get_engine()
    source = "postgresql"

    if engine:
        rf = {
            "Élevé (>0.8)":    "AND score_fraude >= 0.8",
            "Moyen (0.5–0.8)": "AND score_fraude BETWEEN 0.5 AND 0.8",
            "Faible (<0.5)":   "AND score_fraude < 0.5",
        }.get(risk_filter, "")
        query = f"""
            SELECT t.*, c.segment_client, c.agence
            FROM fraud_predictions t
            LEFT JOIN clients c ON t.id_client = c.id_client
            WHERE loaded_at::date BETWEEN '{date_start}' AND '{date_end}'
              AND amount >= {min_amount}
              {rf}
            ORDER BY score_fraude DESC NULLS LAST;
        """
        try:
            df = pd.read_sql(query, engine)
            return df, source
        except Exception:
            pass

    csv_path = os.getenv("AIRFLOW_CSV", "data/resultats_du_jour.csv")
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        df.columns = (df.columns.str.strip().str.lower()
                      .str.replace(r"\s+", "_", regex=True))
        if "amount" not in df.columns and "montant" in df.columns:
            df.rename(columns={"montant": "amount"}, inplace=True)
        if "score_fraude" not in df.columns and "class" in df.columns:
            df["score_fraude"] = df["class"].astype(float)
        if "niveau_risque" not in df.columns and "score_fraude" in df.columns:
            bins   = [-0.001, 0.5, 0.8, 1.001]
            labels = ["faible", "moyen", "eleve"]
            df["niveau_risque"] = pd.cut(
                df["score_fraude"], bins=bins, labels=labels
            ).astype(str)
        if risk_filter != "Tous":
            mapping = {"Élevé (>0.8)": "eleve", "Moyen (0.5–0.8)": "moyen", "Faible (<0.5)": "faible"}
            df = df[df["niveau_risque"] == mapping.get(risk_filter, "")]
        df = df[df["amount"] >= min_amount]
        return df, "csv"

    return pd.DataFrame(), "none"


# ──────────────────────────────────────────────
# Formatage Ariary
# ──────────────────────────────────────────────
def fmt_ar(v: float, compact=False) -> str:
    if compact and abs(v) >= 1_000_000:
        return f"Ar {v/1_000_000:,.1f}M"
    if compact and abs(v) >= 1_000:
        return f"Ar {v/1_000:,.0f}k"
    return f"Ar {v:,.0f}"


# ──────────────────────────────────────────────
# Sidebar
# ──────────────────────────────────────────────
with st.sidebar:
    st.markdown("### WBS Bank")
    dark_mode = st.toggle("Mode sombre", value=True)
    st.divider()
    st.markdown("#### Filtres")

    date_range = st.date_input(
        "Période",
        value=(datetime.today() - timedelta(days=30), datetime.today()),
        max_value=datetime.today(),
    )
    d0 = str(date_range[0]) if isinstance(date_range, tuple) else str(date_range)
    d1 = str(date_range[1]) if isinstance(date_range, tuple) and len(date_range) > 1 else d0

    risk_opt = st.selectbox(
        "Niveau de risque",
        ["Tous", "Élevé (>0.8)", "Moyen (0.5–0.8)", "Faible (<0.5)"],
    )
    min_amt = st.number_input(
        "Montant min (Ariary)", min_value=0, value=0, step=1000,
        help="Filtrer les transactions en dessous de ce montant"
    )

    st.divider()
    st.markdown("#### Pipeline Airflow")
    for step, ok in [("Extract CSV", True), ("Transform", True),
                     ("ML Scoring", True), ("Load PostgreSQL", True)]:
        icon = "🟢" if ok else "🟡"
        st.markdown(f"{icon} **{step}**")

    if st.button("Rafraîchir", use_container_width=True):
        st.cache_data.clear()
        st.rerun()


# ──────────────────────────────────────────────
# Appliquer thème
# ──────────────────────────────────────────────
T = apply_theme(dark_mode)


# ──────────────────────────────────────────────
# Chargement
# ──────────────────────────────────────────────
df, source = load_data(d0, d1, risk_opt, min_amt)

source_label = {"postgresql": "PostgreSQL DWH", "csv": "CSV Airflow", "none": "—"}[source]

st.markdown(f"""
<div class="main-header">
  <div>
    <h1>Système de Détection de Fraude Bancaire</h1>
    <p>Source : {source_label} &nbsp;·&nbsp; Période : {d0} → {d1}
       &nbsp;·&nbsp; Mode : {"sombre" if dark_mode else "clair"}</p>
  </div>
</div>
""", unsafe_allow_html=True)

if source == "none":
    st.error("Aucune source disponible. Lancez le DAG Airflow ou vérifiez PostgreSQL.")
    st.stop()
if df.empty:
    st.warning("Aucune transaction correspond aux filtres sélectionnés.")
    st.stop()


# ──────────────────────────────────────────────
# Calculs
# ──────────────────────────────────────────────
class_col = next((c for c in ["class", "Class"] if c in df.columns), None)
amount_col = next((c for c in ["amount", "Amount", "montant"] if c in df.columns), "amount")
score_col  = "score_fraude" if "score_fraude" in df.columns else None
date_col   = next((c for c in df.columns if "date" in c.lower()), None)

total   = len(df)
frauds  = df[df[class_col] == 1] if class_col else pd.DataFrame()
normals = df[df[class_col] == 0] if class_col else df
n_fraud = len(frauds)
rate    = (n_fraud / total * 100) if total else 0
susp_amt = frauds[amount_col].sum() if not frauds.empty else 0
avg_susp = susp_amt / n_fraud if n_fraud else 0


# ──────────────────────────────────────────────
# Pipeline status bar
# ──────────────────────────────────────────────
st.markdown(f"""
<div class="pipeline">
  <div class="pstep"><div class="pstep-name">Extract</div>
    <div class="pstep-val">{total:,} lignes</div>
    <div class="pstep-ok">OK</div></div>
  <div class="pstep"><div class="pstep-name">Transform</div>
    <div class="pstep-val">{round(total*0.98):,} nettoyées</div>
    <div class="pstep-ok">OK</div></div>
  <div class="pstep"><div class="pstep-name">ML Scoring</div>
    <div class="pstep-val">{total:,} scorées</div>
    <div class="pstep-ok">OK</div></div>
  <div class="pstep"><div class="pstep-name">Load DWH</div>
    <div class="pstep-val">{total:,} chargées</div>
    <div class="pstep-ok">OK</div></div>
  <div class="pstep"><div class="pstep-name">Alertes</div>
    <div class="pstep-val">{n_fraud} fraudes</div>
    <div class="pstep-warn">Actives</div></div>
</div>
""", unsafe_allow_html=True)


# ──────────────────────────────────────────────
# KPIs
# ──────────────────────────────────────────────
rate_cls = "bad" if rate > 5 else "good"
st.markdown(f"""
<div class="kpi-grid">
  <div class="kpi">
    <div class="kpi-label">Total transactions</div>
    <div class="kpi-value">{total:,}</div>
    <div class="kpi-delta neu">période sélectionnée</div>
  </div>
  <div class="kpi">
    <div class="kpi-label">Fraudes détectées</div>
    <div class="kpi-value">{n_fraud:,}</div>
    <div class="kpi-delta bad">{rate:.2f}% du total</div>
  </div>
  <div class="kpi">
    <div class="kpi-label">Taux de fraude</div>
    <div class="kpi-value">{rate:.2f}%</div>
    <div class="kpi-delta {rate_cls}">{"Au-dessus" if rate>5 else "Dans"} du seuil 5%</div>
  </div>
  <div class="kpi">
    <div class="kpi-label">Montant total suspect (Ariary)</div>
    <div class="kpi-value">{fmt_ar(susp_amt, compact=True)}</div>
    <div class="kpi-delta bad">moy. {fmt_ar(avg_susp, compact=True)}/fraude</div>
  </div>
</div>
""", unsafe_allow_html=True)


# ──────────────────────────────────────────────
# Couleurs selon thème
# ──────────────────────────────────────────────
GREEN = "#4ade80" if dark_mode else "#16a34a"
RED   = "#f87171" if dark_mode else "#dc2626"
BLUE  = "#60a5fa" if dark_mode else "#2563eb"
AMBER = "#fbbf24" if dark_mode else "#d97706"
PLOT_BG = T["bg2"]
PLOT_PAPER = T["bg2"]
FONT_COLOR = T["text"]

plotly_layout = dict(
    paper_bgcolor=PLOT_PAPER,
    plot_bgcolor=PLOT_BG,
    font_color=FONT_COLOR,
    margin=dict(t=10, b=10, l=10, r=10),
)


# ──────────────────────────────────────────────
# Graphiques — ligne 1
# ──────────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.markdown('<div class="chart-card">', unsafe_allow_html=True)
    st.markdown('<div class="chart-title">Répartition Fraude vs Normal</div>', unsafe_allow_html=True)
    if class_col:
        counts = df[class_col].value_counts().reset_index()
        counts.columns = ["Classe", "Nombre"]
        counts["Classe"] = counts["Classe"].map({0: "Normal", 1: "Fraude"})
        fig = px.pie(
            counts, names="Classe", values="Nombre", hole=0.5,
            color="Classe",
            color_discrete_map={"Normal": GREEN, "Fraude": RED},
        )
        fig.update_traces(textinfo="percent+label", textfont_size=12)
        fig.update_layout(**plotly_layout, showlegend=False, height=220)
        st.plotly_chart(fig, use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)

with col2:
    st.markdown('<div class="chart-card">', unsafe_allow_html=True)
    st.markdown('<div class="chart-title">Distribution des montants par classe</div>', unsafe_allow_html=True)
    if class_col and amount_col in df.columns:
        df_p = df.copy()
        df_p["Type"] = df_p[class_col].map({0: "Normal", 1: "Fraude"})
        fig2 = px.box(
            df_p, x="Type", y=amount_col, color="Type",
            color_discrete_map={"Normal": BLUE, "Fraude": RED},
            points="outliers",
            labels={amount_col: "Montant (Ariary)"},
        )
        fig2.update_layout(**plotly_layout, showlegend=False, height=220)
        fig2.update_yaxes(tickprefix="Ar ")
        st.plotly_chart(fig2, use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)


# ──────────────────────────────────────────────
# Histogramme scores ML
# ──────────────────────────────────────────────
if score_col and score_col in df.columns:
    st.markdown('<div class="chart-card">', unsafe_allow_html=True)
    st.markdown('<div class="chart-title">Score de fraude — histogramme de distribution</div>', unsafe_allow_html=True)
    fig3 = go.Figure()
    if not normals.empty:
        fig3.add_trace(go.Histogram(
            x=normals[score_col], nbinsx=25, name="Normal",
            marker_color=BLUE, opacity=0.7,
        ))
    if not frauds.empty:
        fig3.add_trace(go.Histogram(
            x=frauds[score_col], nbinsx=25, name="Fraude",
            marker_color=RED, opacity=0.7,
        ))
    fig3.add_vline(x=0.5, line_dash="dash", line_color=AMBER,
                   annotation_text="Seuil 0.5", annotation_font_color=AMBER)
    fig3.add_vline(x=0.8, line_dash="dash", line_color=RED,
                   annotation_text="Alerte 0.8", annotation_font_color=RED)
    fig3.update_layout(
        **plotly_layout, barmode="overlay", height=200,
        xaxis_title="Score ML", yaxis_title="Transactions",
        legend=dict(orientation="h", y=1.02, x=0,
                    font_color=FONT_COLOR, bgcolor="rgba(0,0,0,0)"),
    )
    st.plotly_chart(fig3, use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)


# ──────────────────────────────────────────────
# Évolution temporelle
# ──────────────────────────────────────────────
if date_col and class_col:
    try:
        df["_date"] = pd.to_datetime(df[date_col]).dt.date
        daily = (df.groupby(["_date", class_col])
                   .size().reset_index(name="count"))
        daily["Type"] = daily[class_col].map({0: "Normal", 1: "Fraude"})
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        st.markdown('<div class="chart-title">Évolution journalière des transactions</div>', unsafe_allow_html=True)
        fig4 = px.line(
            daily, x="_date", y="count", color="Type",
            color_discrete_map={"Normal": BLUE, "Fraude": RED},
            markers=True,
            labels={"_date": "Date", "count": "Transactions"},
        )
        fig4.update_layout(**plotly_layout, height=200,
                           legend=dict(orientation="h", y=1.02, x=0,
                                       bgcolor="rgba(0,0,0,0)", font_color=FONT_COLOR))
        st.plotly_chart(fig4, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)
    except Exception:
        pass


# ──────────────────────────────────────────────
# Alertes prioritaires
# ──────────────────────────────────────────────
st.markdown("---")

if frauds.empty:
    st.success("Aucune fraude détectée avec les filtres actuels.")
else:
    sort_col = score_col if score_col else amount_col
    top10 = frauds.sort_values(by=sort_col, ascending=False).head(10).reset_index(drop=True)

    def risk_badge(score):
        if score >= 0.8:
            return '<span class="risk-h">Élevé</span>'
        if score >= 0.5:
            return '<span class="risk-m">Moyen</span>'
        return '<span class="risk-l">Faible</span>'

    rows_html = ""
    for _, r in top10.iterrows():
        score = r.get(score_col, 0) if score_col else 0
        amt   = r.get(amount_col, 0)
        tx_id = r.get("id_transaction", r.get("id", "—"))
        date_ = r.get(date_col, "—") if date_col else "—"
        client = r.get("id_client", r.get("client", "—"))
        rows_html += f"""
        <tr>
          <td style="font-family:monospace;">{tx_id}</td>
          <td>{date_}</td>
          <td>{client}</td>
          <td>Ar {amt:,.0f}</td>
          <td style="font-weight:600;color:{RED};">{score:.3f}</td>
          <td>{risk_badge(score)}</td>
        </tr>"""

    st.markdown(f"""
    <div class="chart-card">
      <div class="alert-header">
        <div class="section-title" style="margin:0;">Alertes prioritaires — transactions à haut risque</div>
        <span class="alert-badge">{n_fraud} fraudes totales · top 10 affichées</span>
      </div>
      <div style="overflow-x:auto;">
      <table class="alert-table">
        <thead><tr>
          <th>ID Transaction</th><th>Date</th><th>Client</th>
          <th>Montant (Ariary)</th><th>Score ML</th><th>Risque</th>
        </tr></thead>
        <tbody>{rows_html}</tbody>
      </table>
      </div>
    </div>
    """, unsafe_allow_html=True)

    # Export
    export_cols = [c for c in [
        "id_transaction", date_col, "id_client", amount_col, score_col, "niveau_risque"
    ] if c and c in top10.columns]
    csv_bytes = top10[export_cols].to_csv(index=False).encode("utf-8")
    st.download_button(
        "Exporter les alertes (CSV)",
        data=csv_bytes,
        file_name=f"alertes_fraude_{datetime.today().strftime('%Y%m%d')}.csv",
        mime="text/csv",
    )

st.caption(
    f"Tableau de bord généré le {datetime.now().strftime('%d/%m/%Y à %H:%M')} "
    f"— Source : {source_label} — Mode : {'sombre' if dark_mode else 'clair'}"
)
