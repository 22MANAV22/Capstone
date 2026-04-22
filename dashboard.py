"""
dashboard.py — E-Commerce Data Lakehouse BI Dashboard
Streamlit in Snowflake version — uses st.connection("snowflake") natively.
No secrets file needed. No external connector import needed.

Packages already installed in Snowflake environment:
  pandas 3.0.2 | plotly 6.6.0 | snowflake-connector-python 4.4.0 | streamlit
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="E-Commerce Lakehouse",
    page_icon="◈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── Global CSS — luxury dark terminal aesthetic ───────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@300;400;500&family=Syne:wght@400;600;700;800&family=DM+Sans:wght@300;400;500&display=swap');

:root {
    --bg-primary:    #080C14;
    --bg-secondary:  #0D1421;
    --bg-card:       #111827;
    --bg-card-hover: #161F30;
    --border:        #1E2D45;
    --border-bright: #2A3F5F;
    --gold:          #F5A623;
    --gold-dim:      #C8841A;
    --teal:          #00D4B4;
    --coral:         #FF6B6B;
    --blue:          #4B9EFF;
    --purple:        #A855F7;
    --text-primary:  #E8EFF8;
    --text-secondary:#7A92B0;
    --text-muted:    #3D5470;
    --font-display:  'Syne', sans-serif;
    --font-body:     'DM Sans', sans-serif;
    --font-mono:     'DM Mono', monospace;
}

html, body, [class*="css"] {
    font-family: var(--font-body) !important;
    background-color: var(--bg-primary) !important;
    color: var(--text-primary) !important;
}
.main .block-container {
    background-color: var(--bg-primary) !important;
    padding: 1.5rem 2rem 3rem !important;
    max-width: 1600px !important;
}
[data-testid="stSidebar"] {
    background-color: var(--bg-secondary) !important;
    border-right: 1px solid var(--border) !important;
}
[data-testid="stSidebar"] * { color: var(--text-primary) !important; }
[data-testid="stSidebar"] .stSelectbox label,
[data-testid="stSidebar"] .stMultiSelect label,
[data-testid="stSidebar"] .stSlider label {
    font-family: var(--font-mono) !important;
    font-size: 0.7rem !important;
    letter-spacing: 0.12em !important;
    text-transform: uppercase !important;
    color: var(--text-secondary) !important;
}
[data-testid="stSidebar"] [data-baseweb="select"] {
    background-color: var(--bg-card) !important;
    border: 1px solid var(--border) !important;
    border-radius: 6px !important;
}
[data-testid="metric-container"] {
    background: var(--bg-card) !important;
    border: 1px solid var(--border) !important;
    border-radius: 12px !important;
    padding: 1.2rem 1.4rem !important;
    transition: all 0.3s ease !important;
}
[data-testid="metric-container"]:hover {
    border-color: var(--border-bright) !important;
    background: var(--bg-card-hover) !important;
    transform: translateY(-2px) !important;
    box-shadow: 0 8px 32px rgba(0,0,0,0.4) !important;
}
[data-testid="metric-container"] label {
    font-family: var(--font-mono) !important;
    font-size: 0.65rem !important;
    letter-spacing: 0.15em !important;
    text-transform: uppercase !important;
    color: var(--text-secondary) !important;
}
[data-testid="metric-container"] [data-testid="stMetricValue"] {
    font-family: var(--font-display) !important;
    font-size: 2rem !important;
    font-weight: 700 !important;
    color: var(--text-primary) !important;
    line-height: 1.1 !important;
}
h1, h2, h3 { font-family: var(--font-display) !important; letter-spacing: -0.02em !important; }
h1 { font-size: 2.4rem !important; font-weight: 800 !important; }
h2 { font-size: 1.4rem !important; font-weight: 700 !important; }
h3 { font-size: 1.1rem !important; font-weight: 600 !important; color: var(--text-secondary) !important; }
hr { border-color: var(--border) !important; margin: 1.5rem 0 !important; }
.section-label {
    font-family: var(--font-mono);
    font-size: 0.65rem;
    letter-spacing: 0.2em;
    text-transform: uppercase;
    color: var(--text-muted);
    margin-bottom: 0.8rem;
    display: flex;
    align-items: center;
    gap: 8px;
}
.section-label::before {
    content: '';
    display: inline-block;
    width: 20px;
    height: 1px;
    background: var(--gold);
}
.kpi-gold  { border-top: 2px solid var(--gold)   !important; }
.kpi-teal  { border-top: 2px solid var(--teal)   !important; }
.kpi-coral { border-top: 2px solid var(--coral)  !important; }
.kpi-blue  { border-top: 2px solid var(--blue)   !important; }
.kpi-purple{ border-top: 2px solid var(--purple) !important; }
[data-baseweb="tab-list"] {
    background-color: var(--bg-secondary) !important;
    border-bottom: 1px solid var(--border) !important;
    gap: 0 !important;
}
[data-baseweb="tab"] {
    font-family: var(--font-mono) !important;
    font-size: 0.72rem !important;
    letter-spacing: 0.1em !important;
    text-transform: uppercase !important;
    color: var(--text-secondary) !important;
    padding: 0.7rem 1.2rem !important;
}
[aria-selected="true"][data-baseweb="tab"] {
    color: var(--gold) !important;
    border-bottom: 2px solid var(--gold) !important;
    background: transparent !important;
}
.info-card {
    background: var(--bg-card);
    border: 1px solid var(--border);
    border-left: 3px solid var(--gold);
    border-radius: 8px;
    padding: 0.8rem 1rem;
    font-family: var(--font-mono);
    font-size: 0.75rem;
    color: var(--text-secondary);
    margin-bottom: 1rem;
    line-height: 1.8;
}
[data-testid="stDataFrame"] {
    border: 1px solid var(--border) !important;
    border-radius: 10px !important;
    overflow: hidden !important;
}
</style>
""", unsafe_allow_html=True)

# ── Plotly theme ──────────────────────────────────────────────────────────────
PLOTLY_THEME = dict(
    paper_bgcolor="#111827",
    plot_bgcolor="#111827",
    font=dict(family="DM Sans, sans-serif", color="#7A92B0", size=11),
    colorway=["#F5A623","#00D4B4","#4B9EFF","#A855F7","#FF6B6B","#34D399","#FB7185"],
    margin=dict(l=20, r=20, t=40, b=20),
    xaxis=dict(gridcolor="#1E2D45", linecolor="#1E2D45", tickfont=dict(size=10)),
    yaxis=dict(gridcolor="#1E2D45", linecolor="#1E2D45", tickfont=dict(size=10)),
    legend=dict(bgcolor="rgba(0,0,0,0)", bordercolor="#1E2D45"),
    hoverlabel=dict(bgcolor="#0D1421", bordercolor="#2A3F5F", font=dict(color="#E8EFF8")),
)

def apply_theme(fig, title="", height=380):
    fig.update_layout(
        **PLOTLY_THEME,
        title=dict(
            text=title,
            font=dict(family="Syne, sans-serif", size=13, color="#E8EFF8"),
            x=0.01
        ),
        height=height,
    )
    fig.update_xaxes(gridcolor="#1E2D45", linecolor="#1E2D45", zeroline=False)
    fig.update_yaxes(gridcolor="#1E2D45", linecolor="#1E2D45", zeroline=False)
    return fig

# ── Snowflake connection — native Streamlit in Snowflake ──────────────────────
# st.connection("snowflake") is automatically injected by Snowflake.
# No credentials, no secrets.toml needed.
conn = st.connection("snowflake")

@st.cache_data(ttl=600, show_spinner=False)
def query(sql):
    return conn.query(sql)

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style='padding:1.2rem 0 1.5rem'>
        <div style='font-family:Syne,sans-serif;font-size:1.1rem;font-weight:800;
        color:#E8EFF8;letter-spacing:-0.02em'>◈ LAKEHOUSE</div>
        <div style='font-family:DM Mono,monospace;font-size:0.6rem;letter-spacing:0.2em;
        color:#3D5470;margin-top:4px'>E-COMMERCE ANALYTICS</div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown('<div class="section-label">Filters</div>', unsafe_allow_html=True)

    all_statuses = query(
        "SELECT DISTINCT order_status FROM order_summary "
        "WHERE order_status IS NOT NULL ORDER BY 1"
    )
    status_options = ["All"] + all_statuses["ORDER_STATUS"].tolist()
    selected_status = st.selectbox("Order Status", status_options)

    all_states = query("""
        SELECT DISTINCT c.customer_state
        FROM fact_order_items f
        JOIN dim_customers c ON f.customer_id = c.customer_id
        WHERE c.customer_state IS NOT NULL
        ORDER BY 1
    """)
    state_options = ["All States"] + all_states["CUSTOMER_STATE"].tolist()
    selected_state = st.selectbox("Customer State", state_options)

    min_review, max_review = st.slider("Review Score Range", 1, 5, (1, 5))
    max_days = st.slider("Max Delivery Days", 10, 120, 60)

    st.markdown("---")
    st.markdown('<div class="section-label">Pipeline</div>', unsafe_allow_html=True)
    st.markdown("""
    <div class="info-card">
        S3 → Databricks<br>
        Delta Lake → Snowflake<br>
        Airflow on EC2
    </div>
    """, unsafe_allow_html=True)

    if st.button("↺  Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# ── Filter clauses ────────────────────────────────────────────────────────────
state_filter  = f"AND c.customer_state = '{selected_state}'" if selected_state != "All States" else ""
status_filter = f"AND order_status = '{selected_status}'"    if selected_status != "All"         else ""

# ── Header ────────────────────────────────────────────────────────────────────
col_title, col_time = st.columns([4, 1])
with col_title:
    st.markdown("""
    <h1 style='margin:0;background:linear-gradient(135deg,#E8EFF8 0%,#F5A623 100%);
    -webkit-background-clip:text;-webkit-text-fill-color:transparent;
    background-clip:text;padding-bottom:0.3rem'>
        E-Commerce Analytics
    </h1>
    <p style='font-family:DM Mono,monospace;font-size:0.7rem;letter-spacing:0.15em;
    color:#3D5470;margin:0;text-transform:uppercase'>
        ◈ Data Lakehouse · Real-Time Intelligence
    </p>
    """, unsafe_allow_html=True)
with col_time:
    st.markdown(f"""
    <div style='text-align:right;padding-top:0.5rem'>
        <div style='font-family:DM Mono,monospace;font-size:0.65rem;
        color:#3D5470;letter-spacing:0.1em'>LAST REFRESH</div>
        <div style='font-family:DM Mono,monospace;font-size:0.8rem;color:#F5A623'>
            {pd.Timestamp.now().strftime('%H:%M:%S')}
        </div>
    </div>
    """, unsafe_allow_html=True)

st.markdown("<hr>", unsafe_allow_html=True)

# ── KPI Metrics ───────────────────────────────────────────────────────────────
st.markdown('<div class="section-label">Key Performance Indicators</div>', unsafe_allow_html=True)

kpi = query("""
    SELECT
        ROUND(SUM(f.total_amount), 0)   AS total_revenue,
        COUNT(DISTINCT f.order_id)      AS total_orders,
        ROUND(AVG(f.delivery_days), 1)  AS avg_delivery_days,
        ROUND(AVG(CASE WHEN f.on_time_flag IS NOT NULL
              THEN f.on_time_flag END) * 100, 1)              AS on_time_pct,
        COUNT(DISTINCT f.customer_id)   AS unique_customers,
        ROUND(AVG(f.review_score), 2)   AS avg_review,
        ROUND(SUM(CASE WHEN f.delay_flag = 1
              THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1)      AS delay_pct,
        ROUND(AVG(f.total_amount), 2)   AS avg_order_value
    FROM fact_order_items f
    JOIN dim_customers c ON f.customer_id = c.customer_id
    WHERE f.review_score BETWEEN {min_r} AND {max_r}
    {sf}
""".format(min_r=min_review, max_r=max_review, sf=state_filter))

k = kpi.iloc[0]

c1,c2,c3,c4,c5,c6,c7,c8 = st.columns(8)
for container, css, label, value in [
    (c1, "kpi-gold",   "Total Revenue",  f"R${k['TOTAL_REVENUE']:,.0f}"),
    (c2, "kpi-teal",   "Total Orders",   f"{k['TOTAL_ORDERS']:,}"),
    (c3, "kpi-blue",   "Customers",      f"{k['UNIQUE_CUSTOMERS']:,}"),
    (c4, "kpi-purple", "Avg Order",      f"R${k['AVG_ORDER_VALUE']:,.0f}"),
    (c5, "kpi-gold",   "Avg Delivery",   f"{k['AVG_DELIVERY_DAYS']} d"),
    (c6, "kpi-teal",   "On-Time Rate",   f"{k['ON_TIME_PCT']}%"),
    (c7, "kpi-coral",  "Delay Rate",     f"{k['DELAY_PCT']}%"),
    (c8, "kpi-blue",   "Avg Review",     f"★ {k['AVG_REVIEW']}"),
]:
    with container:
        st.markdown(f'<div class="{css}">', unsafe_allow_html=True)
        st.metric(label, value)
        st.markdown('</div>', unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "◈  Revenue", "◈  Delivery", "◈  Customers", "◈  Sellers", "◈  Products"
])

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — Revenue
# ══════════════════════════════════════════════════════════════════════════════
with tab1:
    col_l, col_r = st.columns([3, 2])

    with col_l:
        rev_state = query("""
            SELECT
                c.customer_state                  AS state,
                ROUND(SUM(f.total_amount), 0)     AS revenue,
                COUNT(DISTINCT f.order_id)         AS orders
            FROM fact_order_items f
            JOIN dim_customers c ON f.customer_id = c.customer_id
            WHERE f.review_score BETWEEN {min_r} AND {max_r} {sf}
            GROUP BY c.customer_state
            ORDER BY revenue DESC LIMIT 15
        """.format(min_r=min_review, max_r=max_review, sf=state_filter))

        fig = go.Figure(go.Bar(
            x=rev_state["REVENUE"], y=rev_state["STATE"], orientation="h",
            marker=dict(
                color=rev_state["REVENUE"],
                colorscale=[[0,"#1E2D45"],[0.5,"#C8841A"],[1,"#F5A623"]],
                showscale=False,
            ),
            text=[f"R${v:,.0f}" for v in rev_state["REVENUE"]],
            textposition="outside",
            textfont=dict(size=9, color="#7A92B0", family="DM Mono"),
            hovertemplate="<b>%{y}</b><br>R$%{x:,.0f}<extra></extra>",
        ))
        apply_theme(fig, "Revenue by State  (Top 15)", height=430)
        fig.update_layout(yaxis=dict(categoryorder="total ascending"))
        st.plotly_chart(fig, use_container_width=True)

    with col_r:
        rev_status = query("""
            SELECT order_status,
                   ROUND(SUM(total_amount), 0) AS revenue
            FROM order_summary
            WHERE 1=1 {sf}
            GROUP BY order_status ORDER BY revenue DESC
        """.format(sf=status_filter))

        fig2 = go.Figure(go.Pie(
            labels=rev_status["ORDER_STATUS"], values=rev_status["REVENUE"],
            hole=0.6,
            marker=dict(
                colors=["#F5A623","#00D4B4","#4B9EFF","#A855F7","#FF6B6B","#34D399"],
                line=dict(color="#080C14", width=2)
            ),
            textinfo="label+percent",
            textfont=dict(size=10, family="DM Mono"),
            hovertemplate="<b>%{label}</b><br>R$%{value:,.0f}<extra></extra>",
        ))
        total_rev = rev_status["REVENUE"].sum()
        apply_theme(fig2, "Revenue by Order Status", height=300)
        fig2.update_layout(annotations=[dict(
            text=f"R${total_rev/1e6:.1f}M",
            x=0.5, y=0.5,
            font=dict(size=18, family="Syne", color="#E8EFF8"),
            showarrow=False
        )])
        st.plotly_chart(fig2, use_container_width=True)

        monthly = query("""
            SELECT DATE_TRUNC('month', purchase_timestamp) AS month,
                   COUNT(DISTINCT order_id)                 AS orders,
                   ROUND(SUM(total_amount), 0)              AS revenue
            FROM fact_order_items
            WHERE purchase_timestamp IS NOT NULL
            GROUP BY 1 ORDER BY 1 LIMIT 24
        """)
        if len(monthly) > 0:
            fig3 = go.Figure(go.Scatter(
                x=monthly["MONTH"], y=monthly["REVENUE"],
                fill="tozeroy",
                fillcolor="rgba(245,166,35,0.08)",
                line=dict(color="#F5A623", width=2),
                hovertemplate="%{x|%b %Y}<br>R$%{y:,.0f}<extra></extra>",
            ))
            apply_theme(fig3, "Monthly Revenue Trend", height=205)
            fig3.update_layout(showlegend=False)
            st.plotly_chart(fig3, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — Delivery
# ══════════════════════════════════════════════════════════════════════════════
with tab2:
    col_l, col_r = st.columns(2)

    with col_l:
        delivery_dist = query("""
            SELECT delivery_days, COUNT(*) AS orders, delay_flag
            FROM fact_order_items
            WHERE delivery_days BETWEEN 0 AND {max_d}
              AND delay_flag IS NOT NULL
              AND review_score BETWEEN {min_r} AND {max_r}
            GROUP BY delivery_days, delay_flag
            ORDER BY delivery_days
        """.format(max_d=max_days, min_r=min_review, max_r=max_review))

        on_time = delivery_dist[delivery_dist["DELAY_FLAG"] == 0]
        delayed = delivery_dist[delivery_dist["DELAY_FLAG"] == 1]
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=on_time["DELIVERY_DAYS"], y=on_time["ORDERS"],
            name="On Time", marker_color="#00D4B4", opacity=0.85,
            hovertemplate="Day %{x}: %{y:,} orders<extra>On Time</extra>"
        ))
        fig.add_trace(go.Bar(
            x=delayed["DELIVERY_DAYS"], y=delayed["ORDERS"],
            name="Delayed", marker_color="#FF6B6B", opacity=0.85,
            hovertemplate="Day %{x}: %{y:,} orders<extra>Delayed</extra>"
        ))
        apply_theme(fig, "Delivery Days Distribution", height=380)
        fig.update_layout(barmode="stack", bargap=0.05)
        st.plotly_chart(fig, use_container_width=True)

    with col_r:
        ontime_state = query("""
            SELECT c.customer_state                        AS state,
                   ROUND(AVG(f.on_time_flag)*100, 1)       AS on_time_rate,
                   COUNT(DISTINCT f.order_id)               AS orders,
                   ROUND(AVG(f.delivery_days), 1)           AS avg_days
            FROM fact_order_items f
            JOIN dim_customers c ON f.customer_id = c.customer_id
            WHERE f.on_time_flag IS NOT NULL
              AND f.review_score BETWEEN {min_r} AND {max_r} {sf}
            GROUP BY c.customer_state
            HAVING COUNT(*) > 50
            ORDER BY on_time_rate DESC LIMIT 20
        """.format(min_r=min_review, max_r=max_review, sf=state_filter))

        fig2 = go.Figure(go.Scatter(
            x=ontime_state["AVG_DAYS"], y=ontime_state["ON_TIME_RATE"],
            mode="markers+text",
            marker=dict(
                size=ontime_state["ORDERS"] / ontime_state["ORDERS"].max() * 30 + 8,
                color=ontime_state["ON_TIME_RATE"],
                colorscale=[[0,"#FF6B6B"],[0.5,"#F5A623"],[1,"#00D4B4"]],
                showscale=True,
                colorbar=dict(
                    title=dict(text="On-Time %", font=dict(size=9, color="#7A92B0")),
                    tickfont=dict(size=8, color="#7A92B0"), thickness=8,
                ),
                line=dict(color="#080C14", width=1),
            ),
            text=ontime_state["STATE"],
            textposition="top center",
            textfont=dict(size=8, color="#7A92B0", family="DM Mono"),
            hovertemplate="<b>%{text}</b><br>Avg days: %{x}<br>On-time: %{y}%<extra></extra>",
        ))
        apply_theme(fig2, "On-Time Rate vs Avg Delivery Days", height=380)
        fig2.update_xaxes(title_text="Avg Delivery Days", title_font=dict(size=10))
        fig2.update_yaxes(title_text="On-Time Rate (%)", title_font=dict(size=10))
        st.plotly_chart(fig2, use_container_width=True)

    delay_review = query("""
        SELECT CAST(review_score AS INT) AS score,
               delay_flag,
               COUNT(*) AS cnt
        FROM fact_order_items
        WHERE review_score > 0 AND delay_flag IS NOT NULL AND delivery_days IS NOT NULL
        GROUP BY 1, 2 ORDER BY 1, 2
    """)
    if len(delay_review) > 0:
        pivot = delay_review.pivot_table(
            index="DELAY_FLAG", columns="SCORE", values="CNT", fill_value=0
        )
        fig3 = go.Figure(go.Heatmap(
            z=pivot.values,
            x=[f"★{c}" for c in pivot.columns],
            y=["On Time", "Delayed"],
            colorscale=[[0,"#0D1421"],[0.5,"#C8841A"],[1,"#F5A623"]],
            showscale=True,
            colorbar=dict(tickfont=dict(size=9, color="#7A92B0"), thickness=8),
            text=pivot.values,
            texttemplate="%{text:,}",
            textfont=dict(size=10, family="DM Mono", color="#E8EFF8"),
            hovertemplate="Score %{x} | %{y}<br>Orders: %{z:,}<extra></extra>",
        ))
        apply_theme(fig3, "Order Count: Delay Flag × Review Score", height=200)
        st.plotly_chart(fig3, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — Customers
# ══════════════════════════════════════════════════════════════════════════════
with tab3:
    col_l, col_r = st.columns([2, 3])

    with col_l:
        clv = query("""
            SELECT total_spent, total_orders, avg_review_score
            FROM customer_metrics
            WHERE total_spent > 0
            ORDER BY total_spent DESC LIMIT 5000
        """)
        fig = go.Figure(go.Histogram(
            x=clv["TOTAL_SPENT"], nbinsx=60,
            marker=dict(color="#F5A623", opacity=0.8,
                        line=dict(color="#080C14", width=0.5)),
            hovertemplate="R$%{x:,.0f} — %{y:,} customers<extra></extra>",
        ))
        apply_theme(fig, "Customer Lifetime Value Distribution", height=300)
        fig.update_xaxes(title_text="Total Spend (BRL)", title_font=dict(size=10))
        st.plotly_chart(fig, use_container_width=True)

        top_cust = query("""
            SELECT LEFT(customer_id,8)||'...' AS customer,
                   total_orders,
                   ROUND(total_spent, 0)      AS total_spent,
                   ROUND(avg_order_value, 0)  AS avg_order,
                   ROUND(avg_review_score, 1) AS review
            FROM customer_metrics
            ORDER BY total_spent DESC LIMIT 10
        """)
        st.markdown('<div class="section-label">Top 10 Customers by LTV</div>',
                    unsafe_allow_html=True)
        st.dataframe(top_cust, use_container_width=True, height=280,
            column_config={
                "CUSTOMER":     st.column_config.TextColumn("Customer ID"),
                "TOTAL_ORDERS": st.column_config.NumberColumn("Orders"),
                "TOTAL_SPENT":  st.column_config.NumberColumn("Spend (R$)", format="R$%d"),
                "AVG_ORDER":    st.column_config.NumberColumn("Avg Order", format="R$%d"),
                "REVIEW":       st.column_config.NumberColumn("Review", format="★%.1f"),
            })

    with col_r:
        sample = clv.sample(min(1500, len(clv)))
        fig2 = go.Figure(go.Scatter(
            x=sample["TOTAL_ORDERS"], y=sample["TOTAL_SPENT"],
            mode="markers",
            marker=dict(
                size=5,
                color=sample["AVG_REVIEW_SCORE"],
                colorscale=[[0,"#FF6B6B"],[0.5,"#F5A623"],[1,"#00D4B4"]],
                showscale=True, opacity=0.65,
                colorbar=dict(
                    title=dict(text="Avg Review", font=dict(size=9, color="#7A92B0")),
                    tickfont=dict(size=8, color="#7A92B0"), thickness=8
                ),
                line=dict(color="rgba(0,0,0,0)", width=0),
            ),
            hovertemplate="Orders: %{x}<br>Spent: R$%{y:,.0f}<extra></extra>",
        ))
        apply_theme(fig2, "Customer Spend vs Order Frequency (color = review score)", height=360)
        fig2.update_xaxes(title_text="Number of Orders", title_font=dict(size=10))
        fig2.update_yaxes(title_text="Total Spend (BRL)", title_font=dict(size=10))
        st.plotly_chart(fig2, use_container_width=True)

        state_rev = query("""
            SELECT c.customer_state                  AS state,
                   ROUND(SUM(f.total_amount), 0)     AS revenue
            FROM fact_order_items f
            JOIN dim_customers c ON f.customer_id = c.customer_id
            GROUP BY c.customer_state ORDER BY revenue DESC
        """)
        fig3 = go.Figure(go.Treemap(
            labels=state_rev["STATE"], values=state_rev["REVENUE"],
            parents=[""] * len(state_rev),
            textinfo="label+value+percent root",
            textfont=dict(size=11, family="DM Mono"),
            marker=dict(
                colors=state_rev["REVENUE"],
                colorscale=[[0,"#0D1421"],[0.4,"#1E3A5F"],[0.7,"#C8841A"],[1,"#F5A623"]],
                showscale=False,
                line=dict(color="#080C14", width=2),
            ),
            hovertemplate="<b>%{label}</b><br>R$%{value:,.0f}<extra></extra>",
        ))
        apply_theme(fig3, "Revenue Share by State", height=290)
        st.plotly_chart(fig3, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — Sellers
# ══════════════════════════════════════════════════════════════════════════════
with tab4:
    col_l, col_r = st.columns(2)

    sellers = query("""
        SELECT LEFT(seller_id,8)||'...'        AS seller,
               total_orders,
               ROUND(total_revenue, 0)          AS revenue,
               ROUND(avg_review_score, 2)       AS review,
               ROUND(on_time_delivery_rate, 1)  AS on_time,
               ROUND(avg_delivery_days, 1)      AS avg_days
        FROM seller_performance
        WHERE total_orders >= 5
        ORDER BY revenue DESC LIMIT 200
    """)

    with col_l:
        fig = go.Figure(go.Scatter(
            x=sellers["REVENUE"], y=sellers["REVIEW"],
            mode="markers",
            marker=dict(
                size=sellers["ON_TIME"] / 5 + 5,
                color=sellers["ON_TIME"],
                colorscale=[[0,"#FF6B6B"],[0.5,"#F5A623"],[1,"#00D4B4"]],
                showscale=True, opacity=0.8,
                colorbar=dict(
                    title=dict(text="On-Time %", font=dict(size=9, color="#7A92B0")),
                    tickfont=dict(size=8, color="#7A92B0"), thickness=8
                ),
                line=dict(color="#080C14", width=1),
            ),
            text=sellers["SELLER"],
            hovertemplate="<b>%{text}</b><br>Revenue: R$%{x:,.0f}<br>"
                          "Review: %{y}★<br>On-time: %{marker.color:.1f}%<extra></extra>",
        ))
        apply_theme(fig, "Revenue vs Review Score (bubble = on-time rate)", height=400)
        fig.update_xaxes(title_text="Total Revenue (BRL)", title_font=dict(size=10))
        fig.update_yaxes(title_text="Avg Review Score", title_font=dict(size=10))
        st.plotly_chart(fig, use_container_width=True)

    with col_r:
        top15 = sellers.head(15).sort_values("REVENUE")
        fig2 = go.Figure(go.Bar(
            x=top15["REVENUE"], y=top15["SELLER"], orientation="h",
            marker=dict(
                color=top15["REVIEW"],
                colorscale=[[0,"#FF6B6B"],[0.5,"#F5A623"],[1,"#00D4B4"]],
                showscale=False,
                line=dict(color="#080C14", width=0.5),
            ),
            text=[f"★{r}" for r in top15["REVIEW"]],
            textposition="inside",
            textfont=dict(size=9, color="#080C14", family="DM Mono"),
            hovertemplate="<b>%{y}</b><br>R$%{x:,.0f}<extra></extra>",
        ))
        apply_theme(fig2, "Top 15 Sellers by Revenue (color = review score)", height=400)
        fig2.update_layout(yaxis=dict(categoryorder="total ascending"))
        st.plotly_chart(fig2, use_container_width=True)

    fig3 = go.Figure(go.Histogram(
        x=sellers["ON_TIME"], nbinsx=40,
        marker=dict(color="#00D4B4", opacity=0.8,
                    line=dict(color="#080C14", width=0.5)),
        hovertemplate="%{x:.1f}% on-time — %{y:,} sellers<extra></extra>",
    ))
    mean_ot = sellers["ON_TIME"].mean()
    fig3.add_vline(x=mean_ot, line=dict(color="#F5A623", width=1.5, dash="dash"),
        annotation_text=f"Mean: {mean_ot:.1f}%",
        annotation_font=dict(size=10, color="#F5A623", family="DM Mono"))
    apply_theme(fig3, "On-Time Delivery Rate Distribution Across Sellers", height=220)
    fig3.update_xaxes(title_text="On-Time Delivery Rate (%)", title_font=dict(size=10))
    st.plotly_chart(fig3, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# TAB 5 — Products
# ══════════════════════════════════════════════════════════════════════════════
with tab5:
    col_l, col_r = st.columns([3, 2])

    cat_perf = query("""
        SELECT COALESCE(p.product_category_name_english,'Unknown') AS category,
               ROUND(SUM(pp.total_sales), 0)    AS total_sales,
               SUM(pp.total_quantity)            AS total_qty,
               ROUND(AVG(pp.avg_price), 0)       AS avg_price,
               ROUND(AVG(pp.avg_review_score),2) AS avg_review
        FROM product_performance pp
        JOIN dim_products p ON pp.product_id = p.product_id
        GROUP BY category ORDER BY total_sales DESC LIMIT 20
    """)

    with col_l:
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(go.Bar(
            x=cat_perf["CATEGORY"], y=cat_perf["TOTAL_SALES"],
            name="Revenue",
            marker=dict(
                color=cat_perf["TOTAL_SALES"],
                colorscale=[[0,"#1E2D45"],[1,"#F5A623"]],
                showscale=False,
            ),
            hovertemplate="%{x}<br>R$%{y:,.0f}<extra>Revenue</extra>",
        ), secondary_y=False)
        fig.add_trace(go.Scatter(
            x=cat_perf["CATEGORY"], y=cat_perf["AVG_REVIEW"],
            name="Avg Review", mode="lines+markers",
            line=dict(color="#00D4B4", width=2),
            marker=dict(size=6, color="#00D4B4"),
            hovertemplate="%{x}<br>★%{y:.2f}<extra>Review</extra>",
        ), secondary_y=True)

        apply_theme(fig, "Category Revenue & Review Score (Top 20)", height=420)
        fig.update_xaxes(tickangle=-35, tickfont=dict(size=8))
        fig.update_yaxes(title_text="Revenue (BRL)", secondary_y=False,
                         title_font=dict(size=10))
        fig.update_yaxes(title_text="Avg Review", secondary_y=True,
                         title_font=dict(size=10), range=[0, 5.5], showgrid=False)
        st.plotly_chart(fig, use_container_width=True)

    with col_r:
        prod_sample = query("""
            SELECT COALESCE(p.product_category_name_english,'Other') AS category,
                   pp.avg_price, pp.avg_review_score, pp.total_sales
            FROM product_performance pp
            JOIN dim_products p ON pp.product_id = p.product_id
            WHERE pp.total_quantity >= 3 AND pp.avg_price > 0
            LIMIT 2000
        """)
        fig2 = go.Figure(go.Scatter(
            x=prod_sample["AVG_PRICE"], y=prod_sample["AVG_REVIEW_SCORE"],
            mode="markers",
            marker=dict(
                size=4,
                color=prod_sample["TOTAL_SALES"],
                colorscale=[[0,"#1E2D45"],[0.5,"#4B9EFF"],[1,"#F5A623"]],
                opacity=0.6, showscale=True,
                colorbar=dict(
                    title=dict(text="Total Sales", font=dict(size=9, color="#7A92B0")),
                    tickfont=dict(size=8, color="#7A92B0"), thickness=8
                ),
            ),
            hovertemplate="Price: R$%{x:,.0f}<br>Review: %{y:.2f}★<extra></extra>",
        ))
        apply_theme(fig2, "Product Price vs Review Score", height=300)
        fig2.update_xaxes(title_text="Avg Price (BRL)", title_font=dict(size=10))
        fig2.update_yaxes(title_text="Avg Review Score", title_font=dict(size=10))
        st.plotly_chart(fig2, use_container_width=True)

        top_cat = cat_perf.head(12)
        fig3 = go.Figure(go.Pie(
            labels=top_cat["CATEGORY"], values=top_cat["TOTAL_QTY"],
            hole=0.5,
            marker=dict(
                colors=["#F5A623","#00D4B4","#4B9EFF","#A855F7","#FF6B6B",
                        "#34D399","#FB7185","#FBBF24","#60A5FA","#C084FC",
                        "#F87171","#6EE7B7"],
                line=dict(color="#080C14", width=2)
            ),
            textinfo="label+percent",
            textfont=dict(size=9, family="DM Mono"),
            hovertemplate="<b>%{label}</b><br>%{value:,} units<extra></extra>",
        ))
        apply_theme(fig3, "Units Sold by Category", height=280)
        st.plotly_chart(fig3, use_container_width=True)

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown("<hr>", unsafe_allow_html=True)
st.markdown("""
<div style='display:flex;justify-content:space-between;align-items:center;
padding:0.5rem 0;font-family:DM Mono,monospace;font-size:0.62rem;
letter-spacing:0.1em;color:#3D5470'>
    <span>◈ E-COMMERCE DATA LAKEHOUSE  ·  CAPSTONE TEAM 8</span>
    <span>S3 → DATABRICKS → SNOWFLAKE → STREAMLIT</span>
    <span>CACHE TTL: 10 MIN</span>
</div>
""", unsafe_allow_html=True)