import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
from datetime import date, timedelta
import boto3
import awswrangler as wr
import json
import re
from typing import Union
from decimal import Decimal

# ---------------------------- Page config ----------------------------
st.set_page_config(
    page_title="ProcureIQ | P2P Analytics",
    layout="wide",
    page_icon=":bar_chart:",
)

# ---------------------------- Athena configuration ----------------------------
DATABASE = "procure2pay"
ATHENA_REGION = "us-east-1"
BEDROCK_MODEL_ID = "amazon.nova-micro-v1:0"

session = boto3.Session()
athena_client = session.client("athena", region_name=ATHENA_REGION)
bedrock_runtime = session.client("bedrock-runtime", region_name=ATHENA_REGION)

def run_query(sql: str) -> pd.DataFrame:
    """Execute SQL on Athena and return DataFrame with Decimal->float conversion."""
    try:
        df = wr.athena.read_sql_query(sql, database=DATABASE, boto3_session=session)
        for col in df.columns:
            if df[col].dtype == object and df[col].apply(lambda x: isinstance(x, Decimal)).any():
                df[col] = df[col].astype(float)
        return df
    except Exception as e:
        st.error(f"Athena query failed: {e}\nSQL: {sql[:500]}")
        return pd.DataFrame()

# ---------------------------- Helper functions ----------------------------
def safe_number(val, default=0.0):
    try:
        if pd.isna(val):
            return default
        return float(val)
    except Exception:
        return default

def safe_int(val, default=0):
    try:
        if pd.isna(val):
            return default
        return int(float(val))
    except Exception:
        return default

def abbr_currency(v: float, currency_symbol: str = "$") -> str:
    n = abs(v)
    sign = "-" if v < 0 else ""
    if n >= 1_000_000_000:
        return f"{sign}{currency_symbol}{n/1_000_000_000:.1f}B"
    if n >= 1_000_000:
        return f"{sign}{currency_symbol}{n/1_000_000:.1f}M"
    if n >= 1_000:
        return f"{sign}{currency_symbol}{n/1_000:.1f}K"
    return f"{sign}{currency_symbol}{n:.0f}"

def compute_range_preset(preset: str):
    today = date.today()
    if preset == "Last 30 Days":
        return today - timedelta(days=30), today
    if preset == "QTD":
        start = date(today.year, ((today.month - 1)//3)*3 + 1, 1)
        return start, today
    if preset == "YTD":
        return date(today.year, 1, 1), today
    return today.replace(day=1), today

def prior_window(start: date, end: date):
    days = (end - start).days + 1
    prev_end = start - timedelta(days=1)
    prev_start = prev_end - timedelta(days=days - 1)
    return prev_start, prev_end

def sql_date(d: date) -> str:
    return f"DATE '{d.strftime('%Y-%m-%d')}'"

def build_vendor_where(selected_vendor: str) -> str:
    if selected_vendor == "All Vendors":
        return ""
    safe_vendor = selected_vendor.replace("'", "''")
    return f"AND UPPER(v.vendor_name) = UPPER('{safe_vendor}')"

def pct_delta(cur, prev):
    if prev == 0:
        if cur == 0:
            return "0%", True, True
        return "+100%", True, False
    change = (cur - prev) / prev * 100
    if abs(change) < 0.05:
        return "0%", True, True
    sign = "+" if change >= 0 else "−"
    return f"{sign}{abs(change):.1f}%", change >= 0, False

# ---------------------------- AI Chat Functions (unchanged) ----------------------------
SYSTEM_PROMPT = """
You are an AI assistant that helps users query a procurement database using SQL (Athena/Presto). Given a user's natural language question, generate a valid SQL query for Athena (Presto dialect) based on the following schema.

Tables and views in the `procure2pay` database:

1. `fact_all_sources_vw` – main fact table with invoice and PO data. Columns:
   - invoice_number, invoice_amount_local, posting_date (DATE), due_date, purchase_order_reference, invoice_status (Open, Due, Overdue, Disputed, Paid, etc.), vendor_id, company_code, plant_code, aging_days, po_amount, po_purpose, payment_date, etc.

2. `dim_vendor_vw` – vendor master data. Columns: vendor_id, vendor_name, vendor_name_2, country_code, city, postal_code, street, region_code, industry_sector, vendor_account_group, tax_number_1, tax_number_2, deletion_flag, posting_block, system.

3. `invoice_status_history_vw` – status change history. Columns: invoice_number, status, effective_date, status_notes, sequence_nbr, posting_date, due_date, vendor_id, invoice_amount_local, payment_date, clearing_document, aging_days, purchase_order_reference, po_amount, po_purpose, document_type, discount_percent, region, system.

4. `cash_flow_unpaid_obligations_vw` – unpaid invoices for cash flow. Columns: document_number, vendor_id, invoice_amount_local, due_date, invoice_status, days_until_due.

5. `payment_processing_cycle_time_vw` – payment cycle metrics. Columns: year, month, avg_payment_cycle_time_days, cleared_invoices.

6. `gr_ir_outstanding_balance_vw` – GR/IR outstanding. Columns: year, month, invoice_count, total_grir_blnc.

7. `gr_ir_aging_vw` – GR/IR aging. Columns: year, month, age_days, total_grir_balance, grir_over_30, grir_over_60, grir_over_90, pct_grir_over_30, pct_grir_over_60, pct_grir_over_90, cnt_grir_over_30, cnt_grir_over_60, cnt_grir_over_90.

8. `dim_company_code_vw` – company codes. Columns: company_code, company_name, street, city, postal_code, country_code, region_code, currency, vat_reg_number, chart_of_accounts, system.

9. `dim_plant_vw` – plant master. Columns: plant_code, plant_name, plant_name_2, company_code, country_code, region_code, city, postal_code, street, system.

Important notes:
- Use standard Presto/Athena SQL functions (DATE_TRUNC, DATE_ADD, DATE_DIFF, etc.).
- For date filtering, prefer `posting_date BETWEEN DATE '...' AND DATE '...'`.
- Always use COALESCE for null amounts.
- Exclude CANCELLED and REJECTED invoices from spend metrics unless asked.
- For aggregate queries, always add a reasonable LIMIT (e.g., 100) unless the user asks for all rows.
- Output only a JSON object with two keys: "sql" containing the SQL query string, and "explanation" containing a brief explanation of what the query does. Do not include any other text.
"""

def ask_bedrock(prompt: str, system_prompt: str = SYSTEM_PROMPT) -> str:
    try:
        body = json.dumps({
            "messages": [
                {
                    "role": "user",
                    "content": [{"text": prompt}]
                }
            ],
            "system": [{"text": system_prompt}],
            "inferenceConfig": {
                "maxTokens": 4096,
                "temperature": 0.0,
                "topP": 0.9
            }
        })
        response = bedrock_runtime.invoke_model(
            modelId=BEDROCK_MODEL_ID,
            contentType="application/json",
            accept="application/json",
            body=body
        )
        response_body = json.loads(response['body'].read())
        return response_body['output']['message']['content'][0]['text']
    except Exception as e:
        st.error(f"Bedrock invocation failed: {e}")
        return ""

def generate_sql(question: str) -> tuple:
    prompt = f"User question: {question}\n\nGenerate SQL query and explanation as JSON."
    response = ask_bedrock(prompt)
    if not response:
        return None, "Bedrock returned empty response."
    json_match = re.search(r'\{.*\}$', response, re.DOTALL)
    json_str = json_match.group(0) if json_match else response
    try:
        data = json.loads(json_str)
        sql = data.get("sql", "").strip()
        explanation = data.get("explanation", "")
        return sql, explanation
    except json.JSONDecodeError as e:
        st.error(f"Failed to parse JSON: {e}\nResponse: {response}")
        return None, "Could not parse SQL from AI response."

def is_safe_sql(sql: str) -> bool:
    sql_lower = sql.lower().strip()
    if not sql_lower.startswith("select"):
        return False
    dangerous = ["insert", "update", "delete", "drop", "alter", "create", "truncate", "grant", "revoke"]
    for word in dangerous:
        if re.search(r'\b' + word + r'\b', sql_lower):
            return False
    return True

def ensure_limit(sql: str, default_limit: int = 100) -> str:
    sql_lower = sql.lower()
    if "limit" in sql_lower:
        return sql
    if re.search(r'\b(count|sum|avg|min|max)\b', sql_lower) and "group by" not in sql_lower:
        return sql
    return f"{sql.rstrip(';')} LIMIT {default_limit}"

def auto_chart(df: pd.DataFrame) -> Union[alt.Chart, None]:
    if df.empty or len(df) > 200:
        return None
    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
    if not numeric_cols:
        return None
    dim_candidates = [c for c in df.columns if c not in numeric_cols]
    if dim_candidates:
        dim = dim_candidates[0]
        if len(numeric_cols) == 1:
            chart = alt.Chart(df).mark_bar().encode(
                x=alt.X(dim, sort=None),
                y=alt.Y(numeric_cols[0]),
                tooltip=[dim, numeric_cols[0]]
            )
        else:
            melted = df.melt(id_vars=[dim], value_vars=numeric_cols)
            chart = alt.Chart(melted).mark_line(point=True).encode(
                x=alt.X(dim, sort=None),
                y=alt.Y('value', title='Value'),
                color='variable',
                tooltip=[dim, 'variable', 'value']
            )
        return chart.interactive()
    return None

def render_genie():
    st.subheader("🤖 YashNovaAI – Genie")
    st.markdown("Ask any question about your procurement data in plain English. The AI will generate SQL, run it on Athena, and explain the results.")
    if "messages" not in st.session_state:
        st.session_state.messages = []
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
            if "sql" in msg:
                with st.expander("🔍 View SQL"):
                    st.code(msg["sql"], language="sql")
            if "df" in msg and msg["df"] is not None and not msg["df"].empty:
                st.dataframe(msg["df"], use_container_width=True)
                chart = auto_chart(msg["df"])
                if chart:
                    st.altair_chart(chart, use_container_width=True)
    if prompt := st.chat_input("Ask a question, e.g., 'Show top 5 vendors by spend this year'..."):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)
        with st.chat_message("assistant"):
            with st.spinner("Generating SQL query..."):
                sql, explanation = generate_sql(prompt)
                if not sql:
                    st.error("Failed to generate SQL. Please rephrase your question.")
                    st.session_state.messages.append({"role": "assistant", "content": "Sorry, I couldn't generate a valid SQL query."})
                    return
            if not is_safe_sql(sql):
                st.error("Generated SQL is not a SELECT statement or contains unsafe keywords.")
                return
            sql = ensure_limit(sql)
            with st.spinner("Running query on Athena..."):
                df = run_query(sql)
                if df.empty:
                    st.warning("The query returned no data.")
                    st.session_state.messages.append({"role": "assistant", "content": "The query returned no results.", "sql": sql, "df": df})
                    return
            with st.spinner("Interpreting results..."):
                results_prompt = f"""
The user asked: "{prompt}"
We generated and executed this SQL:
{sql}

The query returned the following data (first 5 rows shown):
{df.head(5).to_string()}

Please provide a natural language answer to the user's original question based on these results. Be concise, highlight key numbers, and mention any trends or outliers if visible.
"""
                answer = ask_bedrock(results_prompt, system_prompt="You are a helpful data analyst assistant. Answer concisely based only on the provided data.")
                if not answer:
                    answer = "I generated the SQL and ran the query, but I could not interpret the results. Here is the data instead."
            st.markdown(answer)
            with st.expander("🔍 View SQL"):
                st.code(sql, language="sql")
            st.dataframe(df, use_container_width=True)
            chart = auto_chart(df)
            if chart:
                st.altair_chart(chart, use_container_width=True)
            st.session_state.messages.append({"role": "assistant", "content": answer, "sql": sql, "df": df})

# ---------------------------- Custom CSS for modern look ----------------------------
def load_css():
    st.markdown("""
    <style>
    .stApp { background-color: #f7f8fb; }
    .block-container { padding-top: 1rem; max-width: 1400px; }
    .kpi-card {
        background: white;
        border-radius: 16px;
        padding: 1rem;
        box-shadow: 0 1px 3px rgba(0,0,0,0.05);
        border: 1px solid #e5e7eb;
        text-align: center;
    }
    .kpi-title { font-size: 0.85rem; font-weight: 600; color: #6b7280; letter-spacing: 0.05em; }
    .kpi-value { font-size: 1.8rem; font-weight: 700; color: #111827; margin: 0.25rem 0; }
    .delta-up { color: #10b981; font-size: 0.8rem; }
    .delta-down { color: #ef4444; font-size: 0.8rem; }
    .attention-card {
        background: white;
        border-radius: 16px;
        padding: 0.75rem;
        border-left: 4px solid;
        margin-bottom: 0.5rem;
    }
    .sidebar-logo {
        display: flex;
        justify-content: center;
        margin-bottom: 1rem;
    }
    </style>
    """, unsafe_allow_html=True)

load_css()

# ---------------------------- Sidebar with Logo & Navigation ----------------------------
with st.sidebar:
    st.markdown('<div class="sidebar-logo">', unsafe_allow_html=True)
    st.image("https://th.bing.com/th/id/OIP.Vy1yFQtg8-D1SsAxcqqtSgHaE6?w=235&h=180&c=7&r=0&o=7&dpr=1.5&pid=1.7&rm=3", width=80)
    st.markdown('</div>', unsafe_allow_html=True)
    st.markdown("## ProcureIQ")
    st.markdown("P2P Analytics")
    st.markdown("---")
    page = st.radio(
        "Navigation",
        ["Dashboard", "Genie", "Forecast", "Invoices"],
        label_visibility="collapsed"
    )
    st.markdown("---")
    st.caption("© 2026 Yash Technologies")

# ---------------------------- Dashboard Page (redesigned) ----------------------------
def render_dashboard():
    # Date and vendor filters (preserve existing logic)
    if "preset" not in st.session_state:
        st.session_state.preset = "Last 30 Days"
    if "date_range" not in st.session_state:
        st.session_state.date_range = compute_range_preset(st.session_state.preset)

    col_date, col_vendor, col_preset = st.columns([2, 2, 3])
    with col_date:
        date_range = st.date_input(
            "Date Range",
            value=st.session_state.date_range,
            format="YYYY-MM-DD",
            label_visibility="collapsed"
        )
        if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
            rng_start, rng_end = date_range
        else:
            rng_start, rng_end = st.session_state.date_range
        st.session_state.date_range = (rng_start, rng_end)

    with col_vendor:
        vendor_sql = f"""
        SELECT DISTINCT v.vendor_name
        FROM {DATABASE}.fact_all_sources_vw f
        LEFT JOIN {DATABASE}.dim_vendor_vw v ON f.vendor_id = v.vendor_id
        WHERE f.posting_date BETWEEN {sql_date(rng_start)} AND {sql_date(rng_end)}
          AND v.vendor_name IS NOT NULL
        ORDER BY 1
        """
        vendors_df = run_query(vendor_sql)
        vendor_list = ["All Vendors"] + vendors_df["vendor_name"].tolist() if not vendors_df.empty else ["All Vendors"]
        selected_vendor = st.selectbox("Vendor", vendor_list, index=0, label_visibility="collapsed")

    with col_preset:
        presets = ["Last 30 Days", "QTD", "YTD", "Custom"]
        current_preset = st.session_state.preset
        for p in presets:
            if st.button(p, key=f"preset_{p}", use_container_width=True, type="primary" if p == current_preset else "secondary"):
                if p == "Custom":
                    st.session_state.preset = p
                else:
                    new_start, new_end = compute_range_preset(p)
                    st.session_state.date_range = (new_start, new_end)
                    st.session_state.preset = p
                st.rerun()

    start_lit = sql_date(rng_start)
    end_lit = sql_date(rng_end)
    p_start, p_end = prior_window(rng_start, rng_end)
    p_start_lit = sql_date(p_start)
    p_end_lit = sql_date(p_end)
    vendor_where = build_vendor_where(selected_vendor)

    # --- KPI Queries ---
    cur_kpi_sql = f"""
    SELECT
        COUNT(DISTINCT CASE WHEN UPPER(invoice_status) = 'OPEN' THEN purchase_order_reference END) AS active_pos,
        SUM(CASE WHEN UPPER(invoice_status) NOT IN ('CANCELLED','REJECTED')
                 THEN COALESCE(invoice_amount_local, 0) ELSE 0 END) AS total_spend,
        COUNT(DISTINCT CASE WHEN UPPER(invoice_status) = 'OPEN' THEN invoice_number END) AS pending_inv,
        AVG(DATE_DIFF('day', posting_date, payment_date)) AS avg_processing_days
    FROM {DATABASE}.fact_all_sources_vw f
    LEFT JOIN {DATABASE}.dim_vendor_vw v ON f.vendor_id = v.vendor_id
    WHERE f.posting_date BETWEEN {start_lit} AND {end_lit}
    {vendor_where}
      AND UPPER(invoice_status) = 'PAID'  -- for avg processing days
    """
    cur_df = run_query(cur_kpi_sql)
    cur_spend = safe_number(cur_df.loc[0, "total_spend"]) if not cur_df.empty else 0
    cur_pos = safe_int(cur_df.loc[0, "active_pos"]) if not cur_df.empty else 0
    cur_pend = safe_int(cur_df.loc[0, "pending_inv"]) if not cur_df.empty else 0
    cur_avg_days = safe_number(cur_df.loc[0, "avg_processing_days"]) if not cur_df.empty else 0

    # Previous period for deltas
    prev_kpi_sql = f"""
    SELECT
        COUNT(DISTINCT CASE WHEN UPPER(invoice_status) = 'OPEN' THEN purchase_order_reference END) AS active_pos,
        SUM(CASE WHEN UPPER(invoice_status) NOT IN ('CANCELLED','REJECTED')
                 THEN COALESCE(invoice_amount_local, 0) ELSE 0 END) AS total_spend,
        COUNT(DISTINCT CASE WHEN UPPER(invoice_status) = 'OPEN' THEN invoice_number END) AS pending_inv
    FROM {DATABASE}.fact_all_sources_vw f
    LEFT JOIN {DATABASE}.dim_vendor_vw v ON f.vendor_id = v.vendor_id
    WHERE f.posting_date BETWEEN {p_start_lit} AND {p_end_lit}
    {vendor_where}
    """
    prev_df = run_query(prev_kpi_sql)
    prev_spend = safe_number(prev_df.loc[0, "total_spend"]) if not prev_df.empty else 0
    prev_pos = safe_int(prev_df.loc[0, "active_pos"]) if not prev_df.empty else 0
    prev_pend = safe_int(prev_df.loc[0, "pending_inv"]) if not prev_df.empty else 0

    spend_delta, spend_up, _ = pct_delta(cur_spend, prev_spend)
    pos_delta, pos_up, _ = pct_delta(cur_pos, prev_pos)
    pend_delta, pend_up, _ = pct_delta(cur_pend, prev_pend)

    # Display KPI cards
    kpi_cols = st.columns(4)
    kpis = [
        ("TOTAL SPEND", abbr_currency(cur_spend), spend_delta, spend_up),
        ("ACTIVE PO'S", f"{cur_pos:,}", pos_delta, pos_up),
        ("AVG INVOICE PROCESSING TIME", f"{cur_avg_days:.1f}d", "↓ -", False),  # static
        ("PENDING INVOICES", f"{cur_pend:,}", pend_delta, pend_up),
    ]
    for col, (label, value, delta, is_up) in zip(kpi_cols, kpis):
        with col:
            delta_class = "delta-up" if is_up else "delta-down"
            delta_sign = "↑" if is_up else "↓"
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-title">{label}</div>
                <div class="kpi-value">{value}</div>
                <div class="{delta_class}">{delta_sign} {delta}</div>
            </div>
            """, unsafe_allow_html=True)

    # --- Needs Attention (Overdue, Disputed, Due) ---
    st.markdown("---")
    st.subheader("Needs Attention")

    # Count queries for attention categories
    overdue_sql = f"""
    SELECT COUNT(*) AS cnt
    FROM {DATABASE}.fact_all_sources_vw f
    WHERE f.posting_date BETWEEN {start_lit} AND {end_lit}
      {vendor_where.replace('AND', 'AND') if vendor_where else ''}
      AND f.due_date < CURRENT_DATE
      AND UPPER(f.invoice_status) IN ('OVERDUE')
    """
    disputed_sql = f"""
    SELECT COUNT(*) AS cnt
    FROM {DATABASE}.fact_all_sources_vw f
    WHERE f.posting_date BETWEEN {start_lit} AND {end_lit}
      {vendor_where.replace('AND', 'AND') if vendor_where else ''}
      AND UPPER(f.invoice_status) IN ('DISPUTE','DISPUTED')
    """
    due_sql = f"""
    SELECT COUNT(*) AS cnt
    FROM {DATABASE}.fact_all_sources_vw f
    WHERE f.posting_date BETWEEN {start_lit} AND {end_lit}
      {vendor_where.replace('AND', 'AND') if vendor_where else ''}
      AND f.due_date >= CURRENT_DATE
      AND f.due_date <= DATE_ADD('day', 30, CURRENT_DATE)
      AND UPPER(f.invoice_status) IN ('OPEN')
    """
    overdue_cnt = safe_int(run_query(overdue_sql).loc[0, "cnt"]) if not run_query(overdue_sql).empty else 0
    disputed_cnt = safe_int(run_query(disputed_sql).loc[0, "cnt"]) if not run_query(disputed_sql).empty else 0
    due_cnt = safe_int(run_query(due_sql).loc[0, "cnt"]) if not run_query(due_sql).empty else 0

    total_attention = overdue_cnt + disputed_cnt + due_cnt
    st.markdown(f"**{total_attention} items need attention**")

    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown(f"""
        <div class="attention-card" style="border-left-color: #ef4444;">
            <strong>⚠️ Overdue</strong><br>
            <span style="font-size: 1.5rem; font-weight: 600;">{overdue_cnt}</span>
        </div>
        """, unsafe_allow_html=True)
    with col2:
        st.markdown(f"""
        <div class="attention-card" style="border-left-color: #f59e0b;">
            <strong>⚖️ Disputed</strong><br>
            <span style="font-size: 1.5rem; font-weight: 600;">{disputed_cnt}</span>
        </div>
        """, unsafe_allow_html=True)
    with col3:
        st.markdown(f"""
        <div class="attention-card" style="border-left-color: #3b82f6;">
            <strong>📅 Due Next 30 Days</strong><br>
            <span style="font-size: 1.5rem; font-weight: 600;">{due_cnt}</span>
        </div>
        """, unsafe_allow_html=True)

    # Optional: simple ring chart for attention distribution
    if total_attention > 0:
        attention_df = pd.DataFrame({
            "category": ["Overdue", "Disputed", "Due Next 30 Days"],
            "count": [overdue_cnt, disputed_cnt, due_cnt]
        })
        chart = alt.Chart(attention_df).mark_arc(innerRadius=50).encode(
            theta="count",
            color=alt.Color("category", scale=alt.Scale(domain=["Overdue", "Disputed", "Due Next 30 Days"], range=["#ef4444", "#f59e0b", "#3b82f6"])),
            tooltip=["category", "count"]
        ).properties(height=200, width=200)
        st.altair_chart(chart, use_container_width=False)

    # Optional: recent invoices table (compact)
    st.markdown("---")
    st.subheader("Recent Invoices")
    recent_sql = f"""
    SELECT DISTINCT
        f.invoice_number AS "INVOICE NUMBER",
        v.vendor_name AS "VENDOR NAME",
        f.posting_date AS "POSTING DATE",
        f.due_date AS "DUE DATE",
        f.invoice_amount_local AS "INVOICE AMOUNT",
        f.purchase_order_reference AS "PO NUMBER",
        UPPER(f.invoice_status) AS "STATUS"
    FROM {DATABASE}.fact_all_sources_vw f
    LEFT JOIN {DATABASE}.dim_vendor_vw v ON f.vendor_id = v.vendor_id
    WHERE f.posting_date BETWEEN {start_lit} AND {end_lit}
    {vendor_where}
    ORDER BY f.posting_date DESC
    LIMIT 10
    """
    recent_df = run_query(recent_sql)
    if not recent_df.empty:
        st.dataframe(recent_df, use_container_width=True)
    else:
        st.info("No invoices found for the selected period.")

# ---------------------------- Forecast Page (Cash Flow) ----------------------------
def render_forecast():
    st.subheader("Cash Flow Need Forecast")
    cf_sql = """
    WITH base AS (
        SELECT document_number, vendor_id, invoice_amount_local, due_date, invoice_status, days_until_due
        FROM procure2pay.cash_flow_unpaid_obligations_vw
    ),
    cycle_time AS (
        SELECT avg_payment_cycle_time_days AS lag_days
        FROM procure2pay.payment_processing_cycle_time_vw
        ORDER BY year DESC, month DESC
        LIMIT 1
    ),
    buckets AS (
        SELECT
            CASE
                WHEN days_until_due < 0 THEN 'OVERDUE_NOW'
                WHEN days_until_due <= 7 THEN 'DUE_7_DAYS'
                WHEN days_until_due <= 14 THEN 'DUE_14_DAYS'
                WHEN days_until_due <= 30 THEN 'DUE_30_DAYS'
                WHEN days_until_due <= 60 THEN 'DUE_60_DAYS'
                WHEN days_until_due <= 90 THEN 'DUE_90_DAYS'
                ELSE 'BEYOND_90_DAYS'
            END AS forecast_bucket,
            COUNT(*) AS invoice_count,
            SUM(invoice_amount_local) AS total_amount,
            MIN(due_date) AS earliest_due,
            MAX(due_date) AS latest_due
        FROM base
        GROUP BY 1
    ),
    summary AS (
        SELECT 'TOTAL_UNPAID' AS forecast_bucket,
               SUM(invoice_count) AS invoice_count,
               SUM(total_amount) AS total_amount,
               NULL AS earliest_due,
               NULL AS latest_due
        FROM buckets
    ),
    processing_note AS (
        SELECT 'PROCESSING_LAG_DAYS' AS forecast_bucket,
               (SELECT lag_days FROM cycle_time) AS invoice_count,
               NULL AS total_amount,
               NULL AS earliest_due,
               NULL AS latest_due
    )
    SELECT * FROM summary
    UNION ALL
    SELECT * FROM buckets
    UNION ALL
    SELECT * FROM processing_note
    ORDER BY CASE forecast_bucket
        WHEN 'TOTAL_UNPAID' THEN 0
        WHEN 'OVERDUE_NOW' THEN 1
        WHEN 'DUE_7_DAYS' THEN 2
        WHEN 'DUE_14_DAYS' THEN 3
        WHEN 'DUE_30_DAYS' THEN 4
        WHEN 'DUE_60_DAYS' THEN 5
        WHEN 'DUE_90_DAYS' THEN 6
        WHEN 'BEYOND_90_DAYS' THEN 7
        ELSE 8 END
    """
    cf_df = run_query(cf_sql)
    if not cf_df.empty:
        # Show summary metrics
        total_unpaid = cf_df[cf_df["forecast_bucket"] == "TOTAL_UNPAID"]["total_amount"].values[0] if not cf_df[cf_df["forecast_bucket"] == "TOTAL_UNPAID"].empty else 0
        overdue_now = cf_df[cf_df["forecast_bucket"] == "OVERDUE_NOW"]["total_amount"].values[0] if not cf_df[cf_df["forecast_bucket"] == "OVERDUE_NOW"].empty else 0
        due_30 = cf_df[cf_df["forecast_bucket"].isin(["DUE_7_DAYS","DUE_14_DAYS","DUE_30_DAYS"])]["total_amount"].sum()
        pct_due_30 = (due_30 / total_unpaid * 100) if total_unpaid > 0 else 0
        st.metric("TOTAL UNPAID", abbr_currency(total_unpaid))
        col1, col2, col3 = st.columns(3)
        col1.metric("OVERDUE NOW", abbr_currency(overdue_now))
        col2.metric("DUE NEXT 30 DAYS", abbr_currency(due_30))
        col3.metric("% DUE ≤ 30 DAYS", f"{pct_due_30:.1f}%")
        st.dataframe(cf_df, use_container_width=True)
        # Bar chart for buckets (excluding TOTAL_UNPAID and PROCESSING_LAG_DAYS)
        chart_df = cf_df[~cf_df["forecast_bucket"].isin(["TOTAL_UNPAID", "PROCESSING_LAG_DAYS"])].copy()
        if not chart_df.empty:
            chart = alt.Chart(chart_df).mark_bar(color="#10b981").encode(
                x=alt.X("forecast_bucket:N", sort=None, axis=alt.Axis(title=None, labelAngle=-30)),
                y=alt.Y("total_amount:Q", axis=alt.Axis(title="Amount", format="~s")),
                tooltip=["forecast_bucket", "total_amount"]
            ).properties(height=300)
            st.altair_chart(chart, use_container_width=True)
        # Download button
        csv = cf_df.to_csv(index=False).encode('utf-8')
        st.download_button("Download forecast (CSV)", data=csv, file_name="cash_flow_forecast.csv", mime="text/csv")
    else:
        st.info("No cash flow forecast data")

    # Additional GR/IR section (optional, keep from original)
    st.markdown("---")
    st.subheader("GR/IR Outstanding Balance (Latest Month)")
    grir_summary_sql = """
    WITH latest AS (
        SELECT year, month, invoice_count, total_grir_blnc
        FROM procure2pay.gr_ir_outstanding_balance_vw
        ORDER BY year DESC, month DESC
        LIMIT 1
    ),
    aging AS (
        SELECT year, month, age_days,
               total_grir_balance,
               grir_over_30, grir_over_60, grir_over_90,
               pct_grir_over_30, pct_grir_over_60, pct_grir_over_90,
               cnt_grir_over_30, cnt_grir_over_60, cnt_grir_over_90
        FROM procure2pay.gr_ir_aging_vw
        ORDER BY year DESC, month DESC
        LIMIT 1
    )
    SELECT
        l.year,
        l.month,
        l.invoice_count AS grir_items,
        l.total_grir_blnc AS total_grir_balance,
        a.grir_over_30, a.grir_over_60, a.grir_over_90,
        a.pct_grir_over_30, a.pct_grir_over_60, a.pct_grir_over_90,
        a.cnt_grir_over_30, a.cnt_grir_over_60, a.cnt_grir_over_90
    FROM latest l
    LEFT JOIN aging a ON a.year = l.year AND a.month = l.month
    """
    grir_sum_df = run_query(grir_summary_sql)
    if not grir_sum_df.empty:
        st.dataframe(grir_sum_df, use_container_width=True)
    else:
        st.info("No GR/IR summary data")

# ---------------------------- Invoices Page (Search + Table) ----------------------------
def render_invoices():
    st.subheader("Invoices")
    st.markdown("Search, track and manage all invoices in one place")
    
    # Search bar
    col1, col2 = st.columns([3, 1])
    with col1:
        search_term = st.text_input("Search by Invoice or PO Number", placeholder="e.g., 9000946 or 5000315", label_visibility="collapsed")
    with col2:
        search_clicked = st.button("Search", use_container_width=True)
        reset = st.button("Reset", use_container_width=True)
        if reset:
            search_term = ""
            st.rerun()
    
    # Filters for vendor and status
    col_vendor, col_status = st.columns(2)
    with col_vendor:
        vendor_list = ["All Vendors"]
        vendor_sql = "SELECT DISTINCT vendor_name FROM procure2pay.dim_vendor_vw ORDER BY vendor_name"
        vendor_df = run_query(vendor_sql)
        if not vendor_df.empty:
            vendor_list = ["All Vendors"] + vendor_df["vendor_name"].tolist()
        selected_vendor = st.selectbox("Vendor", vendor_list)
    with col_status:
        status_list = ["All Status", "OPEN", "PAID", "DISPUTED", "OVERDUE"]
        selected_status = st.selectbox("Status", status_list)
    
    # Build base query
    where_clauses = []
    if search_term and not reset:
        safe_term = search_term.replace("'", "''")
        where_clauses.append(f"(f.invoice_number = '{safe_term}' OR f.purchase_order_reference = '{safe_term}')")
    if selected_vendor != "All Vendors":
        safe_vendor = selected_vendor.replace("'", "''")
        where_clauses.append(f"UPPER(v.vendor_name) = UPPER('{safe_vendor}')")
    if selected_status != "All Status":
        where_clauses.append(f"UPPER(f.invoice_status) = '{selected_status}'")
    
    where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
    
    # Query for invoices
    invoices_sql = f"""
    SELECT DISTINCT
        f.invoice_number AS "INVOICE NUMBER",
        v.vendor_name AS "VENDOR NAME",
        f.posting_date AS "POSTING DATE",
        f.due_date AS "DUE DATE",
        f.invoice_amount_local AS "INVOICE AMOUNT",
        f.purchase_order_reference AS "PO NUMBER",
        UPPER(f.invoice_status) AS "STATUS"
    FROM {DATABASE}.fact_all_sources_vw f
    LEFT JOIN {DATABASE}.dim_vendor_vw v ON f.vendor_id = v.vendor_id
    WHERE {where_sql}
    ORDER BY f.posting_date DESC
    LIMIT 500
    """
    invoices_df = run_query(invoices_sql)
    if not invoices_df.empty:
        st.dataframe(invoices_df, use_container_width=True, height=400)
        # If a single invoice is searched, show details (optional)
        if search_term and len(invoices_df) == 1:
            inv_num = invoices_df.iloc[0]["INVOICE NUMBER"]
            st.markdown("---")
            st.subheader(f"Invoice Details: {inv_num}")
            # Fetch full details
            details_sql = f"""
            SELECT
                f.invoice_number AS "INVOICE NUMBER",
                f.posting_date AS "INVOICE DATE",
                f.invoice_amount_local AS "INVOICE AMOUNT",
                f.purchase_order_reference AS "PO NUMBER",
                f.po_amount AS "PO AMOUNT",
                f.due_date AS "DUE DATE",
                f.status AS "INVOICE STATUS",
                f.company_code AS "COMPANY CODE",
                f.fiscal_year AS "FISCAL YEAR",
                f.aging_days AS "AGING DAYS"
            FROM {DATABASE}.fact_all_sources_vw f
            WHERE f.invoice_number = '{inv_num}'
            LIMIT 1
            """
            details_df = run_query(details_sql)
            if not details_df.empty:
                st.dataframe(details_df, use_container_width=True)
            # Status history
            hist_sql = f"""
            SELECT
                invoice_number AS "INVOICE NUMBER",
                UPPER(status) AS "STATUS",
                effective_date AS "EFFECTIVE DATE",
                status_notes AS "STATUS NOTES"
            FROM {DATABASE}.invoice_status_history_vw
            WHERE invoice_number = '{inv_num}'
            ORDER BY sequence_nbr
            """
            hist_df = run_query(hist_sql)
            if not hist_df.empty:
                st.subheader("Status History")
                st.dataframe(hist_df, use_container_width=True)
            # Vendor info
            vendor_info_sql = f"""
            SELECT DISTINCT
                v.vendor_id AS "VENDOR ID",
                v.vendor_name AS "VENDOR NAME",
                v.vendor_name_2 AS "ALIAS / NAME 2",
                v.country_code AS "COUNTRY",
                v.city AS "CITY",
                v.postal_code AS "POSTAL CODE",
                v.street AS "STREET",
                v.region_code AS "REGION",
                v.industry_sector AS "INDUSTRY",
                v.vendor_account_group AS "ACCOUNT GROUP",
                v.tax_number_1 AS "TAX NUMBER 1",
                v.tax_number_2 AS "TAX NUMBER 2"
            FROM {DATABASE}.fact_all_sources_vw f
            LEFT JOIN {DATABASE}.dim_vendor_vw v ON f.vendor_id = v.vendor_id
            WHERE f.invoice_number = '{inv_num}'
            """
            vendor_df = run_query(vendor_info_sql)
            if not vendor_df.empty:
                st.subheader("Vendor Information")
                st.dataframe(vendor_df, use_container_width=True)
            # Company info
            company_sql = f"""
            SELECT DISTINCT
                f.company_code AS "COMPANY CODE",
                COALESCE(cc.company_name, 'N/A') AS "COMPANY NAME",
                f.plant_code AS "PLANT CODE",
                COALESCE(plt.plant_name, 'N/A') AS "PLANT NAME"
            FROM {DATABASE}.fact_all_sources_vw f
            LEFT JOIN {DATABASE}.dim_company_code_vw cc ON f.company_code = cc.company_code
            LEFT JOIN {DATABASE}.dim_plant_vw plt ON f.plant_code = plt.plant_code
            WHERE f.invoice_number = '{inv_num}'
            """
            company_df = run_query(company_sql)
            if not company_df.empty:
                st.subheader("Company & Plant Information")
                st.dataframe(company_df, use_container_width=True)
    else:
        st.info("No invoices found.")

# ---------------------------- Main Router ----------------------------
if page == "Dashboard":
    render_dashboard()
elif page == "Genie":
    render_genie()
elif page == "Forecast":
    render_forecast()
else:
    render_invoices()
