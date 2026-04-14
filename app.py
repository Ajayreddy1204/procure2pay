import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
from datetime import date, timedelta
import boto3
import awswrangler as wr
import json
import re

# ---------------------------- Page config ----------------------------
st.set_page_config(
    page_title="P2P Analytics Dashboard",
    layout="wide",
    page_icon=":bar_chart:",
)

# ---------------------------- Athena configuration ----------------------------
DATABASE = "procure2pay"          # Glue database name
ATHENA_REGION = "us-east-1"       # your Athena region
# Use Amazon Nova Micro model (available in Bedrock)
BEDROCK_MODEL_ID = "amazon.nova-micro-v1:0"

session = boto3.Session()
athena_client = session.client("athena", region_name=ATHENA_REGION)
bedrock_runtime = session.client("bedrock-runtime", region_name=ATHENA_REGION)

def run_query(sql: str) -> pd.DataFrame:
    """Execute SQL on Athena and return DataFrame."""
    try:
        df = wr.athena.read_sql_query(sql, database=DATABASE, boto3_session=session)
        return df
    except Exception as e:
        st.error(f"Athena query failed: {e}\nSQL: {sql[:500]}")
        return pd.DataFrame()

# ---------------------------- Helper functions (original) ----------------------------
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

# ---------------------------- AI Chat Functions (Nova Micro) ----------------------------
# System prompt describing the actual tables/views in procure2pay database
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
    """
    Invoke Amazon Nova Micro via Bedrock and return the response text.
    Uses the required request/response format for Nova models.
    """
    try:
        # Nova request body format
        body = json.dumps({
            "messages": [
                {
                    "role": "user",
                    "content": [{"text": prompt}]
                }
            ],
            "system": system_prompt,
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
        # Nova response: output.message.content[0].text
        return response_body['output']['message']['content'][0]['text']
    except Exception as e:
        st.error(f"Bedrock invocation failed: {e}")
        return ""

def generate_sql(question: str) -> tuple:
    """Ask Nova to generate SQL and explanation from user question."""
    prompt = f"User question: {question}\n\nGenerate SQL query and explanation as JSON."
    response = ask_bedrock(prompt)
    if not response:
        return None, "Bedrock returned empty response."
    # Try to extract JSON from the response (in case it includes markdown or extra text)
    json_match = re.search(r'\{.*\}$', response, re.DOTALL)
    if not json_match:
        # fallback: assume the whole response is JSON
        json_str = response
    else:
        json_str = json_match.group(0)
    try:
        data = json.loads(json_str)
        sql = data.get("sql", "").strip()
        explanation = data.get("explanation", "")
        return sql, explanation
    except json.JSONDecodeError as e:
        st.error(f"Failed to parse JSON from Nova: {e}\nResponse: {response}")
        return None, "Could not parse SQL from AI response."

def is_safe_sql(sql: str) -> bool:
    """Basic guard: only allow SELECT statements and prevent dangerous keywords."""
    sql_lower = sql.lower().strip()
    if not sql_lower.startswith("select"):
        return False
    dangerous = ["insert", "update", "delete", "drop", "alter", "create", "truncate", "grant", "revoke"]
    for word in dangerous:
        if re.search(r'\b' + word + r'\b', sql_lower):
            return False
    return True

def ensure_limit(sql: str, default_limit: int = 100) -> str:
    """Add LIMIT if not present and not an aggregation-only query."""
    sql_lower = sql.lower()
    if "limit" in sql_lower:
        return sql
    # If the query has aggregate functions but no GROUP BY, it's a single row result
    if re.search(r'\b(count|sum|avg|min|max)\b', sql_lower) and "group by" not in sql_lower:
        return sql
    return f"{sql.rstrip(';')} LIMIT {default_limit}"

def auto_chart(df: pd.DataFrame) -> alt.Chart | None:
    """Automatically create an Altair chart if the dataframe is suitable."""
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

def render_ai_chat():
    st.subheader("🤖 AI Analytics Chat")
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
                    st.session_state.messages.append({"role": "assistant", "content": "Sorry, I couldn't generate a valid SQL query. Please try a different question."})
                    return

            if not is_safe_sql(sql):
                st.error("Generated SQL is not a SELECT statement or contains unsafe keywords.")
                return
            sql = ensure_limit(sql)

            with st.spinner("Running query on Athena..."):
                df = run_query(sql)
                if df.empty:
                    st.warning("The query returned no data.")
                    st.session_state.messages.append({
                        "role": "assistant",
                        "content": "The query executed but returned no results. Try adjusting your question.",
                        "sql": sql,
                        "df": df
                    })
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

            st.session_state.messages.append({
                "role": "assistant",
                "content": answer,
                "sql": sql,
                "df": df
            })

# ---------------------------- Custom CSS ----------------------------
def load_css():
    st.markdown("""
    <style>
    .stApp { background-color: #f7f8fb; }
    .block-container { padding-top: 1rem; max-width: 1200px; }
    .kpi-card {
        background: white;
        border-radius: 16px;
        padding: 1rem;
        box-shadow: 0 1px 3px rgba(0,0,0,0.05);
        border: 1px solid #e5e7eb;
    }
    .kpi-title { font-size: 0.85rem; font-weight: 600; color: #6b7280; }
    .kpi-value { font-size: 1.8rem; font-weight: 700; color: #111827; }
    .delta-up { color: #10b981; }
    .delta-down { color: #ef4444; }
    .nav-button { border-radius: 999px !important; }
    </style>
    """, unsafe_allow_html=True)

load_css()

# ---------------------------- Header ----------------------------
st.markdown("<h1 style='font-size: 1.8rem;'>ProcureIQ · P2P Analytics</h1>", unsafe_allow_html=True)
st.markdown("<hr style='margin: 0.5rem 0 1rem 0;'>", unsafe_allow_html=True)

# ---------------------------- Page navigation ----------------------------
if "page" not in st.session_state:
    st.session_state.page = "dashboard"

col1, col2, col3, col4 = st.columns(4)
with col1:
    if st.button("Dashboard", use_container_width=True, type="primary" if st.session_state.page == "dashboard" else "secondary"):
        st.session_state.page = "dashboard"
        st.rerun()
with col2:
    if st.button("Cash Flow & GR/IR", use_container_width=True, type="primary" if st.session_state.page == "cash_flow" else "secondary"):
        st.session_state.page = "cash_flow"
        st.rerun()
with col3:
    if st.button("Invoices", use_container_width=True, type="primary" if st.session_state.page == "invoice" else "secondary"):
        st.session_state.page = "invoice"
        st.rerun()
with col4:
    if st.button("AI Chat", use_container_width=True, type="primary" if st.session_state.page == "ai_chat" else "secondary"):
        st.session_state.page = "ai_chat"
        st.rerun()

st.markdown("<hr style='margin: 1rem 0 1.5rem 0;'>", unsafe_allow_html=True)

# ---------------------------- Dashboard Page (unchanged) ----------------------------
def render_dashboard():
    # Date and vendor filters
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

    cur_kpi_sql = f"""
    SELECT
        COUNT(DISTINCT CASE WHEN UPPER(invoice_status) = 'OPEN' THEN purchase_order_reference END) AS active_pos,
        COUNT(DISTINCT purchase_order_reference) AS total_pos,
        SUM(CASE WHEN UPPER(invoice_status) NOT IN ('CANCELLED','REJECTED')
                 THEN COALESCE(invoice_amount_local, 0) ELSE 0 END) AS total_spend,
        COUNT(DISTINCT v.vendor_name) AS active_vendors,
        COUNT(DISTINCT CASE WHEN UPPER(invoice_status) = 'OPEN' THEN invoice_number END) AS pending_inv
    FROM {DATABASE}.fact_all_sources_vw f
    LEFT JOIN {DATABASE}.dim_vendor_vw v ON f.vendor_id = v.vendor_id
    WHERE f.posting_date BETWEEN {start_lit} AND {end_lit}
    {vendor_where}
    """
    cur_df = run_query(cur_kpi_sql)
    cur_spend = safe_number(cur_df.loc[0, "total_spend"]) if not cur_df.empty else 0
    cur_pos = safe_int(cur_df.loc[0, "active_pos"]) if not cur_df.empty else 0
    cur_vend = safe_int(cur_df.loc[0, "active_vendors"]) if not cur_df.empty else 0
    cur_pend = safe_int(cur_df.loc[0, "pending_inv"]) if not cur_df.empty else 0

    prev_kpi_sql = f"""
    SELECT
        COUNT(DISTINCT CASE WHEN UPPER(invoice_status) = 'OPEN' THEN purchase_order_reference END) AS active_pos,
        COUNT(DISTINCT purchase_order_reference) AS total_pos,
        SUM(CASE WHEN UPPER(invoice_status) NOT IN ('CANCELLED','REJECTED')
                 THEN COALESCE(invoice_amount_local, 0) ELSE 0 END) AS total_spend,
        COUNT(DISTINCT v.vendor_name) AS active_vendors,
        COUNT(DISTINCT CASE WHEN UPPER(invoice_status) = 'OPEN' THEN invoice_number END) AS pending_inv
    FROM {DATABASE}.fact_all_sources_vw f
    LEFT JOIN {DATABASE}.dim_vendor_vw v ON f.vendor_id = v.vendor_id
    WHERE f.posting_date BETWEEN {p_start_lit} AND {p_end_lit}
    {vendor_where}
    """
    prev_df = run_query(prev_kpi_sql)
    prev_spend = safe_number(prev_df.loc[0, "total_spend"]) if not prev_df.empty else 0
    prev_pos = safe_int(prev_df.loc[0, "active_pos"]) if not prev_df.empty else 0
    prev_vend = safe_int(prev_df.loc[0, "active_vendors"]) if not prev_df.empty else 0
    prev_pend = safe_int(prev_df.loc[0, "pending_inv"]) if not prev_df.empty else 0

    spend_delta, spend_up, _ = pct_delta(cur_spend, prev_spend)
    pos_delta, pos_up, _ = pct_delta(cur_pos, prev_pos)
    vend_delta, vend_up, _ = pct_delta(cur_vend, prev_vend)
    pend_delta, pend_up, _ = pct_delta(cur_pend, prev_pend)

    kpi_cols = st.columns(4)
    kpis = [
        ("Total Spend", abbr_currency(cur_spend), spend_delta, spend_up),
        ("Active POs", f"{cur_pos:,}", pos_delta, pos_up),
        ("Active Vendors", f"{cur_vend:,}", vend_delta, vend_up),
        ("Pending Invoices", f"{cur_pend:,}", pend_delta, pend_up),
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

    avg_processing_sql = f"""
    SELECT AVG(DATE_DIFF('day', posting_date, payment_date)) AS avg_processing_days
    FROM {DATABASE}.fact_all_sources_vw
    WHERE posting_date BETWEEN {start_lit} AND {end_lit}
      AND UPPER(invoice_status) = 'PAID'
    """
    avg_df = run_query(avg_processing_sql)
    avg_days = safe_number(avg_df.loc[0, "avg_processing_days"]) if not avg_df.empty else 0

    first_pass_sql = f"""
    WITH hist AS (
        SELECT invoice_number,
               MAX(CASE WHEN UPPER(status) IN ('PAID','CLEARED','CLOSED','POSTED','SETTLED') THEN 1 ELSE 0 END) AS has_paid,
               MAX(CASE WHEN UPPER(status) IN ('DISPUTE','DISPUTED','OVERDUE') THEN 1 ELSE 0 END) AS has_issue
        FROM {DATABASE}.invoice_status_history_vw
        WHERE posting_date BETWEEN {start_lit} AND {end_lit}
        GROUP BY invoice_number
    )
    SELECT
        COUNT(*) AS total_inv,
        SUM(CASE WHEN has_paid = 1 AND has_issue = 0 THEN 1 ELSE 0 END) AS first_pass_inv
    FROM hist
    """
    fp_df = run_query(first_pass_sql)
    total_inv = safe_int(fp_df.loc[0, "total_inv"]) if not fp_df.empty else 0
    fp_inv = safe_int(fp_df.loc[0, "first_pass_inv"]) if not fp_df.empty else 0
    first_pass_rate = (fp_inv / total_inv * 100) if total_inv > 0 else 0

    auto_rate_sql = f"""
    WITH paid_invoices AS (
        SELECT invoice_number, status_notes
        FROM {DATABASE}.invoice_status_history_vw
        WHERE posting_date BETWEEN {start_lit} AND {end_lit}
          AND UPPER(status) = 'PAID'
    )
    SELECT
        COUNT(*) AS total_cleared,
        SUM(CASE WHEN UPPER(status_notes) = 'AUTO PROCESSED' THEN 1 ELSE 0 END) AS auto_processed
    FROM paid_invoices
    """
    auto_df = run_query(auto_rate_sql)
    total_cleared = safe_int(auto_df.loc[0, "total_cleared"]) if not auto_df.empty else 0
    auto_proc = safe_int(auto_df.loc[0, "auto_processed"]) if not auto_df.empty else 0
    auto_rate = (auto_proc / total_cleared * 100) if total_cleared > 0 else 0

    st.markdown("---")
    adv_cols = st.columns(3)
    with adv_cols[0]:
        st.metric("Avg Processing Days", f"{avg_days:.1f}")
    with adv_cols[1]:
        st.metric("First Pass Rate", f"{first_pass_rate:.1f}%")
    with adv_cols[2]:
        st.metric("Auto‑processed Rate", f"{auto_rate:.1f}%")

    st.markdown("---")
    chart_cols = st.columns(3)

    with chart_cols[0]:
        st.subheader("Invoice Status")
        status_sql = f"""
        SELECT
            CASE
                WHEN UPPER(invoice_status) IN ('PAID','CLEARED','CLOSED','POSTED','SETTLED') THEN 'Paid'
                WHEN UPPER(invoice_status) IN ('OPEN','PENDING','ON HOLD','PARKED','IN PROGRESS') THEN 'Pending'
                WHEN UPPER(invoice_status) IN ('DISPUTE','DISPUTED','BLOCKED','CONTESTED') THEN 'Disputed'
                ELSE 'Other'
            END AS status,
            COUNT(*) AS cnt
        FROM {DATABASE}.fact_all_sources_vw
        WHERE posting_date BETWEEN {start_lit} AND {end_lit}
        GROUP BY 1
        ORDER BY cnt DESC
        """
        status_df = run_query(status_sql)
        if not status_df.empty:
            chart = alt.Chart(status_df).mark_arc(innerRadius=40).encode(
                theta=alt.Theta(field="cnt", type="quantitative"),
                color=alt.Color(field="status", type="nominal", scale=alt.Scale(scheme="pastel1")),
                tooltip=["status", "cnt"]
            ).properties(height=280)
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("No invoice status data")

    with chart_cols[1]:
        st.subheader("Top 10 Vendors by Spend")
        top_vendors_sql = f"""
        SELECT
            v.vendor_name,
            SUM(COALESCE(f.invoice_amount_local, 0)) AS spend
        FROM {DATABASE}.fact_all_sources_vw f
        LEFT JOIN {DATABASE}.dim_vendor_vw v ON f.vendor_id = v.vendor_id
        WHERE f.posting_date BETWEEN {start_lit} AND {end_lit}
        {vendor_where}
        GROUP BY 1
        ORDER BY spend DESC
        LIMIT 10
        """
        top_df = run_query(top_vendors_sql)
        if not top_df.empty:
            chart = alt.Chart(top_df).mark_bar(color="#1e88e5", cornerRadiusTopLeft=4).encode(
                x=alt.X("spend:Q", axis=alt.Axis(title=None, format="~s")),
                y=alt.Y("vendor_name:N", sort="-x", axis=alt.Axis(title=None)),
                tooltip=["vendor_name", alt.Tooltip("spend:Q", format=",.0f")]
            ).properties(height=280)
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("No vendor spend data")

    with chart_cols[2]:
        st.subheader("Monthly Spend Trend (Actual + Forecast)")
        trend_sql = f"""
        WITH monthly_data AS (
            SELECT
                DATE_TRUNC('month', posting_date) AS month_start,
                EXTRACT(year FROM posting_date) AS year_num,
                EXTRACT(month FROM posting_date) AS month_num,
                DATE_FORMAT(posting_date, '%Y-%m') AS month,
                SUM(CASE WHEN UPPER(invoice_status) NOT IN ('CANCELLED','REJECTED')
                         THEN COALESCE(invoice_amount_local, 0) ELSE 0 END) AS actual
            FROM {DATABASE}.fact_all_sources_vw
            WHERE posting_date >= DATE_ADD('year', -2, {end_lit})
              AND posting_date <= {end_lit}
            GROUP BY 1, 2, 3, 4
        )
        SELECT
            month_start,
            year_num,
            month_num,
            month,
            actual,
            AVG(actual) OVER (PARTITION BY month_num ORDER BY year_num ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS forecast
        FROM monthly_data
        ORDER BY month_start
        """
        trend_df = run_query(trend_sql)
        if not trend_df.empty:
            trend_long = trend_df.melt(id_vars=["month"], value_vars=["actual", "forecast"],
                                       var_name="type", value_name="amount")
            chart = alt.Chart(trend_long).mark_line(point=True).encode(
                x=alt.X("month:N", axis=alt.Axis(title=None, labelAngle=-45)),
                y=alt.Y("amount:Q", axis=alt.Axis(title=None, format="~s")),
                color=alt.Color("type:N", scale=alt.Scale(domain=["actual", "forecast"], range=["#1e88e5", "#ffb74d"])),
                tooltip=["month", "type", alt.Tooltip("amount:Q", format=",.0f")]
            ).properties(height=280)
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("No trend data")

    st.markdown("---")
    st.subheader("Needs Attention")
    tab1, tab2, tab3 = st.tabs(["Overdue", "Disputed", "Due Next 30 Days"])

    with tab1:
        overdue_sql = f"""
        SELECT
            f.invoice_number AS ref_no,
            f.invoice_amount_local AS amount,
            f.due_date,
            UPPER(f.invoice_status) AS status,
            v.vendor_name,
            f.aging_days
        FROM {DATABASE}.fact_all_sources_vw f
        LEFT JOIN {DATABASE}.dim_vendor_vw v ON f.vendor_id = v.vendor_id
        WHERE f.posting_date BETWEEN {start_lit} AND {end_lit}
          {vendor_where}
          AND f.due_date < CURRENT_DATE
          AND UPPER(f.invoice_status) IN ('OVERDUE')
        ORDER BY f.due_date ASC
        LIMIT 20
        """
        overdue_df = run_query(overdue_sql)
        if not overdue_df.empty:
            st.dataframe(overdue_df, use_container_width=True)
        else:
            st.info("No overdue invoices")

    with tab2:
        disputed_sql = f"""
        SELECT
            f.invoice_number AS ref_no,
            f.invoice_amount_local AS amount,
            f.due_date,
            UPPER(f.invoice_status) AS status,
            v.vendor_name,
            f.aging_days
        FROM {DATABASE}.fact_all_sources_vw f
        LEFT JOIN {DATABASE}.dim_vendor_vw v ON f.vendor_id = v.vendor_id
        WHERE f.posting_date BETWEEN {start_lit} AND {end_lit}
          {vendor_where}
          AND UPPER(f.invoice_status) IN ('DISPUTE','DISPUTED')
        ORDER BY f.due_date ASC
        LIMIT 20
        """
        disputed_df = run_query(disputed_sql)
        if not disputed_df.empty:
            st.dataframe(disputed_df, use_container_width=True)
        else:
            st.info("No disputed invoices")

    with tab3:
        due_sql = f"""
        SELECT
            f.invoice_number AS ref_no,
            f.invoice_amount_local AS amount,
            f.due_date,
            UPPER(f.invoice_status) AS status,
            v.vendor_name,
            f.aging_days
        FROM {DATABASE}.fact_all_sources_vw f
        LEFT JOIN {DATABASE}.dim_vendor_vw v ON f.vendor_id = v.vendor_id
        WHERE f.posting_date BETWEEN {start_lit} AND {end_lit}
          {vendor_where}
          AND f.due_date IS NOT NULL
          AND f.due_date >= CURRENT_DATE
          AND f.due_date <= DATE_ADD('day', 30, CURRENT_DATE)
          AND UPPER(f.invoice_status) IN ('OPEN')
        ORDER BY f.due_date ASC
        LIMIT 20
        """
        due_df = run_query(due_sql)
        if not due_df.empty:
            st.dataframe(due_df, use_container_width=True)
        else:
            st.info("No invoices due in next 30 days")

# ---------------------------- Cash Flow & GR/IR Page (unchanged) ----------------------------
def render_cash_flow():
    st.subheader("Cash Flow Forecast")
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
        st.dataframe(cf_df, use_container_width=True)
        chart_df = cf_df[~cf_df["forecast_bucket"].isin(["TOTAL_UNPAID", "PROCESSING_LAG_DAYS"])].copy()
        if not chart_df.empty:
            chart = alt.Chart(chart_df).mark_bar(color="#10b981").encode(
                x=alt.X("forecast_bucket:N", sort=None, axis=alt.Axis(title=None, labelAngle=-30)),
                y=alt.Y("total_amount:Q", axis=alt.Axis(title="Amount", format="~s")),
                tooltip=["forecast_bucket", "total_amount"]
            ).properties(height=300)
            st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No cash flow forecast data")

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

    st.subheader("GR/IR Trend (Last 24 Months)")
    grir_trend_sql = """
    SELECT
        DATE_PARSE(CONCAT(CAST(year AS VARCHAR), '-', LPAD(CAST(month AS VARCHAR), 2, '0'), '-01'), '%Y-%m-%d') AS month_date,
        invoice_count,
        total_grir_blnc
    FROM procure2pay.gr_ir_outstanding_balance_vw
    ORDER BY year DESC, month DESC
    LIMIT 24
    """
    grir_trend_df = run_query(grir_trend_sql)
    if not grir_trend_df.empty:
        grir_trend_df = grir_trend_df.sort_values("month_date")
        chart = alt.Chart(grir_trend_df).mark_line(point=True, color="#ef4444").encode(
            x=alt.X("month_date:T", axis=alt.Axis(title="Month")),
            y=alt.Y("total_grir_blnc:Q", axis=alt.Axis(title="Balance", format="~s")),
            tooltip=["month_date", "total_grir_blnc"]
        ).properties(height=300)
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No GR/IR trend data")

    st.markdown("---")
    st.subheader("GR/IR Aging")
    aging_sql = """
    SELECT year, month, age_days,
           total_grir_balance,
           grir_over_30, grir_over_60, grir_over_90,
           pct_grir_over_30, pct_grir_over_60, pct_grir_over_90,
           cnt_grir_over_30, cnt_grir_over_60, cnt_grir_over_90
    FROM procure2pay.gr_ir_aging_vw
    ORDER BY year DESC, month DESC, age_days
    """
    aging_df = run_query(aging_sql)
    if not aging_df.empty:
        st.dataframe(aging_df, use_container_width=True)
    else:
        st.info("No GR/IR aging data")

# ---------------------------- Invoice Details Page (unchanged) ----------------------------
def render_invoice():
    st.subheader("Invoice Search")
    search_term = st.text_input("Search by Invoice Number or PO Number", placeholder="e.g., INV-12345 or PO-67890")
    if search_term:
        safe_term = search_term.replace("'", "''")
        inv_list_sql = f"""
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
        WHERE f.invoice_number = '{safe_term}' OR f.purchase_order_reference = '{safe_term}'
        ORDER BY f.posting_date DESC
        LIMIT 10
        """
        inv_df = run_query(inv_list_sql)
        if not inv_df.empty:
            st.dataframe(inv_df, use_container_width=True)
            inv_num = inv_df.iloc[0]["INVOICE NUMBER"]
            st.markdown("---")
            st.subheader(f"Invoice Details: {inv_num}")

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
                v.tax_number_2 AS "TAX NUMBER 2",
                v.deletion_flag AS "DELETION FLAG",
                v.posting_block AS "POSTING BLOCK",
                v.system AS "SOURCE SYSTEM"
            FROM {DATABASE}.fact_all_sources_vw f
            LEFT JOIN {DATABASE}.dim_vendor_vw v ON f.vendor_id = v.vendor_id
            WHERE f.invoice_number = '{inv_num}'
            """
            vendor_df = run_query(vendor_info_sql)
            if not vendor_df.empty:
                st.subheader("Vendor Information")
                st.dataframe(vendor_df, use_container_width=True)

            company_sql = f"""
            SELECT DISTINCT
                f.company_code AS "COMPANY CODE",
                COALESCE(cc.company_name, 'N/A') AS "COMPANY NAME",
                f.plant_code AS "PLANT CODE",
                COALESCE(plt.plant_name, 'N/A') AS "PLANT NAME",
                CONCAT(COALESCE(cc.street, ''), ', ', COALESCE(cc.city, ''), ' ', COALESCE(cc.postal_code, ''), ', ', COALESCE(cc.country_code, '')) AS "COMPANY ADDRESS"
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
            st.warning("No invoice found for the given search term.")
    else:
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
        ORDER BY f.posting_date DESC
        LIMIT 50
        """
        recent_df = run_query(recent_sql)
        if not recent_df.empty:
            st.subheader("Recent Invoices")
            st.dataframe(recent_df, use_container_width=True)
        else:
            st.info("No invoices found.")

# ---------------------------- Main Router ----------------------------
if st.session_state.page == "dashboard":
    render_dashboard()
elif st.session_state.page == "cash_flow":
    render_cash_flow()
elif st.session_state.page == "invoice":
    render_invoice()
else:
    render_ai_chat()
