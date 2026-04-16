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
            return "0%", True
        return "↑ +100%", True
    change = (cur - prev) / prev * 100
    if abs(change) < 0.05:
        return "0%", True
    sign = "↑" if change >= 0 else "↓"
    return f"{sign} {change:+.1f}%".replace("+", "+"), change >= 0

# ---------------------------- AI Chat Functions ----------------------------
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

# ---------------------------- Genie Page (AI Assistant) ----------------------------
def run_spending_overview():
    st.markdown("## Spending Overview")
    today = date.today()
    ytd_start = date(today.year, 1, 1)
    start_lit = sql_date(ytd_start)
    end_lit = sql_date(today)
    
    cur_spend_sql = f"""
    SELECT SUM(COALESCE(invoice_amount_local, 0)) AS total_spend
    FROM {DATABASE}.fact_all_sources_vw
    WHERE posting_date BETWEEN {start_lit} AND {end_lit}
      AND UPPER(invoice_status) NOT IN ('CANCELLED','REJECTED')
    """
    cur_spend = safe_number(run_query(cur_spend_sql).loc[0, "total_spend"]) if not run_query(cur_spend_sql).empty else 0
    
    last_month_start = (today.replace(day=1) - timedelta(days=1)).replace(day=1)
    last_month_end = today.replace(day=1) - timedelta(days=1)
    prev_spend_sql = f"""
    SELECT SUM(COALESCE(invoice_amount_local, 0)) AS total_spend
    FROM {DATABASE}.fact_all_sources_vw
    WHERE posting_date BETWEEN {sql_date(last_month_start)} AND {sql_date(last_month_end)}
      AND UPPER(invoice_status) NOT IN ('CANCELLED','REJECTED')
    """
    prev_spend = safe_number(run_query(prev_spend_sql).loc[0, "total_spend"]) if not run_query(prev_spend_sql).empty else 0
    mom_change = ((cur_spend - prev_spend) / prev_spend * 100) if prev_spend > 0 else 0
    
    col1, col2 = st.columns(2)
    col1.metric("Total Spend (YTD)", abbr_currency(cur_spend))
    col2.metric("MoM Change", f"{mom_change:+.1f}%", delta=f"{mom_change:+.1f}%", delta_color="normal")
    
    top_vendors_sql = f"""
    SELECT v.vendor_name, SUM(COALESCE(f.invoice_amount_local, 0)) AS spend
    FROM {DATABASE}.fact_all_sources_vw f
    LEFT JOIN {DATABASE}.dim_vendor_vw v ON f.vendor_id = v.vendor_id
    WHERE f.posting_date BETWEEN {start_lit} AND {end_lit}
      AND UPPER(f.invoice_status) NOT IN ('CANCELLED','REJECTED')
    GROUP BY 1
    ORDER BY spend DESC
    LIMIT 5
    """
    top_df = run_query(top_vendors_sql)
    if not top_df.empty:
        st.markdown("**Top 5 Vendors**")
        for _, row in top_df.iterrows():
            pct = (row['spend'] / cur_spend * 100) if cur_spend > 0 else 0
            st.write(f"- {row['vendor_name']}: {abbr_currency(row['spend'])} ({pct:.1f}% of total)")
    
    anomaly_sql = f"""
    WITH monthly AS (
        SELECT 
            DATE_TRUNC('month', posting_date) AS month,
            SUM(COALESCE(invoice_amount_local, 0)) AS spend
        FROM {DATABASE}.fact_all_sources_vw
        WHERE posting_date >= DATE_ADD('month', -12, {end_lit})
          AND UPPER(invoice_status) NOT IN ('CANCELLED','REJECTED')
        GROUP BY 1
    ),
    mom AS (
        SELECT 
            month,
            spend,
            LAG(spend) OVER (ORDER BY month) AS prev_spend,
            (spend - LAG(spend) OVER (ORDER BY month)) / NULLIF(LAG(spend) OVER (ORDER BY month), 0) * 100 AS pct_change
        FROM monthly
    )
    SELECT month, spend, pct_change
    FROM mom
    WHERE pct_change = (SELECT MAX(pct_change) FROM mom)
    LIMIT 1
    """
    anomaly_df = run_query(anomaly_sql)
    if not anomaly_df.empty:
        anomaly_df['month'] = pd.to_datetime(anomaly_df['month'])
        month = anomaly_df.iloc[0]['month'].strftime('%Y-%m')
        pct = anomaly_df.iloc[0]['pct_change']
        st.info(f"**Anomaly Detected** – {month} spending spiked by {pct:.0f}% vs prior month.")
    
    trend_sql = f"""
    SELECT 
        DATE_TRUNC('month', posting_date) AS month,
        SUM(COALESCE(invoice_amount_local, 0)) AS spend
    FROM {DATABASE}.fact_all_sources_vw
    WHERE posting_date >= DATE_ADD('month', -12, {end_lit})
      AND UPPER(invoice_status) NOT IN ('CANCELLED','REJECTED')
    GROUP BY 1
    ORDER BY 1
    """
    trend_df = run_query(trend_sql)
    if not trend_df.empty:
        trend_df['month'] = pd.to_datetime(trend_df['month'])
        trend_df['month_str'] = trend_df['month'].dt.strftime('%b %Y')
        chart = alt.Chart(trend_df).mark_line(point=True, color="#1e88e5").encode(
            x=alt.X("month_str:N", sort=None, axis=alt.Axis(title=None, labelAngle=-45)),
            y=alt.Y("spend:Q", axis=alt.Axis(title="Spend", format="~s")),
            tooltip=["month_str", alt.Tooltip("spend:Q", format=",.0f")]
        ).properties(title="Monthly Spend Trend (Last 12 Months)", height=300)
        st.altair_chart(chart, use_container_width=True)
    
    volume_sql = f"""
    SELECT 
        DATE_TRUNC('month', posting_date) AS month,
        COUNT(DISTINCT invoice_number) AS invoice_volume,
        COUNT(DISTINCT vendor_id) AS active_vendors
    FROM {DATABASE}.fact_all_sources_vw
    WHERE posting_date >= DATE_ADD('month', -12, {end_lit})
    GROUP BY 1
    ORDER BY 1
    """
    volume_df = run_query(volume_sql)
    if not volume_df.empty:
        volume_df['month'] = pd.to_datetime(volume_df['month'])
        volume_df['month_str'] = volume_df['month'].dt.strftime('%Y-%m')
        st.subheader("Invoice Volume & Active Vendors by Month")
        st.dataframe(volume_df[['month_str', 'invoice_volume', 'active_vendors']].rename(
            columns={'month_str': 'Month', 'invoice_volume': 'Invoice volume by month', 'active_vendors': 'Active vendors by month'}
        ), use_container_width=True)
    
    top10_sql = f"""
    SELECT v.vendor_name, SUM(COALESCE(f.invoice_amount_local, 0)) AS spend
    FROM {DATABASE}.fact_all_sources_vw f
    LEFT JOIN {DATABASE}.dim_vendor_vw v ON f.vendor_id = v.vendor_id
    WHERE f.posting_date BETWEEN {start_lit} AND {end_lit}
      AND UPPER(f.invoice_status) NOT IN ('CANCELLED','REJECTED')
    GROUP BY 1
    ORDER BY spend DESC
    LIMIT 10
    """
    top10_df = run_query(top10_sql)
    if not top10_df.empty:
        st.subheader("Top 10 Vendors by Spend (YTD)")
        bar = alt.Chart(top10_df).mark_bar(color="#1e88e5", cornerRadiusTopLeft=4).encode(
            x=alt.X("spend:Q", axis=alt.Axis(title="Spend", format="~s")),
            y=alt.Y("vendor_name:N", sort="-x", axis=alt.Axis(title=None)),
            tooltip=["vendor_name", alt.Tooltip("spend:Q", format=",.0f")]
        ).properties(height=300)
        st.altair_chart(bar, use_container_width=True)
    
    st.markdown("---")
    st.caption("Query outputs")
    with st.expander("Show full result tables"):
        st.dataframe(top10_df, use_container_width=True)
    with st.expander("Show SQL used"):
        st.code(top10_sql, language="sql")

def run_vendor_analysis():
    st.markdown("## Vendor Analysis")
    st.info("Vendor analysis is under development. You can ask a specific question in the chat below.")

def run_payment_performance():
    st.markdown("## Payment Performance")
    st.info("Payment performance analysis is under development. You can ask a specific question in the chat below.")

def run_invoice_aging():
    st.markdown("## Invoice Aging")
    st.info("Invoice aging analysis is under development. You can ask a specific question in the chat below.")

def render_genie():
    # Sidebar for AI Assistant
    with st.sidebar:
        st.markdown("## AI Assistant")
        st.markdown("- Saved insights")
        st.markdown("- Frequently asked by you")
        st.markdown("- Most frequent (all)")
        st.markdown("---")
        st.markdown("### Start a Conversation")
        st.markdown("Ask questions about your Procurement to Pay data, or select a pre-built analysis from the library.")
    
    # Main area
    # Check if there is a pending prompt from Forecast or from quick buttons
    if "genie_prompt" in st.session_state and st.session_state.genie_prompt:
        prompt = st.session_state.genie_prompt
        st.session_state.genie_prompt = None
        if prompt == "Spending Overview":
            run_spending_overview()
        elif prompt == "Vendor Analysis":
            run_vendor_analysis()
        elif prompt == "Payment Performance":
            run_payment_performance()
        elif prompt == "Invoice Aging":
            run_invoice_aging()
        else:
            # Free‑text question – add to messages and process
            if "messages" not in st.session_state:
                st.session_state.messages = []
            st.session_state.messages.append({"role": "user", "content": prompt})
            with st.chat_message("user"):
                st.markdown(prompt)
            with st.chat_message("assistant"):
                with st.spinner("Generating SQL query..."):
                    sql, explanation = generate_sql(prompt)
                    if not sql:
                        st.error("Failed to generate SQL. Please rephrase your question.")
                        st.session_state.messages.append({"role": "assistant", "content": "Sorry, I couldn't generate a valid SQL query."})
                    else:
                        if not is_safe_sql(sql):
                            st.error("Generated SQL is not a SELECT statement or contains unsafe keywords.")
                        else:
                            sql = ensure_limit(sql)
                            with st.spinner("Running query on Athena..."):
                                df = run_query(sql)
                                if df.empty:
                                    st.warning("The query returned no data.")
                                    st.session_state.messages.append({"role": "assistant", "content": "The query returned no results.", "sql": sql, "df": df})
                                else:
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
        # After handling the prompt, we may still need to show the chat input below. We'll just return to avoid double rendering.
        # But we also want to show the chat input. So we will not return; instead we let the rest of the function run to display the chat input.
        # However, we must avoid showing the welcome screen again. We'll check if messages exist or if we just ran an analysis.
        # For simplicity, after running a pre‑defined analysis, we still show the chat input (which is at the end of this function).
        # So we do not return.
    
    # If there are no messages and no pending prompt, show the welcome screen with four cards
    if "messages" not in st.session_state or len(st.session_state.messages) == 0:
        st.markdown("# Welcome to ProcureIQ Genie")
        st.markdown("Let Genie run one of these quick analyses for you.")
        col1, col2 = st.columns(2)
        with col1:
            with st.container():
                st.markdown("### Spending Overview")
                st.markdown("Track total spend, monthly trends and major changes")
                if st.button("Ask Genie", key="btn_spending", use_container_width=True):
                    st.session_state.genie_prompt = "Spending Overview"
                    st.rerun()
        with col2:
            with st.container():
                st.markdown("### Vendor Analysis")
                st.markdown("Understand vendor-wise spend, concentration, and dependency")
                if st.button("Ask Genie", key="btn_vendor", use_container_width=True):
                    st.session_state.genie_prompt = "Vendor Analysis"
                    st.rerun()
        col3, col4 = st.columns(2)
        with col3:
            with st.container():
                st.markdown("### Payment Performance")
                st.markdown("Identify delays, late payments, and cycle time issues")
                if st.button("Ask Genie", key="btn_payment", use_container_width=True):
                    st.session_state.genie_prompt = "Payment Performance"
                    st.rerun()
        with col4:
            with st.container():
                st.markdown("### Invoice Aging")
                st.markdown("See overdue invoices, risk buckets, and problem areas")
                if st.button("Ask Genie", key="btn_aging", use_container_width=True):
                    st.session_state.genie_prompt = "Invoice Aging"
                    st.rerun()
        # Divider before chat input
        st.markdown("---")
    
    # Display existing chat messages (if any)
    if "messages" in st.session_state and len(st.session_state.messages) > 0:
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
        st.markdown("---")
    
    # Persistent chat input for free‑text questions
    if prompt := st.chat_input("Ask a question here..."):
        if "messages" not in st.session_state:
            st.session_state.messages = []
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)
        with st.chat_message("assistant"):
            with st.spinner("Generating SQL query..."):
                sql, explanation = generate_sql(prompt)
                if not sql:
                    st.error("Failed to generate SQL. Please rephrase your question.")
                    st.session_state.messages.append({"role": "assistant", "content": "Sorry, I couldn't generate a valid SQL query."})
                else:
                    if not is_safe_sql(sql):
                        st.error("Generated SQL is not a SELECT statement or contains unsafe keywords.")
                    else:
                        sql = ensure_limit(sql)
                        with st.spinner("Running query on Athena..."):
                            df = run_query(sql)
                            if df.empty:
                                st.warning("The query returned no data.")
                                st.session_state.messages.append({"role": "assistant", "content": "The query returned no results.", "sql": sql, "df": df})
                            else:
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
        st.rerun()

# ---------------------------- Forecast Page ----------------------------
def render_forecast():
    st.subheader("Cash Flow Need Forecast")
    cf_sql = """
    SELECT
        forecast_bucket,
        invoice_count,
        total_amount,
        earliest_due,
        latest_due
    FROM procure2pay.cash_flow_forecast_vw
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
        total_unpaid = cf_df[cf_df["forecast_bucket"] == "TOTAL_UNPAID"]["total_amount"].values[0] if not cf_df[cf_df["forecast_bucket"] == "TOTAL_UNPAID"].empty else 0
        overdue_now = cf_df[cf_df["forecast_bucket"] == "OVERDUE_NOW"]["total_amount"].values[0] if not cf_df[cf_df["forecast_bucket"] == "OVERDUE_NOW"].empty else 0
        due_30 = cf_df[cf_df["forecast_bucket"].isin(["DUE_7_DAYS","DUE_14_DAYS","DUE_30_DAYS"])]["total_amount"].sum()
        pct_due_30 = (due_30 / total_unpaid * 100) if total_unpaid > 0 else 0
        
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("TOTAL UNPAID", abbr_currency(total_unpaid))
        col2.metric("OVERDUE NOW", abbr_currency(overdue_now))
        col3.metric("DUE NEXT 30 DAYS", abbr_currency(due_30))
        col4.metric("% DUE ≤ 30 DAYS", f"{pct_due_30:.1f}%")
        
        st.markdown("---")
        st.subheader("Obligations by time bucket")
        st.dataframe(cf_df, use_container_width=True)
        csv = cf_df.to_csv(index=False).encode('utf-8')
        st.download_button("Download forecast (CSV)", data=csv, file_name="cash_flow_forecast.csv", mime="text/csv")
        
        chart_df = cf_df[~cf_df["forecast_bucket"].isin(["TOTAL_UNPAID", "PROCESSING_LAG_DAYS"])].copy()
        if not chart_df.empty:
            st.markdown("---")
            st.subheader("Forecast Distribution")
            chart = alt.Chart(chart_df).mark_bar(color="#10b981").encode(
                x=alt.X("forecast_bucket:N", sort=None, axis=alt.Axis(title=None, labelAngle=-30)),
                y=alt.Y("total_amount:Q", axis=alt.Axis(title="Amount", format="~s")),
                tooltip=["forecast_bucket", "total_amount"]
            ).properties(height=300)
            st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No cash flow forecast data")
    
    st.markdown("---")
    st.subheader("Action Playbook")
    st.markdown("Use these guided analyses to turn the forecast into decisions: who to pay now, who to pay early, and where we are at risk of paying late. Each button opens Genie with a pre‑built question.")
    actions = [
        ("📊 Forecast cash outflow (7–90 days)", "Spending Overview"),
        ("💰 Invoices to pay early to capture discounts", "Invoices to pay early to capture discounts"),
        ("⏰ Optimal payment timing for this week", "Optimal payment timing for this week"),
        ("⚠️ Late payment trend and risk", "Late payment trend and risk")
    ]
    cols = st.columns(2)
    for idx, (label, question) in enumerate(actions):
        with cols[idx % 2]:
            if st.button(label, use_container_width=True):
                st.session_state.page = "Genie"
                st.session_state.genie_prompt = question
                st.rerun()
    
    st.markdown("---")
    st.subheader("GR/IR Reconciliation")
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

# ---------------------------- Invoices Page ----------------------------
def render_invoices(initial_invoice_number=None):
    st.subheader("Invoices")
    st.markdown("Search, track and manage all invoices in one place")
    if initial_invoice_number:
        try:
            clean_num = str(int(float(initial_invoice_number)))
        except:
            clean_num = str(initial_invoice_number)
        st.session_state.invoice_search_term = clean_num
    else:
        st.session_state.invoice_search_term = st.session_state.get("invoice_search_term", "")
    col1, col2 = st.columns([3, 1])
    with col1:
        search_term = st.text_input("Search by Invoice or PO Number", value=st.session_state.invoice_search_term, placeholder="e.g., 9000946", label_visibility="collapsed", key="invoice_search_input")
    with col2:
        if st.button("Reset"):
            st.session_state.invoice_search_term = ""
            st.session_state.invoice_status_filter = "All Status"
            st.rerun()
    col_vendor, col_status = st.columns(2)
    with col_vendor:
        vendor_list = ["All Vendors"]
        vendor_df = run_query("SELECT DISTINCT vendor_name FROM procure2pay.dim_vendor_vw ORDER BY vendor_name")
        if not vendor_df.empty:
            vendor_list += vendor_df["vendor_name"].tolist()
        selected_vendor = st.selectbox("Vendor", vendor_list)
    with col_status:
        status_options = ["All Status", "OPEN", "PAID", "DISPUTED", "OVERDUE", "DUE_NEXT_30"]
        selected_status_display = st.selectbox("Status", status_options, index=status_options.index(st.session_state.get("invoice_status_filter", "All Status")) if st.session_state.get("invoice_status_filter", "All Status") in status_options else 0)
        selected_status = selected_status_display
        if selected_status == "DUE_NEXT_30":
            selected_status = "OPEN"
    where = []
    if search_term:
        safe_term = search_term.replace("'", "''")
        where.append(f"CAST(f.invoice_number AS VARCHAR) = '{safe_term}'")
    if selected_vendor != "All Vendors":
        safe_vendor = selected_vendor.replace("'", "''")
        where.append(f"UPPER(v.vendor_name) = UPPER('{safe_vendor}')")
    if selected_status_display != "All Status":
        if selected_status_display == "DUE_NEXT_30":
            where.append(f"UPPER(f.invoice_status) = 'OPEN' AND f.due_date >= CURRENT_DATE AND f.due_date <= DATE_ADD('day', 30, CURRENT_DATE)")
        else:
            where.append(f"UPPER(f.invoice_status) = '{selected_status}'")
    where_sql = " AND ".join(where) if where else "1=1"
    query = f"""
    SELECT DISTINCT
        f.invoice_number AS invoice_number,
        v.vendor_name AS vendor_name,
        f.posting_date AS posting_date,
        f.due_date AS due_date,
        f.invoice_amount_local AS invoice_amount,
        f.purchase_order_reference AS po_number,
        UPPER(f.invoice_status) AS status
    FROM {DATABASE}.fact_all_sources_vw f
    LEFT JOIN {DATABASE}.dim_vendor_vw v ON f.vendor_id = v.vendor_id
    WHERE {where_sql}
    ORDER BY f.posting_date DESC
    LIMIT 500
    """
    df = run_query(query)
    if not df.empty:
        df_display = df.rename(columns={
            'invoice_number': 'INVOICE NUMBER',
            'vendor_name': 'VENDOR NAME',
            'posting_date': 'POSTING DATE',
            'due_date': 'DUE DATE',
            'invoice_amount': 'INVOICE AMOUNT',
            'po_number': 'PO NUMBER',
            'status': 'STATUS'
        })
        st.dataframe(df_display, use_container_width=True, height=400)
        if search_term and len(df) == 1:
            inv_num = df.iloc[0, 0]
            st.markdown("---")
            st.subheader(f"Invoice Details: {inv_num}")
            details_sql = f"""
            SELECT
                f.invoice_number AS invoice_number,
                f.posting_date AS invoice_date,
                f.invoice_amount_local AS invoice_amount,
                f.purchase_order_reference AS po_number,
                f.po_amount AS po_amount,
                f.due_date AS due_date,
                f.invoice_status AS invoice_status,
                f.company_code AS company_code,
                f.fiscal_year AS fiscal_year,
                f.aging_days AS aging_days
            FROM {DATABASE}.fact_all_sources_vw f
            WHERE CAST(f.invoice_number AS VARCHAR) = '{inv_num}'
            LIMIT 1
            """
            details_df = run_query(details_sql)
            if not details_df.empty:
                details_display = details_df.rename(columns={
                    'invoice_number': 'INVOICE NUMBER',
                    'invoice_date': 'INVOICE DATE',
                    'invoice_amount': 'INVOICE AMOUNT',
                    'po_number': 'PO NUMBER',
                    'po_amount': 'PO AMOUNT',
                    'due_date': 'DUE DATE',
                    'invoice_status': 'INVOICE STATUS',
                    'company_code': 'COMPANY CODE',
                    'fiscal_year': 'FISCAL YEAR',
                    'aging_days': 'AGING DAYS'
                })
                st.dataframe(details_display, use_container_width=True)
            hist_sql = f"""
            SELECT
                invoice_number AS invoice_number,
                UPPER(status) AS status,
                effective_date AS effective_date,
                status_notes AS status_notes
            FROM {DATABASE}.invoice_status_history_vw
            WHERE CAST(invoice_number AS VARCHAR) = '{inv_num}'
            ORDER BY sequence_nbr
            """
            hist_df = run_query(hist_sql)
            if not hist_df.empty:
                st.subheader("Status History")
                hist_display = hist_df.rename(columns={
                    'invoice_number': 'INVOICE NUMBER',
                    'status': 'STATUS',
                    'effective_date': 'EFFECTIVE DATE',
                    'status_notes': 'STATUS NOTES'
                })
                st.dataframe(hist_display, use_container_width=True)
            vendor_info_sql = f"""
            SELECT DISTINCT
                v.vendor_id AS vendor_id,
                v.vendor_name AS vendor_name,
                v.vendor_name_2 AS vendor_name_2,
                v.country_code AS country_code,
                v.city AS city,
                v.postal_code AS postal_code,
                v.street AS street,
                v.region_code AS region_code,
                v.industry_sector AS industry_sector,
                v.vendor_account_group AS account_group,
                v.tax_number_1 AS tax_number_1,
                v.tax_number_2 AS tax_number_2
            FROM {DATABASE}.fact_all_sources_vw f
            LEFT JOIN {DATABASE}.dim_vendor_vw v ON f.vendor_id = v.vendor_id
            WHERE CAST(f.invoice_number AS VARCHAR) = '{inv_num}'
            """
            vendor_df = run_query(vendor_info_sql)
            if not vendor_df.empty:
                st.subheader("Vendor Information")
                vendor_display = vendor_df.rename(columns={
                    'vendor_id': 'VENDOR ID',
                    'vendor_name': 'VENDOR NAME',
                    'vendor_name_2': 'ALIAS / NAME 2',
                    'country_code': 'COUNTRY',
                    'city': 'CITY',
                    'postal_code': 'POSTAL CODE',
                    'street': 'STREET',
                    'region_code': 'REGION',
                    'industry_sector': 'INDUSTRY',
                    'account_group': 'ACCOUNT GROUP',
                    'tax_number_1': 'TAX NUMBER 1',
                    'tax_number_2': 'TAX NUMBER 2'
                })
                st.dataframe(vendor_display, use_container_width=True)
            company_sql = f"""
            SELECT DISTINCT
                f.company_code AS company_code,
                COALESCE(cc.company_name, 'N/A') AS company_name,
                f.plant_code AS plant_code,
                COALESCE(plt.plant_name, 'N/A') AS plant_name
            FROM {DATABASE}.fact_all_sources_vw f
            LEFT JOIN {DATABASE}.dim_company_code_vw cc ON f.company_code = cc.company_code
            LEFT JOIN {DATABASE}.dim_plant_vw plt ON f.plant_code = plt.plant_code
            WHERE CAST(f.invoice_number AS VARCHAR) = '{inv_num}'
            """
            company_df = run_query(company_sql)
            if not company_df.empty:
                st.subheader("Company & Plant Information")
                company_display = company_df.rename(columns={
                    'company_code': 'COMPANY CODE',
                    'company_name': 'COMPANY NAME',
                    'plant_code': 'PLANT CODE',
                    'plant_name': 'PLANT NAME'
                })
                st.dataframe(company_display, use_container_width=True)
    else:
        st.info("No invoices found.")

# ---------------------------- Dashboard Page ----------------------------
def load_custom_css():
    st.markdown("""
    <style>
    div[data-testid="column"] button { background-color: transparent; border: none; box-shadow: none; padding: 0.25rem 0.5rem; margin: 0; font-weight: bold; width: 100%; text-align: center; border-radius: 8px; transition: background-color 0.2s; }
    div[data-testid="column"] button:hover { background-color: rgba(0,0,0,0.05); border: none; }
    div[data-testid="column"] > div:has(button) { margin-top: 0; padding-top: 0; }
    </style>
    """, unsafe_allow_html=True)

def render_dashboard():
    load_custom_css()
    if "preset" not in st.session_state:
        st.session_state.preset = "Last 30 Days"
    if "date_range" not in st.session_state:
        st.session_state.date_range = compute_range_preset(st.session_state.preset)
    col_date, col_vendor, col_preset = st.columns([2, 2, 3])
    with col_date:
        date_range = st.date_input("Date Range", value=st.session_state.date_range, format="YYYY-MM-DD", label_visibility="collapsed", key="date_picker")
        if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
            new_start, new_end = date_range
        else:
            new_start, new_end = st.session_state.date_range
        if (new_start, new_end) != st.session_state.date_range:
            st.session_state.date_range = (new_start, new_end)
            st.session_state.preset = "Custom"
            st.rerun()
    with col_vendor:
        vendor_sql = f"""
        SELECT DISTINCT v.vendor_name
        FROM {DATABASE}.fact_all_sources_vw f
        LEFT JOIN {DATABASE}.dim_vendor_vw v ON f.vendor_id = v.vendor_id
        WHERE f.posting_date BETWEEN {sql_date(st.session_state.date_range[0])} AND {sql_date(st.session_state.date_range[1])}
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
    start_lit = sql_date(st.session_state.date_range[0])
    end_lit = sql_date(st.session_state.date_range[1])
    p_start, p_end = prior_window(st.session_state.date_range[0], st.session_state.date_range[1])
    p_start_lit = sql_date(p_start)
    p_end_lit = sql_date(p_end)
    vendor_where = build_vendor_where(selected_vendor)
    
    cur_kpi_sql = f"""
    SELECT
        COUNT(DISTINCT CASE WHEN UPPER(invoice_status) = 'OPEN' THEN purchase_order_reference END) AS active_pos,
        COUNT(DISTINCT purchase_order_reference) AS total_pos,
        COUNT(DISTINCT v.vendor_name) AS active_vendors,
        SUM(CASE WHEN UPPER(invoice_status) NOT IN ('CANCELLED','REJECTED')
                 THEN COALESCE(invoice_amount_local, 0) ELSE 0 END) AS total_spend,
        COUNT(DISTINCT CASE WHEN UPPER(invoice_status) = 'OPEN' THEN invoice_number END) AS pending_inv,
        AVG(CASE WHEN UPPER(invoice_status) = 'PAID' THEN DATE_DIFF('day', posting_date, payment_date) END) AS avg_processing_days
    FROM {DATABASE}.fact_all_sources_vw f
    LEFT JOIN {DATABASE}.dim_vendor_vw v ON f.vendor_id = v.vendor_id
    WHERE f.posting_date BETWEEN {start_lit} AND {end_lit}
    {vendor_where}
    """
    cur_df = run_query(cur_kpi_sql)
    cur_spend = safe_number(cur_df.loc[0, "total_spend"]) if not cur_df.empty else 0
    cur_active_pos = safe_int(cur_df.loc[0, "active_pos"]) if not cur_df.empty else 0
    cur_total_pos = safe_int(cur_df.loc[0, "total_pos"]) if not cur_df.empty else 0
    cur_active_vendors = safe_int(cur_df.loc[0, "active_vendors"]) if not cur_df.empty else 0
    cur_pending = safe_int(cur_df.loc[0, "pending_inv"]) if not cur_df.empty else 0
    cur_avg_processing = safe_number(cur_df.loc[0, "avg_processing_days"]) if not cur_df.empty else 0
    
    prev_kpi_sql = f"""
    SELECT
        COUNT(DISTINCT CASE WHEN UPPER(invoice_status) = 'OPEN' THEN purchase_order_reference END) AS active_pos,
        COUNT(DISTINCT purchase_order_reference) AS total_pos,
        COUNT(DISTINCT v.vendor_name) AS active_vendors,
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
    prev_active_pos = safe_int(prev_df.loc[0, "active_pos"]) if not prev_df.empty else 0
    prev_total_pos = safe_int(prev_df.loc[0, "total_pos"]) if not prev_df.empty else 0
    prev_active_vendors = safe_int(prev_df.loc[0, "active_vendors"]) if not prev_df.empty else 0
    prev_pending = safe_int(prev_df.loc[0, "pending_inv"]) if not prev_df.empty else 0
    
    spend_delta, _ = pct_delta(cur_spend, prev_spend)
    active_pos_delta, _ = pct_delta(cur_active_pos, prev_active_pos)
    total_pos_delta, _ = pct_delta(cur_total_pos, prev_total_pos)
    active_vendors_delta, _ = pct_delta(cur_active_vendors, prev_active_vendors)
    pending_delta, _ = pct_delta(cur_pending, prev_pending)
    
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
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("TOTAL SPEND", abbr_currency(cur_spend), delta=spend_delta, delta_color="normal")
    col2.metric("ACTIVE PO'S", f"{cur_active_pos:,}", delta=active_pos_delta, delta_color="normal")
    col3.metric("TOTAL PO'S", f"{cur_total_pos:,}", delta=total_pos_delta, delta_color="normal")
    col4.metric("ACTIVE VENDORS", f"{cur_active_vendors:,}", delta=active_vendors_delta, delta_color="normal")
    
    col5, col6, col7, col8 = st.columns(4)
    col5.metric("PENDING INVOICES", f"{cur_pending:,}", delta=pending_delta, delta_color="normal")
    col6.metric("AVG INVOICE PROCESSING TIME", f"{cur_avg_processing:.1f}d")
    col7.metric("FIRST PASS INVOICES %", f"{first_pass_rate:.1f}%")
    col8.metric("AUTOPROCESSED %", f"{auto_rate:.1f}%")
    st.markdown("---")
    
    st.subheader("Needs Attention")
    attention_sql = f"""
    SELECT
        f.invoice_number,
        v.vendor_name,
        f.invoice_amount_local AS amount,
        f.due_date,
        CASE
            WHEN f.due_date < CURRENT_DATE AND UPPER(f.invoice_status) = 'OVERDUE' THEN 'Overdue'
            WHEN UPPER(f.invoice_status) IN ('DISPUTE','DISPUTED') THEN 'Disputed'
            WHEN f.due_date >= CURRENT_DATE AND f.due_date <= DATE_ADD('day', 30, CURRENT_DATE) AND UPPER(f.invoice_status) = 'OPEN' THEN 'Due Next 30 Days'
        END AS attention_type,
        f.aging_days
    FROM {DATABASE}.fact_all_sources_vw f
    LEFT JOIN {DATABASE}.dim_vendor_vw v ON f.vendor_id = v.vendor_id
    WHERE f.posting_date BETWEEN {start_lit} AND {end_lit}
    {vendor_where}
      AND (
          (f.due_date < CURRENT_DATE AND UPPER(f.invoice_status) = 'OVERDUE')
          OR UPPER(f.invoice_status) IN ('DISPUTE','DISPUTED')
          OR (f.due_date >= CURRENT_DATE AND f.due_date <= DATE_ADD('day', 30, CURRENT_DATE) AND UPPER(f.invoice_status) = 'OPEN')
      )
    ORDER BY f.due_date ASC
    """
    attention_df = run_query(attention_sql)
    if not attention_df.empty:
        col_type1, col_type2, col_type3 = st.columns(3)
        with col_type1:
            if st.button(f"⚠️ Overdue ({len(attention_df[attention_df['attention_type']=='Overdue'])})", use_container_width=True):
                st.session_state.page = "Invoices"
                st.session_state.invoice_status_filter = "OVERDUE"
                st.session_state.attention_page = 0
                st.rerun()
        with col_type2:
            if st.button(f"⚖️ Disputed ({len(attention_df[attention_df['attention_type']=='Disputed'])})", use_container_width=True):
                st.session_state.page = "Invoices"
                st.session_state.invoice_status_filter = "DISPUTED"
                st.session_state.attention_page = 0
                st.rerun()
        with col_type3:
            if st.button(f"📅 Due Next 30 Days ({len(attention_df[attention_df['attention_type']=='Due Next 30 Days'])})", use_container_width=True):
                st.session_state.page = "Invoices"
                st.session_state.invoice_status_filter = "DUE_NEXT_30"
                st.session_state.attention_page = 0
                st.rerun()
        st.markdown("---")
        items_per_page = 10
        total_items = len(attention_df)
        if "attention_page" not in st.session_state:
            st.session_state.attention_page = 0
        total_pages = (total_items - 1) // items_per_page + 1 if total_items > 0 else 1
        start_idx = st.session_state.attention_page * items_per_page
        end_idx = min(start_idx + items_per_page, total_items)
        page_df = attention_df.iloc[start_idx:end_idx]
        rows = page_df.to_dict('records')
        for i in range(0, len(rows), 5):
            cols = st.columns(5)
            for col_idx, col in enumerate(cols):
                if i + col_idx < len(rows):
                    row = rows[i + col_idx]
                    inv_num = row['invoice_number']
                    vendor = row['vendor_name']
                    amount = row['amount']
                    due_date = row['due_date']
                    att_type = row['attention_type']
                    if att_type == "Overdue":
                        border_color = "#ef4444"
                        bg_color = "#fee2e2"
                    elif att_type == "Disputed":
                        border_color = "#f59e0b"
                        bg_color = "#fef3c7"
                    else:
                        border_color = "#3b82f6"
                        bg_color = "#dbeafe"
                    with col:
                        st.markdown(f"""
                        <div style="border: 1px solid {border_color}; border-radius: 12px; padding: 0.5rem; background-color: {bg_color}; margin-bottom: 0.5rem;">
                            <div style="font-weight: bold; margin-bottom: 0.25rem;">{vendor}</div>
                            <div style="font-size: 0.8rem; color: {border_color}; margin-bottom: 0.25rem;">{att_type}</div>
                            <div style="font-size: 1.1rem; font-weight: 600; margin-bottom: 0.25rem;">{abbr_currency(amount)}</div>
                            <div style="font-size: 0.7rem; margin-bottom: 0.5rem;">Due: {due_date}</div>
                        </div>
                        """, unsafe_allow_html=True)
                        try:
                            clean_inv = str(int(float(inv_num)))
                        except:
                            clean_inv = str(inv_num)
                        if st.button(f"📄 Invoice {clean_inv}", key=f"att_{inv_num}_{start_idx+col_idx}", use_container_width=True):
                            st.session_state.page = "Invoices"
                            st.session_state.invoice_search_term = clean_inv
                            st.rerun()
        col_prev, col_page_info, col_next = st.columns([1, 2, 1])
        with col_prev:
            if st.button("← Prev", disabled=(st.session_state.attention_page == 0)):
                st.session_state.attention_page -= 1
                st.rerun()
        with col_page_info:
            st.markdown(f"<div style='text-align: center;'>Page {st.session_state.attention_page + 1} of {total_pages}</div>", unsafe_allow_html=True)
        with col_next:
            if st.button("Next →", disabled=(st.session_state.attention_page >= total_pages - 1)):
                st.session_state.attention_page += 1
                st.rerun()
    else:
        st.info("No attention items found for the selected period.")
    st.markdown("---")
    
    st.subheader("Analytics")
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
    """
    status_df = run_query(status_sql)
    if not status_df.empty:
        total = status_df['cnt'].sum()
        status_df['percentage'] = (status_df['cnt'] / total * 100).round(1)
        pie = alt.Chart(status_df).mark_arc(innerRadius=40).encode(
            theta=alt.Theta(field="cnt", type="quantitative"),
            color=alt.Color(field="status", type="nominal", scale=alt.Scale(scheme="pastel1")),
            tooltip=["status", "cnt", alt.Tooltip("percentage:Q", format=".1f")]
        ).properties(title="Invoice Status", height=300)
        st.altair_chart(pie, use_container_width=True)
    else:
        st.info("No invoice status data")
    
    top_vendors_sql = f"""
    SELECT v.vendor_name, SUM(COALESCE(f.invoice_amount_local, 0)) AS spend
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
        total_spend = top_df['spend'].sum()
        top_df['percentage'] = (top_df['spend'] / total_spend * 100).round(1)
        bar = alt.Chart(top_df).mark_bar(color="#1e88e5", cornerRadiusTopLeft=4).encode(
            x=alt.X("spend:Q", axis=alt.Axis(title="Spend", format="~s")),
            y=alt.Y("vendor_name:N", sort="-x", axis=alt.Axis(title=None)),
            tooltip=["vendor_name", alt.Tooltip("spend:Q", format=",.0f"), alt.Tooltip("percentage:Q", format=".1f")]
        ).properties(title="Top 10 Vendors by Spend", height=300)
        st.altair_chart(bar, use_container_width=True)
    else:
        st.info("No vendor spend data")
    
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
        trend_long = trend_df.melt(id_vars=["month"], value_vars=["actual", "forecast"], var_name="type", value_name="amount")
        line = alt.Chart(trend_long).mark_line(point=True).encode(
            x=alt.X("month:N", axis=alt.Axis(title=None, labelAngle=-45)),
            y=alt.Y("amount:Q", axis=alt.Axis(title="Spend", format="~s")),
            color=alt.Color("type:N", scale=alt.Scale(domain=["actual", "forecast"], range=["#1e88e5", "#ffb74d"])),
            tooltip=["month", "type", alt.Tooltip("amount:Q", format=",.0f")]
        ).properties(title="Monthly Spend Trend (Actual + Forecast)", height=300)
        st.altair_chart(line, use_container_width=True)
    else:
        st.info("No trend data")

# ---------------------------- Main App Layout ----------------------------
logo_url = "https://th.bing.com/th/id/OIP.Vy1yFQtg8-D1SsAxcqqtSgHaE6?w=235&h=180&c=7&r=0&o=7&dpr=1.5&pid=1.7&rm=3"
col_title, col_nav, col_logo = st.columns([1, 3, 1])
with col_title:
    st.markdown("<h1 style='font-weight: bold; margin-bottom: 0;'>procure2pay</h1>", unsafe_allow_html=True)
with col_nav:
    nav_cols = st.columns(4)
    with nav_cols[0]:
        if st.button("Dashboard", use_container_width=True):
            st.session_state.page = "Dashboard"
            st.rerun()
    with nav_cols[1]:
        if st.button("Genie", use_container_width=True):
            st.session_state.page = "Genie"
            st.rerun()
    with nav_cols[2]:
        if st.button("Forecast", use_container_width=True):
            st.session_state.page = "Forecast"
            st.rerun()
    with nav_cols[3]:
        if st.button("Invoices", use_container_width=True):
            st.session_state.page = "Invoices"
            st.rerun()
with col_logo:
    st.image(logo_url, width=50)
st.markdown("<p style='font-size: 0.9rem; color: gray; margin-top: -0.5rem;'>p2pAnalytics</p>", unsafe_allow_html=True)
st.markdown("---")

if "page" not in st.session_state:
    st.session_state.page = "Dashboard"

if st.session_state.page == "Dashboard":
    render_dashboard()
elif st.session_state.page == "Genie":
    render_genie()
elif st.session_state.page == "Forecast":
    render_forecast()
else:
    render_invoices()
