# genie.py
import streamlit as st
import pandas as pd
import json
import html
import uuid
from datetime import datetime, date
from athena_client import run_query
from bedrock_client import ask_bedrock
from utils import abbr_currency, auto_chart, alt_line_monthly, alt_bar, alt_donut_status, ensure_limit, is_safe_sql, safe_number
from semantic_model import SYSTEM_PROMPT, DESCRIPTIVE_PROMPT_TEMPLATE, generate_sql
from persistence import get_saved_insights_cached, get_frequent_questions_by_user_cached, get_frequent_questions_all_cached, save_chat_message, save_question, set_cache, get_cache
from quick_analysis import run_quick_analysis
from config import DATABASE

# ------------------------------------------------------------
# Cash Flow Forecast handler
# ------------------------------------------------------------
def process_cash_flow_forecast(question: str) -> dict:
    cf_sql = f"""
        SELECT
            forecast_bucket,
            invoice_count,
            total_amount,
            earliest_due,
            latest_due
        FROM {DATABASE}.cash_flow_forecast_vw
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
    if cf_df.empty:
        cf_sql_fallback = f"""
            WITH base AS (
                SELECT
                    invoice_number,
                    invoice_amount_local,
                    due_date,
                    invoice_status,
                    DATE_DIFF('day', CURRENT_DATE, due_date) AS days_until_due
                FROM {DATABASE}.fact_all_sources_vw
                WHERE UPPER(invoice_status) IN ('OPEN', 'DUE', 'OVERDUE')
                  AND due_date IS NOT NULL
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
            total AS (
                SELECT 'TOTAL_UNPAID' AS forecast_bucket,
                       SUM(invoice_count) AS invoice_count,
                       SUM(total_amount) AS total_amount,
                       NULL AS earliest_due,
                       NULL AS latest_due
                FROM buckets
            )
            SELECT * FROM total
            UNION ALL SELECT * FROM buckets
            ORDER BY CASE forecast_bucket
                WHEN 'TOTAL_UNPAID' THEN 0
                WHEN 'OVERDUE_NOW' THEN 1
                WHEN 'DUE_7_DAYS' THEN 2
                WHEN 'DUE_14_DAYS' THEN 3
                WHEN 'DUE_30_DAYS' THEN 4
                WHEN 'DUE_60_DAYS' THEN 5
                WHEN 'DUE_90_DAYS' THEN 6
                ELSE 7 END
        """
        cf_df = run_query(cf_sql_fallback)
        used_sql = cf_sql_fallback
    else:
        used_sql = cf_sql

    if cf_df.empty:
        return {"layout": "error", "message": "No cash flow forecast data available."}

    cf_df.columns = [c.lower() for c in cf_df.columns]
    data_preview = cf_df.to_string(index=False)
    prompt = f"""
You are a senior procurement analyst. Based on the following cash flow forecast data, write a response with two sections:

1. **Descriptive** – What the data shows. Cite exact numbers for each bucket (TOTAL_UNPAID, OVERDUE_NOW, DUE_7_DAYS, DUE_14_DAYS, DUE_30_DAYS, DUE_60_DAYS, DUE_90_DAYS, BEYOND_90_DAYS). Explain the cash outflow expected in each period.
2. **Prescriptive** – Specific recommended actions and risks based on the data. List 3‑5 bullet points. Each bullet must include a specific finding, a concrete action, and a brief 'Why it matters'.

Data:
{data_preview}

Respond in plain text, using markdown for headings and bullet points. Do not include any extra commentary.
"""
    analyst_text = ask_bedrock(prompt, system_prompt="You are a helpful procurement analyst focusing on cash flow management.")
    if not analyst_text:
        analyst_text = "Unable to generate insights at this time."

    return {
        "layout": "cash_flow",
        "df": cf_df.to_dict(orient="records"),
        "sql": used_sql,
        "analyst_response": analyst_text,
        "question": question
    }

# ------------------------------------------------------------
# Early Payment Candidates handler
# ------------------------------------------------------------
def process_early_payment(question: str) -> dict:
    ep_sql = f"""
        SELECT
            document_number,
            vendor_name,
            invoice_amount,
            due_date,
            days_until_due,
            savings_if_2pct_discount,
            vendor_tier,
            early_pay_priority
        FROM {DATABASE}.early_payment_candidates_vw
        ORDER BY early_pay_priority ASC, savings_if_2pct_discount DESC
        LIMIT 20
    """
    ep_df = run_query(ep_sql)
    if ep_df.empty:
        ep_sql_fallback = f"""
            SELECT
                CAST(f.invoice_number AS VARCHAR) AS document_number,
                v.vendor_name,
                f.invoice_amount_local AS invoice_amount,
                f.due_date,
                DATE_DIFF('day', CURRENT_DATE, f.due_date) AS days_until_due,
                ROUND(f.invoice_amount_local * 0.02, 2) AS savings_if_2pct_discount,
                CASE WHEN DATE_DIFF('day', CURRENT_DATE, f.due_date) <= 7 THEN 'High'
                     WHEN DATE_DIFF('day', CURRENT_DATE, f.due_date) <= 14 THEN 'Medium'
                     ELSE 'Low' END AS early_pay_priority
            FROM {DATABASE}.fact_all_sources_vw f
            LEFT JOIN {DATABASE}.dim_vendor_vw v ON f.vendor_id = v.vendor_id
            WHERE UPPER(f.invoice_status) IN ('OPEN', 'DUE')
              AND f.due_date > CURRENT_DATE
              AND DATE_DIFF('day', CURRENT_DATE, f.due_date) <= 30
            ORDER BY early_pay_priority ASC, savings_if_2pct_discount DESC
            LIMIT 20
        """
        ep_df = run_query(ep_sql_fallback)
        used_sql = ep_sql_fallback
    else:
        used_sql = ep_sql

    if ep_df.empty:
        return {"layout": "error", "message": "No early payment candidates found."}

    ep_df.columns = [c.lower() for c in ep_df.columns]
    data_preview = ep_df.head(10).to_string(index=False)
    prompt = f"""
You are a senior procurement analyst. Based on the following list of invoices that are candidates for early payment (to capture discounts), write a response with two sections:

1. **Descriptive** – Summarize the total potential savings, the number of high‑priority invoices, and the range of due dates. Highlight the top 2‑3 invoices with the largest savings.
2. **Prescriptive** – Specific recommendations: which invoices to pay first, how to sequence payments to maximize discounts, and any risks (e.g., cash flow constraints). Provide 3‑5 bullet points with specific findings, actions, and why it matters.

Data (top 10 rows):
{data_preview}

Respond in plain text, using markdown for headings and bullet points. Do not include any extra commentary.
"""
    analyst_text = ask_bedrock(prompt, system_prompt="You are a helpful procurement analyst specializing in working capital optimization.")
    if not analyst_text:
        analyst_text = "Unable to generate insights at this time."

    return {
        "layout": "early_payment",
        "df": ep_df.to_dict(orient="records"),
        "sql": used_sql,
        "analyst_response": analyst_text,
        "question": question
    }

# ------------------------------------------------------------
# Optimal Payment Timing handler
# ------------------------------------------------------------
def process_payment_timing(question: str) -> dict:
    timing_sql = f"""
        WITH due_buckets AS (
            SELECT
                CASE
                    WHEN due_date < CURRENT_DATE THEN 'Overdue'
                    WHEN due_date <= CURRENT_DATE + INTERVAL '7' DAY THEN 'Due in 0-7 days'
                    WHEN due_date <= CURRENT_DATE + INTERVAL '14' DAY THEN 'Due in 8-14 days'
                    WHEN due_date <= CURRENT_DATE + INTERVAL '30' DAY THEN 'Due in 15-30 days'
                    ELSE 'Due later'
                END AS payment_window,
                COUNT(*) AS invoice_count,
                SUM(invoice_amount_local) AS total_amount
            FROM {DATABASE}.fact_all_sources_vw
            WHERE UPPER(invoice_status) IN ('OPEN', 'DUE')
            GROUP BY 1
        )
        SELECT * FROM due_buckets ORDER BY
            CASE payment_window
                WHEN 'Overdue' THEN 1
                WHEN 'Due in 0-7 days' THEN 2
                WHEN 'Due in 8-14 days' THEN 3
                WHEN 'Due in 15-30 days' THEN 4
                ELSE 5
            END
    """
    timing_df = run_query(timing_sql)
    if timing_df.empty:
        return {"layout": "error", "message": "No payment timing data available."}

    timing_df.columns = [c.lower() for c in timing_df.columns]
    data_preview = timing_df.to_string(index=False)
    prompt = f"""
You are a senior procurement analyst. Based on the following payment timing buckets (overdue, due in 0-7 days, 8-14 days, 15-30 days, later), write a response with two sections:

1. **Descriptive** – Summarize the total amounts due in each window, highlighting the most urgent buckets (overdue and 0-7 days). Mention the number of invoices.
2. **Prescriptive** – Provide a recommended payment schedule for this week. Prioritize overdue invoices to avoid penalties, then invoices due in 0-7 days to maintain supplier relationships. Suggest cash allocation percentages. List 3‑5 bullet points with specific findings, actions, and why it matters.

Data:
{data_preview}

Respond in plain text, using markdown for headings and bullet points. Do not include any extra commentary.
"""
    analyst_text = ask_bedrock(prompt, system_prompt="You are a helpful procurement analyst focusing on cash flow timing.")
    if not analyst_text:
        analyst_text = "Unable to generate insights at this time."

    return {
        "layout": "payment_timing",
        "df": timing_df.to_dict(orient="records"),
        "sql": timing_sql,
        "analyst_response": analyst_text,
        "question": question
    }

# ------------------------------------------------------------
# Late Payment Trend handler
# ------------------------------------------------------------
def process_late_payment_trend(question: str) -> dict:
    trend_sql = f"""
        SELECT
            DATE_TRUNC('month', payment_date) AS month,
            COUNT(*) AS total_payments,
            SUM(CASE WHEN payment_date > due_date THEN 1 ELSE 0 END) AS late_payments,
            AVG(CASE WHEN payment_date > due_date THEN DATE_DIFF('day', due_date, payment_date) END) AS avg_late_days
        FROM {DATABASE}.fact_all_sources_vw
        WHERE payment_date IS NOT NULL
          AND payment_date >= DATE_ADD('month', -12, CURRENT_DATE)
        GROUP BY 1
        ORDER BY 1
    """
    trend_df = run_query(trend_sql)
    if trend_df.empty:
        return {"layout": "error", "message": "No payment trend data available."}

    trend_df.columns = [c.lower() for c in trend_df.columns]
    trend_df["late_pct"] = (trend_df["late_payments"] / trend_df["total_payments"]) * 100
    data_preview = trend_df.tail(6).to_string(index=False)
    prompt = f"""
You are a senior procurement analyst. Based on the following monthly payment performance data (last 12 months), write a response with two sections:

1. **Descriptive** – Describe the trend in late payments (percentage and average days late). Identify any months with spikes or improvements. Cite specific numbers.
2. **Prescriptive** – Recommend actions to reduce late payments, such as process improvements, early payment discounts, or supplier communication. List 3‑5 bullet points with specific findings, actions, and why it matters.

Data (last 6 months):
{data_preview}

Respond in plain text, using markdown for headings and bullet points. Do not include any extra commentary.
"""
    analyst_text = ask_bedrock(prompt, system_prompt="You are a helpful procurement analyst focusing on payment performance.")
    if not analyst_text:
        analyst_text = "Unable to generate insights at this time."

    return {
        "layout": "late_payment_trend",
        "df": trend_df.to_dict(orient="records"),
        "sql": trend_sql,
        "analyst_response": analyst_text,
        "question": question
    }

# ------------------------------------------------------------
# Generic custom query processor
# ------------------------------------------------------------
def process_custom_query(query: str) -> dict:
    sql, _ = generate_sql(query)
    if not sql or not is_safe_sql(sql):
        return {"layout": "error", "message": "Failed to generate valid SQL."}
    sql = ensure_limit(sql)
    df = run_query(sql)
    if df.empty:
        return {"layout": "error", "message": "Query returned no data."}
    data_preview = df.head(10).to_string(index=False, max_colwidth=40)
    prompt = DESCRIPTIVE_PROMPT_TEMPLATE.format(question=query, sql=sql, data_preview=data_preview)
    analyst_text = ask_bedrock(prompt, system_prompt="You are a helpful procurement analyst. Provide concise, data-driven insights.")
    return {
        "layout": "analyst",
        "sql": sql,
        "df": df.to_dict(orient="records"),
        "question": query,
        "analyst_response": analyst_text
    }

# ------------------------------------------------------------
# Rendering functions for each layout type
# ------------------------------------------------------------
def render_cash_flow_response(result: dict):
    df = pd.DataFrame(result["df"])
    if df.empty:
        st.error("No cash flow data to display.")
        return
    total_unpaid = df[df["forecast_bucket"] == "TOTAL_UNPAID"]["total_amount"].values[0] if not df[df["forecast_bucket"] == "TOTAL_UNPAID"].empty else 0
    overdue_now = df[df["forecast_bucket"] == "OVERDUE_NOW"]["total_amount"].values[0] if not df[df["forecast_bucket"] == "OVERDUE_NOW"].empty else 0
    due_30 = df[df["forecast_bucket"].isin(["DUE_7_DAYS","DUE_14_DAYS","DUE_30_DAYS"])]["total_amount"].sum()
    pct_due_30 = (due_30 / total_unpaid * 100) if total_unpaid > 0 else 0
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Unpaid", abbr_currency(total_unpaid))
    with col2:
        st.metric("Overdue Now", abbr_currency(overdue_now))
    with col3:
        st.metric("Due Next 30 Days", f"{abbr_currency(due_30)} ({pct_due_30:.0f}%)")
    chart_df = df[df["forecast_bucket"] != "TOTAL_UNPAID"].copy()
    if not chart_df.empty:
        st.subheader("Cash Outflow by Time Bucket")
        alt_bar(chart_df, x="forecast_bucket", y="total_amount", horizontal=True, height=300, color="#3b82f6")
    st.subheader("Forecast Details")
    st.dataframe(df, use_container_width=True, hide_index=True)
    if result.get("analyst_response"):
        st.markdown("### 💡 Key Insights")
        st.markdown(result["analyst_response"])
    with st.expander("View SQL used"):
        st.code(result["sql"], language="sql")

def render_early_payment_response(result: dict):
    df = pd.DataFrame(result["df"])
    if df.empty:
        st.error("No early payment candidates.")
        return
    total_savings = df["savings_if_2pct_discount"].sum()
    high_priority = df[df["early_pay_priority"] == "High"].shape[0]
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Total Potential Savings", abbr_currency(total_savings))
    with col2:
        st.metric("High‑Priority Invoices", high_priority)
    st.subheader("Top Candidates for Early Payment")
    st.dataframe(df.head(10), use_container_width=True, hide_index=True)
    if result.get("analyst_response"):
        st.markdown("### 💡 Key Insights")
        st.markdown(result["analyst_response"])
    with st.expander("View SQL used"):
        st.code(result["sql"], language="sql")

def render_payment_timing_response(result: dict):
    df = pd.DataFrame(result["df"])
    if df.empty:
        st.error("No payment timing data.")
        return
    st.subheader("Payment Timing Summary")
    st.dataframe(df, use_container_width=True, hide_index=True)
    if result.get("analyst_response"):
        st.markdown("### 💡 Key Insights")
        st.markdown(result["analyst_response"])
    with st.expander("View SQL used"):
        st.code(result["sql"], language="sql")

def render_late_payment_trend_response(result: dict):
    df = pd.DataFrame(result["df"])
    if df.empty:
        st.error("No trend data.")
        return
    if not df.empty and "month" in df.columns:
        df["month_str"] = pd.to_datetime(df["month"]).dt.strftime("%b %Y")
        chart_df = df[["month_str", "late_pct"]].rename(columns={"late_pct": "VALUE"})
        st.subheader("Late Payment Percentage Trend")
        alt_line_monthly(chart_df, month_col="month_str", value_col="VALUE", height=300, title="Late Payments %")
        if "avg_late_days" in df.columns:
            days_df = df[["month_str", "avg_late_days"]].rename(columns={"avg_late_days": "VALUE"})
            st.subheader("Average Days Late")
            alt_line_monthly(days_df, month_col="month_str", value_col="VALUE", height=300, title="Avg Days Late")
    st.subheader("Payment Performance Data")
    st.dataframe(df, use_container_width=True, hide_index=True)
    if result.get("analyst_response"):
        st.markdown("### 💡 Key Insights")
        st.markdown(result["analyst_response"])
    with st.expander("View SQL used"):
        st.code(result["sql"], language="sql")

def render_quick_analysis_response(result: dict):
    analysis_type = result.get("type", "spending_overview")
    metrics = result.get("metrics", {})
    anomaly = result.get("anomaly")
    monthly_df = result.get("monthly_df")
    vendors_df = result.get("vendors_df")

    if metrics:
        st.markdown("### 📊 Key Metrics")
        cols = st.columns(len(metrics))
        colors = ["#fef3c7", "#dbeafe", "#dcfce7", "#fce7f3", "#e0e7ff", "#fef9c3"]
        for i, (key, value) in enumerate(metrics.items()):
            with cols[i]:
                label = key.replace("_", " ").title()
                if isinstance(value, (int, float)):
                    if "pct" in key or "rate" in key:
                        display = f"{value:.1f}%"
                    elif "spend" in key or "amount" in key:
                        display = abbr_currency(value)
                    else:
                        display = f"{value:,}"
                else:
                    display = str(value)
                st.markdown(f"""
                <div style="background-color: {colors[i % len(colors)]}; border-radius: 12px; padding: 12px; text-align: center;">
                    <div style="font-size: 0.85rem; color: #4b5563;">{label}</div>
                    <div style="font-size: 1.5rem; font-weight: 700; color: #1f2937;">{display}</div>
                </div>
                """, unsafe_allow_html=True)

    if anomaly:
        st.warning(f"⚠️ **Anomaly Detected**\n\n{anomaly}")

    if analysis_type == "spending_overview" and monthly_df is not None and not monthly_df.empty:
        monthly_df.columns = [c.upper() for c in monthly_df.columns]
        if "MONTH" in monthly_df.columns:
            monthly_df = monthly_df.rename(columns={"MONTH": "MONTH_STR"})
        elif "MONTH_STR" not in monthly_df.columns:
            monthly_df = monthly_df.rename(columns={monthly_df.columns[0]: "MONTH_STR"})
        st.subheader("Spending Trends")
        col1, col2, col3 = st.columns(3)
        with col1:
            if "MONTHLY_SPEND" in monthly_df.columns:
                spend_df = monthly_df[["MONTH_STR", "MONTHLY_SPEND"]].rename(columns={"MONTHLY_SPEND": "VALUE"})
                alt_line_monthly(spend_df, month_col="MONTH_STR", value_col="VALUE", height=200, title="Monthly Spend")
        with col2:
            if "INVOICE_COUNT" in monthly_df.columns:
                inv_df = monthly_df[["MONTH_STR", "INVOICE_COUNT"]].rename(columns={"INVOICE_COUNT": "VALUE"})
                alt_bar(inv_df, x="MONTH_STR", y="VALUE", title="Invoice Volume", height=200, color="#3b82f6")
        with col3:
            if "VENDOR_COUNT" in monthly_df.columns:
                vend_df = monthly_df[["MONTH_STR", "VENDOR_COUNT"]].rename(columns={"VENDOR_COUNT": "VALUE"})
                alt_bar(vend_df, x="MONTH_STR", y="VALUE", title="Active Vendors", height=200, color="#10b981")

    if vendors_df is not None and not vendors_df.empty:
        vendors_df.columns = [c.upper() for c in vendors_df.columns]
        if "VENDOR_NAME" in vendors_df.columns and "SPEND" in vendors_df.columns:
            st.subheader("Top Vendors by Spend")
            alt_bar(vendors_df.head(10), x="VENDOR_NAME", y="SPEND", horizontal=True, height=400, color="#22c55e")

    if result.get("analyst_response"):
        st.markdown("### 💡 Key Insights")
        st.markdown(result["analyst_response"])

    with st.expander("View SQL used"):
        sql_dict = result.get("sql", {})
        if sql_dict:
            for name, sql_text in sql_dict.items():
                st.code(sql_text, language="sql")
        elif "sql" in result and isinstance(result["sql"], str):
            st.code(result["sql"], language="sql")

# ------------------------------------------------------------
# Main Genie render function
# ------------------------------------------------------------
def render_genie():
    st.markdown("""
    <style>
    .genie-card {
        background: linear-gradient(135deg, #ffffff 0%, #f8fafc 100%);
        border-radius: 20px;
        padding: 1.5rem;
        box-shadow: 0 8px 20px rgba(0,0,0,0.08);
        transition: transform 0.3s ease, box-shadow 0.3s ease;
        height: 100%;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
        border: 1px solid rgba(203, 213, 225, 0.3);
    }
    .genie-card:hover {
        transform: translateY(-6px);
        box-shadow: 0 20px 30px -12px rgba(0,0,0,0.15);
    }
    .genie-card h3 {
        font-size: 1.3rem;
        font-weight: 700;
        margin-bottom: 0.5rem;
        background: linear-gradient(135deg, #1e293b, #334155);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .chat-message-user {
        background: linear-gradient(135deg, #3b82f6 0%, #2563eb 100%);
        color: white;
        padding: 12px 18px;
        border-radius: 20px 20px 4px 20px;
        margin: 8px 0;
        max-width: 80%;
        align-self: flex-end;
        box-shadow: 0 2px 6px rgba(0,0,0,0.1);
    }
    .chat-message-assistant {
        background: linear-gradient(135deg, #f1f5f9 0%, #e2e8f0 100%);
        color: #0f172a;
        padding: 12px 18px;
        border-radius: 20px 20px 20px 4px;
        margin: 8px 0;
        max-width: 80%;
        align-self: flex-start;
        box-shadow: 0 2px 6px rgba(0,0,0,0.05);
    }
    .chat-scrollable {
        max-height: 500px;
        overflow-y: auto;
        padding-right: 8px;
        margin-bottom: 1rem;
    }
    </style>
    """, unsafe_allow_html=True)

    # Session state initialisation
    if "genie_session_id" not in st.session_state:
        st.session_state.genie_session_id = str(uuid.uuid4())
        st.session_state.genie_messages = []
        st.session_state.genie_turn_index = 0
    if "genie_response" not in st.session_state:
        st.session_state.genie_response = None
    if "selected_analysis" not in st.session_state:
        st.session_state.selected_analysis = None
    if "last_custom_query" not in st.session_state:
        st.session_state.last_custom_query = ""
    if "genie_prefill" not in st.session_state:
        st.session_state.genie_prefill = ""

    quick_map = {
        "Spending Overview": "spending_overview",
        "Vendor Analysis": "vendor_analysis",
        "Payment Performance": "payment_performance",
        "Invoice Aging": "invoice_aging"
    }

    # --- Process auto-run query if present ---
    auto_query = st.session_state.pop("auto_run_query", None)
    if auto_query:
        st.session_state.genie_messages = []
        st.session_state.genie_turn_index = 0
        st.session_state.selected_analysis = "custom"
        st.session_state.last_custom_query = auto_query
        with st.spinner("Running analysis..."):
            lower_q = auto_query.lower()
            if any(kw in lower_q for kw in ["forecast cash outflow", "cash flow forecast", "7, 14, 30, 60, 90"]):
                result = process_cash_flow_forecast(auto_query)
            elif any(kw in lower_q for kw in ["pay early", "capture discounts", "early payment"]):
                result = process_early_payment(auto_query)
            elif any(kw in lower_q for kw in ["optimal payment timing", "payment timing strategy"]):
                result = process_payment_timing(auto_query)
            elif any(kw in lower_q for kw in ["late payment trend", "late payment risk"]):
                result = process_late_payment_trend(auto_query)
            elif auto_query in quick_map:
                result = run_quick_analysis(quick_map[auto_query])
            else:
                result = process_custom_query(auto_query)
            st.session_state.genie_response = result
            st.session_state.genie_messages.append({"role": "user", "content": auto_query, "timestamp": datetime.now()})
            if result.get("layout") in ("quick", "analyst", "sql", "cash_flow", "early_payment", "payment_timing", "late_payment_trend"):
                st.session_state.genie_messages.append({"role": "assistant", "content": "Analysis complete.", "response": result, "timestamp": datetime.now()})
                save_chat_message(st.session_state.genie_session_id, st.session_state.genie_turn_index, "user", auto_query)
                st.session_state.genie_turn_index += 1
                sql_used_val = result.get("sql", "")
                if isinstance(sql_used_val, dict):
                    sql_used_val = json.dumps(sql_used_val)
                save_chat_message(st.session_state.genie_session_id, st.session_state.genie_turn_index, "assistant", "Analysis complete.", sql_used=sql_used_val)
                st.session_state.genie_turn_index += 1
                save_question(auto_query, "forecast")
                set_cache(auto_query, result)
            else:
                st.session_state.genie_messages.append({"role": "assistant", "content": result.get("message", "Error"), "timestamp": datetime.now()})
        st.rerun()

    # --- UI: Welcome and quick cards ---
    st.markdown("## 🧞 Welcome to ProcureIQ Genie")
    st.markdown("Your AI‑powered procurement assistant. Choose a quick analysis or ask a custom question.")

    cols = st.columns(4)
    quick_options = {
        "spending_overview": ("💰 Spending Overview", "Track total spend, monthly trends and major changes", "#3b82f6"),
        "vendor_analysis": ("🏭 Vendor Analysis", "Understand vendor-wise spend, concentration, and dependency", "#8b5cf6"),
        "payment_performance": ("⏱️ Payment Performance", "Identify delays, late payments, and cycle time issues", "#10b981"),
        "invoice_aging": ("📅 Invoice Aging", "See overdue invoices, risk buckets, and problem areas", "#f59e0b")
    }
    for idx, (key, (title, desc, color)) in enumerate(quick_options.items()):
        with cols[idx]:
            with st.container():
                st.markdown(f"""
                <div class="genie-card" style="border-top: 4px solid {color};">
                    <div>
                        <h3>{title}</h3>
                        <p>{desc}</p>
                    </div>
                </div>
                """, unsafe_allow_html=True)
                if st.button("Ask Genie", key=f"card_{key}", use_container_width=True):
                    st.session_state.auto_run_query = title.split(" ", 1)[1]
                    st.rerun()

    st.markdown("---")

    left_col, right_col = st.columns([0.35, 0.65], gap="large")

    with left_col:
        with st.expander("📌 Saved insights", expanded=False):
            insights = get_saved_insights_cached(page="genie")
            if insights:
                for ins in insights:
                    if st.button(ins["title"], key=f"insight_{ins['id']}", use_container_width=True):
                        st.session_state.auto_run_query = ins["question"]
                        st.rerun()
            else:
                st.caption("Save any Genie answer to see it here.")

        with st.expander("🔥 Frequently asked by you", expanded=False):
            suggestions = [
                "forecast cash outflow for the next 7, 14, 30, 60, and 90 days",
                "show me total spend ytd, monthly trends, and top 5 vendors",
                "which invoices should we pay early to capture discounts"
            ]
            st.markdown("Click a chip to fill the input:")
            for chip in suggestions:
                if st.button(chip, key=f"chip_{chip[:20]}", use_container_width=True):
                    st.session_state.genie_prefill = chip
                    st.rerun()
            faqs = get_frequent_questions_by_user_cached(5)
            if faqs:
                st.markdown("---")
                st.markdown("**Your top questions**")
                for faq in faqs:
                    if st.button(f"{faq['query'][:50]} ({faq['count']})", key=f"faq_user_{faq['query']}", use_container_width=True):
                        st.session_state.genie_prefill = faq["query"]
                        st.rerun()

        with st.expander("🌍 Most frequent (all)", expanded=False):
            all_faqs = get_frequent_questions_all_cached(5)
            if all_faqs:
                for faq in all_faqs:
                    st.button(f"{faq['query'][:50]} ({faq['count']})", key=f"faq_all_{faq['query']}", use_container_width=True, disabled=True)

    with right_col:
        st.markdown("<div style='text-align: center; margin-bottom: 1rem;'><h3>💬 Start a Conversation</h3><p>Ask a natural language question about your procurement data.</p></div>", unsafe_allow_html=True)
        st.markdown('<div class="chat-scrollable">', unsafe_allow_html=True)
        for msg in st.session_state.genie_messages:
            if msg["role"] == "user":
                st.markdown(f'<div class="chat-message-user"><strong>You</strong><br/>{html.escape(msg["content"])}</div>', unsafe_allow_html=True)
            else:
                st.markdown(f'<div class="chat-message-assistant"><strong>🧞 Genie</strong><br/>{html.escape(msg["content"])}</div>', unsafe_allow_html=True)
                if "response" in msg and msg["response"]:
                    resp = msg["response"]
                    layout = resp.get("layout")
                    if layout == "cash_flow":
                        render_cash_flow_response(resp)
                    elif layout == "early_payment":
                        render_early_payment_response(resp)
                    elif layout == "payment_timing":
                        render_payment_timing_response(resp)
                    elif layout == "late_payment_trend":
                        render_late_payment_trend_response(resp)
                    elif layout == "quick":
                        render_quick_analysis_response(resp)
                    elif layout == "analyst":
                        if resp.get("analyst_response"):
                            st.markdown(resp["analyst_response"])
                        else:
                            st.info("No descriptive analysis available.")
                        df = pd.DataFrame(resp["df"])
                        st.dataframe(df, use_container_width=True)
                        chart = auto_chart(df)
                        if chart:
                            st.altair_chart(chart, use_container_width=True)
                        with st.expander("View SQL used"):
                            st.code(resp["sql"], language="sql")
                    elif layout == "sql":
                        df = pd.DataFrame(resp["df"])
                        st.dataframe(df, use_container_width=True)
                        chart = auto_chart(df)
                        if chart:
                            st.altair_chart(chart, use_container_width=True)
                        with st.expander("View SQL"):
                            st.code(resp["sql"], language="sql")
                    elif layout == "error":
                        st.error(resp.get("message", "Unknown error"))
        st.markdown('</div>', unsafe_allow_html=True)

        with st.form(key="genie_form", clear_on_submit=True):
            col_input, col_btn = st.columns([0.85, 0.15])
            with col_input:
                user_question = st.text_input(
                    "Ask a question",
                    value=st.session_state.pop("genie_prefill", ""),
                    placeholder="e.g., Show me total spend YTD",
                    label_visibility="collapsed"
                )
            with col_btn:
                submitted = st.form_submit_button("→", type="primary")
            if submitted and user_question:
                with st.spinner("Generating SQL and insights..."):
                    cached = get_cache(user_question)
                    if cached:
                        st.session_state.genie_response = cached
                        st.session_state.genie_messages.append({"role": "user", "content": user_question, "timestamp": datetime.now()})
                        st.session_state.genie_messages.append({"role": "assistant", "content": "Answer from cache.", "response": cached, "timestamp": datetime.now()})
                        save_chat_message(st.session_state.genie_session_id, st.session_state.genie_turn_index, "user", user_question)
                        st.session_state.genie_turn_index += 1
                        sql_used_val = cached.get("sql", "") if isinstance(cached, dict) else ""
                        if isinstance(sql_used_val, dict):
                            sql_used_val = json.dumps(sql_used_val)
                        save_chat_message(st.session_state.genie_session_id, st.session_state.genie_turn_index, "assistant", "Answer from cache.", source="cache", sql_used=sql_used_val)
                        st.session_state.genie_turn_index += 1
                        save_question(user_question, "custom")
                    else:
                        lower_q = user_question.lower()
                        if any(kw in lower_q for kw in ["forecast cash outflow", "cash flow forecast", "7, 14, 30, 60, 90"]):
                            result = process_cash_flow_forecast(user_question)
                        elif any(kw in lower_q for kw in ["pay early", "capture discounts", "early payment"]):
                            result = process_early_payment(user_question)
                        elif any(kw in lower_q for kw in ["optimal payment timing", "payment timing strategy"]):
                            result = process_payment_timing(user_question)
                        elif any(kw in lower_q for kw in ["late payment trend", "late payment risk"]):
                            result = process_late_payment_trend(user_question)
                        elif user_question in quick_map:
                            result = run_quick_analysis(quick_map[user_question])
                        else:
                            result = process_custom_query(user_question)
                        if result.get("layout") not in ("error",):
                            set_cache(user_question, result)
                            st.session_state.genie_response = result
                            st.session_state.genie_messages.append({"role": "user", "content": user_question, "timestamp": datetime.now()})
                            st.session_state.genie_messages.append({"role": "assistant", "content": "Analysis complete.", "response": result, "timestamp": datetime.now()})
                            save_chat_message(st.session_state.genie_session_id, st.session_state.genie_turn_index, "user", user_question)
                            st.session_state.genie_turn_index += 1
                            sql_used_val = result.get("sql", "")
                            if isinstance(sql_used_val, dict):
                                sql_used_val = json.dumps(sql_used_val)
                            save_chat_message(st.session_state.genie_session_id, st.session_state.genie_turn_index, "assistant", "Analysis complete.", sql_used=sql_used_val)
                            st.session_state.genie_turn_index += 1
                            save_question(user_question, "forecast")
                        else:
                            st.error(result.get("message", "Query failed"))
                st.rerun()
