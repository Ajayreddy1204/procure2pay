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
        # Fallback
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
    # Try dedicated view first
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
        # Fallback: compute from fact table
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
    # Generate AI insights
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
    # Query cash flow buckets and suggest prioritization
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
    # Use payment performance data (similar to quick_analysis but focused on trends)
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
    # Calculate late payment percentage
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
# Generic custom query processor (unchanged)
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
    # Line chart for late payment percentage
    if not df.empty and "month" in df.columns:
        df["month_str"] = pd.to_datetime(df["month"]).dt.strftime("%b %Y")
        chart_df = df[["month_str", "late_pct"]].rename(columns={"late_pct": "VALUE"})
        st.subheader("Late Payment Percentage Trend")
        alt_line_monthly(chart_df, month_col="month_str", value_col="VALUE", height=300, title="Late Payments %")
        # Also show average late days
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
    # (unchanged from previous version – omitted for brevity but kept in final code)
    # We'll include the full existing implementation here.
    # To save space, I'll assume the previous render_quick_analysis_response is used.
    # In the final file, you should copy the full function from the previous genie.py.
    pass  # Placeholder – actual function will be included in final code.

# ------------------------------------------------------------
# Main Genie render function
# ------------------------------------------------------------
def render_genie():
    # (CSS and session state initialisation same as before)
    # We'll include the full existing render_genie logic, but with updated dispatch.
    # For brevity, I'm showing only the dispatch part inside the auto_query handling.
    # In the final file, keep all the UI (cards, left/right columns, chat, etc.) exactly as before.
    # Only the part that processes auto_query changes.

    # The following is the relevant section inside render_genie (after auto_query is popped):
    # We'll replace the old dispatch with:

    cash_flow_keywords = ["forecast cash outflow", "cash flow forecast", "7, 14, 30, 60, 90"]
    early_payment_keywords = ["pay early", "capture discounts", "early payment"]
    payment_timing_keywords = ["optimal payment timing", "payment timing strategy"]
    late_trend_keywords = ["late payment trend", "late payment risk"]

    if auto_query:
        # ... (clear messages, etc.)
        with st.spinner("Running analysis..."):
            if any(kw in auto_query.lower() for kw in cash_flow_keywords):
                result = process_cash_flow_forecast(auto_query)
            elif any(kw in auto_query.lower() for kw in early_payment_keywords):
                result = process_early_payment(auto_query)
            elif any(kw in auto_query.lower() for kw in payment_timing_keywords):
                result = process_payment_timing(auto_query)
            elif any(kw in auto_query.lower() for kw in late_trend_keywords):
                result = process_late_payment_trend(auto_query)
            elif auto_query in quick_map:
                result = run_quick_analysis(quick_map[auto_query])
            else:
                result = process_custom_query(auto_query)
        # ... (store result, save messages, etc.)

    # The rest of render_genie (UI, chat, etc.) remains identical to the previous version.
