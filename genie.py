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
# Cash Flow Forecast handler (unchanged)
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
    used_sql = ep_sql
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

    if not ep_df.empty:
        ep_df.columns = [c.lower() for c in ep_df.columns]
    else:
        ep_df = pd.DataFrame()

    if ep_df.empty:
        prompt = f"""
You are a senior procurement analyst. The user asked: "{question}".
However, the query returned no data. Possible reasons: no open invoices with due dates in the next 30 days, or the early_payment_candidates view is empty.
Write a response with two sections:

1. **Descriptive** – Explain that no invoices were found that meet the early payment criteria (due within 30 days and still open). Suggest that the user may have already captured available discounts or that all invoices are either paid or outside the window.
2. **Prescriptive** – Provide general best practices for identifying early payment opportunities: regularly review open invoices, focus on those with due dates within 7-14 days, calculate potential savings using a 2% discount rate, and prioritize high-value invoices. List 3‑5 bullet points with actionable steps.

Respond in plain text, using markdown for headings and bullet points. Do not include any extra commentary.
"""
        analyst_text = ask_bedrock(prompt, system_prompt="You are a helpful procurement analyst.")
        if not analyst_text:
            analyst_text = "No early payment candidates were found. Please check that there are open invoices with due dates within the next 30 days."
    else:
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
        "df": ep_df.to_dict(orient="records") if not ep_df.empty else [],
        "sql": used_sql,
        "analyst_response": analyst_text,
        "question": question,
        "empty": ep_df.empty
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
# GR/IR Clearing Playbook handlers
# ------------------------------------------------------------
def process_grir_hotspots(question: str) -> dict:
    sql = f"""
        SELECT
            year,
            month,
            invoice_count,
            total_grir_blnc AS total_grir_balance
        FROM {DATABASE}.gr_ir_outstanding_balance_vw
        ORDER BY year DESC, month DESC
    """
    df = run_query(sql)
    if df.empty:
        return {"layout": "error", "message": "No GR/IR outstanding balance data found."}
    df.columns = [c.lower() for c in df.columns]
    df['balance_rank'] = df['total_grir_balance'].rank(ascending=False, method='dense').astype(int)
    data_preview = df.head(12).to_string(index=False)
    prompt = f"""
You are a senior procurement analyst. Based on the following GR/IR outstanding balance by month, write a response with two sections:

1. **Descriptive** – Highlight the months with the highest GR/IR balances (top 3). Mention the total balance and invoice count for those months.
2. **Prescriptive** – Recommend which months to prioritize for clearing, and suggest concrete steps (e.g., review POs with missing receipts, contact vendors for missing invoices). List 3‑5 bullet points with specific findings, actions, and why it matters.

Data (most recent months):
{data_preview}

Respond in plain text, using markdown for headings and bullet points. Do not include any extra commentary.
"""
    analyst_text = ask_bedrock(prompt, system_prompt="You are a helpful procurement analyst focusing on GR/IR reconciliation.")
    if not analyst_text:
        analyst_text = "Unable to generate insights at this time."
    return {
        "layout": "grir_hotspots",
        "df": df.to_dict(orient="records"),
        "sql": sql,
        "analyst_response": analyst_text,
        "question": question
    }

def process_grir_root_causes(question: str) -> dict:
    aging_sql = f"""
        SELECT
            year,
            month,
            pct_grir_over_60,
            cnt_grir_over_60
        FROM {DATABASE}.gr_ir_aging_vw
        ORDER BY year DESC, month DESC
        LIMIT 6
    """
    aging_df = run_query(aging_sql)
    balance_sql = f"""
        SELECT
            year,
            month,
            total_grir_blnc
        FROM {DATABASE}.gr_ir_outstanding_balance_vw
        ORDER BY year DESC, month DESC
        LIMIT 6
    """
    balance_df = run_query(balance_sql)
    if aging_df.empty and balance_df.empty:
        return {"layout": "error", "message": "No GR/IR aging or balance data found."}
    context = "GR/IR aging (last 6 months):\n" + aging_df.to_string(index=False) + "\n\nOutstanding balances:\n" + balance_df.to_string(index=False)
    prompt = f"""
You are a senior procurement analyst. Based on the following GR/IR data (aging and outstanding balances), write a response with two sections:

1. **Descriptive** – Explain the likely root‑cause buckets for GR/IR discrepancies: missing goods receipt, invoice not posted, price/quantity mismatch, etc. Use the data to infer which buckets are most likely.
2. **Prescriptive** – For each root‑cause bucket, suggest 2‑3 concrete remediation actions. Focus on actionable steps like matching POs to receipts, following up with vendors, etc. List as bullet points.

Data:
{context}

Respond in plain text, using markdown for headings and bullet points. Do not include any extra commentary.
"""
    analyst_text = ask_bedrock(prompt, system_prompt="You are a helpful procurement analyst specializing in GR/IR reconciliation.")
    if not analyst_text:
        analyst_text = "Unable to generate insights at this time."
    return {
        "layout": "grir_root_causes",
        "df": aging_df.to_dict(orient="records") if not aging_df.empty else [],
        "extra_df": balance_df.to_dict(orient="records") if not balance_df.empty else [],
        "sql": f"{aging_sql}\n\n{balance_sql}",
        "analyst_response": analyst_text,
        "question": question
    }

def process_grir_working_capital(question: str) -> dict:
    sql = f"""
        SELECT
            year,
            month,
            total_grir_blnc,
            CASE WHEN (year * 100 + month) <= (EXTRACT(YEAR FROM CURRENT_DATE) * 100 + EXTRACT(MONTH FROM CURRENT_DATE) - 60)
                 THEN total_grir_blnc ELSE 0 END AS older_than_60_days,
            CASE WHEN (year * 100 + month) <= (EXTRACT(YEAR FROM CURRENT_DATE) * 100 + EXTRACT(MONTH FROM CURRENT_DATE) - 90)
                 THEN total_grir_blnc ELSE 0 END AS older_than_90_days
        FROM {DATABASE}.gr_ir_outstanding_balance_vw
        ORDER BY year DESC, month DESC
    """
    df = run_query(sql)
    if df.empty:
        return {"layout": "error", "message": "No GR/IR balance data found."}
    df.columns = [c.lower() for c in df.columns]
    total_old_60 = df['older_than_60_days'].sum()
    total_old_90 = df['older_than_90_days'].sum()
    data_preview = df.head(12).to_string(index=False)
    prompt = f"""
You are a senior procurement analyst. Based on the following GR/IR outstanding balance by month, with estimated amounts older than 60 and 90 days, write a response with two sections:

1. **Descriptive** – State the total working capital that could be released by clearing GR/IR items older than 60 days (${total_old_60:,.2f}) and older than 90 days (${total_old_90:,.2f}). Mention which months contribute most.
2. **Prescriptive** – Recommend a phased approach to clear old items, prioritising those >90 days first. Suggest how to use this released working capital (e.g., pay down debt, early payment discounts). List 3‑5 bullet points with specific findings, actions, and why it matters.

Data:
{data_preview}

Respond in plain text, using markdown for headings and bullet points. Do not include any extra commentary.
"""
    analyst_text = ask_bedrock(prompt, system_prompt="You are a helpful procurement analyst focusing on working capital.")
    if not analyst_text:
        analyst_text = "Unable to generate insights at this time."
    return {
        "layout": "grir_working_capital",
        "df": df.to_dict(orient="records"),
        "metrics": {"older_60": total_old_60, "older_90": total_old_90},
        "sql": sql,
        "analyst_response": analyst_text,
        "question": question
    }

def process_grir_vendor_followup(question: str) -> dict:
    sql = f"""
        SELECT
            v.vendor_name,
            COUNT(*) AS grir_count,
            SUM(f.invoice_amount_local) AS total_amount,
            AVG(DATE_DIFF('day', f.posting_date, CURRENT_DATE)) AS avg_age_days
        FROM {DATABASE}.fact_all_sources_vw f
        LEFT JOIN {DATABASE}.dim_vendor_vw v ON f.vendor_id = v.vendor_id
        WHERE f.invoice_status = 'OPEN' AND f.purchase_order_reference IS NOT NULL
        GROUP BY v.vendor_name
        ORDER BY total_amount DESC
        LIMIT 10
    """
    df = run_query(sql)
    if df.empty:
        return {"layout": "error", "message": "No GR/IR vendor data found."}
    df.columns = [c.lower() for c in df.columns]
    data_preview = df.to_string(index=False)
    prompt = f"""
You are a senior procurement analyst. Based on the following top vendors with outstanding GR/IR items (count, total amount, average age), draft vendor-facing follow-up templates. Write a response with two sections:

1. **Descriptive** – Summarise the top vendors and the scale of GR/IR items.
2. **Prescriptive** – Provide 3‑5 template messages (subject line and bullet points) that can be used to follow up with these vendors. Each template should be realistic and concise, tailored to the likely root cause (e.g., missing invoice, goods receipt not posted). Also include a recommended escalation timeline.

Data:
{data_preview}

Respond in plain text, using markdown for headings and bullet points. Do not include any extra commentary.
"""
    analyst_text = ask_bedrock(prompt, system_prompt="You are a helpful procurement analyst skilled in vendor communication.")
    if not analyst_text:
        analyst_text = "Unable to generate insights at this time."
    return {
        "layout": "grir_vendor_followup",
        "df": df.to_dict(orient="records"),
        "sql": sql,
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
    empty = result.get("empty", False)
    if empty or df.empty:
        st.info("No early payment candidates were found in the current data. The SQL query returned zero rows.")
    else:
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

def render_grir_hotspots(result: dict):
    df = pd.DataFrame(result["df"])
    if df.empty:
        st.error("No GR/IR data.")
        return
    st.subheader("GR/IR Outstanding Balance by Month")
    chart_df = df.head(12).copy()
    chart_df['year_month'] = chart_df['year'].astype(str) + '-' + chart_df['month'].astype(str).str.zfill(2)
    alt_bar(chart_df, x="year_month", y="total_grir_balance", title="Top months with highest GR/IR", horizontal=False, height=300, color="#ef4444")
    st.dataframe(df, use_container_width=True, hide_index=True)
    if result.get("analyst_response"):
        st.markdown("### 💡 Key Insights")
        st.markdown(result["analyst_response"])
    with st.expander("View SQL used"):
        st.code(result["sql"], language="sql")

def render_grir_root_causes(result: dict):
    df = pd.DataFrame(result.get("df", []))
    extra_df = pd.DataFrame(result.get("extra_df", []))
    if not df.empty:
        st.subheader("GR/IR Aging (Last 6 Months)")
        st.dataframe(df, use_container_width=True)
    if not extra_df.empty:
        st.subheader("Outstanding Balances (Last 6 Months)")
        st.dataframe(extra_df, use_container_width=True)
    if result.get("analyst_response"):
        st.markdown("### 💡 Key Insights")
        st.markdown(result["analyst_response"])
    with st.expander("View SQL used"):
        st.code(result["sql"], language="sql")

def render_grir_working_capital(result: dict):
    metrics = result.get("metrics", {})
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Working Capital Release (>60 days)", abbr_currency(metrics.get("older_60", 0)))
    with col2:
        st.metric("Working Capital Release (>90 days)", abbr_currency(metrics.get("older_90", 0)))
    df = pd.DataFrame(result["df"])
    if not df.empty:
        st.subheader("GR/IR Balance by Month (with aging estimates)")
        st.dataframe(df, use_container_width=True, hide_index=True)
    if result.get("analyst_response"):
        st.markdown("### 💡 Key Insights")
        st.markdown(result["analyst_response"])
    with st.expander("View SQL used"):
        st.code(result["sql"], language="sql")

def render_grir_vendor_followup(result: dict):
    df = pd.DataFrame(result["df"])
    if not df.empty:
        st.subheader("Top Vendors with Outstanding GR/IR Items")
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
        label_map = {
            "total_ytd": "Total Spend (YTD)",
            "mom_pct": "MoM Change",
            "top5_pct": "Top 5 Vendors",
            "qoq_pct": "QoQ Change",
            "summary": "Summary"
        }
        cols = st.columns(len(metrics))
        colors = ["#fef3c7", "#dbeafe", "#dcfce7", "#fce7f3", "#e0e7ff", "#fef9c3"]
        for i, (key, value) in enumerate(metrics.items()):
            with cols[i]:
                label = label_map.get(key, key.replace("_", " ").title())
                if isinstance(value, (int, float)):
                    if "pct" in key or "rate" in key:
                        display = f"{value:+.1f}%" if key != "top5_pct" else f"{value:.0f}%"
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
        chart_col1, chart_col2, chart_col3 = st.columns(3)
        with chart_col1:
            if "MONTHLY_SPEND" in monthly_df.columns:
                spend_df = monthly_df[["MONTH_STR", "MONTHLY_SPEND"]].rename(columns={"MONTHLY_SPEND": "VALUE"})
                alt_line_monthly(spend_df, month_col="MONTH_STR", value_col="VALUE", height=200, title="Monthly Spend")
        with chart_col2:
            if "INVOICE_COUNT" in monthly_df.columns:
                inv_df = monthly_df[["MONTH_STR", "INVOICE_COUNT"]].rename(columns={"INVOICE_COUNT": "VALUE"})
                alt_bar(inv_df, x="MONTH_STR", y="VALUE", title="Invoice Volume", height=200, color="#3b82f6")
        with chart_col3:
            if "VENDOR_COUNT" in monthly_df.columns:
                vend_df = monthly_df[["MONTH_STR", "VENDOR_COUNT"]].rename(columns={"VENDOR_COUNT": "VALUE"})
                alt_bar(vend_df, x="MONTH_STR", y="VALUE", title="Active Vendors", height=200, color="#10b981")

    if vendors_df is not None and not vendors_df.empty:
        vendors_df.columns = [c.upper() for c in vendors_df.columns]
        if "VENDOR_NAME" in vendors_df.columns and "SPEND" in vendors_df.columns:
            st.subheader("Top 10 Vendors by Spend (YTD)")
            alt_bar(vendors_df.head(10), x="VENDOR_NAME", y="SPEND", horizontal=True, height=400, color="#22c55e")

    if analysis_type == "vendor_analysis" and vendors_df is not None and not vendors_df.empty:
        if "INVOICE_COUNT" in vendors_df.columns:
            st.subheader("Invoice Frequency by Vendor")
            freq_df = vendors_df[["VENDOR_NAME", "INVOICE_COUNT"]].head(10)
            alt_bar(freq_df, x="VENDOR_NAME", y="INVOICE_COUNT", horizontal=True, height=300, color="#f59e0b")

    if analysis_type == "payment_performance" and monthly_df is not None and not monthly_df.empty:
        monthly_df.columns = [c.upper() for c in monthly_df.columns]
        if "MONTH" in monthly_df.columns:
            monthly_df = monthly_df.rename(columns={"MONTH": "MONTH_STR"})
        elif "MONTH_STR" not in monthly_df.columns:
            monthly_df = monthly_df.rename(columns={monthly_df.columns[0]: "MONTH_STR"})
        st.subheader("Payment Performance Trend")
        col1, col2 = st.columns(2)
        with col1:
            if "AVG_DAYS" in monthly_df.columns:
                days_df = monthly_df[["MONTH_STR", "AVG_DAYS"]].rename(columns={"AVG_DAYS": "VALUE"})
                alt_line_monthly(days_df, month_col="MONTH_STR", value_col="VALUE", height=250, title="Avg Days to Pay")
        with col2:
            if "LATE_PAYMENTS" in monthly_df.columns and "TOTAL_PAYMENTS" in monthly_df.columns:
                monthly_df["LATE_PCT"] = (monthly_df["LATE_PAYMENTS"] / monthly_df["TOTAL_PAYMENTS"]) * 100
                late_df = monthly_df[["MONTH_STR", "LATE_PCT"]].rename(columns={"LATE_PCT": "VALUE"})
                alt_line_monthly(late_df, month_col="MONTH_STR", value_col="VALUE", height=250, title="Late Payments (%)")

    if analysis_type == "invoice_aging" and vendors_df is not None and not vendors_df.empty:
        vendors_df.columns = [c.upper() for c in vendors_df.columns]
        if "AGING_BUCKET" in vendors_df.columns and "SPEND" in vendors_df.columns:
            st.subheader("Invoice Aging Buckets")
            alt_bar(vendors_df, x="AGING_BUCKET", y="SPEND", title="Outstanding Spend by Aging", horizontal=False, height=300, color="#ef4444")
        elif "CNT" in vendors_df.columns:
            st.subheader("Aging Distribution")
            alt_donut_status(vendors_df, label_col="AGING_BUCKET", value_col="CNT", title="Invoice Count by Age", height=300)

    if "analyst_response" not in result or not result["analyst_response"]:
        monthly_preview = ""
        if monthly_df is not None:
            monthly_preview = monthly_df.head(6).to_string(index=False)
        vendors_preview = ""
        if vendors_df is not None:
            vendors_preview = vendors_df.head(10).to_string(index=False)
        metrics_str = json.dumps({k: (float(v) if isinstance(v, (int, float)) else str(v)) for k, v in metrics.items()}, indent=2)
        analysis_prompts = {
            "spending_overview": "Focus on total spend, month‑over‑month changes, vendor concentration, and any anomalies. Provide actions to optimise spend.",
            "vendor_analysis": "Focus on vendor concentration (top vendors' share), over‑reliance risks, invoice frequency, and vendor performance. Suggest diversification and contingency plans.",
            "payment_performance": "Focus on average days to pay, late payment trends, and their impact on supplier relationships and cash flow. Suggest process improvements.",
            "invoice_aging": "Focus on overdue amounts, aging buckets, and potential cash flow risks. Suggest collection strategies and early payment discounts."
        }
        prompt = f"""
You are a senior procurement analyst. Based on the following data from a {analysis_type.replace('_', ' ')} analysis, write a response with two sections:

1. **Descriptive** – What the data shows. Cite exact numbers, identify trends, and highlight anomalies. Keep it concise (3‑5 sentences).
2. **Prescriptive** – Specific recommended actions and risks based on the data. List 3‑5 bullet points. Each bullet must include a specific finding, a concrete action, and a brief 'Why it matters'.

Data metrics:
{metrics_str}

Monthly trend (first 6 rows):
{monthly_preview}

Top vendors / aging data (first 10 rows):
{vendors_preview}

Analysis focus: {analysis_prompts.get(analysis_type, "Provide actionable procurement insights.")}

Respond in plain text, using markdown for headings and bullet points. Do not include any extra commentary.
"""
        analyst_text = ask_bedrock(prompt, system_prompt="You are a helpful procurement analyst.")
        result["analyst_response"] = analyst_text

    if result.get("analyst_response"):
        st.markdown("### 💡 Key Insights")
        st.markdown(result["analyst_response"])

    with st.expander("Query outputs"):
        sql_dict = result.get("sql", {})
        if sql_dict:
            for name, sql_text in sql_dict.items():
                st.code(sql_text, language="sql")
        elif "sql" in result and isinstance(result["sql"], str):
            st.code(result["sql"], language="sql")

# ------------------------------------------------------------
# Main Genie render function – fully working chat input
# ------------------------------------------------------------
def render_genie():
    # CSS for rectangle cards + chat styling
    st.markdown("""
    <style>
    /* Rectangle cards */
    .genie-card {
        background: white;
        border-radius: 20px;
        padding: 1rem;
        box-shadow: 0 4px 12px rgba(0,0,0,0.08);
        transition: transform 0.2s ease, box-shadow 0.2s ease;
        height: 100%;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
        border: 1px solid #eef2f6;
        overflow: hidden;
    }
    .genie-card:hover {
        transform: translateY(-4px);
        box-shadow: 0 12px 24px rgba(0,0,0,0.12);
    }
    .genie-card h3 {
        font-size: 1.2rem;
        font-weight: 600;
        margin: 0.5rem 0 0.25rem;
        color: #1e293b;
        line-height: 1.3;
    }
    .genie-card p {
        color: #475569;
        font-size: 0.8rem;
        line-height: 1.4;
        margin-bottom: 1rem;
        overflow-wrap: break-word;
        display: -webkit-box;
        -webkit-line-clamp: 3;
        -webkit-box-orient: vertical;
        overflow: hidden;
        text-overflow: ellipsis;
    }
    .card-icon {
        font-size: 2rem;
        text-align: center;
        margin-bottom: 0.5rem;
    }
    .genie-card .stButton button {
        margin-top: auto;
        width: 100%;
        background-color: #3b82f6;
        color: white;
        border: none;
        border-radius: 30px;
        padding: 0.4rem;
        font-weight: 500;
    }
    .genie-card .stButton button:hover {
        background-color: #2563eb;
    }
    /* Chat messages */
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
        background: #f1f5f9;
        color: #0f172a;
        padding: 12px 18px;
        border-radius: 20px 20px 20px 4px;
        margin: 8px 0;
        max-width: 80%;
        align-self: flex-start;
        box-shadow: 0 2px 6px rgba(0,0,0,0.05);
    }
    .chat-scrollable {
        max-height: 450px;
        overflow-y: auto;
        padding-right: 8px;
        margin-bottom: 1rem;
    }
    .centered-container {
        text-align: center;
        margin-bottom: 1rem;
    }
    /* Input row */
    .input-row {
        display: flex;
        gap: 8px;
        margin-top: 8px;
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

    # Process auto-run query from card buttons
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
            elif "gr/ir outstanding balance by month" in lower_q or "hotspots" in lower_q:
                result = process_grir_hotspots(auto_query)
            elif "root-cause" in lower_q or "root causes" in lower_q or "explain likely gr/ir" in lower_q:
                result = process_grir_root_causes(auto_query)
            elif "working-capital" in lower_q or "working capital" in lower_q or "older than 60" in lower_q:
                result = process_grir_working_capital(auto_query)
            elif "vendor follow-up" in lower_q or "draft vendor" in lower_q or "follow-up messages" in lower_q:
                result = process_grir_vendor_followup(auto_query)
            elif auto_query in quick_map:
                result = run_quick_analysis(quick_map[auto_query])
            else:
                result = process_custom_query(auto_query)
            st.session_state.genie_response = result
            st.session_state.genie_messages.append({"role": "user", "content": auto_query, "timestamp": datetime.now()})
            if result.get("layout") not in ("error",):
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

    # ----- HEADER SECTION -----
    st.markdown("## 🧞 Welcome to ProcureIQ Genie")
    st.markdown("Let Genie run one of these quick analyses for you")

    # ----- QUICK ANALYSIS CARDS (4 columns) -----
    col1, col2, col3, col4 = st.columns(4)
    cards = [
        ("💰", "Spending Overview", "Track total spend, monthly trends and major changes"),
        ("🏭", "Vendor Analysis", "Understand vendor-wise spend, concentration, and dependency"),
        ("⏱️", "Payment Performance", "Identify delays, late payments, and cycle time issues"),
        ("📅", "Invoice Aging", "See overdue invoices, risk buckets, and problem areas")
    ]
    for col, (icon, title, desc) in zip([col1, col2, col3, col4], cards):
        with col:
            st.markdown(f"""
            <div class="genie-card">
                <div>
                    <div class="card-icon">{icon}</div>
                    <h3>{title}</h3>
                    <p>{desc}</p>
                </div>
            </div>
            """, unsafe_allow_html=True)
            if st.button("Ask Genie", key=f"quick_card_{title.replace(' ', '_')}", use_container_width=True):
                st.session_state.auto_run_query = title
                st.rerun()

    st.markdown("---")

    # ----- MAIN WORKSPACE (2 columns) -----
    left_col, right_col = st.columns([0.35, 0.65], gap="large")

    with left_col:
        # Saved insights
        with st.expander("📌 Saved insights", expanded=False):
            insights = get_saved_insights_cached(page="genie")
            if insights:
                for ins in insights:
                    if st.button(ins["title"], key=f"insight_{ins['id']}", use_container_width=True):
                        st.session_state.auto_run_query = ins["question"]
                        st.rerun()
            else:
                st.caption("Save any Genie answer to see it here.")

        # Frequently asked by you
        with st.expander("🔥 Frequently asked by you", expanded=False):
            suggestions = [
                "forecast cash outflow for the next 7, 14, 30, 60, and 90 days",
                "show me total spend ytd, monthly trends, and top 5 vendors",
                "which invoices should we pay early to capture discounts"
            ]
            st.markdown("**Click a chip to fill the input:**")
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
            else:
                st.caption("Your frequent questions will appear here.")

        # Most frequent (all)
        with st.expander("🌍 Most frequent (all)", expanded=False):
            all_faqs = get_frequent_questions_all_cached(5)
            if all_faqs:
                for faq in all_faqs:
                    st.button(f"{faq['query'][:50]} ({faq['count']})", key=f"faq_all_{faq['query']}", use_container_width=True, disabled=True)
            else:
                st.caption("No questions yet.")

    with right_col:
        # Centered welcome
        st.markdown('<div class="centered-container">', unsafe_allow_html=True)
        st.markdown("### 💬 Start a Conversation")
        st.markdown("Ask questions about your Procurement to Pay data, or select a pre-built analysis from the library.")
        st.markdown('</div>', unsafe_allow_html=True)

        # Chat history area (scrollable)
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
                    elif layout == "grir_hotspots":
                        render_grir_hotspots(resp)
                    elif layout == "grir_root_causes":
                        render_grir_root_causes(resp)
                    elif layout == "grir_working_capital":
                        render_grir_working_capital(resp)
                    elif layout == "grir_vendor_followup":
                        render_grir_vendor_followup(resp)
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

        # ----- WORKING INPUT FORM (below chat history) -----
        with st.form(key="genie_form", clear_on_submit=True):
            # Use columns to place text input and submit button side by side
            col_input, col_btn = st.columns([0.85, 0.15])
            with col_input:
                prefill_value = st.session_state.pop("genie_prefill", "")
                user_question = st.text_input(
                    "Ask a question",
                    value=prefill_value,
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
                        elif "gr/ir outstanding balance by month" in lower_q or "hotspots" in lower_q:
                            result = process_grir_hotspots(user_question)
                        elif "root-cause" in lower_q or "root causes" in lower_q or "explain likely gr/ir" in lower_q:
                            result = process_grir_root_causes(user_question)
                        elif "working-capital" in lower_q or "working capital" in lower_q or "older than 60" in lower_q:
                            result = process_grir_working_capital(user_question)
                        elif "vendor follow-up" in lower_q or "draft vendor" in lower_q or "follow-up messages" in lower_q:
                            result = process_grir_vendor_followup(user_question)
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
