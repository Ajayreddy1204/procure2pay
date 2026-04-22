# genie.py
import streamlit as st
import pandas as pd
import json
import html
import uuid
import re
import altair as alt
from datetime import datetime
from athena_client import run_query
from bedrock_client import ask_bedrock
from utils import abbr_currency, auto_chart, alt_line_monthly, alt_bar, alt_donut_status, ensure_limit, is_safe_sql, safe_number
from persistence import get_saved_insights_cached, get_frequent_questions_by_user_cached, get_frequent_questions_all_cached, save_chat_message, save_question, set_cache, get_cache
from quick_analysis import run_quick_analysis
from config import DATABASE


# ------------------------------------------------------------
# Helper to safely convert sql_used to string
# ------------------------------------------------------------
def _safe_sql_string(sql_val):
    if sql_val is None:
        return ""
    if isinstance(sql_val, (dict, list)):
        return json.dumps(sql_val)
    return str(sql_val)


# ------------------------------------------------------------
# SQL generation – template‑based with fallback to LLM
# ------------------------------------------------------------
def get_sql_for_question(question: str) -> str:
    q = question.lower()

    # 1. Total spend YTD
    if ("total spend" in q or "spend ytd" in q or "year-to-date spend" in q) and ("ytd" in q or "year to date" in q):
        return f"""
            SELECT
                SUM(COALESCE(f.invoice_amount_local, 0)) AS total_spend_ytd,
                MIN(f.posting_date) AS earliest_invoice,
                MAX(f.posting_date) AS latest_invoice,
                COUNT(DISTINCT f.invoice_number) AS invoice_count
            FROM {DATABASE}.fact_all_sources_vw f
            WHERE f.invoice_status NOT IN ('Cancelled', 'Rejected')
              AND f.posting_date >= DATE_TRUNC('YEAR', CURRENT_DATE)
        """

    # 2. Top vendors by spend
    if ("top" in q and "vendor" in q and ("spend" in q or "spending" in q)) or ("vendor analysis" in q):
        return f"""
            SELECT
                COALESCE(v.vendor_name, 'Unknown') AS vendor_name,
                SUM(COALESCE(f.invoice_amount_local, 0)) AS total_spend
            FROM {DATABASE}.fact_all_sources_vw f
            LEFT JOIN {DATABASE}.dim_vendor_vw v ON f.vendor_id = v.vendor_id
            WHERE f.invoice_status NOT IN ('Cancelled', 'Rejected')
            GROUP BY v.vendor_name
            ORDER BY total_spend DESC
            LIMIT 10
        """

    # 3. Monthly spend trend
    if ("monthly" in q and ("spend" in q or "trend" in q)) or ("spending trend" in q):
        return f"""
            SELECT
                DATE_TRUNC('month', f.posting_date) AS month,
                SUM(COALESCE(f.invoice_amount_local, 0)) AS monthly_spend,
                COUNT(*) AS invoice_count
            FROM {DATABASE}.fact_all_sources_vw f
            WHERE f.invoice_status NOT IN ('Cancelled', 'Rejected')
              AND f.posting_date >= DATE_ADD('month', -12, CURRENT_DATE)
            GROUP BY 1
            ORDER BY month DESC
        """

    # 4. Payment performance / late payments
    if ("payment performance" in q) or ("late payment" in q) or ("cycle time" in q):
        return f"""
            SELECT
                DATE_TRUNC('month', f.payment_date) AS month,
                COUNT(*) AS total_payments,
                SUM(CASE WHEN f.payment_date > f.due_date THEN 1 ELSE 0 END) AS late_payments,
                AVG(CASE WHEN f.payment_date > f.due_date THEN DATE_DIFF('day', f.due_date, f.payment_date) ELSE 0 END) AS avg_late_days,
                AVG(DATE_DIFF('day', f.posting_date, f.payment_date)) AS avg_cycle_days
            FROM {DATABASE}.fact_all_sources_vw f
            WHERE f.payment_date IS NOT NULL
              AND f.payment_date >= DATE_ADD('month', -12, CURRENT_DATE)
            GROUP BY 1
            ORDER BY month DESC
        """

    # 5. Invoice aging / overdue
    if ("invoice aging" in q) or ("overdue" in q) or ("open invoices" in q):
        return f"""
            SELECT
                CASE
                    WHEN f.due_date < CURRENT_DATE THEN 'Overdue'
                    WHEN f.due_date <= CURRENT_DATE + INTERVAL '7' DAY THEN 'Due in 0-7 days'
                    WHEN f.due_date <= CURRENT_DATE + INTERVAL '30' DAY THEN 'Due in 8-30 days'
                    WHEN f.due_date <= CURRENT_DATE + INTERVAL '90' DAY THEN 'Due in 31-90 days'
                    ELSE 'Due in >90 days'
                END AS aging_bucket,
                COUNT(*) AS invoice_count,
                SUM(COALESCE(f.invoice_amount_local, 0)) AS total_amount
            FROM {DATABASE}.fact_all_sources_vw f
            WHERE f.invoice_status IN ('OPEN', 'DUE', 'OVERDUE')
            GROUP BY 1
            ORDER BY 
                CASE aging_bucket
                    WHEN 'Overdue' THEN 1
                    WHEN 'Due in 0-7 days' THEN 2
                    WHEN 'Due in 8-30 days' THEN 3
                    WHEN 'Due in 31-90 days' THEN 4
                    ELSE 5
                END
        """

    # 6. Early payment candidates
    if ("early payment" in q) or ("capture discount" in q):
        return f"""
            SELECT
                document_number,
                vendor_name,
                invoice_amount,
                due_date,
                days_until_due,
                savings_if_2pct_discount,
                early_pay_priority
            FROM {DATABASE}.early_payment_candidates_vw
            ORDER BY early_pay_priority ASC, savings_if_2pct_discount DESC
            LIMIT 10
        """

    # 7. Cash flow forecast
    if ("cash flow" in q) or ("forecast outflow" in q):
        return f"""
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
                ELSE 7 END
        """

    # 8. GR/IR hotspots
    if ("gr/ir" in q and "hotspots" in q) or ("gr/ir outstanding" in q):
        return f"""
            SELECT
                year,
                month,
                invoice_count,
                total_grir_blnc AS total_grir_balance
            FROM {DATABASE}.gr_ir_outstanding_balance_vw
            ORDER BY year DESC, month DESC
            LIMIT 12
        """

    # 9. GR/IR root causes
    if ("gr/ir" in q and "root cause" in q) or ("gr/ir aging" in q):
        return f"""
            SELECT
                year,
                month,
                pct_grir_over_60,
                cnt_grir_over_60
            FROM {DATABASE}.gr_ir_aging_vw
            ORDER BY year DESC, month DESC
            LIMIT 6
        """

    # 10. GR/IR working capital
    if ("gr/ir" in q and "working capital" in q) or ("release working capital" in q):
        return f"""
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

    # 11. GR/IR vendor follow‑up
    if ("gr/ir" in q and "vendor" in q) or ("follow up" in q and "gr/ir" in q):
        return f"""
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

    # 12. If no template matches, use LLM to generate SQL (safe fallback)
    schema_prompt = f"""
You are an Athena SQL expert. Generate ONLY a valid SELECT statement for the user's question.

Schema:
- Table {DATABASE}.fact_all_sources_vw: columns invoice_amount_local, posting_date, invoice_status, due_date, payment_date, vendor_id, invoice_number
- Table {DATABASE}.dim_vendor_vw: columns vendor_id, vendor_name

For vendor name, join: LEFT JOIN {DATABASE}.dim_vendor_vw v ON f.vendor_id = v.vendor_id

Do NOT use JSON functions.

Always include LIMIT 1000.

Question: {question}

SQL:
"""
    sql = ask_bedrock(schema_prompt, system_prompt="You are an Athena SQL expert.")
    if sql:
        sql = re.sub(r"```sql\s*", "", sql)
        sql = re.sub(r"```\s*", "", sql).strip()
        if not sql.lower().startswith("select"):
            sql = ""

    if not sql:
        # Ultimate fallback: return a summary of all data
        sql = f"""
            SELECT
                SUM(COALESCE(f.invoice_amount_local, 0)) AS total_spend,
                COUNT(DISTINCT f.invoice_number) AS total_invoices,
                COUNT(DISTINCT f.vendor_id) AS active_vendors
            FROM {DATABASE}.fact_all_sources_vw f
            WHERE f.invoice_status NOT IN ('Cancelled', 'Rejected')
        """
    return sql


def process_custom_query(query: str) -> dict:
    sql = get_sql_for_question(query)
    if not sql or not is_safe_sql(sql):
        return {"layout": "error", "message": "Could not generate safe SQL for this question."}
    sql = ensure_limit(sql)
    try:
        df = run_query(sql)
    except Exception as e:
        return {"layout": "error", "message": f"Athena query failed: {e}"}

    if df.empty:
        return {"layout": "error", "message": "Query returned no data. Try rephrasing your question."}

    data_preview = df.head(10).to_string(index=False, max_colwidth=40)
    prompt = f"""
You are a senior procurement analyst. The user asked: "{query}".

Based on the data from the SQL below, write a response in exactly this structure:

**Descriptive — What the data shows**

First write "This is our interpretation of your question:" followed by a clear restatement of the user's question. Then describe the key findings using exact numbers from the data.

**Prescriptive — Recommendations & next steps**

Write "Based on the provided data, here are the prescriptive insights, specific recommended actions, and risks:" then provide bullet points under subheadings like "Key Insights:", "Recommended Actions:", "Risks:". Each bullet must include specific findings, actions, and where relevant potential losses/savings. End with a concluding sentence.

Data preview:
{data_preview}

SQL used:
{sql}

Respond in plain text using markdown for headings and bullet points. Do not include any extra commentary.
"""
    analyst_text = ask_bedrock(prompt, system_prompt="You are a helpful procurement analyst.")
    if not analyst_text:
        analyst_text = f"**Analysis complete.**\n\nHere are the results:\n\n{data_preview}"

    return {
        "layout": "analyst",
        "sql": sql,
        "df": df.to_dict(orient="records"),
        "question": query,
        "analyst_response": analyst_text
    }


# ------------------------------------------------------------
# Specialised handlers (full implementations – same as previous)
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
        "sql": {"aging_sql": aging_sql, "balance_sql": balance_sql},
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
# Rendering functions for each layout type
# ------------------------------------------------------------
def render_cash_flow_response(result: dict):
    df = pd.DataFrame(result["df"])
    if df.empty:
        st.error("No cash flow data to display.")
        return

    total_unpaid = df[df["forecast_bucket"] == "TOTAL_UNPAID"]["total_amount"].values[0] if not df[df["forecast_bucket"] == "TOTAL_UNPAID"].empty else 0
    overdue_now = df[df["forecast_bucket"] == "OVERDUE_NOW"]["total_amount"].values[0] if not df[df["forecast_bucket"] == "OVERDUE_NOW"].empty else 0
    due_30 = df[df["forecast_bucket"].isin(["DUE_7_DAYS", "DUE_14_DAYS", "DUE_30_DAYS"])]["total_amount"].sum()
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
        st.code(_safe_sql_string(result.get("sql")), language="sql")


def render_early_payment_response(result: dict):
    df = pd.DataFrame(result["df"])
    empty = result.get("empty", False)
    if empty or df.empty:
        st.info("No early payment candidates were found.")
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
        st.code(_safe_sql_string(result.get("sql")), language="sql")


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
        st.code(_safe_sql_string(result.get("sql")), language="sql")


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
        st.code(_safe_sql_string(result.get("sql")), language="sql")


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
        st.code(_safe_sql_string(result.get("sql")), language="sql")


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
        st.code(_safe_sql_string(result.get("sql")), language="sql")


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
        st.code(_safe_sql_string(result.get("sql")), language="sql")


def render_grir_vendor_followup(result: dict):
    df = pd.DataFrame(result["df"])
    if not df.empty:
        st.subheader("Top Vendors with Outstanding GR/IR Items")
        st.dataframe(df, use_container_width=True, hide_index=True)

    if result.get("analyst_response"):
        st.markdown("### 💡 Key Insights")
        st.markdown(result["analyst_response"])

    with st.expander("View SQL used"):
        st.code(_safe_sql_string(result.get("sql")), language="sql")


def render_quick_analysis_response(result: dict):
    metrics = result.get("metrics", {})
    monthly_df = result.get("monthly_df")
    vendors_df = result.get("vendors_df")
    anomaly = result.get("anomaly")
    prescriptive = result.get("analyst_response")
    sql_queries = result.get("sql", {})

    def get_metric(key, default=0):
        val = metrics.get(key, default)
        if isinstance(val, (int, float)):
            return val
        return default

    st.markdown(f"**Your question**\n{result.get('question', 'Spending Overview')}")
    st.markdown("---")

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        total_spend = get_metric("total_ytd", get_metric("total_spend", 0))
        st.metric("Total Spend (YTD)", abbr_currency(total_spend))
    with col2:
        mom = get_metric("mom_pct", 0)
        st.metric("MoM Change", f"{mom:+.1f}%")
    with col3:
        top5_pct = get_metric("top5_pct", 0)
        st.metric("Top 5 Vendors", f"{top5_pct:.1f}% of total spend")
    with col4:
        qoq = get_metric("qoq_pct", 0)
        st.metric("QoQ Change", f"{qoq:+.1f}%")

    if anomaly:
        st.warning(f"⚠️ **Anomaly Detected**\n\n{anomaly}")

    if monthly_df is not None and not monthly_df.empty:
        st.subheader("Spending Trends")
        monthly_df = monthly_df.copy()
        if "month" in monthly_df.columns:
            monthly_df["month_dt"] = pd.to_datetime(monthly_df["month"])
            monthly_df = monthly_df.sort_values("month_dt")
            monthly_df["month_str"] = monthly_df["month_dt"].dt.strftime("%b %Y")

        if "monthly_spend" in monthly_df.columns:
            spend_chart = alt.Chart(monthly_df).mark_bar(color="#22c55e", cornerRadiusEnd=4).encode(
                x=alt.X("month_str:N", title=None, sort=None),
                y=alt.Y("monthly_spend:Q", title=None, axis=alt.Axis(format="~s")),
                tooltip=["month_str:N", alt.Tooltip("monthly_spend:Q", format="$,.0f")]
            ).properties(height=250, title="Monthly Spend Trend (Last 12 Months)")
            st.altair_chart(spend_chart, use_container_width=True)

        if "invoice_count" in monthly_df.columns:
            invoice_chart = alt.Chart(monthly_df).mark_bar(color="#3b82f6", cornerRadiusEnd=4).encode(
                x=alt.X("month_str:N", title=None, sort=None),
                y=alt.Y("invoice_count:Q", title=None),
                tooltip=["month_str:N", "invoice_count:Q"]
            ).properties(height=250, title="Invoice volume by month")
            st.altair_chart(invoice_chart, use_container_width=True)

        if "vendor_count" in monthly_df.columns:
            vendor_chart = alt.Chart(monthly_df).mark_bar(color="#f59e0b", cornerRadiusEnd=4).encode(
                x=alt.X("month_str:N", title=None, sort=None),
                y=alt.Y("vendor_count:Q", title=None),
                tooltip=["month_str:N", "vendor_count:Q"]
            ).properties(height=250, title="Active vendors by month")
            st.altair_chart(vendor_chart, use_container_width=True)

    if vendors_df is not None and not vendors_df.empty:
        st.subheader("Top 10 Vendors by Spend (YTD)")
        top_vendors = vendors_df.head(10).copy()
        if "spend" in top_vendors.columns and "vendor_name" in top_vendors.columns:
            bar_chart = alt.Chart(top_vendors).mark_bar(color="#22c55e", cornerRadiusEnd=4).encode(
                x=alt.X("spend:Q", title=None, axis=alt.Axis(format="~s")),
                y=alt.Y("vendor_name:N", sort="-x", title=None),
                tooltip=["vendor_name:N", alt.Tooltip("spend:Q", format="$,.0f")]
            ).properties(height=400)
            st.altair_chart(bar_chart, use_container_width=True)

    if prescriptive:
        st.markdown("### Prescriptive — Recommendations & next steps")
        st.markdown(prescriptive)

    with st.expander("Query outputs"):
        if monthly_df is not None and not monthly_df.empty:
            st.subheader("Monthly trend")
            st.dataframe(monthly_df, use_container_width=True, hide_index=True)
        if vendors_df is not None and not vendors_df.empty:
            st.subheader("Top / bucket breakdown")
            st.dataframe(vendors_df, use_container_width=True, hide_index=True)

    with st.expander("Show SQL used"):
        if isinstance(sql_queries, dict):
            if "monthly_trend" in sql_queries:
                st.code(sql_queries["monthly_trend"], language="sql")
            if "top_vendors" in sql_queries:
                st.code(sql_queries["top_vendors"], language="sql")
        elif isinstance(sql_queries, str):
            st.code(sql_queries, language="sql")
        else:
            st.caption("No SQL available.")


# ------------------------------------------------------------
# Main Genie render function – with expandable left column
# ------------------------------------------------------------
def render_genie():
    st.markdown("""
<style>
    .main-container { max-width: 1400px; margin: 0 auto; }
    .welcome-header { text-align: center; padding: 0.5rem 0 0.5rem 0; }
    .welcome-header h1 { font-size: 1.8rem; font-weight: 600; color: #1e293b; margin-bottom: 0.25rem; }
    .welcome-header p { color: #64748b; font-size: 0.9rem; }
    .quick-card {
        background: white;
        border-radius: 16px;
        padding: 1.2rem;
        box-shadow: 0 2px 8px rgba(0,0,0,0.06);
        border: 1px solid #e2e8f0;
        text-align: center;
        transition: all 0.2s ease;
        height: 100%;
        display: flex;
        flex-direction: column;
    }
    .quick-card:hover { transform: translateY(-2px); box-shadow: 0 8px 20px rgba(0,0,0,0.08); }
    .card-icon {
        width: 48px; height: 48px; background: #3b82f6; border-radius: 12px;
        display: flex; align-items: center; justify-content: center;
        margin: 0 auto 0.8rem auto; font-size: 1.3rem;
    }
    .quick-card h3 { font-size: 1rem; font-weight: 600; color: #1e293b; margin: 0 0 0.4rem 0; }
    .quick-card p { font-size: 0.8rem; color: #64748b; line-height: 1.4; margin: 0 0 0.8rem 0; flex-grow: 1; }
    .quick-card button { margin-top: auto; }
    .chat-messages {
        max-height: 400px; overflow-y: auto; padding: 0.5rem; margin-bottom: 1rem;
        background: #fafcff; border-radius: 16px;
        border: 1px solid #e2e8f0;
    }
    .message-user {
        background: linear-gradient(135deg, #3b82f6 0%, #2563eb 100%);
        color: white; padding: 10px 16px; border-radius: 18px 18px 4px 18px;
        margin: 8px 0; max-width: 80%; margin-left: auto; text-align: right;
    }
    .message-assistant {
        background: #f1f5f9; color: #1e293b; padding: 10px 16px;
        border-radius: 18px 18px 18px 4px; margin: 8px 0; max-width: 85%;
    }
    .start-conversation {
        text-align: center; padding: 2rem 1rem; background: #f8fafc;
        border-radius: 20px; margin: 1rem 0;
    }
    .plus-button {
        width: 56px; height: 56px; background: linear-gradient(135deg, #60a5fa 0%, #3b82f6 100%);
        border-radius: 50%; display: flex; align-items: center; justify-content: center;
        margin: 0 auto 1rem auto; cursor: pointer; box-shadow: 0 4px 12px rgba(59,130,246,0.3);
    }
    .plus-button span { font-size: 1.8rem; color: white; font-weight: 300; }
    hr { margin: 0.5rem 0; }
</style>
    """, unsafe_allow_html=True)

    # Session state
    if "genie_session_id" not in st.session_state:
        st.session_state.genie_session_id = str(uuid.uuid4())
    if "current_messages" not in st.session_state:
        st.session_state.current_messages = []
    if "genie_prefill" not in st.session_state:
        st.session_state.genie_prefill = ""

    quick_map = {
        "Spending Overview": "spending_overview",
        "Vendor Analysis": "vendor_analysis",
        "Payment Performance": "payment_performance",
        "Invoice Aging": "invoice_aging"
    }

    # Process auto-run from card buttons
    auto_query = st.session_state.pop("auto_run_query", None)
    if auto_query:
        with st.spinner("Running analysis..."):
            lower_q = auto_query.lower()
            if any(kw in lower_q for kw in ["forecast cash outflow", "cash flow forecast"]):
                result = process_cash_flow_forecast(auto_query)
            elif any(kw in lower_q for kw in ["pay early", "capture discounts"]):
                result = process_early_payment(auto_query)
            elif any(kw in lower_q for kw in ["optimal payment timing"]):
                result = process_payment_timing(auto_query)
            elif any(kw in lower_q for kw in ["late payment trend"]):
                result = process_late_payment_trend(auto_query)
            elif "gr/ir" in lower_q and "hotspots" in lower_q:
                result = process_grir_hotspots(auto_query)
            elif "root-cause" in lower_q:
                result = process_grir_root_causes(auto_query)
            elif "working-capital" in lower_q:
                result = process_grir_working_capital(auto_query)
            elif "vendor follow-up" in lower_q:
                result = process_grir_vendor_followup(auto_query)
            elif auto_query in quick_map:
                result = run_quick_analysis(quick_map[auto_query])
            else:
                result = process_custom_query(auto_query)

            st.session_state.current_messages = []
            st.session_state.current_messages.append({"role": "user", "content": auto_query, "timestamp": datetime.now()})
            if result.get("layout") != "error":
                assistant_content = result.get('analyst_response', 'Analysis complete.')
                st.session_state.current_messages.append({"role": "assistant", "content": assistant_content, "response": result, "timestamp": datetime.now()})
                save_chat_message(st.session_state.genie_session_id, 0, "user", auto_query)
                sql_used = _safe_sql_string(result.get("sql"))
                save_chat_message(st.session_state.genie_session_id, 1, "assistant", assistant_content, sql_used=sql_used)
                save_question(auto_query, "forecast")
                set_cache(auto_query, result)
            else:
                st.session_state.current_messages.append({"role": "assistant", "content": result.get("message", "Error"), "timestamp": datetime.now()})
            st.rerun()

    # ----- TOP: four quick analysis cards -----
    st.markdown('<div class="welcome-header"><h1>Welcome to ProcureIQ Genie</h1><p>Let Genie run one of these quick analyses for you</p></div>', unsafe_allow_html=True)
    cards_data = [
        {"icon": "📊", "title": "Spending Overview", "description": "Track total spend, monthly trends and major changes"},
        {"icon": "🏭", "title": "Vendor Analysis", "description": "Understand vendor-wise spend, concentration, and dependency"},
        {"icon": "⏱️", "title": "Payment Performance", "description": "Identify delays, late payments, and cycle time issues"},
        {"icon": "📅", "title": "Invoice Aging", "description": "See overdue invoices, risk buckets, and problem areas"}
    ]
    cols = st.columns(4, gap="small")
    for idx, (col, card) in enumerate(zip(cols, cards_data)):
        with col:
            st.markdown(f"""
<div class="quick-card">
<div class="card-icon">{card['icon']}</div>
<h3>{card['title']}</h3>
<p>{card['description']}</p>
</div>
            """, unsafe_allow_html=True)
            if st.button("Ask Genie", key=f"card_{idx}", use_container_width=True):
                st.session_state.auto_run_query = card['title']
                st.rerun()

    st.markdown("---")

    # ----- BOTTOM: Two columns (left expanders, right chat) -----
    left_info, right_chat = st.columns([0.35, 0.65], gap="large")

    with left_info:
        # Saved insights (expandable)
        with st.expander("Saved insights"):
            insights = get_saved_insights_cached(page="genie")
            if insights:
                for ins in insights[:5]:
                    if st.button(f"› {ins['title'][:40]}...", key=f"insight_{ins['id']}", use_container_width=True):
                        st.session_state.auto_run_query = ins["question"]
                        st.rerun()
            else:
                st.caption("No saved insights yet")

        # Frequently asked by you (expandable)
        with st.expander("Frequently asked by you"):
            faqs = get_frequent_questions_by_user_cached(5)
            if faqs:
                for faq in faqs[:5]:
                    if st.button(f"› {faq['query'][:40]}...", key=f"faq_user_{faq['query'][:20]}", use_container_width=True):
                        st.session_state.genie_prefill = faq["query"]
                        st.rerun()
            else:
                suggestions = ["Total spend YTD and trends", "Top vendors by spend", "Overdue invoices summary"]
                for sug in suggestions:
                    if st.button(f"› {sug}", key=f"sug_{sug[:15]}", use_container_width=True):
                        st.session_state.genie_prefill = sug
                        st.rerun()

        # Most frequent (all) (expandable)
        with st.expander("Most frequent (all)"):
            all_faqs = get_frequent_questions_all_cached(5)
            if all_faqs:
                for faq in all_faqs[:5]:
                    st.markdown(f"<div style='color: #64748b; font-size: 0.85rem; padding: 0.25rem 0;'>› {faq['query'][:40]}...</div>", unsafe_allow_html=True)
            else:
                st.caption("No questions yet")

    with right_chat:
        st.markdown('<div style="text-align: right; margin-bottom: 0.5rem;"><span style="font-size: 1rem; font-weight: 600; color: #1e293b;">AI Assistant</span></div>', unsafe_allow_html=True)

        if not st.session_state.current_messages:
            st.markdown("""
<div class="start-conversation">
<div class="plus-button"><span>+</span></div>
<div style="font-size: 1.1rem; font-weight: 600; color: #1e293b;">Start a Conversation</div>
<div style="color: #64748b; font-size: 0.85rem; max-width: 280px; margin: 0.5rem auto;">Ask questions about your Procurement to Pay data, or select a pre-built analysis from the library.</div>
</div>
            """, unsafe_allow_html=True)
        else:
            st.markdown('<div class="chat-messages">', unsafe_allow_html=True)
            for msg in st.session_state.current_messages:
                if msg["role"] == "user":
                    st.markdown(f'<div class="message-user"><strong>You</strong><br/>{html.escape(msg["content"])}</div>', unsafe_allow_html=True)
                else:
                    st.markdown('<div class="message-assistant"><strong>🧞 Genie</strong></div>', unsafe_allow_html=True)
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
                            df = pd.DataFrame(resp["df"])
                            if not df.empty:
                                st.subheader("Supporting Data")
                                st.dataframe(df, use_container_width=True, hide_index=True)
                                chart = auto_chart(df)
                                if chart:
                                    st.altair_chart(chart, use_container_width=True)
                            with st.expander("View SQL used"):
                                st.code(_safe_sql_string(resp.get("sql")), language="sql")
                        elif layout == "error":
                            st.error(resp.get("message", "Unknown error"))
                    else:
                        st.markdown(msg["content"])
            st.markdown('</div>', unsafe_allow_html=True)

        # Chat input form (placed at bottom)
        with st.form(key="genie_chat_form", clear_on_submit=True):
            col_in, col_btn = st.columns([0.85, 0.15])
            with col_in:
                prefill = st.session_state.pop("genie_prefill", "")
                user_question = st.text_input(
                    "Ask a question",
                    value=prefill,
                    placeholder="Ask a question here...",
                    label_visibility="collapsed"
                )
            with col_btn:
                submitted = st.form_submit_button("→", type="primary", use_container_width=True)
            if submitted and user_question:
                process_user_question(user_question, quick_map)


def process_user_question(user_question: str, quick_map: dict):
    with st.spinner("Generating insights..."):
        cached = get_cache(user_question)
        if cached:
            st.session_state.current_messages = []
            st.session_state.current_messages.append({"role": "user", "content": user_question, "timestamp": datetime.now()})
            assistant_content = cached.get('analyst_response', 'Analysis complete.')
            st.session_state.current_messages.append({"role": "assistant", "content": assistant_content, "response": cached, "timestamp": datetime.now()})
            save_chat_message(st.session_state.genie_session_id, 0, "user", user_question)
            sql_used = _safe_sql_string(cached.get("sql"))
            save_chat_message(st.session_state.genie_session_id, 1, "assistant", assistant_content, source="cache", sql_used=sql_used)
            save_question(user_question, "custom")
        else:
            lower_q = user_question.lower()
            if any(kw in lower_q for kw in ["forecast cash outflow", "cash flow forecast"]):
                result = process_cash_flow_forecast(user_question)
            elif any(kw in lower_q for kw in ["pay early", "capture discounts"]):
                result = process_early_payment(user_question)
            elif any(kw in lower_q for kw in ["optimal payment timing"]):
                result = process_payment_timing(user_question)
            elif any(kw in lower_q for kw in ["late payment trend"]):
                result = process_late_payment_trend(user_question)
            elif "gr/ir" in lower_q and "hotspots" in lower_q:
                result = process_grir_hotspots(user_question)
            elif "root-cause" in lower_q:
                result = process_grir_root_causes(user_question)
            elif "working-capital" in lower_q:
                result = process_grir_working_capital(user_question)
            elif "vendor follow-up" in lower_q:
                result = process_grir_vendor_followup(user_question)
            elif user_question in quick_map:
                result = run_quick_analysis(quick_map[user_question])
            else:
                result = process_custom_query(user_question)

            st.session_state.current_messages = []
            st.session_state.current_messages.append({"role": "user", "content": user_question, "timestamp": datetime.now()})
            if result.get("layout") != "error":
                assistant_content = result.get('analyst_response', 'Analysis complete.')
                st.session_state.current_messages.append({"role": "assistant", "content": assistant_content, "response": result, "timestamp": datetime.now()})
                set_cache(user_question, result)
                save_chat_message(st.session_state.genie_session_id, 0, "user", user_question)
                sql_used = _safe_sql_string(result.get("sql"))
                save_chat_message(st.session_state.genie_session_id, 1, "assistant", assistant_content, sql_used=sql_used)
                save_question(user_question, "forecast")
            else:
                st.session_state.current_messages.append({"role": "assistant", "content": result.get("message", "Error"), "timestamp": datetime.now()})
    st.rerun()
