# ================================
# P2P Analytics + Genie (Athena + Bedrock Nova)
# OPTIMIZED VERSION: Full caching, no Snowflake, pure AWS
# ================================

import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
from datetime import date, timedelta, datetime
import boto3
import awswrangler as wr
import json
import re
import hashlib
import uuid
import sqlite3
import math
import html
from typing import Optional, Dict, Any, List, Union
from decimal import Decimal
from difflib import SequenceMatcher
from functools import lru_cache

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

@st.cache_resource
def get_aws_session():
    return boto3.Session()

@st.cache_resource
def get_bedrock_runtime():
    return get_aws_session().client("bedrock-runtime", region_name=ATHENA_REGION)

@st.cache_resource
def get_athena_client():
    return get_aws_session().client("athena", region_name=ATHENA_REGION)

# Cached query execution
@st.cache_data(ttl=300, show_spinner=False)
def run_query_cached(sql: str) -> pd.DataFrame:
    """Execute Athena query with caching (5 minutes TTL)."""
    try:
        session = get_aws_session()
        df = wr.athena.read_sql_query(sql, database=DATABASE, boto3_session=session)
        # Convert Decimal columns to float once
        for col in df.columns:
            if df[col].dtype == object and df[col].apply(lambda x: isinstance(x, Decimal)).any():
                df[col] = df[col].astype(float)
        return df
    except Exception as e:
        st.error(f"Athena query failed: {e}\nSQL: {sql[:500]}")
        return pd.DataFrame()

run_query = run_query_cached

# ---------------------------- Helper functions (memoized) ----------------------------
@st.cache_data
def safe_number(val, default=0.0):
    try:
        if pd.isna(val):
            return default
        return float(val)
    except Exception:
        return default

@st.cache_data
def safe_int(val, default=0):
    try:
        if pd.isna(val):
            return default
        return int(float(val))
    except Exception:
        return default

@st.cache_data
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

def _safe_pct_str(val, default=0.0):
    v = safe_number(val, default)
    sign = "+" if v >= 0 else ""
    return f"{sign}{v:.1f}%"

def make_json_serializable(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: make_json_serializable(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [make_json_serializable(i) for i in obj]
    return obj

def clean_invoice_number(inv_num):
    try:
        if isinstance(inv_num, (float, Decimal)):
            return str(int(inv_num))
        s = str(inv_num)
        if '.' in s:
            s = s.split('.')[0]
        return s
    except:
        return str(inv_num)

# ---------------------------- AI Chat Functions (Bedrock Nova) ----------------------------
SEMANTIC_MODEL_YAML = f"""
name: "P2P Procure-to-Pay Analytics"
description: "Procure-to-Pay and Invoice-to-Pay analytics..."
custom_instructions: |
  FIRST PASS PO'S (HIGHEST PRIORITY - MANDATORY):
  - When user asks ANY variation of "first pass PO's", you MUST use verified query first_pass_pos.
  PRESCRIPTIVE RESPONSE RULES:
  - NEVER give generic advice without citing SPECIFIC numbers.
  - Exclude CANCELLED and REJECTED from spend metrics unless asked.
tables:
  - name: fact_invoices
    base_table: {DATABASE}.fact_all_sources_vw
    measures:
      - name: invoice_amount
        expr: invoice_amount_local
        default_aggregation: sum
  - name: dim_vendor
    base_table: {DATABASE}.dim_vendor_vw
  - name: cash_flow_forecast
    base_table: {DATABASE}.cash_flow_forecast_vw
  - name: gr_ir_outstanding
    base_table: {DATABASE}.gr_ir_outstanding_balance_vw
  - name: gr_ir_aging
    base_table: {DATABASE}.gr_ir_aging_vw
"""

SYSTEM_PROMPT = f"""
You are an AI assistant that helps users query a procurement database using SQL (Athena/Presto). Given a user's natural language question, generate a valid SQL query for Athena (Presto dialect) based on the following semantic model.

Semantic Model (YAML):
{SEMANTIC_MODEL_YAML}

Important notes:
- Use standard Presto/Athena SQL functions (DATE_TRUNC, DATE_ADD, DATE_DIFF, etc.).
- For date filtering, prefer `posting_date BETWEEN DATE '...' AND DATE '...'`.
- Always use COALESCE for null amounts.
- Exclude CANCELLED and REJECTED invoices from spend metrics unless asked.
- Output only a JSON object with two keys: "sql" containing the SQL query string, and "explanation". Do not include any other text.
"""

@lru_cache(maxsize=100)
def ask_bedrock_cached(prompt: str, system_prompt: str = SYSTEM_PROMPT) -> str:
    """Cached Bedrock invocation."""
    try:
        body = json.dumps({
            "messages": [{"role": "user", "content": [{"text": prompt}]}],
            "system": [{"text": system_prompt}],
            "inferenceConfig": {"maxTokens": 4096, "temperature": 0.0, "topP": 0.9}
        })
        bedrock = get_bedrock_runtime()
        response = bedrock.invoke_model(
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
    response = ask_bedrock_cached(prompt)
    if not response:
        return None, "Bedrock returned empty response."
    json_match = re.search(r'\{.*\}$', response, re.DOTALL)
    json_str = json_match.group(0) if json_match else response
    try:
        data = json.loads(json_str)
        sql = data.get("sql", "").strip()
        explanation = data.get("explanation", "")
        return sql, explanation
    except json.JSONDecodeError:
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

def _pick_chart_columns(df: pd.DataFrame) -> tuple:
    if df.empty or len(df.columns) < 2:
        return (None, None)
    cols = list(df.columns)
    cat_prefer = ("vendor_name", "month", "status", "aging_bucket", "po_purpose")
    num_prefer = ("spend", "total_spend", "invoice_count", "amount")
    cols_lower = {c.lower(): c for c in cols}
    x_col = next((cols_lower[p] for p in cat_prefer if p in cols_lower), cols[0])
    y_col = next((cols_lower[p] for p in num_prefer if p in cols_lower and cols_lower[p] != x_col), None)
    if not y_col:
        for c in cols:
            if c != x_col and pd.api.types.is_numeric_dtype(df[c]):
                y_col = c
                break
    if not y_col and len(cols) > 1:
        y_col = cols[1]
    return (x_col, y_col)

def alt_bar(df, x, y, title=None, horizontal=False, color="#1459d2", height=320):
    if df.empty:
        st.info("No data for this chart.")
        return
    if horizontal:
        chart = alt.Chart(df).mark_bar(color=color, cornerRadiusTopLeft=4).encode(
            x=alt.X(y, type='quantitative', axis=alt.Axis(title=None, format="~s")),
            y=alt.Y(x, type='nominal', sort='-x', axis=alt.Axis(title=None)),
            tooltip=[x, alt.Tooltip(y, format=",.0f")]
        )
    else:
        chart = alt.Chart(df).mark_bar(color=color, cornerRadiusTopLeft=4).encode(
            x=alt.X(x, type='nominal', axis=alt.Axis(title=None)),
            y=alt.Y(y, type='quantitative', axis=alt.Axis(title=None, format="~s")),
            tooltip=[x, alt.Tooltip(y, format=",.0f")]
        )
    chart = chart.properties(height=height)
    if title:
        chart = chart.properties(title=title)
    st.altair_chart(chart, use_container_width=True)

def alt_line_monthly(df, month_col='month', value_col='value', height=140, title=None):
    if df.empty:
        st.info("No data for this chart.")
        return
    data = df.copy()
    try:
        data['_month_dt'] = pd.to_datetime(data[month_col].astype(str) + '-01')
        data = data.sort_values('_month_dt')
        data['month_label'] = data['_month_dt'].dt.strftime('%b %Y')
    except:
        data['month_label'] = data[month_col].astype(str)
    chart = alt.Chart(data).mark_line(point=True, color='#1e88e5').encode(
        x=alt.X('month_label:N', sort=None, axis=alt.Axis(title=None, labelAngle=-45)),
        y=alt.Y(f'{value_col}:Q', axis=alt.Axis(title=None, grid=False, format='~s')),
        tooltip=[alt.Tooltip('month_label:N', title='Month'), alt.Tooltip(f'{value_col}:Q', format=',.0f')]
    ).properties(height=height)
    if title:
        chart = chart.properties(title=title)
    st.altair_chart(chart, use_container_width=True)

def alt_donut_status(df, label_col="status", value_col="cnt", title=None, height=340):
    if df.empty or df[value_col].sum() == 0:
        st.info("No data for donut chart.")
        return
    total = df[value_col].sum()
    df['pct'] = df[value_col] / total
    order = ["Paid", "Pending", "Disputed", "Other"]
    palette = {"Paid": "#22C55E", "Pending": "#FBBF24", "Disputed": "#EF4444", "Other": "#1E88E5"}
    for cat in order:
        if cat not in df[label_col].values:
            df = pd.concat([df, pd.DataFrame({label_col: [cat], value_col: [0], 'pct': [0.0]})], ignore_index=True)
    base = alt.Chart(df).encode(
        theta=alt.Theta(field=value_col, type='quantitative', stack=True),
        color=alt.Color(field=label_col, type='nominal', scale=alt.Scale(domain=order, range=[palette[k] for k in order])),
        tooltip=[label_col, value_col, alt.Tooltip('pct:Q', format='.1%')]
    )
    arc = base.mark_arc(innerRadius=40, outerRadius=100)
    text = base.transform_filter(alt.datum.pct >= 0.01).mark_text(radius=115, color='#0f172a', fontSize=12, fontWeight='bold').encode(text=alt.Text('pct:Q', format='.1%'))
    chart = (arc + text).properties(height=height)
    if title:
        chart = chart.properties(title=title)
    st.altair_chart(chart, use_container_width=True)

# ---------------------------- Quick Analysis Functions (Athena) ----------------------------
@st.cache_data(ttl=600)
def run_quick_analysis_cached(key: str) -> dict:
    """Run SQL for quick-analysis tiles; cached for 10 minutes."""
    base = f"{DATABASE}.fact_all_sources_vw f LEFT JOIN {DATABASE}.dim_vendor_vw v ON f.vendor_id = v.vendor_id"
    flt = "AND UPPER(f.invoice_status) NOT IN ('CANCELLED','REJECTED')"
    out = {"layout": "quick", "type": key, "metrics": {}, "monthly_df": None, "vendors_df": None, "extra_dfs": {}, "sql": {}, "anomaly": None}
    today = date.today()
    ytd_start = date(today.year, 1, 1)
    start_lit = sql_date(ytd_start)
    end_lit = sql_date(today)

    if key == "spending_overview":
        total_sql = f"""
            SELECT SUM(COALESCE(f.invoice_amount_local,0)) AS total_spend
            FROM {base}
            WHERE f.posting_date BETWEEN {start_lit} AND {end_lit} {flt}
        """
        total_df = run_query(total_sql)
        total_spend = safe_number(total_df.loc[0,"total_spend"]) if not total_df.empty else 0

        mom_sql = f"""
            WITH monthly AS (
                SELECT DATE_TRUNC('month', f.posting_date) AS month,
                       SUM(COALESCE(f.invoice_amount_local,0)) AS spend
                FROM {base}
                WHERE f.posting_date BETWEEN {start_lit} AND {end_lit} {flt}
                GROUP BY 1
            )
            SELECT spend FROM monthly ORDER BY month DESC LIMIT 1
        """
        cur_m = safe_number(run_query(mom_sql).loc[0,"spend"]) if not run_query(mom_sql).empty else 0
        prev_m_sql = f"""
            WITH monthly AS (
                SELECT DATE_TRUNC('month', f.posting_date) AS month,
                       SUM(COALESCE(f.invoice_amount_local,0)) AS spend
                FROM {base}
                WHERE f.posting_date BETWEEN DATE_ADD('month', -1, {start_lit}) AND DATE_ADD('month', -1, {end_lit}) {flt}
                GROUP BY 1
            )
            SELECT spend FROM monthly ORDER BY month DESC LIMIT 1
        """
        prev_m = safe_number(run_query(prev_m_sql).loc[0,"spend"]) if not run_query(prev_m_sql).empty else 0
        mom_pct = ((cur_m - prev_m)/prev_m*100) if prev_m else 0

        current_quarter_start = date(today.year, ((today.month-1)//3)*3 + 1, 1)
        prev_quarter_start = date(today.year if current_quarter_start.month > 1 else today.year-1,
                                  ((current_quarter_start.month-1)//3)*3 + 1 if current_quarter_start.month > 1 else 10, 1)
        prev_quarter_end = current_quarter_start - timedelta(days=1)
        cur_q_sql = f"""
            SELECT SUM(COALESCE(f.invoice_amount_local,0)) AS spend
            FROM {base}
            WHERE f.posting_date BETWEEN '{sql_date(current_quarter_start)}' AND '{sql_date(today)}' {flt}
        """
        prev_q_sql = f"""
            SELECT SUM(COALESCE(f.invoice_amount_local,0)) AS spend
            FROM {base}
            WHERE f.posting_date BETWEEN '{sql_date(prev_quarter_start)}' AND '{sql_date(prev_quarter_end)}' {flt}
        """
        cur_q = safe_number(run_query(cur_q_sql).loc[0,"spend"]) if not run_query(cur_q_sql).empty else 0
        prev_q = safe_number(run_query(prev_q_sql).loc[0,"spend"]) if not run_query(prev_q_sql).empty else 0
        qoq_pct = ((cur_q - prev_q)/prev_q*100) if prev_q else 0

        top5_sql = f"""
            SELECT v.vendor_name, SUM(COALESCE(f.invoice_amount_local,0)) AS spend
            FROM {base}
            WHERE f.posting_date BETWEEN {start_lit} AND {end_lit} {flt}
            GROUP BY 1 ORDER BY spend DESC LIMIT 5
        """
        top5 = run_query(top5_sql)
        top5_sum = safe_number(top5["spend"].sum()) if not top5.empty else 0
        top5_pct = (top5_sum / total_spend * 100) if total_spend else 0

        out["metrics"] = {"total_ytd": total_spend, "mom_pct": mom_pct, "qoq_pct": qoq_pct, "top5_pct": top5_pct}

        monthly_sql = f"""
            SELECT DATE_FORMAT(f.posting_date, '%Y-%m') AS MONTH,
                   SUM(COALESCE(f.invoice_amount_local,0)) AS MONTHLY_SPEND,
                   COUNT(DISTINCT f.invoice_number) AS INVOICE_COUNT,
                   COUNT(DISTINCT f.vendor_id) AS VENDOR_COUNT
            FROM {base}
            WHERE f.posting_date >= DATE_ADD('month', -12, {end_lit}) {flt}
            GROUP BY 1 ORDER BY 1
        """
        monthly_df = run_query(monthly_sql)
        out["monthly_df"] = monthly_df
        out["extra_dfs"]["monthly_full"] = monthly_df

        anomaly = None
        if monthly_df is not None and not monthly_df.empty and "MONTHLY_SPEND" in monthly_df.columns:
            monthly_df = monthly_df.sort_values("MONTH")
            monthly_df["prev_spend"] = monthly_df["MONTHLY_SPEND"].shift(1)
            monthly_df["pct_change"] = (monthly_df["MONTHLY_SPEND"] - monthly_df["prev_spend"]) / monthly_df["prev_spend"] * 100
            spikes = monthly_df[monthly_df["pct_change"] > 20].copy()
            if not spikes.empty:
                max_spike = spikes.loc[spikes["pct_change"].idxmax()]
                spike_month = max_spike["MONTH"]
                spike_pct = max_spike["pct_change"]
                top_vendor_sql = f"""
                    SELECT v.vendor_name, SUM(COALESCE(f.invoice_amount_local,0)) AS spend
                    FROM {base}
                    WHERE DATE_FORMAT(f.posting_date, '%Y-%m') = '{spike_month}' {flt}
                    GROUP BY 1 ORDER BY 2 DESC LIMIT 1
                """
                top_vendor_df = run_query(top_vendor_sql)
                vendor = top_vendor_df.at[0, "vendor_name"] if not top_vendor_df.empty else "a top vendor"
                vendor_amt = safe_number(top_vendor_df.at[0, "spend"]) if not top_vendor_df.empty else 0
                anomaly = f"{spike_month} spending spiked by {spike_pct:.0f}% vs prior month, primarily driven by {vendor} ({abbr_currency(vendor_amt)})."
        out["anomaly"] = anomaly

        vendors_sql = f"""
            SELECT v.vendor_name, SUM(COALESCE(f.invoice_amount_local,0)) AS SPEND
            FROM {base}
            WHERE f.posting_date BETWEEN {start_lit} AND {end_lit} {flt}
            GROUP BY 1 ORDER BY SPEND DESC LIMIT 20
        """
        out["vendors_df"] = run_query(vendors_sql)
        out["sql"]["monthly_trend"] = monthly_sql
        out["sql"]["top_vendors"] = vendors_sql

    elif key == "vendor_analysis":
        vendors_sql = f"""
            SELECT v.vendor_name, SUM(COALESCE(f.invoice_amount_local,0)) AS SPEND, COUNT(*) AS INVOICE_COUNT
            FROM {base}
            WHERE f.posting_date >= DATE_ADD('month', -6, CURRENT_DATE) {flt}
            GROUP BY 1 ORDER BY SPEND DESC
        """
        out["vendors_df"] = run_query(vendors_sql)
        out["metrics"] = {"summary": "Top vendors by spend last 6 months."}
        out["sql"]["vendor_analysis"] = vendors_sql

    elif key == "payment_performance":
        pm_sql = f"""
            SELECT DATE_FORMAT(f.payment_date, '%Y-%m') AS MONTH,
                   ROUND(AVG(DATE_DIFF('day', f.posting_date, f.payment_date)),1) AS AVG_DAYS,
                   SUM(CASE WHEN DATE_DIFF('day', f.due_date, f.payment_date) > 0 THEN 1 ELSE 0 END) AS LATE_PAYMENTS,
                   COUNT(*) AS TOTAL_PAYMENTS
            FROM {base}
            WHERE f.payment_date IS NOT NULL AND f.payment_date >= DATE_ADD('month', -6, CURRENT_DATE) {flt}
            GROUP BY 1 ORDER BY 1
        """
        out["monthly_df"] = run_query(pm_sql)
        out["metrics"] = {"summary": "Avg days-to-pay and late payments."}
        out["sql"]["payment_performance"] = pm_sql

    elif key == "invoice_aging":
        aging_sql = f"""
            SELECT CASE WHEN f.aging_days <= 30 THEN '0-30 days'
                        WHEN f.aging_days <= 60 THEN '31-60 days'
                        WHEN f.aging_days <= 90 THEN '61-90 days'
                        ELSE '90+ days' END AS AGING_BUCKET,
                   COUNT(*) AS CNT, SUM(COALESCE(f.invoice_amount_local,0)) AS SPEND
            FROM {base}
            WHERE UPPER(f.invoice_status) IN ('OPEN','PENDING') AND f.aging_days IS NOT NULL {flt}
            GROUP BY 1 ORDER BY 1
        """
        out["vendors_df"] = run_query(aging_sql)
        out["metrics"] = {"summary": "Aging buckets for open invoices."}
        out["sql"]["invoice_aging"] = aging_sql

    return out

run_quick_analysis = run_quick_analysis_cached

# ---------------------------- Persistence (SQLite) with in-memory caching ----------------------------
DB_PATH = "procureiq.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS chat_sessions (
        session_id TEXT PRIMARY KEY, session_label TEXT, created_at TIMESTAMP, last_updated TIMESTAMP
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS chat_messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT, session_id TEXT, turn_index INTEGER, role TEXT, content TEXT,
        sql_used TEXT, source TEXT, timestamp TIMESTAMP, FOREIGN KEY(session_id) REFERENCES chat_sessions(session_id)
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS question_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT, normalized_query TEXT, query_text TEXT, user_name TEXT,
        analysis_type TEXT, asked_at TIMESTAMP
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS saved_insights (
        insight_id TEXT PRIMARY KEY, created_by TEXT, page TEXT, title TEXT, question TEXT,
        verified_query_name TEXT, created_at TIMESTAMP
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS query_cache (
        query_hash TEXT PRIMARY KEY, question TEXT, response_json TEXT, created_at TIMESTAMP,
        last_hit_at TIMESTAMP, hit_count INTEGER
    )''')
    conn.commit()
    conn.close()

init_db()

def get_current_user():
    return "user1"

# Cached DB reads
@st.cache_data(ttl=300)
def get_saved_insights_cached(page="genie", limit=20):
    user = get_current_user()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''SELECT insight_id, title, question, verified_query_name, created_at FROM saved_insights
                 WHERE page = ? AND created_by = ? ORDER BY created_at DESC LIMIT ?''', (page, user, limit))
    rows = c.fetchall()
    conn.close()
    return [{"id": row[0], "title": row[1], "question": row[2], "type": row[3], "created_at": row[4]} for row in rows]

@st.cache_data(ttl=300)
def get_frequent_questions_by_user_cached(limit=10):
    user = get_current_user()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''SELECT normalized_query, COUNT(*) as cnt FROM question_history
                 WHERE user_name = ? GROUP BY normalized_query ORDER BY cnt DESC LIMIT ?''', (user, limit))
    rows = c.fetchall()
    conn.close()
    return [{"query": row[0], "count": row[1]} for row in rows]

@st.cache_data(ttl=300)
def get_frequent_questions_all_cached(limit=10):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''SELECT normalized_query, COUNT(*) as cnt FROM question_history
                 GROUP BY normalized_query ORDER BY cnt DESC LIMIT ?''', (limit,))
    rows = c.fetchall()
    conn.close()
    return [{"query": row[0], "count": row[1]} for row in rows]

def save_chat_message(session_id, turn_index, role, content, sql_used="", source=""):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''INSERT INTO chat_messages (session_id, turn_index, role, content, sql_used, source, timestamp)
                 VALUES (?, ?, ?, ?, ?, ?, ?)''',
              (session_id, turn_index, role, content, sql_used, source, datetime.now()))
    conn.commit()
    conn.close()

def save_question(query, analysis_type):
    norm = query.lower().strip()
    user = get_current_user()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('INSERT INTO question_history (normalized_query, query_text, user_name, analysis_type, asked_at) VALUES (?, ?, ?, ?, ?)',
              (norm, query, user, analysis_type, datetime.now()))
    conn.commit()
    conn.close()
    get_frequent_questions_by_user_cached.clear()
    get_frequent_questions_all_cached.clear()

def save_insight(question, title, analysis_type="custom", page="genie"):
    insight_id = str(uuid.uuid4())
    user = get_current_user()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''INSERT INTO saved_insights (insight_id, created_by, page, title, question, verified_query_name, created_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?)''',
              (insight_id, user, page, title, question, analysis_type, datetime.now()))
    conn.commit()
    conn.close()
    get_saved_insights_cached.clear()

def get_cache(question):
    q_hash = hashlib.md5(question.lower().strip().encode()).hexdigest()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT response_json FROM query_cache WHERE query_hash = ?', (q_hash,))
    row = c.fetchone()
    conn.close()
    if row:
        return json.loads(row[0])
    return None

def set_cache(question, response):
    q_hash = hashlib.md5(question.lower().strip().encode()).hexdigest()
    serializable_response = make_json_serializable(response)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''INSERT OR REPLACE INTO query_cache (query_hash, question, response_json, created_at, last_hit_at, hit_count)
                 VALUES (?, ?, ?, ?, ?, COALESCE((SELECT hit_count+1 FROM query_cache WHERE query_hash=?), 1))''',
              (q_hash, question, json.dumps(serializable_response), datetime.now(), datetime.now(), q_hash))
    conn.commit()
    conn.close()

# ---------------------------- DASHBOARD PAGE ----------------------------
def render_dashboard():
    if "preset" not in st.session_state:
        st.session_state.preset = "Last 30 Days"
    if "date_range" not in st.session_state:
        st.session_state.date_range = compute_range_preset(st.session_state.preset)
    if "na_tab" not in st.session_state:
        st.session_state.na_tab = "Overdue"
    if "na_page" not in st.session_state:
        st.session_state.na_page = 0

    col_date, col_vendor, col_preset = st.columns([2, 2, 3])
    with col_date:
        date_range = st.date_input("Date Range", value=st.session_state.date_range, format="YYYY-MM-DD", label_visibility="collapsed")
        if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
            rng_start, rng_end = date_range
        else:
            rng_start, rng_end = st.session_state.date_range
        if (rng_start, rng_end) != st.session_state.date_range:
            st.session_state.date_range = (rng_start, rng_end)
            st.session_state.preset = "Custom"
            st.rerun()
    with col_vendor:
        vendor_cache_key = f"vendor_list_{rng_start}_{rng_end}"
        if vendor_cache_key not in st.session_state:
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
            st.session_state[vendor_cache_key] = vendor_list
        else:
            vendor_list = st.session_state[vendor_cache_key]
        selected_vendor = st.selectbox("Vendor", vendor_list, label_visibility="collapsed")
    with col_preset:
        presets = ["Last 30 Days", "QTD", "YTD", "Custom"]
        current_preset = st.session_state.preset
        p_cols = st.columns(4)
        for idx, p in enumerate(presets):
            with p_cols[idx]:
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
            COUNT(DISTINCT CASE WHEN UPPER(f.invoice_status) = 'OPEN' THEN f.purchase_order_reference END) AS active_pos,
            COUNT(DISTINCT f.purchase_order_reference) AS total_pos,
            COUNT(DISTINCT v.vendor_name) AS active_vendors,
            SUM(CASE WHEN UPPER(f.invoice_status) NOT IN ('CANCELLED','REJECTED') THEN COALESCE(f.invoice_amount_local,0) ELSE 0 END) AS total_spend,
            COUNT(DISTINCT CASE WHEN UPPER(f.invoice_status) = 'OPEN' THEN f.invoice_number END) AS pending_inv,
            AVG(CASE WHEN UPPER(f.invoice_status) = 'PAID' THEN DATE_DIFF('day', f.posting_date, f.payment_date) END) AS avg_processing_days
        FROM {DATABASE}.fact_all_sources_vw f
        LEFT JOIN {DATABASE}.dim_vendor_vw v ON f.vendor_id = v.vendor_id
        WHERE f.posting_date BETWEEN {start_lit} AND {end_lit}
        {vendor_where}
    """
    cur_df = run_query(cur_kpi_sql)
    cur_spend = safe_number(cur_df.loc[0,"total_spend"]) if not cur_df.empty else 0
    cur_active_pos = safe_int(cur_df.loc[0,"active_pos"]) if not cur_df.empty else 0
    cur_total_pos = safe_int(cur_df.loc[0,"total_pos"]) if not cur_df.empty else 0
    cur_active_vendors = safe_int(cur_df.loc[0,"active_vendors"]) if not cur_df.empty else 0
    cur_pending = safe_int(cur_df.loc[0,"pending_inv"]) if not cur_df.empty else 0
    cur_avg_processing = safe_number(cur_df.loc[0,"avg_processing_days"]) if not cur_df.empty else 0

    prev_kpi_sql = f"""
        SELECT
            COUNT(DISTINCT CASE WHEN UPPER(f.invoice_status) = 'OPEN' THEN f.purchase_order_reference END) AS active_pos,
            COUNT(DISTINCT f.purchase_order_reference) AS total_pos,
            COUNT(DISTINCT v.vendor_name) AS active_vendors,
            SUM(CASE WHEN UPPER(f.invoice_status) NOT IN ('CANCELLED','REJECTED') THEN COALESCE(f.invoice_amount_local,0) ELSE 0 END) AS total_spend,
            COUNT(DISTINCT CASE WHEN UPPER(f.invoice_status) = 'OPEN' THEN f.invoice_number END) AS pending_inv
        FROM {DATABASE}.fact_all_sources_vw f
        LEFT JOIN {DATABASE}.dim_vendor_vw v ON f.vendor_id = v.vendor_id
        WHERE f.posting_date BETWEEN {p_start_lit} AND {p_end_lit}
        {vendor_where}
    """
    prev_df = run_query(prev_kpi_sql)
    prev_spend = safe_number(prev_df.loc[0,"total_spend"]) if not prev_df.empty else 0
    prev_active_pos = safe_int(prev_df.loc[0,"active_pos"]) if not prev_df.empty else 0
    prev_total_pos = safe_int(prev_df.loc[0,"total_pos"]) if not prev_df.empty else 0
    prev_active_vendors = safe_int(prev_df.loc[0,"active_vendors"]) if not prev_df.empty else 0
    prev_pending = safe_int(prev_df.loc[0,"pending_inv"]) if not prev_df.empty else 0

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
    total_inv = safe_int(fp_df.loc[0,"total_inv"]) if not fp_df.empty else 0
    fp_inv = safe_int(fp_df.loc[0,"first_pass_inv"]) if not fp_df.empty else 0
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
    total_cleared = safe_int(auto_df.loc[0,"total_cleared"]) if not auto_df.empty else 0
    auto_proc = safe_int(auto_df.loc[0,"auto_processed"]) if not auto_df.empty else 0
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

    # Needs Attention
    st.subheader("Needs Attention")
    counts_sql = f"""
        SELECT
            SUM(CASE WHEN f.due_date < CURRENT_DATE AND UPPER(f.invoice_status) = 'OVERDUE' THEN 1 ELSE 0 END) AS overdue_count,
            SUM(CASE WHEN UPPER(f.invoice_status) IN ('DISPUTE','DISPUTED') THEN 1 ELSE 0 END) AS disputed_count,
            SUM(CASE WHEN f.due_date >= CURRENT_DATE AND f.due_date <= DATE_ADD('day', 30, CURRENT_DATE) AND UPPER(f.invoice_status) = 'OPEN' THEN 1 ELSE 0 END) AS due_count
        FROM {DATABASE}.fact_all_sources_vw f
        WHERE f.posting_date BETWEEN {start_lit} AND {end_lit}
        {vendor_where}
    """
    cnt_df = run_query(counts_sql)
    overdue_count = safe_int(cnt_df.loc[0,"overdue_count"]) if not cnt_df.empty else 0
    disputed_count = safe_int(cnt_df.loc[0,"disputed_count"]) if not cnt_df.empty else 0
    due_count = safe_int(cnt_df.loc[0,"due_count"]) if not cnt_df.empty else 0

    tab_cols = st.columns(3)
    with tab_cols[0]:
        if st.button(f"⚠️ Overdue ({overdue_count})", key="na_btn_overdue", use_container_width=True):
            st.session_state.na_tab = "Overdue"
            st.session_state.na_page = 0
            st.rerun()
    with tab_cols[1]:
        if st.button(f"⚖️ Disputed ({disputed_count})", key="na_btn_disputed", use_container_width=True):
            st.session_state.na_tab = "Disputed"
            st.session_state.na_page = 0
            st.rerun()
    with tab_cols[2]:
        if st.button(f"📅 Due Next 30 Days ({due_count})", key="na_btn_due", use_container_width=True):
            st.session_state.na_tab = "Due"
            st.session_state.na_page = 0
            st.rerun()

    if st.session_state.na_tab == "Overdue":
        attention_sql = f"""
            SELECT f.invoice_number, v.vendor_name, f.invoice_amount_local AS amount, f.due_date, f.aging_days
            FROM {DATABASE}.fact_all_sources_vw f
            LEFT JOIN {DATABASE}.dim_vendor_vw v ON f.vendor_id = v.vendor_id
            WHERE f.posting_date BETWEEN {start_lit} AND {end_lit}
              {vendor_where}
              AND f.due_date < CURRENT_DATE AND UPPER(f.invoice_status) = 'OVERDUE'
            ORDER BY f.due_date ASC
        """
    elif st.session_state.na_tab == "Disputed":
        attention_sql = f"""
            SELECT f.invoice_number, v.vendor_name, f.invoice_amount_local AS amount, f.due_date, f.aging_days
            FROM {DATABASE}.fact_all_sources_vw f
            LEFT JOIN {DATABASE}.dim_vendor_vw v ON f.vendor_id = v.vendor_id
            WHERE f.posting_date BETWEEN {start_lit} AND {end_lit}
              {vendor_where}
              AND UPPER(f.invoice_status) IN ('DISPUTE','DISPUTED')
            ORDER BY f.due_date ASC
        """
    else:
        attention_sql = f"""
            SELECT f.invoice_number, v.vendor_name, f.invoice_amount_local AS amount, f.due_date, f.aging_days
            FROM {DATABASE}.fact_all_sources_vw f
            LEFT JOIN {DATABASE}.dim_vendor_vw v ON f.vendor_id = v.vendor_id
            WHERE f.posting_date BETWEEN {start_lit} AND {end_lit}
              {vendor_where}
              AND f.due_date >= CURRENT_DATE AND f.due_date <= DATE_ADD('day', 30, CURRENT_DATE) AND UPPER(f.invoice_status) = 'OPEN'
            ORDER BY f.due_date ASC
        """
    attention_df = run_query(attention_sql)
    if not attention_df.empty:
        items_per_page = 8
        total_items = len(attention_df)
        total_pages = (total_items - 1) // items_per_page + 1
        start_idx = st.session_state.na_page * items_per_page
        end_idx = min(start_idx + items_per_page, total_items)
        page_df = attention_df.iloc[start_idx:end_idx]
        rows = [page_df.iloc[i:i+4] for i in range(0, len(page_df), 4)]
        for row in rows:
            cols = st.columns(4)
            for col, (_, row_data) in zip(cols, row.iterrows()):
                with col:
                    inv_num_raw = row_data['invoice_number']
                    inv_num_clean = clean_invoice_number(inv_num_raw)
                    vendor = row_data['vendor_name']
                    amount = row_data['amount']
                    due_date = row_data['due_date']
                    st.markdown(f"""
                        <div style="border:1px solid #e5e7eb; border-radius:12px; padding:12px; margin-bottom:12px;">
                            <div style="font-weight:bold">{vendor}</div>
                            <div style="font-size:0.9rem">{abbr_currency(amount)}</div>
                            <div style="font-size:0.8rem; color:#666">Due: {due_date}</div>
                        </div>
                    """, unsafe_allow_html=True)
                    if st.button(f"View Invoice {inv_num_clean}", key=f"na_card_{inv_num_clean}"):
                        st.session_state.page = "Invoices"
                        st.session_state.invoice_search_term = inv_num_clean
                        st.rerun()
        col_prev, col_info, col_next = st.columns([1,2,1])
        with col_prev:
            if st.button("← Prev", disabled=(st.session_state.na_page == 0)):
                st.session_state.na_page -= 1
                st.rerun()
        with col_info:
            st.markdown(f"<div style='text-align:center'>Page {st.session_state.na_page+1} of {total_pages}</div>", unsafe_allow_html=True)
        with col_next:
            if st.button("Next →", disabled=(st.session_state.na_page >= total_pages-1)):
                st.session_state.na_page += 1
                st.rerun()
    else:
        st.info("No attention items found.")
    st.markdown("---")

    # Analytics charts
    st.subheader("Analytics")
    chart_col1, chart_col2, chart_col3 = st.columns(3)

    with chart_col1:
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
            alt_donut_status(status_df, label_col="status", value_col="cnt", title="Invoice Status", height=300)
        else:
            st.info("No status data")

    with chart_col2:
        top_vendors_sql = f"""
            SELECT v.vendor_name, SUM(COALESCE(f.invoice_amount_local,0)) AS spend
            FROM {DATABASE}.fact_all_sources_vw f
            LEFT JOIN {DATABASE}.dim_vendor_vw v ON f.vendor_id = v.vendor_id
            WHERE f.posting_date BETWEEN {start_lit} AND {end_lit}
            {vendor_where}
            GROUP BY 1 ORDER BY spend DESC LIMIT 10
        """
        top_df = run_query(top_vendors_sql)
        if not top_df.empty:
            alt_bar(top_df, x="vendor_name", y="spend", title="Top 10 Vendors by Spend", horizontal=True, height=300)
        else:
            st.info("No vendor data")

    with chart_col3:
        trend_sql = f"""
            SELECT
                DATE_TRUNC('month', posting_date) AS month,
                SUM(COALESCE(invoice_amount_local,0)) AS spend
            FROM {DATABASE}.fact_all_sources_vw
            WHERE posting_date >= DATE_ADD('month', -12, {end_lit})
              AND UPPER(invoice_status) NOT IN ('CANCELLED','REJECTED')
            GROUP BY 1 ORDER BY 1
        """
        trend_df = run_query(trend_sql)
        if not trend_df.empty:
            trend_df['month_str'] = pd.to_datetime(trend_df['month']).dt.strftime('%b %Y')
            alt_line_monthly(trend_df.rename(columns={'month_str':'MONTH', 'spend':'VALUE'}), month_col='MONTH', value_col='VALUE', height=300, title="Monthly Spend Trend")
        else:
            st.info("No trend data")

# ---------------------------- GENIE PAGE ----------------------------
def process_custom_query(query: str) -> dict:
    sql, _ = generate_sql(query)
    if not sql or not is_safe_sql(sql):
        return {"layout": "error", "message": "Failed to generate valid SQL."}
    sql = ensure_limit(sql)
    df = run_query(sql)
    if df.empty:
        return {"layout": "error", "message": "Query returned no data."}
    return {"layout": "sql", "sql": sql, "df": df.to_dict(orient="records"), "question": query}

def render_genie():
    st.markdown("""
    <style>
    .kpi-card { background: #fff; border: 1px solid #e6e8ee; border-radius: 12px; padding: 12px; }
    .kpi-title { font-size: 12px; color: #64748b; font-weight: 800; }
    .kpi-value { font-size: 28px; font-weight: 900; margin-top: 6px; }
    .chat-message-user { background: #1459d2; color: white; padding: 10px 14px; border-radius: 16px; margin: 6px 0; }
    .chat-message-assistant { background: #f1f5f9; color: #0f172a; padding: 10px 14px; border-radius: 16px; margin: 6px 0; }
    .cache-badge { background: #eff6ff; color: #1d4ed8; border-radius: 999px; font-size: 11px; padding: 2px 9px; display: inline-block; margin-bottom: 4px; }
    .chat-scrollable { max-height: 400px; overflow-y: auto; padding-right: 8px; }
    .anomaly-banner { background: #fffbeb; border-left: 4px solid #f59e0b; border-radius: 8px; padding: 10px 16px; margin-bottom: 16px; font-size: 14px; }
    </style>
    """, unsafe_allow_html=True)

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

    auto_query = st.session_state.pop("auto_run_query", None)
    if auto_query:
        st.session_state.selected_analysis = "custom"
        st.session_state.last_custom_query = auto_query
        with st.spinner("Running query..."):
            result = process_custom_query(auto_query)
            st.session_state.genie_response = result
            st.session_state.genie_messages.append({"role": "user", "content": auto_query, "timestamp": datetime.now()})
            if result.get("layout") == "sql":
                st.session_state.genie_messages.append({"role": "assistant", "content": "Query executed.", "response": result, "timestamp": datetime.now()})
                save_chat_message(st.session_state.genie_session_id, st.session_state.genie_turn_index, "user", auto_query)
                st.session_state.genie_turn_index += 1
                save_chat_message(st.session_state.genie_session_id, st.session_state.genie_turn_index, "assistant", "Query executed.", sql_used=result.get("sql", ""))
                st.session_state.genie_turn_index += 1
                save_question(auto_query, "custom")
                set_cache(auto_query, result)
            else:
                st.session_state.genie_messages.append({"role": "assistant", "content": result.get("message", "Error"), "timestamp": datetime.now()})
        st.rerun()

    # Quick analysis tiles
    st.markdown("## Welcome to ProcureIQ Genie")
    st.markdown("Let Genie run one of these quick analyses for you.")
    cols = st.columns(4)
    quick_options = {
        "spending_overview": ("Spending Overview", "Track total spend, monthly trends and major changes"),
        "vendor_analysis": ("Vendor Analysis", "Understand vendor-wise spend, concentration, and dependency"),
        "payment_performance": ("Payment Performance", "Identify delays, late payments, and cycle time issues"),
        "invoice_aging": ("Invoice Aging", "See overdue invoices, risk buckets, and problem areas")
    }
    for idx, (key, (title, desc)) in enumerate(quick_options.items()):
        with cols[idx]:
            with st.container(border=True):
                st.markdown(f"**{title}**")
                st.caption(desc)
                if st.button(f"Ask Genie", key=f"quick_{key}", use_container_width=True):
                    st.session_state.genie_messages = []
                    st.session_state.genie_turn_index = 0
                    st.session_state.selected_analysis = key
                    st.session_state.last_custom_query = title
                    with st.spinner(f"Running {title}..."):
                        result = run_quick_analysis(key)
                        st.session_state.genie_response = result
                        st.session_state.genie_messages.append({"role": "user", "content": title, "timestamp": datetime.now()})
                        st.session_state.genie_messages.append({"role": "assistant", "content": f"Analysis for {title} complete.", "response": result, "timestamp": datetime.now()})
                        save_chat_message(st.session_state.genie_session_id, st.session_state.genie_turn_index, "user", title)
                        st.session_state.genie_turn_index += 1
                        save_chat_message(st.session_state.genie_session_id, st.session_state.genie_turn_index, "assistant", "Analysis complete.", source="quick")
                        st.session_state.genie_turn_index += 1
                        save_question(title, key)
                    st.rerun()
    st.markdown("---")

    left_col, right_col = st.columns([0.35, 0.65], gap="medium")

    with left_col:
        with st.expander("Saved insights", expanded=False):
            insights = get_saved_insights_cached(page="genie")
            if insights:
                for ins in insights:
                    if st.button(ins["title"], key=f"insight_{ins['id']}", use_container_width=True):
                        st.session_state.selected_analysis = "custom"
                        st.session_state.last_custom_query = ins["question"]
                        with st.spinner("Running saved insight..."):
                            result = process_custom_query(ins["question"])
                            st.session_state.genie_response = result
                            st.session_state.genie_messages.append({"role": "user", "content": ins["question"], "timestamp": datetime.now()})
                            st.session_state.genie_messages.append({"role": "assistant", "content": "Query executed.", "response": result, "timestamp": datetime.now()})
                            save_chat_message(st.session_state.genie_session_id, st.session_state.genie_turn_index, "user", ins["question"])
                            st.session_state.genie_turn_index += 1
                            save_chat_message(st.session_state.genie_session_id, st.session_state.genie_turn_index, "assistant", "Query executed.", sql_used=result.get("sql", ""))
                            st.session_state.genie_turn_index += 1
                            save_question(ins["question"], "custom")
                            set_cache(ins["question"], result)
                        st.rerun()
            else:
                st.caption("Save any Genie answer to see it here.")

        with st.expander("Frequently asked by you", expanded=False):
            faqs = get_frequent_questions_by_user_cached(5)
            if faqs:
                for faq in faqs:
                    if st.button(f"{faq['query'][:50]} ({faq['count']})", key=f"faq_user_{faq['query']}", use_container_width=True):
                        st.session_state.selected_analysis = "custom"
                        st.session_state.last_custom_query = faq["query"]
                        with st.spinner("Running..."):
                            result = process_custom_query(faq["query"])
                            st.session_state.genie_response = result
                            st.session_state.genie_messages.append({"role": "user", "content": faq["query"], "timestamp": datetime.now()})
                            st.session_state.genie_messages.append({"role": "assistant", "content": "Query executed.", "response": result, "timestamp": datetime.now()})
                            save_chat_message(st.session_state.genie_session_id, st.session_state.genie_turn_index, "user", faq["query"])
                            st.session_state.genie_turn_index += 1
                            save_chat_message(st.session_state.genie_session_id, st.session_state.genie_turn_index, "assistant", "Query executed.", sql_used=result.get("sql", ""))
                            st.session_state.genie_turn_index += 1
                            save_question(faq["query"], "custom")
                            set_cache(faq["query"], result)
                        st.rerun()
            else:
                st.caption("Your frequent questions will appear here.")

        with st.expander("Most frequent (all)", expanded=False):
            all_faqs = get_frequent_questions_all_cached(5)
            if all_faqs:
                for faq in all_faqs:
                    st.button(f"{faq['query'][:50]} ({faq['count']})", key=f"faq_all_{faq['query']}", use_container_width=True, disabled=True)
            else:
                st.caption("No questions yet.")

    with right_col:
        st.markdown("### AI Assistant")
        st.markdown('<div class="chat-scrollable">', unsafe_allow_html=True)
        for msg in st.session_state.genie_messages:
            if msg["role"] == "user":
                st.markdown(f'<div class="chat-message-user"><strong>You</strong><br/>{html.escape(msg["content"])}</div>', unsafe_allow_html=True)
            else:
                st.markdown(f'<div class="chat-message-assistant"><strong>Genie</strong><br/>{html.escape(msg["content"])}</div>', unsafe_allow_html=True)
                if "response" in msg and msg["response"]:
                    resp = msg["response"]
                    if resp.get("layout") == "quick":
                        metrics = resp.get("metrics", {})
                        if metrics:
                            metric_cols = st.columns(len(metrics))
                            for i, (k, v) in enumerate(metrics.items()):
                                with metric_cols[i]:
                                    if k == "total_ytd":
                                        st.metric("Total Spend (YTD)", abbr_currency(v))
                                    elif k == "mom_pct":
                                        st.metric("MoM Change", _safe_pct_str(v))
                                    elif k == "qoq_pct":
                                        st.metric("QoQ Change", _safe_pct_str(v))
                                    elif k == "top5_pct":
                                        st.metric("Top 5 Vendors", f"{v:.0f}% of total spend")
                                    else:
                                        st.metric(k.replace("_"," ").title(), str(v))

                        anomaly = resp.get("anomaly")
                        if anomaly:
                            st.markdown(f'<div class="anomaly-banner">⚠️ <strong>Anomaly Detected</strong><br/>{html.escape(anomaly)}</div>', unsafe_allow_html=True)

                        monthly_df = resp.get("monthly_df")
                        if monthly_df is not None and not monthly_df.empty and "MONTHLY_SPEND" in monthly_df.columns:
                            st.subheader("Spending Trends")
                            alt_line_monthly(monthly_df.rename(columns={"MONTH":"MONTH", "MONTHLY_SPEND":"VALUE"}), month_col="MONTH", value_col="VALUE", height=300, title="Monthly Spend Trend (Last 12 Months)")

                        if monthly_df is not None and not monthly_df.empty and "INVOICE_COUNT" in monthly_df.columns:
                            st.subheader("Invoice volume by month")
                            alt_bar(monthly_df, x="MONTH", y="INVOICE_COUNT", color="#1e88e5", height=250)

                        if monthly_df is not None and not monthly_df.empty and "VENDOR_COUNT" in monthly_df.columns:
                            st.subheader("Active vendors by month")
                            alt_bar(monthly_df, x="MONTH", y="VENDOR_COUNT", color="#7c3aed", height=250)

                        vendors_df = resp.get("vendors_df")
                        if vendors_df is not None and not vendors_df.empty and "VENDOR_NAME" in vendors_df.columns and "SPEND" in vendors_df.columns:
                            st.subheader("Top 10 Vendors by Spend (YTD)")
                            alt_bar(vendors_df.head(10), x="VENDOR_NAME", y="SPEND", horizontal=True, height=400)

                        st.subheader("Prescriptive — Recommendations & next steps")
                        prescriptive_text = generate_prescriptive_from_quick(resp)
                        if prescriptive_text:
                            st.markdown(f'<div style="font-size:14px; line-height:1.6;">{prescriptive_text}</div>', unsafe_allow_html=True)
                        else:
                            st.info("No prescriptive insights available.")

                        with st.expander("Query outputs"):
                            st.caption("Show full result tables")
                            if monthly_df is not None and not monthly_df.empty:
                                st.dataframe(monthly_df, use_container_width=True)
                            if vendors_df is not None and not vendors_df.empty:
                                st.dataframe(vendors_df, use_container_width=True)
                        with st.expander("Show SQL used"):
                            sql_dict = resp.get("sql", {})
                            for name, sql_text in sql_dict.items():
                                st.markdown(f"**{name}**")
                                st.code(sql_text, language="sql")

                    elif resp.get("layout") == "sql":
                        df = pd.DataFrame(resp["df"])
                        st.dataframe(df, use_container_width=True)
                        chart = auto_chart(df)
                        if chart:
                            st.altair_chart(chart, use_container_width=True)
                        with st.expander("View SQL"):
                            st.code(resp["sql"], language="sql")
                    elif resp.get("layout") == "error":
                        st.error(resp.get("message", "Unknown error"))
        st.markdown('</div>', unsafe_allow_html=True)

        with st.form(key="genie_form", clear_on_submit=True):
            col_input, col_btn = st.columns([0.85, 0.15])
            with col_input:
                user_question = st.text_input("Ask a question", placeholder="e.g., Show me total spend YTD", label_visibility="collapsed")
            with col_btn:
                submitted = st.form_submit_button("Send", type="primary")
            if submitted and user_question:
                with st.spinner("Generating SQL..."):
                    cached = get_cache(user_question)
                    if cached:
                        st.session_state.genie_response = cached
                        st.session_state.genie_messages.append({"role": "user", "content": user_question, "timestamp": datetime.now()})
                        st.session_state.genie_messages.append({"role": "assistant", "content": "Answer from cache.", "response": cached, "timestamp": datetime.now()})
                        save_chat_message(st.session_state.genie_session_id, st.session_state.genie_turn_index, "user", user_question)
                        st.session_state.genie_turn_index += 1
                        save_chat_message(st.session_state.genie_session_id, st.session_state.genie_turn_index, "assistant", "Answer from cache.", source="cache")
                        st.session_state.genie_turn_index += 1
                        save_question(user_question, "custom")
                    else:
                        result = process_custom_query(user_question)
                        if result.get("layout") == "sql":
                            set_cache(user_question, result)
                            st.session_state.genie_response = result
                            st.session_state.genie_messages.append({"role": "user", "content": user_question, "timestamp": datetime.now()})
                            st.session_state.genie_messages.append({"role": "assistant", "content": "Query executed.", "response": result, "timestamp": datetime.now()})
                            save_chat_message(st.session_state.genie_session_id, st.session_state.genie_turn_index, "user", user_question)
                            st.session_state.genie_turn_index += 1
                            save_chat_message(st.session_state.genie_session_id, st.session_state.genie_turn_index, "assistant", "Query executed.", sql_used=result.get("sql", ""))
                            st.session_state.genie_turn_index += 1
                            save_question(user_question, "custom")
                        else:
                            st.error(result.get("message", "Query failed"))
                st.rerun()

def generate_prescriptive_from_quick(resp: dict) -> str:
    insights = []
    metrics = resp.get("metrics", {})
    total_ytd = metrics.get("total_ytd", 0)
    mom_pct = metrics.get("mom_pct", 0)
    qoq_pct = metrics.get("qoq_pct", 0)
    top5_pct = metrics.get("top5_pct", 0)

    if total_ytd:
        insights.append(f"• Total Spend YTD: {abbr_currency(total_ytd)}. Action: Review and optimize procurement processes to reduce costs.")
    if mom_pct != 0:
        trend = "decrease" if mom_pct < 0 else "increase"
        insights.append(f"• Monthly spend has {trend} by {abs(mom_pct):.1f}% MoM. Action: Analyze root causes and adjust procurement strategies.")
    if qoq_pct != 0:
        trend = "decrease" if qoq_pct < 0 else "increase"
        insights.append(f"• Quarterly spend {trend} by {abs(qoq_pct):.1f}% QoQ. Action: Review category-level performance.")
    if top5_pct:
        insights.append(f"• Top 5 vendors account for {top5_pct:.0f}% of spend. Action: Negotiate volume discounts and consider consolidation.")

    anomaly = resp.get("anomaly")
    if anomaly:
        insights.append(f"• Anomaly: {anomaly[:100]}... Action: Investigate cause and prevent recurrence.")

    if not insights:
        return "No specific prescriptive insights available based on the data."
    return "<br/>".join(insights[:6])

# ---------------------------- FORECAST PAGE ----------------------------
def render_forecast():
    st.subheader("Cash Flow Need Forecast")

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
        st.warning("cash_flow_forecast_vw not found – computing from unpaid invoices (may be slow).")
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

        st.markdown("**Obligations by time bucket**")
        st.dataframe(cf_df, use_container_width=True)
        csv = cf_df.to_csv(index=False).encode('utf-8')
        st.download_button("Download forecast (CSV)", data=csv, file_name="cash_flow_forecast.csv", mime="text/csv")

        chart_df = cf_df[~cf_df["forecast_bucket"].isin(["TOTAL_UNPAID", "PROCESSING_LAG_DAYS"])].copy()
        if not chart_df.empty:
            st.markdown("**Forecast Distribution**")
            chart = alt.Chart(chart_df).mark_bar(color="#10b981").encode(
                x=alt.X("forecast_bucket:N", sort=None, axis=alt.Axis(title=None, labelAngle=-30)),
                y=alt.Y("total_amount:Q", axis=alt.Axis(title="Amount", format="~s")),
                tooltip=["forecast_bucket", "total_amount"]
            ).properties(height=300)
            st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No cash flow forecast data available.")

    # Action Playbook
    st.markdown("---")
    st.markdown("### Action Playbook")
    st.markdown("Use these guided analyses to turn the forecast into decisions: who to pay now, who to pay early, and where we are at risk of paying late.")
    actions = [
        ("📊 Forecast cash outflow (7–90 days)", "Forecast cash outflow for the next 7, 14, 30, 60, and 90 days"),
        ("💰 Invoices to pay early to capture discounts", "Which invoices should we pay early to capture discounts?"),
        ("⏰ Optimal payment timing for this week", "What is the optimal payment timing strategy for this week?"),
        ("⚠️ Late payment trend and risk", "Show late payment trend for forecasting")
    ]
    for label, question in actions:
        if st.button(label, use_container_width=True):
            st.session_state.auto_run_query = question
            st.session_state.page = "Genie"
            st.rerun()

    # GR/IR Reconciliation
    st.markdown("---")
    st.subheader("GR/IR Reconciliation")
    tab1, tab2 = st.tabs(["Outstanding Balance", "Aging Analysis"])

    with tab1:
        grir_summary_sql = f"""
            WITH latest AS (
                SELECT year, month, invoice_count, total_grir_blnc
                FROM {DATABASE}.gr_ir_outstanding_balance_vw
                ORDER BY year DESC, month DESC
                LIMIT 1
            )
            SELECT year, month, invoice_count AS grir_items, total_grir_blnc AS total_grir_balance
            FROM latest
        """
        grir_df = run_query(grir_summary_sql)
        if not grir_df.empty:
            row = grir_df.iloc[0]
            total_grir = safe_number(row.get("total_grir_balance", 0))
            grir_items = safe_int(row.get("grir_items", 0))
            col_a, col_b = st.columns(2)
            col_a.metric("TOTAL GR/IR", abbr_currency(total_grir))
            col_b.metric("OUTSTANDING ITEMS", f"{grir_items:,}")
            st.dataframe(grir_df, use_container_width=True)
        else:
            st.info("No GR/IR outstanding data found.")

    with tab2:
        aging_sql = f"""
            SELECT year, month, age_days, total_grir_balance, grir_over_30, grir_over_60, grir_over_90
            FROM {DATABASE}.gr_ir_aging_vw
            ORDER BY year DESC, month DESC, age_days
            LIMIT 50
        """
        aging_df = run_query(aging_sql)
        if not aging_df.empty:
            over_60 = safe_number(aging_df["grir_over_60"].iloc[0] if not aging_df.empty else 0)
            st.metric(">60 DAYS GR/IR", abbr_currency(over_60))
            st.dataframe(aging_df, use_container_width=True)
        else:
            st.info("No GR/IR aging data found.")

# ---------------------------- INVOICES PAGE ----------------------------
def _get_ai_invoice_suggestion(invoice_number: str, inv_row: dict, status_history: str = "") -> str:
    """Use Bedrock Nova to generate a short, actionable suggestion for the selected invoice."""
    status = str(inv_row.get("invoice_status", "")).strip()
    due = inv_row.get("due_date")
    aging = inv_row.get("aging_days")
    amount = inv_row.get("invoice_amount")
    due_str = str(due) if due else "unknown"
    aging_str = f"{int(aging)} days" if aging is not None else "unknown"
    amount_str = f"{float(amount):,.2f}" if amount is not None else "unknown"

    is_overdue = False
    try:
        if due and status.upper() not in ("PAID", "CLEARED"):
            due_date = date.fromisoformat(str(due)[:10])
            is_overdue = due_date < date.today()
    except Exception:
        pass

    overdue_context = ""
    if is_overdue:
        overdue_context = f"This invoice IS overdue (due date {due_str} has passed and it is not yet paid). "
    elif status.upper() in ("PAID", "CLEARED"):
        overdue_context = "This invoice is already PAID/CLEARED. It is NOT overdue. "
    else:
        overdue_context = "This invoice is NOT overdue (the due date has not passed yet). "

    prompt = (
        "Concise procure-to-pay analyst. 2-3 sentences of actionable advice based ONLY on the data below. "
        f"{overdue_context}"
        "OPEN & not overdue: say proceed to pay. Overdue: recommend immediate review. PAID: no action. "
        f"Invoice: {invoice_number}. Status: {status}. Due: {due_str}. Aging: {aging_str}. Amount: {amount_str}."
    )
    # Use Bedrock Nova via cached function
    response = ask_bedrock_cached(prompt, system_prompt="You are a helpful procurement analyst. Provide concise, actionable advice.")
    if response and len(response.strip()) > 10:
        return response.strip()
    # Fallback
    if status.upper() in ("PAID", "CLEARED"):
        return f"Invoice {invoice_number} has already been **paid**. No further action is needed."
    elif is_overdue:
        return f"Invoice {invoice_number} is **overdue** (due {due_str}). Recommend **immediate review** to avoid penalties."
    else:
        return f"Invoice {invoice_number} is {status.lower()} with due date {due_str}. Proceed to pay."

def render_invoices():
    st.subheader("Invoices")
    st.markdown("Search, track and manage all invoices in one place")

    if "invoice_search_term" not in st.session_state:
        st.session_state.invoice_search_term = ""

    prefill = st.session_state.pop("invoice_search_term", None)
    if prefill:
        st.session_state.inv_search_q = clean_invoice_number(prefill)

    search_term = st.session_state.get("inv_search_q", "")

    col1, col2 = st.columns([3,1])
    with col1:
        user_search = st.text_input(
            "Search by Invoice or PO Number",
            value=search_term,
            placeholder="e.g., 9001767",
            label_visibility="collapsed",
            key="inv_search_input"
        )
    with col2:
        if st.button("Reset", key="btn_inv_reset"):
            st.session_state.inv_search_q = ""
            st.session_state.invoice_search_term = ""
            st.session_state.invoice_status_filter = "All Status"
            st.rerun()

    if user_search != search_term:
        st.session_state.inv_search_q = user_search
        st.rerun()

    col_vendor, col_status = st.columns(2)
    with col_vendor:
        if "inv_vendor_list" not in st.session_state:
            vendor_df = run_query(f"SELECT DISTINCT vendor_name FROM {DATABASE}.dim_vendor_vw ORDER BY vendor_name")
            vendor_list = ["All Vendors"] + vendor_df["vendor_name"].tolist() if not vendor_df.empty else ["All Vendors"]
            st.session_state.inv_vendor_list = vendor_list
        selected_vendor = st.selectbox("Vendor", st.session_state.inv_vendor_list, key="inv_sel_vendor")
    with col_status:
        status_options = ["All Status", "OPEN", "PAID", "DISPUTED", "OVERDUE", "DUE_NEXT_30"]
        selected_status_display = st.selectbox(
            "Status", status_options,
            index=status_options.index(st.session_state.get("invoice_status_filter", "All Status")) if st.session_state.get("invoice_status_filter", "All Status") in status_options else 0,
            key="inv_sel_status"
        )
        selected_status = selected_status_display
        if selected_status == "DUE_NEXT_30":
            selected_status = "OPEN"

    where = []
    if user_search:
        clean_search = clean_invoice_number(user_search)
        where.append(f"CAST(f.invoice_number AS VARCHAR) = '{clean_search}'")
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

        if user_search and len(df) == 1:
            inv_num = clean_invoice_number(df.iloc[0,0])
            st.markdown("---")
            st.subheader(f"Invoice Details: {inv_num}")

            details_sql = f"""
                SELECT
                    f.invoice_number,
                    f.posting_date AS invoice_date,
                    f.invoice_amount_local AS invoice_amount,
                    f.purchase_order_reference AS po_number,
                    f.po_amount AS po_amount,
                    f.due_date,
                    f.invoice_status,
                    f.company_code,
                    f.fiscal_year,
                    f.aging_days
                FROM {DATABASE}.fact_all_sources_vw f
                WHERE CAST(f.invoice_number AS VARCHAR) = '{inv_num}'
                LIMIT 1
            """
            details_df = run_query(details_sql)
            if not details_df.empty:
                st.dataframe(details_df, use_container_width=True)

            hist_sql = f"""
                SELECT
                    invoice_number,
                    UPPER(status) AS status,
                    effective_date,
                    status_notes
                FROM {DATABASE}.invoice_status_history_vw
                WHERE CAST(invoice_number AS VARCHAR) = '{inv_num}'
                ORDER BY sequence_nbr
            """
            hist_df = run_query(hist_sql)
            if not hist_df.empty:
                st.subheader("Status History")
                st.dataframe(hist_df, use_container_width=True)

            vendor_sql = f"""
                SELECT DISTINCT
                    v.vendor_id,
                    v.vendor_name,
                    v.vendor_name_2,
                    v.country_code,
                    v.city,
                    v.postal_code,
                    v.street,
                    v.region_code,
                    v.industry_sector,
                    v.vendor_account_group,
                    v.tax_number_1,
                    v.tax_number_2,
                    v.deletion_flag,
                    v.posting_block,
                    v.system
                FROM {DATABASE}.fact_all_sources_vw f
                LEFT JOIN {DATABASE}.dim_vendor_vw v ON f.vendor_id = v.vendor_id
                WHERE CAST(f.invoice_number AS VARCHAR) = '{inv_num}'
                LIMIT 1
            """
            vendor_df = run_query(vendor_sql)
            if not vendor_df.empty:
                st.subheader("Vendor Info")
                st.dataframe(vendor_df, use_container_width=True)

            company_sql = f"""
                SELECT DISTINCT
                    f.company_code,
                    cc.company_name,
                    f.plant_code,
                    plt.plant_name
                FROM {DATABASE}.fact_all_sources_vw f
                LEFT JOIN {DATABASE}.dim_company_code_vw cc ON f.company_code = cc.company_code
                LEFT JOIN {DATABASE}.dim_plant_vw plt ON f.plant_code = plt.plant_code
                WHERE CAST(f.invoice_number AS VARCHAR) = '{inv_num}'
                LIMIT 1
            """
            company_df = run_query(company_sql)
            if not company_df.empty:
                st.subheader("Company Info")
                st.dataframe(company_df, use_container_width=True)

            st.subheader("Genie insights")
            inv_row = details_df.iloc[0].to_dict() if not details_df.empty else {}
            status_history = hist_df[["status", "effective_date", "status_notes"]].head(5).to_string(index=False) if not hist_df.empty else ""
            suggestion = _get_ai_invoice_suggestion(inv_num, inv_row, status_history)
            st.markdown(f'<div style="background:#f0f9ff; border-left:4px solid #1459d2; padding:12px; border-radius:8px;">{suggestion}</div>', unsafe_allow_html=True)
    else:
        st.info("No invoices found. Try a different search term.")

# ---------------------------- Main App Layout ----------------------------
st.markdown("""
<style>
.kpi { background: #fff; border: 1px solid #e6e8ee; border-radius: 12px; padding: 12px 14px; box-shadow: 0 2px 10px rgba(2,8,23,.06); }
.kpi .title { font-size: 12px; color: #64748b; font-weight: 800; }
.kpi .value { font-size: 28px; font-weight: 900; margin-top: 6px; }
.kpi .delta { margin-top: 4px; font-weight: 900; display: flex; align-items: center; gap: 6px; }
.kpi .delta.up { color: #118d57; }
.kpi .delta.down { color: #d32f2f; }
</style>
""", unsafe_allow_html=True)

logo_url = "https://th.bing.com/th/id/OIP.Vy1yFQtg8-D1SsAxcqqtSgHaE6?w=235&h=180&c=7&r=0&o=7&dpr=1.5&pid=1.7&rm=3"
col_title, col_nav, col_logo = st.columns([1, 3, 1])
with col_title:
    st.markdown("<h1 style='font-weight: bold; margin-bottom: 0;'>ProcureIQ</h1>", unsafe_allow_html=True)
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
st.markdown("<p style='font-size: 0.9rem; color: gray; margin-top: -0.5rem;'>P2P Analytics</p>", unsafe_allow_html=True)
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
