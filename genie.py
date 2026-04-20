# genie.py
import streamlit as st
import pandas as pd
import json
import html
import uuid
from datetime import datetime
from athena_client import run_query
from bedrock_client import ask_bedrock
from utils import abbr_currency, auto_chart, alt_line_monthly, alt_bar, alt_donut_status, ensure_limit, is_safe_sql
from semantic_model import SYSTEM_PROMPT, DESCRIPTIVE_PROMPT_TEMPLATE, generate_sql
from persistence import get_saved_insights_cached, get_frequent_questions_by_user_cached, get_frequent_questions_all_cached, save_chat_message, save_question, set_cache, get_cache
from quick_analysis import run_quick_analysis
from config import DATABASE

# ------------------------------------------------------------
# Cash Flow Forecast handler (for Forecast tab buttons)
# ------------------------------------------------------------
def process_cash_flow_forecast(question: str) -> dict:
    """Run cash flow forecast query and return rich response."""
    # Try the dedicated view first
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
        # Fallback query (same as forecast.py)
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

    # Convert columns to lowercase for consistency
    cf_df.columns = [c.lower() for c in cf_df.columns]
    # Ensure required columns exist
    required = ['forecast_bucket', 'total_amount', 'invoice_count']
    for col in required:
        if col not in cf_df.columns:
            cf_df[col] = 0

    # Prepare data for insights
    data_preview = cf_df.to_string(index=False)
    # Generate AI insights using Bedrock
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

def render_cash_flow_response(result: dict):
    """Render cash flow forecast response with metrics, chart, table, and insights."""
    df = pd.DataFrame(result["df"])
    if df.empty:
        st.error("No cash flow data to display.")
        return

    # Metrics: total unpaid, overdue now, due next 30 days
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

    # Bar chart of total_amount per bucket (exclude TOTAL_UNPAID for clarity)
    chart_df = df[df["forecast_bucket"] != "TOTAL_UNPAID"].copy()
    if not chart_df.empty:
        st.subheader("Cash Outflow by Time Bucket")
        alt_bar(chart_df, x="forecast_bucket", y="total_amount", horizontal=True, height=300, color="#3b82f6")

    # Show data table
    st.subheader("Forecast Details")
    st.dataframe(df, use_container_width=True, hide_index=True)

    # AI insights
    if result.get("analyst_response"):
        st.markdown("### 💡 Key Insights")
        st.markdown(result["analyst_response"])

    # Expandable SQL
    with st.expander("View SQL used"):
        st.code(result["sql"], language="sql")

def render_quick_analysis_response(result: dict):
    """Render rich, type‑specific response with colorful styling."""
    analysis_type = result.get("type", "spending_overview")
    metrics = result.get("metrics", {})
    anomaly = result.get("anomaly")
    monthly_df = result.get("monthly_df")
    vendors_df = result.get("vendors_df")
    extra_dfs = result.get("extra_dfs", {})

    # Metrics row with colorful metric cards
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
                <div style="background-color: {colors[i % len(colors)]}; border-radius: 12px; padding: 12px; text-align: center; box-shadow: 0 2px 4px rgba(0,0,0,0.05);">
                    <div style="font-size: 0.85rem; color: #4b5563;">{label}</div>
                    <div style="font-size: 1.5rem; font-weight: 700; color: #1f2937;">{display}</div>
                </div>
                """, unsafe_allow_html=True)

    # Anomaly warning with gradient background
    if anomaly:
        st.markdown(f"""
        <div style="background: linear-gradient(135deg, #fef3c7 0%, #fde68a 100%); border-left: 6px solid #d97706; border-radius: 12px; padding: 12px 16px; margin: 16px 0;">
            <strong style="color: #92400e;">⚠️ Anomaly Detected</strong><br/>
            <span style="color: #78350f;">{anomaly}</span>
        </div>
        """, unsafe_allow_html=True)

    # Type‑specific charts
    if analysis_type == "spending_overview":
        if monthly_df is not None and not monthly_df.empty:
            monthly_df.columns = [c.upper() for c in monthly_df.columns]
            if "MONTH" in monthly_df.columns:
                monthly_df = monthly_df.rename(columns={"MONTH": "MONTH_STR"})
            elif "MONTH_STR" not in monthly_df.columns:
                monthly_df = monthly_df.rename(columns={monthly_df.columns[0]: "MONTH_STR"})

            st.markdown("### 📈 Spending Trends")
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
                st.markdown("### 🏆 Top 10 Vendors by Spend (YTD)")
                alt_bar(vendors_df.head(10), x="VENDOR_NAME", y="SPEND", horizontal=True, height=400, color="#22c55e")

    elif analysis_type == "vendor_analysis":
        if vendors_df is not None and not vendors_df.empty:
            vendors_df.columns = [c.upper() for c in vendors_df.columns]
            if "VENDOR_NAME" in vendors_df.columns and "SPEND" in vendors_df.columns:
                st.markdown("### 🏭 Top Vendors by Spend (Last 6 Months)")
                alt_bar(vendors_df.head(15), x="VENDOR_NAME", y="SPEND", horizontal=True, height=500, color="#8b5cf6")
                if "INVOICE_COUNT" in vendors_df.columns:
                    st.markdown("### 📄 Invoice Frequency by Vendor")
                    freq_df = vendors_df[["VENDOR_NAME", "INVOICE_COUNT"]].head(10)
                    alt_bar(freq_df, x="VENDOR_NAME", y="INVOICE_COUNT", horizontal=True, height=300, color="#f59e0b")

    elif analysis_type == "payment_performance":
        if monthly_df is not None and not monthly_df.empty:
            monthly_df.columns = [c.upper() for c in monthly_df.columns]
            if "MONTH" in monthly_df.columns:
                monthly_df = monthly_df.rename(columns={"MONTH": "MONTH_STR"})
            elif "MONTH_STR" not in monthly_df.columns:
                monthly_df = monthly_df.rename(columns={monthly_df.columns[0]: "MONTH_STR"})
            st.markdown("### ⏱️ Payment Performance Trend")
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

    elif analysis_type == "invoice_aging":
        if vendors_df is not None and not vendors_df.empty:
            vendors_df.columns = [c.upper() for c in vendors_df.columns]
            if "AGING_BUCKET" in vendors_df.columns and "SPEND" in vendors_df.columns:
                st.markdown("### 📅 Invoice Aging Buckets")
                alt_bar(vendors_df, x="AGING_BUCKET", y="SPEND", title="Outstanding Spend by Aging", horizontal=False, height=300, color="#ef4444")
            elif "CNT" in vendors_df.columns:
                st.markdown("### 🥧 Aging Distribution")
                alt_donut_status(vendors_df, label_col="AGING_BUCKET", value_col="CNT", title="Invoice Count by Age", height=300)

    # Prescriptive Insights (AI‑generated) for quick analyses
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

    # Expandable SQL
    with st.expander("🔍 Query outputs"):
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
    # Enhanced global CSS for Genie tab
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
        border-color: #cbd5e1;
    }
    .genie-card h3 {
        font-size: 1.3rem;
        font-weight: 700;
        margin-bottom: 0.5rem;
        background: linear-gradient(135deg, #1e293b, #334155);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .genie-card p {
        color: #475569;
        font-size: 0.9rem;
        line-height: 1.5;
        margin-bottom: 1.5rem;
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
        scroll-behavior: smooth;
    }
    .suggestion-chip {
        background: #f1f5f9;
        border-radius: 40px;
        padding: 0.4rem 1rem;
        font-size: 0.8rem;
        cursor: pointer;
        transition: all 0.2s;
        display: inline-block;
        margin: 0.2rem;
        border: 1px solid #e2e8f0;
    }
    .suggestion-chip:hover {
        background: #3b82f6;
        color: white;
        border-color: #3b82f6;
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

    # List of cash flow forecast questions (from Forecast tab buttons)
    cash_flow_questions = [
        "Forecast cash outflow for the next 7, 14, 30, 60, and 90 days",
        "Which invoices should we pay early to capture discounts?",
        "What is the optimal payment timing strategy for this week?",
        "Show late payment trend for forecasting"
    ]

    quick_map = {
        "Spending Overview": "spending_overview",
        "Vendor Analysis": "vendor_analysis",
        "Payment Performance": "payment_performance",
        "Invoice Aging": "invoice_aging"
    }

    auto_query = st.session_state.pop("auto_run_query", None)
    if auto_query:
        st.session_state.genie_messages = []
        st.session_state.genie_turn_index = 0
        st.session_state.selected_analysis = "custom"
        st.session_state.last_custom_query = auto_query
        with st.spinner("Running analysis..."):
            # Check if it's a cash flow forecast question
            if any(q in auto_query for q in cash_flow_questions):
                result = process_cash_flow_forecast(auto_query)
            elif auto_query in quick_map:
                result = run_quick_analysis(quick_map[auto_query])
            else:
                result = process_custom_query(auto_query)
            st.session_state.genie_response = result
            st.session_state.genie_messages.append({"role": "user", "content": auto_query, "timestamp": datetime.now()})
            if result.get("layout") in ("quick", "analyst", "sql", "cash_flow"):
                st.session_state.genie_messages.append({"role": "assistant", "content": "Analysis complete.", "response": result, "timestamp": datetime.now()})
                save_chat_message(st.session_state.genie_session_id, st.session_state.genie_turn_index, "user", auto_query)
                st.session_state.genie_turn_index += 1
                sql_used_val = result.get("sql", "")
                if isinstance(sql_used_val, dict):
                    sql_used_val = json.dumps(sql_used_val)
                save_chat_message(st.session_state.genie_session_id, st.session_state.genie_turn_index, "assistant", "Analysis complete.", sql_used=sql_used_val)
                st.session_state.genie_turn_index += 1
                save_question(auto_query, "cash_flow" if any(q in auto_query for q in cash_flow_questions) else ("quick" if auto_query in quick_map else "custom"))
                set_cache(auto_query, result)
            else:
                st.session_state.genie_messages.append({"role": "assistant", "content": result.get("message", "Error"), "timestamp": datetime.now()})
        st.rerun()

    st.markdown("## 🧞 Welcome to ProcureIQ Genie")
    st.markdown("Your AI‑powered procurement assistant. Choose a quick analysis or ask a custom question.")

    # Quick analysis cards
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
                    st.session_state.auto_run_query = title.split(" ", 1)[1]  # remove emoji
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
            st.markdown('<div style="margin-bottom: 0.5rem;">Click a chip to fill the input:</div>', unsafe_allow_html=True)
            chip_html = '<div style="display: flex; flex-wrap: wrap;">'
            for chip in suggestions:
                chip_html += f'<div class="suggestion-chip" onclick="document.querySelector(\'input[type=text]\').value = \'{chip}\'; document.querySelector(\'form\').submit();">{chip}</div>'
            chip_html += '</div>'
            st.markdown(chip_html, unsafe_allow_html=True)
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

        with st.expander("🌍 Most frequent (all)", expanded=False):
            all_faqs = get_frequent_questions_all_cached(5)
            if all_faqs:
                for faq in all_faqs:
                    st.button(f"{faq['query'][:50]} ({faq['count']})", key=f"faq_all_{faq['query']}", use_container_width=True, disabled=True)
            else:
                st.caption("No questions yet.")

    with right_col:
        st.markdown('<div style="text-align: center; margin-bottom: 1rem;">', unsafe_allow_html=True)
        st.markdown("### 💬 Start a Conversation")
        st.markdown("Ask a natural language question about your procurement data.")
        st.markdown('</div>', unsafe_allow_html=True)

        st.markdown('<div class="chat-scrollable">', unsafe_allow_html=True)
        for msg in st.session_state.genie_messages:
            if msg["role"] == "user":
                st.markdown(f'<div class="chat-message-user"><strong>You</strong><br/>{html.escape(msg["content"])}</div>', unsafe_allow_html=True)
            else:
                st.markdown(f'<div class="chat-message-assistant"><strong>🧞 Genie</strong><br/>{html.escape(msg["content"])}</div>', unsafe_allow_html=True)
                if "response" in msg and msg["response"]:
                    resp = msg["response"]
                    if resp.get("layout") == "cash_flow":
                        render_cash_flow_response(resp)
                    elif resp.get("layout") == "quick":
                        render_quick_analysis_response(resp)
                    elif resp.get("layout") == "analyst":
                        analyst_text = resp.get("analyst_response", "")
                        if analyst_text:
                            st.markdown(analyst_text)
                        else:
                            st.info("No descriptive analysis available.")
                        df = pd.DataFrame(resp["df"])
                        st.dataframe(df, use_container_width=True)
                        chart = auto_chart(df)
                        if chart:
                            st.altair_chart(chart, use_container_width=True)
                        with st.expander("View SQL used"):
                            st.code(resp["sql"], language="sql")
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
                        if any(q in user_question for q in cash_flow_questions):
                            result = process_cash_flow_forecast(user_question)
                        elif user_question in quick_map:
                            result = run_quick_analysis(quick_map[user_question])
                        else:
                            result = process_custom_query(user_question)
                        if result.get("layout") in ("quick", "analyst", "sql", "cash_flow"):
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
                            save_question(user_question, "cash_flow" if any(q in user_question for q in cash_flow_questions) else ("quick" if user_question in quick_map else "custom"))
                        else:
                            st.error(result.get("message", "Query failed"))
                st.rerun()
