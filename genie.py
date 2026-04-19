# genie.py
import streamlit as st
import pandas as pd
import json
import re
import html
import uuid
from datetime import datetime
from athena_client import run_query
from bedrock_client import ask_bedrock
from utils import abbr_currency, auto_chart, alt_line_monthly, alt_bar, ensure_limit, is_safe_sql
from semantic_model import SYSTEM_PROMPT, DESCRIPTIVE_PROMPT_TEMPLATE, generate_sql
from persistence import get_saved_insights_cached, get_frequent_questions_by_user_cached, get_frequent_questions_all_cached, save_chat_message, save_question, set_cache, get_cache
from quick_analysis import run_quick_analysis

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

def render_genie():
    st.markdown("""
    <style>
    .genie-card {
        background: white;
        border-radius: 16px;
        padding: 1.5rem;
        box-shadow: 0 2px 8px rgba(0,0,0,0.05);
        transition: transform 0.2s ease, box-shadow 0.2s ease;
        height: 100%;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
    }
    .genie-card:hover {
        transform: translateY(-4px);
        box-shadow: 0 12px 24px rgba(0,0,0,0.1);
    }
    .genie-card h3 {
        font-size: 1.25rem;
        font-weight: 600;
        margin-bottom: 0.5rem;
    }
    .genie-card p {
        color: #64748b;
        font-size: 0.875rem;
        line-height: 1.5;
        margin-bottom: 1.5rem;
    }
    .genie-card button {
        width: 100%;
        margin-top: auto;
    }
    .suggestion-chip {
        background: #f1f5f9;
        border-radius: 999px;
        padding: 0.4rem 1rem;
        font-size: 0.8rem;
        cursor: pointer;
        transition: background 0.2s;
        display: inline-block;
        margin: 0.2rem;
    }
    .suggestion-chip:hover {
        background: #e2e8f0;
    }
    .chat-scrollable {
        max-height: 400px;
        overflow-y: auto;
        padding-right: 8px;
        margin-bottom: 1rem;
    }
    .chat-message-user {
        background: #1459d2;
        color: white;
        padding: 10px 14px;
        border-radius: 16px;
        margin: 6px 0;
        max-width: 80%;
        align-self: flex-end;
    }
    .chat-message-assistant {
        background: #f1f5f9;
        color: #0f172a;
        padding: 10px 14px;
        border-radius: 16px;
        margin: 6px 0;
        max-width: 80%;
    }
    .centered-container {
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        text-align: center;
        padding: 2rem;
    }
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
    if "genie_prefill" not in st.session_state:
        st.session_state.genie_prefill = ""

    # Map quick analysis titles to keys
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
            # Check if it's a quick analysis
            if auto_query in quick_map:
                result = run_quick_analysis(quick_map[auto_query])
            else:
                result = process_custom_query(auto_query)
            st.session_state.genie_response = result
            st.session_state.genie_messages.append({"role": "user", "content": auto_query, "timestamp": datetime.now()})
            if result.get("layout") in ("quick", "analyst", "sql"):
                st.session_state.genie_messages.append({"role": "assistant", "content": "Analysis complete.", "response": result, "timestamp": datetime.now()})
                save_chat_message(st.session_state.genie_session_id, st.session_state.genie_turn_index, "user", auto_query)
                st.session_state.genie_turn_index += 1
                # Convert sql_used to JSON string if it's a dict
                sql_used_val = result.get("sql", "")
                if isinstance(sql_used_val, dict):
                    sql_used_val = json.dumps(sql_used_val)
                save_chat_message(st.session_state.genie_session_id, st.session_state.genie_turn_index, "assistant", "Analysis complete.", sql_used=sql_used_val)
                st.session_state.genie_turn_index += 1
                save_question(auto_query, "quick" if auto_query in quick_map else "custom")
                set_cache(auto_query, result)
            else:
                st.session_state.genie_messages.append({"role": "assistant", "content": result.get("message", "Error"), "timestamp": datetime.now()})
        st.rerun()

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
            with st.container():
                st.markdown(f"""
                <div class="genie-card">
                    <div>
                        <h3>{title}</h3>
                        <p>{desc}</p>
                    </div>
                </div>
                """, unsafe_allow_html=True)
                if st.button("Ask Genie", key=f"card_{key}", use_container_width=True):
                    st.session_state.auto_run_query = title
                    st.rerun()

    st.markdown("---")

    left_col, right_col = st.columns([0.35, 0.65], gap="large")

    with left_col:
        with st.expander("Saved insights", expanded=False):
            insights = get_saved_insights_cached(page="genie")
            if insights:
                for ins in insights:
                    if st.button(ins["title"], key=f"insight_{ins['id']}", use_container_width=True):
                        st.session_state.auto_run_query = ins["question"]
                        st.rerun()
            else:
                st.caption("Save any Genie answer to see it here.")

        with st.expander("Frequently asked by you", expanded=False):
            suggestions = [
                "forecast cash outflow for the next 7, 14, 30, 60, and 90 days",
                "show me total spend ytd, monthly trends, and top 5 vendors",
                "which invoices should we pay early to capture discounts"
            ]
            st.markdown('<div style="margin-bottom: 0.5rem;">Click a chip to fill the input:</div>', unsafe_allow_html=True)
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

        with st.expander("Most frequent (all)", expanded=False):
            all_faqs = get_frequent_questions_all_cached(5)
            if all_faqs:
                for faq in all_faqs:
                    st.button(f"{faq['query'][:50]} ({faq['count']})", key=f"faq_all_{faq['query']}", use_container_width=True, disabled=True)
            else:
                st.caption("No questions yet.")

    with right_col:
        st.markdown('<div class="centered-container">', unsafe_allow_html=True)
        st.markdown("### Start a Conversation")
        st.markdown("Ask questions about your Procurement to Pay data, or select a pre-built analysis from the library.")
        st.markdown('</div>', unsafe_allow_html=True)

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
                                    st.metric(k.replace("_"," ").title(), abbr_currency(v) if isinstance(v, (int,float)) else str(v))
                        anomaly = resp.get("anomaly")
                        if anomaly:
                            st.warning(f"⚠️ {anomaly}")
                        monthly_df = resp.get("monthly_df")
                        if monthly_df is not None and not monthly_df.empty:
                            st.subheader("Spending Trends")
                            alt_line_monthly(monthly_df.rename(columns={"MONTH":"MONTH", "MONTHLY_SPEND":"VALUE"}), month_col="MONTH", value_col="VALUE", height=200)
                        vendors_df = resp.get("vendors_df")
                        if vendors_df is not None and not vendors_df.empty:
                            st.subheader("Top Vendors")
                            alt_bar(vendors_df.head(10), x="VENDOR_NAME", y="SPEND", horizontal=True, height=300)
                        with st.expander("View SQL used"):
                            sql_dict = resp.get("sql", {})
                            for name, sql_text in sql_dict.items():
                                st.code(sql_text, language="sql")
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
                        # Convert sql_used to JSON string if needed
                        sql_used_val = cached.get("sql", "") if isinstance(cached, dict) else ""
                        if isinstance(sql_used_val, dict):
                            sql_used_val = json.dumps(sql_used_val)
                        save_chat_message(st.session_state.genie_session_id, st.session_state.genie_turn_index, "assistant", "Answer from cache.", source="cache", sql_used=sql_used_val)
                        st.session_state.genie_turn_index += 1
                        save_question(user_question, "custom")
                    else:
                        # Check if it matches a quick analysis title
                        if user_question in quick_map:
                            result = run_quick_analysis(quick_map[user_question])
                        else:
                            result = process_custom_query(user_question)
                        if result.get("layout") in ("quick", "analyst", "sql"):
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
                            save_question(user_question, "quick" if user_question in quick_map else "custom")
                        else:
                            st.error(result.get("message", "Query failed"))
                st.rerun()
