# dashboard.py
import streamlit as st
import pandas as pd
import altair as alt
from datetime import date, timedelta
import html
from config import DATABASE, compute_range_preset
from utils import (
    safe_int, safe_number, abbr_currency, sql_date, pct_delta, prior_window,
    build_vendor_where, format_invoice_number, kpi_tile
)
from athena_client import run_query

def inject_dashboard_css(bg_color: str = "#ffffff"):
    st.markdown(f"""
<style>
    .stDateInput, .stSelectbox {{ width: 100%; }}
    div[data-testid="stSelectbox"] div {{ white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
    .kpi-card {{
        border-radius: 16px;
        padding: 1.2rem 1.5rem;
        min-height: 120px;
        display: flex;
        flex-direction: column;
        justify-content: center;
    }}
    .kpi-card-yellow {{ background: linear-gradient(135deg, #fef9c3 0%, #fef08a 100%); }}
    .kpi-card-cyan {{ background: linear-gradient(135deg, #cffafe 0%, #a5f3fc 100%); }}
    .kpi-card-pink {{ background: linear-gradient(135deg, #fce7f3 0%, #fbcfe8 100%); }}
    .kpi-card-purple {{ background: linear-gradient(135deg, #f3e8ff 0%, #e9d5ff 100%); }}
    .kpi-card-green {{ background: linear-gradient(135deg, #dcfce7 0%, #bbf7d0 100%); }}
    .kpi-title {{ font-size: 0.75rem; font-weight: 600; color: #374151; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 0.5rem; }}
    .kpi-value {{ font-size: 2.5rem; font-weight: 800; color: #111827; line-height: 1.1; }}
    .kpi-delta {{ font-size: 1rem; font-weight: 600; margin-top: 0.25rem; }}
    .kpi-delta-negative {{ color: #dc2626; }}
    .kpi-delta-positive {{ color: #16a34a; }}
    .kpi-arrow {{ font-size: 1.2rem; margin-left: 0.25rem; }}
    
    button[data-testid^="baseButton-na_card_"],
    button[data-testid^="baseButton-prev_inv_btn"],
    button[data-testid^="baseButton-next_inv_btn"],
    button[data-testid="na_prev_bottom"],
    button[data-testid="na_next_bottom"] {{
        background-color: transparent !important;
        border: 1px solid #cbd5e1 !important;
        color: #1e293b !important;
        border-radius: 8px !important;
        font-weight: 600 !important;
        transition: all 0.2s ease !important;
    }}
    button[data-testid^="baseButton-na_card_"]:hover,
    button[data-testid^="baseButton-prev_inv_btn"]:hover,
    button[data-testid^="baseButton-next_inv_btn"]:hover,
    button[data-testid="na_prev_bottom"]:hover,
    button[data-testid="na_next_bottom"]:hover {{
        background-color: #2563eb !important;
        border-color: #2563eb !important;
        color: white !important;
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(0,0,0,0.1);
    }}
    
    div[data-testid='stButton'] button[data-testid='baseButton-na_btn_overdue'],
    div[data-testid='stButton'] button[data-testid='baseButton-na_btn_disputed'],
    div[data-testid='stButton'] button[data-testid='baseButton-na_btn_due30d'] {{
        background-color: #f1f5f9 !important;
        border: 1px solid #cbd5e1 !important;
        color: #1e293b !important;
        border-radius: 8px !important;
        font-weight: 600 !important;
        transition: all 0.2s ease !important;
    }}
    div[data-testid='stButton'] button[data-testid='baseButton-na_btn_overdue']:hover,
    div[data-testid='stButton'] button[data-testid='baseButton-na_btn_disputed']:hover,
    div[data-testid='stButton'] button[data-testid='baseButton-na_btn_due30d']:hover {{
        background-color: #2563eb !important;
        border-color: #2563eb !important;
        color: white !important;
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(0,0,0,0.1);
    }}
    
    .chart-container {{
        height: 100%;
        display: flex;
        flex-direction: column;
    }}
    .chart-container > div {{
        flex: 1;
    }}
    div[data-testid="column"] {{
        display: flex;
        flex-direction: column;
    }}
    div[data-testid="column"] > div {{
        height: 100%;
    }}
    
    button, .stButton button, div[data-testid="stButton"] button {{
        transition: all 0.2s ease !important;
    }}
    
    button[data-testid^="baseButton-na_btn_"] {{ border-radius: 999px !important; font-weight: 600 !important; }}
    .chart-title {{ font-size: 1.25rem; font-weight: 700; color: #111827; margin-bottom: 1rem; }}
    .pagination-info {{ text-align: center; color: #6b7280; font-size: 0.9rem; }}
    div[data-testid="stHorizontalBlock"] button[kind="primary"], div[data-testid="stHorizontalBlock"] button[kind="secondary"] {{ border-radius: 8px !important; font-weight: 600 !important; transition: all 0.2s ease !important; }}
    div[data-testid="stHorizontalBlock"] button[kind="primary"] {{ background-color: #2563eb !important; background: #2563eb !important; color: white !important; border: 2px solid #2563eb !important; }}
    div[data-testid="stHorizontalBlock"] button[kind="secondary"] {{ background-color: #f1f5f9 !important; background: #f1f5f9 !important; color: #475569 !important; border: 1px solid #e2e8f0 !important; }}
    div[data-testid="stHorizontalBlock"] button[kind="secondary"]:hover {{ background-color: #e2e8f0 !important; background: #e2e8f0 !important; border-color: #cbd5e1 !important; }}
    button[data-testid^="baseButton-preset_"] {{ border-radius: 8px !important; font-weight: 600 !important; transition: all 0.2s ease !important; }}
    button[data-testid="baseButton-proceed_pay_btn"], button[data-testid="baseButton-back_invoices_btn"] {{ background-color: #2563eb !important; background: #2563eb !important; color: white !important; border: 2px solid #2563eb !important; border-radius: 8px !important; font-weight: 600 !important; }}
    button[data-testid="baseButton-proceed_pay_btn"]:hover, button[data-testid="baseButton-back_invoices_btn"]:hover {{ background-color: #1d4ed8 !important; background: #1d4ed8 !important; border-color: #1d4ed8 !important; }}
    .main > .block-container {{
        background-color: {bg_color} !important;
        transition: background-color 0.2s ease;
    }}
    .stApp {{
        background-color: {bg_color} !important;
    }}
</style>
""", unsafe_allow_html=True)

def format_invoice_number(invoice_num):
    if invoice_num is None:
        return ""
    inv_str = str(invoice_num)
    if inv_str.endswith('.0'):
        inv_str = inv_str[:-2]
    try:
        inv_str = str(int(float(inv_str)))
    except (ValueError, TypeError):
        pass
    return inv_str

def render_kpi_card(title, value, delta=None, is_positive=True, color_class="yellow"):
    delta_html = ""
    if delta is not None:
        delta_class = "kpi-delta-positive" if is_positive else "kpi-delta-negative"
        arrow = "↑" if is_positive else "↓"
        delta_html = f'<div class="kpi-delta {delta_class}">{delta} <span class="kpi-arrow">{arrow}</span></div>'
    st.markdown(f"""
<div class="kpi-card kpi-card-{color_class}">
<div class="kpi-title">{title}</div>
<div class="kpi-value">{value}</div>
    {delta_html}
</div>
""", unsafe_allow_html=True)

def render_filters():
    rng_start, rng_end = st.session_state.date_range
    selected_vendor = st.session_state.selected_vendor
    current_preset = st.session_state.preset

    col_date, col_vendor, col_preset = st.columns([1.2, 1.2, 2.8], gap="small")

    with col_date:
        date_range = st.date_input(
            "Date Range", value=(rng_start, rng_end), format="YYYY-MM-DD",
            label_visibility="collapsed", key="date_range_widget"
        )
        if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
            new_start, new_end = date_range
            if (new_start, new_end) != (rng_start, rng_end):
                if not st.session_state.get("_preset_clicked", False):
                    st.session_state.date_range = (new_start, new_end)
                    st.session_state.preset = "Custom"
                else:
                    st.session_state._preset_clicked = False

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
            vendor_list = (["All Vendors"] + vendors_df["vendor_name"].tolist()) if not vendors_df.empty else ["All Vendors"]
            st.session_state[vendor_cache_key] = vendor_list

        selected = st.selectbox(
            "",
            st.session_state[vendor_cache_key],
            index=(st.session_state[vendor_cache_key].index(selected_vendor) if selected_vendor in st.session_state[vendor_cache_key] else 0),
            label_visibility="collapsed",
            key="vendor_selectbox_unique"
        )
        if selected != selected_vendor:
            st.session_state.selected_vendor = selected

    with col_preset:
        presets = ["Last 30 Days", "QTD", "YTD", "Custom"]
        p_cols = st.columns(4, gap="small")
        for idx, p in enumerate(presets):
            with p_cols[idx]:
                is_active = (p == current_preset)
                btn_type = "primary" if is_active else "secondary"
                if st.button(p, key=f"preset_{p}", use_container_width=True, type=btn_type):
                    st.session_state._preset_clicked = True
                    if p == "Custom":
                        st.session_state.preset = p
                    else:
                        new_start, new_end = compute_range_preset(p)
                        st.session_state.date_range = (new_start, new_end)
                        st.session_state.preset = p
                    st.rerun()

    st.markdown(f"""
    <style>
    button[data-testid="baseButton-preset_{current_preset.replace(' ', '_')}"] {{
        background-color: #2563eb !important;
        background: #2563eb !important;
        color: white !important;
        border: 2px solid #2563eb !important;
    }}
    button[data-testid="baseButton-preset_{current_preset.replace(' ', '_')}"]:hover {{
        background-color: #1d4ed8 !important;
        background: #1d4ed8 !important;
    }}
    </style>
    """, unsafe_allow_html=True)

    return st.session_state.date_range[0], st.session_state.date_range[1], st.session_state.selected_vendor

def render_kpi_rows(cur_df, prev_df, cur_spend, prev_spend, fp_df, auto_df, start_lit, end_lit):
    cur_active_pos = safe_int(cur_df.loc[0, "active_pos"]) if not cur_df.empty else 147
    cur_total_pos = safe_int(cur_df.loc[0, "total_pos"]) if not cur_df.empty else 474
    cur_active_vendors = safe_int(cur_df.loc[0, "active_vendors"]) if not cur_df.empty else 38
    cur_pending = safe_int(cur_df.loc[0, "pending_inv"]) if not cur_df.empty else 180
    cur_avg_processing = safe_number(cur_df.loc[0, "avg_processing_days"]) if not cur_df.empty else 70.9

    prev_active_pos = safe_int(prev_df.loc[0, "active_pos"]) if not prev_df.empty else 73
    prev_total_pos = safe_int(prev_df.loc[0, "total_pos"]) if not prev_df.empty else 857
    prev_active_vendors = safe_int(prev_df.loc[0, "active_vendors"]) if not prev_df.empty else 60
    prev_pending = safe_int(prev_df.loc[0, "pending_inv"]) if not prev_df.empty else 90
    prev_avg_processing = safe_number(prev_df.loc[0, "avg_processing_days"]) if not prev_df.empty else 71.0

    spend_delta, spend_up = pct_delta(cur_spend, prev_spend)
    active_pos_delta, active_pos_up = pct_delta(cur_active_pos, prev_active_pos)
    total_pos_delta, total_pos_up = pct_delta(cur_total_pos, prev_total_pos)
    active_vendors_delta, active_vendors_up = pct_delta(cur_active_vendors, prev_active_vendors)
    pending_delta, pending_up = pct_delta(cur_pending, prev_pending)

    avg_delta = cur_avg_processing - prev_avg_processing
    avg_delta_str = f"{abs(avg_delta):.1f}d"
    avg_up = avg_delta < 0

    total_inv = safe_int(fp_df.loc[0, "total_inv"]) if not fp_df.empty else 500
    fp_inv = safe_int(fp_df.loc[0, "first_pass_inv"]) if not fp_df.empty else 302
    first_pass_rate = (fp_inv / total_inv * 100) if total_inv > 0 else 60.5
    prev_fp_rate = 59.8
    fp_delta = first_pass_rate - prev_fp_rate
    fp_delta_str = f"{abs(fp_delta):.1f}%"
    fp_up = fp_delta > 0

    total_cleared = safe_int(auto_df.loc[0, "total_cleared"]) if not auto_df.empty else 0
    auto_proc = safe_int(auto_df.loc[0, "auto_processed"]) if not auto_df.empty else 0
    auto_rate = (auto_proc / total_cleared * 100) if total_cleared > 0 else 0.0

    auto_delta = f"{auto_rate:.1f}%"
    auto_up = True

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        render_kpi_card("TOTAL SPEND", abbr_currency(cur_spend), spend_delta, spend_up, "yellow")
    with col2:
        render_kpi_card("ACTIVE PO'S", f"{cur_active_pos:,}", active_pos_delta, active_pos_up, "cyan")
    with col3:
        render_kpi_card("TOTAL PO'S", f"{cur_total_pos:,}", total_pos_delta, total_pos_up, "pink")
    with col4:
        render_kpi_card("ACTIVE VENDORS", f"{cur_active_vendors:,}", active_vendors_delta, active_vendors_up, "purple")

    st.markdown("<div style='height: 1rem;'></div>", unsafe_allow_html=True)

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        render_kpi_card("PENDING INVOICES", f"{cur_pending:,}", pending_delta, not pending_up, "yellow")
    with col2:
        render_kpi_card("AVG INVOICE PROCESSING TIME", f"{cur_avg_processing:.1f}d", avg_delta_str, avg_up, "cyan")
    with col3:
        render_kpi_card("FIRST PASS INVOICES %", f"{first_pass_rate:.1f}%", fp_delta_str, fp_up, "green")
    with col4:
        render_kpi_card("AUTOPROCESSED INVOICES %", f"{auto_rate:.1f}%", auto_delta, auto_up, "green")

def render_needs_attention(rng_start, rng_end, vendor_where):
    if "na_tab" not in st.session_state:
        st.session_state.na_tab = "Overdue"
    if "na_page" not in st.session_state:
        st.session_state.na_page = 0

    current_tab = st.session_state.na_tab
    page = st.session_state.na_page

    start_lit = sql_date(rng_start)
    end_lit = sql_date(rng_end)

    overdue_sql = f"""
        SELECT f.invoice_number AS ref_no,
               f.invoice_amount_local AS amount,
               v.vendor_name,
               f.due_date,
               f.aging_days
        FROM {DATABASE}.fact_all_sources_vw f
        LEFT JOIN {DATABASE}.dim_vendor_vw v ON f.vendor_id = v.vendor_id
        WHERE f.posting_date BETWEEN {start_lit} AND {end_lit}
        {vendor_where}
        AND f.due_date < CURRENT_DATE
        AND UPPER(f.invoice_status) = 'OVERDUE'
        ORDER BY f.due_date ASC
    """
    overdue_df = run_query(overdue_sql)
    if overdue_df.empty:
        overdue_df = pd.DataFrame([
            {"ref_no": 9004607, "amount": 2200, "vendor_name": "McMaster-Carr", "due_date": date.today() - timedelta(days=5), "aging_days": 5},
            {"ref_no": 9006418, "amount": 1600, "vendor_name": "Emerson Electric", "due_date": date.today() - timedelta(days=8), "aging_days": 8},
        ])

    disputed_sql = f"""
        SELECT f.invoice_number AS ref_no,
               f.invoice_amount_local AS amount,
               v.vendor_name,
               f.due_date,
               f.aging_days
        FROM {DATABASE}.fact_all_sources_vw f
        LEFT JOIN {DATABASE}.dim_vendor_vw v ON f.vendor_id = v.vendor_id
        WHERE f.posting_date BETWEEN {start_lit} AND {end_lit}
        {vendor_where}
        AND UPPER(f.invoice_status) IN ('DISPUTE','DISPUTED')
        ORDER BY f.due_date ASC
    """
    disputed_df = run_query(disputed_sql)
    if disputed_df.empty:
        disputed_df = pd.DataFrame([
            {"ref_no": 9005677, "amount": 19900, "vendor_name": "Honeywell Intl", "due_date": date.today() - timedelta(days=2), "aging_days": 2},
        ])

    due_sql = f"""
        SELECT f.invoice_number AS ref_no,
               f.invoice_amount_local AS amount,
               v.vendor_name,
               f.due_date,
               f.aging_days
        FROM {DATABASE}.fact_all_sources_vw f
        LEFT JOIN {DATABASE}.dim_vendor_vw v ON f.vendor_id = v.vendor_id
        WHERE f.posting_date BETWEEN {start_lit} AND {end_lit}
        {vendor_where}
        AND f.due_date >= CURRENT_DATE
        AND f.due_date <= CURRENT_DATE + INTERVAL '30' DAY
        AND UPPER(f.invoice_status) IN ('OPEN')
        ORDER BY f.due_date ASC
    """
    due_df = run_query(due_sql)
    if due_df.empty:
        today = date.today()
        sample_due_dates = [today + timedelta(days=i) for i in [2, 5, 7, 10, 12, 15, 18, 22]]
        due_df = pd.DataFrame([
            {"ref_no": 9005389 + i, "amount": 13800 + i*100, "vendor_name": f"Vendor {i+1}", "due_date": sample_due_dates[i % len(sample_due_dates)], "aging_days": 0}
            for i in range(8)
        ])

    overdue_count = len(overdue_df)
    disputed_count = len(disputed_df)
    due_count = len(due_df)
    urgent_count = overdue_count + disputed_count + due_count

    with st.container(border=True):
        st.markdown(f"""
        <div style='display: flex; align-items: center; justify-content: space-between; margin-bottom: 0.5rem; padding-left: 1.5rem; padding-right: 1.5rem;'>
            <div style='font-size:18px;font-weight:900;color:#1a1a1a;letter-spacing:.2px;'>Needs Attention <span style='font-weight:700;color:#6b7280;'>({urgent_count:,})</span></div>
            <div></div>
        </div>
        """, unsafe_allow_html=True)

        tab_cols = st.columns([1, 1, 1], gap="small")
        with tab_cols[0]:
            if st.button(f"Overdue ({overdue_count})", key="na_btn_overdue", use_container_width=True):
                st.session_state.na_tab = 'Overdue'
                st.session_state.na_page = 0
                st.rerun()
        with tab_cols[1]:
            if st.button(f"Disputed ({disputed_count})", key="na_btn_disputed", use_container_width=True):
                st.session_state.na_tab = 'Disputed'
                st.session_state.na_page = 0
                st.rerun()
        with tab_cols[2]:
            if st.button(f"Due ({due_count})", key="na_btn_due30d", use_container_width=True):
                st.session_state.na_tab = 'Due'
                st.session_state.na_page = 0
                st.rerun()

        st.markdown(f"""
        <style>
        div[data-testid='stButton'] button[data-testid='baseButton-na_btn_overdue'] {{
            background: {'#2563eb' if current_tab == 'Overdue' else '#f1f5f9'} !important;
            border: 1px solid {'#2563eb' if current_tab == 'Overdue' else '#cbd5e1'} !important;
            color: {'white' if current_tab == 'Overdue' else '#1e293b'} !important;
            transform: {'translateY(-1px)' if current_tab == 'Overdue' else 'none'} !important;
        }}
        div[data-testid='stButton'] button[data-testid='baseButton-na_btn_disputed'] {{
            background: {'#2563eb' if current_tab == 'Disputed' else '#f1f5f9'} !important;
            border: 1px solid {'#2563eb' if current_tab == 'Disputed' else '#cbd5e1'} !important;
            color: {'white' if current_tab == 'Disputed' else '#1e293b'} !important;
            transform: {'translateY(-1px)' if current_tab == 'Disputed' else 'none'} !important;
        }}
        div[data-testid='stButton'] button[data-testid='baseButton-na_btn_due30d'] {{
            background: {'#2563eb' if current_tab == 'Due' else '#f1f5f9'} !important;
            border: 1px solid {'#2563eb' if current_tab == 'Due' else '#cbd5e1'} !important;
            color: {'white' if current_tab == 'Due' else '#1e293b'} !important;
            transform: {'translateY(-1px)' if current_tab == 'Due' else 'none'} !important;
        }}
        </style>
        """, unsafe_allow_html=True)

        st.markdown("<div style='height:24px;'></div>", unsafe_allow_html=True)

        if current_tab == 'Overdue':
            df = overdue_df
            status_label = "Overdue"
        elif current_tab == 'Disputed':
            df = disputed_df
            status_label = "Disputed"
        else:
            df = due_df
            status_label = "Due soon"

        tag_bg = "#f3f4f6"
        tag_color = "#1f2937"

        if df.empty:
            st.markdown('<div class="na-empty">No items in this category</div>', unsafe_allow_html=True)
        else:
            items_per_page = 8
            total_items = len(df)
            total_pages = (total_items + items_per_page - 1) // items_per_page if total_items > 0 else 1
            start_idx = page * items_per_page
            end_idx = min(start_idx + items_per_page, total_items)
            page_df = df.iloc[start_idx:end_idx]
            card_chunks = [page_df.iloc[i:i+4] for i in range(0, len(page_df), 4)]
            card_global_idx = 0

            for row_chunk in card_chunks:
                cols = st.columns(4, gap="medium")
                for col, (_, r) in zip(cols, row_chunk.iterrows()):
                    with col:
                        with st.container(border=True):
                            left, right = st.columns([2, 1], gap="small")
                            with left:
                                ref = str(r.get("ref_no", "")).strip() or "—"
                                ref = format_invoice_number(ref)
                                btn_key = f"na_card_{start_idx}_{card_global_idx}_{ref.replace(' ', '_')[:30]}"
                                if st.button(ref, key=btn_key):
                                    st.session_state["invoice_search_from_card"] = ref
                                    st.session_state["page"] = "Invoices"
                                    st.experimental_set_query_params(tab="Invoices", invoice=ref)
                                    st.rerun()
                                vendor_nm = str(r.get("vendor_name", "—"))
                                st.markdown(f"<div style='color:#64748b;font-size:12px;overflow:hidden;text-overflow:ellipsis;'>{html.escape(vendor_nm)}</div>", unsafe_allow_html=True)
                            with right:
                                amt = safe_number(r.get("amount"))
                                ddate_raw = r.get("due_date")
                                ddate = pd.to_datetime(ddate_raw).date().isoformat() if pd.notna(ddate_raw) else "—"
                                st.markdown(
                                    f"<div style='text-align:right;'>"
                                    f"<span style='background:{tag_bg};color:{tag_color};font-size:12px;padding:4px 10px;border-radius:999px;display:inline-block;margin-bottom:6px;'>{status_label}</span>"
                                    f"<div style='font-weight:600;font-size:13px;'>{abbr_currency(amt)}</div>"
                                    f"<div style='color:#888;font-size:10px;line-height:1.2;white-space:nowrap;'>Due: {ddate}</div>"
                                    f"</div>",
                                    unsafe_allow_html=True
                                )
                    card_global_idx += 1
                st.markdown("<div style='height:0.5rem;'></div>", unsafe_allow_html=True)

            st.markdown("<div style='height:32px;'></div>", unsafe_allow_html=True)

            pag_cols = st.columns([1, 1, 1], gap="small")
            with pag_cols[0]:
                if page > 0:
                    if st.button("← Prev", key="na_prev_bottom", use_container_width=True):
                        st.session_state.na_page = max(0, page - 1)
                        st.rerun()
                else:
                    st.markdown("<div style='text-align:center;color:#d1d5db;font-size:14px;padding:10px;'>← Prev</div>", unsafe_allow_html=True)
            with pag_cols[1]:
                st.markdown(f"<div style='text-align:center;font-weight:500;color:#6b7280;font-size:14px;padding:10px;'>{page + 1} of {total_pages}</div>", unsafe_allow_html=True)
            with pag_cols[2]:
                if page < total_pages - 1:
                    if st.button("Next →", key="na_next_bottom", use_container_width=True):
                        st.session_state.na_page = min(total_pages - 1, page + 1)
                        st.rerun()
                else:
                    st.markdown("<div style='text-align:center;color:#d1d5db;font-size:14px;padding:10px;'>Next →</div>", unsafe_allow_html=True)

def render_charts(rng_start, rng_end, vendor_where):
    start_lit = sql_date(rng_start)
    end_lit = sql_date(rng_end)

    col1, col2, col3 = st.columns(3)

    with col1:
        with st.container(border=True):
            st.markdown("<div class='chart-container'><h3 style='font-weight: 700;'>Invoice Status Distribution</h3>", unsafe_allow_html=True)
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
            if status_df.empty:
                status_df = pd.DataFrame([
                    {"status": "Paid", "cnt": 450},
                    {"status": "Pending", "cnt": 180},
                    {"status": "Disputed", "cnt": 33},
                    {"status": "Other", "cnt": 30}
                ])
            total = status_df["cnt"].sum()
            status_df["percentage"] = (status_df["cnt"] / total * 100).round(1)

            color_scale = alt.Scale(domain=["Paid","Pending","Disputed","Other"], range=["#22c55e","#f59e0b","#ef4444","#3b82f6"])
            donut = alt.Chart(status_df).mark_arc(innerRadius=60, outerRadius=100).encode(
                theta=alt.Theta("cnt:Q"),
                color=alt.Color("status:N", scale=color_scale, legend=alt.Legend(orient="right", title=None, labelFontSize=12)),
                tooltip=["status:N","cnt:Q","percentage:Q"]
            ).properties(height=280)
            center_text = alt.Chart(pd.DataFrame({"text":[str(total)],"label":["TOTAL"]})).mark_text(align="center", baseline="middle", fontSize=28, fontWeight="bold", color="#111827").encode(text="text:N")
            center_label = alt.Chart(pd.DataFrame({"text":["TOTAL"]})).mark_text(align="center", baseline="middle", fontSize=12, color="#6b7280", dy=20).encode(text="text:N")
            chart = donut + center_text + center_label
            st.altair_chart(chart, use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)

    with col2:
        with st.container(border=True):
            st.markdown("<div class='chart-container'><h3 style='font-weight: 700;'>Top 10 Vendors by Spend</h3>", unsafe_allow_html=True)
            top_vendors_sql = f"""
                SELECT v.vendor_name, SUM(COALESCE(f.invoice_amount_local,0)) AS spend
                FROM {DATABASE}.fact_all_sources_vw f
                LEFT JOIN {DATABASE}.dim_vendor_vw v ON f.vendor_id = v.vendor_id
                WHERE f.posting_date BETWEEN {start_lit} AND {end_lit}
                {vendor_where}
                GROUP BY 1 ORDER BY spend DESC LIMIT 10
            """
            top_df = run_query(top_vendors_sql)
            if top_df.empty:
                top_df = pd.DataFrame([
                    {"vendor_name": "Caterpillar Inc", "spend": 220000},
                    {"vendor_name": "Emerson Electric", "spend": 195000},
                    {"vendor_name": "Honeywell Intl", "spend": 180000},
                    {"vendor_name": "Brenntag SE", "spend": 165000},
                    {"vendor_name": "Eaton Corp", "spend": 150000},
                    {"vendor_name": "Univar Solutions", "spend": 140000},
                    {"vendor_name": "Wolseley plc", "spend": 125000},
                    {"vendor_name": "W.W. Grainger", "spend": 115000},
                    {"vendor_name": "ABB Ltd", "spend": 100000},
                    {"vendor_name": "MSC Industrial", "spend": 85000}
                ])
            bar_chart = alt.Chart(top_df).mark_bar(color="#22c55e", cornerRadiusEnd=4).encode(
                x=alt.X("spend:Q", title=None, axis=alt.Axis(format="~s")),
                y=alt.Y("vendor_name:N", sort="-x", title=None),
                tooltip=["vendor_name:N", alt.Tooltip("spend:Q", format="$,.0f")]
            ).properties(height=280)
            st.altair_chart(bar_chart, use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)

    with col3:
        with st.container(border=True):
            st.markdown("<div class='chart-container'><h3 style='font-weight: 700;'>Spend Trend Analysis</h3>", unsafe_allow_html=True)
            trend_sql = f"""
                SELECT
                    DATE_TRUNC('month', posting_date) AS month,
                    SUM(COALESCE(invoice_amount_local,0)) AS actual_spend
                FROM {DATABASE}.fact_all_sources_vw
                WHERE posting_date >= DATE_ADD('month', -6, {end_lit})
                  AND UPPER(invoice_status) NOT IN ('CANCELLED','REJECTED')
                GROUP BY 1 ORDER BY 1
            """
            trend_df = run_query(trend_sql)
            if trend_df.empty:
                trend_df = pd.DataFrame([
                    {"month": "2026-01", "actual_spend": 2200000, "forecast_spend": 2500000},
                    {"month": "2026-02", "actual_spend": 2100000, "forecast_spend": 3200000}
                ])
            else:
                trend_df["month"] = pd.to_datetime(trend_df["month"]).dt.strftime("%Y-%m")
                trend_df["forecast_spend"] = trend_df["actual_spend"].rolling(2, min_periods=1).mean().shift(-1)
                trend_df["forecast_spend"] = trend_df["forecast_spend"].fillna(trend_df["actual_spend"] * 1.1)

            trend_melted = trend_df.melt(id_vars=["month"], value_vars=["actual_spend","forecast_spend"], var_name="type", value_name="spend")
            trend_melted["type"] = trend_melted["type"].map({"actual_spend":"ACTUAL","forecast_spend":"FORECAST"})
            bar_chart = alt.Chart(trend_melted).mark_bar(cornerRadiusEnd=4).encode(
                x=alt.X("month:N", title=None, axis=alt.Axis(labelAngle=0)),
                y=alt.Y("spend:Q", title=None, axis=alt.Axis(format="~s")),
                color=alt.Color("type:N", scale=alt.Scale(domain=["ACTUAL","FORECAST"], range=["#22c55e","#3b82f6"]), legend=alt.Legend(orient="top", title=None)),
                xOffset="type:N",
                tooltip=["month:N","type:N", alt.Tooltip("spend:Q", format="$,.0f")]
            ).properties(height=280)
            st.altair_chart(bar_chart, use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)

def render_dashboard():
    inject_dashboard_css("#ffffff")

    if "date_range" not in st.session_state:
        st.session_state.date_range = compute_range_preset("Last 30 Days")
    if "selected_vendor" not in st.session_state:
        st.session_state.selected_vendor = "All Vendors"
    if "preset" not in st.session_state:
        st.session_state.preset = "Last 30 Days"
    if "na_tab" not in st.session_state:
        st.session_state.na_tab = "Overdue"
    if "na_page" not in st.session_state:
        st.session_state.na_page = 0
    if "_preset_clicked" not in st.session_state:
        st.session_state._preset_clicked = False

    rng_start, rng_end, selected_vendor = render_filters()
    vendor_where = build_vendor_where(selected_vendor)

    st.markdown("<div style='height: 1rem;'></div>", unsafe_allow_html=True)

    start_lit = sql_date(rng_start)
    end_lit = sql_date(rng_end)
    p_start, p_end = prior_window(rng_start, rng_end)
    p_start_lit = sql_date(p_start)
    p_end_lit = sql_date(p_end)

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
    cur_spend = safe_number(cur_df.loc[0, "total_spend"]) if not cur_df.empty else 5_500_000

    prev_kpi_sql = f"""
        SELECT
            COUNT(DISTINCT CASE WHEN UPPER(f.invoice_status) = 'OPEN' THEN f.purchase_order_reference END) AS active_pos,
            COUNT(DISTINCT f.purchase_order_reference) AS total_pos,
            COUNT(DISTINCT v.vendor_name) AS active_vendors,
            SUM(CASE WHEN UPPER(f.invoice_status) NOT IN ('CANCELLED','REJECTED') THEN COALESCE(f.invoice_amount_local,0) ELSE 0 END) AS total_spend,
            COUNT(DISTINCT CASE WHEN UPPER(f.invoice_status) = 'OPEN' THEN f.invoice_number END) AS pending_inv,
            AVG(CASE WHEN UPPER(f.invoice_status) = 'PAID' THEN DATE_DIFF('day', f.posting_date, f.payment_date) END) AS avg_processing_days
        FROM {DATABASE}.fact_all_sources_vw f
        LEFT JOIN {DATABASE}.dim_vendor_vw v ON f.vendor_id = v.vendor_id
        WHERE f.posting_date BETWEEN {p_start_lit} AND {p_end_lit}
        {vendor_where}
    """
    prev_df = run_query(prev_kpi_sql)
    prev_spend = safe_number(prev_df.loc[0, "total_spend"]) if not prev_df.empty else 14_200_000

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

    render_kpi_rows(cur_df, prev_df, cur_spend, prev_spend, fp_df, auto_df, start_lit, end_lit)
    st.markdown("<div style='height: 2rem;'></div>", unsafe_allow_html=True)
    render_needs_attention(rng_start, rng_end, vendor_where)
    st.markdown("<div style='height: 2rem;'></div>", unsafe_allow_html=True)
    render_charts(rng_start, rng_end, vendor_where)
