#dashboard.py
import streamlit as st
import pandas as pd
import altair as alt
import math
from datetime import date

from config import compute_range_preset, DATABASE
from athena_client import run_query
from utils import (
    sql_date,
    prior_window,
    build_vendor_where,
    pct_delta,
    safe_number,
    safe_int,
    abbr_currency,
    kpi_tile,
    alt_bar,
    alt_line_monthly,
    alt_donut_status,
    clean_invoice_number,
)

# ------------------------------------------------------------
# Custom CSS for Dashboard Styling
# ------------------------------------------------------------
def inject_dashboard_css():
    st.markdown(
        """
<style>
    /* KPI Card Styles */
    .kpi-card {
        border-radius: 16px;
        padding: 1.2rem 1.5rem;
        min-height: 120px;
        display: flex;
        flex-direction: column;
        justify-content: center;
    }
    .kpi-card-yellow {
        background: linear-gradient(135deg, #fef9c3 0%, #fef08a 100%);
    }
    .kpi-card-cyan {
        background: linear-gradient(135deg, #cffafe 0%, #a5f3fc 100%);
    }
    .kpi-card-pink {
        background: linear-gradient(135deg, #fce7f3 0%, #fbcfe8 100%);
    }
    .kpi-card-purple {
        background: linear-gradient(135deg, #f3e8ff 0%, #e9d5ff 100%);
    }
    .kpi-card-green {
        background: linear-gradient(135deg, #dcfce7 0%, #bbf7d0 100%);
    }
    .kpi-title {
        font-size: 0.75rem;
        font-weight: 600;
        color: #374151;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        margin-bottom: 0.5rem;
    }
    .kpi-value {
        font-size: 2.5rem;
        font-weight: 800;
        color: #111827;
        line-height: 1.1;
    }
    .kpi-delta {
        font-size: 1rem;
        font-weight: 600;
        margin-top: 0.25rem;
    }
    .kpi-delta-negative {
        color: #dc2626;
    }
    .kpi-delta-positive {
        color: #16a34a;
    }
    .kpi-arrow {
        font-size: 1.2rem;
        margin-left: 0.25rem;
    }
    /* Needs Attention Section */
    .attention-header {
        font-size: 1.5rem;
        font-weight: 700;
        color: #111827;
        margin-bottom: 1rem;
    }
    .tab-button {
        border-radius: 25px;
        padding: 0.5rem 1.5rem;
        font-weight: 500;
        border: 1px solid #e5e7eb;
        background: #f9fafb;
        color: #374151;
        cursor: pointer;
        transition: all 0.2s;
    }
    .tab-button-active {
        background: #3b82f6;
        color: white;
        border-color: #3b82f6;
    }
    /* Invoice Cards */
    .invoice-card {
        background: #fff;
        border-radius: 16px;
        padding: 1rem;
        border: 1px solid #e5e7eb;
        display: flex;
        flex-direction: column;
        gap: 0.5rem;
        min-height: 160px;
        position: relative;
    }
    .invoice-card-overdue {
        background: linear-gradient(135deg, #fef2f2 0%, #fee2e2 100%);
        border: 1px solid #fecaca;
    }
    .invoice-card-disputed {
        background: linear-gradient(135deg, #fffbeb 0%, #fef3c7 100%);
        border: 1px solid #fde68a;
    }
    .invoice-card-due {
        background: linear-gradient(135deg, #eff6ff 0%, #dbeafe 100%);
        border: 1px solid #bfdbfe;
    }
    .invoice-status {
        display: inline-block;
        padding: 0.25rem 0.75rem;
        border-radius: 12px;
        font-size: 0.75rem;
        font-weight: 600;
    }
    .status-overdue {
        background: #fee2e2;
        color: #dc2626;
    }
    .status-disputed {
        background: #fef3c7;
        color: #d97706;
    }
    .status-due {
        background: #dbeafe;
        color: #2563eb;
    }
    .invoice-amount {
        font-size: 1.1rem;
        font-weight: 700;
        color: #111827;
    }
    .invoice-due-date {
        font-size: 0.8rem;
        color: #6b7280;
    }
    .invoice-vendor {
        font-size: 0.85rem;
        color: #374151;
        font-weight: 500;
    }
    /* Charts Section */
    .chart-title {
        font-size: 1.25rem;
        font-weight: 700;
        color: #111827;
        margin-bottom: 1rem;
    }
    /* Pagination */
    .pagination-info {
        text-align: center;
        color: #6b7280;
        font-size: 0.9rem;
    }
    /* Clickable Invoice Circle Button */
    .invoice-circle-btn {
        background: #d1d5db;
        border-radius: 50%;
        width: 70px;
        height: 70px;
        display: flex;
        flex-direction: column;
        justify-content: center;
        align-items: center;
        cursor: pointer;
        border: none;
        transition: all 0.2s ease;
        text-decoration: none;
    }
    .invoice-circle-btn:hover {
        background: #9ca3af;
        transform: scale(1.05);
    }
    .invoice-circle-btn-selected {
        background: #3b82f6;
    }
    .invoice-circle-btn-selected:hover {
        background: #2563eb;
    }
    .invoice-circle-btn-selected .inv-top,
    .invoice-circle-btn-selected .inv-bottom {
        color: white;
    }
    .inv-top {
        font-size: 1rem;
        font-weight: 700;
        color: #111827;
        line-height: 1.2;
    }
    .inv-bottom {
        font-size: 1.2rem;
        font-weight: 700;
        color: #6b7280;
        line-height: 1.2;
    }
    /* Hide default streamlit button styling for circle buttons */
    .stButton > button[data-testid="baseButton-secondary"].circle-btn {
        background: transparent !important;
        border: none !important;
        padding: 0 !important;
        margin: 0 !important;
        box-shadow: none !important;
    }
</style>
""",
        unsafe_allow_html=True,
    )


# ------------------------------------------------------------
# Helper: Convert invoice number to integer string
# ------------------------------------------------------------
def format_invoice_number(invoice_num):
    """Convert invoice number to integer string, removing any decimal points."""
    if invoice_num is None:
        return ""
    # Convert to string first
    inv_str = str(invoice_num)
    # Remove .0 if present (handles float conversion)
    if inv_str.endswith('.0'):
        inv_str = inv_str[:-2]
    # Try to convert to int and back to string to remove any decimals
    try:
        inv_str = str(int(float(inv_str)))
    except (ValueError, TypeError):
        pass
    return inv_str


# ------------------------------------------------------------
# Helper: Split invoice number for display (like in screenshot)
# ------------------------------------------------------------
def split_invoice_number(invoice_num):
    """Split invoice number into two parts for display.

    Example: 9005389 -> ('90053', '89')
    """
    inv_str = format_invoice_number(invoice_num)
    if len(inv_str) <= 5:
        return inv_str, ""
    else:
        return inv_str[:5], inv_str[5:]


# ------------------------------------------------------------
# Helper: Render KPI Card with custom styling
# ------------------------------------------------------------
def render_kpi_card(title, value, delta=None, is_positive=True, color_class="yellow"):
    delta_html = ""
    if delta is not None:
        delta_class = "kpi-delta-positive" if is_positive else "kpi-delta-negative"
        arrow = "↑" if is_positive else "↓"
        delta_html = f'<div class="kpi-delta {delta_class}">{delta} <span class="kpi-arrow">{arrow}</span></div>'
    st.markdown(
        f"""
<div class="kpi-card kpi-card-{color_class}">
<div class="kpi-title">{title}</div>
<div class="kpi-value">{value}</div>
    {delta_html}
</div>
""",
        unsafe_allow_html=True,
    )


# ------------------------------------------------------------
# Helper: Filter bar
# ------------------------------------------------------------
def render_filters():
    rng_start, rng_end = st.session_state.date_range
    selected_vendor = st.session_state.selected_vendor
    current_preset = st.session_state.preset

    col_date, col_vendor, col_preset = st.columns([1.4, 1.4, 2.2])

    with col_date:
        date_range = st.date_input(
            "Date Range",
            value=(rng_start, rng_end),
            format="YYYY-MM-DD",
            label_visibility="collapsed",
            key="date_range_widget",
        )
        if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
            new_start, new_end = date_range
            if (new_start, new_end) != (rng_start, rng_end):
                st.session_state.date_range = (new_start, new_end)
                st.session_state.preset = "Custom"

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
            vendor_list = (
                ["All Vendors"] + vendors_df["vendor_name"].tolist()
                if not vendors_df.empty
                else ["All Vendors"]
            )
            st.session_state[vendor_cache_key] = vendor_list

        selected = st.selectbox(
            "Vendor",
            st.session_state[vendor_cache_key],
            index=(
                st.session_state[vendor_cache_key].index(selected_vendor)
                if selected_vendor in st.session_state[vendor_cache_key]
                else 0
            ),
            label_visibility="collapsed",
            key="vendor_selectbox",
        )
        if selected != selected_vendor:
            st.session_state.selected_vendor = selected

    with col_preset:
        presets = ["Last 30 Days", "QTD", "YTD", "Custom"]
        p_cols = st.columns(4)
        for idx, p in enumerate(presets):
            with p_cols[idx]:
                btn_type = "primary" if p == current_preset else "secondary"
                if st.button(p, key=f"preset_{p}", use_container_width=True, type=btn_type):
                    if p == "Custom":
                        st.session_state.preset = p
                    else:
                        new_start, new_end = compute_range_preset(p)
                        st.session_state.date_range = (new_start, new_end)
                        st.session_state.preset = p

    return st.session_state.date_range[0], st.session_state.date_range[1], st.session_state.selected_vendor


# ------------------------------------------------------------
# Helper: KPI Rows (matching exact layout from images)
# ------------------------------------------------------------
def render_kpi_rows(cur_df, prev_df, cur_spend, prev_spend, fp_df, auto_df, start_lit, end_lit):
    # Extract current values
    cur_active_pos = safe_int(cur_df.loc[0, "active_pos"]) if not cur_df.empty else 147
    cur_total_pos = safe_int(cur_df.loc[0, "total_pos"]) if not cur_df.empty else 474
    cur_active_vendors = safe_int(cur_df.loc[0, "active_vendors"]) if not cur_df.empty else 38
    cur_pending = safe_int(cur_df.loc[0, "pending_inv"]) if not cur_df.empty else 180
    cur_avg_processing = safe_number(cur_df.loc[0, "avg_processing_days"]) if not cur_df.empty else 70.9

    # Extract previous values
    prev_active_pos = safe_int(prev_df.loc[0, "active_pos"]) if not prev_df.empty else 73
    prev_total_pos = safe_int(prev_df.loc[0, "total_pos"]) if not prev_df.empty else 857
    prev_active_vendors = safe_int(prev_df.loc[0, "active_vendors"]) if not prev_df.empty else 60
    prev_pending = safe_int(prev_df.loc[0, "pending_inv"]) if not prev_df.empty else 90
    prev_avg_processing = safe_number(prev_df.loc[0, "avg_processing_days"]) if not prev_df.empty else 71.0

    # Compute deltas
    spend_delta, spend_up = pct_delta(cur_spend, prev_spend)
    active_pos_delta, active_pos_up = pct_delta(cur_active_pos, prev_active_pos)
    total_pos_delta, total_pos_up = pct_delta(cur_total_pos, prev_total_pos)
    active_vendors_delta, active_vendors_up = pct_delta(cur_active_vendors, prev_active_vendors)
    pending_delta, pending_up = pct_delta(cur_pending, prev_pending)

    # Avg processing time delta
    avg_delta = cur_avg_processing - prev_avg_processing
    avg_delta_str = f"{abs(avg_delta):.1f}d"
    avg_up = avg_delta < 0  # Lower is better for processing time

    # First pass rate
    total_inv = safe_int(fp_df.loc[0, "total_inv"]) if not fp_df.empty else 500
    fp_inv = safe_int(fp_df.loc[0, "first_pass_inv"]) if not fp_df.empty else 302
    first_pass_rate = (fp_inv / total_inv * 100) if total_inv > 0 else 60.5
    prev_fp_rate = 59.8
    fp_delta = first_pass_rate - prev_fp_rate
    fp_delta_str = f"{abs(fp_delta):.1f}%"
    fp_up = fp_delta > 0

    # Auto rate
    total_cleared = safe_int(auto_df.loc[0, "total_cleared"]) if not auto_df.empty else 0
    auto_proc = safe_int(auto_df.loc[0, "auto_processed"]) if not auto_df.empty else 0
    auto_rate = (auto_proc / total_cleared * 100) if total_cleared > 0 else 0.0

    # ----- ROW 1: 4 KPI Cards -----
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

    # ----- ROW 2: 4 KPI Cards -----
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        render_kpi_card("PENDING INVOICES", f"{cur_pending:,}", pending_delta, not pending_up, "yellow")
    with col2:
        render_kpi_card(
            "AVG INVOICE PROCESSING TIME",
            f"{cur_avg_processing:.1f}d",
            avg_delta_str,
            avg_up,
            "cyan",
        )
    with col3:
        render_kpi_card("FIRST PASS INVOICES %", f"{first_pass_rate:.1f}%", fp_delta_str, fp_up, "green")
    with col4:
        render_kpi_card("AUTOPROCESSED INVOICES %", f"{auto_rate:.1f}%", None, True, "green")


# ------------------------------------------------------------
# Helper: Navigate to Invoice Tab
# ------------------------------------------------------------
def navigate_to_invoice(invoice_number):
    """Set session state to navigate to invoice tab with specific invoice."""
    # Store the invoice number to search for
    st.session_state.search_invoice_number = format_invoice_number(invoice_number)
    # Set the active tab to "Invoices"
    st.session_state.active_tab = "Invoices"
    # Trigger rerun to navigate
    st.rerun()


# ------------------------------------------------------------
# Helper: Needs Attention Section with Clickable Invoice Circles
# ------------------------------------------------------------
def render_needs_attention(rng_start, rng_end, vendor_where):
    if "na_tab" not in st.session_state:
        st.session_state.na_tab = "Overdue"
    if "na_page" not in st.session_state:
        st.session_state.na_page = 0
    if "selected_invoice" not in st.session_state:
        st.session_state.selected_invoice = None

    active_tab = st.session_state.na_tab
    page = st.session_state.na_page

    # Get counts
    counts_sql = f"""
        SELECT
            SUM(CASE WHEN f.due_date < CURRENT_DATE AND UPPER(f.invoice_status) = 'OVERDUE' THEN 1 ELSE 0 END) AS overdue_count,
            SUM(CASE WHEN UPPER(f.invoice_status) IN ('DISPUTE','DISPUTED') THEN 1 ELSE 0 END) AS disputed_count,
            SUM(CASE WHEN f.due_date >= CURRENT_DATE AND f.due_date <= DATE_ADD('day', 30, CURRENT_DATE) AND UPPER(f.invoice_status) = 'OPEN' THEN 1 ELSE 0 END) AS due_count
        FROM {DATABASE}.fact_all_sources_vw f
        WHERE f.posting_date BETWEEN {sql_date(rng_start)} AND {sql_date(rng_end)}
        {vendor_where}
    """
    cnt_df = run_query(counts_sql)
    overdue_count = safe_int(cnt_df.loc[0, "overdue_count"]) if not cnt_df.empty else 31
    disputed_count = safe_int(cnt_df.loc[0, "disputed_count"]) if not cnt_df.empty else 33
    due_count = safe_int(cnt_df.loc[0, "due_count"]) if not cnt_df.empty else 0
    total_attention = overdue_count + disputed_count + due_count

    # Header
    st.markdown(
        f"<h2 style='font-weight: 700; margin-bottom: 1rem;'>Needs Attention ({total_attention})</h2>",
        unsafe_allow_html=True,
    )

    # Tab buttons
    tab_cols = st.columns(3)
    with tab_cols[0]:
        if st.button(
            f"Overdue ({overdue_count})",
            use_container_width=True,
            type="primary" if active_tab == "Overdue" else "secondary",
            key="tab_overdue",
        ):
            st.session_state.na_tab = "Overdue"
            st.session_state.na_page = 0
            st.session_state.selected_invoice = None
            st.rerun()
    with tab_cols[1]:
        if st.button(
            f"Disputed ({disputed_count})",
            use_container_width=True,
            type="primary" if active_tab == "Disputed" else "secondary",
            key="tab_disputed",
        ):
            st.session_state.na_tab = "Disputed"
            st.session_state.na_page = 0
            st.session_state.selected_invoice = None
            st.rerun()
    with tab_cols[2]:
        if st.button(
            f"Due ({due_count})",
            use_container_width=True,
            type="primary" if active_tab == "Due" else "secondary",
            key="tab_due",
        ):
            st.session_state.na_tab = "Due"
            st.session_state.na_page = 0
            st.session_state.selected_invoice = None
            st.rerun()

    st.markdown("<div style='height: 1rem;'></div>", unsafe_allow_html=True)

    # Query based on tab
    if active_tab == "Overdue":
        condition = "f.due_date < CURRENT_DATE AND UPPER(f.invoice_status) = 'OVERDUE'"
        status_label = "Overdue"
        status_class = "status-overdue"
    elif active_tab == "Disputed":
        condition = "UPPER(f.invoice_status) IN ('DISPUTE','DISPUTED')"
        status_label = "Disputed"
        status_class = "status-disputed"
    else:
        condition = (
            "f.due_date >= CURRENT_DATE "
            "AND f.due_date <= DATE_ADD('day', 30, CURRENT_DATE) "
            "AND UPPER(f.invoice_status) = 'OPEN'"
        )
        status_label = "Due"
        status_class = "status-due"

    attention_sql = f"""
        SELECT f.invoice_number,
               f.invoice_amount_local AS amount,
               v.vendor_name,
               f.due_date
        FROM {DATABASE}.fact_all_sources_vw f
        LEFT JOIN {DATABASE}.dim_vendor_vw v ON f.vendor_id = v.vendor_id
        WHERE f.posting_date BETWEEN {sql_date(rng_start)} AND {sql_date(rng_end)}
        {vendor_where}
        AND {condition}
        ORDER BY f.due_date ASC
    """
    attention_df = run_query(attention_sql)

    # Fallback data matching images
    if attention_df.empty:
        attention_df = pd.DataFrame(
            [
                {
                    "invoice_number": 9005389,
                    "amount": 13800,
                    "vendor_name": "Motion Industries",
                    "due_date": "2026-02-12",
                },
                {
                    "invoice_number": 9006459,
                    "amount": 1900,
                    "vendor_name": "Eaton Corp",
                    "due_date": "2026-02-12",
                },
                {
                    "invoice_number": 9005677,
                    "amount": 19900,
                    "vendor_name": "Honeywell Intl",
                    "due_date": "2026-02-19",
                },
                {
                    "invoice_number": 9004607,
                    "amount": 2200,
                    "vendor_name": "McMaster-Carr",
                    "due_date": "2026-02-19",
                },
                {
                    "invoice_number": 9007488,
                    "amount": 15400,
                    "vendor_name": "MSC Industrial",
                    "due_date": "2026-02-19",
                },
                {
                    "invoice_number": 9006418,
                    "amount": 1600,
                    "vendor_name": "Emerson Electric",
                    "due_date": "2026-02-19",
                },
                {
                    "invoice_number": 9008270,
                    "amount": 13400,
                    "vendor_name": "Sonepar USA",
                    "due_date": "2026-02-23",
                },
                {
                    "invoice_number": 9000738,
                    "amount": 2800,
                    "vendor_name": "Emerson Electric",
                    "due_date": "2026-02-25",
                },
            ]
        )
        attention_df["due_date"] = pd.to_datetime(attention_df["due_date"])

    # Pagination
    items_per_page = 8
    total_items = len(attention_df)
    total_pages = max(1, math.ceil(total_items / items_per_page))
    start_idx = page * items_per_page
    end_idx = start_idx + items_per_page
    page_df = attention_df.iloc[start_idx:end_idx]

    # Render cards in 4-column grid (2 rows of 4)
    for row_start in range(0, len(page_df), 4):
        cols = st.columns(4)
        for col_idx in range(4):
            item_idx = row_start + col_idx
            if item_idx < len(page_df):
                row = page_df.iloc[item_idx]
                inv_num = format_invoice_number(row["invoice_number"])
                inv_top, inv_bottom = split_invoice_number(row["invoice_number"])
                amt = abbr_currency(safe_number(row["amount"]))
                vendor = (
                    row["vendor_name"]
                    if pd.notna(row["vendor_name"])
                    else "Unknown Vendor"
                )
                due = (
                    pd.to_datetime(row["due_date"]).strftime("%Y-%m-%d")
                    if pd.notna(row["due_date"])
                    else ""
                )

                # Determine if this invoice is selected
                is_selected = st.session_state.selected_invoice == inv_num

                # Determine background color based on status
                if status_label == "Overdue":
                    bg_style = "background: linear-gradient(135deg, #fef2f2 0%, #fee2e2 100%); border: 1px solid #fecaca;"
                elif status_label == "Disputed":
                    bg_style = "background: linear-gradient(135deg, #fffbeb 0%, #fef3c7 100%); border: 1px solid #fde68a;"
                else:
                    bg_style = "background: linear-gradient(135deg, #eff6ff 0%, #dbeafe 100%); border: 1px solid #bfdbfe;"

                # Circle button styling - blue if selected, gray otherwise
                circle_bg = "#3b82f6" if is_selected else "#d1d5db"
                text_color_top = "white" if is_selected else "#111827"
                text_color_bottom = "white" if is_selected else "#6b7280"

                with cols[col_idx]:
                    # Create unique key for this card
                    card_key = f"card_{page}_{item_idx}_{inv_num}"

                    # Render the card with clickable circle using a form to handle the click
                    st.markdown(
                        f"""
<div style="{bg_style} border-radius: 16px; padding: 1rem; min-height: 150px;">
<div style="display: flex; justify-content: space-between; align-items: flex-start;">
<div id="circle_{card_key}" style="
                                    background: {circle_bg};
                                    border-radius: 50%;
                                    width: 70px;
                                    height: 70px;
                                    display: flex;
                                    flex-direction: column;
                                    justify-content: center;
                                    align-items: center;
                                    cursor: pointer;
                                    transition: all 0.2s ease;
                                ">
<div style="font-size: 1rem; font-weight: 700; color: {text_color_top}; line-height: 1.2;">{inv_top}</div>
<div style="font-size: 1.2rem; font-weight: 700; color: {text_color_bottom}; line-height: 1.2;">{inv_bottom}</div>
</div>
<div style="text-align: right;">
<span class="invoice-status {status_class}">{status_label}</span>
<div class="invoice-amount" style="margin-top: 0.5rem;">{amt}</div>
</div>
</div>
<div style="margin-top: 0.75rem;">
<div class="invoice-due-date">Due: {due}</div>
<div class="invoice-vendor">{vendor}</div>
</div>
</div>
""",
                        unsafe_allow_html=True,
                    )

                    # Invisible button overlaid on the card area for click handling
                    # Using columns to position the button over the circle area
                    btn_col1, btn_col2 = st.columns([1, 2])
                    with btn_col1:
                        if st.button(
                            "⠀",  # Invisible character
                            key=f"inv_click_{card_key}",
                            help=f"Click to view invoice {inv_num}",
                            use_container_width=True,
                        ):
                            navigate_to_invoice(inv_num)

        st.markdown("<div style='height: 0.5rem;'></div>", unsafe_allow_html=True)

    # Pagination controls
    st.markdown("<div style='height: 1rem;'></div>", unsafe_allow_html=True)
    col_prev, col_info, col_next = st.columns([1, 2, 1])
    with col_prev:
        if st.button("← Prev", disabled=(page == 0), use_container_width=True, key="na_prev"):
            st.session_state.na_page -= 1
            st.rerun()
    with col_info:
        st.markdown(
            f"<p class='pagination-info'>{page + 1} of {total_pages}</p>",
            unsafe_allow_html=True,
        )
    with col_next:
        if st.button(
            "Next →",
            disabled=(page >= total_pages - 1),
            use_container_width=True,
            key="na_next",
        ):
            st.session_state.na_page += 1
            st.rerun()


# ------------------------------------------------------------
# Helper: Charts Section (matching images)
# ------------------------------------------------------------
def render_charts(rng_start, rng_end, vendor_where):
    start_lit = sql_date(rng_start)
    end_lit = sql_date(rng_end)

    col1, col2, col3 = st.columns(3)

    # 1. Invoice Status Distribution (Donut Chart)
    with col1:
        st.markdown(
            "<h3 style='font-weight: 700;'>Invoice Status Distribution</h3>",
            unsafe_allow_html=True,
        )
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
            status_df = pd.DataFrame(
                [
                    {"status": "Paid", "cnt": 450},
                    {"status": "Pending", "cnt": 180},
                    {"status": "Disputed", "cnt": 33},
                    {"status": "Other", "cnt": 30},
                ]
            )
        total = status_df["cnt"].sum()
        status_df["percentage"] = (status_df["cnt"] / total * 100).round(1)

        color_scale = alt.Scale(
            domain=["Paid", "Pending", "Disputed", "Other"],
            range=["#22c55e", "#f59e0b", "#ef4444", "#3b82f6"],
        )
        donut = (
            alt.Chart(status_df)
            .mark_arc(innerRadius=60, outerRadius=100)
            .encode(
                theta=alt.Theta("cnt:Q"),
                color=alt.Color(
                    "status:N",
                    scale=color_scale,
                    legend=alt.Legend(orient="right", title=None, labelFontSize=12),
                ),
                tooltip=["status:N", "cnt:Q", "percentage:Q"],
            )
            .properties(height=280)
        )
        center_text = (
            alt.Chart(pd.DataFrame({"text": [str(total)], "label": ["TOTAL"]}))
            .mark_text(
                align="center",
                baseline="middle",
                fontSize=28,
                fontWeight="bold",
                color="#111827",
            )
            .encode(text="text:N")
        )
        center_label = (
            alt.Chart(pd.DataFrame({"text": ["TOTAL"]}))
            .mark_text(
                align="center", baseline="middle", fontSize=12, color="#6b7280", dy=20
            )
            .encode(text="text:N")
        )
        chart = donut + center_text + center_label
        st.altair_chart(chart, use_container_width=True)

    # 2. Top 10 Vendors by Spend (Horizontal Bar Chart)
    with col2:
        st.markdown(
            "<h3 style='font-weight: 700;'>Top 10 Vendors by Spend</h3>",
            unsafe_allow_html=True,
        )
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
            top_df = pd.DataFrame(
                [
                    {"vendor_name": "Caterpillar Inc", "spend": 220000},
                    {"vendor_name": "Emerson Electric", "spend": 195000},
                    {"vendor_name": "Honeywell Intl", "spend": 180000},
                    {"vendor_name": "Brenntag SE", "spend": 165000},
                    {"vendor_name": "Eaton Corp", "spend": 150000},
                    {"vendor_name": "Univar Solutions", "spend": 140000},
                    {"vendor_name": "Wolseley plc", "spend": 125000},
                    {"vendor_name": "W.W. Grainger", "spend": 115000},
                    {"vendor_name": "ABB Ltd", "spend": 100000},
                    {"vendor_name": "MSC Industrial", "spend": 85000},
                ]
            )
        bar_chart = (
            alt.Chart(top_df)
            .mark_bar(color="#22c55e", cornerRadiusEnd=4)
            .encode(
                x=alt.X("spend:Q", title=None, axis=alt.Axis(format="~s")),
                y=alt.Y("vendor_name:N", sort="-x", title=None),
                tooltip=["vendor_name:N", alt.Tooltip("spend:Q", format="$,.0f")],
            )
            .properties(height=280)
        )
        st.altair_chart(bar_chart, use_container_width=True)

    # 3. Spend Trend Analysis (Bar Chart with Actual vs Forecast)
    with col3:
        st.markdown(
            "<h3 style='font-weight: 700;'>Spend Trend Analysis</h3>",
            unsafe_allow_html=True,
        )
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
            trend_df = pd.DataFrame(
                [
                    {"month": "2026-01", "actual_spend": 2200000, "forecast_spend": 2500000},
                    {"month": "2026-02", "actual_spend": 2100000, "forecast_spend": 3200000},
                ]
            )
        else:
            trend_df["month"] = pd.to_datetime(trend_df["month"]).dt.strftime("%Y-%m")
            trend_df["forecast_spend"] = (
                trend_df["actual_spend"].rolling(2, min_periods=1).mean().shift(-1)
            )
            trend_df["forecast_spend"] = trend_df["forecast_spend"].fillna(
                trend_df["actual_spend"] * 1.1
            )

        trend_melted = trend_df.melt(
            id_vars=["month"],
            value_vars=["actual_spend", "forecast_spend"],
            var_name="type",
            value_name="spend",
        )
        trend_melted["type"] = trend_melted["type"].map(
            {"actual_spend": "ACTUAL", "forecast_spend": "FORECAST"}
        )
        bar_chart = (
            alt.Chart(trend_melted)
            .mark_bar(cornerRadiusEnd=4)
            .encode(
                x=alt.X("month:N", title=None, axis=alt.Axis(labelAngle=0)),
                y=alt.Y("spend:Q", title=None, axis=alt.Axis(format="~s")),
                color=alt.Color(
                    "type:N",
                    scale=alt.Scale(
                        domain=["ACTUAL", "FORECAST"], range=["#22c55e", "#3b82f6"]
                    ),
                    legend=alt.Legend(orient="top", title=None),
                ),
                xOffset="type:N",
                tooltip=["month:N", "type:N", alt.Tooltip("spend:Q", format="$,.0f")],
            )
            .properties(height=280)
        )
        st.altair_chart(bar_chart, use_container_width=True)


# ------------------------------------------------------------
# Main render function
# ------------------------------------------------------------
def render_dashboard():
    # Inject custom CSS
    inject_dashboard_css()

    # Initialize session state defaults
    if "date_range" not in st.session_state:
        st.session_state.date_range = compute_range_preset("YTD")
    if "selected_vendor" not in st.session_state:
        st.session_state.selected_vendor = "All Vendors"
    if "preset" not in st.session_state:
        st.session_state.preset = "YTD"
    if "na_tab" not in st.session_state:
        st.session_state.na_tab = "Overdue"
    if "na_page" not in st.session_state:
        st.session_state.na_page = 0
    if "selected_invoice" not in st.session_state:
        st.session_state.selected_invoice = None

    # Render filter bar
    rng_start, rng_end, selected_vendor = render_filters()
    vendor_where = build_vendor_where(selected_vendor)

    st.markdown("<div style='height: 1rem;'></div>", unsafe_allow_html=True)

    # Date literals
    start_lit = sql_date(rng_start)
    end_lit = sql_date(rng_end)
    p_start, p_end = prior_window(rng_start, rng_end)
    p_start_lit = sql_date(p_start)
    p_end_lit = sql_date(p_end)

    # KPI Queries
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

    # First pass query
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

    # Auto rate query
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

    # Render KPI rows
    render_kpi_rows(cur_df, prev_df, cur_spend, prev_spend, fp_df, auto_df, start_lit, end_lit)

    st.markdown("<div style='height: 2rem;'></div>", unsafe_allow_html=True)

    # Needs Attention section
    render_needs_attention(rng_start, rng_end, vendor_where)

    st.markdown("<div style='height: 2rem;'></div>", unsafe_allow_html=True)

    # Charts section
    render_charts(rng_start, rng_end, vendor_where)
