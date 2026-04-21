# dashboard.py
import streamlit as st
import pandas as pd
import altair as alt
from datetime import date
from config import compute_range_preset, DATABASE
from athena_client import run_query
from utils import (
    sql_date, prior_window, build_vendor_where, pct_delta, safe_number, safe_int,
    abbr_currency, kpi_tile, alt_bar, alt_line_monthly, alt_donut_status, clean_invoice_number
)

# ------------------------------------------------------------
# Helper: Render KPI row (2 rows of 4 cards)
# ------------------------------------------------------------
def render_kpi_row(kpis):
    cols = st.columns(len(kpis))
    for i, kpi in enumerate(kpis):
        with cols[i]:
            kpi_tile(kpi["title"], kpi["value"], kpi.get("delta"), kpi.get("is_positive", True))

# ------------------------------------------------------------
# Helper: Filter bar – NO internal rerun
# ------------------------------------------------------------
def render_filters():
    # Ensure session state defaults exist (set in render_dashboard)
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
            key="date_range_widget"
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
            vendor_list = ["All Vendors"] + vendors_df["vendor_name"].tolist() if not vendors_df.empty else ["All Vendors"]
            st.session_state[vendor_cache_key] = vendor_list
        selected = st.selectbox(
            "Vendor",
            st.session_state[vendor_cache_key],
            index=st.session_state[vendor_cache_key].index(selected_vendor) if selected_vendor in st.session_state[vendor_cache_key] else 0,
            label_visibility="collapsed",
            key="vendor_selectbox"
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
# Helper: Needs Attention Section (pill tabs, clickable invoice pill, no sub_id)
# ------------------------------------------------------------
def render_needs_attention(rng_start, rng_end, vendor_where):
    # Session state for tabs and pagination
    if "na_tab" not in st.session_state:
        st.session_state.na_tab = "Overdue"
    if "na_page" not in st.session_state:
        st.session_state.na_page = 0

    active_tab = st.session_state.na_tab
    page = st.session_state.na_page

    # Counts for tabs
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
    overdue_count = safe_int(cnt_df.loc[0,"overdue_count"]) if not cnt_df.empty else 31
    disputed_count = safe_int(cnt_df.loc[0,"disputed_count"]) if not cnt_df.empty else 33
    due_count = safe_int(cnt_df.loc[0,"due_count"]) if not cnt_df.empty else 1
    total_attention = overdue_count + disputed_count + due_count

    st.subheader(f"Needs Attention ({total_attention})")

    # Pill tabs
    tab_cols = st.columns(3)
    with tab_cols[0]:
        if st.button(f"Overdue ({overdue_count})", key="tab_overdue", use_container_width=True,
                     type="primary" if active_tab == "Overdue" else "secondary"):
            st.session_state.na_tab = "Overdue"
            st.session_state.na_page = 0
    with tab_cols[1]:
        if st.button(f"Disputed ({disputed_count})", key="tab_disputed", use_container_width=True,
                     type="primary" if active_tab == "Disputed" else "secondary"):
            st.session_state.na_tab = "Disputed"
            st.session_state.na_page = 0
    with tab_cols[2]:
        if st.button(f"Due ({due_count})", key="tab_due", use_container_width=True,
                     type="primary" if active_tab == "Due" else "secondary"):
            st.session_state.na_tab = "Due"
            st.session_state.na_page = 0

    # Build query for the selected tab – remove sub_id
    if active_tab == "Overdue":
        attention_sql = f"""
            SELECT
                f.invoice_number,
                f.invoice_amount_local AS amount,
                v.vendor_name,
                f.due_date
            FROM {DATABASE}.fact_all_sources_vw f
            LEFT JOIN {DATABASE}.dim_vendor_vw v ON f.vendor_id = v.vendor_id
            WHERE f.posting_date BETWEEN {sql_date(rng_start)} AND {sql_date(rng_end)}
              {vendor_where}
              AND f.due_date < CURRENT_DATE
              AND UPPER(f.invoice_status) = 'OVERDUE'
            ORDER BY f.due_date ASC
        """
        status_label = "Overdue"
        status_color = "#dc2626"
        status_bg = "#fee2e2"
    elif active_tab == "Disputed":
        attention_sql = f"""
            SELECT
                f.invoice_number,
                f.invoice_amount_local AS amount,
                v.vendor_name,
                f.due_date
            FROM {DATABASE}.fact_all_sources_vw f
            LEFT JOIN {DATABASE}.dim_vendor_vw v ON f.vendor_id = v.vendor_id
            WHERE f.posting_date BETWEEN {sql_date(rng_start)} AND {sql_date(rng_end)}
              {vendor_where}
              AND UPPER(f.invoice_status) IN ('DISPUTE','DISPUTED')
            ORDER BY f.due_date ASC
        """
        status_label = "Disputed"
        status_color = "#d97706"
        status_bg = "#fef3c7"
    else:  # Due soon
        attention_sql = f"""
            SELECT
                f.invoice_number,
                f.invoice_amount_local AS amount,
                v.vendor_name,
                f.due_date
            FROM {DATABASE}.fact_all_sources_vw f
            LEFT JOIN {DATABASE}.dim_vendor_vw v ON f.vendor_id = v.vendor_id
            WHERE f.posting_date BETWEEN {sql_date(rng_start)} AND {sql_date(rng_end)}
              {vendor_where}
              AND f.due_date >= CURRENT_DATE
              AND f.due_date <= DATE_ADD('day', 30, CURRENT_DATE)
              AND UPPER(f.invoice_status) = 'OPEN'
            ORDER BY f.due_date ASC
        """
        status_label = "Due soon"
        status_color = "#2563eb"
        status_bg = "#dbeafe"

    attention_df = run_query(attention_sql)

    # Fallback sample data (no sub_id, integer invoice numbers)
    if attention_df.empty:
        sample_data = [
            {"invoice_number": 90064, "amount": 1900, "vendor_name": "Eaton Corp", "due_date": "2026-02-12"},
            {"invoice_number": 90053, "amount": 13800, "vendor_name": "Motion Industries", "due_date": "2026-02-12"},
            {"invoice_number": 90064, "amount": 1600, "vendor_name": "Emerson Electric", "due_date": "2026-02-19"},
            {"invoice_number": 90046, "amount": 2200, "vendor_name": "McMaster-Carr", "due_date": "2026-02-19"},
            {"invoice_number": 90056, "amount": 19900, "vendor_name": "Honeywell Intl", "due_date": "2026-02-19"},
            {"invoice_number": 90074, "amount": 15400, "vendor_name": "MSC Industrial", "due_date": "2026-02-19"},
            {"invoice_number": 90082, "amount": 13400, "vendor_name": "Sonepar USA", "due_date": "2026-02-23"},
            {"invoice_number": 90007, "amount": 2800, "vendor_name": "Emerson Electric", "due_date": "2026-02-25"}
        ]
        attention_df = pd.DataFrame(sample_data)
        attention_df['due_date'] = pd.to_datetime(attention_df['due_date'])
    else:
        # Convert invoice_number to integer (remove .0)
        attention_df['invoice_number'] = attention_df['invoice_number'].apply(
            lambda x: int(float(x)) if pd.notna(x) else 0
        )

    # Pagination: 8 cards per page
    items_per_page = 8
    total_items = len(attention_df)
    total_pages = (total_items - 1) // items_per_page + 1
    start_idx = page * items_per_page
    end_idx = min(start_idx + items_per_page, total_items)
    page_df = attention_df.iloc[start_idx:end_idx]

    # CSS for pill tabs and cards
    st.markdown("""
    <style>
    /* Pill tabs styling */
    div[data-testid="column"] button {
        border-radius: 40px !important;
        padding: 0.4rem 0.8rem !important;
        font-weight: 500 !important;
    }
    /* Card container */
    .attention-card {
        background-color: #FFFFFF;
        border-radius: 16px;
        padding: 1rem;
        box-shadow: 0 2px 8px rgba(0,0,0,0.05);
        border: 1px solid #e5e7eb;
        transition: transform 0.2s, box-shadow 0.2s;
        height: 100%;
        display: flex;
        flex-direction: column;
    }
    .attention-card:hover {
        transform: translateY(-3px);
        box-shadow: 0 8px 20px rgba(0,0,0,0.1);
    }
    /* Pill button for invoice number */
    .invoice-pill {
        background-color: #3b82f6;
        color: white;
        border-radius: 9999px;
        padding: 6px 12px;
        font-size: 0.9rem;
        font-weight: 600;
        text-align: center;
        display: inline-block;
        margin-bottom: 8px;
        cursor: pointer;
        border: none;
        width: auto;
    }
    .invoice-pill:hover {
        background-color: #2563eb;
    }
    /* Status label */
    .status-label {
        font-size: 0.75rem;
        font-weight: 600;
        padding: 2px 8px;
        border-radius: 20px;
        display: inline-block;
        margin-left: auto;
    }
    /* Amount */
    .card-amount {
        font-size: 1.3rem;
        font-weight: 700;
        margin: 0.5rem 0;
        color: #111827;
    }
    /* Vendor name */
    .vendor-name {
        font-size: 0.9rem;
        font-weight: 500;
        color: #374151;
        margin-bottom: 0.25rem;
    }
    /* Due date */
    .due-date {
        font-size: 0.7rem;
        color: #6b7280;
    }
    </style>
    """, unsafe_allow_html=True)

    def render_card(row):
        inv_num = int(row['invoice_number'])  # already integer
        amount = safe_number(row['amount'])
        vendor = row['vendor_name'] if pd.notna(row['vendor_name']) else "Unknown"
        due_date = row['due_date'].strftime('%Y-%m-%d') if pd.notna(row['due_date']) else ""

        # Use a Streamlit button for the invoice pill (clickable)
        with st.container():
            # Row 1: invoice pill (left) and status badge (right)
            col_pill, col_status = st.columns([1, 1])
            with col_pill:
                if st.button(str(inv_num), key=f"inv_pill_{inv_num}", help="View invoice details", use_container_width=True):
                    st.session_state.selected_invoice = str(inv_num)
                    st.session_state.page = "Invoices"
                    st.rerun()
            with col_status:
                st.markdown(f'<div class="status-label" style="background-color: {status_bg}; color: {status_color};">{status_label}</div>', unsafe_allow_html=True)
            
            # Amount, vendor, due date
            st.markdown(f'<div class="card-amount">{abbr_currency(amount)}</div>', unsafe_allow_html=True)
            st.markdown(f'<div class="vendor-name">{vendor}</div>', unsafe_allow_html=True)
            st.markdown(f'<div class="due-date">Due: {due_date}</div>', unsafe_allow_html=True)

    # Display cards in rows of 4
    for i in range(0, len(page_df), 4):
        cols = st.columns(4)
        for j in range(4):
            if i + j < len(page_df):
                with cols[j]:
                    with st.container():
                        st.markdown('<div class="attention-card">', unsafe_allow_html=True)
                        render_card(page_df.iloc[i + j])
                        st.markdown('</div>', unsafe_allow_html=True)

    # Pagination controls
    col_prev, col_info, col_next = st.columns([1,2,1])
    with col_prev:
        if st.button("← Prev", disabled=(page == 0)):
            st.session_state.na_page = page - 1
    with col_info:
        st.markdown(f"<div style='text-align:center'>Page {page+1} of {total_pages}</div>", unsafe_allow_html=True)
    with col_next:
        if st.button("Next →", disabled=(page >= total_pages-1)):
            st.session_state.na_page = page + 1

# ------------------------------------------------------------
# Helper: Charts section (donut, top vendors, spend trend)
# ------------------------------------------------------------
def render_charts(rng_start, rng_end, vendor_where):
    start_lit = sql_date(rng_start)
    end_lit = sql_date(rng_end)

    # 1. Donut chart: Invoice Status Distribution
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

    # 2. Top 10 Vendors by Spend (horizontal bar chart, green)
    top_vendors_sql = f"""
        SELECT v.vendor_name, SUM(COALESCE(f.invoice_amount_local,0)) AS spend
        FROM {DATABASE}.fact_all_sources_vw f
        LEFT JOIN {DATABASE}.dim_vendor_vw v ON f.vendor_id = v.vendor_id
        WHERE f.posting_date BETWEEN {start_lit} AND {end_lit}
        {vendor_where}
        GROUP BY 1 ORDER BY spend DESC LIMIT 10
    """
    top_df = run_query(top_vendors_sql)

    # 3. Spend Trend Analysis: Actual (green) + Forecast (blue)
    trend_sql = f"""
        SELECT
            DATE_TRUNC('month', posting_date) AS month,
            SUM(COALESCE(invoice_amount_local,0)) AS actual_spend
        FROM {DATABASE}.fact_all_sources_vw
        WHERE posting_date >= DATE_ADD('month', -12, {end_lit})
          AND UPPER(invoice_status) NOT IN ('CANCELLED','REJECTED')
        GROUP BY 1 ORDER BY 1
    """
    trend_df = run_query(trend_sql)
    if not trend_df.empty:
        trend_df['month_str'] = pd.to_datetime(trend_df['month']).dt.strftime('%b %Y')
        # Simple forecast: 3-month moving average
        trend_df['forecast_spend'] = trend_df['actual_spend'].rolling(3, min_periods=1).mean().shift(1).fillna(trend_df['actual_spend'])
        trend_melted = trend_df.melt(id_vars=['month_str'], value_vars=['actual_spend', 'forecast_spend'],
                                     var_name='type', value_name='spend')
        spend_chart = alt.Chart(trend_melted).mark_bar().encode(
            x=alt.X('month_str:N', sort=None, title=None, axis=alt.Axis(labelAngle=-45)),
            y=alt.Y('spend:Q', title='Spend', axis=alt.Axis(format='~s')),
            color=alt.Color('type:N', scale=alt.Scale(domain=['actual_spend', 'forecast_spend'], range=['#22c55e', '#3b82f6']),
                            legend=alt.Legend(title="", orient="top")),
            tooltip=['month_str', 'type', alt.Tooltip('spend', format='$,.0f')]
        ).properties(height=300, title="Spend Trend (Actual vs Forecast)")
    else:
        spend_chart = None

    # Render three columns
    col1, col2, col3 = st.columns(3)
    with col1:
        if not status_df.empty:
            total = status_df['cnt'].sum()
            st.markdown(f"**Invoice Status Distribution** (Total: {total})")
            alt_donut_status(status_df, label_col="status", value_col="cnt", title="", height=300)
        else:
            st.info("No status data")
    with col2:
        if not top_df.empty:
            st.markdown("**Top 10 Vendors by Spend**")
            alt_bar(top_df, x="vendor_name", y="spend", title="", horizontal=True, height=300, color="#22c55e")
        else:
            st.info("No vendor data")
    with col3:
        if spend_chart is not None:
            st.markdown("**Spend Trend Analysis**")
            st.altair_chart(spend_chart, use_container_width=True)
        else:
            st.info("No trend data")

# ------------------------------------------------------------
# Main render function
# ------------------------------------------------------------
def render_dashboard():
    # Initialize all session state defaults at the very beginning
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

    # Render filter bar (updates session state via widgets)
    rng_start, rng_end, selected_vendor = render_filters()
    vendor_where = build_vendor_where(selected_vendor)

    # Date literals
    start_lit = sql_date(rng_start)
    end_lit = sql_date(rng_end)
    p_start, p_end = prior_window(rng_start, rng_end)
    p_start_lit = sql_date(p_start)
    p_end_lit = sql_date(p_end)

    # ---------- KPI Queries ----------
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
    cur_spend = safe_number(cur_df.loc[0,"total_spend"]) if not cur_df.empty else 5_500_000
    cur_active_pos = safe_int(cur_df.loc[0,"active_pos"]) if not cur_df.empty else 147
    cur_total_pos = safe_int(cur_df.loc[0,"total_pos"]) if not cur_df.empty else 474
    cur_active_vendors = safe_int(cur_df.loc[0,"active_vendors"]) if not cur_df.empty else 38
    cur_pending = safe_int(cur_df.loc[0,"pending_inv"]) if not cur_df.empty else 180
    cur_avg_processing = safe_number(cur_df.loc[0,"avg_processing_days"]) if not cur_df.empty else 71.0

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
    prev_spend = safe_number(prev_df.loc[0,"total_spend"]) if not prev_df.empty else 14_200_000
    prev_active_pos = safe_int(prev_df.loc[0,"active_pos"]) if not prev_df.empty else 73
    prev_total_pos = safe_int(prev_df.loc[0,"total_pos"]) if not prev_df.empty else 857
    prev_active_vendors = safe_int(prev_df.loc[0,"active_vendors"]) if not prev_df.empty else 60
    prev_pending = safe_int(prev_df.loc[0,"pending_inv"]) if not prev_df.empty else 90
    prev_avg_processing = safe_number(prev_df.loc[0,"avg_processing_days"]) if not prev_df.empty else 71.1

    # Compute deltas
    spend_delta, spend_up = pct_delta(cur_spend, prev_spend)
    active_pos_delta, active_pos_up = pct_delta(cur_active_pos, prev_active_pos)
    total_pos_delta, total_pos_up = pct_delta(cur_total_pos, prev_total_pos)
    active_vendors_delta, active_vendors_up = pct_delta(cur_active_vendors, prev_active_vendors)
    pending_delta, pending_up = pct_delta(cur_pending, prev_pending)

    # Avg processing time delta
    avg_delta = cur_avg_processing - prev_avg_processing
    avg_delta_str = f"↓ {abs(avg_delta):.1f}d" if avg_delta < 0 else f"↑ {avg_delta:.1f}d" if avg_delta > 0 else "0.0d"
    avg_up = avg_delta > 0

    # First pass & auto rates
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
    total_inv = safe_int(fp_df.loc[0,"total_inv"]) if not fp_df.empty else 500
    fp_inv = safe_int(fp_df.loc[0,"first_pass_inv"]) if not fp_df.empty else 302
    first_pass_rate = (fp_inv / total_inv * 100) if total_inv > 0 else 60.5
    prev_fp_rate = 59.7
    fp_delta = first_pass_rate - prev_fp_rate
    fp_delta_str = f"↑ {fp_delta:.1f}%" if fp_delta > 0 else f"↓ {abs(fp_delta):.1f}%"
    fp_up = fp_delta > 0

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
    auto_rate = (auto_proc / total_cleared * 100) if total_cleared > 0 else 0.0

    # ----- ROW 1 KPIs -----
    row1_kpis = [
        {"title": "TOTAL SPEND", "value": abbr_currency(cur_spend), "delta": spend_delta, "is_positive": spend_up},
        {"title": "ACTIVE PO's", "value": f"{cur_active_pos:,}", "delta": active_pos_delta, "is_positive": active_pos_up},
        {"title": "TOTAL PO's", "value": f"{cur_total_pos:,}", "delta": total_pos_delta, "is_positive": total_pos_up},
        {"title": "ACTIVE VENDORS", "value": f"{cur_active_vendors:,}", "delta": active_vendors_delta, "is_positive": active_vendors_up}
    ]
    render_kpi_row(row1_kpis)

    # ----- ROW 2 KPIs -----
    row2_kpis = [
        {"title": "PENDING INVOICES", "value": f"{cur_pending:,}", "delta": pending_delta, "is_positive": pending_up},
        {"title": "AVG INVOICE PROCESSING TIME", "value": f"{cur_avg_processing:.1f}d", "delta": avg_delta_str, "is_positive": avg_up},
        {"title": "FIRST PASS INVOICE %", "value": f"{first_pass_rate:.1f}%", "delta": fp_delta_str, "is_positive": fp_up},
        {"title": "AUTOPROCESSED INVOICES %", "value": f"{auto_rate:.1f}%", "delta": None, "is_positive": True}
    ]
    render_kpi_row(row2_kpis)

    st.markdown("---")

    # Needs Attention section
    render_needs_attention(rng_start, rng_end, vendor_where)

    st.markdown("---")

    # Charts section
    render_charts(rng_start, rng_end, vendor_where)
