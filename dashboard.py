# dashboard.py
import streamlit as st
from datetime import date
from config import compute_range_preset
from athena_client import run_query
from utils import (
    sql_date, prior_window, build_vendor_where, pct_delta, safe_number, safe_int,
    abbr_currency, kpi_tile, alt_bar, alt_line_monthly, alt_donut_status, clean_invoice_number
)

def render_dashboard():
    if "preset" not in st.session_state:
        st.session_state.preset = "Last 30 Days"
    if "date_range" not in st.session_state:
        st.session_state.date_range = compute_range_preset(st.session_state.preset)
    if "na_tab" not in st.session_state:
        st.session_state.na_tab = "Overdue"
    if "na_page" not in st.session_state:
        st.session_state.na_page = 0

    from config import DATABASE

    # Filter bar: Date Range, Vendor, Preset buttons
    col_date, col_vendor, col_preset = st.columns([1.4, 1.4, 2.2])
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

    # KPI queries
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

    spend_delta, spend_up = pct_delta(cur_spend, prev_spend)
    active_pos_delta, active_pos_up = pct_delta(cur_active_pos, prev_active_pos)
    total_pos_delta, total_pos_up = pct_delta(cur_total_pos, prev_total_pos)
    active_vendors_delta, active_vendors_up = pct_delta(cur_active_vendors, prev_active_vendors)
    pending_delta, pending_up = pct_delta(cur_pending, prev_pending)

    # First pass invoices rate
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

    # Auto‑processed rate
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

    # Row 1 KPIs
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        kpi_tile("TOTAL SPEND", abbr_currency(cur_spend), spend_delta, spend_up)
    with col2:
        kpi_tile("ACTIVE PO'S", f"{cur_active_pos:,}", active_pos_delta, active_pos_up)
    with col3:
        kpi_tile("TOTAL PO'S", f"{cur_total_pos:,}", total_pos_delta, total_pos_up)
    with col4:
        kpi_tile("ACTIVE VENDORS", f"{cur_active_vendors:,}", active_vendors_delta, active_vendors_up)

    # Row 2 KPIs
    col5, col6, col7, col8 = st.columns(4)
    with col5:
        kpi_tile("PENDING INVOICES", f"{cur_pending:,}", pending_delta, pending_up)
    with col6:
        kpi_tile("AVG INVOICE PROCESSING TIME", f"{cur_avg_processing:.1f}d")
    with col7:
        kpi_tile("FIRST PASS INVOICES %", f"{first_pass_rate:.1f}%")
    with col8:
        kpi_tile("AUTOPROCESSED %", f"{auto_rate:.1f}%")
    st.markdown("---")

    # Needs Attention
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

    st.subheader(f"Needs Attention ({overdue_count + disputed_count + due_count})")
    tab_cols = st.columns(3)
    active_tab = st.session_state.na_tab
    with tab_cols[0]:
        btn_overdue = st.button(
            f"Overdue ({overdue_count})",
            key="na_tab_overdue",
            use_container_width=True,
            type="primary" if active_tab == "Overdue" else "secondary"
        )
        if btn_overdue:
            st.session_state.na_tab = "Overdue"
            st.session_state.na_page = 0
            st.rerun()
    with tab_cols[1]:
        btn_disputed = st.button(
            f"Disputed ({disputed_count})",
            key="na_tab_disputed",
            use_container_width=True,
            type="primary" if active_tab == "Disputed" else "secondary"
        )
        if btn_disputed:
            st.session_state.na_tab = "Disputed"
            st.session_state.na_page = 0
            st.rerun()
    with tab_cols[2]:
        btn_due = st.button(
            f"Due ({due_count})",
            key="na_tab_due",
            use_container_width=True,
            type="primary" if active_tab == "Due" else "secondary"
        )
        if btn_due:
            st.session_state.na_tab = "Due"
            st.session_state.na_page = 0
            st.rerun()

    # Build query for the selected tab
    if active_tab == "Overdue":
        attention_sql = f"""
            SELECT
                f.invoice_number,
                f.purchase_order_reference AS sub_id,
                f.invoice_amount_local AS amount,
                v.vendor_name,
                f.due_date
            FROM {DATABASE}.fact_all_sources_vw f
            LEFT JOIN {DATABASE}.dim_vendor_vw v ON f.vendor_id = v.vendor_id
            WHERE f.posting_date BETWEEN {start_lit} AND {end_lit}
              {vendor_where}
              AND f.due_date < CURRENT_DATE
              AND UPPER(f.invoice_status) = 'OVERDUE'
            ORDER BY f.due_date ASC
        """
        status_label = "Overdue"
        bg_color = "#fef2f2"
        border_color = "#fecaca"
    elif active_tab == "Disputed":
        attention_sql = f"""
            SELECT
                f.invoice_number,
                f.purchase_order_reference AS sub_id,
                f.invoice_amount_local AS amount,
                v.vendor_name,
                f.due_date
            FROM {DATABASE}.fact_all_sources_vw f
            LEFT JOIN {DATABASE}.dim_vendor_vw v ON f.vendor_id = v.vendor_id
            WHERE f.posting_date BETWEEN {start_lit} AND {end_lit}
              {vendor_where}
              AND UPPER(f.invoice_status) IN ('DISPUTE','DISPUTED')
            ORDER BY f.due_date ASC
        """
        status_label = "Disputed"
        bg_color = "#fffbeb"
        border_color = "#fde68a"
    else:
        attention_sql = f"""
            SELECT
                f.invoice_number,
                f.purchase_order_reference AS sub_id,
                f.invoice_amount_local AS amount,
                v.vendor_name,
                f.due_date
            FROM {DATABASE}.fact_all_sources_vw f
            LEFT JOIN {DATABASE}.dim_vendor_vw v ON f.vendor_id = v.vendor_id
            WHERE f.posting_date BETWEEN {start_lit} AND {end_lit}
              {vendor_where}
              AND f.due_date >= CURRENT_DATE
              AND f.due_date <= DATE_ADD('day', 30, CURRENT_DATE)
              AND UPPER(f.invoice_status) = 'OPEN'
            ORDER BY f.due_date ASC
        """
        status_label = "Due soon"
        bg_color = "#eff6ff"
        border_color = "#bfdbfe"

    attention_df = run_query(attention_sql)

    if not attention_df.empty:
        items_per_page = 8
        total_items = len(attention_df)
        total_pages = (total_items - 1) // items_per_page + 1
        start_idx = st.session_state.na_page * items_per_page
        end_idx = min(start_idx + items_per_page, total_items)
        page_df = attention_df.iloc[start_idx:end_idx]

        st.markdown("""
        <style>
        .na-grid {
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 1rem;
            margin-bottom: 1rem;
        }
        .na-card {
            background-color: var(--card-bg);
            border: 1px solid var(--card-border);
            border-radius: 16px;
            padding: 1rem;
            text-align: center;
            cursor: pointer;
            transition: all 0.2s ease;
            box-shadow: 0 1px 2px rgba(0,0,0,0.05);
            text-decoration: none;
            display: block;
            color: inherit;
        }
        .na-card:hover {
            transform: translateY(-2px);
            box-shadow: 0 8px 16px rgba(0,0,0,0.1);
        }
        .invoice-pill {
            display: inline-block;
            background-color: #3b82f6;
            color: white;
            border-radius: 9999px;
            padding: 4px 12px;
            font-size: 0.9rem;
            font-weight: 600;
            margin-bottom: 8px;
        }
        .sub-id {
            font-size: 0.75rem;
            color: #64748b;
            margin-bottom: 8px;
        }
        .status-badge {
            display: inline-block;
            font-size: 0.7rem;
            font-weight: 600;
            padding: 2px 10px;
            border-radius: 9999px;
            margin-bottom: 8px;
        }
        .amount {
            font-size: 1.25rem;
            font-weight: 700;
            margin: 8px 0;
        }
        .vendor-name {
            font-size: 0.85rem;
            font-weight: 500;
            color: #1f2937;
            margin-bottom: 4px;
        }
        .due-date {
            font-size: 0.7rem;
            color: #6b7280;
        }
        @media (max-width: 1024px) {
            .na-grid { grid-template-columns: repeat(2, 1fr); }
        }
        @media (max-width: 640px) {
            .na-grid { grid-template-columns: 1fr; }
        }
        </style>
        """, unsafe_allow_html=True)

        for i in range(0, len(page_df), 4):
            cols = st.columns(4)
            for j in range(4):
                if i + j < len(page_df):
                    row = page_df.iloc[i + j]
                    inv_num = clean_invoice_number(row['invoice_number'])
                    sub_id = str(row.get('sub_id', '')) if pd.notna(row.get('sub_id')) else ''
                    amount = safe_number(row['amount'])
                    vendor = row['vendor_name'] if pd.notna(row['vendor_name']) else 'Unknown'
                    due_date = row['due_date'].strftime('%Y-%m-%d') if pd.notna(row['due_date']) else ''
                    if status_label == "Overdue":
                        badge_bg = "#fee2e2"
                        badge_color = "#dc2626"
                    elif status_label == "Disputed":
                        badge_bg = "#fef3c7"
                        badge_color = "#d97706"
                    else:
                        badge_bg = "#dbeafe"
                        badge_color = "#2563eb"
                    with cols[j]:
                        link_url = f"?page=Invoices&search_invoice={inv_num}"
                        st.markdown(f'''
                            <a href="{link_url}" style="text-decoration: none;">
                                <div class="na-card" style="--card-bg:{bg_color}; --card-border:{border_color}; background-color:{bg_color}; border:1px solid {border_color};">
                                    <div class="invoice-pill">{inv_num}</div>
                                    <div class="sub-id">{sub_id}</div>
                                    <div class="status-badge" style="background:{badge_bg}; color:{badge_color};">{status_label}</div>
                                    <div class="amount">{abbr_currency(amount)}</div>
                                    <div class="vendor-name">{vendor}</div>
                                    <div class="due-date">Due: {due_date}</div>
                                </div>
                            </a>
                        ''', unsafe_allow_html=True)

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

    # Charts section
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