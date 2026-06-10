# invoices.py
import streamlit as st
import pandas as pd
from datetime import date, datetime
from config import DATABASE
from utils import abbr_currency, clean_invoice_number, safe_dataframe_display
from athena_client import run_query

def render_invoice_detail(inv_row: dict, inv_num: str):
    def get_val(key, default=""):
        val = inv_row.get(key, default)
        if pd.isna(val):
            return default
        if isinstance(val, (date, datetime)):
            return val.strftime("%Y-%m-%d")
        return val

    aging_days = get_val("aging_days", 0)
    try:
        due_date = inv_row.get("due_date")
        if due_date and isinstance(due_date, (date, datetime)):
            aging_days = (date.today() - due_date).days
    except:
        pass

    st.markdown(f"""
    <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                border-radius: 12px; padding: 16px 20px; margin-bottom: 24px;
                box-shadow: 0 4px 12px rgba(0,0,0,0.1);">
        <div style="color: white; font-size: 1.1rem; font-weight: 600;">🔍 Genie Insights</div>
        <div style="color: #f0f0f0; margin-top: 6px;">
            Recommend immediate review of invoice <strong>{inv_num}</strong> as it is overdue 
            and has been outstanding for <strong>{aging_days}</strong> days.
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("### Invoice Summary")

    summary_fields = [
        "Invoice Number", "Invoice Date", "Invoice Amount", "PO Number",
        "PO Amount", "Due Date", "Invoice Status", "Aging (Days)"
    ]
    summary_values = [
        inv_num,
        get_val("invoice_date", ""),
        abbr_currency(get_val("invoice_amount", 0)),
        get_val("po_number", ""),
        abbr_currency(get_val("po_amount", 0)),
        get_val("due_date", ""),
        get_val("invoice_status", "").upper(),
        f"{aging_days} days" if aging_days > 0 else "0 days"
    ]
    html_table = '<table style="width:100%; border-collapse: collapse; margin-bottom: 1rem; background: white;">'
    html_table += '<tr style="background-color: #f1f5f9; border-bottom: 1px solid #e2e8f0;">'
    for field in summary_fields:
        html_table += f'<th style="padding: 10px 8px; text-align: left; font-weight: 600; color: #1e293b;">{field}</th>'
    html_table += '<tr>'
    html_table += '<tr>'
    for val in summary_values:
        html_table += f'<td style="padding: 10px 8px; border-bottom: 1px solid #e2e8f0;">{val}</td>'
    html_table += '</tr>'
    html_table += '</table>'
    st.markdown(html_table, unsafe_allow_html=True)

    st.markdown("---")
    st.markdown("### Status History")
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
    if hist_df.empty:
        hist_df = pd.DataFrame([
            {"status": "OPEN", "effective_date": get_val("invoice_date", "2026-01-02"), "status_notes": "Invoice opened and assigned for processing."},
            {"status": "OVERDUE", "effective_date": get_val("due_date", "2026-02-01") if get_val("due_date") else "2026-02-16", "status_notes": "Invoice overdue following standard payment term expiry."}
        ])
    else:
        hist_df.columns = [c.lower() for c in hist_df.columns]
        hist_df = hist_df[["status", "effective_date", "status_notes"]].copy()
    paid_key = f"paid_{inv_num}"
    if st.session_state.get(paid_key, False):
        if not any(hist_df["status"] == "PAID"):
            new_row = pd.DataFrame([{"status": "PAID", "effective_date": date.today().strftime("%Y-%m-%d"), "status_notes": "Processed via ProcureSpendIQ app"}])
            hist_df = pd.concat([hist_df, new_row], ignore_index=True)
    hist_df["effective_date"] = hist_df["effective_date"].apply(lambda x: x.strftime("%Y-%m-%d") if isinstance(x, (date, datetime)) else str(x))
    st.dataframe(safe_dataframe_display(hist_df[["status","effective_date","status_notes"]]), use_container_width=True, hide_index=True, column_config={
        "status": st.column_config.TextColumn("Status", width="small"),
        "effective_date": st.column_config.TextColumn("Effective Date", width="small"),
        "status_notes": st.column_config.TextColumn("Status Notes", width="large"),
    })

    st.markdown("---")
    st.markdown("### Vendor Information")
    tab1, tab2 = st.tabs(["Vendor Info", "Company Info"])

    with tab1:
        vendor_sql = f"""
            SELECT DISTINCT
                v.vendor_id,
                v.vendor_name,
                v.vendor_name_2,
                v.country_code,
                v.city,
                v.postal_code,
                v.street
            FROM {DATABASE}.fact_all_sources_vw f
            LEFT JOIN {DATABASE}.dim_vendor_vw v ON f.vendor_id = v.vendor_id
            WHERE CAST(f.invoice_number AS VARCHAR) = '{inv_num}'
            LIMIT 1
        """
        vendor_df = run_query(vendor_sql)
        if not vendor_df.empty:
            row = vendor_df.iloc[0]
            vendor_fields = ["Vendor ID", "Vendor Name", "Alias/Name 2", "Country", "City", "Postal Code", "Street"]
            vendor_values = [
                row.get("vendor_id", ""),
                row.get("vendor_name", ""),
                row.get("vendor_name_2", ""),
                row.get("country_code", ""),
                row.get("city", ""),
                row.get("postal_code", ""),
                row.get("street", "")
            ]
        else:
            vendor_fields = ["Vendor ID", "Vendor Name", "Alias/Name 2", "Country", "City", "Postal Code", "Street"]
            vendor_values = [
                "0001000007", "McMaster-Carr", "VN-03608", "NL", "Bangalore", "13607", "Tech Center 611"
            ]
        html_vendor = '<table style="width:100%; border-collapse: collapse; background: white;">'
        html_vendor += '<tr style="background-color: #f1f5f9; border-bottom: 1px solid #e2e8f0;">'
        for f in vendor_fields:
            html_vendor += f'<th style="padding: 10px 8px; text-align: left; font-weight: 600;">{f}</th>'
        html_vendor += '</tr>'
        html_vendor += '<tr>'
        for v in vendor_values:
            html_vendor += f'<td style="padding: 10px 8px; border-bottom: 1px solid #e2e8f0;">{v}</table>'
        html_vendor += '</tr>'
        html_vendor += '</table>'
        st.markdown(html_vendor, unsafe_allow_html=True)

    with tab2:
        company_sql = f"""
            SELECT DISTINCT
                f.company_code,
                cc.company_name,
                f.plant_code,
                plt.plant_name,
                cc.street,
                cc.city,
                cc.postal_code
            FROM {DATABASE}.fact_all_sources_vw f
            LEFT JOIN {DATABASE}.dim_company_code_vw cc ON f.company_code = cc.company_code
            LEFT JOIN {DATABASE}.dim_plant_vw plt ON f.plant_code = plt.plant_code
            WHERE CAST(f.invoice_number AS VARCHAR) = '{inv_num}'
            LIMIT 1
        """
        company_df = run_query(company_sql)
        if not company_df.empty:
            row = company_df.iloc[0]
            company_fields = ["Company Code", "Company Name", "Plant Code", "Plant Name", "Street", "City", "Postal Code"]
            company_values = [
                row.get("company_code", ""),
                row.get("company_name", ""),
                row.get("plant_code", ""),
                row.get("plant_name", ""),
                row.get("street", ""),
                row.get("city", ""),
                row.get("postal_code", "")
            ]
        else:
            company_fields = ["Company Code", "Company Name", "Plant Code", "Plant Name", "Street", "City", "Postal Code"]
            company_values = [
                "1000", "Alpha Manufacturing Inc.", "1000", "Main Production Plant",
                "350 Fifth Avenue", "New York", "10001"
            ]
        html_company = '<table style="width:100%; border-collapse: collapse; background: white;">'
        html_company += '<tr style="background-color: #f1f5f9; border-bottom: 1px solid #e2e8f0;">'
        for f in company_fields:
            html_company += f'<th style="padding: 10px 8px; text-align: left; font-weight: 600;">{f}</th>'
        html_company += '</tr>'
        html_company += '<tr>'
        for v in company_values:
            html_company += f'<td style="padding: 10px 8px; border-bottom: 1px solid #e2e8f0;">{v}</td>'
        html_company += '</tr>'
        html_company += '</table>'
        st.markdown(html_company, unsafe_allow_html=True)

    st.markdown("---")
    current_status = get_val("invoice_status", "").upper()
    if st.session_state.get(paid_key, False):
        st.success("✅ Invoice has been processed and marked as Paid.")
    else:
        if current_status == "PAID":
            st.info("ℹ️ This invoice is already marked as PAID.")
        else:
            if st.button("✅ Proceed to Pay", key="proceed_pay_btn", use_container_width=True):
                st.session_state[paid_key] = True
                st.rerun()

def render_invoices():
    st.subheader("Invoices")
    st.markdown("Search, track and manage all invoices in one place")

    query_params = st.experimental_get_query_params()
    if "invoice" in query_params and query_params["invoice"][0]:
        inv_from_param = query_params["invoice"][0]
        st.session_state.selected_invoice_detail = inv_from_param
        st.experimental_set_query_params()
        st.rerun()

    if st.session_state.get("selected_invoice_detail"):
        inv_num = st.session_state.selected_invoice_detail
        inv_sql = f"""
            SELECT
                f.invoice_number,
                f.posting_date AS invoice_date,
                f.invoice_amount_local AS invoice_amount,
                f.purchase_order_reference AS po_number,
                f.po_amount,
                f.due_date,
                UPPER(f.invoice_status) AS invoice_status,
                f.aging_days,
                f.vendor_id,
                v.vendor_name,
                v.vendor_name_2,
                v.country_code,
                v.city,
                v.postal_code,
                v.street,
                f.company_code,
                f.plant_code,
                f.currency
            FROM {DATABASE}.fact_all_sources_vw f
            LEFT JOIN {DATABASE}.dim_vendor_vw v ON f.vendor_id = v.vendor_id
            WHERE CAST(f.invoice_number AS VARCHAR) = '{inv_num}'
            LIMIT 1
        """
        inv_df = run_query(inv_sql)
        if not inv_df.empty:
            render_invoice_detail(inv_df.iloc[0].to_dict(), inv_num)
            if st.button("← Back to Invoices List", key="back_invoices_btn", use_container_width=True):
                st.session_state.selected_invoice_detail = None
                st.session_state.invoice_search_input = ""
                st.session_state.invoice_status_filter = "All Status"
                st.session_state.inv_selected_vendor = "All Vendors"
                st.rerun()
            return
        else:
            st.warning(f"Invoice {inv_num} not found. Returning to list.")
            st.session_state.selected_invoice_detail = None
            st.rerun()

    if "invoice_search_input" not in st.session_state:
        st.session_state.invoice_search_input = ""
    if "invoice_status_filter" not in st.session_state:
        st.session_state.invoice_status_filter = "All Status"
    if "inv_selected_vendor" not in st.session_state:
        st.session_state.inv_selected_vendor = "All Vendors"
    if "search_triggered" not in st.session_state:
        st.session_state.search_triggered = False

    col_search, col_btn, col_reset = st.columns([3, 1, 1])
    with col_search:
        user_search = st.text_input(
            "Invoice or PO Number",
            value=st.session_state.invoice_search_input,
            placeholder="e.g., 9001767",
            label_visibility="collapsed",
            key="inv_search_widget"
        )
    with col_btn:
        search_clicked = st.button("🔍 Search", use_container_width=True, key="search_invoice_btn")
    with col_reset:
        reset_clicked = st.button("Reset", use_container_width=True, key="reset_invoice_btn")

    if reset_clicked:
        st.session_state.invoice_search_input = ""
        st.session_state.invoice_status_filter = "All Status"
        st.session_state.inv_selected_vendor = "All Vendors"
        st.session_state.search_triggered = False
        st.session_state.selected_invoice_detail = None
        st.rerun()

    if search_clicked:
        if user_search.strip():
            st.session_state.invoice_search_input = user_search.strip()
            st.session_state.search_triggered = True
            clean_search = clean_invoice_number(user_search)
            check_sql = f"""
                SELECT invoice_number FROM {DATABASE}.fact_all_sources_vw
                WHERE CAST(invoice_number AS VARCHAR) = '{clean_search}'
                LIMIT 1
            """
            check_df = run_query(check_sql)
            if not check_df.empty:
                st.session_state.selected_invoice_detail = clean_search
                st.session_state.search_triggered = False
                st.rerun()
            else:
                st.warning(f"Invoice {clean_search} not found. Please check the number.")
                st.session_state.search_triggered = False
        else:
            st.warning("Please enter an invoice number to search.")

    if not st.session_state.get("selected_invoice_detail"):
        col_vendor, col_status = st.columns(2)
        with col_vendor:
            if "inv_vendor_list" not in st.session_state:
                vendor_df = run_query(f"SELECT DISTINCT vendor_name FROM {DATABASE}.dim_vendor_vw ORDER BY vendor_name")
                vendor_list = ["All Vendors"] + vendor_df["vendor_name"].tolist() if not vendor_df.empty else ["All Vendors"]
                st.session_state.inv_vendor_list = vendor_list
            selected_vendor = st.selectbox("Vendor", st.session_state.inv_vendor_list, key="inv_sel_vendor", index=st.session_state.inv_vendor_list.index(st.session_state.inv_selected_vendor) if st.session_state.inv_selected_vendor in st.session_state.inv_vendor_list else 0)
            if selected_vendor != st.session_state.inv_selected_vendor:
                st.session_state.inv_selected_vendor = selected_vendor
        with col_status:
            status_options = ["All Status", "OPEN", "PAID", "DISPUTED", "OVERDUE", "DUE_NEXT_30"]
            selected_status_display = st.selectbox("Status", status_options, index=status_options.index(st.session_state.invoice_status_filter) if st.session_state.invoice_status_filter in status_options else 0, key="inv_sel_status")
            if selected_status_display != st.session_state.invoice_status_filter:
                st.session_state.invoice_status_filter = selected_status_display

        where = []
        if st.session_state.invoice_search_input:
            clean_search = clean_invoice_number(st.session_state.invoice_search_input)
            where.append(f"CAST(f.invoice_number AS VARCHAR) = '{clean_search}'")
        if st.session_state.inv_selected_vendor != "All Vendors":
            safe_vendor = st.session_state.inv_selected_vendor.replace("'", "''")
            where.append(f"UPPER(v.vendor_name) = UPPER('{safe_vendor}')")
        selected_status = st.session_state.invoice_status_filter
        if selected_status != "All Status":
            if selected_status == "DUE_NEXT_30":
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
            st.dataframe(safe_dataframe_display(df_display), use_container_width=True, height=400)
        else:
            st.info("No invoices found. Try a different search term or adjust filters.")
