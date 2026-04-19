# invoices.py
import streamlit as st
import pandas as pd
from datetime import date, datetime
from athena_client import run_query
from utils import clean_invoice_number, abbr_currency, safe_number
from config import DATABASE

def _render_invoice_detail(inv_row: dict, inv_num: str):
    def get_val(key, default=""):
        val = inv_row.get(key, default)
        if pd.isna(val):
            return default
        return val

    aging_days = get_val("aging_days", 0)
    try:
        due_date = inv_row.get("due_date")
        if due_date and isinstance(due_date, (date, datetime)):
            aging_days = (date.today() - due_date).days
    except:
        pass
    st.markdown(f"""
    <div style="background-color: #e9d8fd; border-left: 5px solid #7c3aed; padding: 12px 16px; border-radius: 8px; margin-bottom: 20px;">
        <strong>🔍 Genie Insights</strong><br/>
        Recommend immediate review of invoice {inv_num} as it is overdue and has been outstanding for {aging_days} days.
    </div>
    """, unsafe_allow_html=True)

    st.markdown("### Invoice Summary")
    summary_cols = st.columns(4)
    with summary_cols[0]:
        st.metric("Invoice Number", inv_num)
        st.metric("Invoice Date", get_val("invoice_date", ""))
    with summary_cols[1]:
        st.metric("Invoice Amount", abbr_currency(get_val("invoice_amount", 0)))
        st.metric("PO Number", get_val("po_number", ""))
    with summary_cols[2]:
        st.metric("PO Amount", abbr_currency(get_val("po_amount", 0)))
        st.metric("Due Date", get_val("due_date", ""))
    with summary_cols[3]:
        st.metric("Invoice Status", get_val("invoice_status", ""))
        st.metric("", "")

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
            {"INVOICE_NUMBER": inv_num, "STATUS": "OPEN", "EFFECTIVE_DATE": get_val("invoice_date", ""), "STATUS_NOTES": "Invoice opened and assigned for processing. Pending verification of delivery confirmation, invoice accuracy, and appropriate cost center allocation."},
            {"INVOICE_NUMBER": inv_num, "STATUS": "OVERDUE", "EFFECTIVE_DATE": get_val("due_date", ""), "STATUS_NOTES": "Invoice overdue following standard payment term expiry. Finance team has been notified for priority action. Vendor relations team informed to manage supplier expectations."}
        ])
        st.info("Status history not available. Showing example rows.")
    st.dataframe(hist_df, use_container_width=True, hide_index=True)

    st.markdown("---")
    st.markdown("### Vendor & Company Information")
    tabs = st.tabs(["Vendor Info", "Company Info"])

    with tabs[0]:
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
            col1, col2 = st.columns(2)
            with col1:
                st.write(f"**Vendor ID:** {row.get('vendor_id', '')}")
                st.write(f"**Vendor Name:** {row.get('vendor_name', '')}")
                st.write(f"**Alias/Name 2:** {row.get('vendor_name_2', '')}")
            with col2:
                st.write(f"**Country:** {row.get('country_code', '')}")
                st.write(f"**City:** {row.get('city', '')}")
                st.write(f"**Postal Code:** {row.get('postal_code', '')}")
                st.write(f"**Street:** {row.get('street', '')}")
        else:
            st.info("Vendor information not available.")

    with tabs[1]:
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
            col1, col2 = st.columns(2)
            with col1:
                st.write(f"**Company Code:** {row.get('company_code', '')}")
                st.write(f"**Company Name:** {row.get('company_name', '')}")
                st.write(f"**Plant Code:** {row.get('plant_code', '')}")
            with col2:
                st.write(f"**Plant Name:** {row.get('plant_name', '')}")
                addr_parts = [row.get('street', ''), row.get('city', ''), row.get('postal_code', '')]
                addr = ", ".join([p for p in addr_parts if p])
                st.write(f"**Company Address:** {addr}")
        else:
            st.info("Company information not available.")

    st.markdown("---")
    current_status = get_val("invoice_status", "").upper()
    paid_key = f"paid_{inv_num}"
    if st.session_state.get(paid_key, False):
        st.success("Invoice has been processed and marked as Paid.")
    else:
        if current_status == "PAID":
            st.info("This invoice is already marked as PAID.")
        else:
            if st.button("Proceed to Pay", type="primary", key=f"pay_{inv_num}"):
                st.session_state[paid_key] = True
                new_row = pd.DataFrame([{
                    "INVOICE_NUMBER": inv_num,
                    "STATUS": "PAID",
                    "EFFECTIVE_DATE": date.today(),
                    "STATUS_NOTES": "Processed via ProcureSpendIQ app"
                }])
                if "paid_history_override" not in st.session_state:
                    st.session_state.paid_history_override = {}
                st.session_state.paid_history_override[inv_num] = new_row
                st.success("Invoice has been processed and marked as Paid.")
                st.rerun()

    if st.session_state.get("paid_history_override", {}).get(inv_num) is not None:
        extra_row = st.session_state.paid_history_override[inv_num]
        if hist_df is not None and not hist_df.empty:
            combined = pd.concat([hist_df, extra_row], ignore_index=True)
        else:
            combined = extra_row
        st.markdown("### Updated Status History (including PAID)")
        st.dataframe(combined, use_container_width=True, hide_index=True)

def render_invoices():
    st.subheader("Invoices")
    st.markdown("Search, track and manage all invoices in one place")

    query_params = st.experimental_get_query_params()
    if "search_invoice" in query_params:
        invoice_param = clean_invoice_number(query_params["search_invoice"][0])
        if invoice_param:
            st.session_state.inv_search_q = invoice_param
            st.experimental_set_query_params()

    search_term = st.session_state.get("inv_search_q", "")

    if search_term:
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
            WHERE CAST(f.invoice_number AS VARCHAR) = '{search_term}'
            LIMIT 1
        """
        inv_df = run_query(inv_sql)
        if not inv_df.empty:
            _render_invoice_detail(inv_df.iloc[0].to_dict(), search_term)
            return
        else:
            st.warning(f"Invoice {search_term} not found. Showing general invoice list.")

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

        if len(df) == 1 and user_search:
            inv_num = clean_invoice_number(df.iloc[0,0])
            with st.expander(f"Quick view for invoice {inv_num}"):
                inv_sql = f"""
                    SELECT
                        f.invoice_number,
                        f.posting_date AS invoice_date,
                        f.invoice_amount_local AS invoice_amount,
                        f.purchase_order_reference AS po_number,
                        f.po_amount,
                        f.due_date,
                        UPPER(f.invoice_status) AS invoice_status,
                        f.aging_days
                    FROM {DATABASE}.fact_all_sources_vw f
                    WHERE CAST(f.invoice_number AS VARCHAR) = '{inv_num}'
                    LIMIT 1
                """
                quick_df = run_query(inv_sql)
                if not quick_df.empty:
                    _render_invoice_detail(quick_df.iloc[0].to_dict(), inv_num)
                else:
                    st.info("Could not retrieve invoice details.")
    else:
        st.info("No invoices found. Try a different search term.")
