# invoices.py
import streamlit as st
import pandas as pd
from datetime import date, datetime
from athena_client import run_query
from utils import clean_invoice_number, abbr_currency, safe_number, safe_int
from config import DATABASE

# ------------------------------------------------------------
# Helper: Render Invoice Detail View
# ------------------------------------------------------------
def render_invoice_detail(inv_row: dict, inv_num: str):
    """Render the detailed invoice view with all sections."""
    
    # Helper to safely get value and convert dates to string
    def get_val(key, default=""):
        val = inv_row.get(key, default)
        if pd.isna(val):
            return default
        # Convert date/datetime to string
        if isinstance(val, (date, datetime)):
            return val.strftime("%Y-%m-%d")
        return val

    # Genie Insights Banner
    aging_days = get_val("aging_days", 0)
    try:
        due_date = inv_row.get("due_date")
        if due_date and isinstance(due_date, (date, datetime)):
            aging_days = (date.today() - due_date).days
    except:
        pass
    
    st.markdown(f"""
    <div style="background-color: #f3e8ff; border-left: 5px solid #9333ea; padding: 14px 18px; border-radius: 10px; margin-bottom: 24px;">
        <strong style="font-size: 1rem;">🔍 Genie Insights</strong><br/>
        Recommend immediate review of invoice {inv_num} as it is overdue and has been outstanding for {aging_days} days.
    </div>
    """, unsafe_allow_html=True)

    # Invoice Summary Section
    st.markdown("### Invoice Summary")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.markdown("**Invoice Number**")
        st.write(inv_num)
        st.markdown("**Invoice Date**")
        st.write(get_val("invoice_date", ""))
    with col2:
        st.markdown("**Invoice Amount**")
        st.write(abbr_currency(get_val("invoice_amount", 0)))
        st.markdown("**PO Number**")
        st.write(get_val("po_number", ""))
    with col3:
        st.markdown("**PO Amount**")
        st.write(abbr_currency(get_val("po_amount", 0)))
        st.markdown("**Due Date**")
        st.write(get_val("due_date", ""))
    with col4:
        st.markdown("**Invoice Status**")
        status = get_val("invoice_status", "").upper()
        status_color = "#dc2626" if status == "OVERDUE" else "#16a34a" if status == "PAID" else "#f59e0b"
        st.markdown(f"<span style='color:{status_color}; font-weight:bold;'>{status}</span>", unsafe_allow_html=True)
        st.markdown("")  # empty spacer

    st.markdown("---")

    # Status History Section
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
        # Example rows from spec
        hist_df = pd.DataFrame([
            {
                "STATUS": "OPEN",
                "EFFECTIVE_DATE": get_val("invoice_date", "2026-01-02"),
                "STATUS_NOTES": "Invoice opened and assigned for processing. Pending verification of delivery confirmation, invoice accuracy, and appropriate cost center allocation."
            },
            {
                "STATUS": "OVERDUE",
                "EFFECTIVE_DATE": get_val("due_date", "2026-02-01") if get_val("due_date") else "2026-02-16",
                "STATUS_NOTES": "Invoice overdue following standard payment term expiry. Finance team has been notified for priority action. Vendor relations team informed to manage supplier expectations."
            }
        ])
    
    paid_key = f"paid_{inv_num}"
    if st.session_state.get(paid_key, False):
        if not any(hist_df["STATUS"] == "PAID"):
            new_row = pd.DataFrame([{
                "STATUS": "PAID",
                "EFFECTIVE_DATE": date.today().strftime("%Y-%m-%d"),
                "STATUS_NOTES": "Processed via ProcureSpendIQ app"
            }])
            hist_df = pd.concat([hist_df, new_row], ignore_index=True)
    
    st.dataframe(
        hist_df[["STATUS", "EFFECTIVE_DATE", "STATUS_NOTES"]],
        use_container_width=True,
        hide_index=True,
        column_config={
            "STATUS": st.column_config.TextColumn("Status", width="small"),
            "EFFECTIVE_DATE": st.column_config.DateColumn("Effective Date", width="small"),
            "STATUS_NOTES": st.column_config.TextColumn("Status Notes", width="large"),
        }
    )

    st.markdown("---")

    # Tabs: Vendor Info & Company Info
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
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("**Vendor ID**")
                st.write(row.get("vendor_id", ""))
                st.markdown("**Vendor Name**")
                st.write(row.get("vendor_name", ""))
                st.markdown("**Alias/Name 2**")
                st.write(row.get("vendor_name_2", ""))
            with col2:
                st.markdown("**Country**")
                st.write(row.get("country_code", ""))
                st.markdown("**City**")
                st.write(row.get("city", ""))
                st.markdown("**Postal Code**")
                st.write(row.get("postal_code", ""))
                st.markdown("**Street**")
                st.write(row.get("street", ""))
        else:
            # Fallback example data
            st.markdown("**Vendor ID:** 0001000007")
            st.markdown("**Vendor Name:** McMaster-Carr")
            st.markdown("**Alias/Name 2:** VN-03608")
            st.markdown("**Country:** NL")
            st.markdown("**City:** Bangalore")
            st.markdown("**Postal Code:** 13607")
            st.markdown("**Street:** Tech Center 611")
    
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
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("**Company Code**")
                st.write(row.get("company_code", ""))
                st.markdown("**Company Name**")
                st.write(row.get("company_name", ""))
                st.markdown("**Plant Code**")
                st.write(row.get("plant_code", ""))
            with col2:
                st.markdown("**Plant Name**")
                st.write(row.get("plant_name", ""))
                addr_parts = [row.get("street", ""), row.get("city", ""), row.get("postal_code", "")]
                addr = ", ".join([p for p in addr_parts if p])
                st.markdown("**Company Address**")
                st.write(addr)
        else:
            st.markdown("**Company Code:** 1000")
            st.markdown("**Company Name:** Alpha Manufacturing Inc.")
            st.markdown("**Plant Code:** 1000")
            st.markdown("**Plant Name:** Main Production Plant")
            st.markdown("**Company Address:** 350 Fifth Avenue, New York 10001")
    
    st.markdown("---")
    
    # Proceed to Pay Button
    current_status = get_val("invoice_status", "").upper()
    paid_key = f"paid_{inv_num}"
    if st.session_state.get(paid_key, False):
        st.success("✅ Invoice has been processed and marked as Paid.")
    else:
        if current_status == "PAID":
            st.info("This invoice is already marked as PAID.")
        else:
            if st.button("Proceed to Pay", type="primary", key=f"pay_{inv_num}"):
                st.session_state[paid_key] = True
                st.rerun()

# ------------------------------------------------------------
# Main Invoices Page (handles navigation from dashboard)
# ------------------------------------------------------------
def render_invoices():
    st.subheader("Invoices")
    st.markdown("Search, track and manage all invoices in one place")

    # Check if an invoice was selected from Dashboard
    selected_invoice = st.session_state.get("selected_invoice", None)
    if selected_invoice:
        # Fetch invoice details from database
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
            WHERE CAST(f.invoice_number AS VARCHAR) = '{selected_invoice}'
            LIMIT 1
        """
        inv_df = run_query(inv_sql)
        if not inv_df.empty:
            render_invoice_detail(inv_df.iloc[0].to_dict(), selected_invoice)
            # Back button to return to list
            if st.button("← Back to Invoices List"):
                st.session_state.selected_invoice = None
                st.rerun()
            return
        else:
            st.warning(f"Invoice {selected_invoice} not found. Clearing selection.")
            st.session_state.selected_invoice = None
            st.rerun()

    # --- Invoice List View (searchable table) ---
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
    else:
        st.info("No invoices found. Try a different search term.")
