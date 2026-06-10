# main.py
import streamlit as st
from config import LOGO_URL
from persistence import init_db
from dashboard import render_dashboard
from forecast import render_forecast
from genie import render_genie
from invoices import render_invoices

def main():
    init_db()
    st.set_page_config(page_title="ProcureIQ", layout="wide", initial_sidebar_state="expanded")
    
    st.markdown("""
<style>
.block-container {
    padding-top: 2rem !important;
    padding-bottom: 0rem !important;
}
.kpi {
    background: #fff;
    border: 1px solid #e6e8ee;
    border-radius: 12px;
    padding: 12px 14px;
    box-shadow: 0 2px 10px rgba(2,8,23,.06);
}
.kpi .title {
    font-size: 12px;
    color: #64748b;
    font-weight: 800;
}
.kpi .value {
    font-size: 28px;
    font-weight: 900;
    margin-top: 6px;
}
.title-section {
    text-align: left;
    margin-top: 1rem;
}
.nav-section {
    display: flex;
    justify-content: center;
    gap: 0.5rem;
    margin-top: 1rem;
}
.logo-container {
    display: flex;
    justify-content: flex-end;
    align-items: center;
    height: 100%;
    margin-top: 1rem;
}
button[kind="primary"] {
    background-color: #2563eb !important;
    background: #2563eb !important;
    color: white !important;
    border: 2px solid #2563eb !important;
    border-radius: 8px !important;
    font-weight: 600 !important;
}
button[kind="primary"]:hover {
    background-color: #1d4ed8 !important;
    background: #1d4ed8 !important;
    border-color: #1d4ed8 !important;
}
button[data-testid="baseButton-proceed_pay_btn"],
button[data-testid="baseButton-back_invoices_btn"] {
    background-color: #2563eb !important;
    background: #2563eb !important;
    color: white !important;
    border: 2px solid #2563eb !important;
    border-radius: 8px !important;
    font-weight: 600 !important;
}
button[data-testid="baseButton-proceed_pay_btn"]:hover,
button[data-testid="baseButton-back_invoices_btn"]:hover {
    background-color: #1d4ed8 !important;
    background: #1d4ed8 !important;
    border-color: #1d4ed8 !important;
}
</style>
""", unsafe_allow_html=True)

    if "page" not in st.session_state:
        st.session_state.page = "Dashboard"

    col_title, col_nav, col_logo = st.columns([1.2, 2.5, 1], gap="medium")

    with col_title:
        st.markdown('<div class="title-section">', unsafe_allow_html=True)
        st.markdown("<h1 style='font-weight: bold; margin-bottom: 0;'>ProcureIQ</h1>", unsafe_allow_html=True)
        st.markdown("<p style='font-size: 0.8rem; color: gray; margin-top: -0.2rem;'>P2P Analytics</p>", unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

    with col_nav:
        st.markdown('<div class="nav-section">', unsafe_allow_html=True)
        nav_cols = st.columns(4, gap="small")
        current_page = st.session_state.page

        def set_page(page_name):
            st.session_state.page = page_name
            st.rerun()

        with nav_cols[0]:
            btn_type = "primary" if current_page == "Dashboard" else "secondary"
            if st.button("Dashboard", use_container_width=True, type=btn_type, key="nav_dashboard"):
                set_page("Dashboard")
        with nav_cols[1]:
            btn_type = "primary" if current_page == "Genie" else "secondary"
            if st.button("Genie", use_container_width=True, type=btn_type, key="nav_genie"):
                set_page("Genie")
        with nav_cols[2]:
            btn_type = "primary" if current_page == "Forecast" else "secondary"
            if st.button("Forecast", use_container_width=True, type=btn_type, key="nav_forecast"):
                set_page("Forecast")
        with nav_cols[3]:
            btn_type = "primary" if current_page == "Invoices" else "secondary"
            if st.button("Invoices", use_container_width=True, type=btn_type, key="nav_invoices"):
                set_page("Invoices")
        st.markdown('</div>', unsafe_allow_html=True)

    with col_logo:
        st.markdown(f'<div class="logo-container"><img src="{LOGO_URL}" style="width: 100px; height: auto; object-fit: contain;" /></div>', unsafe_allow_html=True)

    st.markdown("---")

    if st.session_state.page == "Dashboard":
        render_dashboard()
    elif st.session_state.page == "Genie":
        render_genie()
    elif st.session_state.page == "Forecast":
        render_forecast()
    else:
        render_invoices()

if __name__ == "__main__":
    main()