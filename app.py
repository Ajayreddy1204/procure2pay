# app.py
import streamlit as st
from config import LOGO_URL
from persistence import init_db
from dashboard import render_dashboard
from genie import render_genie
from forecast import render_forecast
from invoices import render_invoices

init_db()

st.markdown("""
<style>
/* Reduce top padding to bring content higher */
.block-container {
    padding-top: 0rem !important;
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
/* Title - move up */
.title-section {
    text-align: left;
    margin-top: -1rem;
    margin-bottom: 0rem;
    padding-left: 0rem;
}
/* Navigation - centered */
.nav-section {
    margin-top: 0.5rem;
    margin-bottom: 0rem;
    text-align: center;
}
.logo-container {
    display: flex;
    justify-content: flex-end;
    align-items: flex-start;
    height: 100%;
}
/* Remove extra padding on left column */
.stColumn:first-child {
    padding-left: 0 !important;
    padding-right: 0.5rem !important;
}
</style>
""", unsafe_allow_html=True)

# ------------------------------------------------------------
# Sync query parameters with session state (experimental API)
# ------------------------------------------------------------
query_params = st.experimental_get_query_params()

# If 'tab' is in URL, set the current page
if "tab" in query_params:
    tab_value = query_params["tab"][0]  # returns list
    if tab_value == "Dashboard":
        st.session_state.page = "Dashboard"
    elif tab_value == "Genie":
        st.session_state.page = "Genie"
    elif tab_value == "Forecast":
        st.session_state.page = "Forecast"
    elif tab_value == "Invoices":
        st.session_state.page = "Invoices"

# Default page if not set
if "page" not in st.session_state:
    st.session_state.page = "Dashboard"

# Header: Title (left), Navigation (center), Logo (right)
col_title, col_nav, col_logo = st.columns([1.6, 2.4, 1])

with col_title:
    st.markdown('<div class="title-section">', unsafe_allow_html=True)
    st.markdown("<h1 style='font-weight: bold; margin-bottom: 0;'>ProcureIQ</h1>", unsafe_allow_html=True)
    st.markdown("<p style='font-size: 0.8rem; color: gray; margin-top: -0.2rem;'>P2P Analytics</p>", unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

with col_nav:
    st.markdown('<div class="nav-section">', unsafe_allow_html=True)
    nav_cols = st.columns(4)
    current_page = st.session_state.page

    # Helper to update both session state and URL (experimental)
    def set_page(page_name):
        st.session_state.page = page_name
        # Map page name to tab value
        tab_map = {
            "Dashboard": "Dashboard",
            "Genie": "Genie",
            "Forecast": "Forecast",
            "Invoices": "Invoices"
        }
        # Preserve invoice param if on Invoices, else remove
        current_params = st.experimental_get_query_params()
        new_params = {"tab": tab_map[page_name]}
        if page_name == "Invoices" and "invoice" in current_params:
            new_params["invoice"] = current_params["invoice"]
        st.experimental_set_query_params(**new_params)
        st.rerun()

    with nav_cols[0]:
        if st.button("Dashboard", use_container_width=True, type="primary" if current_page == "Dashboard" else "secondary"):
            set_page("Dashboard")
    with nav_cols[1]:
        if st.button("Genie", use_container_width=True, type="primary" if current_page == "Genie" else "secondary"):
            set_page("Genie")
    with nav_cols[2]:
        if st.button("Forecast", use_container_width=True, type="primary" if current_page == "Forecast" else "secondary"):
            set_page("Forecast")
    with nav_cols[3]:
        if st.button("Invoices", use_container_width=True, type="primary" if current_page == "Invoices" else "secondary"):
            set_page("Invoices")
    st.markdown('</div>', unsafe_allow_html=True)

with col_logo:
    st.markdown(
        f"""
        <div class="logo-container">
            <img src="{LOGO_URL}" style="width: 100px; height: auto; object-fit: contain;" />
        </div>
        """,
        unsafe_allow_html=True
    )

st.markdown("---")

# Render the selected page
if st.session_state.page == "Dashboard":
    render_dashboard()
elif st.session_state.page == "Genie":
    render_genie()
elif st.session_state.page == "Forecast":
    render_forecast()
else:
    render_invoices()
