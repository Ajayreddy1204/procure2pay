import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
from datetime import datetime, timedelta
import random

# ==================== CONFIGURATION ====================
LOGO_URL = "https://via.placeholder.com/100x40?text=ProcureIQ"  # Replace with your actual logo URL

# ==================== DATABASE INITIALIZATION ====================
def init_db():
    conn = sqlite3.connect('procureiq.db')
    c = conn.cursor()
    
    # Create tables
    c.execute('''CREATE TABLE IF NOT EXISTS invoices
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  invoice_number TEXT,
                  supplier TEXT,
                  category TEXT,
                  amount REAL,
                  date TEXT,
                  status TEXT)''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS forecasts
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  category TEXT,
                  month TEXT,
                  predicted_amount REAL,
                  confidence REAL)''')
    
    # Insert sample data if empty
    c.execute("SELECT COUNT(*) FROM invoices")
    if c.fetchone()[0] == 0:
        suppliers = ['ABC Corp', 'XYZ Ltd', 'Global Supplies', 'Tech Parts Inc', 'Logistics Co']
        categories = ['IT Equipment', 'Office Supplies', 'Logistics', 'Software', 'Consulting']
        statuses = ['Paid', 'Pending', 'Overdue']
        
        for i in range(100):
            date = (datetime.now() - timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d')
            c.execute("INSERT INTO invoices (invoice_number, supplier, category, amount, date, status) VALUES (?,?,?,?,?,?)",
                      (f"INV-{1000+i}", random.choice(suppliers), random.choice(categories),
                       round(random.uniform(500, 20000), 2), date, random.choice(statuses)))
    
    c.execute("SELECT COUNT(*) FROM forecasts")
    if c.fetchone()[0] == 0:
        months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun']
        for cat in categories:
            for mon in months:
                c.execute("INSERT INTO forecasts (category, month, predicted_amount, confidence) VALUES (?,?,?,?)",
                          (cat, mon, round(random.uniform(5000, 50000), 2), round(random.uniform(0.7, 0.95), 2)))
    
    conn.commit()
    conn.close()

init_db()

# ==================== HELPER FUNCTIONS ====================
def load_invoice_data():
    conn = sqlite3.connect('procureiq.db')
    df = pd.read_sql_query("SELECT * FROM invoices", conn)
    conn.close()
    df['date'] = pd.to_datetime(df['date'])
    return df

def load_forecast_data():
    conn = sqlite3.connect('procureiq.db')
    df = pd.read_sql_query("SELECT * FROM forecasts", conn)
    conn.close()
    return df

def get_kpi_metrics(df):
    total_spend = df['amount'].sum()
    avg_invoice = df['amount'].mean()
    pending_invoices = df[df['status'] == 'Pending'].shape[0]
    overdue_invoices = df[df['status'] == 'Overdue'].shape[0]
    return total_spend, avg_invoice, pending_invoices, overdue_invoices

# ==================== PAGE RENDERING FUNCTIONS ====================
def render_dashboard():
    st.header("📊 Procurement Dashboard")
    df = load_invoice_data()
    
    # KPI Row
    total_spend, avg_invoice, pending, overdue = get_kpi_metrics(df)
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("💰 Total Spend", f"${total_spend:,.0f}")
    col2.metric("📄 Avg Invoice", f"${avg_invoice:,.0f}")
    col3.metric("⏳ Pending Invoices", pending)
    col4.metric("⚠️ Overdue Invoices", overdue)
    
    st.markdown("---")
    
    # Charts
    col_left, col_right = st.columns(2)
    with col_left:
        spend_by_category = df.groupby('category')['amount'].sum().reset_index()
        fig = px.bar(spend_by_category, x='category', y='amount', title='Spend by Category', color='category')
        st.plotly_chart(fig, use_container_width=True)
    
    with col_right:
        monthly_spend = df.set_index('date').resample('M')['amount'].sum().reset_index()
        fig = px.line(monthly_spend, x='date', y='amount', title='Monthly Spend Trend', markers=True)
        st.plotly_chart(fig, use_container_width=True)
    
    # Supplier table
    st.subheader("Top Suppliers by Spend")
    supplier_spend = df.groupby('supplier')['amount'].sum().sort_values(ascending=False).head(10).reset_index()
    st.dataframe(supplier_spend, use_container_width=True)

def render_genie():
    st.header("🧞 Genie - AI Assistant")
    st.info("Ask me anything about your procurement data!")
    
    # Simple chatbot simulation
    user_question = st.text_input("Your question:")
    if user_question:
        if "spend" in user_question.lower():
            df = load_invoice_data()
            total = df['amount'].sum()
            st.success(f"Total spend across all invoices is ${total:,.2f}")
        elif "supplier" in user_question.lower():
            df = load_invoice_data()
            top_supplier = df.groupby('supplier')['amount'].sum().idxmax()
            st.success(f"Top supplier by spend is {top_supplier}")
        elif "invoice" in user_question.lower():
            pending = df[df['status'] == 'Pending'].shape[0]
            st.success(f"You have {pending} pending invoices.")
        else:
            st.write("I can help with spend analysis, supplier insights, and invoice status. Try asking about 'spend', 'supplier', or 'invoice'.")

def render_forecast():
    st.header("📈 Spend Forecast")
    df_forecast = load_forecast_data()
    
    categories = df_forecast['category'].unique()
    selected_cat = st.selectbox("Select Category", categories)
    
    cat_data = df_forecast[df_forecast['category'] == selected_cat]
    fig = px.bar(cat_data, x='month', y='predicted_amount', 
                 title=f"Predicted Spend for {selected_cat}",
                 labels={'predicted_amount': 'USD', 'month': 'Month'},
                 color='confidence', color_continuous_scale='Blues')
    st.plotly_chart(fig, use_container_width=True)
    
    st.subheader("Forecast Details")
    st.dataframe(cat_data[['month', 'predicted_amount', 'confidence']], use_container_width=True)

def render_invoices():
    st.header("📄 Invoice Management")
    df = load_invoice_data()
    
    # Filters
    col1, col2 = st.columns(2)
    with col1:
        status_filter = st.multiselect("Status", options=df['status'].unique(), default=df['status'].unique())
    with col2:
        category_filter = st.multiselect("Category", options=df['category'].unique(), default=df['category'].unique())
    
    filtered_df = df[(df['status'].isin(status_filter)) & (df['category'].isin(category_filter))]
    
    st.dataframe(filtered_df[['invoice_number', 'supplier', 'category', 'amount', 'date', 'status']], 
                 use_container_width=True, height=400)
    
    # Summary metrics
    st.markdown("---")
    col_a, col_b = st.columns(2)
    col_a.metric("Total Invoices", len(filtered_df))
    col_b.metric("Total Amount", f"${filtered_df['amount'].sum():,.2f}")

# ==================== MAIN APP LAYOUT ====================
st.set_page_config(page_title="ProcureIQ", layout="wide")

# Custom CSS
st.markdown("""
<style>
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
.title-section {
    text-align: left;
    margin-top: -1rem;
    margin-bottom: 0rem;
    padding-left: 0rem;
}
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
.stColumn:first-child {
    padding-left: 0 !important;
    padding-right: 0.5rem !important;
}
</style>
""", unsafe_allow_html=True)

# Navigation state
if "page" not in st.session_state:
    st.session_state.page = "Dashboard"

# Header
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
    
    def set_page(page_name):
        st.session_state.page = page_name
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

# Page rendering
if st.session_state.page == "Dashboard":
    render_dashboard()
elif st.session_state.page == "Genie":
    render_genie()
elif st.session_state.page == "Forecast":
    render_forecast()
else:
    render_invoices()
