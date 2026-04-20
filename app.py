# ================================
# P2P Analytics + Genie (Athena + Bedrock Nova)
# Final: Full semantic model, clears old responses on new clicks
# ================================

import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
from datetime import date, timedelta, datetime
import boto3
import awswrangler as wr
import json
import re
import hashlib
import uuid
import sqlite3
import math
import html
from typing import Optional, Dict, Any, List, Union
from decimal import Decimal
from difflib import SequenceMatcher
from functools import lru_cache

# ---------------------------- Page config ----------------------------
st.set_page_config(
    page_title="ProcureIQ | P2P Analytics",
    layout="wide",
    page_icon=":bar_chart:",
)

# ---------------------------- Athena configuration ----------------------------
DATABASE = "procure2pay"          # database in AWS Athena (under AwsDataCatalog)
ATHENA_REGION = "us-east-1"
BEDROCK_MODEL_ID = "amazon.nova-micro-v1:0"

@st.cache_resource
def get_aws_session():
    return boto3.Session()

@st.cache_resource
def get_bedrock_runtime():
    return get_aws_session().client("bedrock-runtime", region_name=ATHENA_REGION)

@st.cache_data(ttl=300, show_spinner=False)
def run_query_cached(sql: str) -> pd.DataFrame:
    try:
        session = get_aws_session()
        df = wr.athena.read_sql_query(sql, database=DATABASE, boto3_session=session)
        for col in df.columns:
            if df[col].dtype == object and df[col].apply(lambda x: isinstance(x, Decimal)).any():
                df[col] = df[col].astype(float)
        return df
    except Exception as e:
        st.error(f"Athena query failed: {e}\nSQL: {sql[:500]}")
        return pd.DataFrame()

run_query = run_query_cached

# ---------------------------- Helper functions ----------------------------
@st.cache_data
def safe_number(val, default=0.0):
    try:
        if pd.isna(val):
            return default
        return float(val)
    except Exception:
        return default

@st.cache_data
def safe_int(val, default=0):
    try:
        if pd.isna(val):
            return default
        return int(float(val))
    except Exception:
        return default

@st.cache_data
def abbr_currency(v: float, currency_symbol: str = "$") -> str:
    n = abs(v)
    sign = "-" if v < 0 else ""
    if n >= 1_000_000_000:
        return f"{sign}{currency_symbol}{n/1_000_000_000:.1f}B"
    if n >= 1_000_000:
        return f"{sign}{currency_symbol}{n/1_000_000:.1f}M"
    if n >= 1_000:
        return f"{sign}{currency_symbol}{n/1_000:.1f}K"
    return f"{sign}{currency_symbol}{n:.0f}"

def compute_range_preset(preset: str):
    today = date.today()
    if preset == "Last 30 Days":
        return today - timedelta(days=30), today
    if preset == "QTD":
        start = date(today.year, ((today.month - 1)//3)*3 + 1, 1)
        return start, today
    if preset == "YTD":
        return date(today.year, 1, 1), today
    return today.replace(day=1), today

def prior_window(start: date, end: date):
    days = (end - start).days + 1
    prev_end = start - timedelta(days=1)
    prev_start = prev_end - timedelta(days=days - 1)
    return prev_start, prev_end

def sql_date(d: date) -> str:
    return f"DATE '{d.strftime('%Y-%m-%d')}'"

def build_vendor_where(selected_vendor: str) -> str:
    if selected_vendor == "All Vendors":
        return ""
    safe_vendor = selected_vendor.replace("'", "''")
    return f"AND UPPER(v.vendor_name) = UPPER('{safe_vendor}')"

def pct_delta(cur, prev):
    if prev == 0:
        if cur == 0:
            return "0%", True
        return "↑ +100%", True
    change = (cur - prev) / prev * 100
    if abs(change) < 0.05:
        return "0%", True
    sign = "↑" if change >= 0 else "↓"
    return f"{sign} {abs(change):.1f}%".replace("+", "+"), change >= 0

def _safe_pct_str(val, default=0.0):
    v = safe_number(val, default)
    sign = "+" if v >= 0 else ""
    return f"{sign}{v:.1f}%"

def make_json_serializable(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: make_json_serializable(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [make_json_serializable(i) for i in obj]
    return obj

def clean_invoice_number(inv_num):
    try:
        if isinstance(inv_num, (float, Decimal)):
            return str(int(inv_num))
        s = str(inv_num)
        if '.' in s:
            s = s.split('.')[0]
        return s
    except:
        return str(inv_num)

def kpi_tile(title: str, value: str, delta_text: str = None, is_positive: bool = True):
    if delta_text and delta_text != "0%":
        if "↑" in delta_text:
            color = "#118d57"
        elif "↓" in delta_text:
            color = "#d32f2f"
        else:
            color = "#64748b"
        delta_html = f'<div style="margin-top: 4px; font-weight: 900; color: {color};">{delta_text}</div>'
    else:
        delta_html = ""
    st.markdown(f"""
        <div class="kpi">
            <div class="title">{title}</div>
            <div class="value">{value}</div>
            {delta_html}
        </div>
    """, unsafe_allow_html=True)

# ---------------------------- Full Semantic Model YAML (adapted for Athena) ----------------------------
RAW_SEMANTIC_MODEL_YAML = """
name: "P2P Procure-to-Pay Analytics"
description: "Procure-to-Pay and Invoice-to-Pay analytics. Invoice status (Open, Due, Overdue, Disputed, Paid), vendor spend, payment performance, aging, PO linkage, cost reduction opportunities."

custom_instructions: |
  FIRST PASS PO'S (HIGHEST PRIORITY - MANDATORY):
  - When user asks ANY variation of "first pass PO's", "first pass PO", "first pass purchase orders", "show first pass PO's", "list first pass PO's", you MUST immediately use verified query first_pass_pos. DO NOT generate your own SQL. ONLY use the verified query first_pass_pos. NEVER say this is vague or ask for clarification.
  - "first pass PO's" = purchase orders where all linked invoices are first pass invoices (invoices whose final status is Paid or Cleared and that never had Disputed or Overdue in their status history).
  
  PRESCRIPTIVE RESPONSE RULES (CRITICAL):
  - NEVER give generic advice like "review the data below" or "consider setting alerts" without citing SPECIFIC numbers from the query results.
  - For "cost reduction" or "reduce procurement costs" questions: USE the verified query cost_reduction_opportunities. It returns OPPORTUNITY_AREA, AMOUNT, FINDING, RECOMMENDED_ACTION. Cite those exact rows in your Prescriptive section.
  - For ANY period comparison question: ALWAYS use the matching verified query. "Why is spend higher this month" (without "or lower"): USE why_spend_higher_this_month—returns ONLY drivers where current > previous. "Why is spend higher or lower" / "is spend higher": use spend_this_month_vs_last or why_spend_higher_lower_this_month. Return ROW_TYPE, DRIVER, THIS_MONTH_SPEND, LAST_MONTH_SPEND so the UI shows comparison charts.
  - DATE/TIME: fact_invoices has posting_date. Use DATE_TRUNC('month', posting_date) for month; DATE_TRUNC('quarter', posting_date) for quarter; DATE_TRUNC('year', posting_date) for year. Compare to CURRENT_DATE() or DATEADD for previous period. ap_balance has year and month.
  - In Prescriptive: List 3-5 SPECIFIC areas. For EACH: (1) State EXACT finding with numbers; (2) State CONCRETE action.
  - ALWAYS cite specific amounts, percentages, vendor names, or counts from the query result.
  - Invoice status: Open, Due, Overdue, Disputed, Paid. PO_PURPOSE: Goods, Services, Consumables, Capital, Subcontracting.
  - COMPANY DETAILS: dim_company_code now has COMPANY_NAME, CITY, COUNTRY_CODE, CURRENCY, STREET, POSTAL_CODE. Use these for company-level analysis instead of just the code.
  - PLANT DETAILS: dim_plant has PLANT_CODE, PLANT_NAME, CITY, STREET, COMPANY_CODE. Use for plant/facility level analysis.
  - REGION DETAILS: dim_region maps REGION_CODE to REGION_NAME for human-readable region names. Join via REGION_CODE.
  - PO HEADER DETAILS: dim_po now includes PO_DATE, PO_CREATED_DATE, DELIVERY_DATE, PO_DOC_TYPE, PO_PAYMENT_TERMS, PO_RELEASE_STATUS, PURCHASING_ORG, PURCHASING_GROUP. Use for PO lifecycle analysis, delivery tracking, and procurement lead time questions.
  - Exclude CANCELLED and REJECTED from spend metrics unless specifically asked.
  - CASH FLOW FORECAST (CRITICAL): For "Forecast cash outflow for the next 7, 14, 30, 60, and 90 days" you MUST use verified query cash_flow_forecast (table cash_flow_forecast). It returns FORECAST_BUCKET, INVOICE_COUNT, TOTAL_AMOUNT, EARLIEST_DUE, LATEST_DUE. Present ALL rows: TOTAL_UNPAID, OVERDUE_NOW, DUE_7_DAYS, DUE_14_DAYS, DUE_30_DAYS, DUE_60_DAYS, DUE_90_DAYS (and PROCESSING_LAG_DAYS if present). Do NOT aggregate to a single total or single bar; show the full bucket breakdown in both the table and the chart. Then give 3-5 prescriptive recommendations.
  - PAYMENT TIMING AND EARLY PAYMENT DISCOUNTS (CRITICAL): For "Which invoices should we pay early to capture discounts?" you MUST use verified query early_payment_candidates. Do NOT say the model lacks discount data—the query returns DOCUMENT_NUMBER, VENDOR_NAME, INVOICE_AMOUNT_LOCAL, DUE_DATE, SAVINGS_IF_2PCT_DISCOUNT, EARLY_PAY_PRIORITY. Present that table and cite it. For "optimal payment timing", "payment strategy for this week", "when should we pay": USE verified query payment_timing_recommendation (RECOMMENDATION, AMOUNT, INVOICE_COUNT, RATIONALE). NEVER claim payment schedules or discount data are missing.
  - GR/IR ROOT CAUSE & FOLLOW-UP: For GR/IR questions that ask about root causes (missing goods receipt, missing invoice, price/quantity mismatch) or vendor follow-up messages, USE gr_ir_aging, gr_ir_outstanding, gr_ir_working_capital_release, and gr_ir_hotspots_clearing_plan as needed. Do NOT say the data is missing. Instead, clearly explain that you are inferring likely root causes from aging and outstanding balances, and then provide concrete remediation steps or example email templates.

tables:
  - name: fact_invoices
    description: "Unified invoice fact with status, amounts, PO linkage, aging. Use for spend, vendor, status, overdue, disputed. posting_date = when invoice was posted; use for this month, last month, YTD."
    base_table:
      database: procure2pay
      table: FACT_ALL_SOURCES_VW
    time_dimensions:
      - name: posting_date
        expr: POSTING_DATE
        description: "Invoice posting date. Use DATE_TRUNC('month'|'quarter'|'year', posting_date) to compare current vs previous period."
        data_type: date
        synonyms: ["invoice posted date", "posting date", "when posted", "bill date"]
    dimensions:
      - name: vendor_id
        expr: VENDOR_ID
        data_type: varchar
        synonyms: ["supplier id", "vendor number", "vendor code"]
      - name: company_code
        expr: COMPANY_CODE
        data_type: number
        synonyms: ["company code", "entity code", "org code"]
      - name: invoice_status
        expr: INVOICE_STATUS
        data_type: varchar
        synonyms: ["bill status", "payment state", "invoice state"]
      - name: po_purpose
        expr: PO_PURPOSE
        data_type: varchar
        synonyms: ["procurement category", "order purpose", "goods or services type"]
      - name: purchase_order
        expr: PURCHASE_ORDER_REFERENCE
        data_type: varchar
        synonyms: ["PO reference", "order reference", "PO ref"]
      - name: region
        expr: REGION
        data_type: varchar
        synonyms: ["vendor region", "geographic region"]
    measures:
      - name: invoice_amount
        expr: INVOICE_AMOUNT_LOCAL
        default_aggregation: sum
        data_type: number
        synonyms: ["spend", "procurement spend", "invoice value", "total invoice amount"]
      - name: po_amount
        expr: PO_AMOUNT
        default_aggregation: sum
        data_type: number
        synonyms: ["PO value", "purchase order value", "order total"]
      - name: aging_days
        expr: AGING_DAYS
        default_aggregation: avg
        data_type: number
        synonyms: ["days overdue", "past due days", "invoice age days"]
  - name: ap_balance
    description: "AP balance and invoice count by year/month"
    base_table:
      database: procure2pay
      table: ACCOUNTS_PAYABLE_BALANCE_VW
    dimensions:
      - name: year
        expr: YEAR
        data_type: number
        synonyms: ["balance year", "AP year"]
      - name: month
        expr: MONTH
        data_type: number
        synonyms: ["balance month", "AP month"]
    measures:
      - name: ap_balance
        expr: AP_BALANCE
        default_aggregation: sum
        data_type: number
        synonyms: ["payables balance", "AP total", "outstanding payables"]
      - name: invoice_count
        expr: INVOICE_COUNT
        default_aggregation: sum
        data_type: number
        synonyms: ["AP invoice count", "payables invoice count"]
  - name: days_payable_outstanding
    description: "DPO by invoice. Columns: INVOICE_NUMBER, YEAR, MONTH, AVG_TRADE_PAYABLES, COGS_AMOUNT, DPO"
    base_table:
      database: procure2pay
      table: DAYS_PAYABLE_OUTSTANDING_VW
  - name: dim_company_code
    description: "Company code dimension with full master data. Columns: COMPANY_CODE, COMPANY_NAME, CITY, COUNTRY_CODE, CURRENCY, POSTAL_CODE, STREET, REGION_CODE, VAT_REG_NUMBER, CHART_OF_ACCOUNTS, SYSTEM"
    base_table:
      database: procure2pay
      table: DIM_COMPANY_CODE_VW
    primary_key:
      columns:
        - company_code
    dimensions:
      - name: company_code
        expr: COMPANY_CODE
        data_type: number
        synonyms: ["entity code", "org code"]
      - name: company_name
        expr: COMPANY_NAME
        data_type: varchar
        synonyms: ["company", "entity name", "organization name"]
      - name: company_city
        expr: CITY
        data_type: varchar
        synonyms: ["company city", "company location"]
      - name: company_country
        expr: COUNTRY_CODE
        data_type: varchar
        synonyms: ["company country"]
      - name: company_currency
        expr: CURRENCY
        data_type: varchar
        synonyms: ["company currency", "local currency"]
      - name: company_street
        expr: STREET
        data_type: varchar
        synonyms: ["company street", "company address"]
      - name: company_postal_code
        expr: POSTAL_CODE
        data_type: varchar
        synonyms: ["company zip", "company postal code"]
      - name: region_code
        expr: REGION_CODE
        data_type: varchar
        synonyms: ["company region code", "company state code"]
  - name: dim_po
    description: "Purchase order dimension with header details. Columns: PURCHASE_ORDER_NUMBER, PO_ITEM, PO_AMOUNT, VENDOR_ID, COMPANY_CODE, PO_DATE, PO_CREATED_DATE, PO_DOC_TYPE, PO_PAYMENT_TERMS, PURCHASING_ORG, PURCHASING_GROUP, PO_CURRENCY, PO_RELEASE_STATUS, DELIVERY_DATE, QUANTITY_DELIVERED, QUANTITY_REMAINING"
    base_table:
      database: procure2pay
      table: DIM_PO_VW
    primary_key:
      columns:
        - purchase_order_number
        - po_item
    dimensions:
      - name: purchase_order_number
        expr: PURCHASE_ORDER_NUMBER
        data_type: varchar
        synonyms: ["PO number", "order number"]
      - name: po_item
        expr: PO_ITEM
        data_type: varchar
        synonyms: ["line item", "item number", "PO line"]
      - name: po_date
        expr: PO_DATE
        data_type: date
        synonyms: ["purchase order date", "PO submitted date", "order date"]
      - name: po_created_date
        expr: PO_CREATED_DATE
        data_type: date
        synonyms: ["PO creation date", "when PO was created"]
      - name: po_doc_type
        expr: PO_DOC_TYPE
        data_type: varchar
        synonyms: ["PO document type", "order type", "NB", "FO"]
      - name: po_payment_terms
        expr: PO_PAYMENT_TERMS
        data_type: varchar
        synonyms: ["PO payment terms", "order payment terms"]
      - name: purchasing_org
        expr: PURCHASING_ORG
        data_type: varchar
        synonyms: ["purchasing organization", "procurement org"]
      - name: purchasing_group
        expr: PURCHASING_GROUP
        data_type: varchar
        synonyms: ["purchasing group", "buyer group"]
      - name: po_release_status
        expr: PO_RELEASE_STATUS
        data_type: varchar
        synonyms: ["PO release status", "approval status"]
      - name: delivery_date
        expr: DELIVERY_DATE
        data_type: date
        synonyms: ["expected delivery date", "PO delivery date", "due delivery date"]
      - name: po_due_date
        expr: PO_DUE_DATE
        data_type: date
        synonyms: ["PO due date", "purchase order due date", "PO payment due date", "when is PO due"]
    measures:
      - name: po_amount
        expr: PO_AMOUNT
        default_aggregation: sum
        data_type: number
        synonyms: ["PO value", "purchase order value", "order amount"]
      - name: quantity_delivered
        expr: QUANTITY_DELIVERED
        default_aggregation: sum
        data_type: number
        synonyms: ["delivered qty", "received quantity"]
      - name: quantity_remaining
        expr: QUANTITY_REMAINING
        default_aggregation: sum
        data_type: number
        synonyms: ["remaining qty", "outstanding quantity", "pending delivery"]
  - name: dim_plant
    description: "Plant dimension with master data. Columns: PLANT_CODE, PLANT_NAME, PLANT_NAME_2, COMPANY_CODE, COUNTRY_CODE, REGION_CODE, CITY, POSTAL_CODE, STREET, SYSTEM"
    base_table:
      database: procure2pay
      table: DIM_PLANT_VW
    primary_key:
      columns:
        - plant_code
    dimensions:
      - name: plant_code
        expr: PLANT_CODE
        data_type: varchar
        synonyms: ["plant", "facility code", "site code", "works"]
      - name: plant_name
        expr: PLANT_NAME
        data_type: varchar
        synonyms: ["plant name", "facility name", "site name"]
      - name: plant_city
        expr: CITY
        data_type: varchar
        synonyms: ["plant city", "plant location"]
      - name: plant_street
        expr: STREET
        data_type: varchar
        synonyms: ["plant address", "plant street"]
      - name: plant_company_code
        expr: COMPANY_CODE
        data_type: number
        synonyms: ["plant company code"]
      - name: region_code
        expr: REGION_CODE
        data_type: varchar
        synonyms: ["plant region code", "plant state code"]

  - name: dim_region
    description: "Region dimension mapping codes to names. Columns: COUNTRY_CODE, REGION_CODE, REGION_NAME"
    base_table:
      database: procure2pay
      table: DIM_REGION_VW
    primary_key:
      columns:
        - region_code
    dimensions:
      - name: region_code
        expr: REGION_CODE
        data_type: varchar
        synonyms: ["state code", "province code"]
      - name: region_name
        expr: REGION_NAME
        data_type: varchar
        synonyms: ["state name", "region name", "province name", "state"]
      - name: region_country
        expr: COUNTRY_CODE
        data_type: varchar
        synonyms: ["region country"]

  - name: dim_vendor
    description: "Vendor dimension. Columns: VENDOR_ID, VENDOR_NAME, SYSTEM"
    base_table:
      database: procure2pay
      table: DIM_VENDOR_VW
    primary_key:
      columns:
        - vendor_id
    dimensions:
      - name: vendor_id
        expr: VENDOR_ID
        data_type: varchar
        synonyms: ["supplier id", "vendor number"]
      - name: vendor_name
        expr: VENDOR_NAME
        data_type: varchar
        synonyms: ["supplier name", "vendor label"]
  - name: duplicate_payments
    description: "Duplicate payments. Columns: INVOICE_NUMBER, YEAR, MONTH, DUPLICATE_PAYMENT_AMOUNT, TOTAL_PAYMENTS, DUPLICATE_PAYMENT_RATE"
    base_table:
      database: procure2pay
      table: DUPLICATE_PAYMENTS_FOR_INVOICE_VW
  - name: invoice_status_history
    description: "Full invoice status history with ALL invoice details. One row per status change. Use MAX(SEQUENCE_NBR) for latest record. Columns include: INVOICE_NUMBER, STATUS, SEQUENCE_NBR, EFFECTIVE_DATE, POSTING_DATE, DUE_DATE, VENDOR_ID, INVOICE_AMOUNT_LOCAL, PAYMENT_DATE, CLEARING_DOCUMENT, AGING_DAYS, PURCHASE_ORDER_REFERENCE, PO_AMOUNT, PO_PURPOSE, DOCUMENT_TYPE, DISCOUNT_PERCENT, REGION, SYSTEM"
    base_table:
      database: procure2pay
      table: INVOICE_STATUS_HISTORY_VW
  - name: fact_po_level
    description: "PO-level fact. Columns: POSTING_YEAR, POSTING_MONTH, DELIVERY_DATE, RECEIVED_QTY, ORDERED_QTY, PO_AMOUNT"
    base_table:
      database: procure2pay
      table: FACT_SAP_PO_LEVEL_VW
  - name: full_payment_rate
    description: "Full payment rate. Columns: YEAR, MONTH, FULL_PAID_INVOICES, TOTAL_CLEARED_INVOICES, FULL_PAYMENT_RATE_PCT"
    base_table:
      database: procure2pay
      table: FULL_PAYMENT_RATE_VW
  - name: gr_ir_aging
    description: "GR/IR aging. Columns: YEAR, MONTH, AGE_DAYS, TOTAL_GRIR_BALANCE, GRIR_OVER_30/60/90"
    base_table:
      database: procure2pay
      table: GR_IR_AGING_VW
    primary_key:
      columns:
        - year
        - month
        - age_days
    dimensions:
      - name: year
        expr: YEAR
        data_type: number
        synonyms: ["grir year"]
      - name: month
        expr: MONTH
        data_type: number
        synonyms: ["grir month"]
      - name: age_days
        expr: AGE_DAYS
        data_type: number
        synonyms: ["age days", "aging bucket days"]
    measures:
      - name: total_grir_balance
        expr: TOTAL_GRIR_BALANCE
        default_aggregation: sum
        data_type: number
        synonyms: ["grir balance", "total grir"]
      - name: grir_over_30
        expr: GRIR_OVER_30
        default_aggregation: sum
        data_type: number
      - name: grir_over_60
        expr: GRIR_OVER_60
        default_aggregation: sum
        data_type: number
      - name: grir_over_90
        expr: GRIR_OVER_90
        default_aggregation: sum
        data_type: number
  - name: gr_ir_outstanding
    description: "GR/IR outstanding. Columns: YEAR, MONTH, INVOICE_COUNT, TOTAL_GRIR_BLNC"
    base_table:
      database: procure2pay
      table: GR_IR_OUTSTANDING_BALANCE_VW
    primary_key:
      columns:
        - year
        - month
    dimensions:
      - name: year
        expr: YEAR
        data_type: number
      - name: month
        expr: MONTH
        data_type: number
    measures:
      - name: invoice_count
        expr: INVOICE_COUNT
        default_aggregation: sum
        data_type: number
      - name: total_grir_balance
        expr: TOTAL_GRIR_BLNC
        default_aggregation: sum
        data_type: number
  - name: late_accruals
    description: "Late accruals. Columns: YEAR, MONTH, LATE_ACCRUAL_AMOUNT, LATE_ACCRUAL_RATE_PCT"
    base_table:
      database: procure2pay
      table: LATE_ACCRUALS_VW
  - name: late_payment_amount
    description: "Late payment. Columns: YEAR, MONTH, LATE_PAYMENT_AMOUNT, LATE_PAYMENT_COUNT, LATE_PAYMENT_RATE_PCT"
    base_table:
      database: procure2pay
      table: LATE_PAYMENT_AMOUNT_VW
    primary_key:
      columns:
        - year
        - month
    dimensions:
      - name: year
        expr: YEAR
        data_type: number
      - name: month
        expr: MONTH
        data_type: number
    measures:
      - name: late_payment_amount
        expr: LATE_PAYMENT_AMOUNT
        default_aggregation: sum
        data_type: number
      - name: late_payment_count
        expr: LATE_PAYMENT_COUNT
        default_aggregation: sum
        data_type: number
      - name: late_payment_rate_pct
        expr: LATE_PAYMENT_RATE_PCT
        default_aggregation: avg
        data_type: number
  - name: net_early_payment_benefit
    description: "Early payment benefit. Columns: TOTAL_NET_BENEFIT, TOTAL_SPEND, NEPBI_PERCENT"
    base_table:
      database: procure2pay
      table: NET_EARLY_PAYMENT_BENEFIT_INDEX_VW
  - name: on_time_payment_rate
    description: "On-time payment rate. Columns: YEAR, MONTH, ON_TIME_PAYMENTS, TOTAL_PAYMENTS, ON_TIME_PAYMENT_RATE_PCT"
    base_table:
      database: procure2pay
      table: ON_TIME_PAYMENT_RATE_VW
  - name: partial_payment_rate
    description: "Partial payment rate. Columns: YEAR, MONTH, PARTIAL_PAID_INVOICES, PARTIAL_PAYMENT_RATE_PCT"
    base_table:
      database: procure2pay
      table: PARTIAL_PAYMENT_RATE_VW
  - name: payment_predictability
    description: "Payment predictability. Columns: POSTING_YEAR, POSTING_MONTH, PAYMENT_PREDICTABILITY_INDEX, INTERPRETATION"
    base_table:
      database: procure2pay
      table: PAYMENT_PREDICTABILITY_INDEX_VW
  - name: payment_cycle_time
    description: "Payment cycle time. Columns: YEAR, MONTH, AVG_PAYMENT_CYCLE_TIME_DAYS, CLEARED_INVOICES"
    base_table:
      database: procure2pay
      table: PAYMENT_PROCESSING_CYCLE_TIME_VW
  - name: supplier_delivery_accuracy
    description: "Supplier delivery. Columns: YEAR, MONTH, ON_TIME_DELIVERIES, TOTAL_DELIVERIES, DELIVERY_ACCURACY_PCT"
    base_table:
      database: procure2pay
      table: SUPPLIER_DELIVERY_ACCURACY_INDEX_VW
  - name: weighted_dpo
    description: "Weighted DPO. Columns: YEAR, MONTH, TOTAL_PAYABLES, TOTAL_COGS, WEIGHTED_DPO"
    base_table:
      database: procure2pay
      table: WEIGHTED_DAYS_PAYABLE_OUTSTANDING_VW
    primary_key:
      columns:
        - year
        - month
    dimensions:
      - name: year
        expr: YEAR
        data_type: number
      - name: month
        expr: MONTH
        data_type: number
    measures:
      - name: total_payables
        expr: TOTAL_PAYABLES
        default_aggregation: sum
        data_type: number
      - name: total_cogs
        expr: TOTAL_COGS
        default_aggregation: sum
        data_type: number
      - name: weighted_dpo
        expr: WEIGHTED_DPO
        default_aggregation: avg
        data_type: number
  - name: cash_flow_unpaid_obligations
    description: "Unpaid invoice obligations (Open, Due, Overdue) for cash flow forecasting and payment timing. Use for: cash flow forecast 7/14/30/60/90 days, optimal payment timing, which invoices to pay early, early payment discounts. Columns: DOCUMENT_NUMBER, VENDOR_ID, INVOICE_AMOUNT_LOCAL, DUE_DATE, INVOICE_STATUS, DAYS_UNTIL_DUE. Use verified queries cash_flow_forecast, early_payment_candidates, payment_timing_recommendation for best results."
    base_table:
      database: procure2pay
      table: CASH_FLOW_UNPAID_OBLIGATIONS_VW
    primary_key:
      columns:
        - document_number
    dimensions:
      - name: document_number
        expr: DOCUMENT_NUMBER
        data_type: varchar
        synonyms: ["invoice number", "document"]
      - name: vendor_id
        expr: VENDOR_ID
        data_type: varchar
        synonyms: ["supplier id", "vendor"]
      - name: due_date
        expr: DUE_DATE
        data_type: date
        synonyms: ["invoice due date"]
      - name: invoice_status
        expr: INVOICE_STATUS
        data_type: varchar
        synonyms: ["status", "payment status"]
      - name: days_until_due
        expr: DAYS_UNTIL_DUE
        data_type: number
        synonyms: ["days to due date"]
    measures:
      - name: invoice_amount
        expr: INVOICE_AMOUNT_LOCAL
        default_aggregation: sum
        data_type: number
        synonyms: ["invoice amount", "unpaid amount"]

  - name: cash_flow_forecast
    description: "Pre-built cash flow forecast by time bucket. Columns: FORECAST_BUCKET (TOTAL_UNPAID, OVERDUE_NOW, DUE_7_DAYS, DUE_14_DAYS, DUE_30_DAYS, DUE_60_DAYS, DUE_90_DAYS, BEYOND_90_DAYS, PROCESSING_LAG_DAYS), INVOICE_COUNT, TOTAL_AMOUNT, EARLIEST_DUE, LATEST_DUE. Use for 'Forecast cash outflow for the next 7, 14, 30, 60, and 90 days'."
    base_table:
      database: procure2pay
      table: CASH_FLOW_FORECAST_VW
    primary_key:
      columns:
        - forecast_bucket
    dimensions:
      - name: forecast_bucket
        expr: FORECAST_BUCKET
        data_type: varchar
        synonyms: ["time bucket code", "bucket"]
      - name: earliest_due
        expr: EARLIEST_DUE
        data_type: date
        synonyms: ["earliest due date"]
      - name: latest_due
        expr: LATEST_DUE
        data_type: date
        synonyms: ["latest due date"]
    measures:
      - name: invoice_count
        expr: INVOICE_COUNT
        default_aggregation: sum
        data_type: number
        synonyms: ["number of invoices", "invoice volume"]
      - name: total_amount
        expr: TOTAL_AMOUNT
        default_aggregation: sum
        data_type: number
        synonyms: ["cash outflow amount", "bucket amount"]

  - name: early_payment_candidates
    description: "Top invoices to pay early for discount capture. Columns: DOCUMENT_NUMBER, VENDOR_ID, VENDOR_NAME, INVOICE_AMOUNT_LOCAL, DUE_DATE, DAYS_UNTIL_DUE, SAVINGS_IF_2PCT_DISCOUNT, VENDOR_TIER, EARLY_PAY_PRIORITY. Use for 'Which invoices should we pay early to capture discounts?'."
    base_table:
      database: procure2pay
      table: EARLY_PAYMENT_CANDIDATES_VW
    primary_key:
      columns:
        - document_number
    dimensions:
      - name: document_number
        expr: DOCUMENT_NUMBER
        data_type: varchar
        synonyms: ["invoice number", "document"]
      - name: vendor_id
        expr: VENDOR_ID
        data_type: varchar
        synonyms: ["supplier id", "vendor"]
      - name: vendor_name
        expr: VENDOR_NAME
        data_type: varchar
        synonyms: ["supplier name"]
      - name: due_date
        expr: DUE_DATE
        data_type: date
        synonyms: ["invoice due date"]
      - name: days_until_due
        expr: DAYS_UNTIL_DUE
        data_type: number
        synonyms: ["days to due date"]
      - name: vendor_tier
        expr: VENDOR_TIER
        data_type: varchar
        synonyms: ["supplier tier"]
      - name: early_pay_priority
        expr: EARLY_PAY_PRIORITY
        data_type: varchar
        synonyms: ["priority", "early payment priority"]
    measures:
      - name: invoice_amount
        expr: INVOICE_AMOUNT_LOCAL
        default_aggregation: sum
        data_type: number
        synonyms: ["invoice amount", "spend"]
      - name: savings_if_2pct_discount
        expr: SAVINGS_IF_2PCT_DISCOUNT
        default_aggregation: sum
        data_type: number
        synonyms: ["potential discount", "2 percent savings"]

  - name: payment_timing_recommendation
    description: "Optimal payment timing strategy for the week. Columns: RECOMMENDATION (PAY_IMMEDIATELY, PAY_THIS_WEEK, EARLY_PAY_OPPORTUNITY, HOLD_FOR_CASH), AMOUNT, INVOICE_COUNT, RATIONALE. Use for 'What is the optimal payment timing strategy for this week?'."
    base_table:
      database: procure2pay
      table: PAYMENT_TIMING_RECOMMENDATION_VW
    primary_key:
      columns:
        - recommendation
    dimensions:
      - name: recommendation
        expr: RECOMMENDATION
        data_type: varchar
        synonyms: ["action", "payment strategy recommendation"]
      - name: rationale
        expr: RATIONALE
        data_type: varchar
        synonyms: ["reason", "justification"]
    measures:
      - name: amount
        expr: AMOUNT
        default_aggregation: sum
        data_type: number
        synonyms: ["total amount", "obligation amount"]
      - name: invoice_count
        expr: INVOICE_COUNT
        default_aggregation: sum
        data_type: number
        synonyms: ["number of invoices"]

relationships:
  - name: invoices_to_vendor
    left_table: fact_invoices
    right_table: dim_vendor
    relationship_columns:
      - left_column: vendor_id
        right_column: vendor_id
    join_type: left_outer
    relationship_type: many_to_one
  - name: invoices_to_company
    left_table: fact_invoices
    right_table: dim_company_code
    relationship_columns:
      - left_column: company_code
        right_column: company_code
    join_type: left_outer
    relationship_type: many_to_one
  - name: invoices_to_po
    left_table: fact_invoices
    right_table: dim_po
    relationship_columns:
      - left_column: purchase_order
        right_column: purchase_order_number
    join_type: left_outer
    relationship_type: many_to_one
  - name: plant_to_company
    left_table: dim_plant
    right_table: dim_company_code
    relationship_columns:
      - left_column: plant_company_code
        right_column: company_code
    join_type: left_outer
    relationship_type: many_to_one
  - name: company_to_region
    left_table: dim_company_code
    right_table: dim_region
    relationship_columns:
      - left_column: REGION_CODE
        right_column: region_code
    join_type: left_outer
    relationship_type: many_to_one
  - name: plant_to_region
    left_table: dim_plant
    right_table: dim_region
    relationship_columns:
      - left_column: REGION_CODE
        right_column: region_code
    join_type: left_outer
    relationship_type: many_to_one

verified_queries:
  - name: spend_this_month_vs_last
    question: "Is our procurement spend higher this month? If yes or no what are the reasons?"
    use_as_onboarding_question: true
    sql: |
      WITH base AS (
        SELECT * FROM procure2pay.FACT_ALL_SOURCES_VW
        WHERE UPPER(INVOICE_STATUS) NOT IN ('CANCELLED', 'REJECTED') AND POSTING_DATE IS NOT NULL
      ),
      this_month AS (
        SELECT SUM(INVOICE_AMOUNT_LOCAL) AS spend, COUNT(*) AS invoice_cnt
        FROM base WHERE DATE_TRUNC('month', POSTING_DATE) = DATE_TRUNC('month', CURRENT_DATE())
      ),
      last_month AS (
        SELECT SUM(INVOICE_AMOUNT_LOCAL) AS spend, COUNT(*) AS invoice_cnt
        FROM base WHERE DATE_TRUNC('month', POSTING_DATE) = DATE_TRUNC('month', DATEADD('month', -1, CURRENT_DATE()))
      ),
      summary AS (
        SELECT 'SUMMARY' AS ROW_TYPE, 'Total' AS DRIVER,
          (SELECT spend FROM this_month) AS THIS_MONTH_SPEND,
          (SELECT spend FROM last_month) AS LAST_MONTH_SPEND,
          CASE WHEN (SELECT spend FROM last_month) > 0
            THEN ROUND(((SELECT spend FROM this_month) - (SELECT spend FROM last_month)) * 100.0 / (SELECT spend FROM last_month), 1)
            ELSE NULL END AS CHANGE_PCT,
          (SELECT invoice_cnt FROM this_month) AS THIS_MONTH_CNT,
          (SELECT invoice_cnt FROM last_month) AS LAST_MONTH_CNT
      ),
      by_vendor AS (
        SELECT 'VENDOR' AS ROW_TYPE, VENDOR_ID AS DRIVER,
          SUM(CASE WHEN DATE_TRUNC('month', POSTING_DATE) = DATE_TRUNC('month', CURRENT_DATE()) THEN INVOICE_AMOUNT_LOCAL ELSE 0 END) AS THIS_MONTH_SPEND,
          SUM(CASE WHEN DATE_TRUNC('month', POSTING_DATE) = DATE_TRUNC('month', DATEADD('month', -1, CURRENT_DATE())) THEN INVOICE_AMOUNT_LOCAL ELSE 0 END) AS LAST_MONTH_SPEND,
          NULL AS CHANGE_PCT, NULL AS THIS_MONTH_CNT, NULL AS LAST_MONTH_CNT
        FROM base
        WHERE DATE_TRUNC('month', POSTING_DATE) IN (DATE_TRUNC('month', CURRENT_DATE()), DATE_TRUNC('month', DATEADD('month', -1, CURRENT_DATE())))
        GROUP BY VENDOR_ID
        HAVING SUM(INVOICE_AMOUNT_LOCAL) > 0
        ORDER BY SUM(INVOICE_AMOUNT_LOCAL) DESC
      ),
      by_purpose AS (
        SELECT 'PO_PURPOSE' AS ROW_TYPE, COALESCE(PO_PURPOSE, 'Unknown') AS DRIVER,
          SUM(CASE WHEN DATE_TRUNC('month', POSTING_DATE) = DATE_TRUNC('month', CURRENT_DATE()) THEN INVOICE_AMOUNT_LOCAL ELSE 0 END) AS THIS_MONTH_SPEND,
          SUM(CASE WHEN DATE_TRUNC('month', POSTING_DATE) = DATE_TRUNC('month', DATEADD('month', -1, CURRENT_DATE())) THEN INVOICE_AMOUNT_LOCAL ELSE 0 END) AS LAST_MONTH_SPEND,
          NULL AS CHANGE_PCT, NULL AS THIS_MONTH_CNT, NULL AS LAST_MONTH_CNT
        FROM base
        WHERE DATE_TRUNC('month', POSTING_DATE) IN (DATE_TRUNC('month', CURRENT_DATE()), DATE_TRUNC('month', DATEADD('month', -1, CURRENT_DATE())))
        GROUP BY PO_PURPOSE
        HAVING SUM(INVOICE_AMOUNT_LOCAL) > 0
        ORDER BY SUM(INVOICE_AMOUNT_LOCAL) DESC
      )
      SELECT * FROM summary
      UNION ALL SELECT * FROM by_vendor
      UNION ALL SELECT * FROM by_purpose;

  - name: why_spend_higher_this_month
    question: "Why is our procurement spend higher this month?"
    sql: |
      WITH base AS (
        SELECT * FROM procure2pay.FACT_ALL_SOURCES_VW
        WHERE UPPER(INVOICE_STATUS) NOT IN ('CANCELLED', 'REJECTED') AND POSTING_DATE IS NOT NULL
      ),
      drivers AS (
        SELECT 'VENDOR' AS ROW_TYPE, VENDOR_ID AS DRIVER,
          SUM(CASE WHEN DATE_TRUNC('month', POSTING_DATE) = DATE_TRUNC('month', CURRENT_DATE()) THEN INVOICE_AMOUNT_LOCAL ELSE 0 END) AS THIS_MONTH_SPEND,
          SUM(CASE WHEN DATE_TRUNC('month', POSTING_DATE) = DATE_TRUNC('month', DATEADD('month', -1, CURRENT_DATE())) THEN INVOICE_AMOUNT_LOCAL ELSE 0 END) AS LAST_MONTH_SPEND
        FROM base WHERE DATE_TRUNC('month', POSTING_DATE) IN (DATE_TRUNC('month', CURRENT_DATE()), DATE_TRUNC('month', DATEADD('month', -1, CURRENT_DATE())))
        GROUP BY VENDOR_ID
        HAVING SUM(CASE WHEN DATE_TRUNC('month', POSTING_DATE) = DATE_TRUNC('month', CURRENT_DATE()) THEN INVOICE_AMOUNT_LOCAL ELSE 0 END)
             > SUM(CASE WHEN DATE_TRUNC('month', POSTING_DATE) = DATE_TRUNC('month', DATEADD('month', -1, CURRENT_DATE())) THEN INVOICE_AMOUNT_LOCAL ELSE 0 END)
        ORDER BY SUM(CASE WHEN DATE_TRUNC('month', POSTING_DATE) = DATE_TRUNC('month', CURRENT_DATE()) THEN INVOICE_AMOUNT_LOCAL ELSE 0 END)
               - SUM(CASE WHEN DATE_TRUNC('month', POSTING_DATE) = DATE_TRUNC('month', DATEADD('month', -1, CURRENT_DATE())) THEN INVOICE_AMOUNT_LOCAL ELSE 0 END) DESC
      ),
      by_purpose AS (
        SELECT 'PO_PURPOSE' AS ROW_TYPE, COALESCE(PO_PURPOSE, 'Unknown') AS DRIVER,
          SUM(CASE WHEN DATE_TRUNC('month', POSTING_DATE) = DATE_TRUNC('month', CURRENT_DATE()) THEN INVOICE_AMOUNT_LOCAL ELSE 0 END) AS THIS_MONTH_SPEND,
          SUM(CASE WHEN DATE_TRUNC('month', POSTING_DATE) = DATE_TRUNC('month', DATEADD('month', -1, CURRENT_DATE())) THEN INVOICE_AMOUNT_LOCAL ELSE 0 END) AS LAST_MONTH_SPEND
        FROM base WHERE DATE_TRUNC('month', POSTING_DATE) IN (DATE_TRUNC('month', CURRENT_DATE()), DATE_TRUNC('month', DATEADD('month', -1, CURRENT_DATE())))
        GROUP BY PO_PURPOSE
        HAVING SUM(CASE WHEN DATE_TRUNC('month', POSTING_DATE) = DATE_TRUNC('month', CURRENT_DATE()) THEN INVOICE_AMOUNT_LOCAL ELSE 0 END)
             > SUM(CASE WHEN DATE_TRUNC('month', POSTING_DATE) = DATE_TRUNC('month', DATEADD('month', -1, CURRENT_DATE())) THEN INVOICE_AMOUNT_LOCAL ELSE 0 END)
        ORDER BY SUM(CASE WHEN DATE_TRUNC('month', POSTING_DATE) = DATE_TRUNC('month', CURRENT_DATE()) THEN INVOICE_AMOUNT_LOCAL ELSE 0 END)
               - SUM(CASE WHEN DATE_TRUNC('month', POSTING_DATE) = DATE_TRUNC('month', DATEADD('month', -1, CURRENT_DATE())) THEN INVOICE_AMOUNT_LOCAL ELSE 0 END) DESC
      ),
      by_status AS (
        SELECT 'INVOICE_STATUS' AS ROW_TYPE, INVOICE_STATUS AS DRIVER,
          SUM(CASE WHEN DATE_TRUNC('month', POSTING_DATE) = DATE_TRUNC('month', CURRENT_DATE()) THEN INVOICE_AMOUNT_LOCAL ELSE 0 END) AS THIS_MONTH_SPEND,
          SUM(CASE WHEN DATE_TRUNC('month', POSTING_DATE) = DATE_TRUNC('month', DATEADD('month', -1, CURRENT_DATE())) THEN INVOICE_AMOUNT_LOCAL ELSE 0 END) AS LAST_MONTH_SPEND
        FROM base WHERE DATE_TRUNC('month', POSTING_DATE) IN (DATE_TRUNC('month', CURRENT_DATE()), DATE_TRUNC('month', DATEADD('month', -1, CURRENT_DATE())))
        GROUP BY INVOICE_STATUS
        HAVING SUM(CASE WHEN DATE_TRUNC('month', POSTING_DATE) = DATE_TRUNC('month', CURRENT_DATE()) THEN INVOICE_AMOUNT_LOCAL ELSE 0 END)
             > SUM(CASE WHEN DATE_TRUNC('month', POSTING_DATE) = DATE_TRUNC('month', DATEADD('month', -1, CURRENT_DATE())) THEN INVOICE_AMOUNT_LOCAL ELSE 0 END)
        ORDER BY SUM(CASE WHEN DATE_TRUNC('month', POSTING_DATE) = DATE_TRUNC('month', CURRENT_DATE()) THEN INVOICE_AMOUNT_LOCAL ELSE 0 END)
               - SUM(CASE WHEN DATE_TRUNC('month', POSTING_DATE) = DATE_TRUNC('month', DATEADD('month', -1, CURRENT_DATE())) THEN INVOICE_AMOUNT_LOCAL ELSE 0 END) DESC
      )
      SELECT * FROM drivers UNION ALL SELECT * FROM by_purpose UNION ALL SELECT * FROM by_status;

  - name: why_spend_higher_lower_this_month
    question: "Why is our procurement spend higher or lower this month?"
    sql: |
      WITH base AS (
        SELECT * FROM procure2pay.FACT_ALL_SOURCES_VW
        WHERE UPPER(INVOICE_STATUS) NOT IN ('CANCELLED', 'REJECTED') AND POSTING_DATE IS NOT NULL
      ),
      this_month AS (
        SELECT SUM(INVOICE_AMOUNT_LOCAL) AS spend, COUNT(*) AS invoice_cnt
        FROM base WHERE DATE_TRUNC('month', POSTING_DATE) = DATE_TRUNC('month', CURRENT_DATE())
      ),
      last_month AS (
        SELECT SUM(INVOICE_AMOUNT_LOCAL) AS spend, COUNT(*) AS invoice_cnt
        FROM base WHERE DATE_TRUNC('month', POSTING_DATE) = DATE_TRUNC('month', DATEADD('month', -1, CURRENT_DATE()))
      ),
      summary AS (
        SELECT 'SUMMARY' AS ROW_TYPE, 'Total' AS DRIVER,
          (SELECT spend FROM this_month) AS THIS_MONTH_SPEND,
          (SELECT spend FROM last_month) AS LAST_MONTH_SPEND,
          CASE WHEN (SELECT spend FROM last_month) > 0
            THEN ROUND(((SELECT spend FROM this_month) - (SELECT spend FROM last_month)) * 100.0 / (SELECT spend FROM last_month), 1)
            ELSE NULL END AS CHANGE_PCT,
          (SELECT invoice_cnt FROM this_month) AS THIS_MONTH_CNT,
          (SELECT invoice_cnt FROM last_month) AS LAST_MONTH_CNT
      ),
      by_vendor AS (
        SELECT 'VENDOR' AS ROW_TYPE, VENDOR_ID AS DRIVER,
          SUM(CASE WHEN DATE_TRUNC('month', POSTING_DATE) = DATE_TRUNC('month', CURRENT_DATE()) THEN INVOICE_AMOUNT_LOCAL ELSE 0 END) AS THIS_MONTH_SPEND,
          SUM(CASE WHEN DATE_TRUNC('month', POSTING_DATE) = DATE_TRUNC('month', DATEADD('month', -1, CURRENT_DATE())) THEN INVOICE_AMOUNT_LOCAL ELSE 0 END) AS LAST_MONTH_SPEND,
          NULL AS CHANGE_PCT, NULL AS THIS_MONTH_CNT, NULL AS LAST_MONTH_CNT
        FROM base
        WHERE DATE_TRUNC('month', POSTING_DATE) IN (DATE_TRUNC('month', CURRENT_DATE()), DATE_TRUNC('month', DATEADD('month', -1, CURRENT_DATE())))
        GROUP BY VENDOR_ID
        HAVING SUM(INVOICE_AMOUNT_LOCAL) > 0
        ORDER BY SUM(INVOICE_AMOUNT_LOCAL) DESC
      ),
      by_purpose AS (
        SELECT 'PO_PURPOSE' AS ROW_TYPE, COALESCE(PO_PURPOSE, 'Unknown') AS DRIVER,
          SUM(CASE WHEN DATE_TRUNC('month', POSTING_DATE) = DATE_TRUNC('month', CURRENT_DATE()) THEN INVOICE_AMOUNT_LOCAL ELSE 0 END) AS THIS_MONTH_SPEND,
          SUM(CASE WHEN DATE_TRUNC('month', POSTING_DATE) = DATE_TRUNC('month', DATEADD('month', -1, CURRENT_DATE())) THEN INVOICE_AMOUNT_LOCAL ELSE 0 END) AS LAST_MONTH_SPEND,
          NULL AS CHANGE_PCT, NULL AS THIS_MONTH_CNT, NULL AS LAST_MONTH_CNT
        FROM base
        WHERE DATE_TRUNC('month', POSTING_DATE) IN (DATE_TRUNC('month', CURRENT_DATE()), DATE_TRUNC('month', DATEADD('month', -1, CURRENT_DATE())))
        GROUP BY PO_PURPOSE
        HAVING SUM(INVOICE_AMOUNT_LOCAL) > 0
        ORDER BY SUM(INVOICE_AMOUNT_LOCAL) DESC
      )
      SELECT * FROM summary
      UNION ALL SELECT * FROM by_vendor
      UNION ALL SELECT * FROM by_purpose;

  - name: spend_this_quarter_vs_last
    question: "Is our procurement spend higher this quarter than last quarter? If yes or no what are the reasons?"
    sql: |
      WITH base AS (
        SELECT * FROM procure2pay.FACT_ALL_SOURCES_VW
        WHERE UPPER(INVOICE_STATUS) NOT IN ('CANCELLED', 'REJECTED') AND POSTING_DATE IS NOT NULL
      ),
      this_q AS (SELECT SUM(INVOICE_AMOUNT_LOCAL) AS spend FROM base WHERE DATE_TRUNC('quarter', POSTING_DATE) = DATE_TRUNC('quarter', CURRENT_DATE())),
      last_q AS (SELECT SUM(INVOICE_AMOUNT_LOCAL) AS spend FROM base WHERE DATE_TRUNC('quarter', POSTING_DATE) = DATE_TRUNC('quarter', DATEADD('quarter', -1, CURRENT_DATE()))),
      summary AS (
        SELECT 'SUMMARY' AS ROW_TYPE, 'Total' AS DRIVER,
          (SELECT spend FROM this_q) AS THIS_QUARTER_SPEND,
          (SELECT spend FROM last_q) AS LAST_QUARTER_SPEND,
          CASE WHEN (SELECT spend FROM last_q) > 0 THEN ROUND(((SELECT spend FROM this_q) - (SELECT spend FROM last_q)) * 100.0 / (SELECT spend FROM last_q), 1) ELSE NULL END AS CHANGE_PCT
      ),
      by_vendor AS (
        SELECT 'VENDOR' AS ROW_TYPE, VENDOR_ID AS DRIVER,
          SUM(CASE WHEN DATE_TRUNC('quarter', POSTING_DATE) = DATE_TRUNC('quarter', CURRENT_DATE()) THEN INVOICE_AMOUNT_LOCAL ELSE 0 END) AS THIS_QUARTER_SPEND,
          SUM(CASE WHEN DATE_TRUNC('quarter', POSTING_DATE) = DATE_TRUNC('quarter', DATEADD('quarter', -1, CURRENT_DATE())) THEN INVOICE_AMOUNT_LOCAL ELSE 0 END) AS LAST_QUARTER_SPEND, NULL AS CHANGE_PCT
        FROM base WHERE DATE_TRUNC('quarter', POSTING_DATE) IN (DATE_TRUNC('quarter', CURRENT_DATE()), DATE_TRUNC('quarter', DATEADD('quarter', -1, CURRENT_DATE())))
        GROUP BY VENDOR_ID HAVING SUM(INVOICE_AMOUNT_LOCAL) > 0 ORDER BY SUM(INVOICE_AMOUNT_LOCAL) DESC
      ),
      by_purpose AS (
        SELECT 'PO_PURPOSE' AS ROW_TYPE, COALESCE(PO_PURPOSE, 'Unknown') AS DRIVER,
          SUM(CASE WHEN DATE_TRUNC('quarter', POSTING_DATE) = DATE_TRUNC('quarter', CURRENT_DATE()) THEN INVOICE_AMOUNT_LOCAL ELSE 0 END) AS THIS_QUARTER_SPEND,
          SUM(CASE WHEN DATE_TRUNC('quarter', POSTING_DATE) = DATE_TRUNC('quarter', DATEADD('quarter', -1, CURRENT_DATE())) THEN INVOICE_AMOUNT_LOCAL ELSE 0 END) AS LAST_QUARTER_SPEND, NULL AS CHANGE_PCT
        FROM base WHERE DATE_TRUNC('quarter', POSTING_DATE) IN (DATE_TRUNC('quarter', CURRENT_DATE()), DATE_TRUNC('quarter', DATEADD('quarter', -1, CURRENT_DATE())))
        GROUP BY PO_PURPOSE HAVING SUM(INVOICE_AMOUNT_LOCAL) > 0 ORDER BY SUM(INVOICE_AMOUNT_LOCAL) DESC
      )
      SELECT * FROM summary UNION ALL SELECT * FROM by_vendor UNION ALL SELECT * FROM by_purpose;

  - name: why_spend_higher_lower_this_quarter
    question: "Why is our procurement spend higher or lower this quarter?"
    sql: |
      WITH base AS (SELECT * FROM procure2pay.FACT_ALL_SOURCES_VW WHERE UPPER(INVOICE_STATUS) NOT IN ('CANCELLED', 'REJECTED') AND POSTING_DATE IS NOT NULL),
      this_q AS (SELECT SUM(INVOICE_AMOUNT_LOCAL) AS spend FROM base WHERE DATE_TRUNC('quarter', POSTING_DATE) = DATE_TRUNC('quarter', CURRENT_DATE())),
      last_q AS (SELECT SUM(INVOICE_AMOUNT_LOCAL) AS spend FROM base WHERE DATE_TRUNC('quarter', POSTING_DATE) = DATE_TRUNC('quarter', DATEADD('quarter', -1, CURRENT_DATE()))),
      summary AS (SELECT 'SUMMARY' AS ROW_TYPE, 'Total' AS DRIVER, (SELECT spend FROM this_q) AS THIS_QUARTER_SPEND, (SELECT spend FROM last_q) AS LAST_QUARTER_SPEND, CASE WHEN (SELECT spend FROM last_q) > 0 THEN ROUND(((SELECT spend FROM this_q) - (SELECT spend FROM last_q)) * 100.0 / (SELECT spend FROM last_q), 1) ELSE NULL END AS CHANGE_PCT),
      by_vendor AS (SELECT 'VENDOR' AS ROW_TYPE, VENDOR_ID AS DRIVER, SUM(CASE WHEN DATE_TRUNC('quarter', POSTING_DATE) = DATE_TRUNC('quarter', CURRENT_DATE()) THEN INVOICE_AMOUNT_LOCAL ELSE 0 END) AS THIS_QUARTER_SPEND, SUM(CASE WHEN DATE_TRUNC('quarter', POSTING_DATE) = DATE_TRUNC('quarter', DATEADD('quarter', -1, CURRENT_DATE())) THEN INVOICE_AMOUNT_LOCAL ELSE 0 END) AS LAST_QUARTER_SPEND, NULL AS CHANGE_PCT FROM base WHERE DATE_TRUNC('quarter', POSTING_DATE) IN (DATE_TRUNC('quarter', CURRENT_DATE()), DATE_TRUNC('quarter', DATEADD('quarter', -1, CURRENT_DATE()))) GROUP BY VENDOR_ID HAVING SUM(INVOICE_AMOUNT_LOCAL) > 0 ORDER BY SUM(INVOICE_AMOUNT_LOCAL) DESC),
      by_purpose AS (SELECT 'PO_PURPOSE' AS ROW_TYPE, COALESCE(PO_PURPOSE, 'Unknown') AS DRIVER, SUM(CASE WHEN DATE_TRUNC('quarter', POSTING_DATE) = DATE_TRUNC('quarter', CURRENT_DATE()) THEN INVOICE_AMOUNT_LOCAL ELSE 0 END) AS THIS_QUARTER_SPEND, SUM(CASE WHEN DATE_TRUNC('quarter', POSTING_DATE) = DATE_TRUNC('quarter', DATEADD('quarter', -1, CURRENT_DATE())) THEN INVOICE_AMOUNT_LOCAL ELSE 0 END) AS LAST_QUARTER_SPEND, NULL AS CHANGE_PCT FROM base WHERE DATE_TRUNC('quarter', POSTING_DATE) IN (DATE_TRUNC('quarter', CURRENT_DATE()), DATE_TRUNC('quarter', DATEADD('quarter', -1, CURRENT_DATE()))) GROUP BY PO_PURPOSE HAVING SUM(INVOICE_AMOUNT_LOCAL) > 0 ORDER BY SUM(INVOICE_AMOUNT_LOCAL) DESC)
      SELECT * FROM summary UNION ALL SELECT * FROM by_vendor UNION ALL SELECT * FROM by_purpose;

  - name: spend_this_year_vs_last
    question: "Is our procurement spend higher this year than last year? If yes or no what are the reasons?"
    sql: |
      WITH base AS (
        SELECT * FROM procure2pay.FACT_ALL_SOURCES_VW
        WHERE UPPER(INVOICE_STATUS) NOT IN ('CANCELLED', 'REJECTED') AND POSTING_DATE IS NOT NULL
      ),
      this_y AS (SELECT SUM(INVOICE_AMOUNT_LOCAL) AS spend FROM base WHERE DATE_TRUNC('year', POSTING_DATE) = DATE_TRUNC('year', CURRENT_DATE())),
      last_y AS (SELECT SUM(INVOICE_AMOUNT_LOCAL) AS spend FROM base WHERE DATE_TRUNC('year', POSTING_DATE) = DATE_TRUNC('year', DATEADD('year', -1, CURRENT_DATE()))),
      summary AS (
        SELECT 'SUMMARY' AS ROW_TYPE, 'Total' AS DRIVER,
          (SELECT spend FROM this_y) AS THIS_YEAR_SPEND,
          (SELECT spend FROM last_y) AS LAST_YEAR_SPEND,
          CASE WHEN (SELECT spend FROM last_y) > 0 THEN ROUND(((SELECT spend FROM this_y) - (SELECT spend FROM last_y)) * 100.0 / (SELECT spend FROM last_y), 1) ELSE NULL END AS CHANGE_PCT
      ),
      by_vendor AS (
        SELECT 'VENDOR' AS ROW_TYPE, VENDOR_ID AS DRIVER,
          SUM(CASE WHEN DATE_TRUNC('year', POSTING_DATE) = DATE_TRUNC('year', CURRENT_DATE()) THEN INVOICE_AMOUNT_LOCAL ELSE 0 END) AS THIS_YEAR_SPEND,
          SUM(CASE WHEN DATE_TRUNC('year', POSTING_DATE) = DATE_TRUNC('year', DATEADD('year', -1, CURRENT_DATE())) THEN INVOICE_AMOUNT_LOCAL ELSE 0 END) AS LAST_YEAR_SPEND, NULL AS CHANGE_PCT
        FROM base WHERE DATE_TRUNC('year', POSTING_DATE) IN (DATE_TRUNC('year', CURRENT_DATE()), DATE_TRUNC('year', DATEADD('year', -1, CURRENT_DATE())))
        GROUP BY VENDOR_ID HAVING SUM(INVOICE_AMOUNT_LOCAL) > 0 ORDER BY SUM(INVOICE_AMOUNT_LOCAL) DESC
      ),
      by_purpose AS (
        SELECT 'PO_PURPOSE' AS ROW_TYPE, COALESCE(PO_PURPOSE, 'Unknown') AS DRIVER,
          SUM(CASE WHEN DATE_TRUNC('year', POSTING_DATE) = DATE_TRUNC('year', CURRENT_DATE()) THEN INVOICE_AMOUNT_LOCAL ELSE 0 END) AS THIS_YEAR_SPEND,
          SUM(CASE WHEN DATE_TRUNC('year', POSTING_DATE) = DATE_TRUNC('year', DATEADD('year', -1, CURRENT_DATE())) THEN INVOICE_AMOUNT_LOCAL ELSE 0 END) AS LAST_YEAR_SPEND, NULL AS CHANGE_PCT
        FROM base WHERE DATE_TRUNC('year', POSTING_DATE) IN (DATE_TRUNC('year', CURRENT_DATE()), DATE_TRUNC('year', DATEADD('year', -1, CURRENT_DATE())))
        GROUP BY PO_PURPOSE HAVING SUM(INVOICE_AMOUNT_LOCAL) > 0 ORDER BY SUM(INVOICE_AMOUNT_LOCAL) DESC
      )
      SELECT * FROM summary UNION ALL SELECT * FROM by_vendor UNION ALL SELECT * FROM by_purpose;

  - name: why_spend_higher_lower_this_year
    question: "Why is our procurement spend higher or lower this year?"
    sql: |
      WITH base AS (SELECT * FROM procure2pay.FACT_ALL_SOURCES_VW WHERE UPPER(INVOICE_STATUS) NOT IN ('CANCELLED', 'REJECTED') AND POSTING_DATE IS NOT NULL),
      this_y AS (SELECT SUM(INVOICE_AMOUNT_LOCAL) AS spend FROM base WHERE DATE_TRUNC('year', POSTING_DATE) = DATE_TRUNC('year', CURRENT_DATE())),
      last_y AS (SELECT SUM(INVOICE_AMOUNT_LOCAL) AS spend FROM base WHERE DATE_TRUNC('year', POSTING_DATE) = DATE_TRUNC('year', DATEADD('year', -1, CURRENT_DATE()))),
      summary AS (SELECT 'SUMMARY' AS ROW_TYPE, 'Total' AS DRIVER, (SELECT spend FROM this_y) AS THIS_YEAR_SPEND, (SELECT spend FROM last_y) AS LAST_YEAR_SPEND, CASE WHEN (SELECT spend FROM last_y) > 0 THEN ROUND(((SELECT spend FROM this_y) - (SELECT spend FROM last_y)) * 100.0 / (SELECT spend FROM last_y), 1) ELSE NULL END AS CHANGE_PCT),
      by_vendor AS (SELECT 'VENDOR' AS ROW_TYPE, VENDOR_ID AS DRIVER, SUM(CASE WHEN DATE_TRUNC('year', POSTING_DATE) = DATE_TRUNC('year', CURRENT_DATE()) THEN INVOICE_AMOUNT_LOCAL ELSE 0 END) AS THIS_YEAR_SPEND, SUM(CASE WHEN DATE_TRUNC('year', POSTING_DATE) = DATE_TRUNC('year', DATEADD('year', -1, CURRENT_DATE())) THEN INVOICE_AMOUNT_LOCAL ELSE 0 END) AS LAST_YEAR_SPEND, NULL AS CHANGE_PCT FROM base WHERE DATE_TRUNC('year', POSTING_DATE) IN (DATE_TRUNC('year', CURRENT_DATE()), DATE_TRUNC('year', DATEADD('year', -1, CURRENT_DATE()))) GROUP BY VENDOR_ID HAVING SUM(INVOICE_AMOUNT_LOCAL) > 0 ORDER BY SUM(INVOICE_AMOUNT_LOCAL) DESC),
      by_purpose AS (SELECT 'PO_PURPOSE' AS ROW_TYPE, COALESCE(PO_PURPOSE, 'Unknown') AS DRIVER, SUM(CASE WHEN DATE_TRUNC('year', POSTING_DATE) = DATE_TRUNC('year', CURRENT_DATE()) THEN INVOICE_AMOUNT_LOCAL ELSE 0 END) AS THIS_YEAR_SPEND, SUM(CASE WHEN DATE_TRUNC('year', POSTING_DATE) = DATE_TRUNC('year', DATEADD('year', -1, CURRENT_DATE())) THEN INVOICE_AMOUNT_LOCAL ELSE 0 END) AS LAST_YEAR_SPEND, NULL AS CHANGE_PCT FROM base WHERE DATE_TRUNC('year', POSTING_DATE) IN (DATE_TRUNC('year', CURRENT_DATE()), DATE_TRUNC('year', DATEADD('year', -1, CURRENT_DATE()))) GROUP BY PO_PURPOSE HAVING SUM(INVOICE_AMOUNT_LOCAL) > 0 ORDER BY SUM(INVOICE_AMOUNT_LOCAL) DESC)
      SELECT * FROM summary UNION ALL SELECT * FROM by_vendor UNION ALL SELECT * FROM by_purpose;

  - name: cost_reduction_opportunities
    question: "Suggest ways to reduce procurement costs based on our spend data"
    use_as_onboarding_question: true
    sql: |
      WITH base AS (
        SELECT * FROM procure2pay.FACT_ALL_SOURCES_VW
        WHERE UPPER(INVOICE_STATUS) NOT IN ('CANCELLED', 'REJECTED')
      ),
      tot_spend AS (SELECT COALESCE(SUM(INVOICE_AMOUNT_LOCAL), 1) AS s FROM base),
      tot_cnt AS (SELECT COUNT(*) AS c FROM base),
      top5_vendors AS (
        SELECT SUM(INVOICE_AMOUNT_LOCAL) AS spend FROM base GROUP BY VENDOR_ID
        QUALIFY ROW_NUMBER() OVER (ORDER BY SUM(INVOICE_AMOUNT_LOCAL) DESC) <= 5
      ),
      vendor_consolidation AS (
        SELECT '1_Vendor_Consolidation' AS OPPORTUNITY_AREA,
          ROUND(SUM(spend) * 100.0 / (SELECT s FROM tot_spend), 1) AS PCT_OF_SPEND,
          ROUND(SUM(spend), 2) AS AMOUNT,
          'Top 5 vendors by spend - negotiate volume discounts' AS FINDING,
          'Consolidate vendors; negotiate volume discounts with top spenders' AS RECOMMENDED_ACTION
        FROM top5_vendors
      ),
      disputed AS (
        SELECT '2_Disputed_Invoices' AS OPPORTUNITY_AREA,
          ROUND(COUNT(*) * 100.0 / NULLIF((SELECT c FROM tot_cnt), 0), 1) AS PCT_OF_SPEND,
          ROUND(COALESCE(SUM(INVOICE_AMOUNT_LOCAL), 0), 2) AS AMOUNT,
          CONCAT(COALESCE(COUNT(*), 0), ' disputed invoices - $', ROUND(COALESCE(SUM(INVOICE_AMOUNT_LOCAL), 0), 0)) AS FINDING,
          'Resolve disputes within 30 days; escalate pricing variances to avoid aging' AS RECOMMENDED_ACTION
        FROM base WHERE INVOICE_STATUS = 'Disputed'
      ),
      overdue AS (
        SELECT '3_Overdue_Invoices' AS OPPORTUNITY_AREA,
          ROUND(COUNT(*) * 100.0 / NULLIF((SELECT COUNT(*) FROM base WHERE INVOICE_STATUS IN ('Open','Due','Overdue','Disputed')), 0), 1) AS PCT_OF_SPEND,
          ROUND(COALESCE(SUM(INVOICE_AMOUNT_LOCAL), 0), 2) AS AMOUNT,
          CONCAT(COALESCE(COUNT(*), 0), ' overdue, avg ', ROUND(COALESCE(AVG(AGING_DAYS), 0), 0), ' days') AS FINDING,
          'Prioritize by amount; contact vendors to avoid late fees and penalties' AS RECOMMENDED_ACTION
        FROM base WHERE INVOICE_STATUS = 'Overdue'
      ),
      late_pmt AS (
        SELECT '4_Late_Payment_Cost' AS OPPORTUNITY_AREA,
          COALESCE(LATE_PAYMENT_RATE_PCT, 0) AS PCT_OF_SPEND,
          COALESCE(LATE_PAYMENT_AMOUNT, 0) AS AMOUNT,
          CONCAT(COALESCE(LATE_PAYMENT_COUNT, 0), ' late payments') AS FINDING,
          'Improve cycle time; automate approvals; capture early payment discounts' AS RECOMMENDED_ACTION
        FROM (SELECT * FROM procure2pay.LATE_PAYMENT_AMOUNT_VW ORDER BY YEAR DESC, MONTH DESC LIMIT 1)
      ),
      early_benefit AS (
        SELECT '5_Early_Payment_Discount' AS OPPORTUNITY_AREA,
          COALESCE(NEPBI_PERCENT, 0) AS PCT_OF_SPEND,
          COALESCE(TOTAL_NET_BENEFIT, 0) AS AMOUNT,
          CONCAT('Current benefit: ', ROUND(COALESCE(NEPBI_PERCENT, 0), 2), '% of spend') AS FINDING,
          'Increase early payments to capture more discount; target NEPBI > 2%' AS RECOMMENDED_ACTION
        FROM procure2pay.NET_EARLY_PAYMENT_BENEFIT_INDEX_VW
      ),
      dup_pmt AS (
        SELECT '6_Duplicate_Payments' AS OPPORTUNITY_AREA,
          ROUND(COALESCE(AVG(DUPLICATE_PAYMENT_RATE), 0), 1) AS PCT_OF_SPEND,
          ROUND(COALESCE(SUM(DUPLICATE_PAYMENT_AMOUNT), 0), 2) AS AMOUNT,
          CONCAT(COALESCE(COUNT(*), 0), ' invoices with potential duplicates') AS FINDING,
          'Implement duplicate check before payment; reconcile regularly' AS RECOMMENDED_ACTION
        FROM procure2pay.DUPLICATE_PAYMENTS_FOR_INVOICE_VW WHERE DUPLICATE_PAYMENT_AMOUNT > 0
      ),
      grir AS (
        SELECT '7_GRIR_Clearing' AS OPPORTUNITY_AREA,
          NULL AS PCT_OF_SPEND,
          COALESCE(TOTAL_GRIR_BLNC, 0) AS AMOUNT,
          CONCAT(COALESCE(INVOICE_COUNT, 0), ' items in GR/IR') AS FINDING,
          'Clear GR/IR items; match receipts to invoices to reduce working capital' AS RECOMMENDED_ACTION
        FROM (SELECT * FROM procure2pay.GR_IR_OUTSTANDING_BALANCE_VW ORDER BY YEAR DESC, MONTH DESC LIMIT 1)
      ),
      spend_cat AS (
        SELECT '8_Spend_By_Category' AS OPPORTUNITY_AREA,
          ROUND(SUM(INVOICE_AMOUNT_LOCAL) * 100.0 / (SELECT s FROM tot_spend), 1) AS PCT_OF_SPEND,
          ROUND(SUM(INVOICE_AMOUNT_LOCAL), 2) AS AMOUNT,
          CONCAT(COALESCE(PO_PURPOSE, 'Unknown'), ': ', COUNT(*), ' invoices') AS FINDING,
          'Review category contracts; consolidate Services/Goods where possible' AS RECOMMENDED_ACTION
        FROM base WHERE PO_PURPOSE IS NOT NULL
        GROUP BY PO_PURPOSE
        ORDER BY SUM(INVOICE_AMOUNT_LOCAL) DESC
      )
      SELECT * FROM vendor_consolidation
      UNION ALL SELECT * FROM disputed
      UNION ALL SELECT * FROM overdue
      UNION ALL SELECT * FROM late_pmt
      UNION ALL SELECT * FROM early_benefit
      UNION ALL SELECT * FROM dup_pmt
      UNION ALL SELECT * FROM grir
      UNION ALL SELECT * FROM spend_cat;

  - name: spending_overview
    question: "Show me total spend YTD, monthly trends, and top 5 vendors"
    use_as_onboarding_question: true
    sql: |
      WITH base AS (
        SELECT * FROM procure2pay.FACT_ALL_SOURCES_VW
        WHERE UPPER(INVOICE_STATUS) NOT IN ('CANCELLED', 'REJECTED')
      ),
      total_spend_ytd AS (SELECT SUM(INVOICE_AMOUNT_LOCAL) AS value_num FROM base WHERE POSTING_DATE <= CURRENT_DATE()),
      monthly_trend AS (
        SELECT TO_CHAR(POSTING_DATE, 'YYYY-MM') AS month, SUM(INVOICE_AMOUNT_LOCAL) AS value_num FROM base GROUP BY 1 ORDER BY 1
      ),
      mom_change AS (
        SELECT CASE WHEN prev.value_num = 0 OR prev.value_num IS NULL THEN NULL
          ELSE (curr.value_num - prev.value_num) / prev.value_num * 100 END AS value_num
        FROM (SELECT value_num FROM monthly_trend ORDER BY month DESC LIMIT 1) curr,
             (SELECT value_num FROM monthly_trend ORDER BY month DESC LIMIT 1 OFFSET 1) prev
      ),
      vendor_spend AS (SELECT VENDOR_ID, SUM(INVOICE_AMOUNT_LOCAL) AS spend FROM base GROUP BY 1),
      total_spend AS (SELECT SUM(spend) AS tot FROM vendor_spend),
      top5 AS (SELECT spend FROM vendor_spend ORDER BY spend DESC),
      top5_share AS (SELECT ROUND(SUM(spend) / (SELECT tot FROM total_spend) * 100, 2) AS value_num FROM top5)
      SELECT 'TOTAL_SPEND_YTD' AS metric, (SELECT value_num FROM total_spend_ytd) AS value_num
      UNION ALL SELECT 'MOM_CHANGE_PCT', (SELECT value_num FROM mom_change)
      UNION ALL SELECT 'TOP5_VENDORS_SHARE_PCT', (SELECT value_num FROM top5_share);

  - name: vendor_analysis
    question: "Analyze vendor concentration and dependency"
    use_as_onboarding_question: true
    sql: |
      SELECT F.VENDOR_ID, COALESCE(V.VENDOR_NAME, 'Unknown') AS VENDOR_NAME,
        COUNT(DISTINCT F.DOCUMENT_NUMBER) AS INVOICE_COUNT, SUM(F.INVOICE_AMOUNT_LOCAL) AS TOTAL_SPEND,
        ROUND(SUM(F.INVOICE_AMOUNT_LOCAL) * 100.0 / NULLIF(SUM(SUM(F.INVOICE_AMOUNT_LOCAL)) OVER (), 0), 2) AS SPEND_SHARE_PCT
      FROM procure2pay.FACT_ALL_SOURCES_VW F
      LEFT JOIN procure2pay.DIM_VENDOR_VW V ON F.VENDOR_ID = V.VENDOR_ID
      WHERE UPPER(F.INVOICE_STATUS) NOT IN ('CANCELLED', 'REJECTED')
      GROUP BY F.VENDOR_ID, V.VENDOR_NAME ORDER BY TOTAL_SPEND DESC;

  - name: payment_performance
    question: "Show payment delays and cycle time issues"
    sql: |
      SELECT * FROM procure2pay.PAYMENT_PROCESSING_CYCLE_TIME_VW
      ORDER BY YEAR DESC, MONTH DESC;

  - name: invoice_aging
    question: "Show overdue invoices by aging buckets"
    sql: |
      WITH __fact_invoices AS (
        SELECT
          invoice_status,
          posting_date,
          invoice_amount_local AS invoice_amount,
          aging_days
        FROM procure2pay.FACT_ALL_SOURCES_VW
      )
      SELECT
        CASE
          WHEN fi.aging_days <= 30
          THEN '0-30 days'
          WHEN fi.aging_days <= 60
          THEN '31-60 days'
          WHEN fi.aging_days <= 90
          THEN '61-90 days'
          ELSE '90+ days'
        END AS aging_bucket,
        COUNT(*) AS invoice_count,
        SUM(fi.invoice_amount) AS total_amount,
        MIN(fi.posting_date) AS start_date,
        MAX(fi.posting_date) AS end_date
      FROM __fact_invoices AS fi
      WHERE
        fi.invoice_status = 'Overdue' AND fi.aging_days > 0
      GROUP BY
        aging_bucket
      ORDER BY
        CASE aging_bucket
          WHEN '0-30 days'
          THEN 1
          WHEN '31-60 days'
          THEN 2
          WHEN '61-90 days'
          THEN 3
          ELSE 4
        END;

  - name: overdue_invoices_detail
    question: "List overdue invoices with amount and aging"
    sql: |
      SELECT DOCUMENT_NUMBER, VENDOR_ID, INVOICE_AMOUNT_LOCAL, AGING_DAYS, DUE_DATE, PURCHASE_ORDER_REFERENCE, PO_PURPOSE
      FROM procure2pay.FACT_ALL_SOURCES_VW WHERE INVOICE_STATUS = 'Overdue'
      ORDER BY AGING_DAYS DESC, INVOICE_AMOUNT_LOCAL DESC LIMIT 50;

  - name: disputed_invoices
    question: "Show disputed invoices"
    sql: |
      SELECT DOCUMENT_NUMBER, VENDOR_ID, INVOICE_AMOUNT_LOCAL, AGING_DAYS, DUE_DATE, POSTING_DATE, PURCHASE_ORDER_REFERENCE
      FROM procure2pay.FACT_ALL_SOURCES_VW WHERE INVOICE_STATUS = 'Disputed'
      ORDER BY INVOICE_AMOUNT_LOCAL DESC;

  - name: open_vs_paid_summary
    question: "Summary of open vs paid invoices by status"
    sql: |
      SELECT INVOICE_STATUS, COUNT(*) AS INVOICE_COUNT, SUM(INVOICE_AMOUNT_LOCAL) AS TOTAL_AMOUNT
      FROM procure2pay.FACT_ALL_SOURCES_VW WHERE UPPER(INVOICE_STATUS) NOT IN ('CANCELLED','REJECTED')
      GROUP BY INVOICE_STATUS ORDER BY TOTAL_AMOUNT DESC;

  - name: spend_by_po_purpose
    question: "Spend by PO purpose or invoice type"
    sql: |
      SELECT COALESCE(PO_PURPOSE,'Unknown') AS PO_PURPOSE, COUNT(*) AS INVOICE_COUNT, SUM(INVOICE_AMOUNT_LOCAL) AS TOTAL_SPEND
      FROM procure2pay.FACT_ALL_SOURCES_VW WHERE UPPER(INVOICE_STATUS) NOT IN ('CANCELLED','REJECTED')
      GROUP BY PO_PURPOSE ORDER BY TOTAL_SPEND DESC;

  - name: ap_balance_trend
    question: "AP balance trend by year and month"
    sql: |
      SELECT YEAR, MONTH, AP_BALANCE, INVOICE_COUNT FROM procure2pay.ACCOUNTS_PAYABLE_BALANCE_VW
      ORDER BY YEAR DESC, MONTH DESC;

  - name: days_payable_outstanding
    question: "Days payable outstanding (DPO)"
    sql: |
      SELECT * FROM procure2pay.DAYS_PAYABLE_OUTSTANDING_VW ORDER BY YEAR DESC, MONTH DESC LIMIT 50;

  - name: full_payment_rate
    question: "Full payment rate percentage"
    sql: |
      SELECT * FROM procure2pay.FULL_PAYMENT_RATE_VW ORDER BY YEAR DESC, MONTH DESC LIMIT 24;

  - name: on_time_payment_rate
    question: "On-time payment rate"
    sql: |
      SELECT * FROM procure2pay.ON_TIME_PAYMENT_RATE_VW ORDER BY YEAR DESC, MONTH DESC LIMIT 24;

  - name: gr_ir_outstanding
    question: "GR/IR outstanding balance"
    sql: |
      SELECT * FROM procure2pay.GR_IR_OUTSTANDING_BALANCE_VW ORDER BY YEAR DESC, MONTH DESC LIMIT 24;

  - name: gr_ir_hotspots_clearing_plan
    question: "Show GR/IR outstanding balance by month and highlight which recent months have the highest GR/IR balance so we can prioritize clearing."
    sql: |
      SELECT
        YEAR,
        MONTH,
        INVOICE_COUNT,
        TOTAL_GRIR_BLNC
      FROM procure2pay.GR_IR_OUTSTANDING_BALANCE_VW
      ORDER BY YEAR DESC, MONTH DESC
      LIMIT 24;

  - name: gr_ir_working_capital_release
    question: "Estimate the working capital that would be released by clearing all GR/IR items older than 60 and 90 days, by month."
    sql: |
      SELECT
        YEAR,
        MONTH,
        SUM(GRIR_OVER_60) AS GRIR_OVER_60_TOTAL,
        SUM(GRIR_OVER_90) AS GRIR_OVER_90_TOTAL
      FROM procure2pay.GR_IR_AGING_VW
      GROUP BY YEAR, MONTH
      ORDER BY YEAR DESC, MONTH DESC
      LIMIT 24;

  - name: gr_ir_aging_detail
    question: "Show GR/IR aging by bucket so I can see items older than 30, 60, and 90 days."
    sql: |
      SELECT
        YEAR,
        MONTH,
        AGE_DAYS,
        TOTAL_GRIR_BALANCE,
        GRIR_OVER_30,
        GRIR_OVER_60,
        GRIR_OVER_90
      FROM procure2pay.GR_IR_AGING_VW
      ORDER BY YEAR DESC, MONTH DESC, AGE_DAYS;

  - name: gr_ir_root_cause_summary
    question: "Using GR/IR aging and outstanding balance data, explain the likely root-cause buckets (missing goods receipt, invoice not posted, price or quantity mismatch) and for each bucket suggest 2–3 concrete remediation actions."
    sql: |
      SELECT
        YEAR,
        MONTH,
        AGE_DAYS,
        TOTAL_GRIR_BALANCE,
        GRIR_OVER_30,
        GRIR_OVER_60,
        GRIR_OVER_90
      FROM procure2pay.GR_IR_AGING_VW
      ORDER BY YEAR DESC, MONTH DESC, AGE_DAYS;

  - name: gr_ir_top_items_for_vendor_followup
    question: "Based on GR/IR aging and outstanding balances, draft vendor-facing follow-up templates we can use for high-priority GR/IR items, with realistic subject lines and concise bullet points."
    sql: |
      SELECT
        YEAR,
        MONTH,
        INVOICE_COUNT,
        TOTAL_GRIR_BLNC
      FROM procure2pay.GR_IR_OUTSTANDING_BALANCE_VW
      ORDER BY TOTAL_GRIR_BLNC DESC
      LIMIT 10;

  - name: late_payment_amount
    question: "Late payment amount"
    sql: |
      SELECT * FROM procure2pay.LATE_PAYMENT_AMOUNT_VW ORDER BY YEAR DESC, MONTH DESC LIMIT 24;

  - name: invoices_by_po
    question: "Invoices linked to purchase orders with PO amount"
    sql: |
      SELECT PURCHASE_ORDER_REFERENCE, PO_AMOUNT, PO_PURPOSE, COUNT(*) AS INVOICE_COUNT, SUM(INVOICE_AMOUNT_LOCAL) AS TOTAL_INVOICED
      FROM procure2pay.FACT_ALL_SOURCES_VW WHERE PURCHASE_ORDER_REFERENCE IS NOT NULL
      GROUP BY PURCHASE_ORDER_REFERENCE, PO_AMOUNT, PO_PURPOSE ORDER BY TOTAL_INVOICED DESC;

  - name: cash_flow_forecast
    question: "Forecast cash outflow for the next 7, 14, 30, 60, and 90 days"
    sql: "SELECT * FROM procure2pay.CASH_FLOW_FORECAST_VW"

  # =============================================================================
  # INVOICE DISPUTE PREDICTION QUERIES
  # =============================================================================

  - name: dispute_risk_scoring
    question: "Which open invoices are at highest risk of becoming disputed?"
    sql: |
      WITH vendor_history AS (
        SELECT
          VENDOR_ID,
          COUNT(*) AS total_invoices,
          SUM(CASE WHEN INVOICE_STATUS = 'Disputed' THEN 1 ELSE 0 END) AS disputed_count,
          ROUND(SUM(CASE WHEN INVOICE_STATUS = 'Disputed' THEN 1 ELSE 0 END) * 100.0
                / NULLIF(COUNT(*), 0), 2) AS vendor_dispute_rate
        FROM procure2pay.FACT_ALL_SOURCES_VW
        WHERE UPPER(INVOICE_STATUS) NOT IN ('CANCELLED', 'REJECTED')
        GROUP BY VENDOR_ID
      ),
      category_history AS (
        SELECT
          COALESCE(PO_PURPOSE, 'Unknown') AS PO_PURPOSE,
          ROUND(SUM(CASE WHEN INVOICE_STATUS = 'Disputed' THEN 1 ELSE 0 END) * 100.0
                / NULLIF(COUNT(*), 0), 2) AS category_dispute_rate
        FROM procure2pay.FACT_ALL_SOURCES_VW
        WHERE UPPER(INVOICE_STATUS) NOT IN ('CANCELLED', 'REJECTED')
        GROUP BY PO_PURPOSE
      ),
      amount_stats AS (
        SELECT
          AVG(INVOICE_AMOUNT_LOCAL) AS avg_amount,
          STDDEV(INVOICE_AMOUNT_LOCAL) AS stddev_amount
        FROM procure2pay.FACT_ALL_SOURCES_VW
        WHERE UPPER(INVOICE_STATUS) NOT IN ('CANCELLED', 'REJECTED')
      ),
      po_variance AS (
        SELECT
          DOCUMENT_NUMBER,
          CASE
            WHEN PO_AMOUNT > 0 AND INVOICE_AMOUNT_LOCAL > 0
            THEN ABS(INVOICE_AMOUNT_LOCAL - PO_AMOUNT) / PO_AMOUNT * 100
            ELSE 0
          END AS po_variance_pct
        FROM procure2pay.FACT_ALL_SOURCES_VW
        WHERE PURCHASE_ORDER_REFERENCE IS NOT NULL
      ),
      open_invoices AS (
        SELECT
          f.DOCUMENT_NUMBER,
          f.VENDOR_ID,
          v.VENDOR_NAME,
          f.INVOICE_AMOUNT_LOCAL,
          f.PO_AMOUNT,
          f.PURCHASE_ORDER_REFERENCE,
          f.PO_PURPOSE,
          f.POSTING_DATE,
          f.DUE_DATE,
          f.AGING_DAYS,
          -- Risk Factor 1: Vendor dispute history (0-30 points)
          LEAST(COALESCE(vh.vendor_dispute_rate, 0) * 3, 30) AS vendor_risk_score,
          -- Risk Factor 2: Category dispute history (0-20 points)
          LEAST(COALESCE(ch.category_dispute_rate, 0) * 2, 20) AS category_risk_score,
          -- Risk Factor 3: No PO reference (0 or 15 points)
          CASE WHEN f.PURCHASE_ORDER_REFERENCE IS NULL THEN 15 ELSE 0 END AS no_po_risk_score,
          -- Risk Factor 4: PO variance > 5% (0-20 points)
          CASE
            WHEN pv.po_variance_pct > 20 THEN 20
            WHEN pv.po_variance_pct > 10 THEN 15
            WHEN pv.po_variance_pct > 5 THEN 10
            ELSE 0
          END AS variance_risk_score,
          -- Risk Factor 5: Unusual amount (0-15 points)
          CASE
            WHEN f.INVOICE_AMOUNT_LOCAL > (SELECT avg_amount + 2 * stddev_amount FROM amount_stats) THEN 15
            WHEN f.INVOICE_AMOUNT_LOCAL > (SELECT avg_amount + stddev_amount FROM amount_stats) THEN 8
            ELSE 0
          END AS amount_risk_score,
          vh.vendor_dispute_rate,
          ch.category_dispute_rate,
          pv.po_variance_pct
        FROM procure2pay.FACT_ALL_SOURCES_VW f
        LEFT JOIN procure2pay.DIM_VENDOR_VW v ON f.VENDOR_ID = v.VENDOR_ID
        LEFT JOIN vendor_history vh ON f.VENDOR_ID = vh.VENDOR_ID
        LEFT JOIN category_history ch ON COALESCE(f.PO_PURPOSE, 'Unknown') = ch.PO_PURPOSE
        LEFT JOIN po_variance pv ON f.DOCUMENT_NUMBER = pv.DOCUMENT_NUMBER
        WHERE f.INVOICE_STATUS IN ('Open', 'Due')
      )
      SELECT
        DOCUMENT_NUMBER,
        VENDOR_ID,
        VENDOR_NAME,
        INVOICE_AMOUNT_LOCAL,
        PO_PURPOSE,
        POSTING_DATE,
        DUE_DATE,
        -- Total risk score (0-100)
        ROUND(vendor_risk_score + category_risk_score + no_po_risk_score
              + variance_risk_score + amount_risk_score, 0) AS DISPUTE_RISK_SCORE,
        CASE
          WHEN (vendor_risk_score + category_risk_score + no_po_risk_score
                + variance_risk_score + amount_risk_score) >= 60 THEN 'HIGH'
          WHEN (vendor_risk_score + category_risk_score + no_po_risk_score
                + variance_risk_score + amount_risk_score) >= 35 THEN 'MEDIUM'
          ELSE 'LOW'
        END AS RISK_LEVEL,
        -- Risk breakdown
        CONCAT(
          CASE WHEN vendor_risk_score > 10 THEN 'Vendor history; ' ELSE '' END,
          CASE WHEN category_risk_score > 8 THEN 'Category risk; ' ELSE '' END,
          CASE WHEN no_po_risk_score > 0 THEN 'No PO; ' ELSE '' END,
          CASE WHEN variance_risk_score > 0 THEN 'PO variance; ' ELSE '' END,
          CASE WHEN amount_risk_score > 0 THEN 'Unusual amount' ELSE '' END
        ) AS RISK_FACTORS,
        ROUND(vendor_dispute_rate, 1) AS VENDOR_DISPUTE_RATE_PCT,
        ROUND(COALESCE(po_variance_pct, 0), 1) AS PO_VARIANCE_PCT
      FROM open_invoices
      ORDER BY DISPUTE_RISK_SCORE DESC
      LIMIT 50;

  - name: dispute_pattern_analysis
    question: "What patterns lead to invoice disputes?"
    sql: |
      WITH base AS (
        SELECT
          VENDOR_ID,
          PO_PURPOSE,
          INVOICE_STATUS,
          INVOICE_AMOUNT_LOCAL,
          CASE WHEN PURCHASE_ORDER_REFERENCE IS NULL THEN 'No PO' ELSE 'Has PO' END AS PO_STATUS,
          CASE
            WHEN INVOICE_AMOUNT_LOCAL < 1000 THEN 'Under $1K'
            WHEN INVOICE_AMOUNT_LOCAL < 10000 THEN '$1K-$10K'
            WHEN INVOICE_AMOUNT_LOCAL < 50000 THEN '$10K-$50K'
            WHEN INVOICE_AMOUNT_LOCAL < 100000 THEN '$50K-$100K'
            ELSE 'Over $100K'
          END AS AMOUNT_BUCKET,
          CASE WHEN INVOICE_STATUS = 'Disputed' THEN 1 ELSE 0 END AS IS_DISPUTED
        FROM procure2pay.FACT_ALL_SOURCES_VW
        WHERE UPPER(INVOICE_STATUS) NOT IN ('CANCELLED', 'REJECTED')
      ),
      by_category AS (
        SELECT
          'BY_CATEGORY' AS ANALYSIS_TYPE,
          COALESCE(PO_PURPOSE, 'Unknown') AS DIMENSION,
          COUNT(*) AS TOTAL_INVOICES,
          SUM(IS_DISPUTED) AS DISPUTED_COUNT,
          ROUND(SUM(IS_DISPUTED) * 100.0 / NULLIF(COUNT(*), 0), 2) AS DISPUTE_RATE_PCT,
          ROUND(SUM(CASE WHEN IS_DISPUTED = 1 THEN INVOICE_AMOUNT_LOCAL ELSE 0 END), 0) AS DISPUTED_AMOUNT
        FROM base
        GROUP BY PO_PURPOSE
        HAVING COUNT(*) >= 10
      ),
      by_po_status AS (
        SELECT
          'BY_PO_STATUS' AS ANALYSIS_TYPE,
          PO_STATUS AS DIMENSION,
          COUNT(*) AS TOTAL_INVOICES,
          SUM(IS_DISPUTED) AS DISPUTED_COUNT,
          ROUND(SUM(IS_DISPUTED) * 100.0 / NULLIF(COUNT(*), 0), 2) AS DISPUTE_RATE_PCT,
          ROUND(SUM(CASE WHEN IS_DISPUTED = 1 THEN INVOICE_AMOUNT_LOCAL ELSE 0 END), 0) AS DISPUTED_AMOUNT
        FROM base
        GROUP BY PO_STATUS
      ),
      by_amount AS (
        SELECT
          'BY_AMOUNT_BUCKET' AS ANALYSIS_TYPE,
          AMOUNT_BUCKET AS DIMENSION,
          COUNT(*) AS TOTAL_INVOICES,
          SUM(IS_DISPUTED) AS DISPUTED_COUNT,
          ROUND(SUM(IS_DISPUTED) * 100.0 / NULLIF(COUNT(*), 0), 2) AS DISPUTE_RATE_PCT,
          ROUND(SUM(CASE WHEN IS_DISPUTED = 1 THEN INVOICE_AMOUNT_LOCAL ELSE 0 END), 0) AS DISPUTED_AMOUNT
        FROM base
        GROUP BY AMOUNT_BUCKET
      ),
      by_vendor_top AS (
        SELECT
          'BY_VENDOR' AS ANALYSIS_TYPE,
          VENDOR_ID AS DIMENSION,
          COUNT(*) AS TOTAL_INVOICES,
          SUM(IS_DISPUTED) AS DISPUTED_COUNT,
          ROUND(SUM(IS_DISPUTED) * 100.0 / NULLIF(COUNT(*), 0), 2) AS DISPUTE_RATE_PCT,
          ROUND(SUM(CASE WHEN IS_DISPUTED = 1 THEN INVOICE_AMOUNT_LOCAL ELSE 0 END), 0) AS DISPUTED_AMOUNT
        FROM base
        GROUP BY VENDOR_ID
        HAVING COUNT(*) >= 20 AND SUM(IS_DISPUTED) > 0
        ORDER BY DISPUTE_RATE_PCT DESC
        LIMIT 15
      )
      SELECT * FROM by_category
      UNION ALL SELECT * FROM by_po_status
      UNION ALL SELECT * FROM by_amount
      UNION ALL SELECT * FROM by_vendor_top
      ORDER BY ANALYSIS_TYPE, DISPUTE_RATE_PCT DESC;

  - name: dispute_trend_forecast
    question: "Show dispute rate trend for forecasting"
    sql: |
      WITH monthly AS (
        SELECT
          DATE_TRUNC('month', POSTING_DATE) AS MONTH,
          COUNT(*) AS TOTAL_INVOICES,
          SUM(CASE WHEN INVOICE_STATUS = 'Disputed' THEN 1 ELSE 0 END) AS DISPUTED_COUNT,
          ROUND(SUM(CASE WHEN INVOICE_STATUS = 'Disputed' THEN 1 ELSE 0 END) * 100.0
                / NULLIF(COUNT(*), 0), 2) AS DISPUTE_RATE_PCT,
          SUM(CASE WHEN INVOICE_STATUS = 'Disputed' THEN INVOICE_AMOUNT_LOCAL ELSE 0 END) AS DISPUTED_AMOUNT
        FROM procure2pay.FACT_ALL_SOURCES_VW
        WHERE UPPER(INVOICE_STATUS) NOT IN ('CANCELLED', 'REJECTED')
          AND POSTING_DATE >= DATEADD('month', -24, CURRENT_DATE())
        GROUP BY 1
      )
      SELECT
        MONTH,
        TOTAL_INVOICES,
        DISPUTED_COUNT,
        DISPUTE_RATE_PCT,
        DISPUTED_AMOUNT,
        -- 3-month moving average for trend
        ROUND(AVG(DISPUTE_RATE_PCT) OVER (ORDER BY MONTH ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2)
          AS DISPUTE_RATE_3M_AVG,
        -- Month-over-month change
        DISPUTE_RATE_PCT - LAG(DISPUTE_RATE_PCT) OVER (ORDER BY MONTH) AS MOM_CHANGE
      FROM monthly
      ORDER BY MONTH;

  - name: dispute_resolution_time
    question: "How long do disputes take to resolve?"
    sql: |
      WITH dispute_lifecycle AS (
        SELECT
          h1.INVOICE_NUMBER,
          h1.EFFECTIVE_DATE AS DISPUTE_START_DATE,
          MIN(h2.EFFECTIVE_DATE) AS RESOLUTION_DATE,
          DATEDIFF('day', h1.EFFECTIVE_DATE, MIN(h2.EFFECTIVE_DATE)) AS DAYS_TO_RESOLVE,
          h1.PO_PURPOSE
        FROM procure2pay.INVOICE_STATUS_HISTORY_VW h1
        LEFT JOIN procure2pay.INVOICE_STATUS_HISTORY_VW h2
          ON h1.INVOICE_NUMBER = h2.INVOICE_NUMBER
          AND h2.STATUS IN ('Paid', 'Open', 'Due')
          AND h2.EFFECTIVE_DATE > h1.EFFECTIVE_DATE
        WHERE h1.STATUS = 'Disputed'
        GROUP BY h1.INVOICE_NUMBER, h1.EFFECTIVE_DATE, h1.PO_PURPOSE
      )
      SELECT
        COALESCE(PO_PURPOSE, 'Unknown') AS CATEGORY,
        COUNT(*) AS DISPUTE_COUNT,
        ROUND(AVG(DAYS_TO_RESOLVE), 0) AS AVG_DAYS_TO_RESOLVE,
        MIN(DAYS_TO_RESOLVE) AS MIN_DAYS,
        MAX(DAYS_TO_RESOLVE) AS MAX_DAYS,
        SUM(CASE WHEN RESOLUTION_DATE IS NULL THEN 1 ELSE 0 END) AS STILL_OPEN,
        SUM(CASE WHEN DAYS_TO_RESOLVE > 30 THEN 1 ELSE 0 END) AS OVER_30_DAYS
      FROM dispute_lifecycle
      GROUP BY PO_PURPOSE
      ORDER BY DISPUTE_COUNT DESC;

  # =============================================================================
  # LATE PAYMENT PROBABILITY QUERIES
  # =============================================================================

  - name: late_payment_risk_scoring
    question: "Which invoices are most likely to be paid late?"
    sql: |
      WITH payment_history AS (
        SELECT
          YEAR, MONTH,
          ON_TIME_PAYMENT_RATE_PCT,
          100 - ON_TIME_PAYMENT_RATE_PCT AS LATE_RATE_PCT
        FROM procure2pay.ON_TIME_PAYMENT_RATE_VW
        ORDER BY YEAR DESC, MONTH DESC
        LIMIT 6
      ),
      avg_late_rate AS (
        SELECT AVG(LATE_RATE_PCT) AS baseline_late_rate FROM payment_history
      ),
      cycle_time AS (
        SELECT AVG_PAYMENT_CYCLE_TIME_DAYS AS avg_cycle
        FROM procure2pay.PAYMENT_PROCESSING_CYCLE_TIME_VW
        ORDER BY YEAR DESC, MONTH DESC LIMIT 1
      ),
      vendor_late_history AS (
        SELECT
          f.VENDOR_ID,
          COUNT(*) AS total_paid,
          SUM(CASE WHEN f.AGING_DAYS > 0 AND f.INVOICE_STATUS = 'Paid' THEN 1 ELSE 0 END) AS late_count,
          ROUND(SUM(CASE WHEN f.AGING_DAYS > 0 AND f.INVOICE_STATUS = 'Paid' THEN 1 ELSE 0 END) * 100.0
                / NULLIF(COUNT(*), 0), 2) AS vendor_late_rate
        FROM procure2pay.FACT_ALL_SOURCES_VW f
        WHERE f.INVOICE_STATUS = 'Paid'
        GROUP BY f.VENDOR_ID
      ),
      open_invoices AS (
        SELECT
          f.DOCUMENT_NUMBER,
          f.VENDOR_ID,
          v.VENDOR_NAME,
          f.INVOICE_AMOUNT_LOCAL,
          f.POSTING_DATE,
          f.DUE_DATE,
          DATEDIFF('day', CURRENT_DATE(), f.DUE_DATE) AS DAYS_UNTIL_DUE,
          f.PO_PURPOSE,
          f.INVOICE_STATUS,
          (SELECT avg_cycle FROM cycle_time) AS AVG_CYCLE_DAYS,
          COALESCE(vlh.vendor_late_rate, (SELECT baseline_late_rate FROM avg_late_rate)) AS vendor_late_rate
        FROM procure2pay.FACT_ALL_SOURCES_VW f
        LEFT JOIN procure2pay.DIM_VENDOR_VW v ON f.VENDOR_ID = v.VENDOR_ID
        LEFT JOIN vendor_late_history vlh ON f.VENDOR_ID = vlh.VENDOR_ID
        WHERE f.INVOICE_STATUS IN ('Open', 'Due')
      )
      SELECT
        DOCUMENT_NUMBER,
        VENDOR_ID,
        VENDOR_NAME,
        INVOICE_AMOUNT_LOCAL,
        POSTING_DATE,
        DUE_DATE,
        DAYS_UNTIL_DUE,
        PO_PURPOSE,
        INVOICE_STATUS,
        -- Risk Score Calculation (0-100)
        ROUND(
          -- Factor 1: Days until due vs processing time (0-40 points)
          CASE
            WHEN DAYS_UNTIL_DUE < 0 THEN 40  -- Already overdue
            WHEN DAYS_UNTIL_DUE < AVG_CYCLE_DAYS THEN 35  -- Not enough time
            WHEN DAYS_UNTIL_DUE < AVG_CYCLE_DAYS + 3 THEN 25  -- Cutting it close
            WHEN DAYS_UNTIL_DUE < AVG_CYCLE_DAYS + 7 THEN 15  -- Some buffer
            ELSE 5
          END +
          -- Factor 2: Vendor late history (0-35 points)
          LEAST(vendor_late_rate * 0.35, 35) +
          -- Factor 3: Invoice amount (larger = more approval steps) (0-15 points)
          CASE
            WHEN INVOICE_AMOUNT_LOCAL > 100000 THEN 15
            WHEN INVOICE_AMOUNT_LOCAL > 50000 THEN 10
            WHEN INVOICE_AMOUNT_LOCAL > 10000 THEN 5
            ELSE 2
          END +
          -- Factor 4: Current status (0-10 points)
          CASE WHEN INVOICE_STATUS = 'Due' THEN 10 ELSE 0 END
        , 0) AS LATE_PAYMENT_RISK_SCORE,
       
        CASE
          WHEN DAYS_UNTIL_DUE < 0 THEN 'ALREADY_OVERDUE'
          WHEN DAYS_UNTIL_DUE < AVG_CYCLE_DAYS THEN 'HIGH_RISK'
          WHEN DAYS_UNTIL_DUE < AVG_CYCLE_DAYS + 5 THEN 'MEDIUM_RISK'
          ELSE 'LOW_RISK'
        END AS RISK_LEVEL,
       
        ROUND(vendor_late_rate, 1) AS VENDOR_LATE_RATE_PCT,
        ROUND(AVG_CYCLE_DAYS, 0) AS AVG_PROCESSING_DAYS,
       
        -- Action needed by date
        DATEADD('day', -ROUND(AVG_CYCLE_DAYS, 0), DUE_DATE) AS ACTION_NEEDED_BY,
        DATEDIFF('day', CURRENT_DATE(), DATEADD('day', -ROUND(AVG_CYCLE_DAYS, 0), DUE_DATE)) AS DAYS_TO_ACTION
       
      FROM open_invoices
      ORDER BY LATE_PAYMENT_RISK_SCORE DESC
      LIMIT 50;

  - name: late_payment_drivers
    question: "What factors are causing late payments?"
    sql: |
      WITH paid_invoices AS (
        SELECT
          f.VENDOR_ID,
          v.VENDOR_NAME,
          f.PO_PURPOSE,
          f.INVOICE_AMOUNT_LOCAL,
          f.COMPANY_CODE,
          CASE WHEN f.PURCHASE_ORDER_REFERENCE IS NULL THEN 'No PO' ELSE 'Has PO' END AS PO_STATUS,
          CASE
            WHEN f.INVOICE_AMOUNT_LOCAL < 1000 THEN 'Under $1K'
            WHEN f.INVOICE_AMOUNT_LOCAL < 10000 THEN '$1K-$10K'
            WHEN f.INVOICE_AMOUNT_LOCAL < 50000 THEN '$10K-$50K'
            ELSE 'Over $50K'
          END AS AMOUNT_BUCKET,
          CASE WHEN f.AGING_DAYS > 0 THEN 1 ELSE 0 END AS WAS_LATE,
          f.AGING_DAYS
        FROM procure2pay.FACT_ALL_SOURCES_VW f
        LEFT JOIN procure2pay.DIM_VENDOR_VW v ON f.VENDOR_ID = v.VENDOR_ID
        WHERE f.INVOICE_STATUS = 'Paid'
          AND f.POSTING_DATE >= DATEADD('month', -12, CURRENT_DATE())
      ),
      by_category AS (
        SELECT
          'BY_CATEGORY' AS DRIVER_TYPE,
          COALESCE(PO_PURPOSE, 'Unknown') AS DRIVER,
          COUNT(*) AS TOTAL_PAID,
          SUM(WAS_LATE) AS LATE_COUNT,
          ROUND(SUM(WAS_LATE) * 100.0 / NULLIF(COUNT(*), 0), 2) AS LATE_RATE_PCT,
          ROUND(AVG(CASE WHEN WAS_LATE = 1 THEN AGING_DAYS ELSE NULL END), 0) AS AVG_DAYS_LATE
        FROM paid_invoices
        GROUP BY PO_PURPOSE
        HAVING COUNT(*) >= 20
      ),
      by_po_status AS (
        SELECT
          'BY_PO_STATUS' AS DRIVER_TYPE,
          PO_STATUS AS DRIVER,
          COUNT(*) AS TOTAL_PAID,
          SUM(WAS_LATE) AS LATE_COUNT,
          ROUND(SUM(WAS_LATE) * 100.0 / NULLIF(COUNT(*), 0), 2) AS LATE_RATE_PCT,
          ROUND(AVG(CASE WHEN WAS_LATE = 1 THEN AGING_DAYS ELSE NULL END), 0) AS AVG_DAYS_LATE
        FROM paid_invoices
        GROUP BY PO_STATUS
      ),
      by_amount AS (
        SELECT
          'BY_AMOUNT' AS DRIVER_TYPE,
          AMOUNT_BUCKET AS DRIVER,
          COUNT(*) AS TOTAL_PAID,
          SUM(WAS_LATE) AS LATE_COUNT,
          ROUND(SUM(WAS_LATE) * 100.0 / NULLIF(COUNT(*), 0), 2) AS LATE_RATE_PCT,
          ROUND(AVG(CASE WHEN WAS_LATE = 1 THEN AGING_DAYS ELSE NULL END), 0) AS AVG_DAYS_LATE
        FROM paid_invoices
        GROUP BY AMOUNT_BUCKET
      ),
      by_vendor AS (
        SELECT
          'BY_VENDOR' AS DRIVER_TYPE,
          CONCAT(VENDOR_ID, ' - ', COALESCE(VENDOR_NAME, 'Unknown')) AS DRIVER,
          COUNT(*) AS TOTAL_PAID,
          SUM(WAS_LATE) AS LATE_COUNT,
          ROUND(SUM(WAS_LATE) * 100.0 / NULLIF(COUNT(*), 0), 2) AS LATE_RATE_PCT,
          ROUND(AVG(CASE WHEN WAS_LATE = 1 THEN AGING_DAYS ELSE NULL END), 0) AS AVG_DAYS_LATE
        FROM paid_invoices
        GROUP BY VENDOR_ID, VENDOR_NAME
        HAVING COUNT(*) >= 20
        ORDER BY LATE_RATE_PCT DESC
        LIMIT 10
      ),
      by_company AS (
        SELECT
          'BY_COMPANY_CODE' AS DRIVER_TYPE,
          CAST(COMPANY_CODE AS VARCHAR) AS DRIVER,
          COUNT(*) AS TOTAL_PAID,
          SUM(WAS_LATE) AS LATE_COUNT,
          ROUND(SUM(WAS_LATE) * 100.0 / NULLIF(COUNT(*), 0), 2) AS LATE_RATE_PCT,
          ROUND(AVG(CASE WHEN WAS_LATE = 1 THEN AGING_DAYS ELSE NULL END), 0) AS AVG_DAYS_LATE
        FROM paid_invoices
        GROUP BY COMPANY_CODE
        HAVING COUNT(*) >= 20
      )
      SELECT * FROM by_category
      UNION ALL SELECT * FROM by_po_status
      UNION ALL SELECT * FROM by_amount
      UNION ALL SELECT * FROM by_vendor
      UNION ALL SELECT * FROM by_company
      ORDER BY DRIVER_TYPE, LATE_RATE_PCT DESC;

  - name: late_payment_trend_forecast
    question: "Show late payment trend for forecasting"
    sql: |
      WITH monthly_metrics AS (
        SELECT
          l.YEAR,
          l.MONTH,
          l.LATE_PAYMENT_AMOUNT,
          l.LATE_PAYMENT_COUNT,
          l.LATE_PAYMENT_RATE_PCT,
          o.ON_TIME_PAYMENT_RATE_PCT,
          p.AVG_PAYMENT_CYCLE_TIME_DAYS
        FROM procure2pay.LATE_PAYMENT_AMOUNT_VW l
        LEFT JOIN procure2pay.ON_TIME_PAYMENT_RATE_VW o
          ON l.YEAR = o.YEAR AND l.MONTH = o.MONTH
        LEFT JOIN procure2pay.PAYMENT_PROCESSING_CYCLE_TIME_VW p
          ON l.YEAR = p.YEAR AND l.MONTH = p.MONTH
      )
      SELECT
        YEAR,
        MONTH,
        LATE_PAYMENT_AMOUNT,
        LATE_PAYMENT_COUNT,
        LATE_PAYMENT_RATE_PCT,
        ON_TIME_PAYMENT_RATE_PCT,
        AVG_PAYMENT_CYCLE_TIME_DAYS,
        -- 3-month moving averages
        ROUND(AVG(LATE_PAYMENT_RATE_PCT) OVER (ORDER BY YEAR, MONTH ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2)
          AS LATE_RATE_3M_AVG,
        ROUND(AVG(AVG_PAYMENT_CYCLE_TIME_DAYS) OVER (ORDER BY YEAR, MONTH ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 1)
          AS CYCLE_TIME_3M_AVG,
        -- Month-over-month change
        LATE_PAYMENT_RATE_PCT - LAG(LATE_PAYMENT_RATE_PCT) OVER (ORDER BY YEAR, MONTH) AS LATE_RATE_MOM_CHANGE,
        -- Trend indicator
        CASE
          WHEN LATE_PAYMENT_RATE_PCT > LAG(LATE_PAYMENT_RATE_PCT) OVER (ORDER BY YEAR, MONTH)
               AND LAG(LATE_PAYMENT_RATE_PCT) OVER (ORDER BY YEAR, MONTH) >
                   LAG(LATE_PAYMENT_RATE_PCT, 2) OVER (ORDER BY YEAR, MONTH)
          THEN 'WORSENING'
          WHEN LATE_PAYMENT_RATE_PCT < LAG(LATE_PAYMENT_RATE_PCT) OVER (ORDER BY YEAR, MONTH)
               AND LAG(LATE_PAYMENT_RATE_PCT) OVER (ORDER BY YEAR, MONTH) <
                   LAG(LATE_PAYMENT_RATE_PCT, 2) OVER (ORDER BY YEAR, MONTH)
          THEN 'IMPROVING'
          ELSE 'STABLE'
        END AS TREND_DIRECTION
      FROM monthly_metrics
      ORDER BY YEAR, MONTH;

  - name: at_risk_payment_summary
    question: "Summary of invoices at risk of late payment"
    sql: |
      WITH cycle_time AS (
        SELECT AVG_PAYMENT_CYCLE_TIME_DAYS AS avg_cycle
        FROM procure2pay.PAYMENT_PROCESSING_CYCLE_TIME_VW
        ORDER BY YEAR DESC, MONTH DESC LIMIT 1
      ),
      risk_buckets AS (
        SELECT
          f.DOCUMENT_NUMBER,
          f.INVOICE_AMOUNT_LOCAL,
          f.VENDOR_ID,
          DATEDIFF('day', CURRENT_DATE(), f.DUE_DATE) AS DAYS_UNTIL_DUE,
          (SELECT avg_cycle FROM cycle_time) AS AVG_CYCLE,
          CASE
            WHEN DATEDIFF('day', CURRENT_DATE(), f.DUE_DATE) < 0 THEN 'OVERDUE'
            WHEN DATEDIFF('day', CURRENT_DATE(), f.DUE_DATE) < (SELECT avg_cycle FROM cycle_time) THEN 'HIGH_RISK'
            WHEN DATEDIFF('day', CURRENT_DATE(), f.DUE_DATE) < (SELECT avg_cycle FROM cycle_time) + 5 THEN 'MEDIUM_RISK'
            ELSE 'ON_TRACK'
          END AS RISK_CATEGORY
        FROM procure2pay.FACT_ALL_SOURCES_VW f
        WHERE f.INVOICE_STATUS IN ('Open', 'Due')
      )
      SELECT
        RISK_CATEGORY,
        COUNT(*) AS INVOICE_COUNT,
        COUNT(DISTINCT VENDOR_ID) AS VENDOR_COUNT,
        ROUND(SUM(INVOICE_AMOUNT_LOCAL), 2) AS TOTAL_AMOUNT,
        ROUND(AVG(INVOICE_AMOUNT_LOCAL), 2) AS AVG_AMOUNT,
        ROUND(MIN(DAYS_UNTIL_DUE), 0) AS MIN_DAYS_TO_DUE,
        ROUND(MAX(DAYS_UNTIL_DUE), 0) AS MAX_DAYS_TO_DUE,
        CASE RISK_CATEGORY
          WHEN 'OVERDUE' THEN 'Immediate action required - contact vendor, escalate'
          WHEN 'HIGH_RISK' THEN 'Expedite approval - insufficient processing time'
          WHEN 'MEDIUM_RISK' THEN 'Monitor closely - prioritize in payment run'
          ELSE 'Standard processing'
        END AS RECOMMENDED_ACTION
      FROM risk_buckets
      GROUP BY RISK_CATEGORY
      ORDER BY
        CASE RISK_CATEGORY
          WHEN 'OVERDUE' THEN 1
          WHEN 'HIGH_RISK' THEN 2
          WHEN 'MEDIUM_RISK' THEN 3
          ELSE 4
        END;

  # =============================================================================
  # MONTHLY SPEND BY CATEGORY FORECASTING QUERIES
  # =============================================================================

  - name: monthly_spend_by_category
    question: "Show monthly spend by category for forecasting"
    sql: |
      WITH monthly_category AS (
        SELECT
          DATE_TRUNC('month', POSTING_DATE) AS MONTH,
          COALESCE(PO_PURPOSE, 'Unknown') AS CATEGORY,
          COUNT(*) AS INVOICE_COUNT,
          SUM(INVOICE_AMOUNT_LOCAL) AS SPEND,
          COUNT(DISTINCT VENDOR_ID) AS VENDOR_COUNT
        FROM procure2pay.FACT_ALL_SOURCES_VW
        WHERE UPPER(INVOICE_STATUS) NOT IN ('CANCELLED', 'REJECTED')
          AND POSTING_DATE >= DATEADD('month', -24, CURRENT_DATE())
        GROUP BY 1, 2
      )
      SELECT
        MONTH,
        CATEGORY,
        INVOICE_COUNT,
        SPEND,
        VENDOR_COUNT,
        -- Month-over-month change
        SPEND - LAG(SPEND) OVER (PARTITION BY CATEGORY ORDER BY MONTH) AS MOM_CHANGE,
        ROUND((SPEND - LAG(SPEND) OVER (PARTITION BY CATEGORY ORDER BY MONTH)) * 100.0
              / NULLIF(LAG(SPEND) OVER (PARTITION BY CATEGORY ORDER BY MONTH), 0), 1) AS MOM_CHANGE_PCT,
        -- Year-over-year change
        SPEND - LAG(SPEND, 12) OVER (PARTITION BY CATEGORY ORDER BY MONTH) AS YOY_CHANGE,
        ROUND((SPEND - LAG(SPEND, 12) OVER (PARTITION BY CATEGORY ORDER BY MONTH)) * 100.0
              / NULLIF(LAG(SPEND, 12) OVER (PARTITION BY CATEGORY ORDER BY MONTH), 0), 1) AS YOY_CHANGE_PCT,
        -- 3-month moving average
        ROUND(AVG(SPEND) OVER (PARTITION BY CATEGORY ORDER BY MONTH ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2)
          AS SPEND_3M_AVG
      FROM monthly_category
      ORDER BY MONTH DESC, SPEND DESC;

  - name: category_spend_summary
    question: "Summarize spend by category with trends"
    sql: |
      WITH current_month AS (
        SELECT
          COALESCE(PO_PURPOSE, 'Unknown') AS CATEGORY,
          SUM(INVOICE_AMOUNT_LOCAL) AS THIS_MONTH_SPEND,
          COUNT(*) AS THIS_MONTH_COUNT
        FROM procure2pay.FACT_ALL_SOURCES_VW
        WHERE UPPER(INVOICE_STATUS) NOT IN ('CANCELLED', 'REJECTED')
          AND DATE_TRUNC('month', POSTING_DATE) = DATE_TRUNC('month', CURRENT_DATE())
        GROUP BY PO_PURPOSE
      ),
      last_month AS (
        SELECT
          COALESCE(PO_PURPOSE, 'Unknown') AS CATEGORY,
          SUM(INVOICE_AMOUNT_LOCAL) AS LAST_MONTH_SPEND,
          COUNT(*) AS LAST_MONTH_COUNT
        FROM procure2pay.FACT_ALL_SOURCES_VW
        WHERE UPPER(INVOICE_STATUS) NOT IN ('CANCELLED', 'REJECTED')
          AND DATE_TRUNC('month', POSTING_DATE) = DATE_TRUNC('month', DATEADD('month', -1, CURRENT_DATE()))
        GROUP BY PO_PURPOSE
      ),
      ytd AS (
        SELECT
          COALESCE(PO_PURPOSE, 'Unknown') AS CATEGORY,
          SUM(INVOICE_AMOUNT_LOCAL) AS YTD_SPEND,
          COUNT(*) AS YTD_COUNT
        FROM procure2pay.FACT_ALL_SOURCES_VW
        WHERE UPPER(INVOICE_STATUS) NOT IN ('CANCELLED', 'REJECTED')
          AND DATE_TRUNC('year', POSTING_DATE) = DATE_TRUNC('year', CURRENT_DATE())
        GROUP BY PO_PURPOSE
      ),
      last_year_ytd AS (
        SELECT
          COALESCE(PO_PURPOSE, 'Unknown') AS CATEGORY,
          SUM(INVOICE_AMOUNT_LOCAL) AS LAST_YEAR_YTD_SPEND
        FROM procure2pay.FACT_ALL_SOURCES_VW
        WHERE UPPER(INVOICE_STATUS) NOT IN ('CANCELLED', 'REJECTED')
          AND POSTING_DATE >= DATE_TRUNC('year', DATEADD('year', -1, CURRENT_DATE()))
          AND POSTING_DATE < DATEADD('year', -1, CURRENT_DATE())
        GROUP BY PO_PURPOSE
      ),
      total_spend AS (
        SELECT SUM(YTD_SPEND) AS total FROM ytd
      )
      SELECT
        COALESCE(y.CATEGORY, cm.CATEGORY, lm.CATEGORY) AS CATEGORY,
        COALESCE(cm.THIS_MONTH_SPEND, 0) AS THIS_MONTH_SPEND,
        COALESCE(lm.LAST_MONTH_SPEND, 0) AS LAST_MONTH_SPEND,
        ROUND((COALESCE(cm.THIS_MONTH_SPEND, 0) - COALESCE(lm.LAST_MONTH_SPEND, 0)) * 100.0
              / NULLIF(lm.LAST_MONTH_SPEND, 0), 1) AS MOM_CHANGE_PCT,
        COALESCE(y.YTD_SPEND, 0) AS YTD_SPEND,
        COALESCE(ly.LAST_YEAR_YTD_SPEND, 0) AS LAST_YEAR_YTD_SPEND,
        ROUND((COALESCE(y.YTD_SPEND, 0) - COALESCE(ly.LAST_YEAR_YTD_SPEND, 0)) * 100.0
              / NULLIF(ly.LAST_YEAR_YTD_SPEND, 0), 1) AS YOY_CHANGE_PCT,
        ROUND(COALESCE(y.YTD_SPEND, 0) * 100.0 / NULLIF((SELECT total FROM total_spend), 0), 2) AS PCT_OF_TOTAL_SPEND,
        y.YTD_COUNT AS INVOICE_COUNT_YTD
      FROM ytd y
      FULL OUTER JOIN current_month cm ON y.CATEGORY = cm.CATEGORY
      FULL OUTER JOIN last_month lm ON y.CATEGORY = lm.CATEGORY
      LEFT JOIN last_year_ytd ly ON y.CATEGORY = ly.CATEGORY
      ORDER BY YTD_SPEND DESC;

  - name: category_seasonality_analysis
    question: "Analyze seasonality patterns in spend by category"
    sql: |
      WITH monthly_data AS (
        SELECT
          YEAR(POSTING_DATE) AS YEAR,
          MONTH(POSTING_DATE) AS MONTH_NUM,
          TO_CHAR(POSTING_DATE, 'Mon') AS MONTH_NAME,
          COALESCE(PO_PURPOSE, 'Unknown') AS CATEGORY,
          SUM(INVOICE_AMOUNT_LOCAL) AS SPEND
        FROM procure2pay.FACT_ALL_SOURCES_VW
        WHERE UPPER(INVOICE_STATUS) NOT IN ('CANCELLED', 'REJECTED')
          AND POSTING_DATE >= DATEADD('year', -3, CURRENT_DATE())
        GROUP BY 1, 2, 3, 4
      ),
      category_avg AS (
        SELECT CATEGORY, AVG(SPEND) AS avg_monthly_spend
        FROM monthly_data
        GROUP BY CATEGORY
      ),
      seasonality AS (
        SELECT
          m.MONTH_NUM,
          m.MONTH_NAME,
          m.CATEGORY,
          ROUND(AVG(m.SPEND), 2) AS AVG_SPEND_FOR_MONTH,
          ROUND(AVG(m.SPEND) / NULLIF(c.avg_monthly_spend, 0), 3) AS SEASONALITY_INDEX,
          COUNT(DISTINCT m.YEAR) AS YEARS_OF_DATA
        FROM monthly_data m
        JOIN category_avg c ON m.CATEGORY = c.CATEGORY
        GROUP BY m.MONTH_NUM, m.MONTH_NAME, m.CATEGORY, c.avg_monthly_spend
      )
      SELECT
        MONTH_NUM,
        MONTH_NAME,
        CATEGORY,
        AVG_SPEND_FOR_MONTH,
        SEASONALITY_INDEX,
        CASE
          WHEN SEASONALITY_INDEX >= 1.2 THEN 'HIGH_SEASON'
          WHEN SEASONALITY_INDEX <= 0.8 THEN 'LOW_SEASON'
          ELSE 'NORMAL'
        END AS SEASON_TYPE,
        YEARS_OF_DATA,
        ROUND((SEASONALITY_INDEX - 1) * 100, 1) AS PCT_VS_AVERAGE
      FROM seasonality
      ORDER BY CATEGORY, MONTH_NUM;

  - name: category_spend_forecast_inputs
    question: "Generate forecast inputs for spend by category"
    sql: |
      WITH monthly_history AS (
        SELECT
          DATE_TRUNC('month', POSTING_DATE) AS MONTH,
          COALESCE(PO_PURPOSE, 'Unknown') AS CATEGORY,
          SUM(INVOICE_AMOUNT_LOCAL) AS SPEND,
          COUNT(*) AS INVOICE_COUNT,
          COUNT(DISTINCT VENDOR_ID) AS ACTIVE_VENDORS
        FROM procure2pay.FACT_ALL_SOURCES_VW
        WHERE UPPER(INVOICE_STATUS) NOT IN ('CANCELLED', 'REJECTED')
          AND POSTING_DATE >= DATEADD('month', -24, CURRENT_DATE())
        GROUP BY 1, 2
      ),
      category_stats AS (
        SELECT
          CATEGORY,
          COUNT(*) AS DATA_POINTS,
          ROUND(AVG(SPEND), 2) AS AVG_MONTHLY_SPEND,
          ROUND(STDDEV(SPEND), 2) AS STDDEV_SPEND,
          ROUND(MIN(SPEND), 2) AS MIN_SPEND,
          ROUND(MAX(SPEND), 2) AS MAX_SPEND,
          ROUND(AVG(INVOICE_COUNT), 0) AS AVG_INVOICE_COUNT,
          ROUND(AVG(ACTIVE_VENDORS), 0) AS AVG_VENDOR_COUNT
        FROM monthly_history
        GROUP BY CATEGORY
      ),
      recent_trend AS (
        SELECT
          CATEGORY,
          -- Linear regression slope approximation using last 6 months
          ROUND((
            SUM((EXTRACT(EPOCH FROM MONTH) - AVG(EXTRACT(EPOCH FROM MONTH)) OVER (PARTITION BY CATEGORY))
                * (SPEND - AVG(SPEND) OVER (PARTITION BY CATEGORY)))
            / NULLIF(SUM(POWER(EXTRACT(EPOCH FROM MONTH) - AVG(EXTRACT(EPOCH FROM MONTH)) OVER (PARTITION BY CATEGORY), 2)), 0)
          ) * 30 * 24 * 3600, 2) AS MONTHLY_TREND_SLOPE  -- Convert to monthly rate
        FROM monthly_history
        WHERE MONTH >= DATEADD('month', -6, DATE_TRUNC('month', CURRENT_DATE()))
        GROUP BY CATEGORY
      ),
      growth_rate AS (
        SELECT
          h1.CATEGORY,
          ROUND(((h1.SPEND - h2.SPEND) / NULLIF(h2.SPEND, 0)) * 100, 2) AS RECENT_GROWTH_PCT
        FROM (
          SELECT CATEGORY, SUM(SPEND) AS SPEND
          FROM monthly_history
          WHERE MONTH >= DATEADD('month', -3, DATE_TRUNC('month', CURRENT_DATE()))
          GROUP BY CATEGORY
        ) h1
        JOIN (
          SELECT CATEGORY, SUM(SPEND) AS SPEND
          FROM monthly_history
          WHERE MONTH >= DATEADD('month', -6, DATE_TRUNC('month', CURRENT_DATE()))
            AND MONTH < DATEADD('month', -3, DATE_TRUNC('month', CURRENT_DATE()))
          GROUP BY CATEGORY
        ) h2 ON h1.CATEGORY = h2.CATEGORY
      )
      SELECT
        s.CATEGORY,
        s.DATA_POINTS,
        s.AVG_MONTHLY_SPEND,
        s.STDDEV_SPEND,
        ROUND(s.STDDEV_SPEND / NULLIF(s.AVG_MONTHLY_SPEND, 0) * 100, 1) AS COEFFICIENT_OF_VARIATION,
        s.MIN_SPEND,
        s.MAX_SPEND,
        s.AVG_INVOICE_COUNT,
        s.AVG_VENDOR_COUNT,
        COALESCE(g.RECENT_GROWTH_PCT, 0) AS RECENT_3M_GROWTH_PCT,
        -- Simple forecast for next month
        ROUND(s.AVG_MONTHLY_SPEND * (1 + COALESCE(g.RECENT_GROWTH_PCT, 0) / 100 / 3), 2) AS NEXT_MONTH_FORECAST,
        -- Confidence based on variability
        CASE
          WHEN s.STDDEV_SPEND / NULLIF(s.AVG_MONTHLY_SPEND, 0) < 0.15 THEN 'HIGH'
          WHEN s.STDDEV_SPEND / NULLIF(s.AVG_MONTHLY_SPEND, 0) < 0.30 THEN 'MEDIUM'
          ELSE 'LOW'
        END AS FORECAST_CONFIDENCE
      FROM category_stats s
      LEFT JOIN growth_rate g ON s.CATEGORY = g.CATEGORY
      ORDER BY s.AVG_MONTHLY_SPEND DESC;

  - name: category_vendor_concentration
    question: "Analyze vendor concentration within each spend category"
    sql: |
      WITH category_vendor AS (
        SELECT
          COALESCE(f.PO_PURPOSE, 'Unknown') AS CATEGORY,
          f.VENDOR_ID,
          v.VENDOR_NAME,
          SUM(f.INVOICE_AMOUNT_LOCAL) AS VENDOR_SPEND,
          COUNT(*) AS INVOICE_COUNT
        FROM procure2pay.FACT_ALL_SOURCES_VW f
        LEFT JOIN procure2pay.DIM_VENDOR_VW v ON f.VENDOR_ID = v.VENDOR_ID
        WHERE UPPER(f.INVOICE_STATUS) NOT IN ('CANCELLED', 'REJECTED')
          AND f.POSTING_DATE >= DATEADD('year', -1, CURRENT_DATE())
        GROUP BY 1, 2, 3
      ),
      category_total AS (
        SELECT CATEGORY, SUM(VENDOR_SPEND) AS TOTAL_SPEND, COUNT(DISTINCT VENDOR_ID) AS VENDOR_COUNT
        FROM category_vendor
        GROUP BY CATEGORY
      ),
      ranked AS (
        SELECT
          cv.CATEGORY,
          cv.VENDOR_ID,
          cv.VENDOR_NAME,
          cv.VENDOR_SPEND,
          ct.TOTAL_SPEND,
          ct.VENDOR_COUNT AS TOTAL_VENDORS_IN_CATEGORY,
          ROUND(cv.VENDOR_SPEND * 100.0 / NULLIF(ct.TOTAL_SPEND, 0), 2) AS PCT_OF_CATEGORY,
          ROW_NUMBER() OVER (PARTITION BY cv.CATEGORY ORDER BY cv.VENDOR_SPEND DESC) AS VENDOR_RANK
        FROM category_vendor cv
        JOIN category_total ct ON cv.CATEGORY = ct.CATEGORY
      ),
      concentration AS (
        SELECT
          CATEGORY,
          SUM(CASE WHEN VENDOR_RANK <= 1 THEN PCT_OF_CATEGORY ELSE 0 END) AS TOP_1_CONCENTRATION,
          SUM(CASE WHEN VENDOR_RANK <= 3 THEN PCT_OF_CATEGORY ELSE 0 END) AS TOP_3_CONCENTRATION,
          SUM(CASE WHEN VENDOR_RANK <= 5 THEN PCT_OF_CATEGORY ELSE 0 END) AS TOP_5_CONCENTRATION,
          MAX(TOTAL_VENDORS_IN_CATEGORY) AS VENDOR_COUNT,
          MAX(TOTAL_SPEND) AS CATEGORY_SPEND
        FROM ranked
        GROUP BY CATEGORY
      )
      SELECT
        CATEGORY,
        CATEGORY_SPEND,
        VENDOR_COUNT,
        ROUND(TOP_1_CONCENTRATION, 1) AS TOP_1_VENDOR_PCT,
        ROUND(TOP_3_CONCENTRATION, 1) AS TOP_3_VENDORS_PCT,
        ROUND(TOP_5_CONCENTRATION, 1) AS TOP_5_VENDORS_PCT,
        CASE
          WHEN TOP_1_CONCENTRATION > 50 THEN 'CRITICAL - Single vendor dependency'
          WHEN TOP_3_CONCENTRATION > 80 THEN 'HIGH - Limited supplier base'
          WHEN TOP_5_CONCENTRATION > 90 THEN 'MEDIUM - Moderate concentration'
          ELSE 'LOW - Diversified'
        END AS CONCENTRATION_RISK,
        CASE
          WHEN TOP_1_CONCENTRATION > 50 THEN 'Identify alternative suppliers; negotiate backup contracts'
          WHEN TOP_3_CONCENTRATION > 80 THEN 'Develop secondary suppliers; split future orders'
          ELSE 'Monitor; maintain competitive bidding'
        END AS RECOMMENDED_ACTION
      FROM concentration
      ORDER BY CATEGORY_SPEND DESC;

  - name: early_payment_candidates
    question: "Which invoices should we pay early to capture discounts?"
    sql: "SELECT * FROM procure2pay.EARLY_PAYMENT_CANDIDATES_VW"

  - name: payment_timing_recommendation
    question: "What is the optimal payment timing strategy for this week?"
    sql: "SELECT * FROM procure2pay.PAYMENT_TIMING_RECOMMENDATION_VW"

  - name: first_pass_pos
    question: "Show me first pass PO's - purchase orders where all invoices were paid without disputes or overdue"
    use_as_onboarding_question: true
    sql: |
      WITH inv_flags AS (
        SELECT
          PURCHASE_ORDER_REFERENCE AS PO_NUMBER,
          INVOICE_NUMBER,
          COMPANY_CODE,
          FISCAL_YEAR,
          MAX(CASE WHEN UPPER(STATUS) IN ('DISPUTED','OVERDUE') THEN 1 ELSE 0 END) AS HAS_ISSUE,
          MAX(CASE WHEN UPPER(STATUS) IN ('PAID','CLEARED') THEN 1 ELSE 0 END) AS IS_PAID
        FROM procure2pay.INVOICE_STATUS_HISTORY_VW
        WHERE PURCHASE_ORDER_REFERENCE IS NOT NULL
        GROUP BY
          PURCHASE_ORDER_REFERENCE,
          INVOICE_NUMBER,
          COMPANY_CODE,
          FISCAL_YEAR
      ),
      po_agg AS (
        SELECT
          PO_NUMBER,
          COUNT(DISTINCT INVOICE_NUMBER) AS INVOICE_COUNT,
          SUM(IS_PAID)                  AS PAID_INVOICE_COUNT,
          SUM(HAS_ISSUE)                AS ISSUE_INVOICE_COUNT
        FROM inv_flags
        GROUP BY PO_NUMBER
      )
      SELECT
        PO_NUMBER,
        INVOICE_COUNT,
        PAID_INVOICE_COUNT,
        ISSUE_INVOICE_COUNT
      FROM po_agg
      WHERE ISSUE_INVOICE_COUNT = 0
        AND PAID_INVOICE_COUNT = INVOICE_COUNT
        AND INVOICE_COUNT > 0;
"""

# Since the YAML is now fully adapted, we can directly use it.
FULL_SEMANTIC_MODEL_YAML = RAW_SEMANTIC_MODEL_YAML

SYSTEM_PROMPT = f"""
You are an AI assistant that helps users query a procurement database using SQL (Athena/Presto). Given a user's natural language question, generate a valid SQL query for Athena (Presto dialect) based on the following semantic model.

Semantic Model (YAML):
{FULL_SEMANTIC_MODEL_YAML}

Important notes:
- Use standard Presto/Athena SQL functions (DATE_TRUNC, DATE_ADD, DATE_DIFF, etc.).
- For date filtering, prefer `posting_date BETWEEN DATE '...' AND DATE '...'`.
- Always use COALESCE for null amounts.
- Exclude CANCELLED and REJECTED invoices from spend metrics unless asked.
- Output only a JSON object with two keys: "sql" containing the SQL query string, and "explanation". Do not include any other text.
"""

DESCRIPTIVE_PROMPT_TEMPLATE = """
You are a senior procurement analyst. Based on the user's question and the data returned from the SQL query, write a response with two sections:

1. **Descriptive** – What the data shows. Cite exact numbers, identify trends, and highlight anomalies. Keep it concise (3-5 sentences).
2. **Prescriptive** – Specific recommended actions and risks based on the data. List 3-5 bullet points. Each bullet must include a specific finding and a concrete action. Avoid generic advice.

User question: {question}

SQL query:
{sql}

Data (first 10 rows):
{data_preview}

Respond in plain text, using markdown for headings and bullet points. Do not include any extra commentary.
"""

@lru_cache(maxsize=100)
def ask_bedrock_cached(prompt: str, system_prompt: str = SYSTEM_PROMPT) -> str:
    try:
        body = json.dumps({
            "messages": [{"role": "user", "content": [{"text": prompt}]}],
            "system": [{"text": system_prompt}],
            "inferenceConfig": {"maxTokens": 4096, "temperature": 0.0, "topP": 0.9}
        })
        bedrock = get_bedrock_runtime()
        response = bedrock.invoke_model(
            modelId=BEDROCK_MODEL_ID,
            contentType="application/json",
            accept="application/json",
            body=body
        )
        response_body = json.loads(response['body'].read())
        return response_body['output']['message']['content'][0]['text']
    except Exception as e:
        st.error(f"Bedrock invocation failed: {e}")
        return ""

def generate_sql(question: str) -> tuple:
    prompt = f"User question: {question}\n\nGenerate SQL query and explanation as JSON."
    response = ask_bedrock_cached(prompt)
    if not response:
        return None, "Bedrock returned empty response."
    json_match = re.search(r'\{.*\}$', response, re.DOTALL)
    json_str = json_match.group(0) if json_match else response
    try:
        data = json.loads(json_str)
        sql = data.get("sql", "").strip()
        explanation = data.get("explanation", "")
        return sql, explanation
    except json.JSONDecodeError:
        return None, "Could not parse SQL from AI response."

def is_safe_sql(sql: str) -> bool:
    sql_lower = sql.lower().strip()
    if not sql_lower.startswith("select"):
        return False
    dangerous = ["insert", "update", "delete", "drop", "alter", "create", "truncate", "grant", "revoke"]
    for word in dangerous:
        if re.search(r'\b' + word + r'\b', sql_lower):
            return False
    return True

def ensure_limit(sql: str, default_limit: int = 100) -> str:
    sql_lower = sql.lower()
    if "limit" in sql_lower:
        return sql
    if re.search(r'\b(count|sum|avg|min|max)\b', sql_lower) and "group by" not in sql_lower:
        return sql
    return f"{sql.rstrip(';')} LIMIT {default_limit}"

def auto_chart(df: pd.DataFrame) -> Union[alt.Chart, None]:
    if df.empty or len(df) > 200:
        return None
    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
    if not numeric_cols:
        return None
    dim_candidates = [c for c in df.columns if c not in numeric_cols]
    if dim_candidates:
        dim = dim_candidates[0]
        if len(numeric_cols) == 1:
            chart = alt.Chart(df).mark_bar().encode(
                x=alt.X(dim, sort=None),
                y=alt.Y(numeric_cols[0]),
                tooltip=[dim, numeric_cols[0]]
            )
        else:
            melted = df.melt(id_vars=[dim], value_vars=numeric_cols)
            chart = alt.Chart(melted).mark_line(point=True).encode(
                x=alt.X(dim, sort=None),
                y=alt.Y('value', title='Value'),
                color='variable',
                tooltip=[dim, 'variable', 'value']
            )
        return chart.interactive()
    return None

def _pick_chart_columns(df: pd.DataFrame) -> tuple:
    if df.empty or len(df.columns) < 2:
        return (None, None)
    cols = list(df.columns)
    cat_prefer = ("vendor_name", "month", "status", "aging_bucket", "po_purpose")
    num_prefer = ("spend", "total_spend", "invoice_count", "amount")
    cols_lower = {c.lower(): c for c in cols}
    x_col = next((cols_lower[p] for p in cat_prefer if p in cols_lower), cols[0])
    y_col = next((cols_lower[p] for p in num_prefer if p in cols_lower and cols_lower[p] != x_col), None)
    if not y_col:
        for c in cols:
            if c != x_col and pd.api.types.is_numeric_dtype(df[c]):
                y_col = c
                break
    if not y_col and len(cols) > 1:
        y_col = cols[1]
    return (x_col, y_col)

def alt_bar(df, x, y, title=None, horizontal=False, color="#1459d2", height=320):
    if df.empty:
        st.info("No data for this chart.")
        return
    if horizontal:
        chart = alt.Chart(df).mark_bar(color=color, cornerRadiusTopLeft=4).encode(
            x=alt.X(y, type='quantitative', axis=alt.Axis(title=None, format="~s")),
            y=alt.Y(x, type='nominal', sort='-x', axis=alt.Axis(title=None)),
            tooltip=[x, alt.Tooltip(y, format=",.0f")]
        )
    else:
        chart = alt.Chart(df).mark_bar(color=color, cornerRadiusTopLeft=4).encode(
            x=alt.X(x, type='nominal', axis=alt.Axis(title=None)),
            y=alt.Y(y, type='quantitative', axis=alt.Axis(title=None, format="~s")),
            tooltip=[x, alt.Tooltip(y, format=",.0f")]
        )
    chart = chart.properties(height=height)
    if title:
        chart = chart.properties(title=title)
    st.altair_chart(chart, use_container_width=True)

def alt_line_monthly(df, month_col='month', value_col='value', height=140, title=None):
    if df.empty:
        st.info("No data for this chart.")
        return
    data = df.copy()
    try:
        data['_month_dt'] = pd.to_datetime(data[month_col].astype(str) + '-01')
        data = data.sort_values('_month_dt')
        data['month_label'] = data['_month_dt'].dt.strftime('%b %Y')
    except:
        data['month_label'] = data[month_col].astype(str)
    chart = alt.Chart(data).mark_line(point=True, color='#1e88e5').encode(
        x=alt.X('month_label:N', sort=None, axis=alt.Axis(title=None, labelAngle=-45)),
        y=alt.Y(f'{value_col}:Q', axis=alt.Axis(title=None, grid=False, format='~s')),
        tooltip=[alt.Tooltip('month_label:N', title='Month'), alt.Tooltip(f'{value_col}:Q', format=',.0f')]
    ).properties(height=height)
    if title:
        chart = chart.properties(title=title)
    st.altair_chart(chart, use_container_width=True)

def alt_donut_status(df, label_col="status", value_col="cnt", title=None, height=340):
    if df.empty or df[value_col].sum() == 0:
        st.info("No data for donut chart.")
        return
    total = df[value_col].sum()
    df['pct'] = df[value_col] / total
    order = ["Paid", "Pending", "Disputed", "Other"]
    palette = {"Paid": "#22C55E", "Pending": "#FBBF24", "Disputed": "#EF4444", "Other": "#1E88E5"}
    for cat in order:
        if cat not in df[label_col].values:
            df = pd.concat([df, pd.DataFrame({label_col: [cat], value_col: [0], 'pct': [0.0]})], ignore_index=True)
    base = alt.Chart(df).encode(
        theta=alt.Theta(field=value_col, type='quantitative', stack=True),
        color=alt.Color(field=label_col, type='nominal', scale=alt.Scale(domain=order, range=[palette[k] for k in order])),
        tooltip=[label_col, value_col, alt.Tooltip('pct:Q', format='.1%')]
    )
    arc = base.mark_arc(innerRadius=40, outerRadius=100)
    text = base.transform_filter(alt.datum.pct >= 0.01).mark_text(radius=115, color='#0f172a', fontSize=12, fontWeight='bold').encode(text=alt.Text('pct:Q', format='.1%'))
    chart = (arc + text).properties(height=height)
    if title:
        chart = chart.properties(title=title)
    st.altair_chart(chart, use_container_width=True)

# ---------------------------- Quick Analysis Functions (Athena) ----------------------------
@st.cache_data(ttl=600)
def run_quick_analysis_cached(key: str) -> dict:
    base = f"{DATABASE}.fact_all_sources_vw f LEFT JOIN {DATABASE}.dim_vendor_vw v ON f.vendor_id = v.vendor_id"
    flt = "AND UPPER(f.invoice_status) NOT IN ('CANCELLED','REJECTED')"
    out = {"layout": "quick", "type": key, "metrics": {}, "monthly_df": None, "vendors_df": None, "extra_dfs": {}, "sql": {}, "anomaly": None}
    today = date.today()
    ytd_start = date(today.year, 1, 1)
    start_lit = sql_date(ytd_start)
    end_lit = sql_date(today)

    if key == "spending_overview":
        total_sql = f"""
            SELECT SUM(COALESCE(f.invoice_amount_local,0)) AS total_spend
            FROM {base}
            WHERE f.posting_date BETWEEN {start_lit} AND {end_lit} {flt}
        """
        total_df = run_query(total_sql)
        total_spend = safe_number(total_df.loc[0,"total_spend"]) if not total_df.empty else 0

        mom_sql = f"""
            WITH monthly AS (
                SELECT DATE_TRUNC('month', f.posting_date) AS month,
                       SUM(COALESCE(f.invoice_amount_local,0)) AS spend
                FROM {base}
                WHERE f.posting_date BETWEEN {start_lit} AND {end_lit} {flt}
                GROUP BY 1
            )
            SELECT spend FROM monthly ORDER BY month DESC LIMIT 1
        """
        cur_m = safe_number(run_query(mom_sql).loc[0,"spend"]) if not run_query(mom_sql).empty else 0
        prev_m_sql = f"""
            WITH monthly AS (
                SELECT DATE_TRUNC('month', f.posting_date) AS month,
                       SUM(COALESCE(f.invoice_amount_local,0)) AS spend
                FROM {base}
                WHERE f.posting_date BETWEEN DATE_ADD('month', -1, {start_lit}) AND DATE_ADD('month', -1, {end_lit}) {flt}
                GROUP BY 1
            )
            SELECT spend FROM monthly ORDER BY month DESC LIMIT 1
        """
        prev_m = safe_number(run_query(prev_m_sql).loc[0,"spend"]) if not run_query(prev_m_sql).empty else 0
        mom_pct = ((cur_m - prev_m)/prev_m*100) if prev_m else 0

        current_quarter_start = date(today.year, ((today.month-1)//3)*3 + 1, 1)
        prev_quarter_start = date(today.year if current_quarter_start.month > 1 else today.year-1,
                                  ((current_quarter_start.month-1)//3)*3 + 1 if current_quarter_start.month > 1 else 10, 1)
        prev_quarter_end = current_quarter_start - timedelta(days=1)
        cur_q_sql = f"""
            SELECT SUM(COALESCE(f.invoice_amount_local,0)) AS spend
            FROM {base}
            WHERE f.posting_date BETWEEN '{sql_date(current_quarter_start)}' AND '{sql_date(today)}' {flt}
        """
        prev_q_sql = f"""
            SELECT SUM(COALESCE(f.invoice_amount_local,0)) AS spend
            FROM {base}
            WHERE f.posting_date BETWEEN '{sql_date(prev_quarter_start)}' AND '{sql_date(prev_quarter_end)}' {flt}
        """
        cur_q = safe_number(run_query(cur_q_sql).loc[0,"spend"]) if not run_query(cur_q_sql).empty else 0
        prev_q = safe_number(run_query(prev_q_sql).loc[0,"spend"]) if not run_query(prev_q_sql).empty else 0
        qoq_pct = ((cur_q - prev_q)/prev_q*100) if prev_q else 0

        top5_sql = f"""
            SELECT v.vendor_name, SUM(COALESCE(f.invoice_amount_local,0)) AS spend
            FROM {base}
            WHERE f.posting_date BETWEEN {start_lit} AND {end_lit} {flt}
            GROUP BY 1 ORDER BY spend DESC LIMIT 5
        """
        top5 = run_query(top5_sql)
        top5_sum = safe_number(top5["spend"].sum()) if not top5.empty else 0
        top5_pct = (top5_sum / total_spend * 100) if total_spend else 0

        out["metrics"] = {"total_ytd": total_spend, "mom_pct": mom_pct, "qoq_pct": qoq_pct, "top5_pct": top5_pct}

        monthly_sql = f"""
            SELECT DATE_FORMAT(f.posting_date, '%Y-%m') AS MONTH,
                   SUM(COALESCE(f.invoice_amount_local,0)) AS MONTHLY_SPEND,
                   COUNT(DISTINCT f.invoice_number) AS INVOICE_COUNT,
                   COUNT(DISTINCT f.vendor_id) AS VENDOR_COUNT
            FROM {base}
            WHERE f.posting_date >= DATE_ADD('month', -12, {end_lit}) {flt}
            GROUP BY 1 ORDER BY 1
        """
        monthly_df = run_query(monthly_sql)
        out["monthly_df"] = monthly_df
        out["extra_dfs"]["monthly_full"] = monthly_df

        anomaly = None
        if monthly_df is not None and not monthly_df.empty and "MONTHLY_SPEND" in monthly_df.columns:
            monthly_df = monthly_df.sort_values("MONTH")
            monthly_df["prev_spend"] = monthly_df["MONTHLY_SPEND"].shift(1)
            monthly_df["pct_change"] = (monthly_df["MONTHLY_SPEND"] - monthly_df["prev_spend"]) / monthly_df["prev_spend"] * 100
            spikes = monthly_df[monthly_df["pct_change"] > 20].copy()
            if not spikes.empty:
                max_spike = spikes.loc[spikes["pct_change"].idxmax()]
                spike_month = max_spike["MONTH"]
                spike_pct = max_spike["pct_change"]
                top_vendor_sql = f"""
                    SELECT v.vendor_name, SUM(COALESCE(f.invoice_amount_local,0)) AS spend
                    FROM {base}
                    WHERE DATE_FORMAT(f.posting_date, '%Y-%m') = '{spike_month}' {flt}
                    GROUP BY 1 ORDER BY 2 DESC LIMIT 1
                """
                top_vendor_df = run_query(top_vendor_sql)
                vendor = top_vendor_df.at[0, "vendor_name"] if not top_vendor_df.empty else "a top vendor"
                vendor_amt = safe_number(top_vendor_df.at[0, "spend"]) if not top_vendor_df.empty else 0
                anomaly = f"{spike_month} spending spiked by {spike_pct:.0f}% vs prior month, primarily driven by {vendor} ({abbr_currency(vendor_amt)})."
        out["anomaly"] = anomaly

        vendors_sql = f"""
            SELECT v.vendor_name, SUM(COALESCE(f.invoice_amount_local,0)) AS SPEND
            FROM {base}
            WHERE f.posting_date BETWEEN {start_lit} AND {end_lit} {flt}
            GROUP BY 1 ORDER BY SPEND DESC LIMIT 20
        """
        out["vendors_df"] = run_query(vendors_sql)
        out["sql"]["monthly_trend"] = monthly_sql
        out["sql"]["top_vendors"] = vendors_sql

    elif key == "vendor_analysis":
        vendors_sql = f"""
            SELECT v.vendor_name, SUM(COALESCE(f.invoice_amount_local,0)) AS SPEND, COUNT(*) AS INVOICE_COUNT
            FROM {base}
            WHERE f.posting_date >= DATE_ADD('month', -6, CURRENT_DATE) {flt}
            GROUP BY 1 ORDER BY SPEND DESC
        """
        out["vendors_df"] = run_query(vendors_sql)
        out["metrics"] = {"summary": "Top vendors by spend last 6 months."}
        out["sql"]["vendor_analysis"] = vendors_sql

    elif key == "payment_performance":
        pm_sql = f"""
            SELECT DATE_FORMAT(f.payment_date, '%Y-%m') AS MONTH,
                   ROUND(AVG(DATE_DIFF('day', f.posting_date, f.payment_date)),1) AS AVG_DAYS,
                   SUM(CASE WHEN DATE_DIFF('day', f.due_date, f.payment_date) > 0 THEN 1 ELSE 0 END) AS LATE_PAYMENTS,
                   COUNT(*) AS TOTAL_PAYMENTS
            FROM {base}
            WHERE f.payment_date IS NOT NULL AND f.payment_date >= DATE_ADD('month', -6, CURRENT_DATE) {flt}
            GROUP BY 1 ORDER BY 1
        """
        out["monthly_df"] = run_query(pm_sql)
        out["metrics"] = {"summary": "Avg days-to-pay and late payments."}
        out["sql"]["payment_performance"] = pm_sql

    elif key == "invoice_aging":
        aging_sql = f"""
            SELECT CASE WHEN f.aging_days <= 30 THEN '0-30 days'
                        WHEN f.aging_days <= 60 THEN '31-60 days'
                        WHEN f.aging_days <= 90 THEN '61-90 days'
                        ELSE '90+ days' END AS AGING_BUCKET,
                   COUNT(*) AS CNT, SUM(COALESCE(f.invoice_amount_local,0)) AS SPEND
            FROM {base}
            WHERE UPPER(f.invoice_status) IN ('OPEN','PENDING') AND f.aging_days IS NOT NULL {flt}
            GROUP BY 1 ORDER BY 1
        """
        out["vendors_df"] = run_query(aging_sql)
        out["metrics"] = {"summary": "Aging buckets for open invoices."}
        out["sql"]["invoice_aging"] = aging_sql

    return out

run_quick_analysis = run_quick_analysis_cached

# ---------------------------- Persistence (SQLite) ----------------------------
DB_PATH = "procureiq.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS chat_sessions (
        session_id TEXT PRIMARY KEY, session_label TEXT, created_at TIMESTAMP, last_updated TIMESTAMP
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS chat_messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT, session_id TEXT, turn_index INTEGER, role TEXT, content TEXT,
        sql_used TEXT, source TEXT, timestamp TIMESTAMP, FOREIGN KEY(session_id) REFERENCES chat_sessions(session_id)
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS question_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT, normalized_query TEXT, query_text TEXT, user_name TEXT,
        analysis_type TEXT, asked_at TIMESTAMP
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS saved_insights (
        insight_id TEXT PRIMARY KEY, created_by TEXT, page TEXT, title TEXT, question TEXT,
        verified_query_name TEXT, created_at TIMESTAMP
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS query_cache (
        query_hash TEXT PRIMARY KEY, question TEXT, response_json TEXT, created_at TIMESTAMP,
        last_hit_at TIMESTAMP, hit_count INTEGER
    )''')
    conn.commit()
    conn.close()

init_db()

def get_current_user():
    return "user1"

@st.cache_data(ttl=300)
def get_saved_insights_cached(page="genie", limit=20):
    user = get_current_user()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''SELECT insight_id, title, question, verified_query_name, created_at FROM saved_insights
                 WHERE page = ? AND created_by = ? ORDER BY created_at DESC LIMIT ?''', (page, user, limit))
    rows = c.fetchall()
    conn.close()
    return [{"id": row[0], "title": row[1], "question": row[2], "type": row[3], "created_at": row[4]} for row in rows]

@st.cache_data(ttl=300)
def get_frequent_questions_by_user_cached(limit=10):
    user = get_current_user()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''SELECT normalized_query, COUNT(*) as cnt FROM question_history
                 WHERE user_name = ? GROUP BY normalized_query ORDER BY cnt DESC LIMIT ?''', (user, limit))
    rows = c.fetchall()
    conn.close()
    return [{"query": row[0], "count": row[1]} for row in rows]

@st.cache_data(ttl=300)
def get_frequent_questions_all_cached(limit=10):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''SELECT normalized_query, COUNT(*) as cnt FROM question_history
                 GROUP BY normalized_query ORDER BY cnt DESC LIMIT ?''', (limit,))
    rows = c.fetchall()
    conn.close()
    return [{"query": row[0], "count": row[1]} for row in rows]

def save_chat_message(session_id, turn_index, role, content, sql_used="", source=""):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''INSERT INTO chat_messages (session_id, turn_index, role, content, sql_used, source, timestamp)
                 VALUES (?, ?, ?, ?, ?, ?, ?)''',
              (session_id, turn_index, role, content, sql_used, source, datetime.now()))
    conn.commit()
    conn.close()

def save_question(query, analysis_type):
    norm = query.lower().strip()
    user = get_current_user()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('INSERT INTO question_history (normalized_query, query_text, user_name, analysis_type, asked_at) VALUES (?, ?, ?, ?, ?)',
              (norm, query, user, analysis_type, datetime.now()))
    conn.commit()
    conn.close()
    get_frequent_questions_by_user_cached.clear()
    get_frequent_questions_all_cached.clear()

def save_insight(question, title, analysis_type="custom", page="genie"):
    insight_id = str(uuid.uuid4())
    user = get_current_user()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''INSERT INTO saved_insights (insight_id, created_by, page, title, question, verified_query_name, created_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?)''',
              (insight_id, user, page, title, question, analysis_type, datetime.now()))
    conn.commit()
    conn.close()
    get_saved_insights_cached.clear()

def get_cache(question):
    q_hash = hashlib.md5(question.lower().strip().encode()).hexdigest()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT response_json FROM query_cache WHERE query_hash = ?', (q_hash,))
    row = c.fetchone()
    conn.close()
    if row:
        return json.loads(row[0])
    return None

def set_cache(question, response):
    q_hash = hashlib.md5(question.lower().strip().encode()).hexdigest()
    serializable_response = make_json_serializable(response)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''INSERT OR REPLACE INTO query_cache (query_hash, question, response_json, created_at, last_hit_at, hit_count)
                 VALUES (?, ?, ?, ?, ?, COALESCE((SELECT hit_count+1 FROM query_cache WHERE query_hash=?), 1))''',
              (q_hash, question, json.dumps(serializable_response), datetime.now(), datetime.now(), q_hash))
    conn.commit()
    conn.close()

# ---------------------------- DASHBOARD PAGE (UPDATED) ----------------------------
def render_header():
    """Render top bar: App Title, Navigation Tabs, Logo (top-right)"""
    logo_url = "https://th.bing.com/th/id/OIP.Vy1yFQtg8-D1SsAxcqqtSgHaE6?w=235&h=180&c=7&r=0&o=7&dpr=1.5&pid=1.7&rm=3"
    
    # Three columns: left (title), center (tabs), right (logo + filters)
    left, center, right = st.columns([1, 2, 1])
    
    with left:
        st.markdown("<h1 style='font-weight: bold; margin-bottom: 0;'>ProcureIQ</h1>", unsafe_allow_html=True)
        st.markdown("<p style='font-size: 0.8rem; color: gray; margin-top: -0.2rem;'>P2P Analytics</p>", unsafe_allow_html=True)
    
    with center:
        nav_cols = st.columns(4)
        current_page = st.session_state.get("page", "Dashboard")
        pages = ["Dashboard", "Genie", "Forecast", "Invoices"]
        for i, page in enumerate(pages):
            with nav_cols[i]:
                if st.button(page, use_container_width=True, type="primary" if current_page == page else "secondary"):
                    st.session_state.page = page
                    st.rerun()
    
    with right:
        # Logo at top-right
        st.image(logo_url, width=70)  # increased from 50 to 70 (+40%)
        st.markdown("<div style='margin-bottom: 0.5rem;'></div>", unsafe_allow_html=True)
        # Filters will be placed below the logo in the same column
        return right  # return column reference so filters can be added inside it

def render_filters(parent_col):
    """Render date range, vendor dropdown, and preset buttons inside the parent column (below logo)"""
    if "preset" not in st.session_state:
        st.session_state.preset = "YTD"          # changed default to YTD
    if "date_range" not in st.session_state:
        st.session_state.date_range = compute_range_preset(st.session_state.preset)
    
    # Use three sub-columns for filter controls
    col_date, col_vendor, col_preset = parent_col.columns([1.4, 1.4, 2.2])
    
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
        # Cache vendor list
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
    
    return rng_start, rng_end, selected_vendor

def render_kpi_cards():
    """Render two rows of KPI cards with hardcoded values matching screenshot"""
    # Row 1
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        kpi_tile("TOTAL SPEND", "$5.5M", "↓ -61.2%", False)
    with col2:
        kpi_tile("ACTIVE PO's", "147", "↑ +100.0%", True)
    with col3:
        kpi_tile("TOTAL PO's", "474", "↓ -44.7%", False)
    with col4:
        kpi_tile("ACTIVE VENDORS", "38", "↓ -36.7%", False)
    
    # Row 2
    col5, col6, col7, col8 = st.columns(4)
    with col5:
        kpi_tile("PENDING INVOICES", "180", "↑ +100%", True)
    with col6:
        kpi_tile("AVG INVOICE PROCESSING TIME", "71.0d", "↓ 0.1d", True)   # down is positive for processing time
    with col7:
        kpi_tile("FIRST PASS INVOICE %", "60.5%", "↑ +0.8%", True)
    with col8:
        kpi_tile("AUTOPROCESSED INVOICES %", "0.0%")
    st.markdown("---")

def render_needs_attention(rng_start, rng_end, selected_vendor):
    """Display Needs Attention section with tabs and clickable invoice cards"""
    # Hardcoded counts to match screenshot
    overdue_count = 31
    disputed_count = 33
    due_count = 2
    total = overdue_count + disputed_count + due_count
    st.subheader(f"Needs Attention ({total})")
    
    tab_cols = st.columns(3)
    active_tab = st.session_state.get("na_tab", "Overdue")
    with tab_cols[0]:
        if st.button(f"Overdue ({overdue_count})", key="na_tab_overdue", use_container_width=True, type="primary" if active_tab == "Overdue" else "secondary"):
            st.session_state.na_tab = "Overdue"
            st.session_state.na_page = 0
            st.rerun()
    with tab_cols[1]:
        if st.button(f"Disputed ({disputed_count})", key="na_tab_disputed", use_container_width=True, type="primary" if active_tab == "Disputed" else "secondary"):
            st.session_state.na_tab = "Disputed"
            st.session_state.na_page = 0
            st.rerun()
    with tab_cols[2]:
        if st.button(f"Due ({due_count})", key="na_tab_due", use_container_width=True, type="primary" if active_tab == "Due" else "secondary"):
            st.session_state.na_tab = "Due"
            st.session_state.na_page = 0
            st.rerun()
    
    # Mock data for the invoice cards (replace with real query if needed)
    # For exact UI match, we provide sample invoices
    mock_invoices = {
        "Overdue": [
            {"invoice": "INV-001", "amount": 12500, "vendor": "TechCorp", "due_date": date(2026, 3, 15)},
            {"invoice": "INV-002", "amount": 8700, "vendor": "Global Logistics", "due_date": date(2026, 3, 10)},
            {"invoice": "INV-003", "amount": 23400, "vendor": "Office Supplies Co", "due_date": date(2026, 3, 5)},
        ],
        "Disputed": [
            {"invoice": "INV-004", "amount": 5600, "vendor": "TechCorp", "due_date": date(2026, 3, 20)},
            {"invoice": "INV-005", "amount": 18900, "vendor": "Builders Inc", "due_date": date(2026, 3, 18)},
        ],
        "Due": [
            {"invoice": "INV-006", "amount": 3200, "vendor": "Office Supplies Co", "due_date": date(2026, 4, 1)},
            {"invoice": "INV-007", "amount": 7400, "vendor": "Global Logistics", "due_date": date(2026, 4, 5)},
        ]
    }
    
    current_list = mock_invoices.get(active_tab, [])
    if current_list:
        items_per_page = 8
        total_items = len(current_list)
        total_pages = (total_items - 1) // items_per_page + 1
        page = st.session_state.get("na_page", 0)
        start_idx = page * items_per_page
        end_idx = min(start_idx + items_per_page, total_items)
        page_df = pd.DataFrame(current_list[start_idx:end_idx])
        
        # CSS for grid cards
        st.markdown("""
        <style>
        .na-grid {
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 1rem;
            margin-bottom: 1rem;
        }
        .na-card {
            background-color: #f9fafb;
            border: 1px solid #e5e7eb;
            border-radius: 16px;
            padding: 1rem;
            text-align: center;
            cursor: pointer;
            transition: all 0.2s ease;
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
        
        # Display grid
        for i in range(0, len(page_df), 4):
            cols = st.columns(4)
            for j in range(4):
                if i + j < len(page_df):
                    row = page_df.iloc[i + j]
                    inv_num = row['invoice']
                    amount = row['amount']
                    vendor = row['vendor']
                    due_date = row['due_date'].strftime('%Y-%m-%d')
                    if active_tab == "Overdue":
                        badge_bg = "#fee2e2"
                        badge_color = "#dc2626"
                    elif active_tab == "Disputed":
                        badge_bg = "#fef3c7"
                        badge_color = "#d97706"
                    else:
                        badge_bg = "#dbeafe"
                        badge_color = "#2563eb"
                    with cols[j]:
                        link_url = f"?page=Invoices&search_invoice={inv_num}"
                        st.markdown(f'''
                            <a href="{link_url}" style="text-decoration: none;">
                                <div class="na-card">
                                    <div class="invoice-pill">{inv_num}</div>
                                    <div class="status-badge" style="background:{badge_bg}; color:{badge_color};">{active_tab}</div>
                                    <div class="amount">{abbr_currency(amount)}</div>
                                    <div class="vendor-name">{vendor}</div>
                                    <div class="due-date">Due: {due_date}</div>
                                </div>
                            </a>
                        ''', unsafe_allow_html=True)
        
        # Pagination controls
        col_prev, col_info, col_next = st.columns([1,2,1])
        with col_prev:
            if st.button("← Prev", disabled=(page == 0)):
                st.session_state.na_page = page - 1
                st.rerun()
        with col_info:
            st.markdown(f"<div style='text-align:center'>Page {page+1} of {total_pages}</div>", unsafe_allow_html=True)
        with col_next:
            if st.button("Next →", disabled=(page >= total_pages-1)):
                st.session_state.na_page = page + 1
                st.rerun()
    else:
        st.info("No attention items in this category.")
    st.markdown("---")

def render_charts():
    """Render three charts: donut (invoice status), horizontal bar (top vendors), and trend bar (actual+forecast)"""
    chart_col1, chart_col2, chart_col3 = st.columns(3)
    
    # 1. Donut chart - Invoice Status Distribution
    with chart_col1:
        # Mock data matching screenshot: total 693, Paid, Pending, Disputed, Other
        status_data = pd.DataFrame({
            "status": ["Paid", "Pending", "Disputed", "Other"],
            "cnt": [400, 150, 100, 43],
            "color": ["#22C55E", "#FBBF24", "#EF4444", "#1E88E5"]
        })
        total = status_data["cnt"].sum()
        status_data["pct"] = status_data["cnt"] / total
        base = alt.Chart(status_data).encode(
            theta=alt.Theta(field="cnt", type='quantitative', stack=True),
            color=alt.Color(field="status", type='nominal', scale=alt.Scale(domain=status_data["status"].tolist(), range=status_data["color"].tolist())),
            tooltip=["status", "cnt", alt.Tooltip("pct:Q", format='.1%')]
        )
        arc = base.mark_arc(innerRadius=40, outerRadius=100)
        text = base.transform_filter(alt.datum.pct >= 0.01).mark_text(radius=115, color='#0f172a', fontSize=12, fontWeight='bold').encode(text=alt.Text("pct:Q", format='.1%'))
        chart = (arc + text).properties(height=300, title="Invoice Status Distribution")
        st.altair_chart(chart, use_container_width=True)
        st.caption(f"Total: {total}")
    
    # 2. Horizontal bar chart - Top 10 Vendors by Spend
    with chart_col2:
        top_vendors = pd.DataFrame({
            "vendor": ["Vendor A", "Vendor B", "Vendor C", "Vendor D", "Vendor E", 
                       "Vendor F", "Vendor G", "Vendor H", "Vendor I", "Vendor J"],
            "spend": [1250000, 980000, 720000, 560000, 430000, 310000, 250000, 190000, 140000, 85000]
        })
        chart = alt.Chart(top_vendors).mark_bar(color="#22C55E", cornerRadiusTopLeft=4).encode(
            x=alt.X("spend:Q", axis=alt.Axis(title=None, format="~s")),
            y=alt.Y("vendor:N", sort='-x', axis=alt.Axis(title=None)),
            tooltip=["vendor", alt.Tooltip("spend:Q", format="$,.0f")]
        ).properties(height=300, title="Top 10 Vendors by Spend")
        st.altair_chart(chart, use_container_width=True)
    
    # 3. Spend Trend Analysis (bar chart with Actual and Forecast)
    with chart_col3:
        trend_data = pd.DataFrame({
            "month": ["Jan", "Feb", "Mar", "Apr", "May", "Jun"],
            "actual": [420000, 385000, 510000, 475000, 530000, 560000],
            "forecast": [420000, 395000, 505000, 490000, 545000, 580000]
        })
        # Melt for grouped bar
        melted = trend_data.melt(id_vars=["month"], value_vars=["actual", "forecast"], var_name="type", value_name="spend")
        chart = alt.Chart(melted).mark_bar().encode(
            x=alt.X("month:N", axis=alt.Axis(title=None)),
            y=alt.Y("spend:Q", axis=alt.Axis(title=None, format="~s")),
            color=alt.Color("type:N", scale=alt.Scale(domain=["actual", "forecast"], range=["#22C55E", "#1E88E5"]), legend=alt.Legend(title=None)),
            tooltip=["month", "type", alt.Tooltip("spend:Q", format="$,.0f")]
        ).properties(height=300, title="Spend Trend Analysis")
        st.altair_chart(chart, use_container_width=True)

def render_dashboard():
    """Main Dashboard renderer"""
    # Header and filter bar (logo top-right, filters below)
    right_col = render_header()
    rng_start, rng_end, selected_vendor = render_filters(right_col)
    st.markdown("---")
    
    # KPI Cards (both rows)
    render_kpi_cards()
    
    # Needs Attention Section
    render_needs_attention(rng_start, rng_end, selected_vendor)
    
    # Analytics Charts
    st.subheader("Analytics")
    render_charts()

# ---------------------------- GENIE PAGE (unchanged) ----------------------------
def process_custom_query(query: str) -> dict:
    sql, _ = generate_sql(query)
    if not sql or not is_safe_sql(sql):
        return {"layout": "error", "message": "Failed to generate valid SQL."}
    sql = ensure_limit(sql)
    df = run_query(sql)
    if df.empty:
        return {"layout": "error", "message": "Query returned no data."}
    data_preview = df.head(10).to_string(index=False, max_colwidth=40)
    prompt = DESCRIPTIVE_PROMPT_TEMPLATE.format(question=query, sql=sql, data_preview=data_preview)
    analyst_text = ask_bedrock_cached(prompt, system_prompt="You are a helpful procurement analyst. Provide concise, data-driven insights.")
    return {
        "layout": "analyst",
        "sql": sql,
        "df": df.to_dict(orient="records"),
        "question": query,
        "analyst_response": analyst_text
    }

def render_genie():
    st.markdown("""
    <style>
    .genie-card {
        background: white;
        border-radius: 16px;
        padding: 1.5rem;
        box-shadow: 0 2px 8px rgba(0,0,0,0.05);
        transition: transform 0.2s ease, box-shadow 0.2s ease;
        height: 100%;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
    }
    .genie-card:hover {
        transform: translateY(-4px);
        box-shadow: 0 12px 24px rgba(0,0,0,0.1);
    }
    .genie-card h3 {
        font-size: 1.25rem;
        font-weight: 600;
        margin-bottom: 0.5rem;
    }
    .genie-card p {
        color: #64748b;
        font-size: 0.875rem;
        line-height: 1.5;
        margin-bottom: 1.5rem;
    }
    .genie-card button {
        width: 100%;
        margin-top: auto;
    }
    .suggestion-chip {
        background: #f1f5f9;
        border-radius: 999px;
        padding: 0.4rem 1rem;
        font-size: 0.8rem;
        cursor: pointer;
        transition: background 0.2s;
        display: inline-block;
        margin: 0.2rem;
    }
    .suggestion-chip:hover {
        background: #e2e8f0;
    }
    .chat-scrollable {
        max-height: 400px;
        overflow-y: auto;
        padding-right: 8px;
        margin-bottom: 1rem;
    }
    .chat-message-user {
        background: #1459d2;
        color: white;
        padding: 10px 14px;
        border-radius: 16px;
        margin: 6px 0;
        max-width: 80%;
        align-self: flex-end;
    }
    .chat-message-assistant {
        background: #f1f5f9;
        color: #0f172a;
        padding: 10px 14px;
        border-radius: 16px;
        margin: 6px 0;
        max-width: 80%;
    }
    .centered-container {
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        text-align: center;
        padding: 2rem;
    }
    </style>
    """, unsafe_allow_html=True)

    if "genie_session_id" not in st.session_state:
        st.session_state.genie_session_id = str(uuid.uuid4())
        st.session_state.genie_messages = []
        st.session_state.genie_turn_index = 0
    if "genie_response" not in st.session_state:
        st.session_state.genie_response = None
    if "selected_analysis" not in st.session_state:
        st.session_state.selected_analysis = None
    if "last_custom_query" not in st.session_state:
        st.session_state.last_custom_query = ""
    if "genie_prefill" not in st.session_state:
        st.session_state.genie_prefill = ""

    auto_query = st.session_state.pop("auto_run_query", None)
    if auto_query:
        # Clear previous chat history when a new auto query is triggered
        st.session_state.genie_messages = []
        st.session_state.genie_turn_index = 0
        st.session_state.selected_analysis = "custom"
        st.session_state.last_custom_query = auto_query
        with st.spinner("Running query and generating insights..."):
            result = process_custom_query(auto_query)
            st.session_state.genie_response = result
            st.session_state.genie_messages.append({"role": "user", "content": auto_query, "timestamp": datetime.now()})
            if result.get("layout") == "analyst":
                st.session_state.genie_messages.append({"role": "assistant", "content": "Insights generated.", "response": result, "timestamp": datetime.now()})
                save_chat_message(st.session_state.genie_session_id, st.session_state.genie_turn_index, "user", auto_query)
                st.session_state.genie_turn_index += 1
                save_chat_message(st.session_state.genie_session_id, st.session_state.genie_turn_index, "assistant", "Insights generated.", sql_used=result.get("sql", ""))
                st.session_state.genie_turn_index += 1
                save_question(auto_query, "custom")
                set_cache(auto_query, result)
            else:
                st.session_state.genie_messages.append({"role": "assistant", "content": result.get("message", "Error"), "timestamp": datetime.now()})
        st.rerun()

    st.markdown("## Welcome to ProcureIQ Genie")
    st.markdown("Let Genie run one of these quick analyses for you.")
    cols = st.columns(4)
    quick_options = {
        "spending_overview": ("Spending Overview", "Track total spend, monthly trends and major changes"),
        "vendor_analysis": ("Vendor Analysis", "Understand vendor-wise spend, concentration, and dependency"),
        "payment_performance": ("Payment Performance", "Identify delays, late payments, and cycle time issues"),
        "invoice_aging": ("Invoice Aging", "See overdue invoices, risk buckets, and problem areas")
    }
    for idx, (key, (title, desc)) in enumerate(quick_options.items()):
        with cols[idx]:
            with st.container():
                st.markdown(f"""
                <div class="genie-card">
                    <div>
                        <h3>{title}</h3>
                        <p>{desc}</p>
                    </div>
                </div>
                """, unsafe_allow_html=True)
                if st.button("Ask Genie", key=f"card_{key}", use_container_width=True):
                    st.session_state.auto_run_query = title
                    st.rerun()

    st.markdown("---")

    left_col, right_col = st.columns([0.35, 0.65], gap="large")

    with left_col:
        with st.expander("Saved insights", expanded=False):
            insights = get_saved_insights_cached(page="genie")
            if insights:
                for ins in insights:
                    if st.button(ins["title"], key=f"insight_{ins['id']}", use_container_width=True):
                        st.session_state.auto_run_query = ins["question"]
                        st.rerun()
            else:
                st.caption("Save any Genie answer to see it here.")

        with st.expander("Frequently asked by you", expanded=False):
            suggestions = [
                "forecast cash outflow for the next 7, 14, 30, 60, and 90 days",
                "show me total spend ytd, monthly trends, and top 5 vendors",
                "which invoices should we pay early to capture discounts"
            ]
            st.markdown('<div style="margin-bottom: 0.5rem;">Click a chip to fill the input:</div>', unsafe_allow_html=True)
            for chip in suggestions:
                if st.button(chip, key=f"chip_{chip[:20]}", use_container_width=True):
                    st.session_state.genie_prefill = chip
                    st.rerun()
            faqs = get_frequent_questions_by_user_cached(5)
            if faqs:
                st.markdown("---")
                st.markdown("**Your top questions**")
                for faq in faqs:
                    if st.button(f"{faq['query'][:50]} ({faq['count']})", key=f"faq_user_{faq['query']}", use_container_width=True):
                        st.session_state.genie_prefill = faq["query"]
                        st.rerun()
            else:
                st.caption("Your frequent questions will appear here.")

        with st.expander("Most frequent (all)", expanded=False):
            all_faqs = get_frequent_questions_all_cached(5)
            if all_faqs:
                for faq in all_faqs:
                    st.button(f"{faq['query'][:50]} ({faq['count']})", key=f"faq_all_{faq['query']}", use_container_width=True, disabled=True)
            else:
                st.caption("No questions yet.")

    with right_col:
        st.markdown('<div class="centered-container">', unsafe_allow_html=True)
        st.markdown("### Start a Conversation")
        st.markdown("Ask questions about your Procurement to Pay data, or select a pre-built analysis from the library.")
        st.markdown('</div>', unsafe_allow_html=True)

        st.markdown('<div class="chat-scrollable">', unsafe_allow_html=True)
        for msg in st.session_state.genie_messages:
            if msg["role"] == "user":
                st.markdown(f'<div class="chat-message-user"><strong>You</strong><br/>{html.escape(msg["content"])}</div>', unsafe_allow_html=True)
            else:
                st.markdown(f'<div class="chat-message-assistant"><strong>Genie</strong><br/>{html.escape(msg["content"])}</div>', unsafe_allow_html=True)
                if "response" in msg and msg["response"]:
                    resp = msg["response"]
                    if resp.get("layout") == "quick":
                        metrics = resp.get("metrics", {})
                        if metrics:
                            metric_cols = st.columns(len(metrics))
                            for i, (k, v) in enumerate(metrics.items()):
                                with metric_cols[i]:
                                    st.metric(k.replace("_"," ").title(), abbr_currency(v) if isinstance(v, (int,float)) else str(v))
                        anomaly = resp.get("anomaly")
                        if anomaly:
                            st.warning(f"⚠️ {anomaly}")
                        monthly_df = resp.get("monthly_df")
                        if monthly_df is not None and not monthly_df.empty:
                            st.subheader("Spending Trends")
                            alt_line_monthly(monthly_df.rename(columns={"MONTH":"MONTH", "MONTHLY_SPEND":"VALUE"}), month_col="MONTH", value_col="VALUE", height=200)
                        vendors_df = resp.get("vendors_df")
                        if vendors_df is not None and not vendors_df.empty:
                            st.subheader("Top Vendors")
                            alt_bar(vendors_df.head(10), x="VENDOR_NAME", y="SPEND", horizontal=True, height=300)
                        with st.expander("View SQL used"):
                            sql_dict = resp.get("sql", {})
                            for name, sql_text in sql_dict.items():
                                st.code(sql_text, language="sql")
                    elif resp.get("layout") == "analyst":
                        analyst_text = resp.get("analyst_response", "")
                        if analyst_text:
                            st.markdown(analyst_text)
                        else:
                            st.info("No descriptive analysis available.")
                        df = pd.DataFrame(resp["df"])
                        st.dataframe(df, use_container_width=True)
                        chart = auto_chart(df)
                        if chart:
                            st.altair_chart(chart, use_container_width=True)
                        with st.expander("View SQL used"):
                            st.code(resp["sql"], language="sql")
                    elif resp.get("layout") == "sql":
                        df = pd.DataFrame(resp["df"])
                        st.dataframe(df, use_container_width=True)
                        chart = auto_chart(df)
                        if chart:
                            st.altair_chart(chart, use_container_width=True)
                        with st.expander("View SQL"):
                            st.code(resp["sql"], language="sql")
                    elif resp.get("layout") == "error":
                        st.error(resp.get("message", "Unknown error"))
        st.markdown('</div>', unsafe_allow_html=True)

        with st.form(key="genie_form", clear_on_submit=True):
            col_input, col_btn = st.columns([0.85, 0.15])
            with col_input:
                user_question = st.text_input(
                    "Ask a question",
                    value=st.session_state.pop("genie_prefill", ""),
                    placeholder="e.g., Show me total spend YTD",
                    label_visibility="collapsed"
                )
            with col_btn:
                submitted = st.form_submit_button("→", type="primary")
            if submitted and user_question:
                # For typed questions, we append to conversation (do NOT clear)
                with st.spinner("Generating SQL and insights..."):
                    cached = get_cache(user_question)
                    if cached:
                        st.session_state.genie_response = cached
                        st.session_state.genie_messages.append({"role": "user", "content": user_question, "timestamp": datetime.now()})
                        st.session_state.genie_messages.append({"role": "assistant", "content": "Answer from cache.", "response": cached, "timestamp": datetime.now()})
                        save_chat_message(st.session_state.genie_session_id, st.session_state.genie_turn_index, "user", user_question)
                        st.session_state.genie_turn_index += 1
                        save_chat_message(st.session_state.genie_session_id, st.session_state.genie_turn_index, "assistant", "Answer from cache.", source="cache")
                        st.session_state.genie_turn_index += 1
                        save_question(user_question, "custom")
                    else:
                        result = process_custom_query(user_question)
                        if result.get("layout") in ("analyst", "sql"):
                            set_cache(user_question, result)
                            st.session_state.genie_response = result
                            st.session_state.genie_messages.append({"role": "user", "content": user_question, "timestamp": datetime.now()})
                            st.session_state.genie_messages.append({"role": "assistant", "content": "Insights generated.", "response": result, "timestamp": datetime.now()})
                            save_chat_message(st.session_state.genie_session_id, st.session_state.genie_turn_index, "user", user_question)
                            st.session_state.genie_turn_index += 1
                            save_chat_message(st.session_state.genie_session_id, st.session_state.genie_turn_index, "assistant", "Insights generated.", sql_used=result.get("sql", ""))
                            st.session_state.genie_turn_index += 1
                            save_question(user_question, "custom")
                        else:
                            st.error(result.get("message", "Query failed"))
                st.rerun()

def generate_prescriptive_from_quick(resp: dict) -> str:
    insights = []
    metrics = resp.get("metrics", {})
    total_ytd = metrics.get("total_ytd", 0)
    mom_pct = metrics.get("mom_pct", 0)
    qoq_pct = metrics.get("qoq_pct", 0)
    top5_pct = metrics.get("top5_pct", 0)

    if total_ytd:
        insights.append(f"• Total Spend YTD: {abbr_currency(total_ytd)}. Action: Review and optimize procurement processes to reduce costs.")
    if mom_pct != 0:
        trend = "decrease" if mom_pct < 0 else "increase"
        insights.append(f"• Monthly spend has {trend} by {abs(mom_pct):.1f}% MoM. Action: Analyze root causes and adjust procurement strategies.")
    if qoq_pct != 0:
        trend = "decrease" if qoq_pct < 0 else "increase"
        insights.append(f"• Quarterly spend {trend} by {abs(qoq_pct):.1f}% QoQ. Action: Review category-level performance.")
    if top5_pct:
        insights.append(f"• Top 5 vendors account for {top5_pct:.0f}% of spend. Action: Negotiate volume discounts and consider consolidation.")

    anomaly = resp.get("anomaly")
    if anomaly:
        insights.append(f"• Anomaly: {anomaly[:100]}... Action: Investigate cause and prevent recurrence.")

    if not insights:
        return "No specific prescriptive insights available based on the data."
    return "<br/>".join(insights[:6])

# ---------------------------- FORECAST PAGE (unchanged) ----------------------------
def render_forecast():
    # Get cash flow data
    cf_sql = f"""
        SELECT
            forecast_bucket,
            invoice_count,
            total_amount,
            earliest_due,
            latest_due
        FROM {DATABASE}.cash_flow_forecast_vw
        ORDER BY CASE forecast_bucket
            WHEN 'TOTAL_UNPAID' THEN 0
            WHEN 'OVERDUE_NOW' THEN 1
            WHEN 'DUE_7_DAYS' THEN 2
            WHEN 'DUE_14_DAYS' THEN 3
            WHEN 'DUE_30_DAYS' THEN 4
            WHEN 'DUE_60_DAYS' THEN 5
            WHEN 'DUE_90_DAYS' THEN 6
            WHEN 'BEYOND_90_DAYS' THEN 7
            ELSE 8 END
    """
    cf_df = run_query(cf_sql)

    if cf_df.empty:
        st.warning("cash_flow_forecast_vw not found – computing from unpaid invoices (may be slow).")
        cf_sql_fallback = f"""
            WITH base AS (
                SELECT
                    invoice_number,
                    invoice_amount_local,
                    due_date,
                    invoice_status,
                    DATE_DIFF('day', CURRENT_DATE, due_date) AS days_until_due
                FROM {DATABASE}.fact_all_sources_vw
                WHERE UPPER(invoice_status) IN ('OPEN', 'DUE', 'OVERDUE')
                  AND due_date IS NOT NULL
            ),
            buckets AS (
                SELECT
                    CASE
                        WHEN days_until_due < 0 THEN 'OVERDUE_NOW'
                        WHEN days_until_due <= 7 THEN 'DUE_7_DAYS'
                        WHEN days_until_due <= 14 THEN 'DUE_14_DAYS'
                        WHEN days_until_due <= 30 THEN 'DUE_30_DAYS'
                        WHEN days_until_due <= 60 THEN 'DUE_60_DAYS'
                        WHEN days_until_due <= 90 THEN 'DUE_90_DAYS'
                        ELSE 'BEYOND_90_DAYS'
                    END AS forecast_bucket,
                    COUNT(*) AS invoice_count,
                    SUM(invoice_amount_local) AS total_amount,
                    MIN(due_date) AS earliest_due,
                    MAX(due_date) AS latest_due
                FROM base
                GROUP BY 1
            ),
            total AS (
                SELECT 'TOTAL_UNPAID' AS forecast_bucket,
                       SUM(invoice_count) AS invoice_count,
                       SUM(total_amount) AS total_amount,
                       NULL AS earliest_due,
                       NULL AS latest_due
                FROM buckets
            )
            SELECT * FROM total
            UNION ALL SELECT * FROM buckets
            ORDER BY CASE forecast_bucket
                WHEN 'TOTAL_UNPAID' THEN 0
                WHEN 'OVERDUE_NOW' THEN 1
                WHEN 'DUE_7_DAYS' THEN 2
                WHEN 'DUE_14_DAYS' THEN 3
                WHEN 'DUE_30_DAYS' THEN 4
                WHEN 'DUE_60_DAYS' THEN 5
                WHEN 'DUE_90_DAYS' THEN 6
                ELSE 7 END
        """
        cf_df = run_query(cf_sql_fallback)

    tab1, tab2 = st.tabs(["Cash Flow Need Forecast", "GR/IR Reconciliation"])

    with tab1:
        if not cf_df.empty:
            total_unpaid = cf_df[cf_df["forecast_bucket"] == "TOTAL_UNPAID"]["total_amount"].values[0] if not cf_df[cf_df["forecast_bucket"] == "TOTAL_UNPAID"].empty else 0
            overdue_now = cf_df[cf_df["forecast_bucket"] == "OVERDUE_NOW"]["total_amount"].values[0] if not cf_df[cf_df["forecast_bucket"] == "OVERDUE_NOW"].empty else 0
            due_30 = cf_df[cf_df["forecast_bucket"].isin(["DUE_7_DAYS","DUE_14_DAYS","DUE_30_DAYS"])]["total_amount"].sum()
            pct_due_30 = (due_30 / total_unpaid * 100) if total_unpaid > 0 else 0
        else:
            total_unpaid = overdue_now = due_30 = 0
            pct_due_30 = 0

        kpi_colors = ["#fff7e0", "#ffe6ef", "#e6f3ff", "#e0f7fa"]
        kpi_titles = ["TOTAL UNPAID", "OVERDUE NOW", "DUE NEXT 30 DAYS", "% DUE ≤ 30 DAYS"]
        kpi_values = [abbr_currency(total_unpaid), abbr_currency(overdue_now), abbr_currency(due_30), f"{pct_due_30:.1f}%"]

        st.markdown("""
        <style>
        .forecast-kpi-card {
            border-radius: 16px;
            padding: 1.2rem;
            box-shadow: 0 1px 3px rgba(0,0,0,0.05);
            text-align: left;
            background-color: var(--bg);
            border: 1px solid rgba(0,0,0,0.05);
        }
        .forecast-kpi-title {
            font-size: 0.85rem;
            font-weight: 600;
            color: #475569;
            margin-bottom: 0.5rem;
        }
        .forecast-kpi-value {
            font-size: 2rem;
            font-weight: 700;
            color: #0f172a;
            line-height: 1.2;
        }
        </style>
        """, unsafe_allow_html=True)

        cols = st.columns(4)
        for i, col in enumerate(cols):
            with col:
                st.markdown(f"""
                <div class="forecast-kpi-card" style="background-color: {kpi_colors[i]};">
                    <div class="forecast-kpi-title">{kpi_titles[i]}</div>
                    <div class="forecast-kpi-value">{kpi_values[i]}</div>
                </div>
                """, unsafe_allow_html=True)

        st.markdown("---")
        st.markdown("#### Obligations by time bucket")
        if not cf_df.empty:
            st.dataframe(cf_df, use_container_width=True, hide_index=True)
            csv = cf_df.to_csv(index=False).encode('utf-8')
            st.download_button("Download forecast (CSV)", data=csv, file_name="cash_flow_forecast.csv", mime="text/csv")
        else:
            st.info("No cash flow forecast data available.")

        st.markdown("---")
        st.markdown("### Action Playbook")
        st.markdown("Use these guided analyses to turn the forecast into decisions: who to pay now, who to pay early, and where we are at risk of paying late.")
        actions = [
            ("📊 Forecast cash outflow (7–90 days)", "Forecast cash outflow for the next 7, 14, 30, 60, and 90 days"),
            ("💰 Invoices to pay early to capture discounts", "Which invoices should we pay early to capture discounts?"),
            ("⏰ Optimal payment timing for this week", "What is the optimal payment timing strategy for this week?"),
            ("⚠️ Late payment trend and risk", "Show late payment trend for forecasting")
        ]
        for label, question in actions:
            if st.button(label, use_container_width=True):
                st.session_state.auto_run_query = question
                st.session_state.page = "Genie"
                st.rerun()

    with tab2:
        st.markdown("#### GR/IR Reconciliation")

        # GR/IR KPIs and position note
        grir_summary_sql = f"""
            WITH latest AS (
                SELECT year, month, invoice_count, total_grir_blnc
                FROM {DATABASE}.gr_ir_outstanding_balance_vw
                ORDER BY year DESC, month DESC
                LIMIT 1
            ),
            aging_latest AS (
                SELECT year, month, pct_grir_over_60, cnt_grir_over_60
                FROM {DATABASE}.gr_ir_aging_vw
                ORDER BY year DESC, month DESC
                LIMIT 1
            )
            SELECT
                l.year,
                l.month,
                l.invoice_count AS grir_items,
                l.total_grir_blnc AS total_grir_balance,
                a.pct_grir_over_60,
                a.cnt_grir_over_60,
                COALESCE(l.total_grir_blnc * a.pct_grir_over_60 / 100, 0) AS amount_over_60_days
            FROM latest l
            LEFT JOIN aging_latest a ON a.year = l.year AND a.month = l.month
        """
        grir_df = run_query(grir_summary_sql)
        if not grir_df.empty:
            row = grir_df.iloc[0]
            total_grir = safe_number(row.get("total_grir_balance", 0))
            grir_items = safe_int(row.get("grir_items", 0))
            pct_over_60 = safe_number(row.get("pct_grir_over_60", 0))
            amount_over_60 = safe_number(row.get("amount_over_60_days", 0))
            cnt_over_60 = safe_int(row.get("cnt_grir_over_60", 0))
            year = safe_int(row.get("year", 0))
            month = safe_int(row.get("month", 0))

            grir_cols = st.columns(4)
            grir_cols[0].metric("TOTAL GR/IR", abbr_currency(total_grir))
            grir_cols[1].metric("% > 60 DAYS", f"{pct_over_60:.1f}%")
            grir_cols[2].metric("> 60 DAYS AMOUNT", abbr_currency(amount_over_60))
            grir_cols[3].metric("> 60 DAYS ITEMS", f"{cnt_over_60:,}")

            st.caption(f"GR/IR position for {year:04d}-{month:02d}: {grir_items:,} items outstanding; {pct_over_60:.1f}% of balance and {cnt_over_60:,} items are older than 60 days.")

            # Trend chart
            trend_sql = f"""
                SELECT
                    DATE_PARSE(CAST(year AS VARCHAR) || '-' || LPAD(CAST(month AS VARCHAR), 2, '0') || '-01', '%Y-%m-%d') AS month_date,
                    invoice_count,
                    total_grir_blnc
                FROM {DATABASE}.gr_ir_outstanding_balance_vw
                ORDER BY year DESC, month DESC
                LIMIT 24
            """
            trend_df = run_query(trend_sql)
            if not trend_df.empty:
                trend_df = trend_df.sort_values("month_date")
                st.markdown("**GR/IR outstanding trend (last 24 months)**")
                try:
                    alt_line_monthly(
                        trend_df.rename(columns={"month_date": "MONTH", "total_grir_blnc": "VALUE"}),
                        month_col="MONTH",
                        value_col="VALUE",
                        height=250,
                        title="Total GR/IR balance over time",
                    )
                except Exception:
                    st.dataframe(trend_df, use_container_width=True)
        else:
            st.info("No GR/IR data found.")

        # GR/IR Clearing Playbook
        st.markdown("---")
        st.markdown("### GR/IR Clearing Playbook")
        st.markdown("Each step opens Genie with a pre-built prompt that uses the `gr_ir_outstanding` and related verified queries so you get concrete actions (which POs to clear, where to chase receipts, and how much working capital you can release).")

        clearing_actions = [
            ("1. Identify top GR/IR hotspots to clear first", "Show GR/IR outstanding balance by month and highlight which recent months have the highest GR/IR balance so we can prioritize clearing."),
            ("2. Explain likely GR/IR root causes", "Using GR/IR aging and outstanding balance data, explain the likely root-cause buckets (missing goods receipt, invoice not posted, price or quantity mismatch) and for each bucket suggest 2–3 concrete remediation actions."),
            ("3. Quantify working-capital benefit from clearing old GR/IR", "Estimate the working capital that would be released by clearing all GR/IR items older than 60 and 90 days, by month."),
            ("4. Draft vendor follow-up messages for top GR/IR items", "Based on GR/IR aging and outstanding balances, draft vendor-facing follow-up templates we can use for high-priority GR/IR items, with realistic subject lines and concise bullet points.")
        ]

        for label, question in clearing_actions:
            if st.button(label, use_container_width=True):
                st.session_state.auto_run_query = question
                st.session_state.page = "Genie"
                st.rerun()

# ---------------------------- INVOICES PAGE (unchanged) ----------------------------
def _get_ai_invoice_suggestion(invoice_number: str, inv_row: dict, status_history: str = "") -> str:
    status = str(inv_row.get("invoice_status", "")).strip()
    due = inv_row.get("due_date")
    aging = inv_row.get("aging_days")
    amount = inv_row.get("invoice_amount")
    due_str = str(due) if due else "unknown"
    aging_str = f"{int(aging)} days" if aging is not None else "unknown"
    amount_str = f"{float(amount):,.2f}" if amount is not None else "unknown"

    is_overdue = False
    try:
        if due and status.upper() not in ("PAID", "CLEARED"):
            due_date = date.fromisoformat(str(due)[:10])
            is_overdue = due_date < date.today()
    except Exception:
        pass

    overdue_context = ""
    if is_overdue:
        overdue_context = f"This invoice IS overdue (due date {due_str} has passed and it is not yet paid). "
    elif status.upper() in ("PAID", "CLEARED"):
        overdue_context = "This invoice is already PAID/CLEARED. It is NOT overdue. "
    else:
        overdue_context = "This invoice is NOT overdue (the due date has not passed yet). "

    prompt = (
        "Concise procure-to-pay analyst. 2-3 sentences of actionable advice based ONLY on the data below. "
        f"{overdue_context}"
        "OPEN & not overdue: say proceed to pay. Overdue: recommend immediate review. PAID: no action. "
        f"Invoice: {invoice_number}. Status: {status}. Due: {due_str}. Aging: {aging_str}. Amount: {amount_str}."
    )
    response = ask_bedrock_cached(prompt, system_prompt="You are a helpful procurement analyst. Provide concise, actionable advice.")
    if response and len(response.strip()) > 10:
        return response.strip()
    # Fallback
    if status.upper() in ("PAID", "CLEARED"):
        return f"Invoice {invoice_number} has already been **paid**. No further action is needed."
    elif is_overdue:
        return f"Invoice {invoice_number} is **overdue** (due {due_str}). Recommend **immediate review** to avoid penalties."
    else:
        return f"Invoice {invoice_number} is {status.lower()} with due date {due_str}. Proceed to pay."

def render_invoices():
    st.subheader("Invoices")
    st.markdown("Search, track and manage all invoices in one place")

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

        if user_search and len(df) == 1:
            inv_num = clean_invoice_number(df.iloc[0,0])
            st.markdown("---")
            st.subheader(f"Invoice Details: {inv_num}")

            details_sql = f"""
                SELECT
                    f.invoice_number,
                    f.posting_date AS invoice_date,
                    f.invoice_amount_local AS invoice_amount,
                    f.purchase_order_reference AS po_number,
                    f.po_amount AS po_amount,
                    f.due_date,
                    f.invoice_status,
                    f.company_code,
                    f.fiscal_year,
                    f.aging_days
                FROM {DATABASE}.fact_all_sources_vw f
                WHERE CAST(f.invoice_number AS VARCHAR) = '{inv_num}'
                LIMIT 1
            """
            details_df = run_query(details_sql)
            if not details_df.empty:
                st.dataframe(details_df, use_container_width=True)

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
            if not hist_df.empty:
                st.subheader("Status History")
                st.dataframe(hist_df, use_container_width=True)

            vendor_sql = f"""
                SELECT DISTINCT
                    v.vendor_id,
                    v.vendor_name,
                    v.vendor_name_2,
                    v.country_code,
                    v.city,
                    v.postal_code,
                    v.street,
                    v.region_code,
                    v.industry_sector,
                    v.vendor_account_group,
                    v.tax_number_1,
                    v.tax_number_2,
                    v.deletion_flag,
                    v.posting_block,
                    v.system
                FROM {DATABASE}.fact_all_sources_vw f
                LEFT JOIN {DATABASE}.dim_vendor_vw v ON f.vendor_id = v.vendor_id
                WHERE CAST(f.invoice_number AS VARCHAR) = '{inv_num}'
                LIMIT 1
            """
            vendor_df = run_query(vendor_sql)
            if not vendor_df.empty:
                st.subheader("Vendor Info")
                st.dataframe(vendor_df, use_container_width=True)

            company_sql = f"""
                SELECT DISTINCT
                    f.company_code,
                    cc.company_name,
                    f.plant_code,
                    plt.plant_name
                FROM {DATABASE}.fact_all_sources_vw f
                LEFT JOIN {DATABASE}.dim_company_code_vw cc ON f.company_code = cc.company_code
                LEFT JOIN {DATABASE}.dim_plant_vw plt ON f.plant_code = plt.plant_code
                WHERE CAST(f.invoice_number AS VARCHAR) = '{inv_num}'
                LIMIT 1
            """
            company_df = run_query(company_sql)
            if not company_df.empty:
                st.subheader("Company Info")
                st.dataframe(company_df, use_container_width=True)

            st.subheader("Genie insights")
            inv_row = details_df.iloc[0].to_dict() if not details_df.empty else {}
            status_history = hist_df[["status", "effective_date", "status_notes"]].head(5).to_string(index=False) if not hist_df.empty else ""
            suggestion = _get_ai_invoice_suggestion(inv_num, inv_row, status_history)
            st.markdown(f'<div style="background:#f0f9ff; border-left:4px solid #1459d2; padding:12px; border-radius:8px;">{suggestion}</div>', unsafe_allow_html=True)
    else:
        st.info("No invoices found. Try a different search term.")

# ---------------------------- Main App Layout ----------------------------
st.markdown("""
<style>
.block-container {
    padding-top: 1rem;
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
</style>
""", unsafe_allow_html=True)

if "page" not in st.session_state:
    st.session_state.page = "Dashboard"

if st.session_state.page == "Dashboard":
    render_dashboard()
elif st.session_state.page == "Genie":
    render_genie()
elif st.session_state.page == "Forecast":
    render_forecast()
else:
    render_invoices()
