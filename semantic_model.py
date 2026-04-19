# semantic_model.py
from config import DATABASE

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
  - CASH FLOW FORECAST (CRITICAL): For "Forecast cash outflow for the next 7, 14, 30, 60, and 90 days" you MUST use verified query cash_flow_forecast (table cash_flow_forecast). It returns FORECAST_BUCKET, INVOICE_COUNT, TOTAL_AMOUNT, EARLIEST_DUE, LATEST_DUE. Present ALL rows: TOTAL_UNPAID, OVERDUE_NOW, DUE_7_DAYS, DUE_14_DAYS, DUE_30_DAYS, DUE_60_DAYS, DUE_90_DAYS, BEYOND_90_DAYS (and PROCESSING_LAG_DAYS if present). Do NOT aggregate to a single total or single bar; show the full bucket breakdown in both the table and the chart. Then give 3-5 prescriptive recommendations.
  - PAYMENT TIMING AND EARLY PAYMENT DISCOUNTS (CRITICAL): For "Which invoices should we pay early to capture discounts?" you MUST use verified query early_payment_candidates. Do NOT say the model lacks discount data—the query returns DOCUMENT_NUMBER, VENDOR_NAME, INVOICE_AMOUNT_LOCAL, DUE_DATE, SAVINGS_IF_2PCT_DISCOUNT, EARLY_PAY_PRIORITY. Present that table and cite it. For "optimal payment timing", "payment strategy for this week", "when should we pay": USE verified query payment_timing_recommendation (RECOMMENDATION, AMOUNT, INVOICE_COUNT, RATIONALE). NEVER claim payment schedules or discount data are missing.
  - GR/IR ROOT CAUSE & FOLLOW-UP: For GR/IR questions that ask about root causes (missing goods receipt, missing invoice, price/quantity mismatch) or vendor follow-up messages, USE gr_ir_aging, gr_ir_outstanding, gr_ir_working_capital_release, and gr_ir_hotspots_clearing_plan as needed. Do NOT say the data is missing. Instead, clearly explain that you are inferring likely root causes from aging and outstanding balances, and then provide concrete remediation steps or example email templates.

tables:
  - name: fact_invoices
    description: "Unified invoice fact with status, amounts, PO linkage, aging. Use for spend, vendor, status, overdue, disputed. posting_date = when invoice was posted; use for this month, last month, YTD."
    base_table:
      database: PROCURE2PAY
      schema: INFORMATION_MART
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
      database: PROCURE2PAY
      schema: INFORMATION_MART
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
      database: PROCURE2PAY
      schema: INFORMATION_MART
      table: DAYS_PAYABLE_OUTSTANDING_VW
  - name: dim_company_code
    description: "Company code dimension with full master data. Columns: COMPANY_CODE, COMPANY_NAME, CITY, COUNTRY_CODE, CURRENCY, POSTAL_CODE, STREET, REGION_CODE, VAT_REG_NUMBER, CHART_OF_ACCOUNTS, SYSTEM"
    base_table:
      database: PROCURE2PAY
      schema: INFORMATION_MART
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
      database: PROCURE2PAY
      schema: INFORMATION_MART
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
      database: PROCURE2PAY
      schema: INFORMATION_MART
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
      database: PROCURE2PAY
      schema: INFORMATION_MART
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
      database: PROCURE2PAY
      schema: INFORMATION_MART
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
      database: PROCURE2PAY
      schema: INFORMATION_MART
      table: DUPLICATE_PAYMENTS_FOR_INVOICE_VW
  - name: invoice_status_history
    description: "Full invoice status history with ALL invoice details. One row per status change. Use MAX(SEQUENCE_NBR) for latest record. Columns include: INVOICE_NUMBER, STATUS, SEQUENCE_NBR, EFFECTIVE_DATE, POSTING_DATE, DUE_DATE, VENDOR_ID, INVOICE_AMOUNT_LOCAL, PAYMENT_DATE, CLEARING_DOCUMENT, AGING_DAYS, PURCHASE_ORDER_REFERENCE, PO_AMOUNT, PO_PURPOSE, DOCUMENT_TYPE, DISCOUNT_PERCENT, REGION, SYSTEM"
    base_table:
      database: PROCURE2PAY
      schema: INFORMATION_MART
      table: INVOICE_STATUS_HISTORY_VW
  - name: fact_po_level
    description: "PO-level fact. Columns: POSTING_YEAR, POSTING_MONTH, DELIVERY_DATE, RECEIVED_QTY, ORDERED_QTY, PO_AMOUNT"
    base_table:
      database: PROCURE2PAY
      schema: INFORMATION_MART
      table: FACT_SAP_PO_LEVEL_VW
  - name: full_payment_rate
    description: "Full payment rate. Columns: YEAR, MONTH, FULL_PAID_INVOICES, TOTAL_CLEARED_INVOICES, FULL_PAYMENT_RATE_PCT"
    base_table:
      database: PROCURE2PAY
      schema: INFORMATION_MART
      table: FULL_PAYMENT_RATE_VW
  - name: gr_ir_aging
    description: "GR/IR aging. Columns: YEAR, MONTH, AGE_DAYS, TOTAL_GRIR_BALANCE, GRIR_OVER_30/60/90"
    base_table:
      database: PROCURE2PAY
      schema: INFORMATION_MART
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
      database: PROCURE2PAY
      schema: INFORMATION_MART
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
      database: PROCURE2PAY
      schema: INFORMATION_MART
      table: LATE_ACCRUALS_VW
  - name: late_payment_amount
    description: "Late payment. Columns: YEAR, MONTH, LATE_PAYMENT_AMOUNT, LATE_PAYMENT_COUNT, LATE_PAYMENT_RATE_PCT"
    base_table:
      database: PROCURE2PAY
      schema: INFORMATION_MART
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
      database: PROCURE2PAY
      schema: INFORMATION_MART
      table: NET_EARLY_PAYMENT_BENEFIT_INDEX_VW
  - name: on_time_payment_rate
    description: "On-time payment rate. Columns: YEAR, MONTH, ON_TIME_PAYMENTS, TOTAL_PAYMENTS, ON_TIME_PAYMENT_RATE_PCT"
    base_table:
      database: PROCURE2PAY
      schema: INFORMATION_MART
      table: ON_TIME_PAYMENT_RATE_VW
  - name: partial_payment_rate
    description: "Partial payment rate. Columns: YEAR, MONTH, PARTIAL_PAID_INVOICES, PARTIAL_PAYMENT_RATE_PCT"
    base_table:
      database: PROCURE2PAY
      schema: INFORMATION_MART
      table: PARTIAL_PAYMENT_RATE_VW
  - name: payment_predictability
    description: "Payment predictability. Columns: POSTING_YEAR, POSTING_MONTH, PAYMENT_PREDICTABILITY_INDEX, INTERPRETATION"
    base_table:
      database: PROCURE2PAY
      schema: INFORMATION_MART
      table: PAYMENT_PREDICTABILITY_INDEX_VW
  - name: payment_cycle_time
    description: "Payment cycle time. Columns: YEAR, MONTH, AVG_PAYMENT_CYCLE_TIME_DAYS, CLEARED_INVOICES"
    base_table:
      database: PROCURE2PAY
      schema: INFORMATION_MART
      table: PAYMENT_PROCESSING_CYCLE_TIME_VW
  - name: supplier_delivery_accuracy
    description: "Supplier delivery. Columns: YEAR, MONTH, ON_TIME_DELIVERIES, TOTAL_DELIVERIES, DELIVERY_ACCURACY_PCT"
    base_table:
      database: PROCURE2PAY
      schema: INFORMATION_MART
      table: SUPPLIER_DELIVERY_ACCURACY_INDEX_VW
  - name: weighted_dpo
    description: "Weighted DPO. Columns: YEAR, MONTH, TOTAL_PAYABLES, TOTAL_COGS, WEIGHTED_DPO"
    base_table:
      database: PROCURE2PAY
      schema: INFORMATION_MART
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
      database: PROCURE2PAY
      schema: INFORMATION_MART
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
      database: PROCURE2PAY
      schema: INFORMATION_MART
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
      database: PROCURE2PAY
      schema: INFORMATION_MART
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
      database: PROCURE2PAY
      schema: INFORMATION_MART
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
        SELECT * FROM PROCURE2PAY.INFORMATION_MART.FACT_ALL_SOURCES_VW
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
        SELECT * FROM PROCURE2PAY.INFORMATION_MART.FACT_ALL_SOURCES_VW
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
        SELECT * FROM PROCURE2PAY.INFORMATION_MART.FACT_ALL_SOURCES_VW
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
        SELECT * FROM PROCURE2PAY.INFORMATION_MART.FACT_ALL_SOURCES_VW
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
      WITH base AS (SELECT * FROM PROCURE2PAY.INFORMATION_MART.FACT_ALL_SOURCES_VW WHERE UPPER(INVOICE_STATUS) NOT IN ('CANCELLED', 'REJECTED') AND POSTING_DATE IS NOT NULL),
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
        SELECT * FROM PROCURE2PAY.INFORMATION_MART.FACT_ALL_SOURCES_VW
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
      WITH base AS (SELECT * FROM PROCURE2PAY.INFORMATION_MART.FACT_ALL_SOURCES_VW WHERE UPPER(INVOICE_STATUS) NOT IN ('CANCELLED', 'REJECTED') AND POSTING_DATE IS NOT NULL),
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
        SELECT * FROM PROCURE2PAY.INFORMATION_MART.FACT_ALL_SOURCES_VW
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
        FROM (SELECT * FROM PROCURE2PAY.INFORMATION_MART.LATE_PAYMENT_AMOUNT_VW ORDER BY YEAR DESC, MONTH DESC LIMIT 1)
      ),
      early_benefit AS (
        SELECT '5_Early_Payment_Discount' AS OPPORTUNITY_AREA,
          COALESCE(NEPBI_PERCENT, 0) AS PCT_OF_SPEND,
          COALESCE(TOTAL_NET_BENEFIT, 0) AS AMOUNT,
          CONCAT('Current benefit: ', ROUND(COALESCE(NEPBI_PERCENT, 0), 2), '% of spend') AS FINDING,
          'Increase early payments to capture more discount; target NEPBI > 2%' AS RECOMMENDED_ACTION
        FROM PROCURE2PAY.INFORMATION_MART.NET_EARLY_PAYMENT_BENEFIT_INDEX_VW
      ),
      dup_pmt AS (
        SELECT '6_Duplicate_Payments' AS OPPORTUNITY_AREA,
          ROUND(COALESCE(AVG(DUPLICATE_PAYMENT_RATE), 0), 1) AS PCT_OF_SPEND,
          ROUND(COALESCE(SUM(DUPLICATE_PAYMENT_AMOUNT), 0), 2) AS AMOUNT,
          CONCAT(COALESCE(COUNT(*), 0), ' invoices with potential duplicates') AS FINDING,
          'Implement duplicate check before payment; reconcile regularly' AS RECOMMENDED_ACTION
        FROM PROCURE2PAY.INFORMATION_MART.DUPLICATE_PAYMENTS_FOR_INVOICE_VW WHERE DUPLICATE_PAYMENT_AMOUNT > 0
      ),
      grir AS (
        SELECT '7_GRIR_Clearing' AS OPPORTUNITY_AREA,
          NULL AS PCT_OF_SPEND,
          COALESCE(TOTAL_GRIR_BLNC, 0) AS AMOUNT,
          CONCAT(COALESCE(INVOICE_COUNT, 0), ' items in GR/IR') AS FINDING,
          'Clear GR/IR items; match receipts to invoices to reduce working capital' AS RECOMMENDED_ACTION
        FROM (SELECT * FROM PROCURE2PAY.INFORMATION_MART.GR_IR_OUTSTANDING_BALANCE_VW ORDER BY YEAR DESC, MONTH DESC LIMIT 1)
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
        SELECT * FROM PROCURE2PAY.INFORMATION_MART.FACT_ALL_SOURCES_VW
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
      FROM PROCURE2PAY.INFORMATION_MART.FACT_ALL_SOURCES_VW F
      LEFT JOIN PROCURE2PAY.INFORMATION_MART.DIM_VENDOR_VW V ON F.VENDOR_ID = V.VENDOR_ID
      WHERE UPPER(F.INVOICE_STATUS) NOT IN ('CANCELLED', 'REJECTED')
      GROUP BY F.VENDOR_ID, V.VENDOR_NAME ORDER BY TOTAL_SPEND DESC;

  - name: payment_performance
    question: "Show payment delays and cycle time issues"
    sql: |
      SELECT * FROM PROCURE2PAY.INFORMATION_MART.PAYMENT_PROCESSING_CYCLE_TIME_VW
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
        FROM PROCURE2PAY.INFORMATION_MART.FACT_ALL_SOURCES_VW
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
      FROM PROCURE2PAY.INFORMATION_MART.FACT_ALL_SOURCES_VW WHERE INVOICE_STATUS = 'Overdue'
      ORDER BY AGING_DAYS DESC, INVOICE_AMOUNT_LOCAL DESC LIMIT 50;

  - name: disputed_invoices
    question: "Show disputed invoices"
    sql: |
      SELECT DOCUMENT_NUMBER, VENDOR_ID, INVOICE_AMOUNT_LOCAL, AGING_DAYS, DUE_DATE, POSTING_DATE, PURCHASE_ORDER_REFERENCE
      FROM PROCURE2PAY.INFORMATION_MART.FACT_ALL_SOURCES_VW WHERE INVOICE_STATUS = 'Disputed'
      ORDER BY INVOICE_AMOUNT_LOCAL DESC;

  - name: open_vs_paid_summary
    question: "Summary of open vs paid invoices by status"
    sql: |
      SELECT INVOICE_STATUS, COUNT(*) AS INVOICE_COUNT, SUM(INVOICE_AMOUNT_LOCAL) AS TOTAL_AMOUNT
      FROM PROCURE2PAY.INFORMATION_MART.FACT_ALL_SOURCES_VW WHERE UPPER(INVOICE_STATUS) NOT IN ('CANCELLED','REJECTED')
      GROUP BY INVOICE_STATUS ORDER BY TOTAL_AMOUNT DESC;

  - name: spend_by_po_purpose
    question: "Spend by PO purpose or invoice type"
    sql: |
      SELECT COALESCE(PO_PURPOSE,'Unknown') AS PO_PURPOSE, COUNT(*) AS INVOICE_COUNT, SUM(INVOICE_AMOUNT_LOCAL) AS TOTAL_SPEND
      FROM PROCURE2PAY.INFORMATION_MART.FACT_ALL_SOURCES_VW WHERE UPPER(INVOICE_STATUS) NOT IN ('CANCELLED','REJECTED')
      GROUP BY PO_PURPOSE ORDER BY TOTAL_SPEND DESC;

  - name: ap_balance_trend
    question: "AP balance trend by year and month"
    sql: |
      SELECT YEAR, MONTH, AP_BALANCE, INVOICE_COUNT FROM PROCURE2PAY.INFORMATION_MART.ACCOUNTS_PAYABLE_BALANCE_VW
      ORDER BY YEAR DESC, MONTH DESC;

  - name: days_payable_outstanding
    question: "Days payable outstanding (DPO)"
    sql: |
      SELECT * FROM PROCURE2PAY.INFORMATION_MART.DAYS_PAYABLE_OUTSTANDING_VW ORDER BY YEAR DESC, MONTH DESC LIMIT 50;

  - name: full_payment_rate
    question: "Full payment rate percentage"
    sql: |
      SELECT * FROM PROCURE2PAY.INFORMATION_MART.FULL_PAYMENT_RATE_VW ORDER BY YEAR DESC, MONTH DESC LIMIT 24;

  - name: on_time_payment_rate
    question: "On-time payment rate"
    sql: |
      SELECT * FROM PROCURE2PAY.INFORMATION_MART.ON_TIME_PAYMENT_RATE_VW ORDER BY YEAR DESC, MONTH DESC LIMIT 24;

  - name: gr_ir_outstanding
    question: "GR/IR outstanding balance"
    sql: |
      SELECT * FROM PROCURE2PAY.INFORMATION_MART.GR_IR_OUTSTANDING_BALANCE_VW ORDER BY YEAR DESC, MONTH DESC LIMIT 24;

  - name: gr_ir_hotspots_clearing_plan
    question: "Show GR/IR outstanding balance by month and highlight which recent months have the highest GR/IR balance so we can prioritize clearing."
    sql: |
      SELECT
        YEAR,
        MONTH,
        INVOICE_COUNT,
        TOTAL_GRIR_BLNC
      FROM PROCURE2PAY.INFORMATION_MART.GR_IR_OUTSTANDING_BALANCE_VW
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
      FROM PROCURE2PAY.INFORMATION_MART.GR_IR_AGING_VW
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
      FROM PROCURE2PAY.INFORMATION_MART.GR_IR_AGING_VW
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
      FROM PROCURE2PAY.INFORMATION_MART.GR_IR_AGING_VW
      ORDER BY YEAR DESC, MONTH DESC, AGE_DAYS;

  - name: gr_ir_top_items_for_vendor_followup
    question: "Based on GR/IR aging and outstanding balances, draft vendor-facing follow-up templates we can use for high-priority GR/IR items, with realistic subject lines and concise bullet points."
    sql: |
      SELECT
        YEAR,
        MONTH,
        INVOICE_COUNT,
        TOTAL_GRIR_BLNC
      FROM PROCURE2PAY.INFORMATION_MART.GR_IR_OUTSTANDING_BALANCE_VW
      ORDER BY TOTAL_GRIR_BLNC DESC
      LIMIT 10;

  - name: late_payment_amount
    question: "Late payment amount"
    sql: |
      SELECT * FROM PROCURE2PAY.INFORMATION_MART.LATE_PAYMENT_AMOUNT_VW ORDER BY YEAR DESC, MONTH DESC LIMIT 24;

  - name: invoices_by_po
    question: "Invoices linked to purchase orders with PO amount"
    sql: |
      SELECT PURCHASE_ORDER_REFERENCE, PO_AMOUNT, PO_PURPOSE, COUNT(*) AS INVOICE_COUNT, SUM(INVOICE_AMOUNT_LOCAL) AS TOTAL_INVOICED
      FROM PROCURE2PAY.INFORMATION_MART.FACT_ALL_SOURCES_VW WHERE PURCHASE_ORDER_REFERENCE IS NOT NULL
      GROUP BY PURCHASE_ORDER_REFERENCE, PO_AMOUNT, PO_PURPOSE ORDER BY TOTAL_INVOICED DESC;

  - name: cash_flow_forecast
    question: "Forecast cash outflow for the next 7, 14, 30, 60, and 90 days"
    sql: "SELECT * FROM PROCURE2PAY.INFORMATION_MART.CASH_FLOW_FORECAST_VW"

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
        FROM PROCURE2PAY.INFORMATION_MART.FACT_ALL_SOURCES_VW
        WHERE UPPER(INVOICE_STATUS) NOT IN ('CANCELLED', 'REJECTED')
        GROUP BY VENDOR_ID
      ),
      category_history AS (
        SELECT
          COALESCE(PO_PURPOSE, 'Unknown') AS PO_PURPOSE,
          ROUND(SUM(CASE WHEN INVOICE_STATUS = 'Disputed' THEN 1 ELSE 0 END) * 100.0
                / NULLIF(COUNT(*), 0), 2) AS category_dispute_rate
        FROM PROCURE2PAY.INFORMATION_MART.FACT_ALL_SOURCES_VW
        WHERE UPPER(INVOICE_STATUS) NOT IN ('CANCELLED', 'REJECTED')
        GROUP BY PO_PURPOSE
      ),
      amount_stats AS (
        SELECT
          AVG(INVOICE_AMOUNT_LOCAL) AS avg_amount,
          STDDEV(INVOICE_AMOUNT_LOCAL) AS stddev_amount
        FROM PROCURE2PAY.INFORMATION_MART.FACT_ALL_SOURCES_VW
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
        FROM PROCURE2PAY.INFORMATION_MART.FACT_ALL_SOURCES_VW
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
        FROM PROCURE2PAY.INFORMATION_MART.FACT_ALL_SOURCES_VW f
        LEFT JOIN PROCURE2PAY.INFORMATION_MART.DIM_VENDOR_VW v ON f.VENDOR_ID = v.VENDOR_ID
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
        FROM PROCURE2PAY.INFORMATION_MART.FACT_ALL_SOURCES_VW
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
        FROM PROCURE2PAY.INFORMATION_MART.FACT_ALL_SOURCES_VW
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
        FROM PROCURE2PAY.INFORMATION_MART.INVOICE_STATUS_HISTORY_VW h1
        LEFT JOIN PROCURE2PAY.INFORMATION_MART.INVOICE_STATUS_HISTORY_VW h2
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
        FROM PROCURE2PAY.INFORMATION_MART.ON_TIME_PAYMENT_RATE_VW
        ORDER BY YEAR DESC, MONTH DESC
        LIMIT 6
      ),
      avg_late_rate AS (
        SELECT AVG(LATE_RATE_PCT) AS baseline_late_rate FROM payment_history
      ),
      cycle_time AS (
        SELECT AVG_PAYMENT_CYCLE_TIME_DAYS AS avg_cycle
        FROM PROCURE2PAY.INFORMATION_MART.PAYMENT_PROCESSING_CYCLE_TIME_VW
        ORDER BY YEAR DESC, MONTH DESC LIMIT 1
      ),
      vendor_late_history AS (
        SELECT
          f.VENDOR_ID,
          COUNT(*) AS total_paid,
          SUM(CASE WHEN f.AGING_DAYS > 0 AND f.INVOICE_STATUS = 'Paid' THEN 1 ELSE 0 END) AS late_count,
          ROUND(SUM(CASE WHEN f.AGING_DAYS > 0 AND f.INVOICE_STATUS = 'Paid' THEN 1 ELSE 0 END) * 100.0
                / NULLIF(COUNT(*), 0), 2) AS vendor_late_rate
        FROM PROCURE2PAY.INFORMATION_MART.FACT_ALL_SOURCES_VW f
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
        FROM PROCURE2PAY.INFORMATION_MART.FACT_ALL_SOURCES_VW f
        LEFT JOIN PROCURE2PAY.INFORMATION_MART.DIM_VENDOR_VW v ON f.VENDOR_ID = v.VENDOR_ID
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
        FROM PROCURE2PAY.INFORMATION_MART.FACT_ALL_SOURCES_VW f
        LEFT JOIN PROCURE2PAY.INFORMATION_MART.DIM_VENDOR_VW v ON f.VENDOR_ID = v.VENDOR_ID
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
        FROM PROCURE2PAY.INFORMATION_MART.LATE_PAYMENT_AMOUNT_VW l
        LEFT JOIN PROCURE2PAY.INFORMATION_MART.ON_TIME_PAYMENT_RATE_VW o
          ON l.YEAR = o.YEAR AND l.MONTH = o.MONTH
        LEFT JOIN PROCURE2PAY.INFORMATION_MART.PAYMENT_PROCESSING_CYCLE_TIME_VW p
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
        FROM PROCURE2PAY.INFORMATION_MART.PAYMENT_PROCESSING_CYCLE_TIME_VW
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
        FROM PROCURE2PAY.INFORMATION_MART.FACT_ALL_SOURCES_VW f
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
        FROM PROCURE2PAY.INFORMATION_MART.FACT_ALL_SOURCES_VW
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
        FROM PROCURE2PAY.INFORMATION_MART.FACT_ALL_SOURCES_VW
        WHERE UPPER(INVOICE_STATUS) NOT IN ('CANCELLED', 'REJECTED')
          AND DATE_TRUNC('month', POSTING_DATE) = DATE_TRUNC('month', CURRENT_DATE())
        GROUP BY PO_PURPOSE
      ),
      last_month AS (
        SELECT
          COALESCE(PO_PURPOSE, 'Unknown') AS CATEGORY,
          SUM(INVOICE_AMOUNT_LOCAL) AS LAST_MONTH_SPEND,
          COUNT(*) AS LAST_MONTH_COUNT
        FROM PROCURE2PAY.INFORMATION_MART.FACT_ALL_SOURCES_VW
        WHERE UPPER(INVOICE_STATUS) NOT IN ('CANCELLED', 'REJECTED')
          AND DATE_TRUNC('month', POSTING_DATE) = DATE_TRUNC('month', DATEADD('month', -1, CURRENT_DATE()))
        GROUP BY PO_PURPOSE
      ),
      ytd AS (
        SELECT
          COALESCE(PO_PURPOSE, 'Unknown') AS CATEGORY,
          SUM(INVOICE_AMOUNT_LOCAL) AS YTD_SPEND,
          COUNT(*) AS YTD_COUNT
        FROM PROCURE2PAY.INFORMATION_MART.FACT_ALL_SOURCES_VW
        WHERE UPPER(INVOICE_STATUS) NOT IN ('CANCELLED', 'REJECTED')
          AND DATE_TRUNC('year', POSTING_DATE) = DATE_TRUNC('year', CURRENT_DATE())
        GROUP BY PO_PURPOSE
      ),
      last_year_ytd AS (
        SELECT
          COALESCE(PO_PURPOSE, 'Unknown') AS CATEGORY,
          SUM(INVOICE_AMOUNT_LOCAL) AS LAST_YEAR_YTD_SPEND
        FROM PROCURE2PAY.INFORMATION_MART.FACT_ALL_SOURCES_VW
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
        FROM PROCURE2PAY.INFORMATION_MART.FACT_ALL_SOURCES_VW
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
        FROM PROCURE2PAY.INFORMATION_MART.FACT_ALL_SOURCES_VW
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
        FROM PROCURE2PAY.INFORMATION_MART.FACT_ALL_SOURCES_VW f
        LEFT JOIN PROCURE2PAY.INFORMATION_MART.DIM_VENDOR_VW v ON f.VENDOR_ID = v.VENDOR_ID
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
    sql: "SELECT * FROM PROCURE2PAY.INFORMATION_MART.EARLY_PAYMENT_CANDIDATES_VW"

  - name: payment_timing_recommendation
    question: "What is the optimal payment timing strategy for this week?"
    sql: "SELECT * FROM PROCURE2PAY.INFORMATION_MART.PAYMENT_TIMING_RECOMMENDATION_VW"

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
        FROM PROCURE2PAY.INFORMATION_MART.INVOICE_STATUS_HISTORY_VW
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

def adapt_semantic_model_for_athena(yaml_str: str) -> str:
    return yaml_str.replace("PROCURE2PAY.INFORMATION_MART.", f"{DATABASE}.")

FULL_SEMANTIC_MODEL_YAML = adapt_semantic_model_for_athena(RAW_SEMANTIC_MODEL_YAML)

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