# quick_analysis.py
import streamlit as st
import pandas as pd
from datetime import date, timedelta
from athena_client import run_query
from utils import sql_date, safe_number, abbr_currency
from config import DATABASE

@st.cache_data(ttl=600)
def run_quick_analysis(key: str) -> dict:
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