# athena_client.py
import streamlit as st
import boto3
import awswrangler as wr
import pandas as pd
from decimal import Decimal
from config import DATABASE, ATHENA_REGION

@st.cache_resource
def get_aws_session():
    return boto3.Session()

@st.cache_data(ttl=300, show_spinner=False)
def run_query(sql: str) -> pd.DataFrame:
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