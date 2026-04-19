# config.py
import os
from datetime import date, timedelta

# Athena configuration
DATABASE = "procure2pay"
ATHENA_REGION = "us-east-1"

# Bedrock configuration
BEDROCK_MODEL_ID = "amazon.nova-micro-v1:0"

# SQLite database path
DB_PATH = "procureiq.db"

# Logo URL
LOGO_URL = "https://th.bing.com/th/id/OIP.Vy1yFQtg8-D1SsAxcqqtSgHaE6?w=235&h=180&c=7&r=0&o=7&dpr=1.5&pid=1.7&rm=3"

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