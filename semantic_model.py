# semantic_model.py
import json
import re
from bedrock_client import ask_bedrock

# Paste the full RAW_SEMANTIC_MODEL_YAML here (same as before)...
# And the adapt_semantic_model_for_athena function, etc.
# Then define SYSTEM_PROMPT and DESCRIPTIVE_PROMPT_TEMPLATE.

# For production, you can copy the exact YAML from the previous working version.
# I will include a placeholder but you must replace with the actual YAML.

RAW_SEMANTIC_MODEL_YAML = """ ... """  # Paste the full YAML here

def adapt_semantic_model_for_athena(yaml_str: str) -> str:
    from config import DATABASE
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

def generate_sql(question: str) -> tuple:
    prompt = f"User question: {question}\n\nGenerate SQL query and explanation as JSON."
    response = ask_bedrock(prompt, SYSTEM_PROMPT)
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
