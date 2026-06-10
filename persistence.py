# persistence.py
import sqlite3
import uuid
import hashlib
import json
from datetime import datetime, timedelta
from typing import List, Dict
import streamlit as st
from config import DB_PATH
from utils import make_json_serializable

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS chat_sessions (
        session_id TEXT PRIMARY KEY, session_label TEXT, created_at TIMESTAMP, last_updated TIMESTAMP, user_name TEXT
    )''')
    try:
        c.execute("ALTER TABLE chat_sessions ADD COLUMN user_name TEXT")
    except sqlite3.OperationalError:
        pass
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

def get_current_user():
    return "user1"

def save_chat_message(session_id, turn_index, role, content, sql_used="", source=""):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''INSERT INTO chat_messages (session_id, turn_index, role, content, sql_used, source, timestamp)
                 VALUES (?, ?, ?, ?, ?, ?, ?)''',
              (session_id, turn_index, role, content, sql_used, source, datetime.now()))
    conn.commit()
    conn.close()

def save_chat_session(session_id: str, label: str = None):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    user = get_current_user()
    if label is None:
        label = f"Session {session_id[:8]}"
    c.execute('''INSERT OR REPLACE INTO chat_sessions (session_id, session_label, created_at, last_updated, user_name)
                 VALUES (?, ?, COALESCE((SELECT created_at FROM chat_sessions WHERE session_id=?), ?),
                         COALESCE((SELECT last_updated FROM chat_sessions WHERE session_id=?), ?), ?)''',
              (session_id, label, session_id, datetime.now(), session_id, datetime.now(), user))
    conn.commit()
    conn.close()

def update_session_timestamp(session_id: str):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('UPDATE chat_sessions SET last_updated = ? WHERE session_id = ?', (datetime.now(), session_id))
    conn.commit()
    conn.close()

def get_chat_sessions(limit: int = 20) -> List[Dict]:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''SELECT session_id, session_label, created_at, last_updated
                 FROM chat_sessions ORDER BY last_updated DESC LIMIT ?''', (limit,))
    rows = c.fetchall()
    conn.close()
    return [{"id": r[0], "label": r[1], "created": r[2], "last_updated": r[3]} for r in rows]

def load_session_messages(session_id: str) -> List[Dict]:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''SELECT role, content, sql_used, source, timestamp
                 FROM chat_messages WHERE session_id = ? ORDER BY turn_index, timestamp''', (session_id,))
    rows = c.fetchall()
    conn.close()
    messages = []
    for r in rows:
        messages.append({"role": r[0], "content": r[1], "sql_used": r[2], "source": r[3], "timestamp": datetime.fromisoformat(r[4]) if isinstance(r[4], str) else r[4]})
    return messages

def save_question(query, analysis_type):
    norm = query.lower().strip()
    user = get_current_user()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('INSERT INTO question_history (normalized_query, query_text, user_name, analysis_type, asked_at) VALUES (?, ?, ?, ?, ?)',
              (norm, query, user, analysis_type, datetime.now()))
    conn.commit()
    conn.close()

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
    try:
        response_json = json.dumps(serializable_response)
    except Exception as e:
        st.error(f"Failed to serialize response for caching: {e}")
        return
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''INSERT OR REPLACE INTO query_cache (query_hash, question, response_json, created_at, last_hit_at, hit_count)
                 VALUES (?, ?, ?, ?, ?, COALESCE((SELECT hit_count+1 FROM query_cache WHERE query_hash=?), 1))''',
              (q_hash, question, response_json, datetime.now(), datetime.now(), q_hash))
    conn.commit()
    conn.close()

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

def get_recent_conversation_context(limit: int = 20, max_age_days: int = 2) -> str:
    user = get_current_user()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    cutoff = datetime.now() - timedelta(days=max_age_days)
    c.execute('''
        SELECT m.role, m.content, m.timestamp
        FROM chat_messages m
        JOIN chat_sessions s ON m.session_id = s.session_id
        WHERE s.user_name = ? AND m.timestamp >= ?
        ORDER BY m.timestamp DESC
        LIMIT ?
    ''', (user, cutoff, limit))
    rows = c.fetchall()
    conn.close()
    if not rows:
        return ""
    rows.reverse()
    context_parts = []
    for role, content, ts in rows:
        if role == "user":
            context_parts.append(f"User: {content}")
        else:
            context_parts.append(f"Assistant: {content}")
    return "Here is the conversation history from the last 2 days (most recent context):\n\n" + "\n\n".join(context_parts) + "\n\nNow answer the following new question taking into account the history:\n"
