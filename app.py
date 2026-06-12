# ================================
# P2P Analytics + Genie (YASH-branded, clean UI, Altair, FACT-safe filters, Box Containment)
# ================================

import html
import json
import logging
import math
import os
import time
import hashlib
import uuid
import yaml
import streamlit as st
import pandas as pd
import numpy as np
from collections import OrderedDict
from datetime import date, datetime, timedelta
from difflib import SequenceMatcher
from typing import Optional, Dict, Any, List
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session
import altair as alt
import urllib.parse


# ══════════════════════════════════════════════════════════════════════════════
# ❶  UI CONFIGURATION  (single source of truth for all design tokens)
# ══════════════════════════════════════════════════════════════════════════════
# To change any colour, size, or chat height → edit the values below.
# The CSS is generated from these tokens inside _build_ui_css().
# Never hard-code colours elsewhere in the app; reference these constants.

class _UIColors:
    BRAND            = "#1459d2"
    BRAND_2          = "#1e88e5"
    BRAND_HOVER      = "#0046CC"
    SEND_BTN         = "#007BFF"
    SUCCESS          = "#118d57"
    DANGER           = "#d32f2f"
    WARNING          = "#f59e0b"
    MUTED            = "#64748b"
    BG               = "#f7f8fb"
    PANEL            = "#ffffff"
    TEXT             = "#0f172a"
    TEXT_SUBTLE      = "#475569"
    DIVIDER          = "#e5e7eb"
    GENIE_ICON_BG    = "#5046e5"
    GENIE_LAVENDER   = "#e8e4f7"
    EXPLORE_BTN      = "#7c3aed"

class _UILayout:
    RADIUS           = "14px"
    RADIUS_SM        = "12px"
    MAX_WIDTH        = "1180px"
    SHADOW_1         = "0 10px 30px rgba(2,8,23,.06)"
    SHADOW_2         = "0 2px 10px rgba(2,8,23,.06)"
    CHAT_SCROLL_HEIGHT = 560   # px — increase for taller chat window

class _UIGenie:
    CACHE_MAX_SIZE       = 200   # in-memory LRU entries
    SHORT_TERM_MAX_MSGS  = 40    # max messages in session_state
    SIMILARITY_THRESHOLD = 0.60  # semantic cache hit threshold (60 %)
    SHOW_TYPING          = True
    AUTO_SCROLL          = True

# Convenience namespace  (UI.Colors.BRAND etc.)
class _UI:
    Colors = _UIColors
    Layout = _UILayout
    Genie  = _UIGenie
UI = _UI()


def _build_ui_css() -> str:
    c = UI.Colors
    l = UI.Layout
    return f"""
<style>
:root {{
  --bg:{c.BG}; --panel:{c.PANEL}; --text:{c.TEXT}; --text-subtle:{c.TEXT_SUBTLE};
  --brand:{c.BRAND}; --brand-2:{c.BRAND_2}; --success:{c.SUCCESS};
  --danger:{c.DANGER}; --warning:{c.WARNING}; --muted:{c.MUTED};
  --divider:{c.DIVIDER}; --radius:{l.RADIUS}; --radius-sm:{l.RADIUS_SM};
  --shadow-1:{l.SHADOW_1}; --shadow-2:{l.SHADOW_2};
  --kpi-title:12px; --kpi-value:28px; --tab-font-size:18px; --tab-font-weight:900;
}}
html,body,[class^="css"]{{background:var(--bg);color:var(--text);font-family:Inter,-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto;}}
.block-container{{padding-top:8px;max-width:{l.MAX_WIDTH};margin-left:auto;margin-right:auto;}}
@media(min-width:1400px){{.block-container{{max-width:1320px;}}.p2p-header{{max-width:1320px;}}}}
@media(min-width:1600px){{.block-container{{max-width:1480px;}}.p2p-header{{max-width:1480px;}}}}
/* Branding bar */
.brandbar{{position:sticky;top:0;z-index:9999;background:linear-gradient(180deg,#ffffff 0%,#f8fbff 100%);border-bottom:1px solid var(--divider);padding:8px 0;box-shadow:var(--shadow-1);}}
.brandrow{{display:flex;align-items:center;justify-content:space-between;}}
.yash-header-logo{{height:120px!important;max-height:120px!important;width:auto!important;object-fit:contain!important;display:block;}}
/* Tabs */
.stTabs [data-baseweb="tab-list"]{{gap:4px;border-bottom:2px solid var(--divider);}}
.stTabs [data-baseweb="tab"]{{background:transparent;color:var(--text-subtle);border-radius:10px 10px 0 0;padding:10px 18px;}}
.stTabs [data-baseweb="tab"] *,.stTabs button[role="tab"] *{{font-size:var(--tab-font-size)!important;font-weight:var(--tab-font-weight)!important;line-height:1.1!important;}}
.stTabs [aria-selected="true"]{{color:var(--text);border-bottom:3px solid var(--brand)!important;}}
/* KPI */
.kpi{{background:#fff;border:1px solid #e6e8ee;border-radius:var(--radius-sm);padding:12px 14px 10px;box-shadow:var(--shadow-2);width:100%;min-height:98px;}}
.kpi .title{{font-size:var(--kpi-title);color:var(--muted);letter-spacing:.3px;font-weight:800;}}
.kpi .value{{font-size:var(--kpi-value);font-weight:900;margin-top:6px;display:flex;align-items:baseline;gap:8px;}}
.kpi .delta{{margin-top:4px;font-weight:900;display:flex;align-items:center;gap:6px;letter-spacing:.2px;}}
.kpi .delta.up{{color:var(--success);}} .kpi .delta.down{{color:var(--danger);}}
/* Badges */
.badge{{font-size:11px;padding:2px 8px;border-radius:999px;border:1px solid #e5e7eb;color:var(--text-subtle);background:#fff;font-weight:800;}}
.badge.high{{background:#fde7e9;color:#b42318;border-color:#f3b4b8;}}
.badge.med{{background:#fff4e5;color:#b54708;border-color:#f7cf97;}}
.badge.low{{background:#ecfdf3;color:#067647;border-color:#a6f0c6;}}
/* Banner */
.banner{{background:linear-gradient(135deg,#e9f2ff 0%,#f2f8ff 100%);border:1px solid #d9e6ff;border-radius:var(--radius);padding:16px;margin:10px 0 14px 0;box-shadow:var(--shadow-1);display:flex;gap:12px;align-items:flex-start;}}
/* Preset buttons */
.preset-btn{{padding:8px 14px;border-radius:999px;font-weight:800;color:var(--text-subtle);background:transparent;border:1px solid transparent;}}
.preset-btn.active{{background:var(--brand);color:#fff;box-shadow:0 6px 18px rgba(20,89,210,.12);border-color:rgba(16,66,168,.12);}}
/* Generic buttons */
div.stButton>button:not([data-testid^="baseButton-na_btn_"]):not([kind="primary"]),
div[data-testid="stButton"] button:not([data-testid^="baseButton-na_btn_"]):not([kind="primary"]){{border-radius:999px!important;padding:.55rem 1.3rem!important;border:none!important;font-weight:600!important;transition:all .18s ease!important;background:#e5e7eb!important;color:#111827!important;}}
div.stButton>button:not([data-testid^="baseButton-na_btn_"]):hover,
div[data-testid="stButton"] button:not([data-testid^="baseButton-na_btn_"]):hover{{background:#2563eb!important;color:white!important;transform:translateY(-1px)!important;}}
div[data-testid="stButton"] button[kind="primary"],div.stButton>button[kind="primary"]{{background:#2563eb!important;color:white!important;border:none!important;}}
/* NA buttons */
button[data-testid^="baseButton-na_btn_"]{{border-radius:999px!important;padding:.55rem 1.3rem!important;border:none!important;font-weight:600!important;background:#e5e7eb!important;color:#111827!important;}}
button[data-testid^="baseButton-na_btn_"]:hover{{background:#2563eb!important;color:white!important;}}
/* Invoice page buttons */
.st-key-btn_inv_search button,.st-key-inv_next button{{background-color:#2563EB!important;border:1px solid #E2E8F0!important;color:#fff!important;}}
.st-key-btn_inv_reset button{{background-color:#FEE2E2!important;color:#991B1B!important;border:1px solid #FCA5A5!important;}}
.st-key-btn_inv_reset button:hover{{background-color:#FCA5A5!important;color:#7F1D1D!important;}}
.st-key-btn_download_csv button{{background-color:#2563EB!important;color:#fff!important;border:none!important;font-weight:600!important;font-size:13px!important;border-radius:8px!important;}}
.st-key-inv_prev button{{background-color:#F8FAFC!important;border:1px solid #E2E8F0!important;color:#64748B!important;}}
.st-key-inv_prev button:hover{{background-color:{c.BRAND_HOVER}!important;color:#fff!important;}}
/* Send button */
[data-testid="stButton"] button[kind="primary"]{{background:{c.SEND_BTN}!important;border:none!important;}}
/* Genie tiles */
form:has(.genie-tile-card){{margin:0;padding:0;border:none!important;box-shadow:none!important;background:transparent;display:flex;flex-direction:column;gap:0;}}
form:has(.genie-tile-card) .genie-tile-card{{border-bottom-left-radius:0!important;border-bottom-right-radius:0!important;border-bottom:0!important;}}
form:has(.genie-tile-card) [data-testid="stFormSubmitButton"]{{margin:0!important;padding:0!important;}}
form:has(.genie-tile-card) [data-testid="stFormSubmitButton"]>button{{width:100%!important;border-radius:0 0 14px 14px!important;border:1.5px solid {c.DIVIDER}!important;border-top:0!important;background:#3b38ff!important;color:#fff!important;font-weight:800!important;font-size:14px!important;padding:10px 12px!important;box-shadow:0 2px 8px rgba(59,56,255,.12)!important;cursor:pointer!important;}}
form:has(.genie-tile-card) input[type="checkbox"],form:has(.genie-tile-card) [role="checkbox"],
form:has(.genie-tile-card) .stCheckbox,form:has(.genie-tile-card) [data-testid="stCheckbox"],
form:has(.genie-tile-card) label{{display:none!important;visibility:hidden!important;}}
/* Genie column alignment */
[data-testid="stHorizontalBlock"]:has(.genie-left-col-top){{align-items:flex-start!important;}}
[data-testid="stHorizontalBlock"]:has(.genie-left-col-top) [data-testid="column"]{{align-items:flex-start!important;align-self:flex-start!important;}}
[data-testid="stHorizontalBlock"]:has(.genie-left-col-top) [data-testid="column"]>div{{align-items:flex-start!important;padding-top:0!important;margin-top:0!important;}}
/* ── LEFT COLUMN BUTTONS (saved insights, faqs) — no pill radius, no overlap ── */
[data-testid="stExpander"] div.stButton>button,
[data-testid="stExpander"] div[data-testid="stButton"] button{{
  border-radius:8px!important;padding:.4rem .8rem!important;
  text-align:left!important;justify-content:flex-start!important;
  white-space:normal!important;word-break:break-word!important;
  height:auto!important;min-height:36px!important;line-height:1.4!important;
  font-size:13px!important;
}}
/* ── AI Assistant header buttons — keep compact, no wrap ── */
[data-testid="stButton"] button[data-testid^="baseButton-btn_genie_"]{{
  border-radius:999px!important;padding:.3rem .6rem!important;
  font-size:12px!important;white-space:nowrap!important;
  overflow:hidden!important;text-overflow:ellipsis!important;
}}
/* ── CHAT SCROLL ────────────────────────────────────────────────────────── */
/* bubble styles */
.g-user{{display:flex;justify-content:flex-end;margin:6px 0;}}
.g-user-inner{{max-width:72%;background:{c.BRAND};color:#fff;padding:10px 14px;border-radius:16px;border-bottom-right-radius:4px;font-size:14px;line-height:1.5;}}
.g-user-lbl{{font-size:11px;font-weight:700;opacity:.8;margin-bottom:3px;}}
.g-ai{{display:flex;justify-content:flex-start;margin:6px 0;}}
.g-ai-inner{{max-width:72%;background:#f1f5f9;color:#0f172a;padding:10px 14px;border-radius:16px;border-bottom-left-radius:4px;font-size:14px;line-height:1.5;}}
.g-ai-lbl{{font-size:11px;font-weight:700;color:#64748b;margin-bottom:3px;}}
.g-card{{background:#f8fafc;border:1px solid #e2e8f0;border-radius:12px;padding:16px;margin:4px 0 18px 0;}}
/* cache-hit badge */
.cache-badge{{display:inline-flex;align-items:center;gap:4px;background:#eff6ff;color:#1d4ed8;border:1px solid #bfdbfe;border-radius:999px;font-size:11px;font-weight:700;padding:2px 9px;margin-bottom:4px;}}
/* typing indicator */
.typing-indicator{{display:flex;gap:5px;align-items:center;padding:10px 14px;background:#f1f5f9;border-radius:16px 16px 16px 4px;width:fit-content;}}
.typing-dot{{width:7px;height:7px;background:#94a3b8;border-radius:50%;animation:typingBounce 1.2s infinite;}}
.typing-dot:nth-child(2){{animation-delay:.2s;}} .typing-dot:nth-child(3){{animation-delay:.4s;}}
@keyframes typingBounce{{0%,60%,100%{{transform:translateY(0);}}30%{{transform:translateY(-6px);}}}}
/* resume-session banner */
.resume-banner{{background:#eff6ff;border:1.5px solid #bfdbfe;border-radius:14px;padding:18px 20px 10px;margin:16px 8px 12px;}}
/* Prescriptive */
.prescriptive-content,.prescriptive-content *{{font-family:inherit!important;font-size:14px!important;line-height:1.6!important;color:{c.TEXT}!important;font-weight:inherit!important;}}
.prescriptive-content strong,.prescriptive-content b{{font-weight:700!important;color:{c.TEXT}!important;}}
.prescriptive-content{{word-wrap:break-word;overflow-wrap:break-word;max-width:100%;box-sizing:border-box;}}
/* Empties / misc */
.empty{{background:#f8fafc;border:1px dashed #d7dce5;border-radius:12px;padding:16px;color:var(--muted);}}
.soft-note{{background:#fff9db;border:1px solid #ffe08a;border-radius:12px;padding:10px 12px;color:#8b6b00;}}
.empty-analysis-box{{border:2px dashed #e2e8f0;border-radius:12px;padding:40px 20px;text-align:center;margin-top:20px;}}
/* QA grid */
.qa-grid{{display:grid;grid-template-columns:repeat(4,1fr);gap:16px;margin:20px 0;}}
.qa-card{{background:#fff;border:1.5px solid #e6e8ee;border-radius:16px;padding:24px 20px;cursor:pointer;transition:all .3s ease;box-shadow:0 2px 8px rgba(2,8,23,.04);}}
.qa-card:hover{{border-color:{c.EXPLORE_BTN};box-shadow:0 8px 24px rgba(124,58,237,.15);transform:translateY(-2px);}}
.qa-card.selected{{background:linear-gradient(135deg,#f5f3ff 0%,#ede9fe 100%);border-color:{c.EXPLORE_BTN};}}
.qa-icon{{width:48px;height:48px;border-radius:12px;display:flex;align-items:center;justify-content:center;font-size:24px;margin-bottom:16px;color:#fff;background:linear-gradient(135deg,{c.EXPLORE_BTN} 0%,#9333ea 100%);}}
.qa-title{{font-size:18px;font-weight:800;color:{c.TEXT};margin-bottom:8px;letter-spacing:.2px;}}
.qa-desc{{font-size:14px;color:#64748b;line-height:1.5;}}
/* Genie sidebar / AI panel */
.genie-sidebar{{background:#fff;border:1.5px solid #e6e8ee;border-radius:16px;padding:20px;min-height:400px;}}
.sidebar-item{{padding:10px 12px;border-radius:8px;cursor:pointer;transition:all .2s ease;margin-bottom:6px;font-size:14px;color:#475569;}}
.sidebar-item:hover{{background:#f8fafc;}} .sidebar-item.active{{background:#ede9fe;color:{c.EXPLORE_BTN};font-weight:700;}}
.ai-panel{{background:#fff;border:1.5px solid #e6e8ee;border-radius:16px;padding:24px;box-shadow:0 2px 8px rgba(2,8,23,.04);min-height:400px;}}
.explore-btn{{background:{c.EXPLORE_BTN};color:#fff;border:none;border-radius:10px;padding:14px 20px;font-size:14px;font-weight:800;cursor:pointer;transition:all .2s ease;box-shadow:0 4px 12px rgba(124,58,237,.2);}}
.explore-btn:hover{{background:#6d28d9;transform:translateY(-1px);}}
.chat-input-container{{display:flex;gap:8px;margin-top:20px;padding-top:16px;border-top:1px solid #e6e8ee;}}
/* Invoice KPI */
.inv-kpi-row{{display:grid;grid-template-columns:repeat(4,1fr);gap:20px;width:100%;margin-bottom:24px;}}
.inv-kpi-card{{padding:20px;border-radius:12px;display:flex;align-items:center;gap:16px;min-height:100px;transition:box-shadow .2s ease;}}
.inv-card-total{{background:#E8F1FD;border:1px solid #EDE9FE;}}
.inv-card-pending{{background:#FFFBEB;border:1px solid #FDE68A;}}
.inv-card-pending .icon-box{{background:#FFEDD5;color:#C2410C;}} .inv-card-pending .label{{color:#9A3412;font-size:13px;font-weight:600;}} .inv-card-pending .value{{color:#7C2D12;}}
.inv-card-blocked{{background:#FEF2F2;border:1px solid #FEE2E2;}}
.inv-card-blocked .icon-box{{background:#FEE2E2;color:#B91C1C;}} .inv-card-blocked .label{{color:#991B1B;font-size:13px;font-weight:600;}} .inv-card-blocked .value{{color:#7F1D1D;}}
.inv-card-overdue{{background:#F5F3FF;border:1px solid #EDE9FE;}}
.inv-card-overdue .icon-box{{background:#EDE9FE;color:#6D28D9;}} .inv-card-overdue .label{{color:#5B21B6;font-size:13px;font-weight:600;}} .inv-card-overdue .value{{color:#4C1D95;}}
.icon-box{{width:44px;height:44px;border-radius:10px;display:flex;align-items:center;justify-content:center;font-size:20px;flex-shrink:0;}}
.inv-kpi-content .value{{font-size:26px;font-weight:800;line-height:1;margin-bottom:4px;}}
/* NA cards */
[class*="st-key-na_bg_due"]{{background:#eff6ff!important;border:1px solid #bfdbfe!important;border-radius:12px!important;box-shadow:0 2px 8px rgba(0,0,0,.05)!important;}}
[class*="st-key-na_bg_overdue"]{{background:#fef2f2!important;border:1px solid #fecaca!important;border-radius:12px!important;box-shadow:0 2px 8px rgba(0,0,0,.05)!important;}}
[class*="st-key-na_bg_disputed"]{{background:#fffbeb!important;border:1px solid #fde68a!important;border-radius:12px!important;box-shadow:0 2px 8px rgba(0,0,0,.05)!important;}}
[class*="st-key-na_bg_other"]{{background:#f9fafb!important;border:1px solid #e5e7eb!important;border-radius:12px!important;box-shadow:0 2px 8px rgba(0,0,0,.05)!important;}}
.na-list{{display:flex;flex-direction:column;gap:10px;}}
.na-item{{background:#fff;border:1px solid #e6e8ee;border-radius:12px;padding:8px 10px;box-shadow:0 2px 10px rgba(2,8,23,.05);display:flex;justify-content:space-between;align-items:flex-start;gap:8px;width:100%;min-height:92px;box-sizing:border-box;overflow:hidden;}}
.na-item .na-left{{flex:1;min-width:0;overflow:hidden;}}
.na-left{{display:flex;flex-direction:column;align-items:flex-start;gap:3px;}}
.na-ref{{font-weight:900;letter-spacing:.2px;font-size:14px;}}
.na-meta{{display:flex;gap:10px;color:#64748b;font-size:12px;}}
.tag{{font-size:11px;padding:2px 8px;border-radius:999px;border:1px solid #e5e7eb;background:#fff;color:#475569;font-weight:800;}}
.tag.overdue{{background:#fde7e9;color:#b42318;border-color:#f3b4b8;}}
.tag.unpaid{{background:#fff4e5;color:#b54708;border-color:#f7cf97;}}
button[data-testid^="baseButton-na_card_"]{{background:transparent!important;border:none!important;box-shadow:none!important;color:#2563eb!important;font-weight:500!important;font-size:13px!important;padding:4px 0 0!important;margin-top:2px!important;cursor:pointer!important;}}
button[data-testid^="baseButton-na_card_"]:hover{{color:#1d4ed8!important;text-decoration:underline!important;}}
/* Section cards */
.section-card{{background:#FFFFFF;border:1px solid #E2E8F0;border-radius:12px;padding:24px;margin-bottom:24px;box-shadow:0 1px 3px rgba(0,0,0,.02);}}
.inv-section-wrapper{{border:1px solid #E5E7EB;border-radius:16px;padding:18px;margin-bottom:22px;background:#fff;}}
.inv-section-wrapper::before{{content:"";display:block;height:1px;background:linear-gradient(to right,transparent,#E5E7EB,transparent);margin-bottom:16px;}}
/* Panel */
.panel-wrap{{width:100%;position:relative;padding:14px;box-sizing:border-box;}}
.panel-wrap .stAltairChart,.panel-wrap .vega-embed,.panel-wrap canvas{{max-width:100%!important;}}
.panel-wrap *{{box-sizing:border-box;}} .panel-wrap>.stMarkdown,.panel-wrap>div{{max-width:100%;}}
[data-testid="stContainer"]>div[role="group"]{{border-radius:14px;}}
[data-testid="stDataFrame"]{{max-width:100%;overflow:hidden;}}
/* Hide Chrome + Sidebar */
#MainMenu,header,footer{{visibility:hidden!important;}}
section[data-testid="stSidebar"]{{display:none!important;}}
.brandbar{{z-index:9999!important;}}
.below-header-spacer{{height:24px;}}
.stColumns{{gap:12px!important;}} .stContainer{{padding:10px 0!important;}}
.kpi{{min-height:98px;padding:12px;}} .p2p-header{{max-width:{l.MAX_WIDTH};margin-left:auto;margin-right:auto;}}
.ctrl-label{{color:var(--text-subtle);font-size:12px;font-weight:700;margin-bottom:4px;}}
#genie-faqs .stButton>button{{justify-content:flex-start;text-align:left;padding-left:28px;white-space:normal;position:relative;}}
#genie-faqs .stButton>button::before{{content:"•";position:absolute;left:12px;top:50%;transform:translateY(-50%);}}
/* BG colour picker pill */
.theme-anchor{{position:fixed;bottom:20px;right:25px;z-index:1000000;display:flex;align-items:center;justify-content:center;width:44px;height:44px;border-radius:9999px;border:1px solid #E5E7EB;box-shadow:0 4px 10px rgba(15,23,42,.10);font-size:11px;font-weight:600;color:#111827;font-family:system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;cursor:pointer;}}
div[data-testid="stColorPicker"]{{position:fixed!important;bottom:20px!important;right:25px!important;width:44px!important;height:44px!important;z-index:1000001!important;opacity:0!important;}}
div[data-testid="stColorPicker"] *{{width:100%!important;height:100%!important;}}
div[data-testid="stColorPicker"] label{{display:none!important;}}
</style>"""


def _build_autoscroll_js() -> str:
    return """<script>
(function(){
    function scrollChat(){
        var anchor=document.getElementById('genie-bottom');
        if(!anchor){setTimeout(scrollChat,150);return;}
        var el=anchor.parentElement;
        while(el){
            var ov=window.getComputedStyle(el).overflowY;
            if(ov==='auto'||ov==='scroll'){el.scrollTop=el.scrollHeight;return;}
            el=el.parentElement;
        }
        anchor.scrollIntoView({behaviour:'smooth',block:'end'});
    }
    scrollChat();
    setTimeout(scrollChat,400);
    setTimeout(scrollChat,900);
})();
</script>"""


def inject_ui():
    """Inject CSS tokens + auto-scroll JS. Call once after set_page_config."""
    st.markdown(_build_ui_css(), unsafe_allow_html=True)
    if UI.Genie.AUTO_SCROLL:
        st.markdown(_build_autoscroll_js(), unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# ❷  GENIE QUERY CACHE  (in-memory LRU + Snowflake persistence)
# ══════════════════════════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════════════════════════
# ❷  GENIE QUERY CACHE  (in-memory LRU + Snowflake persistence)
# ══════════════════════════════════════════════════════════════════════════════

class GenieQueryCache:
    """LRU cache with exact + semantic similarity matching + Snowflake persistence.

    Flow per query:
      1. Exact in-memory hash hit  → return immediately (microseconds)
      2. Exact Snowflake DB hit    → load into memory, return
      3. Semantic similarity hit   → return closest match ≥ threshold
      4. Miss                      → caller runs Cortex, then calls cache.set()
    """

    TABLE_SUFFIX = "GENIE_QUERY_CACHE"

    def __init__(self, session, db: str, schema: str,
                 max_size: int = 200, ttl_seconds: int = 86400,
                 similarity_threshold: float = 0.60):
        self.session   = session
        self.table     = f"{db}.{schema}.{self.TABLE_SUFFIX}"
        self.max_size  = max_size
        self.ttl       = ttl_seconds
        self.threshold = similarity_threshold
        self._mem: Dict[str, Any] = {}
        self._order: List[str]    = []
        self._table_ok            = False
        self._user                = "UNKNOWN"
        self.last_error: str      = ""   # surfaced in UI for debugging

        if session:
            try:
                self._user = session.sql("SELECT CURRENT_USER()").collect()[0][0] or "UNKNOWN"
            except Exception:
                pass
            self._init_table()
            if self._table_ok:
                self._purge_stubs()
                self._warm_from_db()

    # ── table bootstrap ──────────────────────────────────────────────────────
    def _init_table(self):
        try:
            self.session.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.table} (
                    QUERY_HASH    VARCHAR(32)   NOT NULL PRIMARY KEY,
                    QUESTION      VARCHAR(4000) NOT NULL,
                    RESPONSE_JSON VARIANT,
                    CREATED_AT    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                    LAST_HIT_AT   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                    HIT_COUNT     INT           DEFAULT 0,
                    USER_NAME     VARCHAR(256)
                )
            """).collect()
            self._table_ok = True
        except Exception as e1:
            try:
                self.session.sql(f"SELECT COUNT(*) FROM {self.table} LIMIT 1").collect()
                self._table_ok = True
            except Exception as e2:
                self._table_ok = False
                self.last_error = f"Table init failed: {e1} | {e2}"

    def _purge_stubs(self):
        """Delete old stub rows (gen_ok/layout/source only) — they have no real content."""
        try:
            self.session.sql(f"""
                DELETE FROM {self.table}
                WHERE TO_JSON(RESPONSE_JSON) LIKE '%gen_ok%'
                  AND TO_JSON(RESPONSE_JSON) NOT LIKE '%request_id%'
                  AND TO_JSON(RESPONSE_JSON) NOT LIKE '%"message"%'
                  AND TO_JSON(RESPONSE_JSON) NOT LIKE '%"metrics"%'
            """).collect()
        except Exception:
            pass

    def _warm_from_db(self):
        """Pre-warm in-memory cache from recent DB rows."""
        try:
            df = self.session.sql(f"""
                SELECT QUERY_HASH, QUESTION, RESPONSE_JSON
                FROM {self.table}
                WHERE LAST_HIT_AT >= DATEADD('day', -7, CURRENT_TIMESTAMP())
                  AND RESPONSE_JSON IS NOT NULL
                ORDER BY LAST_HIT_AT DESC LIMIT 100
            """).to_pandas()
            for _, row in df.iterrows():
                h   = str(row["QUERY_HASH"])
                raw = row["RESPONSE_JSON"]
                resp = self._parse_variant(raw)
                if resp and self._is_real(resp):
                    self._mem[h] = {"response": resp,
                                    "question": str(row["QUESTION"]),
                                    "ts": time.time()}
                    self._order.append(h)
        except Exception:
            pass

    # ── helpers ──────────────────────────────────────────────────────────────
    @staticmethod
    def _hash(q: str) -> str:
        return hashlib.md5(q.lower().strip().encode()).hexdigest()

    @staticmethod
    def _sim(a: str, b: str) -> float:
        return SequenceMatcher(None, a.lower().strip(), b.lower().strip()).ratio()

    @staticmethod
    def _parse_variant(raw) -> Optional[Dict]:
        """Parse Snowflake VARIANT column — may come back as str, dict, or None."""
        if raw is None:
            return None
        if isinstance(raw, dict):
            return raw
        if isinstance(raw, str):
            try:
                return json.loads(raw)
            except Exception:
                return None
        return None

    @staticmethod
    def _is_real(r: dict) -> bool:
        """True if response has actual content (not a stub)."""
        if not r or not isinstance(r, dict):
            return False
        # Stub check: only stub keys present
        if set(r.keys()) <= {"layout", "source", "gen_ok", "cache_fetch_time_ms"}:
            return False
        has_message = bool(r.get("message", {}).get("content"))
        has_metrics = bool(r.get("metrics"))
        has_layout  = r.get("layout") == "quick"
        return has_message or has_metrics or has_layout

    @staticmethod
    def _to_json_safe(response: Dict) -> str:
        """Serialise response dict to a JSON string, stripping non-serialisable objects.

        Key rules:
        - pd.DataFrame values  → omitted entirely (re-queried at render time)
        - numpy scalars        → converted via .item()
        - pd.Timestamp         → ISO 8601 string
        - everything else      → str() fallback
        The resulting string uses $$ quoting to avoid single-quote escaping issues.
        """
        import math as _math

        def _clean(obj, depth=0):
            if depth > 10:
                return None
            if obj is None:
                return None
            if isinstance(obj, pd.DataFrame):
                return None           # DataFrames are never stored — always re-queried
            if isinstance(obj, pd.Timestamp):
                return obj.isoformat()
            # numpy / pandas scalar types
            if hasattr(obj, 'item') and callable(getattr(obj, 'item', None)):
                try:
                    v = obj.item()
                    if isinstance(v, float) and (_math.isnan(v) or _math.isinf(v)):
                        return None
                    return v
                except Exception:
                    return None
            if isinstance(obj, float):
                if _math.isnan(obj) or _math.isinf(obj):
                    return None
                return obj
            if isinstance(obj, (bool, int, str)):
                return obj
            if isinstance(obj, dict):
                out = {}
                for k, v in obj.items():
                    if isinstance(v, pd.DataFrame):
                        continue   # skip DataFrame values completely
                    cv = _clean(v, depth + 1)
                    if cv is not None:
                        out[str(k)] = cv
                return out
            if isinstance(obj, (list, tuple)):
                return [x for x in (_clean(i, depth + 1) for i in obj) if x is not None]
            try:
                s = str(obj)
                return s[:1000]
            except Exception:
                return None

        cleaned = _clean(response)
        return json.dumps(cleaned, ensure_ascii=True, default=str)

    def _evict_if_full(self, h: str):
        if len(self._mem) >= self.max_size and h not in self._mem:
            old = self._order.pop(0)
            self._mem.pop(old, None)

    # ── get ──────────────────────────────────────────────────────────────────
    def get(self, question: str) -> Optional[Dict]:
        """Return cached response or None. Checks memory → DB exact → DB semantic."""
        h = self._hash(question)

        # 1. Exact in-memory
        entry = self._mem.get(h)
        if entry:
            if time.time() - entry["ts"] < self.ttl:
                # Move to end (LRU)
                if h in self._order:
                    self._order.remove(h)
                self._order.append(h)
                return entry["response"]
            else:
                del self._mem[h]  # expired

        # 2. Exact DB hit
        if self._table_ok:
            try:
                df = self.session.sql(f"""
                    SELECT RESPONSE_JSON FROM {self.table}
                    WHERE QUERY_HASH = '{h}'
                    LIMIT 1
                """).to_pandas()
                if not df.empty:
                    resp = self._parse_variant(df.at[0, "RESPONSE_JSON"])
                    if resp and self._is_real(resp):
                        self._evict_if_full(h)
                        self._mem[h] = {"response": resp, "question": question, "ts": time.time()}
                        self._order.append(h)
                        self._bump_hit(h)
                        return resp
            except Exception as e:
                self.last_error = f"Cache get (DB) failed: {e}"

        # 3. Semantic similarity — in-memory
        best_score, best_resp = 0.0, None
        now = time.time()
        for entry in list(self._mem.values()):
            if now - entry["ts"] > self.ttl:
                continue
            s = self._sim(question, entry["question"])
            if s > best_score and s >= self.threshold and self._is_real(entry["response"]):
                best_score, best_resp = s, entry["response"]

        # 4. Semantic similarity — DB
        if best_resp is None and self._table_ok:
            try:
                df = self.session.sql(f"""
                    SELECT QUESTION, RESPONSE_JSON FROM {self.table}
                    WHERE RESPONSE_JSON IS NOT NULL
                    ORDER BY LAST_HIT_AT DESC LIMIT 100
                """).to_pandas()
                for _, row in df.iterrows():
                    s = self._sim(question, str(row["QUESTION"]))
                    if s > best_score and s >= self.threshold:
                        r = self._parse_variant(row["RESPONSE_JSON"])
                        if r and self._is_real(r):
                            best_score, best_resp = s, r
            except Exception:
                pass

        if best_resp:
            self._evict_if_full(h)
            self._mem[h] = {"response": best_resp, "question": question, "ts": time.time()}
            self._order.append(h)
            return best_resp

        return None

    def _bump_hit(self, h: str):
        try:
            self.session.sql(f"""
                UPDATE {self.table}
                SET HIT_COUNT = HIT_COUNT + 1, LAST_HIT_AT = CURRENT_TIMESTAMP()
                WHERE QUERY_HASH = '{h}'
            """).collect()
        except Exception:
            pass

    # ── set ──────────────────────────────────────────────────────────────────
    def set(self, question: str, response: Dict) -> bool:
        """Store response in memory + Snowflake. Returns True on DB success."""
        if not response or not self._is_real(response):
            return False

        h = self._hash(question)
        self._evict_if_full(h)
        self._mem[h] = {"response": response, "question": question, "ts": time.time()}
        if h not in self._order:
            self._order.append(h)

        if not self._table_ok:
            self.last_error = "Table not accessible — response stored in memory only"
            return False

        try:
            q_esc   = question[:3900].replace("'", "''")
            u_esc   = self._user.replace("'", "''")
            j_str   = self._to_json_safe(response)

            # Truncate if over 500KB to stay well within Snowflake VARIANT limit
            if len(j_str) > 500_000:
                # Keep only the most important parts
                safe = {
                    "layout":  response.get("layout", ""),
                    "source":  response.get("source", ""),
                    "metrics": self._to_json_safe(response.get("metrics", {})),
                    "message": {
                        "content": [
                            b for b in (response.get("message", {}).get("content", []) or [])
                            if isinstance(b, dict) and b.get("type") in ("text", "sql")
                        ]
                    } if "message" in response else {},
                }
                j_str = json.dumps(safe, ensure_ascii=True, default=str)

            # Use $$ dollar-quoting to avoid ALL single-quote escaping issues
            # This is safe as long as response JSON doesn't contain $$  (extremely unlikely)
            self.session.sql(f"""
                MERGE INTO {self.table} AS t
                USING (SELECT '{h}' AS hh, '{q_esc}' AS qq, '{u_esc}' AS uu) AS s
                    ON t.QUERY_HASH = s.hh
                WHEN MATCHED THEN
                    UPDATE SET
                        LAST_HIT_AT   = CURRENT_TIMESTAMP(),
                        HIT_COUNT     = t.HIT_COUNT + 1,
                        RESPONSE_JSON = PARSE_JSON($${j_str}$$)
                WHEN NOT MATCHED THEN
                    INSERT (QUERY_HASH, QUESTION, RESPONSE_JSON, USER_NAME)
                    VALUES (s.hh, s.qq, PARSE_JSON($${j_str}$$), s.uu)
            """).collect()
            self.last_error = ""
            return True

        except Exception as e:
            self.last_error = f"Cache set failed: {e}"
            return False

    def stats(self) -> Dict:
        valid = {k: v for k, v in self._mem.items() if time.time() - v["ts"] < self.ttl}
        return {
            "memory_entries": len(valid),
            "max_size": self.max_size,
            "table_ok": self._table_ok,
            "last_error": self.last_error,
        }


# ══════════════════════════════════════════════════════════════════════════════
# ❸  GENIE LONG-TERM MEMORY  (facts extracted from past queries via Cortex)
# ══════════════════════════════════════════════════════════════════════════════

class GenieLongTermMemory:
    """Extracts durable facts about the user's interests from their question history.

    Sources (tried in order):
      1. GENIE_QUESTION_HISTORY  — populated on every question, best source
      2. GENIE_QUERY_CACHE       — fallback if history table is unavailable
      3. GENIE_CHAT_SESSIONS     — last resort: raw message content

    Facts are shown in the sidebar and prepended to future Cortex prompts.
    Call refresh() after new questions are asked to keep facts current.
    """

    def __init__(self, session, db: str, schema: str, cortex_model: str = "llama3-8b"):
        self.session        = session
        self.db             = db
        self.schema         = schema
        self.model          = cortex_model
        self._memories: List[str] = []
        self._user          = "UNKNOWN"
        self.last_error     = ""          # surfaced in UI for debugging
        self._source        = ""          # which table actually returned data
        self._raw_questions: List[str] = []  # stored so refresh can reuse

        if session:
            try:
                self._user = (
                    session.sql("SELECT CURRENT_USER()").collect()[0][0] or "UNKNOWN"
                )
            except Exception:
                pass
            self._build()

    # ── internal: fetch questions from best available source ─────────────────
    def _fetch_questions(self) -> List[str]:
        """Return up to 20 recent distinct questions from the best available table."""
        u_esc = self._user.replace("'", "''")

        # ── Source 1: GENIE_QUESTION_HISTORY (most reliable — always populated) ──
        try:
            hist_table = f"{self.db}.{self.schema}.GENIE_QUESTION_HISTORY"
            # This table has: normalized_query, type, frequency, last_asked_at, USER
            df = self.session.sql(f"""
                SELECT NORMALIZED_QUERY AS Q, FREQUENCY, LAST_ASKED_AT
                FROM {hist_table}
                WHERE TRIM(NORMALIZED_QUERY) != ''
                  AND NORMALIZED_QUERY IS NOT NULL
                ORDER BY LAST_ASKED_AT DESC
                LIMIT 20
            """).to_pandas()
            if not df.empty:
                qs = [str(r).strip() for r in df["Q"].dropna() if str(r).strip()]
                if qs:
                    self._source = "GENIE_QUESTION_HISTORY"
                    return list(dict.fromkeys(qs))   # deduplicate, keep order
        except Exception as e1:
            self.last_error = f"history table: {e1}"

        # ── Source 2: GENIE_QUERY_CACHE (questions asked + cached answers) ──────
        try:
            cache_table = f"{self.db}.{self.schema}.GENIE_QUERY_CACHE"
            df2 = self.session.sql(f"""
                SELECT QUESTION AS Q, LAST_HIT_AT
                FROM {cache_table}
                WHERE QUESTION IS NOT NULL AND TRIM(QUESTION) != ''
                ORDER BY LAST_HIT_AT DESC
                LIMIT 20
            """).to_pandas()
            if not df2.empty:
                qs2 = [str(r).strip() for r in df2["Q"].dropna() if str(r).strip()]
                if qs2:
                    self._source = "GENIE_QUERY_CACHE"
                    return list(dict.fromkeys(qs2))
        except Exception as e2:
            self.last_error += f" | cache table: {e2}"

        # ── Source 3: GENIE_CHAT_SESSIONS — user-role messages ──────────────────
        try:
            sessions_table = f"{self.db}.{self.schema}.GENIE_CHAT_SESSIONS"
            df3 = self.session.sql(f"""
                SELECT CONTENT AS Q, CREATED_AT
                FROM {sessions_table}
                WHERE ROLE = 'user'
                  AND CONTENT IS NOT NULL AND TRIM(CONTENT) != ''
                ORDER BY CREATED_AT DESC
                LIMIT 20
            """).to_pandas()
            if not df3.empty:
                qs3 = [str(r).strip() for r in df3["Q"].dropna() if str(r).strip()]
                if qs3:
                    self._source = "GENIE_CHAT_SESSIONS"
                    return list(dict.fromkeys(qs3))
        except Exception as e3:
            self.last_error += f" | sessions table: {e3}"

        return []

    # ── internal: call Cortex to extract facts ───────────────────────────────
    def _extract_facts(self, questions: List[str]) -> List[str]:
        if not questions:
            return []
        transcript = "\n".join(f"- {q}" for q in questions[:15])
        prompt = (
            "You are analyzing a procurement analyst's query history to understand their interests.\n\n"
            f"They have recently asked these questions about procurement data:\n{transcript}\n\n"
            "Based on these questions, extract 3-6 concise facts about:\n"
            "- Which KPIs or metrics they track (spend, invoices, vendors, payments)\n"
            "- Which vendors, categories, or time periods they focus on\n"
            "- Any recurring concerns or patterns (overdue, anomalies, comparisons)\n\n"
            "Rules:\n"
            "- Each fact must be ONE short sentence (max 15 words)\n"
            "- Write in present tense: 'User tracks...', 'User monitors...', 'User focuses on...'\n"
            "- Only include facts that would help personalise future answers\n"
            "- If the questions are too generic to extract useful facts, respond: NONE\n\n"
            "Respond with facts only, one per line, no numbering, no bullet symbols:"
        )
        try:
            tdf = self.session.sql(
                "SELECT SNOWFLAKE.CORTEX.COMPLETE(?, ?) AS R",
                params=[self.model, prompt]
            ).to_pandas()
            raw = (tdf.at[0, "R"] if not tdf.empty else "") or ""
            raw = raw.strip()
            if not raw or raw.upper().startswith("NONE"):
                return []
            facts = [
                line.strip()
                for line in raw.splitlines()
                if line.strip()
                and line.strip().upper() not in ("NONE", "N/A", "NA")
                and len(line.strip()) > 8
                and not line.strip().startswith("#")  # skip any markdown headers
            ]
            return facts[:6]
        except Exception as e:
            self.last_error += f" | cortex: {e}"
            return []

    # ── main build ───────────────────────────────────────────────────────────
    def _build(self):
        self.last_error = ""
        self._source    = ""
        questions = self._fetch_questions()
        self._raw_questions = questions
        if not questions:
            self.last_error = self.last_error or "No questions found in any history table."
            return
        self._memories = self._extract_facts(questions)

    # ── public API ───────────────────────────────────────────────────────────
    def get_prefix(self) -> str:
        """Return context string to prepend to Cortex prompts."""
        if not self._memories:
            return ""
        lines = "\n".join(f"- {m}" for m in self._memories)
        return (
            "Context about this user (from past sessions — use to personalise your answer):\n"
            + lines + "\n\n"
        )

    def refresh(self) -> None:
        """Re-run the full build cycle. Call after new questions have been asked."""
        self._memories = []
        self._build()

    @property
    def count(self) -> int:
        return len(self._memories)


# ══════════════════════════════════════════════════════════════════════════════
# ❹  GENIE CHAT PERSISTENCE  (short + long-term conversation storage)
#
#   Short-term : messages live in st.session_state (current browser session)
#   Long-term  : each turn is written to GENIE_CHAT_SESSIONS in Snowflake
#                so the user can resume after logging off and back in.
# ══════════════════════════════════════════════════════════════════════════════

class GenieChatPersistence:
    """Persists Genie conversation turns to Snowflake for cross-session resume.

    Table: GENIE_CHAT_SESSIONS (auto-created on first use)
    """

    TABLE_SUFFIX = "GENIE_CHAT_SESSIONS"
    RESTORE_DAYS = 7   # show sessions from the last 7 days (was 2, too short)
    MAX_TURNS    = 40

    def __init__(self, session, db: str, schema: str):
        self.session    = session
        self.table      = f"{db}.{schema}.{self.TABLE_SUFFIX}"
        self._table_ok  = False
        self._user      = "UNKNOWN"

        if session:
            try:
                self._user = session.sql("SELECT CURRENT_USER()").collect()[0][0] or "UNKNOWN"
            except Exception:
                pass
            self._init_table()

    def _init_table(self):
        """Verify chat sessions table is accessible. Admin pre-created it."""
        try:
            self.session.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.table} (
                    SESSION_ID    STRING        NOT NULL,
                    USER_NAME     STRING        NOT NULL,
                    TURN_INDEX    INT           NOT NULL,
                    ROLE          STRING        NOT NULL,
                    CONTENT       STRING        NOT NULL,
                    CREATED_AT    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() NOT NULL,
                    SQL_USED      STRING,
                    SOURCE        STRING,
                    SESSION_LABEL STRING
                )
            """).collect()
            self._table_ok = True
        except Exception:
            # CREATE failed — table was pre-created by admin. Probe with SELECT.
            try:
                self.session.sql(f"SELECT COUNT(*) FROM {self.table} LIMIT 1").collect()
                self._table_ok = True
            except Exception:
                self._table_ok = False

    # ── write one turn ────────────────────────────────────────────────────────
    def save_turn(self, session_id: str, turn_index: int, role: str,
                  content: str, sql_used: str = "", source: str = "",
                  session_label: str = ""):
        if not self._table_ok or not self.session:
            return
        def _e(s): return str(s or "").replace("'", "''")[:4000]
        try:
            sid_e   = _e(session_id)
            usr_e   = _e(self._user)
            role_e  = _e(role)
            cont_e  = _e(content)
            sql_e   = _e(sql_used)
            src_e   = _e(source)
            lbl_e   = _e(session_label)
            ti      = int(turn_index)
            self.session.sql(f"""
                MERGE INTO {self.table} AS t
                USING (SELECT '{sid_e}' AS sid, {ti} AS ti) AS s
                    ON t.SESSION_ID = s.sid AND t.TURN_INDEX = s.ti
                WHEN NOT MATCHED THEN
                    INSERT (SESSION_ID, USER_NAME, TURN_INDEX, ROLE, CONTENT,
                            SQL_USED, SOURCE, SESSION_LABEL)
                    VALUES ('{sid_e}', '{usr_e}', {ti}, '{role_e}', '{cont_e}',
                            '{sql_e}', '{src_e}', '{lbl_e}')
            """).collect()
        except Exception:
            pass

    # ── list recent sessions ─────────────────────────────────────────────────
    def load_all_sessions(self) -> List[Dict]:
        if not self._table_ok or not self.session:
            return []
        try:
            u = self._user.replace("'", "''")
            df = self.session.sql(f"""
                SELECT SESSION_ID, MAX(SESSION_LABEL) AS SESSION_LABEL,
                       MAX(CREATED_AT) AS LAST_AT, COUNT(*) AS TURN_COUNT
                FROM {self.table}
                WHERE USER_NAME='{u}'
                  AND CREATED_AT >= DATEADD('day',-{self.RESTORE_DAYS},CURRENT_TIMESTAMP())
                GROUP BY SESSION_ID ORDER BY LAST_AT DESC LIMIT 20
            """).to_pandas()
            if df.empty:
                return []
            now  = pd.Timestamp.utcnow().tz_localize(None)
            rows = []
            for _, r in df.iterrows():
                try:
                    age_h = (now - pd.Timestamp(r["LAST_AT"]).tz_localize(None)).total_seconds() / 3600
                except Exception:
                    age_h = 0.0
                rows.append({"session_id": str(r["SESSION_ID"]),
                              "session_label": str(r["SESSION_LABEL"] or "Previous chat"),
                              "age_hours": age_h, "turn_count": int(r["TURN_COUNT"])})
            return rows
        except Exception:
            return []

    # ── load messages for one session ────────────────────────────────────────
    def load_session_messages(self, session_id: str) -> List[Dict]:
        if not self._table_ok or not self.session:
            return []
        try:
            u = self._user.replace("'", "''")
            s = session_id.replace("'", "''")
            df = self.session.sql(f"""
                SELECT ROLE,CONTENT,CREATED_AT,SQL_USED,SOURCE
                FROM {self.table}
                WHERE SESSION_ID='{s}' AND USER_NAME='{u}'
                ORDER BY TURN_INDEX ASC LIMIT {self.MAX_TURNS}
            """).to_pandas()
            return [{"role": str(r["ROLE"]), "content": str(r["CONTENT"]),
                     "timestamp": pd.Timestamp(r["CREATED_AT"]),
                     "response": None, "source": str(r.get("SOURCE",""))}
                    for _, r in df.iterrows()]
        except Exception:
            return []

    # ── housekeeping ─────────────────────────────────────────────────────────
    def purge_old(self, keep_days: int = 3):
        """No-op: DELETE not available to this role. Old rows are simply ignored."""
        pass

# ---------- Dependencies Check ----------
try:
    import streamlit as st
    import pandas as pd
    import altair as alt
    import numpy as np
except ImportError as e:
    st.error(f"Missing dependency: {e}. Please install required packages: streamlit, pandas, altair, numpy, snowflake-snowpark-python")
    st.stop()

# ---------- Page Config ----------
# NOTE: Replace PAGE_ICON_URL with a transparent‑background YASH logo
# hosted at a URL accessible from Snowflake for best visual results.
PAGE_ICON_URL = "https://upload.wikimedia.org/wikipedia/commons/2/2e/Yash_Technologies_logo.png"

st.set_page_config(
    page_title="P2P Analytics Dashboard",
    layout="wide",
    page_icon=PAGE_ICON_URL,
)

# ---------- Snowflake Session ----------
try:
    session = get_active_session()
except:
    try:
        session = Session.builder.configs(st.secrets["snowflake"]).create()
    except Exception as e:
        st.error(f"Failed to connect to Snowflake: {e}")
        st.stop()

# Test connection
try:
    test_df = session.sql("SELECT CURRENT_ACCOUNT() as account, CURRENT_USER() as user").to_pandas()
except Exception as e:
    st.error(f"Failed to query Snowflake: {e}")
    st.stop()

# ---------- Your Snowflake assets ----------
try:
    # Use from secrets if available
    DB = st.secrets["snowflake"]["database"]
    SCHEMA = st.secrets["snowflake"]["schema"]
except:
    # Fallback for Snowflake native app
    DB = "PROCURE2PAY_DEV"
    SCHEMA = "INFORMATION_MART"

STAGE = "cortex_stage"

# Set semantic model stage based on environment
try:
    # If running locally with secrets
    SEMANTIC_MODEL_STAGE = st.secrets["snowflake"]["models_stage"].lstrip("@")
except:
    # In Snowflake native app
    SEMANTIC_MODEL_STAGE = f"{DB}.{SCHEMA}.{STAGE}"

SEMANTIC_MODEL_FILE = "P2P_SEMANTIC_MODEL.yaml"   # — your staged YAML


# ══════════════════════════════════════════════════════════════════════════════
# ❺  YAML AUTO-UPDATE ENGINE
#    Keeps P2P_SEMANTIC_MODEL.yaml in sync with INFORMATION_MART views.
#    On every Genie page load (once per session), the engine:
#      1. Reads the current YAML from the Snowflake stage.
#      2. Compares it against VW_* views that exist in INFORMATION_MART.
#      3. Appends any missing view as a new table definition.
#      4. Writes the updated YAML back to the stage (and a local copy).
#    Column classification mirrors the dealer app reference logic:
#      *_PCT / _PERCENT / _DAYS / _HOURS / _AMOUNT / _RATE / _COUNT /
#      _TOTAL / _MARGIN  → fact   |  PERIOD_* → TIME dimension
#      everything else   → STRING dimension
# ══════════════════════════════════════════════════════════════════════════════

_YAML_UPDATE_LOG: List[str] = []   # module-level log so UI can surface results


def _yaml_get_views_from_snowflake(sf_session) -> List[str]:
    """Return list of VW_* view names from PROCURE2PAY_DEV.INFORMATION_MART."""
    try:
        rows = sf_session.sql(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_SCHEMA = 'INFORMATION_MART' "
            "  AND TABLE_NAME LIKE 'VW_%' "
            "ORDER BY TABLE_NAME"
        ).collect()
        return [r[0] for r in rows]
    except Exception as exc:
        logging.warning(f"[YAML-SYNC] Could not fetch views: {exc}")
        return []


def _yaml_build_table_def(sf_session, view_name: str) -> Dict:
    """Build a semantic table definition dict from a view's columns."""
    try:
        rows = sf_session.sql(
            f"SELECT COLUMN_NAME, DATA_TYPE "
            f"FROM INFORMATION_SCHEMA.COLUMNS "
            f"WHERE TABLE_SCHEMA = 'INFORMATION_MART' "
            f"  AND TABLE_NAME = '{view_name}' "
            f"ORDER BY ORDINAL_POSITION"
        ).collect()
    except Exception as exc:
        logging.warning(f"[YAML-SYNC] Could not read columns for {view_name}: {exc}")
        return {}

    _FACT_SUFFIXES = (
        "_PCT", "_PERCENT", "_DAYS", "_HOURS",
        "_AMOUNT", "_RATE", "_COUNT", "_TOTAL", "_MARGIN",
    )
    _TIME_COLS = {"PERIOD_YEAR", "PERIOD_MONTH", "PERIOD_WEEK"}

    dimensions, facts = [], []
    for col_name, _ in rows:
        col_up = col_name.upper()
        if any(col_up.endswith(s) for s in _FACT_SUFFIXES):
            facts.append({
                "name": col_name,
                "description": f"{col_name} metric from {view_name}",
                "type": "NUMERIC",
            })
        elif col_up in _TIME_COLS:
            dimensions.append({
                "name": col_name,
                "description": f"Time period — {col_name}",
                "type": "TIME",
            })
        else:
            dimensions.append({
                "name": col_name,
                "description": f"{col_name} attribute from {view_name}",
                "type": "STRING",
            })

    if not dimensions and not facts:
        return {}

    return {
        "name": view_name,
        "table": f"PROCURE2PAY_DEV.INFORMATION_MART.{view_name}",
        "description": f"Auto-generated semantic definition for {view_name}",
        "dimensions": dimensions[:15],   # cap per dealer-app convention
        "facts": facts[:10],
    }


def _yaml_read_from_stage(sf_session) -> str:
    """Download the current YAML from the Snowflake stage and return its text."""
    stage_path = f"@{SEMANTIC_MODEL_STAGE}/{SEMANTIC_MODEL_FILE}"
    try:
        # GET to a temp local path inside the Snowpark session's temp dir
        tmp_dir = "/tmp/p2p_yaml_sync"
        sf_session.sql(f"GET {stage_path} file://{tmp_dir}/").collect()
        local_path = f"{tmp_dir}/{SEMANTIC_MODEL_FILE}"
        with open(local_path, "r", encoding="utf-8") as fh:
            return fh.read()
    except Exception as exc:
        logging.warning(f"[YAML-SYNC] Could not read YAML from stage ({stage_path}): {exc}")
        return ""


def _yaml_upload_to_stage(sf_session, yaml_content: str) -> bool:
    """Write updated YAML back to the Snowflake stage."""
    from io import BytesIO
    stage_path = f"@{SEMANTIC_MODEL_STAGE}/{SEMANTIC_MODEL_FILE}"
    try:
        yaml_bytes = yaml_content.encode("utf-8")
        yaml_file  = BytesIO(yaml_bytes)
        sf_session.file.put_stream(
            yaml_file,
            stage_path,
            auto_compress=False,
            overwrite=True,
        )
        logging.info(f"[YAML-SYNC] ✅ Uploaded updated YAML to {stage_path}")
        return True
    except Exception as exc:
        logging.error(f"[YAML-SYNC] ❌ Stage upload failed: {exc}")
        return False


def _yaml_save_local(yaml_content: str) -> bool:
    """Save a local copy of the YAML for debugging / audit trail."""
    try:
        local_path = f"/tmp/{SEMANTIC_MODEL_FILE}"
        with open(local_path, "w", encoding="utf-8") as fh:
            fh.write(yaml_content)
        logging.info(f"[YAML-SYNC] ✅ Local copy saved to {local_path}")
        return True
    except Exception as exc:
        logging.warning(f"[YAML-SYNC] Could not save local copy: {exc}")
        return False


def run_yaml_auto_update(sf_session) -> Dict[str, Any]:
    """
    Main entry point — call once per Genie session startup.

    Returns a result dict:
      {
        "status":      "ok" | "no_changes" | "error",
        "added_views": [...],      # list of view names that were newly added
        "message":     "...",      # human-readable summary
      }
    """
    result: Dict[str, Any] = {
        "status": "ok",
        "added_views": [],
        "message": "",
    }

    if not sf_session:
        result["status"]  = "error"
        result["message"] = "No active Snowflake session — YAML sync skipped."
        return result

    # ── Step 1: Read existing YAML from stage ────────────────────────────────
    current_yaml = _yaml_read_from_stage(sf_session)
    if current_yaml:
        try:
            model = yaml.safe_load(current_yaml) or {}
        except Exception:
            model = {}
    else:
        # YAML not yet on stage — start from an empty skeleton
        model = {"name": "P2P Semantic Model", "tables": []}

    existing_table_names = {
        t.get("name")
        for t in model.get("tables", [])
        if isinstance(t, dict) and t.get("name")
    }

    # ── Step 2: Discover VW_* views in Snowflake ─────────────────────────────
    sf_views = _yaml_get_views_from_snowflake(sf_session)
    if not sf_views:
        result["status"]  = "no_changes"
        result["message"] = "No VW_* views found in INFORMATION_MART — nothing to add."
        return result

    # ── Step 3: Add missing views ─────────────────────────────────────────────
    added: List[str] = []
    for view_name in sf_views:
        if view_name in existing_table_names:
            continue  # already in YAML
        table_def = _yaml_build_table_def(sf_session, view_name)
        if not table_def:
            logging.warning(f"[YAML-SYNC] Skipping {view_name} — could not build definition")
            continue
        model.setdefault("tables", []).append(table_def)
        added.append(view_name)
        logging.info(f"[YAML-SYNC] ✅ Added {view_name}")

    if not added:
        result["status"]  = "no_changes"
        result["message"] = f"YAML is already up to date ({len(existing_table_names)} tables, {len(sf_views)} views checked)."
        return result

    # ── Step 4: Serialise and persist ────────────────────────────────────────
    updated_yaml = yaml.dump(model, default_flow_style=False, sort_keys=False, allow_unicode=True)
    _yaml_save_local(updated_yaml)
    upload_ok = _yaml_upload_to_stage(sf_session, updated_yaml)

    result["added_views"] = added
    result["message"] = (
        f"Added {len(added)} new view(s) to the semantic model: {', '.join(added)}. "
        + ("Stage updated ✅" if upload_ok else "⚠️ Stage upload failed — check permissions.")
    )
    if not upload_ok:
        result["status"] = "error"

    return result


# Genie question history (persistent in Snowflake, one row per unique query + frequency)
GENIE_HISTORY_TABLE = f"{DB}.{SCHEMA}.GENIE_QUESTION_HISTORY"


def apply_custom_theme_picker(default_color: str = "#FBF9F4", link_text: str = "BG"):
    """
    Show a pill‑shaped 'CHANGE BG COLOR' button in the top‑right
    that controls the app background via a hidden color picker.
    """
    # Initialize session state for background color
    if "bg_color" not in st.session_state:
        st.session_state.bg_color = default_color

    current_bg = st.session_state.bg_color

    # Inject CSS for global background + pill button in top‑right
    st.markdown(
        f"""
        <style>
            /* Global app background driven by bg_color */
            .stApp {{
                background-color: {current_bg} !important;
                transition: background-color 0.5s ease;
            }}

            /* Visible small round button with text */
            .theme-anchor {{
                position: fixed;
                bottom: 20px;
                right: 25px;
                z-index: 1000000;
                display: flex;
                align-items: center;
                justify-content: center;
                width: 44px;
                height: 44px;
                border-radius: 9999px;
                background-color: {current_bg};
                border: 1px solid #E5E7EB;
                box-shadow: 0 4px 10px rgba(15,23,42,0.10);
                font-size: 11px;
                font-weight: 600;
                color: #111827;
                font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
                cursor: pointer;
            }}

            .theme-anchor .theme-label-text {{
                pointer-events: none;
            }}

            /* Invisible but clickable color picker over the pill */
            div[data-testid="stColorPicker"] {{
                position: fixed !important;
                bottom: 20px !important;
                right: 25px !important;
                width: 44px !important;
                height: 44px !important;
                z-index: 1000001 !important;
                opacity: 0 !important;           /* hide widget visuals */
            }}

            /* Ensure the inner clickable element fills the area */
            div[data-testid="stColorPicker"] * {{
                width: 100% !important;
                height: 100% !important;
            }}

            /* Hide Streamlit's default label */
            div[data-testid="stColorPicker"] label {{
                display: none !important;
            }}
        </style>
        """,
        unsafe_allow_html=True,
    )

    # Render the visible pill button with text
    st.markdown(
        f"""
        <div class="theme-anchor">
            <span class="theme-label-text">{link_text}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Render the actual color picker (hidden, but clickable over the pill)
    return st.color_picker("picker", key="bg_color", label_visibility="collapsed")


# Initialize the background‑color picker once at the top level
apply_custom_theme_picker(link_text="BG")


def _sql_escape(s: str) -> str:
    """Escape single quotes for SQL string literal."""
    return (s or "").replace("'", "''")


SAVED_INSIGHTS_TABLE = f"{DB}.{SCHEMA}.SAVED_INSIGHTS"


def _get_current_user_raw() -> str:
    """Return current viewer identity for history table.

    In Streamlit in Snowflake (warehouse runtime), CURRENT_USER() returns the *viewer* only if
    an account admin has granted:  GRANT READ SESSION ON ACCOUNT TO ROLE <app_owner_role>;
    Without that, context functions can return NULL and we fall back to UNKNOWN.
    In container runtime, context functions always return owner context; viewer identity is not available.
    """
    try:
        df = session.sql("""
            SELECT COALESCE(
                TRIM(CURRENT_USER()),
                TRIM(SYS_CONTEXT('SNOWFLAKE$SESSION', 'PRINCIPAL_NAME')),
                ''
            ) AS SF_USER
        """).to_pandas()
        if not df.empty and "SF_USER" in df.columns:
            val = df.at[0, "SF_USER"]
            if val is None or (hasattr(pd, "isna") and pd.isna(val)):
                return ""
            s = str(val).strip()
            if s in ("None", "nan", "null", "<NA>"):
                return ""
            return s
    except Exception:
        pass
    return ""


def _get_app_owner_role() -> str:
    """Return CURRENT_ROLE() (app owner role when running with owner's rights). Used to show which role needs READ SESSION."""
    try:
        df = session.sql("SELECT CURRENT_ROLE() AS R").to_pandas()
        if not df.empty and "R" in df.columns:
            r = df.at[0, "R"]
            if r is not None and not (hasattr(pd, "isna") and pd.isna(r)):
                return str(r).strip()
    except Exception:
        pass
    return ""


def _append_genie_question(query: str, analysis_type: str):
    """Record question per user: if same (normalized) query for this user exists, add 1 to frequency; else insert new row. Table has only normalized_query (no query column)."""
    q = query.strip()
    if not q:
        return
    norm = _sql_escape(q.lower())
    t = _sql_escape(analysis_type.strip())
    current_user = _get_current_user_raw()
    # Never store literal "None" or blank; use placeholder if identity unknown (e.g. CURRENT_USER NULL in Streamlit)
    if not current_user:
        current_user = "UNKNOWN"
    user_esc = _sql_escape(current_user)
    try:
        # MERGE: pass current user from Python (same session) so USER is not null
        session.sql(f"""
            MERGE INTO {GENIE_HISTORY_TABLE} AS t
            USING (SELECT '{norm}' AS normalized_query, '{t}' AS type, '{user_esc}' AS usr) AS s
            ON t.normalized_query = s.normalized_query AND t."USER" = s.usr
            WHEN MATCHED THEN
                UPDATE SET t.frequency = t.frequency + 1,
                           t.last_asked_at = CURRENT_TIMESTAMP(),
                           t.type = s.type
            WHEN NOT MATCHED THEN
                INSERT (normalized_query, type, frequency, last_asked_at, "USER")
                VALUES (s.normalized_query, s.type, 1, CURRENT_TIMESTAMP(), s.usr)
        """).collect()
        if "genie_history_error" in st.session_state:
            st.session_state.genie_history_error = None  # clear after successful save
    except Exception as e:
        # Surface error so you can see why history isn't saving (e.g. table/column missing)
        if "genie_history_error" not in st.session_state:
            st.session_state.genie_history_error = None
        st.session_state.genie_history_error = str(e)


def _save_insight(question: str, title: str, analysis_type: str = "custom", page: str = "genie"):
    """Persist a Saved Insight row for the current user."""
    q = (question or "").strip()
    t = (title or "").strip()
    if not q:
        return
    try:
        current_user = _get_current_user_raw() or "UNKNOWN"
        user_esc = _sql_escape(current_user)
        q_esc = _sql_escape(q)
        t_esc = _sql_escape(t or q[:80])
        a_esc = _sql_escape((analysis_type or "custom").strip())
        page_esc = _sql_escape((page or "genie").strip())
        session.sql(f"""
            INSERT INTO {SAVED_INSIGHTS_TABLE}
                (CREATED_BY, PAGE, TITLE, QUESTION, VERIFIED_QUERY_NAME, SQL_TEXT, TAGS)
            VALUES
                ('{user_esc}', '{page_esc}', '{t_esc}', '{q_esc}', '{a_esc}', NULL, NULL)
        """).collect()
    except Exception as e:
        # Non-fatal; just surface a small warning in session state
        st.session_state["saved_insights_error"] = str(e)


def _get_saved_insights_for_user(n: int = 20, page: str = "genie"):
    """Return recent saved insights for the current user on a given page."""
    try:
        current_user = _get_current_user_raw() or "UNKNOWN"
        user_esc = _sql_escape(current_user)
        page_esc = _sql_escape((page or "genie").strip())
        df = run_df(f"""
            SELECT INSIGHT_ID, CREATED_AT, CREATED_BY, PAGE, TITLE, QUESTION, VERIFIED_QUERY_NAME
            FROM {SAVED_INSIGHTS_TABLE}
            WHERE PAGE = '{page_esc}' AND COALESCE(CREATED_BY,'UNKNOWN') = '{user_esc}'
            ORDER BY CREATED_AT DESC
            LIMIT {int(n)}
        """)
        if df is None or df.empty:
            return []
        out = []
        for _, row in df.iterrows():
            out.append({
                "id": row.get("INSIGHT_ID"),
                "title": (row.get("TITLE") or "").strip(),
                "question": (row.get("QUESTION") or "").strip(),
                "created_at": row.get("CREATED_AT"),
                "created_by": (row.get("CREATED_BY") or "").strip(),
                "verified_query_name": (row.get("VERIFIED_QUERY_NAME") or "").strip(),
            })
        return out
    except Exception:
        return []


def _get_recent_questions(n: int = 10):
    """Last n questions from Snowflake (newest first by last_asked_at). Uses normalized_query only."""
    try:
        df = run_df(f"""
            SELECT normalized_query, type, last_asked_at
            FROM {GENIE_HISTORY_TABLE}
            WHERE TRIM(normalized_query) != ''
            ORDER BY last_asked_at DESC
            LIMIT {int(n)}
        """)
        if df is None or df.empty:
            return []
        out = []
        for _, row in df.iterrows():
            ts = row.get("LAST_ASKED_AT")
            ts_iso = pd.Timestamp(ts).isoformat() if ts is not None and pd.notna(ts) else ""
            out.append({
                "query": (row.get("NORMALIZED_QUERY") or "").strip(),
                "type": (row.get("TYPE") or "").strip(),
                "timestamp_iso": ts_iso,
            })
        return out
    except Exception:
        return []


def _get_frequent_questions(n: int = 10):
    """Top n questions by total frequency across all users (global most frequent). Uses normalized_query only."""
    try:
        df = run_df(f"""
            SELECT normalized_query, SUM(frequency) AS cnt
            FROM {GENIE_HISTORY_TABLE}
            WHERE TRIM(normalized_query) != ''
            GROUP BY normalized_query
            ORDER BY cnt DESC
            LIMIT {int(n)}
        """)
        if df is None or df.empty:
            return []
        return [{"query": (row.get("NORMALIZED_QUERY") or "").strip(), "count": int(row.get("CNT", 0))} for _, row in df.iterrows()]
    except Exception:
        return []


def _get_frequent_questions_by_user(n: int = 10):
    """Top n questions by frequency for the current user only. Uses same identity as _append_genie_question (not CURRENT_USER() in SQL, which can be NULL in Streamlit)."""
    try:
        current_user = _get_current_user_raw() or "UNKNOWN"
        user_esc = _sql_escape(current_user)
        df = run_df(f"""
            SELECT normalized_query, frequency AS cnt
            FROM {GENIE_HISTORY_TABLE}
            WHERE "USER" = '{user_esc}' AND TRIM(normalized_query) != ''
            ORDER BY frequency DESC
            LIMIT {int(n)}
        """)
        if df is None or df.empty:
            return []
        return [{"query": (row.get("NORMALIZED_QUERY") or "").strip(), "count": int(row.get("CNT", 0))} for _, row in df.iterrows()]
    except Exception:
        return []


# ========== Utilities ==========

def compute_range_preset(preset: str):
    today = date.today()
    if preset == "Last 30 Days":
        return today - timedelta(days=30), today
    if preset == "QTD":
        start = date(today.year, ((today.month - 1)//3)*3 + 1, 1)
        return start, today
    if preset == "YTD":
        return date(today.year, 1, 1), today
    return today.replace(day=1), today  # Current month

def sql_date(d: date) -> str:
    return f"DATE '{d.strftime('%Y-%m-%d')}'"

def run_df(sql: str) -> pd.DataFrame:
    try:
        if not session:
            st.error("Database session is not active. Please check the connection.")
            return pd.DataFrame()
        return session.sql(sql).to_pandas()
    except Exception as e:
        st.warning(f"Query failed: {e}\nSQL: {sql}")
        return pd.DataFrame()

def safe_number(val, default=0.0):
    try:
        if val is None or (isinstance(val, float) and math.isnan(val)):
            return default
        return float(val)
    except Exception:
        return default

def safe_int(val, default=0):
    try:
        if val is None or (isinstance(val, float) and math.isnan(val)):
            return default
        return int(float(val))
    except Exception:
        return default

# Cortex Complete model for prescriptive analysis (fallback: rule-based if unavailable)
CORTEX_PRESCRIPTIVE_MODEL = "llama3-8b"

def _build_html_table(df, css_class="p2p-html-table"):
    """Build a styled HTML table from a DataFrame with dynamic row heights and text wrapping."""
    html = f'<table class="{css_class}"><thead><tr>'
    for col in df.columns:
        html += f'<th>{col}</th>'
    html += '</tr></thead><tbody>'
    for _, row in df.iterrows():
        html += '<tr>'
        for col in df.columns:
            val = str(row.get(col, "") or "").strip()
            val = val.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")
            html += f'<td>{val}</td>'
        html += '</tr>'
    html += '</tbody></table>'
    return html

def _markdown_bold_to_html(text: str) -> str:
    """Convert simple markdown bold (**text**) to HTML <strong> for safe embedding."""
    import re
    if not isinstance(text, str) or "**" not in text:
        return text
    # Replace **text** with <strong>text</strong>
    return re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", text)

def _get_ai_invoice_suggestion(invoice_number: str, inv_row: dict, status_history: str = "") -> str:
    """Use CORTEX.COMPLETE to generate a short, actionable suggestion for the selected invoice."""
    status = str(inv_row.get("INVOICE_STATUS") or "").strip()
    due = inv_row.get("DUE_DATE")
    aging = inv_row.get("AGING_DAYS")
    amount = inv_row.get("INVOICE_AMOUNT_LOCAL")
    due_str = str(due) if due else "unknown"
    aging_str = f"{int(aging)} days" if aging is not None else "unknown"
    amount_str = f"{float(amount):,.2f}" if amount is not None else "unknown"

    # Determine if the invoice is truly overdue based on actual data
    from datetime import date as _date
    is_overdue = False
    try:
        if due and status.upper() not in ("PAID", "CLEARED"):
            due_date = _date.fromisoformat(str(due)[:10])
            is_overdue = due_date < _date.today()
    except Exception:
        pass

    overdue_context = ""
    if is_overdue:
        overdue_context = "This invoice IS overdue (the due date has passed and it is not yet paid). "
    elif status.upper() in ("PAID", "CLEARED"):
        overdue_context = "This invoice is already PAID/CLEARED. It is NOT overdue. "
    else:
        overdue_context = "This invoice is NOT overdue (the due date has not passed yet, or it is still being processed). "

    status_context = ""
    if status_history:
        status_context = f"\nStatus history (latest events):\n{status_history}\n"

    prompt = (
        "Concise procure-to-pay analyst. 2-3 sentences of actionable advice based ONLY on the data below. "
        f"{overdue_context}"
        "OPEN & not overdue: say proceed to pay. Overdue: recommend immediate review. PAID: no action. BLOCKED/DISPUTED: resolution steps. "
        "Use **bold** for key terms. No bullet lists.\n\n"
        f"Invoice: {invoice_number}. Status: {status}. Due: {due_str}. Aging: {aging_str}. Amount: {amount_str}."
        f"{status_context}"
    )
    try:
        result = session.sql(
            "SELECT SNOWFLAKE.CORTEX.COMPLETE(?, ?) AS RESPONSE",
            params=[CORTEX_PRESCRIPTIVE_MODEL, prompt]
        ).to_pandas()
        if not result.empty and "RESPONSE" in result.columns:
            text = result.at[0, "RESPONSE"]
            if text and isinstance(text, str) and len(text.strip()) > 10:
                return text.strip()
    except Exception:
        pass
    # Fallback if Cortex unavailable – use rule-based logic
    if status.upper() in ("PAID", "CLEARED"):
        return "This invoice has already been **paid**. No further action is needed."
    elif is_overdue:
        return (
            f"This invoice is **overdue** (due date: {due_str}). "
            "I recommend **immediate review** to determine the cause of the delay and **expedite payment** to avoid penalties."
        )
    elif status.upper() in ("BLOCKED", "DISPUTED"):
        return (
            f"This invoice is currently **{status.lower()}**. "
            "Please investigate the issue and work with the vendor to **resolve** it before proceeding."
        )
    else:
        return (
            f"This invoice is **{status.lower()}** with a due date of {due_str}. "
            "There are no blocking issues — you can **proceed to pay** this invoice."
        )

def _cortex_complete_prescriptive(content: list, run_df_func, question: str) -> str:
    """Use SNOWFLAKE.CORTEX.COMPLETE to generate business-driven prescriptive insights from query data."""
    data_parts = []
    for block in content or []:
        if block.get("type") != "sql":
            continue
        sql = block.get("statement", "")
        if not sql.strip():
            continue
        try:
            df = run_df_func(sql)
            if df is None or df.empty:
                continue
            # Limit rows to stay within token budget (~4K tokens for data)
            head = df.head(40)
            data_parts.append(head.to_string(index=False, max_colwidth=40))
        except Exception:
            continue
    if not data_parts:
        return ""
    data_str = "\n\n---\n\n".join(data_parts)
    if len(data_str) > 15000:  # Trim if too long
        data_str = data_str[:15000] + "\n... (truncated)"
    prompt = (
        "You are a procurement business analyst. The user asked a question and received the following data from our analytics. "
        "Provide prescriptive insights: specific recommended actions and risks based on the data. "
        "Be concrete: cite numbers, vendor names, amounts, and percentages from the data. "
        "Format as bullet points (use •). Do NOT use generic phrases like 'review the data'—give actionable recommendations.\n\n"
        f"User question: {question}\n\n"
        f"Data:\n{data_str}"
    )
    try:
        result = session.sql(
            "SELECT SNOWFLAKE.CORTEX.COMPLETE(?, ?) AS RESPONSE",
            params=[CORTEX_PRESCRIPTIVE_MODEL, prompt]
        ).to_pandas()
        if not result.empty and "RESPONSE" in result.columns:
            text = result.at[0, "RESPONSE"]
            if text and isinstance(text, str) and len(text.strip()) > 20:
                return text.strip()
    except Exception:
        pass
    return ""

def _generate_prescriptive_from_data(content: list, run_df_func) -> str:
    """Generate data-driven prescriptive insights from SQL result dataframes when Cortex returns generic text."""
    bullets = []
    for block in content or []:
        if block.get("type") != "sql":
            continue
        sql = block.get("statement", "")
        if not sql.strip():
            continue
        try:
            df = run_df_func(sql)
            if df is None or df.empty or len(df.columns) < 2:
                continue
            upper = {str(c).upper(): c for c in df.columns}
            # Cost reduction / opportunity format: OPPORTUNITY_AREA, AMOUNT, FINDING, RECOMMENDED_ACTION
            if "RECOMMENDED_ACTION" in upper and "OPPORTUNITY_AREA" in upper:
                area_col = upper["OPPORTUNITY_AREA"]
                amt_col = upper.get("AMOUNT")
                find_col = upper.get("FINDING")
                act_col = upper["RECOMMENDED_ACTION"]
                for _, row in df.head(6).iterrows():
                    area = str(row.get(area_col, "")).replace("_", " ").strip()
                    amt = abbr_currency(safe_number(row.get(amt_col), 0)) if amt_col else ""
                    find = str(row.get(find_col, ""))[:80] if find_col else ""
                    act = str(row.get(act_col, "")).strip()
                    if act and area:
                        bullets.append(f"• <b>{area}</b>: {amt}. {act}")
            # Aging buckets: AGING_BUCKET, INVOICE_COUNT, TOTAL_AMOUNT
            elif "AGING_BUCKET" in upper:
                bcol = upper["AGING_BUCKET"]
                cc = upper.get("INVOICE_COUNT") or upper.get("CNT")
                ac = upper.get("TOTAL_AMOUNT") or upper.get("SPEND")
                for _, row in df.iterrows():
                    bucket = row.get(bcol, "")
                    cnt = safe_int(row.get(cc), 0)
                    amt = safe_number(row.get(ac), 0)
                    if bucket and (cnt > 0 or amt > 0):
                        bullets.append(f"• <b>{bucket}</b>: {cnt} invoices, {abbr_currency(amt)} — Prioritize resolution of older buckets.")
            # Period comparison: DRIVER/DRIVER_VALUE, THIS_*_SPEND, LAST_*_SPEND
            elif any(upper.get(k) for k in ("THIS_MONTH_SPEND", "THIS_QUARTER_SPEND", "THIS_YEAR_SPEND", "CURRENT_MONTH_SPEND")):
                curr = upper.get("THIS_MONTH_SPEND") or upper.get("THIS_QUARTER_SPEND") or upper.get("THIS_YEAR_SPEND") or upper.get("CURRENT_MONTH_SPEND")
                prev = upper.get("LAST_MONTH_SPEND") or upper.get("LAST_QUARTER_SPEND") or upper.get("LAST_YEAR_SPEND") or upper.get("PREVIOUS_MONTH_SPEND")
                cat = upper.get("DRIVER_VALUE") or upper.get("DRIVER") or upper.get("CATEGORY")
                if curr and prev and cat:
                    for _, row in df.head(8).iterrows():
                        cval = safe_number(row.get(curr), 0)
                        pval = safe_number(row.get(prev), 0)
                        if cval > pval and str(row.get(cat, "")).lower() not in ("total", "summary"):
                            driver = row.get(cat, "")
                            delta = cval - pval
                            bullets.append(f"• <b>{driver}</b>: Spend increased by {abbr_currency(delta)} vs prior period — review drivers and negotiate if applicable.")
            # Vendor / spend: VENDOR_NAME, SPEND / TOTAL_SPEND
            elif "VENDOR_NAME" in upper or "VENDOR_ID" in upper:
                name_col = upper.get("VENDOR_NAME") or upper.get("VENDOR_ID")
                spend_col = upper.get("TOTAL_SPEND") or upper.get("SPEND") or upper.get("AMOUNT")
                if name_col and spend_col:
                    top = df.nlargest(5, spend_col) if pd.api.types.is_numeric_dtype(df[spend_col]) else df.head(5)
                    for _, row in top.iterrows():
                        name, amt = row.get(name_col, ""), safe_number(row.get(spend_col), 0)
                        if name and amt > 0:
                            bullets.append(f"• <b>{name}</b>: {abbr_currency(amt)} — Consider volume discounts or consolidation.")
            # Generic: pick best categorical + numeric, summarize top rows
            else:
                x_col, y_col = _pick_chart_columns(df)
                if x_col and y_col:
                    try:
                        numeric = pd.to_numeric(df[y_col], errors="coerce")
                        top = df.nlargest(5, y_col) if numeric.notna().any() else df.head(5)
                        for _, row in top.iterrows():
                            lab = str(row.get(x_col, ""))[:50]
                            val = safe_number(row.get(y_col), 0)
                            if lab and (val != 0 or lab):
                                bullets.append(f"• <b>{lab}</b>: {abbr_currency(val) if val >= 100 else f'{val:,.0f}'} — Review for optimization.")
                    except Exception:
                        pass
        except Exception:
            continue
    if not bullets:
        return ""
    return "<br/>".join(bullets[:8])  # Cap at 8 bullets


def _generate_prescriptive_from_dfs(dfs: list) -> str:
    """Generate prescriptive bullets from existing dataframes (quick analyses)."""
    bullets = []
    for df in dfs:
        if df is None or df.empty or len(df.columns) < 2:
            continue
        upper = {str(c).upper(): c for c in df.columns}
        # Aging buckets
        if "AGING_BUCKET" in upper:
            bcol = upper["AGING_BUCKET"]
            cc = upper.get("INVOICE_COUNT") or upper.get("CNT")
            ac = upper.get("TOTAL_AMOUNT") or upper.get("SPEND")
            for _, row in df.iterrows():
                bucket = row.get(bcol, "")
                cnt = safe_int(row.get(cc), 0)
                amt = safe_number(row.get(ac), 0)
                if bucket and (cnt > 0 or amt > 0):
                    bullets.append(f"• <b>{bucket}</b>: {cnt} invoices, {abbr_currency(amt)} — Prioritize resolution of older buckets.")
        # Vendor spend
        elif "VENDOR_NAME" in upper or "VENDOR_ID" in upper:
            name_col = upper.get("VENDOR_NAME") or upper.get("VENDOR_ID")
            spend_col = upper.get("TOTAL_SPEND") or upper.get("SPEND") or upper.get("AMOUNT")
            if name_col and spend_col:
                top = df.nlargest(5, spend_col) if pd.api.types.is_numeric_dtype(df[spend_col]) else df.head(5)
                for _, row in top.iterrows():
                    name, amt = row.get(name_col, ""), safe_number(row.get(spend_col), 0)
                    if name and amt > 0:
                        bullets.append(f"• <b>{name}</b>: {abbr_currency(amt)} — Consider volume discounts or consolidation.")
        # Payment performance (avg days / late payments)
        elif "AVG_DAYS_TO_PAY" in upper or "LATE_PAYMENTS" in upper:
            avg_col = upper.get("AVG_DAYS_TO_PAY")
            late_col = upper.get("LATE_PAYMENTS")
            if avg_col:
                avg_val = safe_number(df[avg_col].mean(), 0)
                bullets.append(f"• <b>Avg days to pay</b>: {avg_val:.1f} — Reduce cycle time to avoid late fees.")
            if late_col:
                late_total = safe_number(df[late_col].sum(), 0)
                if late_total > 0:
                    bullets.append(f"• <b>Late payments</b>: {late_total:,.0f} — Strengthen approvals and vendor follow-ups.")
        # Generic: use top categorical + numeric
        else:
            x_col, y_col = _pick_chart_columns(df)
            if x_col and y_col:
                try:
                    numeric = pd.to_numeric(df[y_col], errors="coerce")
                    top = df.nlargest(5, y_col) if numeric.notna().any() else df.head(5)
                    for _, row in top.iterrows():
                        lab = str(row.get(x_col, ""))[:50]
                        val = safe_number(row.get(y_col), 0)
                        if lab and (val != 0 or lab):
                            bullets.append(f"• <b>{lab}</b>: {abbr_currency(val) if val >= 100 else f'{val:,.0f}'} — Review for optimization.")
                except Exception:
                    pass
    if not bullets:
        return ""
    return "<br/>".join(bullets[:8])


def _cortex_complete_prescriptive_from_dfs(dfs: list, question: str, context_text: str = "") -> str:
    """Use SNOWFLAKE.CORTEX.COMPLETE to generate prescriptive insights from existing dataframes."""
    import html as _html

    data_parts = []
    for df in dfs or []:
        try:
            if df is None or df.empty:
                continue
            # Keep the llama prompt small to avoid long latencies.
            # Spending overview: last 12 months, top 10 vendors.
            cols_upper = {str(c).upper(): c for c in df.columns}
            if "MONTH" in cols_upper and len(df) > 12:
                slice_df = df.tail(12)
            elif "VENDOR_NAME" in cols_upper and ("SPEND" in cols_upper or "TOTAL_SPEND" in cols_upper):
                slice_df = df.head(10)
            else:
                slice_df = df.head(20)
            data_parts.append(slice_df.to_string(index=False, max_colwidth=40))
        except Exception:
            continue

    if not data_parts:
        return ""

    data_str = "\n\n---\n\n".join(data_parts)
    if len(data_str) > 15000:
        data_str = data_str[:15000] + "\n... (truncated)"

    prompt = (
        "You are a senior procure-to-pay analyst writing recommendations for business decision makers.\n"
        "Using ONLY the data provided below, write 4–6 bullet points.\n"
        "Each bullet MUST have: (a) specific finding with numbers from the data, (b) concrete action, (c) why it matters (cost, risk, SLA, cash).\n"
        "Avoid generic advice like 'review for optimization' unless you name exactly what to change.\n"
        "Format: bullet points starting with '•'. Use **bold** for key terms.\n\n"
        f"Business question: {question}\n\n"
        f"Context (already computed metrics): {context_text}\n\n"
        f"Data:\n{data_str}\n"
    )

    try:
        result = session.sql(
            "SELECT SNOWFLAKE.CORTEX.COMPLETE(?, ?) AS RESPONSE",
            params=[CORTEX_PRESCRIPTIVE_MODEL, prompt],
        ).to_pandas()
        if not result.empty and "RESPONSE" in result.columns:
            text = result.at[0, "RESPONSE"]
            if text and isinstance(text, str):
                text = text.strip()
                # Filter out low-signal generic responses
                low = text.lower()
                if len(text) < 60 or any(p in low for p in ("review for optimization", "review the data", "consider setting alerts")):
                    return ""
                # Escape then convert **bold** to <strong>, keep newlines
                safe = _html.escape(text).replace("\n", "<br/>")
                return _markdown_bold_to_html(safe)
    except Exception:
        pass
    return ""

def _parse_descriptive_prescriptive(text: str):
    """Split analyst response into (descriptive, prescriptive) sections if clearly marked; else (None, None)."""
    if not text or not text.strip():
        return None, None
    text = text.strip()
    pres_markers = ("**Prescriptive**", "**Prescriptive**:", "Prescriptive:", "Prescriptive**", "\nPrescriptive:")
    idx = -1
    for m in pres_markers:
        i = text.find(m)
        if i >= 0:
            idx = i
            break
    if idx >= 0:
        descriptive = text[:idx].strip()
        prescriptive = text[idx:].strip()
        for m in pres_markers:
            if prescriptive.startswith(m):
                prescriptive = prescriptive[len(m):].strip().lstrip(":\n ")
                break
        for d in ("**Descriptive**", "**Descriptive**:", "Descriptive:", "Descriptive**"):
            if descriptive.startswith(d):
                descriptive = descriptive[len(d):].strip().lstrip(":\n ")
                break
        return descriptive or None, prescriptive or None
    return None, None


def parse_analysis_sections(text: str):
    """Split Cortex response into (descriptive, prescriptive, predictive) using regex.
    Returns (desc, pres, pred) — any can be empty string if not found.
    """
    import re as _re
    if not text or not text.strip():
        return "", "", ""
    di = _re.search(r'\b(?:\d+\.?\s+)?(?:\*{0,2})descriptive(?:\*{0,2})(?:\s*[-—:])?',  text, _re.IGNORECASE)
    pi = _re.search(r'\b(?:\d+\.?\s+)?(?:\*{0,2})prescriptive(?:\*{0,2})(?:\s*[-—:])?', text, _re.IGNORECASE)
    ri = _re.search(r'\b(?:\d+\.?\s+)?(?:\*{0,2})predictive(?:\*{0,2})(?:\s*[-—:])?',   text, _re.IGNORECASE)

    def _ext(sm, *others):
        if not sm:
            return ""
        s = sm.end()
        e = len(text)
        for o in others:
            if o and o.start() > sm.start():
                e = min(e, o.start())
        return text[s:e].strip().lstrip("*").lstrip(":").strip()

    return _ext(di, pi, ri), _ext(pi, ri, di), _ext(ri, di, pi)


def _generate_predictive_text(question: str, dfs: list, session, metrics: dict = None) -> str:
    """Call Cortex to generate a Predictive (30–90 day forecast) section from data.
    Returns HTML-safe string. Falls back to rule-based text if Cortex unavailable.
    """
    data_parts = []
    for df in (dfs or []):
        try:
            if df is None or df.empty:
                continue
            data_parts.append(df.head(10).to_string(index=False, max_colwidth=40))
        except Exception:
            continue

    data_str = "\n\n---\n\n".join(data_parts)
    if len(data_str) > 8000:
        data_str = data_str[:8000] + "\n... (truncated)"

    metrics_str = ""
    if metrics:
        metrics_str = "Key metrics: " + ", ".join(f"{k}={v}" for k, v in list(metrics.items())[:6])

    prompt = (
        "You are a senior procurement analyst. Based on the data and question below, "
        "write a PREDICTIVE section only — a concise 30–90 day forecast.\n\n"
        "Format:\n"
        "- 2–3 sentences max\n"
        "- State the likely trend and quantify it (e.g. spend may rise by ~12%, ~15 invoices at risk)\n"
        "- List 1–2 key assumptions\n"
        "- End with confidence level: Low / Medium / High\n\n"
        f"Question: {question}\n"
        f"{metrics_str}\n"
        f"Data:\n{data_str}"
    )
    try:
        tdf = session.sql(
            "SELECT SNOWFLAKE.CORTEX.COMPLETE(?, ?) AS R",
            params=[CORTEX_PRESCRIPTIVE_MODEL, prompt]
        ).to_pandas()
        raw = (tdf.at[0, "R"] if not tdf.empty else "") or ""
        raw = raw.strip()
        if raw and len(raw) > 20:
            return raw
    except Exception:
        pass

    # Rule-based fallback
    q = (question or "").lower()
    if "overdue" in q or "aging" in q or "late" in q:
        return ("Based on current aging trends, overdue invoice volumes are likely to increase "
                "unless payment cycle times improve. Confidence: Medium.")
    if "spend" in q or "cost" in q or "vendor" in q:
        return ("If current spend patterns continue, total procurement costs may rise modestly "
                "over the next 30–90 days. Vendor consolidation could offset this trend. Confidence: Medium.")
    if "invoice" in q or "payment" in q:
        return ("Invoice processing times appear stable. Continued automation investment should "
                "reduce cycle times by 10–15% within 90 days. Confidence: Medium.")
    return ("Near-term trends suggest continuation of current patterns. "
            "Monitor key metrics weekly and adjust strategy if significant deviations emerge. Confidence: Low.")


def _safe_pct_str(val, default=0.0):
    v = safe_number(val, default)
    sign = "+" if v >= 0 else ""
    return f"{sign}{v:.1f}%"

def abbr_currency(v: float, currency_symbol: str = "$") -> str:
    """$4.2M style abbreviations."""
    n = abs(v)
    sign = "-" if v < 0 else ""
    if n >= 1_000_000_000: return f"{sign}{currency_symbol}{n/1_000_000_000:.1f}B"
    if n >= 1_000_000:     return f"{sign}{currency_symbol}{n/1_000_000:.1f}M"
    if n >= 1_000:         return f"{sign}{currency_symbol}{n/1_000:.1f}K"
    return f"{sign}{currency_symbol}{n:.0f}"

def period_length_days(start: date, end: date) -> int:
    return (end - start).days + 1

def prior_window(start: date, end: date):
    """Calculate previous period for comparison.
    If the period is a full calendar month (1st to last day), compare to same month previous year.
    Otherwise, compare to the same number of days before.
    """
    from calendar import monthrange
    
    # Check if this is a full calendar month (1st of month to last day of month)
    is_full_month = False
    if start.day == 1:
        last_day_of_month = monthrange(end.year, end.month)[1]
        is_full_month = end.day == last_day_of_month and end.month == start.month
    
    if is_full_month:
        # For full months, get the same calendar month from previous month
        if start.month == 1:
            # Previous month is December of previous year
            prev_year = start.year - 1
            prev_month = 12
        else:
            # Previous month in same year
            prev_year = start.year
            prev_month = start.month - 1
        
        prev_start = date(prev_year, prev_month, 1)
        prev_end_day = monthrange(prev_year, prev_month)[1]
        prev_end = date(prev_year, prev_month, prev_end_day)
        return prev_start, prev_end
    else:
        # For non-month ranges, use the same number of days before
        days = period_length_days(start, end)
        prev_end = start - timedelta(days=1)
        prev_start = prev_end - timedelta(days=days - 1)
        return prev_start, prev_end

def pct_delta(cur: float, prev: float):
    try:
        if prev is None or (isinstance(prev, float) and math.isnan(prev)):
            return None, True, False
        if prev == 0:
            if cur is None or (isinstance(cur, float) and math.isnan(cur)):
                return None, True, False
            if abs(cur) < 0.00001:
                return "0%", True, True
            # Treat no prior data with a positive current as +100%
            return "+100.0%", True, False
        if cur is None or (isinstance(cur, float) and math.isnan(cur)):
            return None, True, False
        change = (cur - prev) / prev * 100.0
        if change is None or (isinstance(change, float) and math.isnan(change)):
            return None, True, False
        # Cap change to max 100% magnitude
        if change > 100:
            change = 100.0
        elif change < -100:
            change = -100.0
        # No change - return "0%" with no_change flag
        if abs(change) < 0.05:  # essentially 0% change
            return "0%", True, True  # third value indicates no change
        sign = "+" if change >= 0 else "−"
        return f"{sign}{abs(change):.1f}%", change >= 0, False
    except Exception:
        return None, True, False

def abs_delta_days(cur: float, prev: float):
    try:
        if prev is None or (isinstance(prev, float) and math.isnan(prev)):
            return None, True, False
        if cur is None or (isinstance(cur, float) and math.isnan(cur)):
            return None, True, False
        diff = cur - prev
        # No change - return "0.0d" with no_change flag
        if abs(diff) < 0.05:  # essentially 0 change
            return "0.0d", True, True  # third value indicates no change
        if isinstance(diff, float) and math.isnan(diff):
            return None, True, False
        return f"{abs(diff):.1f}d", diff < 0, False
    except Exception:
        return None, True, False

def clean_delta_text(delta):
    if delta is None:
        return None
    if not isinstance(delta, str):
        delta = str(delta)
    delta = delta.strip()
    if "<" in delta or ">" in delta:
        return None
    allowed = set("0123456789+-.%d−")
    if any(ch not in allowed for ch in delta):
        return None
    return delta

def normalize_upper(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    df2 = df.copy()
    df2.columns = [str(c).upper() for c in df2.columns]
    return df2

def get_num(df: pd.DataFrame, name: str, default=0.0):
    if df is None or df.empty:
        return default
    cols = {str(c).upper(): c for c in df.columns}
    key_up = name.upper()
    if key_up in cols:
        return safe_number(df.at[0, cols[key_up]], default)
    return default

def as_stage_url(db_sch_stage: str, file_name: str) -> str:
    return f"@{db_sch_stage}/{file_name}".replace("//", "/")

# ========== Clean UI (Light) ==========

def load_clean_ui_light():
    st.markdown("""
    <style>
    :root{
      /* Light theme tokens */
      --bg: #f7f8fb;
      --panel: #ffffff;
      --panel-2: #fafafa;
      --text: #0f172a;
      --text-subtle: #475569;
      --brand: #1459d2;   /* YASH blue-ish */
      --brand-2: #1e88e5;
      --success: #118d57;
      --danger: #d32f2f;
      --warning: #f59e0b;
      --muted: #64748b;

      --ring: rgba(20,89,210,.25);
      --divider: #e5e7eb;
      --radius: 14px;
      --radius-sm: 12px;

      --shadow-1: 0 10px 30px rgba(2,8,23,.06);
      --shadow-2: 0 2px 10px rgba(2,8,23,.06);

      --kpi-title: 12px;
      --kpi-value: 28px;
      --tab-font-size: 18px;
      --tab-font-weight: 900;
    }

    html, body, [class^="css"]{
      background: var(--bg);
      color: var(--text);
      font-family: Inter, -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto;
    }
        /* Center the main block and keep consistent width to match design mock */
        .block-container{ padding-top: 8px; max-width:1180px; margin-left:auto; margin-right:auto; }
        /* Responsive expansion for very wide screens */
        @media (min-width: 1400px) {
            .block-container{ max-width:1320px; }
            .p2p-header{ max-width:1320px; }
        }
        @media (min-width: 1600px) {
            .block-container{ max-width:1480px; }
            .p2p-header{ max-width:1480px; }
        }

    /* Branding bar (sticky) */
    .brandbar{
      position: sticky; top: 0; z-index: 100;
      background: linear-gradient(180deg, #ffffff 0%, #f8fbff 100%);
      border-bottom: 1px solid var(--divider);
      padding: 8px 0;
      box-shadow: var(--shadow-1);
    }
    .brandrow{ display:flex; align-items:center; justify-content:space-between; }
    .brand-left{ display:flex; gap:10px; align-items:center; }
    .brand-title{ font-weight: 900; letter-spacing: .2px; }
    .brand-sub{ color: var(--muted); font-size: 12px; }
    .badge-pill{
      padding: 2px 8px; border-radius: 999px; background: #e8f0ff; color: var(--brand); font-weight: 800; font-size: 11px;
      border: 1px solid #d5e4ff;
    }

    /* Header */
    .p2p-header{
      display:flex;justify-content:space-between;align-items:center;
      padding: 10px 0 12px 0; margin: 8px 0 6px 0;
      border-bottom: 1px solid var(--divider);
    }
    /* Align branding bar: title and nav on same line, vertically centered */
    [data-testid="stHorizontalBlock"]:first-of-type {
      align-items: center !important;
    }
    [data-testid="stHorizontalBlock"]:first-of-type [data-testid="column"] {
      display: flex !important;
      align-items: center !important;
    }
    .p2p-title{ font-size: 20px; font-weight: 900; letter-spacing: .2px; }
    .p2p-sub{ color: var(--text-subtle); font-size: 12px; margin-top: 2px; }
    /* Top navigation inside branding bar */
    .topnav{ display:flex; gap:18px; align-items:center; justify-content:center; }
    .topnav .nav-item{ padding:8px 14px; border-radius:999px; font-weight:800; color:var(--text-subtle); background:transparent; }
    .topnav .nav-item.active{ background:var(--brand); color:#fff; box-shadow:0 6px 18px rgba(20,89,210,.12); }
    .brand-right img.avatar{ height:32px; width:32px; border-radius:50%; }
    /* Header logo — slightly smaller so white box is less dominant. */
    .yash-header-logo {
      height: 120px !important; max-height: 120px !important; width: auto !important;
      object-fit: contain !important;
      display: block;
    }

    /* Preset buttons (Last 30 Days / QTD / YTD / Custom) — same size, one line */
    .preset-btn{ padding:8px 14px; border-radius:999px; font-weight:800; color:var(--text-subtle); background:transparent; border:1px solid transparent; }
    .preset-btn.active{ background:var(--brand); color:#fff; box-shadow:0 6px 18px rgba(20,89,210,.12); border-color:rgba(16,66,168,.12); }
    div[data-testid="column"]:has(button[data-testid*="baseButton-preset_"]) {
      display: flex !important; flex-wrap: nowrap !important; min-width: 0;
    }
    div[data-testid="column"]:has(button[data-testid*="baseButton-preset_"]) > div {
      flex: 1 1 0 !important; min-width: 0 !important;
    }
    div[data-testid="column"]:has(button[data-testid*="baseButton-preset_"]) button {
      min-width: 5rem !important; width: 100% !important;
    }

    /* Controls row */
    .ctrl-label{ color: var(--text-subtle); font-size: 12px; font-weight: 700; margin-bottom: 4px; }

    /* Tabs */
    .stTabs [data-baseweb="tab-list"]{
      gap: 4px; border-bottom: 2px solid var(--divider);
    }
    .stTabs [data-baseweb="tab"]{
      background: transparent; color: var(--text-subtle);
      border-radius: 10px 10px 0 0; padding: 10px 18px;
    }
    .stTabs [data-baseweb="tab"] *,
    .stTabs button[role="tab"] *{
      font-size: var(--tab-font-size) !important;
      font-weight: var(--tab-font-weight) !important;
      line-height: 1.1 !important;
    }
    .stTabs [aria-selected="true"]{
      color: var(--text); border-bottom: 3px solid var(--brand) !important;
    }

    /* Banner */
    .banner{
      background: linear-gradient(135deg, #e9f2ff 0%, #f2f8ff 100%);
      border: 1px solid #d9e6ff;
      border-radius: var(--radius);
      padding: 16px; margin: 10px 0 14px 0;
      box-shadow: var(--shadow-1);
      display:flex; gap:12px; align-items:flex-start;
    }
    .badge{
      font-size: 11px; padding:2px 8px; border-radius: 999px;
      border: 1px solid #e5e7eb; color: var(--text-subtle);
      background: #fff;
      font-weight: 800;
    }
    .badge.high{ background:#fde7e9; color:#b42318; border-color:#f3b4b8; }
    .badge.med{  background:#fff4e5; color:#b54708; border-color:#f7cf97; }
    .badge.low{  background:#ecfdf3; color:#067647; border-color:#a6f0c6; }

    /* KPI tiles */
    .kpi{
      background: #fff;
      border: 1px solid #e6e8ee;
      border-radius: var(--radius-sm);
      padding: 14px 14px 10px 14px;
      box-shadow: var(--shadow-2);
      width: 100%;
      min-height: 110px;
    }
    .kpi .title{ font-size: var(--kpi-title); color: var(--muted); letter-spacing: .3px; font-weight: 800; }
    .kpi .value{ font-size: var(--kpi-value); font-weight: 900; margin-top: 6px; display:flex; align-items:baseline; gap:8px; }
    .kpi .delta{ margin-top: 4px; font-weight: 900; display:flex; align-items:center; gap:6px; letter-spacing:.2px; }
    .kpi .delta .delta-icon { display:inline-flex; width:16px; height:16px; }
    .kpi .delta.up{ color: var(--success); }  /* green */
    .kpi .delta.down{ color: var(--danger); } /* red */

    /* Empty state */
    .empty{
      background: #f8fafc;
      border: 1px dashed #d7dce5;
      border-radius: 12px; padding: 16px; color: var(--muted);
    }

    .soft-note{
      background: #fff9db;
      border: 1px solid #ffe08a;
      border-radius: 12px; padding: 10px 12px; color:#8b6b00;
    }

    /* Buttons - tablet/pill shape; default light grey (exclude primary and Needs Attention - they get own active state) */
    div.stButton > button:not([data-testid^="baseButton-na_btn_"]):not([kind="primary"]),
    div[data-testid="stButton"] button:not([data-testid^="baseButton-na_btn_"]):not([kind="primary"]) {
      border-radius: 999px !important;
      padding: 0.55rem 1.3rem !important;
      border: none !important;
      font-weight: 600 !important;
      transition: all 0.18s ease !important;
      background: #e5e7eb !important;
      color: #111827 !important;
    }
    /* Needs Attention tab buttons - default grey when not selected */
    button[data-testid^="baseButton-na_btn_"] {
      border-radius: 999px !important;
      padding: 0.55rem 1.3rem !important;
      border: none !important;
      font-weight: 600 !important;
      background: #e5e7eb !important;
      color: #111827 !important;
    }
    button[data-testid^="baseButton-na_btn_"]:hover {
      background: #2563eb !important;
      color: white !important;
    }
    button[data-testid^="baseButton-na_btn_"]:hover * { color: white !important; }
    
    /* Primary buttons (selected nav, Search, etc.) - blue with white text */
    div[data-testid="stButton"] button[kind="primary"],
    div.stButton > button[kind="primary"] {
      background: #2563eb !important;
      background-color: #2563eb !important;
      color: white !important;
      border: none !important;
    }
    div[data-testid="stButton"] button[kind="primary"] *,
    div.stButton > button[kind="primary"] * { color: white !important; }
    
    /* Other buttons hover - turn blue */
    div.stButton > button:not([data-testid^="baseButton-na_btn_"]):hover,
    div[data-testid="stButton"] button:not([data-testid^="baseButton-na_btn_"]):hover {
      background: #2563eb !important;
      color: white !important;
      transform: translateY(-1px) !important;
    }
    .btn-primary{
      background: var(--brand); color:#fff; border:1px solid #1042a8;
      border-radius: 999px !important; padding: 8px 12px; cursor:pointer;
      box-shadow: 0 2px 8px rgba(20,89,210,.25);
      font-weight: 800;
    }

    /* Needs Attention cards - light background by tab (styled via container key: na_bg_due, na_bg_overdue, etc.) */
    [class*="st-key-na_bg_due"] { background: #eff6ff !important; border: 1px solid #bfdbfe !important; border-radius: 12px !important; box-shadow: 0 2px 8px rgba(0,0,0,.05) !important; }
    [class*="st-key-na_bg_overdue"] { background: #fef2f2 !important; border: 1px solid #fecaca !important; border-radius: 12px !important; box-shadow: 0 2px 8px rgba(0,0,0,.05) !important; }
    [class*="st-key-na_bg_disputed"] { background: #fffbeb !important; border: 1px solid #fde68a !important; border-radius: 12px !important; box-shadow: 0 2px 8px rgba(0,0,0,.05) !important; }
    [class*="st-key-na_bg_other"] { background: #f9fafb !important; border: 1px solid #e5e7eb !important; border-radius: 12px !important; box-shadow: 0 2px 8px rgba(0,0,0,.05) !important; }
    .na-list{ display:flex; flex-direction:column; gap:10px; }
    .na-item{
      background:#fff; border:1px solid #e6e8ee; border-radius: 12px;
      padding: 8px 10px; box-shadow: 0 2px 10px rgba(2,8,23,.05);
      display:flex; justify-content:space-between; align-items:flex-start; gap:8px;
      width:100%; min-height:92px; box-sizing:border-box;
      overflow:hidden;
    }
    .na-item .na-left{ flex:1; min-width:0; overflow:hidden; }
    .na-item .na-ref, .na-item .na-vendor, .na-item .na-actions{
      overflow:hidden; text-overflow:ellipsis; word-wrap:break-word; max-width:100%;
    }
    .na-left{ display:flex; flex-direction:column; align-items:flex-start; gap:3px; }
    .na-ref{ font-weight:900; letter-spacing:.2px; font-size:14px; }
    .na-meta{ display:flex; gap:10px; color:#64748b; font-size:12px; }
    .tag{
      font-size:11px; padding:2px 8px; border-radius:999px; border:1px solid #e5e7eb; background:#fff; color:#475569; font-weight:800;
    }
    .tag.overdue{ background:#fde7e9; color:#b42318; border-color:#f3b4b8; }
    .tag.unpaid{ background:#fff4e5; color:#b54708; border-color:#f7cf97; }

    /* Click to view button - styled as blue link */
    button[data-testid^="baseButton-na_card_"] {
      background: transparent !important; border: none !important;
      box-shadow: none !important; color: #2563eb !important;
      font-weight: 500 !important; font-size: 13px !important;
      padding: 4px 0 0 0 !important; margin-top: 2px !important;
      text-decoration: none !important; cursor: pointer !important;
    }
    button[data-testid^="baseButton-na_card_"]:hover {
      color: #1d4ed8 !important; text-decoration: underline !important;
    }

    /* Panel containment */
     .panel-wrap { width: 100%; position:relative; }
    .panel-wrap { width: 100%; position:relative; padding:14px; box-sizing:border-box; }
    .panel-wrap .stAltairChart,
    .panel-wrap .vega-embed,
    .panel-wrap canvas { max-width: 100% !important; }
    .panel-wrap * { box-sizing: border-box; }
    /* Make panel content visually consistent with mock */
    .panel-wrap > .stMarkdown, .panel-wrap > div { max-width:100%; }
     [data-testid="stContainer"] > div[role="group"] { border-radius: 14px; }
     [data-testid="stDataFrame"] { max-width: 100%; overflow: hidden; }

     /* Hide Streamlit app chrome that tmay obscure custom header (MainMenu / header / footer)
         This hides the built-in deploy/three-dot menu so our branding_bar stays visible. */
     #MainMenu { visibility: hidden !important; }
     header { visibility: hidden !important; }
     footer { visibility: hidden !important; }

     /* Ensure our brandbar remains on top */
     .brandbar{ z-index: 9999 !important; }
    .below-header-spacer{ height:24px; }
    /* Make KPI row and panels align tighter similar to mock */
    .stColumns { gap: 12px !important; }
    .stContainer { padding: 10px 0 !important; }
    .kpi{ min-height:98px; padding:12px; }
    .p2p-header{ max-width:1180px; margin-left:auto; margin-right:auto; }

    /* ========== GENIE PAGE STYLES ========== */

    /* Genie two-column row: pin Saved Insights and AI Assistant to top (same level) */
    [data-testid="stHorizontalBlock"]:has(.genie-left-col-top) {
      align-items: flex-start !important;
    }
    [data-testid="stHorizontalBlock"]:has(.genie-left-col-top) [data-testid="column"] {
      align-items: flex-start !important;
      align-self: flex-start !important;
    }
    [data-testid="stHorizontalBlock"]:has(.genie-left-col-top) [data-testid="column"] > div {
      align-items: flex-start !important;
      padding-top: 0 !important;
      margin-top: 0 !important;
    }

    /* Genie tile: card with attached Analyze button at bottom */
    form:has(.genie-tile-card) {
      margin: 0;
      padding: 0;
      border: none !important;
      box-shadow: none !important;
      background: transparent;
      display: flex;
      flex-direction: column;
      gap: 0;
    }
    form:has(.genie-tile-card) .genie-tile-card {
      border-bottom-left-radius: 0 !important;
      border-bottom-right-radius: 0 !important;
      border-bottom: 0 !important;
    }
    form:has(.genie-tile-card) [data-testid="stFormSubmitButton"] {
      margin: 0 !important;
      padding: 0 !important;
    }
    form:has(.genie-tile-card) [data-testid="stFormSubmitButton"] > button {
      width: 100% !important;
      border-radius: 0 0 14px 14px !important;
      border: 1.5px solid #e5e7eb !important;
      border-top: 0 !important;
      background: #3b38ff !important;
      color: #fff !important;
      font-weight: 800 !important;
      font-size: 14px !important;
      padding: 10px 12px !important;
      box-shadow: 0 2px 8px rgba(59,56,255,.12) !important;
      cursor: pointer !important;
    }
    form:has(.genie-tile-card[style*="#5046e5"]) [data-testid="stFormSubmitButton"] > button {
      border-color: #5046e5 !important;
    }
    /* Remove checkboxes / extra UI in tile forms completely */
    form:has(.genie-tile-card) input[type="checkbox"],
    form:has(.genie-tile-card) [role="checkbox"],
    form:has(.genie-tile-card) .stCheckbox,
    form:has(.genie-tile-card) [data-testid="stCheckbox"],
    form:has(.genie-tile-card) label { display: none !important; visibility: hidden !important; }

    /* Quick Analysis Cards */
    .qa-grid {
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 16px;
      margin: 20px 0;
    }
    
    .qa-card {
      background: #fff;
      border: 1.5px solid #e6e8ee;
      border-radius: 16px;
      padding: 24px 20px;
      cursor: pointer;
      transition: all 0.3s ease;
      box-shadow: 0 2px 8px rgba(2,8,23,.04);
    }
    
    .qa-card:hover {
      border-color: #7c3aed;
      box-shadow: 0 8px 24px rgba(124,58,237,.15);
      transform: translateY(-2px);
    }
    
    .qa-card.selected {
      background: linear-gradient(135deg, #f5f3ff 0%, #ede9fe 100%);
      border-color: #7c3aed;
    }
    
    .qa-icon {
      width: 48px;
      height: 48px;
      border-radius: 12px;
      display: flex;
      align-items: center;
      justify-content: center;
      font-size: 24px;
      margin-bottom: 16px;
      color: #fff;
      background: linear-gradient(135deg, #7c3aed 0%, #9333ea 100%);
    }
    
    .qa-title {
      font-size: 18px;
      font-weight: 800;
      color: #0f172a;
      margin-bottom: 8px;
      letter-spacing: 0.2px;
    }
    
    .qa-desc {
      font-size: 14px;
      color: #64748b;
      line-height: 1.5;
    }
    
    /* Sidebar Styles */
    .genie-sidebar {
      background: #fff;
      border: 1.5px solid #e6e8ee;
      border-radius: 16px;
      padding: 20px;
      min-height: 400px;
    }
    
    .sidebar-section {
      margin-bottom: 24px;
    }
    
    .sidebar-title {
      font-size: 16px;
      font-weight: 800;
      color: #0f172a;
      margin-bottom: 12px;
      display: flex;
      align-items: center;
      justify-content: space-between;
    }
    
    .sidebar-item {
      padding: 10px 12px;
      border-radius: 8px;
      cursor: pointer;
      transition: all 0.2s ease;
      margin-bottom: 6px;
      font-size: 14px;
      color: #475569;
    }
    
    .sidebar-item:hover {
      background: #f8fafc;
    }
    
    .sidebar-item.active {
      background: #ede9fe;
      color: #7c3aed;
      font-weight: 700;
    }
    
    /* AI Assistant Panel */
    .ai-panel {
      background: #fff;
      border: 1.5px solid #e6e8ee;
      border-radius: 16px;
      padding: 24px;
      box-shadow: 0 2px 8px rgba(2,8,23,.04);
      min-height: 400px;
    }
    
    .ai-header {
      font-size: 16px;
      font-weight: 800;
      color: #0f172a;
      margin-bottom: 20px;
    }
    
    /* Prescriptive box: even font, text contained */
    .prescriptive-content, .prescriptive-content * {
      font-family: inherit !important;
      font-size: 14px !important;
      line-height: 1.6 !important;
      color: #0f172a !important;
      font-weight: inherit !important;
    }
    .prescriptive-content strong, .prescriptive-content b {
      font-weight: 700 !important;
      color: #0f172a !important;
    }
    .prescriptive-content {
      word-wrap: break-word;
      overflow-wrap: break-word;
      max-width: 100%;
      box-sizing: border-box;
    }
    div[data-testid="stExpander"] div[class*="prescriptive-content"],
    div[data-testid="stExpander"] .prescriptive-content {
      word-wrap: break-word;
      overflow-wrap: break-word;
      max-width: 100%;
    }

    /* Empty State for AI Chat */
    .ai-empty-state {
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      padding: 60px 20px;
      text-align: center;
      min-height: 300px;
    }
    
    .ai-empty-icon {
      width: 64px;
      height: 64px;
      border-radius: 50%;
      background: #e0f2fe;
      display: flex;
      align-items: center;
      justify-content: center;
      margin-bottom: 20px;
    }
    
    .ai-empty-icon span {
      font-size: 28px;
      color: #0284c7;
    }
    
    .ai-empty-title {
      font-size: 18px;
      font-weight: 800;
      color: #0f172a;
      margin-bottom: 8px;
    }
    
    .ai-empty-desc {
      font-size: 14px;
      color: #64748b;
      max-width: 400px;
    }
    
    /* Chat Input */
    .chat-input-container {
      display: flex;
      gap: 8px;
      margin-top: 20px;
      padding-top: 16px;
      border-top: 1px solid #e6e8ee;
    }
    
    /* Saved Insights Empty State */
    .empty-analysis-box {
      border: 2px dashed #e2e8f0;
      border-radius: 12px;
      padding: 40px 20px;
      text-align: center;
      margin-top: 20px;
    }
    
    .empty-analysis-title {
      font-size: 15px;
      font-weight: 700;
      color: #0f172a;
      margin-bottom: 6px;
    }
    
    .empty-analysis-desc {
      font-size: 13px;
      color: #94a3b8;
    }
    
    /* Key Insights Box */
    .insights-box {
      background: linear-gradient(135deg, #e0f2fe 0%, #dbeafe 100%);
      border: 1.5px solid #bae6fd;
      border-radius: 14px;
      padding: 20px;
      margin-bottom: 24px;
    }
    
    /* Explore Further Buttons */
    .explore-btn {
      background: #7c3aed;
      color: #fff;
      border: none;
      border-radius: 10px;
      padding: 14px 20px;
      font-size: 14px;
      font-weight: 800;
      cursor: pointer;
      transition: all 0.2s ease;
      box-shadow: 0 4px 12px rgba(124,58,237,.2);
    }
    
    .explore-btn:hover {
      background: #6d28d9;
      transform: translateY(-1px);
    }
    
    /* Send Button Style (Genie chat — match screenshot blue) */
    [data-testid="stButton"] button[kind="primary"] {
      background: #007BFF !important;
      border: none !important;
    }
        /* ============================================= */
    /* NEW: "Cover Line" / Boxed Sections            */
    /* ============================================= */
    .section-card {
        background: #FFFFFF;
        border: 1px solid #E2E8F0;
        border-radius: 12px;
        padding: 24px;
        margin-bottom: 24px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.02);
    }
    /* 1. KPI GRID */
    .inv-kpi-row {
        display: grid;
        grid-template-columns: repeat(4, 1fr);
        gap: 20px;
        width: 100%;
        margin-bottom: 24px;
    }
    .inv-kpi-card {
        padding: 20px;
        border-radius: 12px;
        display: flex;
        align-items: center;
        gap: 16px;
        min-height: 100px;
        transition: box-shadow 0.2s ease;
    }

    /* UPDATED: Dark Navy Total Card (Image 2) */
    .inv-card-total { background: #E8F1FD; border: 1px solid #EDE9FE; }
    .inv-card-total .icon-box { background: rgba(255,255,255,0.1); color: #fff; }
    .inv-card-total .label { color: #7C2D12; font-size: 13px; font-weight: 600; }
    .inv-card-total .value { color: #7C2D12; }
    /* Other Cards (Pastel) */
    .inv-card-pending { background: #FFFBEB; border: 1px solid #FDE68A; }
    .inv-card-pending .icon-box { background: #FFEDD5; color: #C2410C; }
    .inv-card-pending .label { color: #9A3412; font-size: 13px; font-weight: 600; }
    .inv-card-pending .value { color: #7C2D12; }
    .inv-card-blocked { background: #FEF2F2; border: 1px solid #FEE2E2; }
    .inv-card-blocked .icon-box { background: #FEE2E2; color: #B91C1C; }
    .inv-card-blocked .label { color: #991B1B; font-size: 13px; font-weight: 600; }
    .inv-card-blocked .value { color: #7F1D1D; }
    .inv-card-overdue { background: #F5F3FF; border: 1px solid #EDE9FE; }
    .inv-card-overdue .icon-box { background: #EDE9FE; color: #6D28D9; }
    .inv-card-overdue .label { color: #5B21B6; font-size: 13px; font-weight: 600; }
    .inv-card-overdue .value { color: #4C1D95; }
    .icon-box {
        width: 44px; height: 44px; border-radius: 10px;
        display: flex; align-items: center; justify-content: center;
        font-size: 20px; flex-shrink: 0;
    }
    .inv-kpi-content .value { font-size: 26px; font-weight: 800; line-height: 1; margin-bottom: 4px; }

    /* 2. BUTTON COLORS */
    /* Search & Next (Blue) */
    .st-key-btn_inv_search button, .st-key-inv_next button {
        background-color: #2563EB  !important;
        border: 1px solid #E2E8F0 !important;
        color: #ffffff !important;
    }
    .st-key-inv_next button:hover { background-color: #1D4ED8 !important; color: #ffffff !important; }

    .st-key-btn_inv_search button:hover { background-color: #1D4ED8 !important; color: #ffffff !important; }
    /* Reset Button (Dark Slate Text with Border) */
    /* Invoice Reset Button */
    .st-key-btn_inv_reset button {
    background-color: #FEE2E2 !important;
    color: #991B1B !important;
    border: 1px solid #FCA5A5 !important;
    font-weight: 600;
    }
    .st-key-btn_inv_reset button:hover {
    background-color: #FCA5A5 !important;
    color: #7F1D1D !important;
    }
    /* Download (Red) */
    .st-key-btn_download_csv button {
    background-color: #2563EB !important; /* Primary Blue */
    color: #ffffff !important;
    border: none !important;
    font-weight: 600 !important;
    padding: 6px 14px !important;
    font-size: 13px !important;
    border-radius: 8px !important;
    }
    .st-key-btn_download_csv button:hover {
    background-color: #1D4ED8 !important;
    }
    /* Prev (Grey) */
    .st-key-inv_prev button {
        background-color: #F8FAFC !important;
        border: 1px solid #E2E8F0 !important;
        color: #64748B !important;
    }
    .st-key-inv_prev button:hover { background-color: #0046CC !important;  color: #ffffff !important; }

    /* 3. LAYOUT TWEAKS */
    div[data-testid="stSelectbox"] > div > div { min-height: 42px !important; }
    div[data-testid="stTextInput"] > div > div { min-height: 42px !important; }
    #MainMenu, header, footer { visibility: hidden !important; }

    /* === Invoice Page Section Wrapper === */
    .inv-section-wrapper {
    border: 1px solid #E5E7EB;
    border-radius: 16px;
    padding: 18px;
    margin-bottom: 22px;
    background: #ffffff;
    }
    /* subtle fade line effect */
    .inv-section-wrapper::before {
    content: "";
    display: block;
    height: 1px;
    background: linear-gradient(to right, transparent, #E5E7EB, transparent);
    margin-bottom: 16px;
    }
 
     </style>
    """, unsafe_allow_html=True)


# Embedded YASH logo (base64) - no external file needed

def branding_bar():
    user = _get_current_user_display()
    if not user:
        user = "User"
    cur_page = st.session_state.get('page','dashboard')

    # Full header with columns
    header_cols = st.columns([1, 2, 1])
    
    # Left: Title (ProcureIQ)
    with header_cols[0]:
        st.markdown("""
        <div style='display:flex;align-items:center;gap:12px;padding:8px 0;min-height:52px;'>
            <div>
                <div style='font-size:30px;font-weight:900;letter-spacing:.2px;line-height:1.2;'>ProcureIQ</div>
                <div style='color:#64748b;font-size:12px;'>P2P Analytics</div>
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    # Center: Navigation (Dashboard, Genie, Forecast, Invoices)
    with header_cols[1]:
        nav_cols = st.columns([1, 1, 1, 1])
        
        with nav_cols[0]:
            btn_type = 'primary' if cur_page == 'dashboard' else 'secondary'
            if st.button(' Dashboard', key='nav_dashboard', use_container_width=True, type=btn_type):
                st.session_state['page'] = 'dashboard'
                st.session_state['show_analysis'] = False
                st.session_state['analyst_response'] = None
                st.query_params.from_dict({"page": "dashboard"})
                st.rerun()
        
        with nav_cols[1]:
            btn_type = 'primary' if cur_page == 'genie' else 'secondary'
            if st.button(' Genie', key='nav_genie', use_container_width=True, type=btn_type):
                st.session_state['page'] = 'genie'
                st.session_state['show_analysis'] = False
                st.session_state['analyst_response'] = None
                st.query_params.from_dict({"page": "genie"})
                st.rerun()
        
        with nav_cols[2]:
            btn_type = 'primary' if cur_page == 'cash_flow' else 'secondary'
            if st.button(' Forecast', key='nav_cash_flow', use_container_width=True, type=btn_type):
                st.session_state['page'] = 'cash_flow'
                st.query_params.from_dict({"page": "cash_flow"})
                st.rerun()
        
        with nav_cols[3]:
            btn_type = 'primary' if cur_page == 'invoice' else 'secondary'
            if st.button(' Invoices', key='nav_invoice', use_container_width=True, type=btn_type):
                st.session_state['page'] = 'invoice'
                st.query_params.from_dict({"page": "invoice"})
                st.rerun()

        # Force active nav button to blue + white (specific selector so it overrides global grey)
        nav_keys = {"dashboard": "nav_dashboard", "genie": "nav_genie", "cash_flow": "nav_cash_flow", "invoice": "nav_invoice"}
        nav_key = nav_keys.get(cur_page)
        if nav_key:
            sel = f"div[data-testid='stButton'] button[data-testid='baseButton-{nav_key}']"
            st.markdown(f"""
            <style>
            {sel} {{
                background: #2563eb !important;
                background-color: #2563eb !important;
                color: white !important;
                border: none !important;
            }}
            {sel} * {{ color: white !important; }}
            </style>
            """, unsafe_allow_html=True)
    
    # Right: YASH Technologies logo (embedded in this file — no external file or URL)
    with header_cols[2]:
        st.markdown(f"""
        <div style='display:flex;align-items:center;justify-content:flex-end;padding:4px 0;'>
            <img class="yash-header-logo" src="data:image/png;base64,{YASH_LOGO_B64}" alt="YASH Technologies" />
        </div>
        """, unsafe_allow_html=True)
    
    # Divider
    st.markdown("<hr style='margin:4px 0 16px 0;border:none;border-top:1.5px solid #e5e7eb;'>", unsafe_allow_html=True)

def kpi_tile(title:str, value:str, delta_text:str=None, is_up_change:bool=True, up_is_good:bool=True):
    """
    - is_up_change: True if current >= prior (arrow orientation).
    - up_is_good:   True makes 'up' green, False makes 'up' red (kept True to match mock).
    """
    arrow_up_svg = """
    <svg class="delta-icon" viewBox="0 0 20 20" fill="currentColor" aria-hidden="true">
      <path d="M10 3l6 6H4l6-6zm0 14V6h-2v11h2z"/>
    </svg>
    """
    arrow_down_svg = """
    <svg class="delta-icon" viewBox="0 0 20 20" fill="currentColor" aria-hidden="true">
      <path d="M10 17l-6-6h12l-6 6zm0-14v11h2V3h-2z"/>
    </svg>
    """
    is_good_color = (is_up_change and up_is_good) or ((not is_up_change) and (not up_is_good))
    color_cls = "up" if is_good_color else "down"
    arrow_svg = arrow_up_svg if is_up_change else arrow_down_svg

    # Exact color and style mapping for pixel-perfect match
    color_map = {
      "TOTAL SPEND": "#fff7e0",              # yellow
      "ACTIVE PO'S": "#e6f3ff",              # blue
      "PENDING INVOICES": "#e0f7fa",         # cyan
      "AVG PAYMENT TIME": "#f3e8ff",         # purple
      "ACTIVE VENDORS": "#ffe6ef",           # pink
      "TOTAL UNPAID": "#fff7e0",             # reuse yellow
      "OVERDUE NOW": "#ffe6ef",              # soft pink
      "DUE NEXT 30 DAYS": "#e6f3ff",         # soft blue
      "% DUE ≤30 DAYS": "#e0f7fa",           # soft teal
      # GR/IR KPIs – align with dashboard palette
      "TOTAL GR/IR": "#fff7e0",              # same as TOTAL UNPAID
      "% > 60 DAYS": "#ffe6ef",              # risk – soft pink
      "> 60 DAYS AMOUNT": "#e6f3ff",         # blue
      "> 60 DAYS ITEMS": "#e0f7fa",          # teal
    }
    border_map = {
      "TOTAL SPEND": "#ffe6a1",
      "ACTIVE PO'S": "#b3e0ff",
      "PENDING INVOICES": "#b2ebf2",
      "AVG PAYMENT TIME": "#d1b3ff",
      "ACTIVE VENDORS": "#ffb3d6",
      "TOTAL UNPAID": "#ffe6a1",
      "OVERDUE NOW": "#ffb3d6",
      "DUE NEXT 30 DAYS": "#b3e0ff",
      "% DUE ≤30 DAYS": "#b2ebf2",
      "TOTAL GR/IR": "#ffe6a1",
      "% > 60 DAYS": "#ffb3d6",
      "> 60 DAYS AMOUNT": "#b3e0ff",
      "> 60 DAYS ITEMS": "#b2ebf2",
    }
    text_map = {
      "TOTAL SPEND": "#1a1a1a",
      "ACTIVE PO'S": "#1a1a1a",
      "PENDING INVOICES": "#1a1a1a",
      "AVG PAYMENT TIME": "#1a1a1a",
      "ACTIVE VENDORS": "#1a1a1a",
      "TOTAL UNPAID": "#1a1a1a",
      "OVERDUE NOW": "#1a1a1a",
      "DUE NEXT 30 DAYS": "#1a1a1a",
      "% DUE ≤30 DAYS": "#1a1a1a",
      "TOTAL GR/IR": "#1a1a1a",
      "% > 60 DAYS": "#1a1a1a",
      "> 60 DAYS AMOUNT": "#1a1a1a",
      "> 60 DAYS ITEMS": "#1a1a1a",
    }
    bg = color_map.get(title.upper(), "#fff")
    border = border_map.get(title.upper(), "#e6e8ee")
    text = text_map.get(title.upper(), "#1a1a1a")
    st.markdown(
      f"""
      <div class="kpi" style="background:{bg};border:1.5px solid {border};padding:18px 18px 12px 18px;min-height:110px;box-shadow:0 2px 8px rgba(2,8,23,.04);border-radius:16px;">
        <div class="title" style="font-size:13px;font-weight:800;color:#888;margin-bottom:2px;letter-spacing:.5px;">{title}</div>
        <div class="value" style="font-size:32px;font-weight:900;color:{text};margin-bottom:2px;">{value}</div>
        {f'<div class="delta {color_cls}" style="margin-top:2px;font-size:15px;font-weight:900;display:flex;align-items:center;gap:6px;letter-spacing:.2px;">{arrow_svg}<span>{delta_text}</span></div>' if delta_text and delta_text!='—' else ''}
      </div>
      """,
      unsafe_allow_html=True
    )

def banner_insight(title:str, severity:str, body:str, cta:str="Review Contract"):
    sev = severity.lower()
    sev_cls = "low" if sev == "low" else "med" if sev == "med" else "high"
    badge_color = {
        "high": "#d32f2f",
        "med": "#f59e0b",
        "low": "#118d57"
    }.get(sev_cls, "#d32f2f")
    st.markdown(
        f"""
        <div class="banner" style="background:linear-gradient(135deg,#e9e0ff 0%,#f2e8ff 100%);border:1.5px solid #e0d7fa;padding:20px 24px 20px 24px;box-shadow:0 2px 8px rgba(2,8,23,.04);border-radius:18px;display:flex;align-items:flex-start;gap:16px;">
          <div style="font-size:26px;line-height:1;"></div>
          <div style="flex:1;">
            <div style="display:flex;gap:10px;align-items:center;">
              <div style="font-weight:900;font-size:17px;">{title}</div>
              <span style="background:{badge_color};color:#fff;font-size:12px;font-weight:800;padding:2px 12px;border-radius:999px;">{severity}</span>
            </div>
            <div style="color:#5a5a5a;margin-top:7px;font-size:15px;">{body}</div>
          </div>
          <div style="margin-left:auto;">
            <button class="btn-primary" style="background:#3b38ff;color:#fff;font-weight:800;font-size:14px;padding:8px 18px;border-radius:10px;border:none;box-shadow:0 2px 8px rgba(59,56,255,.12);cursor:pointer;">{cta}</button>
          </div>
        </div>
        """,
        unsafe_allow_html=True
    )

def empty_state(msg:str):
    st.markdown(f"""<div class="empty">{msg}</div>""", unsafe_allow_html=True)

# ---------- Altair charts ----------

def _apply_props(chart: alt.Chart, height: int, title: Optional[str]):
    c = chart.properties(height=height)
    if title is not None and str(title).strip():
        c = c.properties(title=str(title)).configure_title(color='#0f172a')
    return c

def _pick_chart_columns(df: pd.DataFrame) -> tuple:
    """Pick best (x_categorical, y_numeric) for bar chart. Prefer common P2P column names."""
    if df is None or df.empty or len(df.columns) < 2:
        return (None, None)
    cols = list(df.columns)
    # Preferred categorical (x) column names (order matters)
    cat_prefer = ("OPPORTUNITY_AREA", "AGING_BUCKET", "INVOICE_STATUS", "PO_PURPOSE", "VENDOR_NAME",
                  "DRIVER_VALUE", "DRIVER", "STATUS", "MONTH", "FINDING", "PURCHASE_ORDER_REFERENCE")
    # Preferred numeric (y) column names
    num_prefer = ("AMOUNT", "TOTAL_AMOUNT", "SPEND_CHANGE", "INVOICE_COUNT", "TOTAL_SPEND", "SPEND", "CNT",
                  "PCT_OF_SPEND", "FULL_PAYMENT_RATE_PCT", "ON_TIME_PAYMENT_RATE_PCT")
    upper_cols = {str(c).upper(): c for c in cols}
    x_col = None
    for name in cat_prefer:
        if name in upper_cols:
            x_col = upper_cols[name]
            break
    if not x_col:
        for c in cols:
            try:
                if pd.api.types.is_string_dtype(df[c]) or pd.api.types.is_object_dtype(df[c]):
                    x_col = c
                    break
            except Exception:
                pass
    if not x_col:
        x_col = cols[0]
    y_col = None
    for name in num_prefer:
        if name in upper_cols and upper_cols[name] != x_col:
            y_col = upper_cols[name]
            break
    if not y_col:
        for c in cols:
            if c == x_col:
                continue
            try:
                if pd.api.types.is_numeric_dtype(df[c]):
                    y_col = c
                    break
            except Exception:
                pass
    if not y_col:
        y_col = cols[1] if len(cols) > 1 else None
    return (x_col, y_col)

def alt_bar(df, x, y, title: Optional[str]=None, horizontal=False, color="#1459d2", height=320):
    if df is None or df.empty:
        empty_state("No data for this chart.")
        return
    data = df.copy()
    if horizontal:
        chart = alt.Chart(data).mark_bar(color=color, cornerRadiusTopLeft=4, cornerRadiusTopRight=4).encode(
            x=alt.X(y, type='quantitative', axis=alt.Axis(grid=False, title=None, format="~s")),
            y=alt.Y(x, type='nominal', sort='-x', axis=alt.Axis(grid=False, title=None)),
            tooltip=[x, alt.Tooltip(y, title="Value", format=",.0f")]
        )
    else:
        chart = alt.Chart(data).mark_bar(color=color, cornerRadiusTopLeft=4, cornerRadiusTopRight=4).encode(
            x=alt.X(x, type='nominal', axis=alt.Axis(grid=False, title=None)),
            y=alt.Y(y, type='quantitative', axis=alt.Axis(grid=False, title=None, format="~s")),
            tooltip=[x, alt.Tooltip(y, title="Value", format=",.0f")]
        )
    chart = _apply_props(chart, height=height, title=title).configure_view(stroke=None)
    st.altair_chart(chart, use_container_width=True)

# Column pairs for period comparison (curr, prev) -> (curr_label, prev_label)
_PERIOD_PAIRS = (
    (("THIS_MONTH_SPEND", "LAST_MONTH_SPEND"), ("This Month", "Previous Month")),
    (("CURRENT_MONTH_SPEND", "PREVIOUS_MONTH_SPEND"), ("This Month", "Previous Month")),
    (("THIS_QUARTER_SPEND", "LAST_QUARTER_SPEND"), ("This Quarter", "Previous Quarter")),
    (("THIS_YEAR_SPEND", "LAST_YEAR_SPEND"), ("This Year", "Previous Year")),
    (("CURRENT_SPEND", "PREVIOUS_SPEND"), ("Current Period", "Previous Period")),
    (("CURRENT_PERIOD_SPEND", "PREVIOUS_PERIOD_SPEND"), ("Current Period", "Previous Period")),
)

def _has_comparison_columns(df: pd.DataFrame) -> tuple:
    """Check if df has current vs previous period columns. Returns (cat_col, curr_col, prev_col, curr_label, prev_label).
    cat_col may be None for single-summary rows (e.g. one row with CURRENT_MONTH_SPEND, PREVIOUS_MONTH_SPEND)."""
    if df is None or df.empty:
        return (None, None, None, None, None)
    upper = {str(c).upper(): c for c in df.columns}
    cat_col = (upper.get("DRIVER_VALUE") or upper.get("CATEGORY") or upper.get("DRIVER") or
               upper.get("ROW_TYPE") or upper.get("OPPORTUNITY_AREA"))
    for (curr_name, prev_name), (curr_label, prev_label) in _PERIOD_PAIRS:
        curr_col = upper.get(curr_name)
        prev_col = upper.get(prev_name)
        if curr_col and prev_col:
            return (cat_col, curr_col, prev_col, curr_label, prev_label)
    return (None, None, None, None, None)

def alt_bar_comparison(df: pd.DataFrame, cat_col: Optional[str], curr_col: str, prev_col: str,
                      curr_label: str = "Current Month", prev_label: str = "Previous Month",
                      title: Optional[str] = None, height: int = 320):
    """Grouped bar chart comparing two periods. cat_col may be None for single-summary rows."""
    if df is None or df.empty:
        empty_state("No data for this chart.")
        return
    if cat_col:
        data = df[[cat_col, curr_col, prev_col]].copy()
        data[curr_col] = pd.to_numeric(data[curr_col], errors="coerce").fillna(0)
        data[prev_col] = pd.to_numeric(data[prev_col], errors="coerce").fillna(0)
        data = data.melt(id_vars=[cat_col], var_name="Period", value_name="Spend")
    else:
        # Single summary row: melt curr and prev into Period, Spend
        row = df.iloc[0]
        data = pd.DataFrame({
            "Period": [curr_label, prev_label],
            "Spend": [safe_number(row.get(curr_col), 0), safe_number(row.get(prev_col), 0)]
        })
        # Ensure Period order for x-axis
        data["_order"] = data["Period"].map({curr_label: 0, prev_label: 1})
    color_scale = alt.Scale(domain=[curr_label, prev_label], range=["#059669", "#1e88e5"])
    if cat_col:
        data["Period"] = data["Period"].replace({curr_col: curr_label, prev_col: prev_label})
        chart = alt.Chart(data).mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4).encode(
            x=alt.X(f"{cat_col}:N", axis=alt.Axis(title=None, labelAngle=-45 if len(data[cat_col].unique()) > 5 else 0)),
            y=alt.Y("Spend:Q", axis=alt.Axis(title=None, grid=False, format="~s")),
            xOffset="Period:N",
            color=alt.Color("Period:N", scale=color_scale, legend=alt.Legend(title=None)),
            tooltip=[alt.Tooltip(f"{cat_col}:N", title="Category"),
                     alt.Tooltip("Period:N"),
                     alt.Tooltip("Spend:Q", format=",.0f")]
        ).properties(height=height).configure_view(stroke=None)
    else:
        # Two bars: Current vs Previous (no category)
        chart = alt.Chart(data).mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4).encode(
            x=alt.X("Period:N", sort=alt.SortField("_order"), axis=alt.Axis(title=None)),
            y=alt.Y("Spend:Q", axis=alt.Axis(title=None, grid=False, format="~s")),
            color=alt.Color("Period:N", scale=color_scale, legend=alt.Legend(title=None)),
            tooltip=[alt.Tooltip("Period:N"),
                     alt.Tooltip("Spend:Q", format=",.0f")]
        ).properties(height=height).configure_view(stroke=None)
    if title:
        chart = chart.properties(title=title).configure_title(color="#0f172a")
    st.altair_chart(chart, use_container_width=True)

def alt_bar_actual_vs_forecast(df_monthly: pd.DataFrame,
                               month_col: str = "MONTH",
                               actual_col: str = "ACTUAL",
                               forecast_col: str = "FORECAST",
                               height: int = 320,
                               title: Optional[str] = None,
                               show_legend: bool = True):
    """Grouped bars: Actual vs Forecast (3-mo rolling mean)"""
    if df_monthly is None or df_monthly.empty:
        empty_state("No spend in selected range.")
        return

    data = df_monthly[[month_col, actual_col, forecast_col]].copy()
    try:
        data['_MONTH_DT'] = pd.to_datetime(data[month_col].astype(str) + '-01')
        data = data.sort_values('_MONTH_DT')
        data['_ORDER'] = data['_MONTH_DT'].rank(method='first').astype(int)
        data[month_col] = data[month_col].astype(str)
    except Exception:
        data['_ORDER'] = range(1, len(data) + 1)

    data = data.melt(id_vars=[month_col, '_ORDER'], var_name="Series", value_name="Value")
    # Use green for Actual and blue for Forecast to match mock
    color_scale = alt.Scale(domain=["ACTUAL","FORECAST"], range=["#2fbf7a", "#1e88e5"])

    legend_obj = alt.Legend(title=None, orient='top', direction='horizontal', labelFontSize=12, symbolSize=160) if show_legend else None
    chart = alt.Chart(data).mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4).encode(
        x=alt.X(f"{month_col}:N",
            sort=alt.SortField('_ORDER', order='ascending'),
            axis=alt.Axis(title=None, labelAngle=0, tickSize=0)),
        xOffset='Series:N',
        y=alt.Y('Value:Q', axis=alt.Axis(title=None, grid=False, format="~s")),
        color=alt.Color('Series:N', scale=color_scale, legend=legend_obj),
        tooltip=[alt.Tooltip(f"{month_col}:N", title="Month"),
                 alt.Tooltip('Series:N'),
                 alt.Tooltip('Value:Q', format=",.0f")]
    ).properties(height=height).configure_view(stroke=None)

    if title:
        chart = chart.properties(title=title).configure_title(color='#0f172a')

    st.altair_chart(chart, use_container_width=True)

def alt_donut_status(df: pd.DataFrame,
                     label_col: str = "STATUS",
                     value_col: str = "CNT",
                     title: Optional[str] = None,
                     height: int = 340,
                     min_label_pct: float = 0.01,
                     show_legend: bool = True):
    """
    Donut optimized for named categories (Paid, Pending, Disputed, Rejected, Other).
    - Stable order and brand-aligned colors
    - Percent labels only for slices >= min_label_pct
    - Center total label + summary legend like the mock
    """
    if df is None or df.empty or df[value_col].fillna(0).sum() <= 0:
        empty_state("No counts to plot for this range.")
        if df is not None and not df.empty:
            st.dataframe(df, use_container_width=True)
        return

    data = df.copy()
    data[label_col] = data[label_col].astype(str).str.title()
    total = float(data[value_col].sum())
    data['pct'] = data[value_col] / total

    order = ["Paid", "Pending", "Disputed", "Other"]
    palette = {
        "Paid": "#22C55E",
        "Pending": "#FBBF24",
        "Disputed": "#EF4444",
        "Other": "#1E88E5",
    }

    present = set(data[label_col].unique())
    for s in order:
        if s not in present:
            data = pd.concat([data, pd.DataFrame({label_col: [s], value_col: [0], 'pct':[0.0]})], ignore_index=True)

    legend_obj = alt.Legend(title=None, orient='right', labelFontSize=12, symbolSize=160, symbolType='circle') if show_legend else None
    base = alt.Chart(data).encode(
        theta=alt.Theta(field=value_col, type='quantitative', stack=True),
        color=alt.Color(field=label_col, type='nominal',
                        scale=alt.Scale(domain=order, range=[palette[k] for k in order]),
                        legend=legend_obj),
        tooltip=[alt.Tooltip(label_col, title="Status"),
                 alt.Tooltip(value_col, title="Count", format=",.0f"),
                 alt.Tooltip("pct:Q", title="Share", format=".1%")]
    )

    arc = base.mark_arc(innerRadius=40, outerRadius=100)
    text_pct = base.transform_filter(alt.datum.pct >= min_label_pct)\
                   .mark_text(radius=115, color='#0f172a', fontSize=12, fontWeight='bold')\
                   .encode(text=alt.Text('pct:Q', format='.1%'))

    # Center total + label
    center_total = alt.Chart(data).transform_aggregate(total=f"sum({value_col})")\
        .mark_text(fontSize=24, fontWeight='bold', color='#0f172a')\
        .encode(text='total:Q')
    center_sub = alt.Chart(pd.DataFrame({'lbl':['TOTAL']}))\
        .mark_text(dy=18, fontSize=11, color='#64748b')\
        .encode(text='lbl:N')

    chart = (arc + text_pct + center_total + center_sub)
    chart = _apply_props(chart, height=height, title=title).configure_view(stroke=None)
    st.altair_chart(chart, use_container_width=True)


def alt_line_monthly(df: pd.DataFrame, month_col: str = 'MONTH', value_col: str = 'VALUE', height: int = 140, title: Optional[str] = None):
    if df is None or df.empty:
        empty_state("No data for this chart.")
        return
    data = df.copy()
    # ensure month ordering
    try:
        data[month_col] = pd.to_datetime(data[month_col].astype(str) + '-01')
        data = data.sort_values(month_col)
        data['MONTH_LABEL'] = data[month_col].dt.strftime('%b')
    except Exception:
        data['MONTH_LABEL'] = data[month_col].astype(str)

    chart = alt.Chart(data).mark_line(point=True, color='#1e88e5').encode(
        x=alt.X('MONTH_LABEL:N', axis=alt.Axis(title=None, labelAngle=0)),
        y=alt.Y(f'{value_col}:Q', axis=alt.Axis(title=None, grid=False, format='~s')),
        tooltip=[alt.Tooltip('MONTH_LABEL:N', title='Month'), alt.Tooltip(f'{value_col}:Q', format=',.0f')]
    ).properties(height=height)
    if title:
        chart = chart.properties(title=title).configure_title(color='#0f172a')
    st.altair_chart(chart, use_container_width=True)
    
def render_cash_flow_page():
    """
    Cash Flow Forecasting & Working Capital Agent:
    - Summarize short-term cash outflows
    - Highlight overdue risk
    - Provide an action playbook (who to pay, when, and why)
    """

    tab_cf, tab_grir = st.tabs(["Cash Flow Need Forecast", "GR/IR Reconciliation"])

    with tab_cf:
        # Run cash flow forecast query (uses CASH_FLOW_UNPAID_OBLIGATIONS_VW for performance)
        cash_flow_sql = f"""
    WITH base AS (
      SELECT DOCUMENT_NUMBER, VENDOR_ID, INVOICE_AMOUNT_LOCAL, DUE_DATE, INVOICE_STATUS, DAYS_UNTIL_DUE
      FROM {DB}.{SCHEMA}.CASH_FLOW_UNPAID_OBLIGATIONS_VW
    ),
    cycle_time AS (
      SELECT AVG_PAYMENT_CYCLE_TIME_DAYS AS lag_days
      FROM {DB}.{SCHEMA}.PAYMENT_PROCESSING_CYCLE_TIME_VW
      ORDER BY YEAR DESC, MONTH DESC LIMIT 1
    ),
    buckets AS (
      SELECT
        CASE
          WHEN DAYS_UNTIL_DUE < 0 THEN 'OVERDUE_NOW'
          WHEN DAYS_UNTIL_DUE <= 7 THEN 'DUE_7_DAYS'
          WHEN DAYS_UNTIL_DUE <= 14 THEN 'DUE_14_DAYS'
          WHEN DAYS_UNTIL_DUE <= 30 THEN 'DUE_30_DAYS'
          WHEN DAYS_UNTIL_DUE <= 60 THEN 'DUE_60_DAYS'
          WHEN DAYS_UNTIL_DUE <= 90 THEN 'DUE_90_DAYS'
          ELSE 'BEYOND_90_DAYS'
        END AS FORECAST_BUCKET,
        COUNT(*) AS INVOICE_COUNT,
        SUM(INVOICE_AMOUNT_LOCAL) AS TOTAL_AMOUNT,
        MIN(DUE_DATE) AS EARLIEST_DUE,
        MAX(DUE_DATE) AS LATEST_DUE
      FROM base
      GROUP BY 1
    ),
    summary AS (
      SELECT 'TOTAL_UNPAID' AS FORECAST_BUCKET, SUM(INVOICE_COUNT) AS INVOICE_COUNT, SUM(TOTAL_AMOUNT) AS TOTAL_AMOUNT,
        NULL AS EARLIEST_DUE, NULL AS LATEST_DUE FROM buckets
    ),
    processing_note AS (
      SELECT 'PROCESSING_LAG_DAYS' AS FORECAST_BUCKET, (SELECT lag_days FROM cycle_time) AS INVOICE_COUNT,
        NULL AS TOTAL_AMOUNT, NULL AS EARLIEST_DUE, NULL AS LATEST_DUE
    )
    SELECT * FROM summary
    UNION ALL SELECT * FROM buckets
    UNION ALL SELECT * FROM processing_note
    ORDER BY CASE FORECAST_BUCKET WHEN 'TOTAL_UNPAID' THEN 0 WHEN 'OVERDUE_NOW' THEN 1 WHEN 'DUE_7_DAYS' THEN 2
      WHEN 'DUE_14_DAYS' THEN 3 WHEN 'DUE_30_DAYS' THEN 4 WHEN 'DUE_60_DAYS' THEN 5 WHEN 'DUE_90_DAYS' THEN 6 WHEN 'BEYOND_90_DAYS' THEN 7 ELSE 8 END
        """
        try:
            cf_df = run_df(cash_flow_sql)
            if cf_df is not None and not cf_df.empty:
                # Compute business KPIs from buckets
                def _amt(bucket: str) -> float:
                    try:
                        row = cf_df.loc[cf_df["FORECAST_BUCKET"] == bucket]
                        if row.empty:
                            return 0.0
                        return safe_number(row.iloc[0]["TOTAL_AMOUNT"], 0.0)
                    except Exception:
                        return 0.0

                total_unpaid = _amt("TOTAL_UNPAID")
                overdue = _amt("OVERDUE_NOW")
                next30 = _amt("DUE_7_DAYS") + _amt("DUE_14_DAYS") + _amt("DUE_30_DAYS")
                beyond30 = max(total_unpaid - overdue - next30, 0.0)
                pct_soon = (next30 + overdue) / total_unpaid * 100 if total_unpaid else 0

                # KPI tiles styled like main dashboard (no delta arrows)
                kpi_cols = st.columns(4, gap="small")
                kpis = [
                    ("TOTAL UNPAID", abbr_currency(total_unpaid)),
                    ("OVERDUE NOW", abbr_currency(overdue)),
                    ("DUE NEXT 30 DAYS", abbr_currency(next30)),
                    ("% DUE ≤30 DAYS", f"{pct_soon:.1f}%"),
                ]
                for col, (label, value) in zip(kpi_cols, kpis):
                    with col:
                        kpi_tile(label, value, delta_text=None)

                st.markdown("<div style='height:10px;'></div>", unsafe_allow_html=True)

                # Forecast table and download
                st.markdown("**Obligations by time bucket**")
                st.dataframe(cf_df, use_container_width=True, height=320)
                st.download_button("Download forecast (CSV)", cf_df.to_csv(index=False), "cash_flow_forecast.csv", key="dl_cash_flow")
            else:
                st.info("No unpaid obligations found for the forecast buckets.")
        except Exception as e:
            st.error(f"Could not load cash flow forecast: {e}")

        st.markdown("<div style='height:20px;'></div>", unsafe_allow_html=True)

        # Cash Flow Playbook – business-friendly action tiles that route into Genie
        st.markdown("""
        <div style="margin-top:4px;margin-bottom:8px;">
          <div style="font-size:18px;font-weight:800;color:#1a1a1a;margin-bottom:4px;">Action Playbook</div>
          <div style="font-size:13px;color:#64748b;line-height:1.5;">
            Use these guided analyses to turn the forecast into decisions: <b>who to pay now</b>, <b>who to pay early</b>, and
            <b>where we are at risk of paying late</b>. Each button opens Genie with a pre-built question wired to the right verified queries.
          </div>
        </div>
        """, unsafe_allow_html=True)

        cf_tasks = [
            {
                "label": "1. Forecast cash outflow (7–90 days)",
                # Match semantic model question for cash_flow_forecast
                "question": "Forecast cash outflow for the next 7, 14, 30, 60, and 90 days",
            },
            {
                "label": "2. Invoices to pay early to capture discounts",
                # Match semantic model question for early_payment_candidates
                "question": "Which invoices should we pay early to capture discounts?",
            },
            {
                "label": "3. Optimal payment timing for this week",
                # Match semantic model question for payment_timing_recommendation
                "question": "What is the optimal payment timing strategy for this week?",
            },
            {
                "label": "4. Late payment trend and risk",
                # Match semantic model question for late_payment_trend_forecast
                "question": "Show late payment trend for forecasting",
            },
        ]

        # Render playbook as vertical buttons
        for task in cf_tasks:
            if st.button(task["label"], key=f"cf_playbook_{hash(task['label']) % 10**8}", use_container_width=True):
                # Route into Genie and auto-run the curated question
                st.session_state["page"] = "genie"
                st.session_state["genie_prefill_question"] = task["question"]
                st.query_params.from_dict({"page": "genie"})
                st.rerun()

    with tab_grir:

        # 1) GR/IR KPIs – latest outstanding balance, aging mix, and high‑risk portion
        try:
            grir_summary_sql = f"""
        WITH latest AS (
          SELECT YEAR, MONTH, INVOICE_COUNT, TOTAL_GRIR_BLNC
          FROM {DB}.{SCHEMA}.GR_IR_OUTSTANDING_BALANCE_VW
          ORDER BY YEAR DESC, MONTH DESC
          LIMIT 1
        ),
        aging AS (
          SELECT YEAR, MONTH, AGE_DAYS,
                 TOTAL_GRIR_BALANCE,
                 GRIR_OVER_30,
                 GRIR_OVER_60,
                 GRIR_OVER_90,
                 PCT_GRIR_OVER_30,
                 PCT_GRIR_OVER_60,
                 PCT_GRIR_OVER_90,
                 CNT_GRIR_OVER_30,
                 CNT_GRIR_OVER_60,
                 CNT_GRIR_OVER_90
          FROM {DB}.{SCHEMA}.GR_IR_AGING_VW
          ORDER BY YEAR DESC, MONTH DESC
          LIMIT 1
        )
        SELECT
          l.YEAR,
          l.MONTH,
          l.INVOICE_COUNT AS GRIR_ITEMS,
          l.TOTAL_GRIR_BLNC AS TOTAL_GRIR_BALANCE,
          a.GRIR_OVER_30,
          a.GRIR_OVER_60,
          a.GRIR_OVER_90,
          a.PCT_GRIR_OVER_30,
          a.PCT_GRIR_OVER_60,
          a.PCT_GRIR_OVER_90,
          a.CNT_GRIR_OVER_30,
          a.CNT_GRIR_OVER_60,
          a.CNT_GRIR_OVER_90
        FROM latest l
        LEFT JOIN aging a ON a.YEAR = l.YEAR AND a.MONTH = l.MONTH
        """
            grir_summary_df = run_df(grir_summary_sql)
        except Exception as e:
            grir_summary_df = None
            st.error(f"Could not load GR/IR summary: {e}")

        if grir_summary_df is not None and not grir_summary_df.empty:
            row = grir_summary_df.iloc[0]
            total = safe_number(row.get("TOTAL_GRIR_BALANCE", 0))
            over_60 = safe_number(row.get("GRIR_OVER_60", 0))
            pct_over_60 = safe_number(row.get("PCT_GRIR_OVER_60", 0))
            cnt_over_60 = safe_number(row.get("CNT_GRIR_OVER_60", 0))
            grir_items = safe_number(row.get("GRIR_ITEMS", 0))

            grir_cols = st.columns(4, gap="small")
            grir_kpis = [
                ("TOTAL GR/IR", abbr_currency(total)),
                ("% > 60 DAYS", f"{pct_over_60:.1f}%"),
                ("> 60 DAYS AMOUNT", abbr_currency(over_60)),
                ("> 60 DAYS ITEMS", f"{int(cnt_over_60):,}" if cnt_over_60 else "0"),
            ]
            for col, (label, value) in zip(grir_cols, grir_kpis):
                with col:
                    kpi_tile(label, value, delta_text=None)
            # Small context note under KPIs
            ym = f"{int(row.get('YEAR'))}-{int(row.get('MONTH')):02d}" if safe_number(row.get("YEAR")) and safe_number(row.get("MONTH")) else "latest period"
            st.caption(f"GR/IR position for {ym}: {int(grir_items):,} items outstanding; {pct_over_60:.1f}% of balance and {int(cnt_over_60):,} items are older than 60 days.")
        else:
            st.info("No GR/IR balance data found in the views yet.")

        st.markdown("<div style='height:16px;'></div>", unsafe_allow_html=True)

        # 2) Trend chart – GR/IR outstanding balance by month (last 12–24 periods)
        try:
            grir_trend_sql = f"""
        SELECT
          TO_DATE(TO_CHAR(YEAR) || '-' || LPAD(MONTH::STRING, 2, '0') || '-01') AS MONTH_DATE,
          INVOICE_COUNT,
          TOTAL_GRIR_BLNC
        FROM {DB}.{SCHEMA}.GR_IR_OUTSTANDING_BALANCE_VW
        ORDER BY YEAR DESC, MONTH DESC
        LIMIT 24
        """
            grir_trend_df = run_df(grir_trend_sql)
            if grir_trend_df is not None and not grir_trend_df.empty and "MONTH_DATE" in grir_trend_df.columns:
                grir_trend_df = grir_trend_df.sort_values("MONTH_DATE")
                st.markdown("**GR/IR outstanding trend (last 24 months)**")
                try:
                    alt_line_monthly(
                        grir_trend_df.rename(columns={"MONTH_DATE": "MONTH"}),
                        month_col="MONTH_DATE",
                        value_col="TOTAL_GRIR_BLNC",
                        height=220,
                        title="Total GR/IR balance over time",
                    )
                except Exception:
                    st.dataframe(grir_trend_df, use_container_width=True, height=260)
        except Exception:
            pass

        st.markdown("<div style='height:16px;'></div>", unsafe_allow_html=True)

        # 3) GR/IR clearing playbook – guided tasks that route into Genie with the right instructions
        st.markdown("""
        <div style="font-size:15px;font-weight:700;color:#0f172a;margin-bottom:6px;">GR/IR Clearing Playbook</div>
        <div style="font-size:13px;color:#64748b;margin-bottom:10px;">
          Each step opens Genie with a pre-built prompt that uses the <code>gr_ir_outstanding</code> and related verified queries
          so you get concrete actions (which POs to clear, where to chase receipts, and how much working capital you can release).
        </div>
        """, unsafe_allow_html=True)

        grir_tasks = [
            {
                "label": "1. Identify top GR/IR hotspots to clear first",
                "question": "Show GR/IR outstanding balance by month and highlight which recent months have the highest GR/IR balance so we can prioritize clearing.",
            },
            {
                "label": "2. Explain likely GR/IR root causes",
                "question": "Using GR/IR aging and outstanding balance data, explain the likely root-cause buckets (missing goods receipt, invoice not posted, price or quantity mismatch) and for each bucket suggest 2–3 concrete remediation actions.",
            },
            {
                "label": "3. Quantify working-capital benefit from clearing old GR/IR",
                "question": "Estimate the working capital that would be released by clearing all GR/IR items older than 60 and 90 days, by month.",
            },
            {
                "label": "4. Draft vendor follow-up messages for top GR/IR items",
                "question": "Based on GR/IR aging and outstanding balances, draft vendor-facing follow-up templates we can use for high-priority GR/IR items, with realistic subject lines and concise bullet points.",
            },
        ]

        for task in grir_tasks:
            if st.button(task["label"], key=f"grir_playbook_{hash(task['label']) % 10**8}", use_container_width=True):
                st.session_state["page"] = "genie"
                st.session_state["genie_prefill_question"] = task["question"]
                st.query_params.from_dict({"page": "genie"})
                st.rerun()


def render_invoice_page():
    """
    Unified Invoices UI:
      - Header
      - KPI Row (synced to filters)
      - Box 1: Search + Filters + Reset
      - Box 2: Paginated Table + CSV Download
    """

    # ---- Optional: carry over prefill from your "Needs Attention card click" and URL params ----
    params = st.query_params
    from_card_click = st.session_state.get("invoice_search_from_card")
    search_from_card = from_card_click or params.get("search_invoice")
    if isinstance(search_from_card, list):
        search_from_card = search_from_card[0] if search_from_card else None
    # Pre-fill unified search (apply once, then clear source to allow new searches)
    applied_prefill = False
    if from_card_click:
        st.session_state["inv_search_q"] = str(from_card_click)
        st.session_state.pop("invoice_search_from_card", None)
        applied_prefill = True
    elif search_from_card and not str(st.session_state.get("inv_search_q", "")).strip():
        st.session_state["inv_search_q"] = str(search_from_card)
        applied_prefill = True
    if applied_prefill:
        try:
            st.query_params.pop("search_invoice", None)
        except Exception:
            pass

    # ---- Pagination state
    if 'inv_page_idx' not in st.session_state:
        st.session_state.inv_page_idx = 0

    def reset_pagination():
        st.session_state.inv_page_idx = 0

    def reset_all():
        st.session_state["inv_search_q"] = ''
        st.session_state["inv_sel_vendor"] = 'All Vendors'
        st.session_state["inv_sel_status"] = 'All Status'
        st.session_state["inv_page_idx"] = 0
        st.session_state.pop("invoice_search_from_card", None)
        # Clear only invoice-related params so dashboard filter (preset, etc.) is preserved
        for key in list(st.query_params.keys()):
            if key in ("search_invoice", "inv_search_q"):
                try:
                    st.query_params.pop(key, None)
                except Exception:
                    pass

    # ---- 1) Header
    st.markdown("""
    <div style="margin-bottom: 12px;">
      <div style="font-size: 24px; font-weight: 800; color: #0F172A;">Invoices</div>
      <div style="font-size: 14px; color: #64748B;">Search, track and manage all invoices in one place</div>
    </div>
    """, unsafe_allow_html=True)

    # Defaults / previously selected state
    user_search = st.session_state.get('inv_search_q', '')
    user_vendor = st.session_state.get('inv_sel_vendor', 'All Vendors')
    user_status = st.session_state.get('inv_sel_status', 'All Status')
    if not user_search.strip():
        user_search = ""

    # ---- 2) Build search-only filter clauses (vendor/status only apply to All Invoices)
    filter_clause = ""
    if user_search:
        safe_q = user_search.replace("'", "''")
        filter_clause += (
            f" AND (FACT.INVOICE_NUMBER LIKE '%{safe_q}%'"
            f" OR FACT.PURCHASE_ORDER_REFERENCE LIKE '%{safe_q}%'"
            f" OR DIM.VENDOR_NAME LIKE '%{safe_q}%')"
        )

    filter_clause_hist = ""
    if user_search:
        safe_q = user_search.replace("'", "''")
        filter_clause_hist += (
            f" AND (FACT.INVOICE_NUMBER LIKE '%{safe_q}%'"
            f" OR FACT.PURCHASE_ORDER_REFERENCE LIKE '%{safe_q}%'"
            f" OR DIM.VENDOR_NAME LIKE '%{safe_q}%')"
        )

    # ---- 2b) Detect whether search is PO or Invoice (for dynamic ordering)
    search_kind = "invoice"
    if user_search:
        safe_q = user_search.replace("'", "''")
        try:
            kind_df = run_df(f"""
                SELECT
                    SUM(CASE WHEN FACT.INVOICE_NUMBER = '{safe_q}' THEN 1 ELSE 0 END) AS INVOICE_MATCHES,
                    SUM(CASE WHEN FACT.PURCHASE_ORDER_REFERENCE = '{safe_q}' THEN 1 ELSE 0 END) AS PO_MATCHES
                FROM {DB}.{SCHEMA}.FACT_ALL_SOURCES_VW FACT
            """)
            if kind_df is not None and not kind_df.empty:
                inv_matches = int(kind_df.iloc[0]["INVOICE_MATCHES"] or 0)
                po_matches = int(kind_df.iloc[0]["PO_MATCHES"] or 0)
                if po_matches > 0 and inv_matches == 0:
                    search_kind = "po"
        except Exception:
            pass

    # ---- If you'd like to keep your date-range silently applied, UNCOMMENT this block:
    # try:
    #     if "preset" in st.session_state and st.session_state.preset != "Custom":
    #         rng_start, rng_end = compute_range_preset(st.session_state.preset)
    #     elif "date_range" in st.session_state:
    #         rng_start, rng_end = st.session_state.date_range
    #     else:
    #         rng_start, rng_end = compute_range_preset("Last 30 Days")
    #     start_lit, end_lit = sql_date(rng_start), sql_date(rng_end)
    #     filter_clause += f" AND FACT.POSTING_DATE BETWEEN {start_lit} AND {end_lit}"
    # except Exception:
    #     pass

    # ---- KPIs removed per request
    
    # st.markdown('<div class="inv-section-wrapper">', unsafe_allow_html=True)
    c_search, c_btn_s, c_btn_r = st.columns([6, 1, 1], gap="small")
    with c_search:
        st.text_input(
            "Search",
            placeholder="Search by Invoice or PO Number",
            label_visibility="collapsed",
            key="inv_search_q",
            on_change=reset_pagination
        )
    with c_btn_s:
        st.button("Search", type="primary", use_container_width=True, key="btn_inv_search", on_click=reset_pagination)
    with c_btn_r:
        st.button("Reset", type="secondary", use_container_width=True, key="btn_inv_reset", on_click=reset_all)
    st.markdown("<div style='height: 8px;'></div>", unsafe_allow_html=True)

    # st.markdown('</div>', unsafe_allow_html=True)  # Commented out - no matching opening div

    # ---- 5) Invoice details (when searching)
    if user_search:
        if search_kind == "po":
            common_title = "PO Summary"
            common_select = f"""
                FACT.PURCHASE_ORDER_REFERENCE AS "PO NUMBER",
                PO_DIM.PO_DATE AS "PO DATE",
                PO_DIM.PO_CREATED_DATE AS "PO CREATED DATE",
                FACT.PO_AMOUNT AS "PO AMOUNT",
                PO_DIM.DELIVERY_DATE AS "DELIVERY DATE",
                PO_DIM.PO_DUE_DATE AS "PO DUE DATE",
                PO_DIM.PO_DOC_TYPE AS "PO TYPE",
                PO_DIM.PO_PAYMENT_TERMS AS "PO PAYMENT TERMS",
                PO_DIM.PO_RELEASE_STATUS AS "RELEASE STATUS",
                FACT.INVOICE_NUMBER AS "INVOICE NUMBER",
                FACT.POSTING_DATE AS "INVOICE DATE",
                FACT.INVOICE_AMOUNT_LOCAL AS "INVOICE AMOUNT",
                FACT.DUE_DATE AS "INVOICE DUE DATE"
            """
        else:
            common_title = "Invoice Summary"
            common_select = """
                FACT.INVOICE_NUMBER AS "INVOICE NUMBER",
                FACT.POSTING_DATE AS "INVOICE DATE",
                FACT.INVOICE_AMOUNT_LOCAL AS "INVOICE AMOUNT",
                FACT.PURCHASE_ORDER_REFERENCE AS "PO NUMBER",
                FACT.PO_AMOUNT AS "PO AMOUNT",
                FACT.DUE_DATE AS "DUE DATE",
                FACT.STATUS AS "INVOICE STATUS",
                FACT.COMPANY_CODE AS "COMPANY CODE",
                FACT.FISCAL_YEAR AS "FISCAL YEAR",
                FACT.AGING_DAYS AS "AGING DAYS"
            """
        common_sql = f"""
            SELECT
                {common_select}
            FROM {DB}.{SCHEMA}.INVOICE_STATUS_HISTORY_VW FACT
            LEFT JOIN {DB}.{SCHEMA}.DIM_VENDOR_VW DIM ON FACT.VENDOR_ID = DIM.VENDOR_ID
            LEFT JOIN {DB}.{SCHEMA}.DIM_PO_VW PO_DIM ON FACT.PURCHASE_ORDER_REFERENCE = PO_DIM.PURCHASE_ORDER_NUMBER
            WHERE 1=1 {filter_clause_hist}
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY FACT.INVOICE_NUMBER, FACT.COMPANY_CODE, FACT.FISCAL_YEAR
                ORDER BY FACT.SEQUENCE_NBR DESC
            ) = 1
        """
        status_sql = f"""
            SELECT
                FACT.INVOICE_NUMBER AS "INVOICE NUMBER",
                UPPER(FACT.STATUS) AS "STATUS",
                FACT.EFFECTIVE_DATE AS "EFFECTIVE DATE",
                FACT.STATUS_NOTES AS "STATUS NOTES"
            FROM {DB}.{SCHEMA}.INVOICE_STATUS_HISTORY_VW FACT
            LEFT JOIN {DB}.{SCHEMA}.DIM_VENDOR_VW DIM ON FACT.VENDOR_ID = DIM.VENDOR_ID
            WHERE 1=1 {filter_clause_hist}
            ORDER BY FACT.INVOICE_NUMBER, FACT.SEQUENCE_NBR
        """
        common_df = run_df(common_sql)
        status_df = run_df(status_sql)
        if common_df is not None and not common_df.empty:
            common_df = common_df.replace(r"^\s*$", np.nan, regex=True).dropna(how="all")

            # ---- Genie insights (AI-powered) – show above when viewing a single invoice ----
            if search_kind == "invoice":
                inv_col = "INVOICE NUMBER" if "INVOICE NUMBER" in common_df.columns else common_df.columns[0]
                selected_inv = str(common_df[inv_col].iloc[0]).strip() if inv_col in common_df.columns else None
                if selected_inv:
                    if "inv_processed_set" not in st.session_state:
                        st.session_state["inv_processed_set"] = set()
                    if "inv_ai_suggestion_cache" not in st.session_state:
                        st.session_state["inv_ai_suggestion_cache"] = {}
                    already_processed = selected_inv in st.session_state["inv_processed_set"]

                    inv_row = {}
                    for col in ("INVOICE_STATUS", "DUE_DATE", "AGING_DAYS", "INVOICE_AMOUNT_LOCAL", "COMPANY_CODE", "FISCAL_YEAR"):
                        mapped = col.replace("_", " ")
                        if col in common_df.columns:
                            inv_row[col] = common_df[col].iloc[0]
                        elif mapped in common_df.columns:
                            inv_row[col] = common_df[mapped].iloc[0]

                    cache = st.session_state["inv_ai_suggestion_cache"]
                    suggestion = cache.get(selected_inv, "")
                    if not suggestion:
                        status_hist_str = ""
                        if status_df is not None and not status_df.empty:
                            hist_cols = [c for c in ("STATUS", "EFFECTIVE_DATE", "STATUS_NOTES") if c in status_df.columns]
                            if hist_cols:
                                status_hist_str = status_df[hist_cols].head(5).to_string(index=False, max_colwidth=60)
                        with st.spinner("Getting Genie insights…"):
                            suggestion = _get_ai_invoice_suggestion(selected_inv, inv_row, status_history=status_hist_str)
                            cache[selected_inv] = suggestion

                    st.markdown(
                        """
                        <style>
                        .genie-insights-card {
                            width: 100%;
                            padding: 18px 24px;
                            margin: 4px 0 16px 0;
                            border-radius: 18px;
                            background: linear-gradient(90deg, #f4e3ff 0%, #e0ecff 50%, #e5f9ff 100%);
                            border: 1px solid rgba(148, 163, 184, 0.4);
                            box-shadow: 0 8px 20px rgba(15, 23, 42, 0.08);
                        }
                        .genie-insights-header { display: flex; align-items: center; justify-content: flex-start; margin-bottom: 8px; }
                        .genie-insights-title { display: flex; align-items: center; gap: 8px; font-size: 15px; font-weight: 700; color: #0f172a; }
                        .genie-insights-icon {
                            width: 26px; height: 26px; border-radius: 999px; display: flex; align-items: center; justify-content: center;
                            background: radial-gradient(circle at 30% 0%, #f97316, #7c3aed); color: white; font-size: 14px; font-weight: 700;
                        }
                        .genie-insights-body { font-size: 13px; color: #0f172a; line-height: 1.5; }
                        .genie-insights-body p { margin-bottom: 0; }
                        div.pay-fixed-container { position: fixed !important; right: 32px !important; bottom: 24px !important; z-index: 1000 !important; }
                        div.pay-fixed-container button { box-shadow: 0 10px 25px rgba(15, 23, 42, 0.35) !important; border-radius: 999px !important; padding: 10px 24px !important; min-width: 140px !important; }
                        </style>
                        """,
                        unsafe_allow_html=True,
                    )
                    sug_text = suggestion or "Review the invoice details and status above."
                    sug_html = _markdown_bold_to_html(sug_text)
                    st.markdown(
                        f"""
                        <div class="genie-insights-card">
                          <div class="genie-insights-header">
                            <div class="genie-insights-title">
                              <div class="genie-insights-icon">G</div>
                              <span>Genie insights</span>
                            </div>
                          </div>
                          <div class="genie-insights-body"><p>{sug_html}</p></div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )

                    if already_processed:
                        st.session_state["_inv_pay_status"] = "processed"
                    else:
                        inv_status = str(inv_row.get("INVOICE_STATUS", "")).upper() if inv_row else ""
                        if inv_status == "PAID":
                            st.session_state["_inv_pay_status"] = "paid"
                        else:
                            st.session_state["_inv_pay_status"] = "ready"
                            st.session_state["_inv_pay_invoice"] = selected_inv
                            st.session_state["_inv_pay_comp_code"] = str(inv_row.get("COMPANY_CODE", "")) if inv_row else ""
                            st.session_state["_inv_pay_fisc_year"] = str(inv_row.get("FISCAL_YEAR", "")) if inv_row else ""

            # Shared CSS for all HTML tables on this page
            st.markdown(
                """
                <style>
                .p2p-html-table {
                    width: 100%;
                    border-collapse: collapse;
                    margin-top: 8px;
                    font-size: 14px;
                    background-color: #ffffff;
                    border-radius: 8px;
                    overflow: hidden;
                    border: 1px solid #e5e7eb;
                }
                .p2p-html-table th {
                    background-color: #f8f9fa;
                    padding: 10px 12px;
                    text-align: left;
                    font-weight: 600;
                    color: #0f172a;
                    border-bottom: 2px solid #e5e7eb;
                }
                .p2p-html-table td {
                    padding: 10px 12px;
                    border-bottom: 1px solid #e5e7eb;
                    vertical-align: top;
                    background-color: #ffffff;
                    white-space: normal;
                    word-wrap: break-word;
                    word-break: break-word;
                }
                .p2p-html-table tr:hover td {
                    background-color: #f9fafb;
                }
                </style>
                """,
                unsafe_allow_html=True,
            )

            # Invoice summary and status history (below Genie insights)
            st.markdown(f"### {common_title}")
            # Hide company code, fiscal year, aging days from summary display (still in common_df for Proceed to Pay / Genie)
            display_df = common_df.drop(columns=["COMPANY CODE", "FISCAL YEAR", "AGING DAYS"], errors="ignore")
            st.markdown(_build_html_table(display_df), unsafe_allow_html=True)

            st.markdown("### Status History")
            if status_df is not None and not status_df.empty:
                status_df = status_df.replace(r"^\s*$", np.nan, regex=True).dropna(how="all")
                # CSS for Status History: wrap only STATUS NOTES (4th column), keep others on one line
                st.markdown(
                    """
                    <style>
                    /* Status History table: all columns no-wrap except STATUS NOTES */
                    .status-history-wrapper .p2p-html-table td,
                    .status-history-wrapper .p2p-html-table th {
                        white-space: nowrap;
                        word-wrap: normal;
                        word-break: normal;
                    }
                    /* Only STATUS NOTES column (4th column) wraps */
                    .status-history-wrapper .p2p-html-table td:nth-child(4),
                    .status-history-wrapper .p2p-html-table th:nth-child(4) {
                        white-space: normal !important;
                        word-wrap: break-word !important;
                        word-break: break-word !important;
                        max-width: 500px;
                        min-width: 250px;
                    }
                    </style>
                    """,
                    unsafe_allow_html=True,
                )
                st.markdown(
                    f'<div class="status-history-wrapper">{_build_html_table(status_df)}</div>',
                    unsafe_allow_html=True,
                )
            else:
                st.info("No status history available for this invoice.")

            tab_vendor, tab_company = st.tabs(["Vendor Info", "Company Info"])
            with tab_vendor:
                # CSS for scrollable vendor info table
                st.markdown(
                    """
                    <style>
                    .vendor-info-scrollable {
                        overflow-x: auto;
                        overflow-y: visible;
                        width: 100%;
                        margin-top: 8px;
                    }
                    .vendor-info-scrollable .p2p-html-table {
                        min-width: 100%;
                        width: max-content;
                    }
                    .vendor-info-scrollable .p2p-html-table td,
                    .vendor-info-scrollable .p2p-html-table th {
                        white-space: nowrap;
                        word-wrap: normal;
                        word-break: normal;
                    }
                    </style>
                    """,
                    unsafe_allow_html=True,
                )
                vinfo_sql = f"""
                    SELECT DISTINCT
                        DIM.VENDOR_ID AS "VENDOR ID",
                        DIM.VENDOR_NAME AS "VENDOR NAME",
                        DIM.VENDOR_NAME_2 AS "ALIAS / NAME 2",
                        DIM.COUNTRY_CODE AS "COUNTRY",
                        DIM.CITY AS "CITY",
                        DIM.POSTAL_CODE AS "POSTAL CODE",
                        DIM.STREET AS "STREET",
                        DIM.REGION_CODE AS "REGION",
                        DIM.INDUSTRY_SECTOR AS "INDUSTRY",
                        DIM.VENDOR_ACCOUNT_GROUP AS "ACCOUNT GROUP",
                        DIM.TAX_NUMBER_1 AS "TAX NUMBER 1",
                        DIM.TAX_NUMBER_2 AS "TAX NUMBER 2",
                        DIM.DELETION_FLAG AS "DELETION FLAG",
                        DIM.POSTING_BLOCK AS "POSTING BLOCK",
                        DIM.SYSTEM AS "SOURCE SYSTEM"
                    FROM {DB}.{SCHEMA}.FACT_ALL_SOURCES_VW FACT
                    LEFT JOIN {DB}.{SCHEMA}.DIM_VENDOR_VW DIM ON FACT.VENDOR_ID = DIM.VENDOR_ID
                    WHERE 1=1 {filter_clause}
                """
                vinfo = run_df(vinfo_sql)
                if vinfo is not None and not vinfo.empty:
                    vinfo = vinfo.replace(r"^\s*$", np.nan, regex=True).dropna(how="all")
                    st.markdown(
                        f'<div class="vendor-info-scrollable">{_build_html_table(vinfo)}</div>',
                        unsafe_allow_html=True,
                    )
                else:
                    st.info("No vendor information for this invoice.")
            with tab_company:
                cinfo_sql = f"""
                    SELECT DISTINCT
                        FACT.COMPANY_CODE AS "COMPANY CODE",
                        COALESCE(CC.COMPANY_NAME, 'N/A') AS "COMPANY NAME",
                        FACT.PLANT_CODE AS "PLANT CODE",
                        COALESCE(PLT.PLANT_NAME, 'N/A') AS "PLANT NAME",
                        COALESCE(CC.STREET, '') || ', ' || COALESCE(CC.CITY, '') || ' ' || COALESCE(CC.POSTAL_CODE, '') || ', ' || COALESCE(CC.COUNTRY_CODE, '') AS "COMPANY ADDRESS"
                    FROM {DB}.{SCHEMA}.FACT_ALL_SOURCES_VW FACT
                    LEFT JOIN {DB}.{SCHEMA}.DIM_VENDOR_VW DIM ON FACT.VENDOR_ID = DIM.VENDOR_ID
                    LEFT JOIN {DB}.{SCHEMA}.DIM_COMPANY_CODE_VW CC ON FACT.COMPANY_CODE = CC.COMPANY_CODE
                    LEFT JOIN {DB}.{SCHEMA}.DIM_PLANT_VW PLT ON FACT.PLANT_CODE = PLT.PLANT_CODE
                    WHERE 1=1 {filter_clause}
                """
                cinfo = run_df(cinfo_sql)
                if cinfo is not None and not cinfo.empty:
                    cinfo = cinfo.replace(r"^\s*$", np.nan, regex=True).dropna(how="all")
                    st.markdown(
                        f'<div class="vendor-info-scrollable">{_build_html_table(cinfo)}</div>',
                        unsafe_allow_html=True,
                    )
                else:
                    st.info("No company information for this invoice.")
            
            # Fixed Proceed to Pay button at bottom-right of page (after all content)
            pay_status = st.session_state.get("_inv_pay_status", "")
            if pay_status == "processed":
                st.success("Invoice has been processed and marked as Paid.")
                st.session_state.pop("_inv_pay_status", None)
            elif pay_status == "paid":
                st.info("This invoice is already Paid.")
                st.session_state.pop("_inv_pay_status", None)
            elif pay_status == "ready":
                # Inject CSS for fixed button positioning
                st.markdown(
                    """
                    <style>
                    /* Target the Streamlit button container for our specific button */
                    div[data-testid="column"]:has(button[key="btn_inv_proceed_process_fixed"]),
                    div.stButton:has(button[key="btn_inv_proceed_process_fixed"]),
                    button[key="btn_inv_proceed_process_fixed"] {
                        position: fixed !important;
                        right: 32px !important;
                        bottom: 24px !important;
                        z-index: 9999 !important;
                    }
                    button[key="btn_inv_proceed_process_fixed"] {
                        box-shadow: 0 10px 25px rgba(15, 23, 42, 0.35) !important;
                        border-radius: 999px !important;
                        padding: 10px 24px !important;
                        min-width: 140px !important;
                    }
                    /* Alternative: target parent container */
                    div:has(> button[key="btn_inv_proceed_process_fixed"]) {
                        position: fixed !important;
                        right: 32px !important;
                        bottom: 24px !important;
                        z-index: 9999 !important;
                    }
                    </style>
                    """,
                    unsafe_allow_html=True,
                )
                # Create a container and render button
                with st.container():
                    clicked_pay = st.button(
                        "Proceed to Pay",
                        type="primary",
                        key="btn_inv_proceed_process_fixed",
                    )
                # Also inject inline style via markdown after button
                st.markdown(
                    """
                    <script>
                    (function() {
                        const btn = document.querySelector('button[key="btn_inv_proceed_process_fixed"]');
                        if (btn) {
                            btn.style.position = 'fixed';
                            btn.style.right = '32px';
                            btn.style.bottom = '24px';
                            btn.style.zIndex = '9999';
                            btn.style.boxShadow = '0 10px 25px rgba(15, 23, 42, 0.35)';
                            btn.style.borderRadius = '999px';
                            btn.style.padding = '10px 24px';
                            btn.style.minWidth = '140px';
                        }
                    })();
                    </script>
                    """,
                    unsafe_allow_html=True,
                )
                if clicked_pay:
                    comp_code = st.session_state.get("_inv_pay_comp_code", "")
                    fisc_year = st.session_state.get("_inv_pay_fisc_year", "")
                    selected_inv = st.session_state.get("_inv_pay_invoice", "")
                    if comp_code and fisc_year:
                        with st.spinner("Processing payment…"):
                            try:
                                result = run_df(f"""
                                    CALL {DB}.{SCHEMA}.P_PROCESS_INVOICE_PAYMENT(
                                        '{selected_inv.replace("'", "''")}',
                                        '{comp_code.replace("'", "''")}',
                                        '{fisc_year.replace("'", "''")}'
                                    )
                                """)
                                proc_result = ""
                                if result is not None and not result.empty:
                                    proc_result = str(result.iloc[0, 0])
                                if "SUCCESS" in proc_result.upper():
                                    st.session_state["inv_processed_set"] = st.session_state.get("inv_processed_set", set()) | {selected_inv}
                                    # Set paid message in cache so rerun doesn't call Genie again (keeps reload fast)
                                    cache = st.session_state.get("inv_ai_suggestion_cache", {})
                                    cache[selected_inv] = "This invoice is **paid**. No further action required."
                                    st.session_state["inv_ai_suggestion_cache"] = cache
                                    st.session_state.pop("_inv_pay_status", None)
                                    st.session_state.pop("_inv_pay_invoice", None)
                                    st.session_state.pop("_inv_pay_comp_code", None)
                                    st.session_state.pop("_inv_pay_fisc_year", None)
                                    st.success("Invoice processed successfully. Status updated to Paid in all tables.")
                                    st.rerun()
                                else:
                                    st.error(f"Processing failed: {proc_result}")
                            except Exception as e:
                                st.error(f"Error processing invoice: {e}")
                    else:
                        st.error("Missing company code or fiscal year — cannot process.")
        else:
            st.info("No invoice results for the search.")
    else:
        # ---- Show all invoices by default when no search is performed ----
        st.markdown("### All Invoices")

        f_col1, f_col2, _ = st.columns([1.5, 1.5, 5], gap="medium")
        with f_col1:
            # Cache vendor list in session to avoid re-running the heavy DISTINCT query on every rerun
            v_opts = st.session_state.get("inv_vendor_opts")
            if not v_opts:
                try:
                    v_list = run_df(f"""
                        SELECT DISTINCT V.VENDOR_NAME
                        FROM {DB}.{SCHEMA}.FACT_ALL_SOURCES_VW F
                        LEFT JOIN {DB}.{SCHEMA}.DIM_VENDOR_VW V ON F.VENDOR_ID = V.VENDOR_ID
                        WHERE V.VENDOR_NAME IS NOT NULL
                        ORDER BY 1
                    """)
                    v_opts = ["All Vendors"] + v_list['VENDOR_NAME'].dropna().astype(str).tolist()
                except Exception:
                    v_opts = ["All Vendors"]
                st.session_state["inv_vendor_opts"] = v_opts
            v_default = st.session_state.get("inv_sel_vendor", "All Vendors")
            v_index = v_opts.index(v_default) if v_default in v_opts else 0
            st.selectbox("Vendor", v_opts, label_visibility="collapsed", key="inv_sel_vendor", index=v_index, on_change=reset_pagination)
        with f_col2:
            s_opts = ["All Status", "Pending Approval", "Paid", "Blocked", "Overdue"]
            s_default = st.session_state.get("inv_sel_status", "All Status")
            s_index = s_opts.index(s_default) if s_default in s_opts else 0
            st.selectbox("Status", s_opts, label_visibility="collapsed", key="inv_sel_status", index=s_index, on_change=reset_pagination)
        
        # Build filter clause for vendor and status filters
        all_inv_filters = ""
        sel_vendor = st.session_state.get("inv_sel_vendor", "All Vendors")
        sel_status = st.session_state.get("inv_sel_status", "All Status")
        
        if sel_vendor != "All Vendors":
            safe_v = sel_vendor.replace("'", "''")
            all_inv_filters += f" AND DIM.VENDOR_NAME = '{safe_v}'"
        
        if sel_status != "All Status":
            status_map = {
                "Pending Approval": "OPEN",
                "Paid": "PAID",
                "Blocked": "BLOCKED",
                "Overdue": "OVERDUE"
            }
            mapped_status = status_map.get(sel_status, sel_status.upper())
            all_inv_filters += f" AND UPPER(FACT.INVOICE_STATUS) = '{mapped_status}'"
        
        all_invoices_sql = f"""
            SELECT DISTINCT
                FACT.INVOICE_NUMBER AS "INVOICE NUMBER",
                DIM.VENDOR_NAME AS "VENDOR NAME",
                FACT.POSTING_DATE AS "POSTING DATE",
                FACT.DUE_DATE AS "DUE DATE",
                FACT.INVOICE_AMOUNT_LOCAL AS "INVOICE AMOUNT",
                FACT.PURCHASE_ORDER_REFERENCE AS "PO NUMBER",
                UPPER(FACT.INVOICE_STATUS) AS "STATUS"
            FROM {DB}.{SCHEMA}.FACT_ALL_SOURCES_VW FACT
            LEFT JOIN {DB}.{SCHEMA}.DIM_VENDOR_VW DIM ON FACT.VENDOR_ID = DIM.VENDOR_ID
            WHERE 1=1 {all_inv_filters}
            ORDER BY FACT.POSTING_DATE DESC
            LIMIT 500
        """
        # Cache All Invoices grid per (vendor,status) combination to avoid re-running the query
        cache_key = f"{sel_vendor}||{sel_status}"
        inv_cache = st.session_state.get("all_invoices_cache", {})
        all_inv_df = inv_cache.get(cache_key)
        if all_inv_df is None:
            all_inv_df = run_df(all_invoices_sql)
            if all_inv_df is not None:
                inv_cache[cache_key] = all_inv_df
                st.session_state["all_invoices_cache"] = inv_cache
        if all_inv_df is not None and not all_inv_df.empty:
            all_inv_df = all_inv_df.replace(r"^\s*$", np.nan, regex=True).dropna(how="all")
            # Dynamic height based on rows (max 20 visible rows)
            hdr_h = 38
            row_h = 35
            visible_rows = min(len(all_inv_df), 20)
            inv_h = hdr_h + row_h * max(visible_rows, 1)
            st.dataframe(all_inv_df, use_container_width=True, hide_index=True, height=inv_h)
            st.caption(f"Showing {len(all_inv_df)} invoices (limited to 500). Use search to find specific invoices.")
        else:
            st.info("No invoices found.")
        
# ========== FACT-safe vendor filter ==========

def build_vendor_where(selected_vendor: str) -> str:
    if selected_vendor == "All Vendors":
        return ""
    safe_vendor = selected_vendor.replace("'", "''")
    return f"""
      AND FACT.VENDOR_ID IN (
        SELECT VENDOR_ID
        FROM {DB}.{SCHEMA}.DIM_VENDOR_VW
        WHERE UPPER(TRIM(VENDOR_NAME)) = UPPER(TRIM('{safe_vendor}'))
      )
    """

def build_vendor_where_history(selected_vendor: str) -> str:
    """Vendor filter for INVOICE_STATUS_HISTORY_VW which requires joining through invoices."""
    if selected_vendor == "All Vendors":
        return ""
    safe_vendor = selected_vendor.replace("'", "''")
    return f"""
      AND INVOICE_NUMBER IN (
        SELECT DISTINCT F.INVOICE_NUMBER
        FROM {DB}.{SCHEMA}.FACT_ALL_SOURCES_VW F
        LEFT JOIN {DB}.{SCHEMA}.DIM_VENDOR_VW V ON F.VENDOR_ID = V.VENDOR_ID
        WHERE UPPER(TRIM(V.VENDOR_NAME)) = UPPER(TRIM('{safe_vendor}'))
      )
    """

def _genie_base_filter():
    return " AND UPPER(F.INVOICE_STATUS) NOT IN ('CANCELLED','REJECTED') "

def run_quick_analysis(key: str) -> dict:
    """Run SQL for quick-analysis tiles; return {layout, type, metrics, monthly_df, vendors_df, ...}."""
    base = f"FROM {DB}.{SCHEMA}.FACT_ALL_SOURCES_VW F LEFT JOIN {DB}.{SCHEMA}.DIM_VENDOR_VW V ON F.VENDOR_ID = V.VENDOR_ID"
    flt = _genie_base_filter()
    out = {
        "layout": "quick",
        "type": key,
        "metrics": {},
        "anomaly": None,
        # primary result sets
        "monthly_df": None,
        "vendors_df": None,
        "extra_dfs": {},   # name -> df
        "sql": {},         # name -> sql string
    }

    if key == "spending_overview":
        # --- Metrics + time series + vendors (with fallbacks if empty) ---
        # Match recreated semantic-model definition:
        # - Total spend to date (<= current_date) using POSTING_DATE (not YTD-trunc)
        # - MoM from latest two months in the full monthly trend
        # - QoQ from latest two quarters in the full quarterly trend
        # - Top5 vendor share from total spend (all-time) (status-filtered)
        total_to_date = normalize_upper(run_df(f"""
            SELECT SUM(F.INVOICE_AMOUNT_LOCAL) AS total_spend
            {base}
            WHERE F.POSTING_DATE <= CURRENT_DATE() {flt}
        """))
        monthly_all = normalize_upper(run_df(f"""
            SELECT TO_CHAR(F.POSTING_DATE,'YYYY-MM') AS MONTH,
                   SUM(F.INVOICE_AMOUNT_LOCAL) AS VALUE_NUM
            {base}
            WHERE F.POSTING_DATE <= CURRENT_DATE() {flt}
            GROUP BY 1
            ORDER BY 1
        """))
        quarterly_all = normalize_upper(run_df(f"""
            SELECT TO_CHAR(F.POSTING_DATE, 'YYYY-\"Q\"Q') AS QUARTER,
                   SUM(F.INVOICE_AMOUNT_LOCAL) AS VALUE_NUM
            {base}
            WHERE F.POSTING_DATE <= CURRENT_DATE() {flt}
            GROUP BY 1
            ORDER BY 1
        """))
        # compute MoM & QoQ from last two buckets
        cur_m = prev_m = 0.0
        cur_q = prev_q = 0.0
        try:
            if monthly_all is not None and not monthly_all.empty and "VALUE_NUM" in monthly_all.columns:
                cur_m = safe_number(monthly_all["VALUE_NUM"].iloc[-1], 0)
                prev_m = safe_number(monthly_all["VALUE_NUM"].iloc[-2], 0) if len(monthly_all) >= 2 else 0
        except Exception:
            cur_m = prev_m = 0.0
        try:
            if quarterly_all is not None and not quarterly_all.empty and "VALUE_NUM" in quarterly_all.columns:
                cur_q = safe_number(quarterly_all["VALUE_NUM"].iloc[-1], 0)
                prev_q = safe_number(quarterly_all["VALUE_NUM"].iloc[-2], 0) if len(quarterly_all) >= 2 else 0
        except Exception:
            cur_q = prev_q = 0.0
        mom_pct = (cur_m - prev_m) / prev_m * 100 if prev_m else 0
        qoq_pct = (cur_q - prev_q) / prev_q * 100 if prev_q else 0
        top5 = normalize_upper(run_df(f"""
            WITH vendor_spend AS (
              SELECT V.VENDOR_NAME, SUM(F.INVOICE_AMOUNT_LOCAL) AS SPEND
              {base}
              WHERE F.POSTING_DATE <= CURRENT_DATE() {flt}
              GROUP BY 1
            ),
            total_spend AS (SELECT SUM(SPEND) AS TOT FROM vendor_spend),
            top5 AS (SELECT SPEND FROM vendor_spend ORDER BY SPEND DESC LIMIT 5)
            SELECT ROUND(SUM(top5.SPEND) / NULLIF((SELECT TOT FROM total_spend), 0) * 100, 2) AS PCT
            FROM top5
        """))
        total_spend = get_num(total_to_date, "TOTAL_SPEND", 0)
        top5_pct = get_num(top5, "PCT", 0)
        out["metrics"] = {
            "total_ytd": safe_number(total_spend, 0),
            "mom_pct": safe_number(mom_pct, 0),
            "qoq_pct": safe_number(qoq_pct, 0),
            "top5_pct": safe_int(round(top5_pct, 0), 0),
        }
        # Data-driven anomaly (largest MoM spike over last 12 months)
        out["anomaly"] = None
        monthly_sql = f"""
            SELECT TO_CHAR(F.POSTING_DATE,'YYYY-MM') AS MONTH,
                   SUM(F.INVOICE_AMOUNT_LOCAL) AS MONTHLY_SPEND,
                   COUNT(DISTINCT F.INVOICE_NUMBER) AS INVOICE_COUNT,
                   COUNT(DISTINCT F.VENDOR_ID) AS VENDOR_COUNT
            {base}
            WHERE F.POSTING_DATE >= DATEADD('month', -12, CURRENT_DATE()) {flt}
            GROUP BY 1 ORDER BY 1
        """
        out["sql"]["monthly_trend"] = monthly_sql
        monthly = run_df(monthly_sql)
        # fallback: broaden range and drop status filter if empty
        if monthly.empty:
            monthly_sql2 = f"""
                SELECT TO_CHAR(F.POSTING_DATE,'YYYY-MM') AS MONTH,
                       SUM(F.INVOICE_AMOUNT_LOCAL) AS MONTHLY_SPEND,
                       COUNT(DISTINCT F.INVOICE_NUMBER) AS INVOICE_COUNT,
                       COUNT(DISTINCT F.VENDOR_ID) AS VENDOR_COUNT
                {base}
                WHERE F.POSTING_DATE >= DATEADD('month', -24, CURRENT_DATE())
                GROUP BY 1 ORDER BY 1
            """
            out["sql"]["monthly_trend_fallback"] = monthly_sql2
            monthly = run_df(monthly_sql2)
        if not monthly.empty:
            monthly = monthly.rename(columns={"MONTHLY_SPEND": "VALUE"})
        out["monthly_df"] = monthly
        out["extra_dfs"]["monthly_full"] = monthly
        try:
            if monthly is not None and not monthly.empty and "MONTH" in monthly.columns and "VALUE" in monthly.columns:
                _m = monthly.copy()
                _m["VALUE"] = _m["VALUE"].apply(lambda v: safe_number(v, 0))
                _m = _m.sort_values("MONTH")
                _m["prev"] = _m["VALUE"].shift(1)
                _m["pct"] = (_m["VALUE"] - _m["prev"]) / _m["prev"].replace({0: np.nan})
                # pick max positive spike above 20%
                cand = _m.dropna(subset=["pct"])
                cand = cand[cand["pct"] > 0.20]
                if not cand.empty:
                    row = cand.loc[cand["pct"].idxmax()]
                    spike_month = str(row["MONTH"])
                    spike_pct = float(row["pct"]) * 100.0
                    topv = normalize_upper(run_df(f"""
                        SELECT V.VENDOR_NAME, SUM(F.INVOICE_AMOUNT_LOCAL) AS SPEND
                        {base}
                        WHERE TO_CHAR(F.POSTING_DATE,'YYYY-MM') = '{spike_month}' {flt}
                        GROUP BY 1 ORDER BY 2 DESC LIMIT 1
                    """))
                    vendor = topv.at[0, "VENDOR_NAME"] if topv is not None and not topv.empty and "VENDOR_NAME" in topv.columns else "a top vendor"
                    v_amt = get_num(topv, "SPEND", 0) if topv is not None else 0
                    out["anomaly"] = (
                        f"{spike_month} spending spiked by {spike_pct:.0f}% vs prior month, "
                        f"primarily driven by {vendor} ({abbr_currency(v_amt)})."
                    )
        except Exception:
            out["anomaly"] = None
        vendors_sql = f"""
            SELECT V.VENDOR_NAME, SUM(F.INVOICE_AMOUNT_LOCAL) AS SPEND
            {base} WHERE F.POSTING_DATE >= DATE_TRUNC('year', CURRENT_DATE()) {flt}
            GROUP BY 1 ORDER BY 2 DESC
        """
        out["sql"]["top_vendors"] = vendors_sql
        vendors = run_df(vendors_sql)
        if vendors.empty:
            vendors_sql2 = f"""
                SELECT V.VENDOR_NAME, SUM(F.INVOICE_AMOUNT_LOCAL) AS SPEND
                {base} WHERE F.POSTING_DATE >= DATEADD('month', -12, CURRENT_DATE())
                GROUP BY 1 ORDER BY 2 DESC
            """
            out["sql"]["top_vendors_fallback"] = vendors_sql2
            vendors = run_df(vendors_sql2)
        out["vendors_df"] = vendors
        out["extra_dfs"]["top_vendors"] = vendors

    elif key == "vendor_analysis":
        vendors_sql = f"""
            SELECT V.VENDOR_NAME, SUM(F.INVOICE_AMOUNT_LOCAL) AS SPEND, COUNT(DISTINCT F.INVOICE_NUMBER) AS INVOICE_COUNT
            {base} WHERE F.POSTING_DATE >= DATEADD('month', -6, CURRENT_DATE()) {flt}
            GROUP BY 1 ORDER BY 2 DESC
        """
        out["sql"]["vendor_top"] = vendors_sql
        vendors = run_df(vendors_sql)
        out["vendors_df"] = vendors
        # concentration: top 5 share
        top5_share = 0
        try:
            if not vendors.empty and "SPEND" in vendors.columns:
                tot = safe_number(vendors["SPEND"].sum(), 0)
                top5 = safe_number(vendors.head(5)["SPEND"].sum(), 0)
                top5_share = (top5 / tot * 100) if tot else 0
        except Exception:
            top5_share = 0
        out["metrics"] = {"summary": f"Top 5 vendors contribute ~{top5_share:.0f}% of spend in the last 6 months."}
        out["extra_dfs"]["vendor_top"] = vendors
    elif key == "payment_performance":
        pm_sql = f"""
            SELECT TO_CHAR(F.PAYMENT_DATE,'YYYY-MM') AS MONTH,
                   ROUND(AVG(DATEDIFF('day', F.POSTING_DATE, F.PAYMENT_DATE)), 1) AS AVG_DAYS,
                   SUM(CASE WHEN DATEDIFF('day', F.DUE_DATE, F.PAYMENT_DATE) > 0 THEN 1 ELSE 0 END) AS LATE_PAYMENTS,
                   COUNT(*) AS TOTAL_PAYMENTS
            {base}
            WHERE F.PAYMENT_DATE IS NOT NULL
              AND F.PAYMENT_DATE >= DATEADD('month', -6, CURRENT_DATE()) {flt}
            GROUP BY 1 ORDER BY 1
        """
        out["sql"]["payment_trend"] = pm_sql
        pm = run_df(pm_sql)
        if not pm.empty:
            pm = pm.rename(columns={"AVG_DAYS": "VALUE"})
        out["monthly_df"] = pm
        out["extra_dfs"]["payment_trend"] = pm
        out["metrics"] = {"summary": "Avg days-to-pay and late payments (last 6 months)."}
    elif key == "invoice_aging":
        base_fact = f"FROM {DB}.{SCHEMA}.FACT_ALL_SOURCES_VW F"
        aging_sql = f"""
            SELECT CASE WHEN F.AGING_DAYS <= 30 THEN '0-30 days'
                        WHEN F.AGING_DAYS <= 60 THEN '31-60 days'
                        WHEN F.AGING_DAYS <= 90 THEN '61-90 days' ELSE '90+ days' END AS bucket,
                   COUNT(*) AS CNT, SUM(F.INVOICE_AMOUNT_LOCAL) AS SPEND
            {base_fact} WHERE UPPER(F.INVOICE_STATUS) IN ('OPEN','PENDING','CLEARED')
              AND F.AGING_DAYS IS NOT NULL
              AND UPPER(F.INVOICE_STATUS) NOT IN ('CANCELLED','REJECTED')
            GROUP BY 1 ORDER BY 1
        """
        out["sql"]["invoice_aging"] = aging_sql
        aging = run_df(aging_sql)
        if not aging.empty:
            aging = aging.rename(columns={"bucket": "VENDOR_NAME"})
        out["vendors_df"] = aging
        out["extra_dfs"]["invoice_aging"] = aging
        total_inv = int(aging["CNT"].sum()) if not aging.empty and "CNT" in aging.columns else 0
        total_amt = safe_number(aging["SPEND"].sum(), 0) if not aging.empty and "SPEND" in aging.columns else 0
        out["metrics"] = {
            "summary": "No overdue invoices in aging buckets." if total_inv == 0 else f"{total_inv} invoices in aging buckets, {abbr_currency(total_amt)} total.",
            "invoice_count": total_inv,
            "total_amount": total_amt,
        }
    return out

# ========== Branding + UI load ==========
# inject_ui() replaces load_clean_ui_light() — all CSS tokens live in _build_ui_css()
inject_ui()

# Ensure session state initialization for Genie
if 'page' not in st.session_state:
    st.session_state.page = 'dashboard'

# Handle query parameters for navigation
params = st.query_params
if 'page' in params:
    new_page = params.get('page')
    if isinstance(new_page, list):
        new_page = new_page[0]
    if new_page != st.session_state.page:
        st.session_state.page = new_page
        st.rerun()

# Persist dashboard filters: restore from query params when present (so they survive page navigation)
if 'preset' not in st.session_state:
    st.session_state.preset = "Last 30 Days"
# Filters are persisted in _dash_* session state keys (non-widget keys survive page navigation)

def _get_current_user_display():
    try:
        df = run_df("SELECT CURRENT_USER() AS SF_USER")
        if not df.empty and 'SF_USER' in df.columns:
            raw = str(df.at[0, 'SF_USER'])
            # simplify identifier (strip domain/warehouse if present)
            user = raw.split('@')[0].split('.')[0]
            return user.title()
    except Exception:
        pass
    try:
        import getpass
        return getpass.getuser()
    except Exception:
        return "User"

branding_bar()
st.markdown("<div class='below-header-spacer'></div>", unsafe_allow_html=True)

# ====================== DASHBOARD PAGE ======================
if st.session_state.page == 'dashboard':

    # Initialize session state for Needs Attention
    if 'na_tab' not in st.session_state:
        st.session_state.na_tab = 'Overdue'
    if 'na_page' not in st.session_state:
        st.session_state.na_page = 0

    # Handle query params for NA tabs
    if st.query_params.get('na_tab'):
        p = st.query_params.get('na_tab')
        if isinstance(p, (list, tuple)) and len(p) > 0:
            p = p[0]
        st.session_state.na_tab = p

    if st.query_params.get('na_page'):
        pp = st.query_params.get('na_page')
        try:
            if isinstance(pp, (list, tuple)) and len(pp) > 0:
                pp = pp[0]
            st.session_state.na_page = max(0, int(pp))
        except Exception:
            pass

    # ---------- Controls (Date, Vendor, Preset) ----------
    if "preset" not in st.session_state:
        st.session_state.preset = "Last 30 Days"

    col_date, col_vendor, col_presets = st.columns([1, 1, 1.8], gap="small")

    with col_date:
        if st.session_state.preset != "Custom":
            rng_start, rng_end = compute_range_preset(st.session_state.preset)
            default_range = (rng_start, rng_end)
        else:
            default_range = st.session_state.get(
                "date_range",
                (date.today().replace(day=1), date.today())
            )
        date_range = st.date_input(
            "Date Range",
            value=default_range,
            format="YYYY-MM-DD",
            label_visibility="collapsed"
        )

        if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
            rng_start, rng_end = date_range
        else:
            rng_start, rng_end = date.today().replace(day=1), date.today()

        st.session_state.date_range = (rng_start, rng_end)

        if st.session_state.preset != "Custom" and (
            rng_start != compute_range_preset(st.session_state.preset)[0] or
            rng_end != compute_range_preset(st.session_state.preset)[1]
        ):
            st.session_state.preset = "Custom"

    start_lit_tmp, end_lit_tmp = sql_date(rng_start), sql_date(rng_end)

    with col_vendor:
        try:
            vendor_df = run_df(f"""
                SELECT DISTINCT V.VENDOR_NAME
                FROM {DB}.{SCHEMA}.FACT_ALL_SOURCES_VW F
                LEFT JOIN {DB}.{SCHEMA}.DIM_VENDOR_VW V ON F.VENDOR_ID = V.VENDOR_ID
                WHERE F.POSTING_DATE BETWEEN {start_lit_tmp} AND {end_lit_tmp}
                  AND V.VENDOR_NAME IS NOT NULL
                ORDER BY 1
            """)
            vendor_list = ["All Vendors"]
            if not vendor_df.empty and "VENDOR_NAME" in vendor_df.columns:
                vendor_list += vendor_df["VENDOR_NAME"].dropna().astype(str).tolist()
            # Restore vendor from persistent key if widget key was cleaned up
            _saved_v = st.session_state.get("_dash_vendor_saved", "All Vendors")
            if "invoice_vendor" not in st.session_state and _saved_v in vendor_list:
                st.session_state.invoice_vendor = _saved_v
            vendor = st.selectbox(
                "Filter by Vendor",
                vendor_list,
                key="invoice_vendor",
                label_visibility="collapsed"
            )
        except Exception as e:
            st.error(f"Failed to load vendors: {e}")
            vendor = "All Vendors"

    # Persist filter values to non-widget keys (survive widget cleanup on other pages)
    st.session_state._dash_vendor_saved = vendor
    st.session_state._dash_date_range_saved = (rng_start, rng_end)
    st.session_state._dash_preset_saved = st.session_state.get("preset", "Last 30 Days")

    with col_presets:
        preset = st.session_state.get('preset', 'Last 30 Days')
        presets = ["Last 30 Days", "QTD", "YTD", "Custom"]
        p_cols = st.columns(4, gap="small")
        for idx, p in enumerate(presets):
            with p_cols[idx]:
                if st.button(
                    p,
                    key=f"preset_{idx}_{p.replace(' ', '_')}",
                    use_container_width=True,
                    type="primary" if p == preset else "secondary"
                ):
                    st.session_state.preset = p
                    st.session_state._dash_preset_saved = p
                    st.query_params.from_dict({"page": "dashboard"})
                    st.rerun()

    # Prepare SQL literals and filters
    start_lit, end_lit = sql_date(rng_start), sql_date(rng_end)
    p_start, p_end = prior_window(rng_start, rng_end)
    p_start_lit, p_end_lit = sql_date(p_start), sql_date(p_end)
    vendor_where = build_vendor_where(vendor)

# ====================== DASHBOARD ======================
if st.session_state.get('page','dashboard') == 'dashboard':
    # Compute counts for NA tabs (match tab filters)
    urgent_count = 0
    overdue_count = disputed_count = DUE_COUNT = 0
    try:
        counts_sql = f"""
        SELECT
            SUM(CASE
                    WHEN FACT.DUE_DATE < CURRENT_DATE()
                     AND UPPER(FACT.INVOICE_STATUS) IN ('OVERDUE')
                    THEN 1 ELSE 0
                END) AS OVERDUE_COUNT,
            SUM(CASE WHEN UPPER(FACT.INVOICE_STATUS) IN ('DISPUTE','DISPUTED') THEN 1 ELSE 0 END) AS DISPUTED_COUNT,
            SUM(CASE
                    WHEN FACT.DUE_DATE IS NOT NULL
                     AND FACT.DUE_DATE >= CURRENT_DATE()
                     AND UPPER(FACT.INVOICE_STATUS) IN ('OPEN')
                    THEN 1 ELSE 0
                END) AS DUE_COUNT
        FROM {DB}.{SCHEMA}.FACT_ALL_SOURCES_VW FACT
        WHERE POSTING_DATE BETWEEN {start_lit} AND {end_lit}
        {vendor_where}
        """
        cnts = run_df(counts_sql)
        overdue_count = safe_int(cnts.at[0, 'OVERDUE_COUNT'], 0) if (not cnts.empty and 'OVERDUE_COUNT' in cnts.columns) else 0
        disputed_count = safe_int(cnts.at[0, 'DISPUTED_COUNT'], 0) if (not cnts.empty and 'DISPUTED_COUNT' in cnts.columns) else 0
        DUE_COUNT = safe_int(cnts.at[0, 'DUE_COUNT'], 0) if (not cnts.empty and 'DUE_COUNT' in cnts.columns) else 0
        urgent_count = overdue_count + disputed_count + DUE_COUNT
    except Exception as e:
        st.error(f"Failed to compute NA counts: {e}")

    # ----- Insight banner (only for HIGH severity) -----
    try:
        insight_sql = f"""
        WITH recent_agg AS (
          SELECT AVG(DATEDIFF('day', POSTING_DATE, PAYMENT_DATE)) AS avg_days
          FROM {DB}.{SCHEMA}.FACT_ALL_SOURCES_VW FACT
          WHERE POSTING_DATE BETWEEN DATEADD('day', -14, {end_lit}) AND {end_lit}
            {vendor_where}
            AND PAYMENT_DATE IS NOT NULL
        ),
        prior_agg AS (
          SELECT AVG(DATEDIFF('day', POSTING_DATE, PAYMENT_DATE)) AS avg_days
          FROM {DB}.{SCHEMA}.FACT_ALL_SOURCES_VW FACT
          WHERE POSTING_DATE BETWEEN DATEADD('day', -28, {end_lit}) AND DATEADD('day', -15, {end_lit})
            {vendor_where}
            AND PAYMENT_DATE IS NOT NULL
        )
        SELECT
          COALESCE(r.avg_days, 0) AS RECENT_DAYS,
          COALESCE(p.avg_days, 0) AS PRIOR_DAYS
        FROM recent_agg r
        CROSS JOIN prior_agg p;
        """
        ins = normalize_upper(run_df(insight_sql))
        recent_days = get_num(ins, 'RECENT_DAYS', 0)
        prior_days = get_num(ins, 'PRIOR_DAYS', 0)
        delta_pct = ((recent_days - prior_days) / prior_days) * 100.0 if prior_days > 0 else 0.0
        severity = "HIGH" if delta_pct >= 10 else ("MED" if delta_pct >= 5 else "LOW")

        open_pos_df = run_df(f"""
          SELECT COUNT(DISTINCT PURCHASE_ORDER_REFERENCE) AS OPEN_POS
          FROM {DB}.{SCHEMA}.FACT_ALL_SOURCES_VW FACT
          WHERE POSTING_DATE BETWEEN {start_lit} AND {end_lit}
            {vendor_where}
            AND UPPER(INVOICE_STATUS) = 'OPEN'
        """)
        open_pos = int(get_num(normalize_upper(open_pos_df), "OPEN_POS", 0))

        if severity == 'HIGH':
            st.markdown(f'''
            <div class="genie-banner" style="background: linear-gradient(90deg, #E0C3FC 0%, #F9A8D4 100%); border-radius: 1.5rem; padding: 1.5rem 2rem; display: flex; align-items: center; gap: 1.5rem; margin-bottom: 2rem; box-shadow: 0 4px 12px rgba(80, 80, 120, 0.08);">
                <div class="genie-icon-circle" style="background: #FFFFFF; border-radius: 50%; width: 56px; height: 56px; min-width: 56px; display: flex; align-items: center; justify-content: center; font-size: 2.2rem; color: #A78BFA; box-shadow: 0 4px 12px rgba(80, 80, 120, 0.12);">⚡</div>
                <div class="genie-content-wrapper" style="flex: 1;">
                    <div class="genie-title-text" style="font-weight: 800; font-size: 1.15rem; color: #1F2937; margin-bottom: 0.3rem; display: flex; align-items: center; gap: 0.8rem;">
                        Genie Insight: Vendor '{vendor}' Risk
                        <span class="genie-severity-badge" style="background: #FF3B30; color: #FFFFFF; font-size: 0.75rem; font-weight: 800; border-radius: 0.8rem; padding: 0.3rem 0.8rem;">{severity}</span>
                    </div>
                    <div class="genie-desc-text" style="font-size: 0.95rem; color: #374151; line-height: 1.4;">
                        {abs(delta_pct):.0f}% change in payment lead time (recent vs prior). Detected {severity.lower()} deviation over the last 14 days. Potential impact on current deliveries for <strong>{open_pos}</strong> outstanding POs.<br>
                        Suggested action: review contract SLA clauses and follow up with vendors.
                    </div>
                </div>
                <button class="genie-action-btn" style="background: #0057FF; color: #FFFFFF; border: none; border-radius: 999px; padding: 0.8rem 2rem; font-weight: 700; font-size: 1rem; cursor: pointer;">Review Contract</button>
            </div>
            ''', unsafe_allow_html=True)
    except Exception as e:
        st.error(f"Failed to generate insight: {e}")


    # ----- KPI row (current vs prior) -----
    kpi_cur = normalize_upper(run_df(f"""
    WITH base AS (
      SELECT FACT.*, V.VENDOR_NAME
      FROM {DB}.{SCHEMA}.FACT_ALL_SOURCES_VW FACT
      LEFT JOIN {DB}.{SCHEMA}.DIM_VENDOR_VW V ON FACT.VENDOR_ID = V.VENDOR_ID
      WHERE POSTING_DATE BETWEEN {start_lit} AND {end_lit}
        {vendor_where}
    )
    SELECT
      COUNT(DISTINCT CASE WHEN UPPER(INVOICE_STATUS) = 'OPEN' THEN PURCHASE_ORDER_REFERENCE END) AS ACTIVE_POS,
      COUNT(DISTINCT PURCHASE_ORDER_REFERENCE) AS TOTAL_POS,
      SUM(CASE WHEN UPPER(INVOICE_STATUS) NOT IN ('CANCELLED','REJECTED')
               THEN COALESCE(INVOICE_AMOUNT_LOCAL,0) ELSE 0 END) AS TOTAL_SPEND,
      COUNT(DISTINCT VENDOR_NAME) AS ACTIVE_VENDORS,
      COUNT(DISTINCT CASE WHEN UPPER(INVOICE_STATUS) = 'OPEN' THEN INVOICE_NUMBER END) AS PENDING_INV
    FROM base;
    """))

    kpi_prev = normalize_upper(run_df(f"""
    WITH base AS (
      SELECT FACT.*, V.VENDOR_NAME
      FROM {DB}.{SCHEMA}.FACT_ALL_SOURCES_VW FACT
      LEFT JOIN {DB}.{SCHEMA}.DIM_VENDOR_VW V ON FACT.VENDOR_ID = V.VENDOR_ID
      WHERE POSTING_DATE BETWEEN {p_start_lit} AND {p_end_lit}
        {vendor_where}
    )
    SELECT
      COUNT(DISTINCT CASE WHEN UPPER(INVOICE_STATUS) = 'OPEN' THEN PURCHASE_ORDER_REFERENCE END) AS ACTIVE_POS,
      COUNT(DISTINCT PURCHASE_ORDER_REFERENCE) AS TOTAL_POS,
      SUM(CASE WHEN UPPER(INVOICE_STATUS) NOT IN ('CANCELLED','REJECTED')
               THEN COALESCE(INVOICE_AMOUNT_LOCAL,0) ELSE 0 END) AS TOTAL_SPEND,
      COUNT(DISTINCT VENDOR_NAME) AS ACTIVE_VENDORS,
      COUNT(DISTINCT CASE WHEN UPPER(INVOICE_STATUS) = 'OPEN' THEN INVOICE_NUMBER END) AS PENDING_INV
    FROM base;
    """))

    cur_pos   = get_num(kpi_cur, 'ACTIVE_POS', 0)
    cur_total_pos = get_num(kpi_cur, 'TOTAL_POS', 0)
    cur_spend = get_num(kpi_cur, 'TOTAL_SPEND', 0)
    cur_vend  = get_num(kpi_cur, 'ACTIVE_VENDORS', 0)
    cur_pend  = get_num(kpi_cur, 'PENDING_INV', 0)

    prev_pos   = get_num(kpi_prev, 'ACTIVE_POS', 0)
    prev_total_pos = get_num(kpi_prev, 'TOTAL_POS', 0)
    prev_spend = get_num(kpi_prev, 'TOTAL_SPEND', 0)
    prev_vend  = get_num(kpi_prev, 'ACTIVE_VENDORS', 0)
    prev_pend  = get_num(kpi_prev, 'PENDING_INV', 0)

    d_pos, _up_pos, nc_pos = pct_delta(cur_pos, prev_pos)
    d_total_pos, _up_total_pos, nc_total_pos = pct_delta(cur_total_pos, prev_total_pos)
    d_spend, _up_spend, nc_spend = pct_delta(cur_spend, prev_spend)
    d_pend, _up_pend, nc_pend = pct_delta(cur_pend, prev_pend)
    d_vend, _up_vend, nc_vend = pct_delta(cur_vend, prev_vend)

    pos_up = (cur_pos - prev_pos) >= 0
    total_pos_up = (cur_total_pos - prev_total_pos) >= 0
    spend_up = (cur_spend - prev_spend) >= 0
    pend_up = (cur_pend - prev_pend) >= 0
    vend_up = (cur_vend - prev_vend) >= 0

    # KPI Cards (row 1 of 4)
    kpi_cols = st.columns(4, gap="medium")
    kpi_data = [
        ("TOTAL SPEND", f"{abbr_currency(cur_spend)}", d_spend, spend_up, "#FFECB5", nc_spend),
        ("ACTIVE PO'S", f"{int(cur_pos):,}", d_pos, pos_up, "#D3F0F8", nc_pos),
        ("TOTAL PO'S", f"{int(cur_total_pos):,}", d_total_pos, total_pos_up, "#F4E1FD", nc_total_pos),
        ("ACTIVE VENDORS", f"{int(cur_vend):,}", d_vend, vend_up, "#FEE9E7", nc_vend),
    ]

    for idx, (label, value, delta, is_up, bg_color, no_change) in enumerate(kpi_data):
        with kpi_cols[idx]:
            delta = clean_delta_text(delta)
            # Always render a safe delta placeholder to avoid stray text
            if no_change:
                delta_html = (
                    "<div style=\"display: flex; align-items: center; gap: 6px; "
                    "font-size: 25px; font-weight: 500; color: #1a1a1a;\">-</div>"
                )
            elif delta is None:
                delta_html = (
                    "<div style=\"display: flex; align-items: center; gap: 6px; "
                    "font-size: 25px; font-weight: 500; color: #1a1a1a;\">-</div>"
                )
            else:
                delta_icon = "↑" if is_up else "↓"
                delta_color = "#16A34A" if is_up else "#DC2626"
                delta_html = (
                    f"<div style=\"display: flex; align-items: center; gap: 6px; "
                    f"font-size: 25px; font-weight: 500; color: {delta_color};\">"
                    f"{delta_icon} {html.escape(delta)}</div>"
                )
            st.markdown(f'''
            <div class="kpi-card" style="background: {bg_color}; border: 1px solid #E5E7EB; border-radius: 16px; padding: 18px; min-height: 120px; box-shadow: 0 2px 8px rgba(2,8,23,.04); display: flex; flex-direction: column; justify-content: space-between;">
                <div style="font-size: 12px; font-weight: 600; color: #1a1a1a; letter-spacing: 0.5px; margin-bottom: 8px;">{label}</div>
                <div style="display: flex; align-items: baseline; justify-content: space-between; gap: 20px;">
                    <div style="font-size: 32px; font-weight: 600; color: #1a1a1a;">{value}</div>
                    {delta_html}
                </div>
            </div>
            ''', unsafe_allow_html=True)

    st.markdown("")  # spacer

    # ----- Additional invoice process KPIs -----
    avg_proc_days = 0.0
    fp_pct = 0.0
    ap_pct = 0.0
    prev_avg_proc_days = 0.0
    prev_fp_pct = 0.0
    prev_ap_pct = 0.0
    try:
        proc_sql = f"""
        SELECT
          AVG(DATEDIFF('day',
              POSTING_DATE,
              PAYMENT_DATE
          )) AS AVG_PROCESSING_DAYS
        FROM {DB}.{SCHEMA}.FACT_ALL_SOURCES_VW FACT
        WHERE POSTING_DATE BETWEEN {start_lit} AND {end_lit}
          {vendor_where}
          AND UPPER(INVOICE_STATUS) = 'PAID';
        """
        proc_prev_sql = f"""
        SELECT
          AVG(DATEDIFF('day',
              POSTING_DATE,
              PAYMENT_DATE
          )) AS AVG_PROCESSING_DAYS
        FROM {DB}.{SCHEMA}.FACT_ALL_SOURCES_VW FACT
        WHERE POSTING_DATE BETWEEN {p_start_lit} AND {p_end_lit}
          {vendor_where}
          AND UPPER(INVOICE_STATUS) = 'PAID';
        """
        fp_sql = f"""
        WITH hist AS (
          SELECT INVOICE_NUMBER,
                 MAX(CASE WHEN UPPER(STATUS) IN ('PAID','CLEARED','CLOSED','POSTED','SETTLED') THEN 1 ELSE 0 END) AS HAS_PAID,
                 MAX(CASE WHEN UPPER(STATUS) IN ('DISPUTE','DISPUTED','OVERDUE') THEN 1 ELSE 0 END) AS HAS_ISSUE
          FROM {DB}.{SCHEMA}.INVOICE_STATUS_HISTORY_VW
          WHERE POSTING_DATE BETWEEN {start_lit} AND {end_lit}
            {build_vendor_where_history(vendor)}
          GROUP BY INVOICE_NUMBER
        )
        SELECT
          COUNT(*) AS TOTAL_INV,
          SUM(CASE WHEN HAS_PAID = 1 AND HAS_ISSUE = 0 THEN 1 ELSE 0 END) AS FIRST_PASS_INV
        FROM hist;
        """
        fp_prev_sql = f"""
        WITH hist AS (
          SELECT INVOICE_NUMBER,
                 MAX(CASE WHEN UPPER(STATUS) IN ('PAID','CLEARED','CLOSED','POSTED','SETTLED') THEN 1 ELSE 0 END) AS HAS_PAID,
                 MAX(CASE WHEN UPPER(STATUS) IN ('DISPUTE','DISPUTED','OVERDUE') THEN 1 ELSE 0 END) AS HAS_ISSUE
          FROM {DB}.{SCHEMA}.INVOICE_STATUS_HISTORY_VW
          WHERE POSTING_DATE BETWEEN {p_start_lit} AND {p_end_lit}
            {build_vendor_where_history(vendor)}
          GROUP BY INVOICE_NUMBER
        )
        SELECT
          COUNT(*) AS TOTAL_INV,
          SUM(CASE WHEN HAS_PAID = 1 AND HAS_ISSUE = 0 THEN 1 ELSE 0 END) AS FIRST_PASS_INV
        FROM hist;
        """
        ap_sql = f"""
        WITH PAID_INVOICES AS (
            SELECT INVOICE_NUMBER,STATUS_NOTES
            FROM {DB}.{SCHEMA}.INVOICE_STATUS_HISTORY_VW
            WHERE POSTING_DATE BETWEEN {start_lit} AND {end_lit}
                {build_vendor_where_history(vendor)}
                AND UPPER(STATUS) = 'PAID'
        )
        SELECT 
            COUNT(*) AS TOTAL_CLEARED,
            SUM(CASE WHEN UPPER(STATUS_NOTES) = 'AUTO PROCESSED' THEN 1 ELSE 0 END) AS AUTO_PROCESSED
        FROM PAID_INVOICES;
        """
        ap_prev_sql = f"""
        WITH PAID_INVOICES AS (
            SELECT INVOICE_NUMBER,STATUS_NOTES
            FROM {DB}.{SCHEMA}.INVOICE_STATUS_HISTORY_VW
            WHERE POSTING_DATE BETWEEN {p_start_lit} AND {p_end_lit}
                {build_vendor_where_history(vendor)}
                AND UPPER(STATUS) = 'PAID'
        )
        SELECT 
            COUNT(*) AS TOTAL_CLEARED,
            SUM(CASE WHEN UPPER(STATUS_NOTES) = 'AUTO PROCESSED' THEN 1 ELSE 0 END) AS AUTO_PROCESSED
        FROM PAID_INVOICES;
        """
        
        proc_df = normalize_upper(run_df(proc_sql))
        proc_prev_df = normalize_upper(run_df(proc_prev_sql))
        fp_df = normalize_upper(run_df(fp_sql))
        fp_prev_df = normalize_upper(run_df(fp_prev_sql))
        ap_df = normalize_upper(run_df(ap_sql))
        ap_prev_df = normalize_upper(run_df(ap_prev_sql))

        avg_proc_days = get_num(proc_df, "AVG_PROCESSING_DAYS", 0)
        prev_avg_proc_days = get_num(proc_prev_df, "AVG_PROCESSING_DAYS", 0)
        total_inv = get_num(fp_df, "TOTAL_INV", 0)
        first_pass = get_num(fp_df, "FIRST_PASS_INV", 0)
        fp_pct = (first_pass / total_inv * 100.0) if total_inv > 0 else 0.0
        prev_total_inv = get_num(fp_prev_df, "TOTAL_INV", 0)
        prev_first_pass = get_num(fp_prev_df, "FIRST_PASS_INV", 0)
        prev_fp_pct = (prev_first_pass / prev_total_inv * 100.0) if prev_total_inv > 0 else 0.0
        total_paid = get_num(ap_df, "TOTAL_CLEARED", 0)
        auto_proc = get_num(ap_df, "AUTO_PROCESSED", 0)
        ap_pct = (auto_proc/total_paid * 100.0) if total_paid > 0 else 0.0
        prev_total_paid = get_num(ap_prev_df, "TOTAL_CLEARED", 0)
        prev_auto_proc = get_num(ap_prev_df, "AUTO_PROCESSED", 0)
        prev_ap_pct = (prev_auto_proc/prev_total_paid * 100.0) if prev_total_paid > 0 else 0.0

    except Exception as e:
        st.error(f"Failed to compute invoice processing KPIs: {e}")

    # Row 2 of 4 (Pending + process KPIs)
    row2_cols = st.columns(4, gap="medium")
    d_proc_text, _good_proc, nc_proc = abs_delta_days(avg_proc_days, prev_avg_proc_days)
    proc_up = (avg_proc_days - prev_avg_proc_days) >= 0
    d_fp, _up_fp, nc_fp = pct_delta(fp_pct, prev_fp_pct)
    fp_up = (fp_pct - prev_fp_pct) >= 0
    d_ap, _up_ap, nc_ap = pct_delta(ap_pct, prev_ap_pct)
    ap_up = (ap_pct - prev_ap_pct) >= 0
    row2_cards = [
        ("PENDING INVOICES", f"{int(cur_pend):,}", d_pend, pend_up, "#E4F9F2", nc_pend),
        ("AVG INVOICE PROCESSING TIME", f"{avg_proc_days:.1f}d", d_proc_text, not proc_up, "#E0F2FE", nc_proc),
        ("FIRST PASS INVOICES %", f"{fp_pct:.1f}%", d_fp, fp_up, "#DCFCE7", nc_fp),
        ("AUTOPROCESSED INVOICES %", f"{ap_pct:.1f}%", d_ap, ap_up, "#FCE7F3", nc_ap),
    ]
    for col, (label, value, delta, is_up, bg, no_change) in zip(row2_cols, row2_cards):
        with col:
            delta = clean_delta_text(delta)
            # Always render a safe delta placeholder to avoid stray text
            if no_change:
                delta_html = (
                    "<div style=\"display: flex; align-items: center; gap: 6px; "
                    "font-size: 25px; font-weight: 600; color: #1a1a1a;\">-</div>"
                )
            elif delta is None:
                delta_html = (
                    "<div style=\"display: flex; align-items: center; gap: 6px; "
                    "font-size: 25px; font-weight: 600; color: #1a1a1a;\">-</div>"
                )
            else:
                delta_icon = "↑" if is_up else "↓"
                delta_color = "#16A34A" if is_up else "#DC2626"
                delta_html = (
                    f"<div style=\"display: flex; align-items: center; gap: 6px; "
                    f"font-size: 25px; font-weight: 600; color: {delta_color};\">"
                    f"{delta_icon} {html.escape(delta)}</div>"
                )
            st.markdown(f'''
            <div class="kpi-card" style="background: {bg}; border: 1px solid #E5E7EB; border-radius: 16px; padding: 18px; min-height: 120px; box-shadow: 0 2px 8px rgba(2,8,23,.04); display: flex; flex-direction: column; justify-content: space-between;">
                <div style="font-size: 12px; font-weight: 600; color: #1a1a1a; letter-spacing: 0.4px; margin-bottom: 8px;">{label}</div>
                <div style="display: flex; align-items: center; justify-content: space-between; gap: 12px;">
                    <div style="font-size: 28px; font-weight: 700; color: #1a1a1a;">{value}</div>
                    {delta_html}
                </div>
            </div>
            ''', unsafe_allow_html=True)

    st.markdown("")  # spacer

    # ----- Middle row: Needs Attention (tabs: Overdue / Disputed / Due 30d) -----
    with st.container(border=True):
        st.markdown(f"""
        <div style='display: flex; align-items: center; justify-content: space-between; margin-bottom: 0.5rem; padding-left: 1.5rem; padding-right: 1.5rem;'>
            <div style='font-size:18px;font-weight:900;color:#1a1a1a;letter-spacing:.2px;'>Needs Attention <span style='font-weight:700;color:#6b7280;'>({urgent_count:,})</span></div>
            <div></div>
        </div>
        """, unsafe_allow_html=True)

        current_tab = st.session_state.na_tab

        # ── Tab buttons ─────────────────────────────────────────────────────
        tab_cols = st.columns([1, 1, 1], gap="small")

        with tab_cols[0]:
            if st.button(f"Overdue ({overdue_count})", key="na_btn_overdue", use_container_width=True):
                st.session_state.na_tab = 'Overdue'
                st.session_state.na_page = 0
                st.rerun()

        with tab_cols[1]:
            if st.button(f"Disputed ({disputed_count})", key="na_btn_disputed", use_container_width=True):
                st.session_state.na_tab = 'Disputed'
                st.session_state.na_page = 0
                st.rerun()

        with tab_cols[2]:
            if st.button(f"Due ({DUE_COUNT})", key="na_btn_due30d", use_container_width=True):
                st.session_state.na_tab = 'Due'
                st.session_state.na_page = 0
                st.rerun()

        # ── Active tab: blue background, white text (button + all text inside) ────────────────────────────────────────
        st.markdown(f"""
        <style>
        {"div[data-testid='stButton'] button[data-testid='baseButton-na_btn_overdue'] { background: #2563eb !important; background-color: #2563eb !important; color: white !important; border-color: #2563eb !important; font-weight: 800 !important; } div[data-testid='stButton'] button[data-testid='baseButton-na_btn_overdue'] * { color: white !important; }" if current_tab == 'Overdue' else ""}
        {"div[data-testid='stButton'] button[data-testid='baseButton-na_btn_disputed'] { background: #2563eb !important; background-color: #2563eb !important; color: white !important; border-color: #2563eb !important; font-weight: 800 !important; } div[data-testid='stButton'] button[data-testid='baseButton-na_btn_disputed'] * { color: white !important; }" if current_tab == 'Disputed' else ""}
        {"div[data-testid='stButton'] button[data-testid='baseButton-na_btn_due30d'] { background: #2563eb !important; background-color: #2563eb !important; color: white !important; border-color: #2563eb !important; font-weight: 800 !important; } div[data-testid='stButton'] button[data-testid='baseButton-na_btn_due30d'] * { color: white !important; }" if current_tab == 'Due' else ""}

        /* Keep the "View invoice" link-style buttons inside cards */
        button[data-testid^="baseButton-na_card_"] {{
            font-weight: 800 !important;
            background-color: transparent !important;
            border: none !important;
            color: #1d4ed8 !important;
            box-shadow: none !important;
        }}
        </style>
        """, unsafe_allow_html=True)

        st.markdown("<div style='height:24px;'></div>", unsafe_allow_html=True)

        try:
            if current_tab == 'Overdue':
                needs_sql = f"""
                SELECT FACT.INVOICE_NUMBER AS REF_NO, FACT.INVOICE_AMOUNT_LOCAL AS AMOUNT,
                       FACT.DUE_DATE, UPPER(FACT.INVOICE_STATUS) AS STATUS, DIM.VENDOR_NAME, FACT.AGING_DAYS
                FROM {DB}.{SCHEMA}.FACT_ALL_SOURCES_VW FACT
                LEFT JOIN {DB}.{SCHEMA}.DIM_VENDOR_VW DIM ON FACT.VENDOR_ID = DIM.VENDOR_ID
                WHERE POSTING_DATE BETWEEN {start_lit} AND {end_lit} {vendor_where}
                  AND FACT.DUE_DATE < CURRENT_DATE()
                  AND UPPER(FACT.INVOICE_STATUS) IN ('OVERDUE')
                ORDER BY FACT.DUE_DATE ASC;
                """
            elif current_tab == 'Disputed':
                needs_sql = f"""
                SELECT FACT.INVOICE_NUMBER AS REF_NO, FACT.INVOICE_AMOUNT_LOCAL AS AMOUNT,
                       FACT.DUE_DATE, UPPER(FACT.INVOICE_STATUS) AS STATUS, DIM.VENDOR_NAME, FACT.AGING_DAYS
                FROM {DB}.{SCHEMA}.FACT_ALL_SOURCES_VW FACT
                LEFT JOIN {DB}.{SCHEMA}.DIM_VENDOR_VW DIM ON FACT.VENDOR_ID = DIM.VENDOR_ID
                WHERE POSTING_DATE BETWEEN {start_lit} AND {end_lit} {vendor_where}
                  AND UPPER(FACT.INVOICE_STATUS) IN ('DISPUTE','DISPUTED')
                ORDER BY FACT.DUE_DATE ASC;
                """
            else:
                needs_sql = f"""
                SELECT FACT.INVOICE_NUMBER AS REF_NO, FACT.INVOICE_AMOUNT_LOCAL AS AMOUNT,
                       FACT.DUE_DATE, UPPER(FACT.INVOICE_STATUS) AS STATUS, DIM.VENDOR_NAME, FACT.AGING_DAYS
                FROM {DB}.{SCHEMA}.FACT_ALL_SOURCES_VW FACT
                LEFT JOIN {DB}.{SCHEMA}.DIM_VENDOR_VW DIM ON FACT.VENDOR_ID = DIM.VENDOR_ID
                WHERE POSTING_DATE BETWEEN {start_lit} AND {end_lit} {vendor_where}
                  AND FACT.DUE_DATE IS NOT NULL
                  AND FACT.DUE_DATE >= CURRENT_DATE()
                  AND UPPER(FACT.INVOICE_STATUS) IN ('OPEN')
                ORDER BY FACT.DUE_DATE ASC;
                """

            needs = run_df(needs_sql)
        except Exception as e:
            st.error(f"Failed to load needs data: {e}")
            needs = pd.DataFrame()

        items_per_page = 8
        total_items = len(needs) if (needs is not None and not needs.empty) else 0
        total_pages = (total_items + items_per_page - 1) // items_per_page if total_items > 0 else 1

        if needs is None or needs.empty:
            st.markdown('<div class="na-empty">No items in this category</div>', unsafe_allow_html=True)
        else:
            start_idx = st.session_state.na_page * items_per_page
            end_idx = min(start_idx + items_per_page, total_items)
            page_needs = needs.iloc[start_idx:end_idx]

            card_chunks = [page_needs.iloc[i:i+4] for i in range(0, len(page_needs), 4)]
            card_global_idx = 0
            for row in card_chunks:
                cols = st.columns(4, gap="medium")
                for col, (_, r) in zip(cols, row.iterrows()):
                    with col:
                        ref = str(r.get("REF_NO", "")).strip() or "—"
                        amt = safe_number(r.get("AMOUNT"))
                        ddate_raw = r.get("DUE_DATE")
                        ddate = pd.to_datetime(ddate_raw).date().isoformat() if pd.notna(ddate_raw) else "—"
                        status = str(r.get("STATUS", "OPEN")).upper()
                        vendor_nm = str(r.get("VENDOR_NAME", "—"))
                        aging = safe_number(r.get("AGING_DAYS"), 0)

                        is_overdue = False
                        due_soon = False
                        today_dt = date.today()
                        if pd.notna(ddate_raw):
                            try:
                                dd = pd.to_datetime(ddate_raw).date()
                                is_overdue = status in ("OVERDUE")
                                due_soon = today_dt <= dd <= (today_dt + timedelta(days=30))
                            except Exception:
                                pass

                        if current_tab == "Overdue":
                            tag_label = "Overdue"
                            tag_bg, tag_color = "#fde7e9", "#b42318"
                            tab_class = "overdue"
                        elif current_tab == "Due":
                            tag_label = "Due soon"
                            tag_bg, tag_color = "#DBEAFE", "#0284C7"
                            tab_class = "due"
                        elif status == "DISPUTED":
                            tag_label = "Disputed"
                            tag_bg, tag_color = "#fff4e5", "#b54708"
                            tab_class = "disputed"
                        else:
                            tag_label = status.title()
                            tag_bg, tag_color = "#F3F4F6", "#6B7280"
                            tab_class = "other"

                        with st.container(border=True, key=f"na_bg_{tab_class}_{card_global_idx}"):
                            left, right = st.columns([2, 1], gap="small")
                            with left:
                                btn_key = f"na_card_{start_idx}_{card_global_idx}_{ref.replace(' ', '_')[:30]}"
                                if st.button(ref, key=btn_key):
                                    st.session_state["invoice_search_from_card"] = ref
                                    st.session_state["page"] = "invoice"
                                    st.query_params.from_dict({"page": "invoice", "search_invoice": ref})
                                    st.rerun()
                                st.markdown(f"<div style='color:#64748b;font-size:12px;overflow:hidden;text-overflow:ellipsis;'>{html.escape(vendor_nm)}</div>", unsafe_allow_html=True)
                            with right:
                                st.markdown(
                                    f"<div style='text-align:right;'>"
                                    f"<span style='background:{tag_bg};color:{tag_color};font-size:12px;padding:4px 10px;border-radius:999px;display:inline-block;margin-bottom:6px;'>{tag_label}</span>"
                                    f"<div style='font-weight:600;font-size:13px;'>{abbr_currency(amt)}</div>"
                                    f"<div style='color:#888;font-size:10px;line-height:1.2;white-space:nowrap;'>Due: {ddate}</div>"
                                    f"</div>",
                                    unsafe_allow_html=True
                                )
                        card_global_idx += 1

            st.markdown("<div style='height:32px;'></div>", unsafe_allow_html=True)
            pag_cols = st.columns([1, 1, 1], gap="small")

            with pag_cols[0]:
                if st.session_state.na_page > 0:
                    if st.button("← Prev", key="na_prev_bottom", use_container_width=True):
                        st.session_state.na_page = max(0, st.session_state.na_page - 1)
                        st.rerun()
                else:
                    st.markdown("<div style='text-align:center;color:#d1d5db;font-size:14px;padding:10px;'>← Prev</div>", unsafe_allow_html=True)

            with pag_cols[1]:
                st.markdown(f"<div style='text-align:center;font-weight:500;color:#6b7280;font-size:14px;padding:10px;'>{st.session_state.na_page + 1} of {total_pages}</div>", unsafe_allow_html=True)

            with pag_cols[2]:
                if st.session_state.na_page < total_pages - 1:
                    if st.button("Next →", key="na_next_bottom", use_container_width=True):
                        st.session_state.na_page = min(total_pages - 1, st.session_state.na_page + 1)
                        st.rerun()
                else:
                    st.markdown("<div style='text-align:center;color:#d1d5db;font-size:14px;padding:10px;'>Next →</div>", unsafe_allow_html=True)

    # ----- Bottom row: Charts -----
    st.markdown('<div style="margin-top: -1.5rem;"></div>', unsafe_allow_html=True)
    c3, c4, c5 = st.columns([1.2, 1.2, 1.2], gap="small")

    # Invoice Status Donut
    with c3:
        with st.container(border=True):
            st.markdown("### Invoice Status Distribution")
            try:
                status_sql = f"""
                WITH base AS (
                SELECT
                    CASE
                    WHEN UPPER(INVOICE_STATUS) IN ('PAID', 'CLEARED', 'CLOSED', 'POSTED', 'SETTLED') THEN 'Paid'
                    WHEN UPPER(INVOICE_STATUS) IN ('OPEN', 'PENDING', 'ON HOLD', 'PARKED', 'IN PROGRESS') THEN 'Pending'
                    WHEN UPPER(INVOICE_STATUS) IN ('DISPUTE', 'DISPUTED', 'BLOCKED', 'CONTESTED') THEN 'Disputed'
                    ELSE 'Other'
                    END AS STATUS
                FROM {DB}.{SCHEMA}.FACT_ALL_SOURCES_VW FACT
                WHERE POSTING_DATE BETWEEN {start_lit} AND {end_lit}
                    {vendor_where}
                )
                SELECT STATUS, COUNT(*) AS COUNT
                FROM base
                GROUP BY 1
                ORDER BY 2 DESC;
                """
                inv = run_df(status_sql)
                total = int(inv['COUNT'].sum()) if not inv.empty and 'COUNT' in inv.columns else 0
                if total > 0:
                    inv = inv.rename(columns={"COUNT": "CNT"})
                    alt_donut_status(inv, label_col="STATUS", value_col="CNT", height=280, title=None, show_legend=True)
                else:
                    st.info("No invoices in selected range.")
                    st.markdown("<div style='height:200px;'></div>", unsafe_allow_html=True)
            except Exception as e:
                st.error(f"Failed to load invoice status: {e}")
                st.markdown("<div style='height:200px;'></div>", unsafe_allow_html=True)

    # Top 10 Vendors by Spend
    with c4:
        with st.container(border=True):
            st.markdown("### Top 10 Vendors by Spend")
            try:
                top_sql = f"""
                SELECT DIM.VENDOR_NAME, SUM(COALESCE(FACT.INVOICE_AMOUNT_LOCAL,0)) AS SPEND
                FROM {DB}.{SCHEMA}.FACT_ALL_SOURCES_VW FACT
                LEFT JOIN {DB}.{SCHEMA}.DIM_VENDOR_VW DIM ON FACT.VENDOR_ID = DIM.VENDOR_ID
                WHERE POSTING_DATE BETWEEN {start_lit} AND {end_lit} {vendor_where}
                GROUP BY 1 ORDER BY 2 DESC
                LIMIT 10;
                """
                top = run_df(top_sql)
                if top is None or top.empty:
                    top = pd.DataFrame({
                        'VENDOR_NAME': ['Globalogistics Corp','Mainframe Solutions','Apex Supplies','Inorbit','Cosewise','iCraft'],
                        'SPEND': [1200000,880000,600000,580000,550000,520000]
                    })
                    st.info("Using sample vendor data")
                data = top.rename(columns={'VENDOR_NAME':'Vendor','SPEND':'Spend'})
                alt_bar(data, x='Vendor', y='Spend', title=None, horizontal=True, color='#22C55E', height=280)
            except Exception as e:
                st.error(f"Failed to load vendor data: {e}")

    # Spend Trend Analysis
    with c5:
        with st.container(border=True):
            st.markdown("### Spend Trend Analysis")
            try:
                trend_sql = f"""
                SELECT
                    TO_CHAR(POSTING_DATE,'YYYY-MM') AS MONTH,
                    MIN(POSTING_DATE) AS MONTH_START,
                    EXTRACT(month FROM POSTING_DATE) AS MONTH_NUM,
                    EXTRACT(year FROM POSTING_DATE) AS YEAR_NUM,
                    SUM(CASE WHEN UPPER(INVOICE_STATUS) NOT IN ('CANCELLED','REJECTED')
                                THEN COALESCE(INVOICE_AMOUNT_LOCAL,0) ELSE 0 END) AS ACTUAL
                FROM {DB}.{SCHEMA}.FACT_ALL_SOURCES_VW FACT
                WHERE POSTING_DATE BETWEEN DATEADD(year, -2, {start_lit}) AND {end_lit} {vendor_where}
                GROUP BY 1,3,4 ORDER BY MONTH_START;
                """
                spend = run_df(trend_sql)
                if spend.empty:
                    st.info("No spend data for selected range.")
                    st.markdown("<div style='height:200px;'></div>", unsafe_allow_html=True)
                else:
                    spend = spend.sort_values("MONTH_START")
                    # Forecast = average of same month across prior 2 years
                    max_year = int(spend["YEAR_NUM"].max())
                    hist = spend[spend["YEAR_NUM"] < max_year]
                    forecast_map = hist.groupby("MONTH_NUM")["ACTUAL"].mean()
                    spend["FORECAST"] = spend["MONTH_NUM"].map(forecast_map)

                    # Display only selected months in the selected range
                    start_month = rng_start.replace(day=1)
                    end_month = rng_end.replace(day=1)
                    spend = spend[spend["MONTH_START"].between(start_month, end_month)]
                    alt_bar_actual_vs_forecast(
                        spend, month_col="MONTH", actual_col="ACTUAL", forecast_col="FORECAST",
                        height=280, title=None, show_legend=True
                    )
            except Exception as e:
                st.error(f"Failed to load spend trend: {e}")
                st.markdown("<div style='height:200px;'></div>", unsafe_allow_html=True)
# ====================== INVOICE DETAILS PAGE ======================
elif st.session_state.page == 'cash_flow':
    render_cash_flow_page()

elif st.session_state.page == 'invoice':
    render_invoice_page()
    

# ====================== P2P GENIE (Cortex Analyst) ======================
if st.session_state.get('page') == 'genie':
    
    # ── Session-state defaults (safe to call on every rerun) ─────────────────
    _genie_defaults = {
        "selected_analysis":   None,
        "show_analysis":       False,
        "analyst_response":    None,
        "genie_messages":      [],
        "saved_insights":      [],
        "recent_analyses":     [],
        "sidebar_expanded":    True,
        "genie_input_version": 0,
        "last_custom_query":   "",
        # ── cache / memory ────────────────────────────────────────────
        "genie_cache":           None,
        "genie_cache_init":      False,
        "genie_memory":          None,
        "genie_memory_built":    False,
        "_mem_last_q_count":     -1,      # tracks when to rebuild memory
        # ── chat persistence ──────────────────────────────────────────
        "genie_session_id":      None,
        "genie_session_label":   "",
        "chat_persistence":      None,
        "chat_persist_init":     False,
        "chat_turn_index":       0,
        "restore_offered":       False,
        "restore_dismissed":     False,
        "_all_sessions_cache":   [],
        "show_chats_panel":      False,
        # ── YAML auto-sync ────────────────────────────────────────────
        "yaml_sync_done":        False,    # run once per browser session
        "yaml_sync_result":      None,     # stores result dict for UI display
    }
    for _k, _v in _genie_defaults.items():
        if _k not in st.session_state:
            st.session_state[_k] = _v

    # ── ❷ Initialise query cache once per Python process ─────────────────────
    if not st.session_state.get("genie_cache_init"):
        try:
            st.session_state.genie_cache = GenieQueryCache(
                session, DB, SCHEMA,
                max_size          = UI.Genie.CACHE_MAX_SIZE,
                similarity_threshold = UI.Genie.SIMILARITY_THRESHOLD,
            )
        except Exception as _e:
            st.session_state.genie_cache = None
        finally:
            st.session_state.genie_cache_init = True

    # ── ❸ Build / refresh long-term memory ──────────────────────────────────
    # Build on first visit. Rebuild every 5 new questions so facts stay fresh.
    _current_q_count = len([
        m for m in st.session_state.get("genie_messages", [])
        if m.get("role") == "user"
    ])
    _last_mem_q_count = st.session_state.get("_mem_last_q_count", -1)
    _should_build_mem = (
        not st.session_state.get("genie_memory_built")           # first time
        or (                                                        # every 5 new Qs
            _current_q_count > 0
            and _current_q_count != _last_mem_q_count
            and (_current_q_count % 5 == 0)
        )
    )
    if _should_build_mem:
        try:
            _mem_obj_new = GenieLongTermMemory(
                session, DB, SCHEMA, cortex_model=CORTEX_PRESCRIPTIVE_MODEL
            )
            st.session_state.genie_memory = _mem_obj_new
        except Exception:
            if not st.session_state.get("genie_memory"):
                st.session_state.genie_memory = None
        st.session_state.genie_memory_built  = True
        st.session_state._mem_last_q_count   = _current_q_count

    # ── ❹ Chat persistence — init once per browser session ──────────────────
    if not st.session_state.get("chat_persist_init"):
        try:
            _cp = GenieChatPersistence(session, DB, SCHEMA)
            st.session_state.chat_persistence = _cp
            if _cp._table_ok:
                _cp.purge_old(keep_days=7)
            else:
                st.session_state["_chat_persist_error"] = (
                    "Chat table could not be created — check Snowflake permissions."
                )
        except Exception as _pe:
            st.session_state.chat_persistence = None
            st.session_state["_chat_persist_error"] = str(_pe)
        finally:
            st.session_state.chat_persist_init = True

    # ── ❺ YAML Auto-Sync — run once per browser session ─────────────────────
    # Checks INFORMATION_MART for new VW_* views and patches the staged YAML
    # so Cortex Analyst automatically knows about any new tables.
    if not st.session_state.get("yaml_sync_done"):
        try:
            _yaml_result = run_yaml_auto_update(session)
            st.session_state["yaml_sync_result"] = _yaml_result
        except Exception as _yaml_exc:
            st.session_state["yaml_sync_result"] = {
                "status": "error",
                "added_views": [],
                "message": f"YAML sync raised an exception: {_yaml_exc}",
            }
        finally:
            st.session_state["yaml_sync_done"] = True

    # ── Session ID (one UUID per browser login) ──────────────────────────────
    if not st.session_state.get("genie_session_id"):
        import uuid as _uuid_mod
        st.session_state.genie_session_id    = str(_uuid_mod.uuid4())
        st.session_state.genie_session_label = "Chat on " + datetime.now().strftime("%b %d %H:%M")

    # Define quick analysis options - match YAML verified queries; icons as SVG (screenshot-style)
    _BAR_CHART_SVG = '''<svg width="24" height="24" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg"><rect x="4" y="14" width="4" height="6" rx="1" fill="white"/><rect x="10" y="10" width="4" height="10" rx="1" fill="white"/><rect x="16" y="6" width="4" height="14" rx="1" fill="white"/></svg>'''
    _VENDOR_SVG = '''<svg width="24" height="24" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg"><circle cx="9" cy="7" r="3" stroke="white" stroke-width="1.5" fill="none"/><path d="M3 21v-2a4 4 0 0 1 4-4h4a4 4 0 0 1 4 4v2" stroke="white" stroke-width="1.5" fill="none"/><rect x="14" y="8" width="8" height="2" rx="0.5" fill="white"/><rect x="14" y="12" width="6" height="2" rx="0.5" fill="white"/><rect x="14" y="16" width="8" height="2" rx="0.5" fill="white"/></svg>'''
    _CLOCK_SVG = '''<svg width="24" height="24" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg"><circle cx="12" cy="12" r="9" stroke="white" stroke-width="1.5" fill="none"/><path d="M12 6v6l4 2" stroke="white" stroke-width="1.5" stroke-linecap="round"/></svg>'''
    _DOC_SVG = '''<svg width="24" height="24" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg"><path d="M7 3h8l5 5v11a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2z" stroke="white" stroke-width="1.5" fill="none"/><line x1="7" y1="9" x2="17" y2="9" stroke="white" stroke-width="1.5"/><line x1="7" y1="13" x2="15" y2="13" stroke="white" stroke-width="1.5"/><line x1="7" y1="17" x2="13" y2="17" stroke="white" stroke-width="1.5"/></svg>'''
    QUICK_ANALYSES = {
        "spending_overview": {
            "title": "Spending Overview",
            "icon_svg": _BAR_CHART_SVG,
            "desc": "Track total spend, monthly trends and major changes",
            "verified_query": "spending_overview",
            "question": "Show me total spend YTD, monthly trends, and top 5 vendors",
        },
        "vendor_analysis": {
            "title": "Vendor Analysis",
            "icon_svg": _VENDOR_SVG,
            "desc": "Understand vendor-wise spend, concentration, and dependency",
            "verified_query": "vendor_analysis",
            "question": "Analyze vendor concentration and dependency",
        },
        "payment_performance": {
            "title": "Payment Performance",
            "icon_svg": _CLOCK_SVG,
            "desc": "Identify delays, late payments, and cycle time issues",
            "verified_query": "payment_performance",
            "question": "Show payment delays and cycle time issues",
        },
        "invoice_aging": {
            "title": "Invoice Aging",
            "icon_svg": _DOC_SVG,
            "desc": "See overdue invoices, risk buckets, and problem areas",
            "verified_query": "invoice_aging",
            "question": "Show overdue invoices by aging buckets",
        },
    }
    
    # Instruction: reason-with-data; always return Descriptive + Prescriptive + Predictive
    DECISION_SUPPORT_INSTRUCTION = (
        "Do NOT start with 'This is our interpretation of your question.' "
        "You MUST respond with exactly THREE sections in this order:\n"
        "For ANY YES/NO question: start the Descriptive section with a clear **Yes** or **No** first, then explain with specific numbers.\n"
        "(1) **Descriptive**: What the data shows — cite exact numbers, vendors, time periods, and anomalies. 2–4 sentences.\n"
        "(2) **Prescriptive**: 3–5 SPECIFIC prioritized actions with exact findings and concrete next steps. Never use vague phrases.\n"
        "(3) **Predictive**: A short 30–90 day forecast tied to current metrics. State assumptions and end with confidence level (Low/Medium/High).\n"
        "NEVER skip any section. NEVER use vague phrases like 'review the data below' without citing specific numbers.\n"
        "Answer the following question:\n\n"
    )

    def call_cortex_analyst(query_text: str, conversation_history: list = None):
        """Call Cortex Analyst with a natural language query.
        
        conversation_history: list of strictly alternating {"role":"user","content":[...]}
                              and {"role":"analyst","content":[...]} dicts.
                              MUST start with "user" and strictly alternate.
        """
        try:
            import _snowflake
        except ImportError:
            return {
                "error": (
                    "Cortex Analyst is only available when running inside Snowflake (Streamlit in Snowflake). "
                    "Please deploy this app as a Streamlit in Snowflake app to use AI queries."
                )
            }

        try:
            raw_query = (query_text or "").strip()
            normalized = raw_query.lower()
            alias_map = {
                "first pass po's": (
                    "Show me first pass PO's - purchase orders where all invoices were paid without disputes or overdue"
                ),
                "first pass pos": (
                    "Show me first pass PO's - purchase orders where all invoices were paid without disputes or overdue"
                ),
            }
            rewritten_query = alias_map.get(normalized, raw_query)
            # Apply instruction prefix only to the current standalone question
            augmented_query = DECISION_SUPPORT_INSTRUCTION + rewritten_query

            semantic_path = as_stage_url(SEMANTIC_MODEL_STAGE, SEMANTIC_MODEL_FILE)

            # Build messages array
            # CRITICAL: Cortex Analyst requires strict alternation: user → analyst → user → …
            # First message MUST be user. Validate and sanitise history.
            messages = []
            if conversation_history:
                # Only include history if it is strictly alternating and starts with user
                _valid = True
                _expected_role = "user"
                for _turn in conversation_history:
                    if _turn.get("role") != _expected_role:
                        _valid = False
                        break
                    _expected_role = "analyst" if _expected_role == "user" else "user"
                if _valid and conversation_history[0].get("role") == "user":
                    messages = list(conversation_history)

            # Append the current user question as the final message
            messages.append({
                "role": "user",
                "content": [{"type": "text", "text": augmented_query}]
            })

            body = {
                "messages": messages,
                "semantic_model_file": semantic_path
            }

            resp = _snowflake.send_snow_api_request(
                "POST",
                "/api/v2/cortex/analyst/message",
                {"Content-Type": "application/json"},
                {},
                body,
                None,
                60000
            )

            status = resp.get("status", 500)
            if status >= 400:
                return {"error": f"HTTP {status}: {resp.get('content','')}"}

            return json.loads(resp.get("content", "{}"))

        except Exception as e:
            return {"error": str(e)}

    def process_genie_query(query: str, analysis_type: str = "custom"):
        """Process a query: cache lookup → Cortex → persist → render."""
        import time as _time

        # ── 1. Add user bubble ────────────────────────────────────────────────
        st.session_state.genie_messages.append({
            "role": "user", "content": query,
            "timestamp": pd.Timestamp.now(), "response": None,
        })

        _cache = st.session_state.get("genie_cache")
        _t0    = _time.time()

        # ── 2. Determine if this is a follow-up (needs conversation history) ──
        # A "follow-up" is a question that refers to previous context
        # (pronouns: "them", "those", "it", "their", or "same", "above", "previous")
        # For cache purposes: EXACT matches can always be served from cache even in
        # a conversation — the answer is deterministic for the same question text.
        _followup_signals = {"them", "those", "their", "it", "same", "above",
                             "previous", "that", "these", "which one", "how about",
                             "what about", "and", "also"}
        _q_words = set(query.lower().split())
        _is_contextual = bool(_q_words & _followup_signals) and len(query.split()) < 8

        # Build conversation history for contextual follow-ups
        _conv_history = []
        _prior_user_msgs = [m for m in st.session_state.genie_messages[:-1]
                            if m.get("role") == "user"]
        _is_followup = len(_prior_user_msgs) > 0

        # ── 3. Cache lookup — try for any question (exact match is always safe) ─
        cached_resp = None
        if _cache and not _is_contextual:
            # Try cache for non-contextual questions regardless of position in conversation
            cached_resp = _cache.get(query)

        from_cache = cached_resp is not None and _cache._is_real(cached_resp)

        if from_cache:
            response = cached_resp
            response["cache_fetch_time_ms"] = (_time.time() - _t0) * 1000

        else:
            # ── 4. Build conversation history (strict user→analyst alternation) ──
            if _is_followup:
                _all_prev = st.session_state.genie_messages[:-1]
                _pairs = []
                _i = 0
                while _i < len(_all_prev) - 1:
                    _um = _all_prev[_i]
                    _am = _all_prev[_i + 1]
                    if _um.get("role") == "user" and _am.get("role") == "assistant":
                        _u_txt = (_um.get("content") or "").strip()
                        # Get analyst text from stored response or bubble content
                        _prev_resp = _am.get("response")
                        _a_txt = ""
                        if isinstance(_prev_resp, dict):
                            _blocks = _prev_resp.get("message", {}).get("content", [])
                            _a_txt = " ".join(
                                b.get("text", "") for b in _blocks if b.get("type") == "text"
                            ).strip()
                        if not _a_txt:
                            _a_txt = (_am.get("content") or "").strip()
                        if _u_txt and _a_txt:
                            _pairs.append((_u_txt[:1500], _a_txt[:1500]))
                        _i += 2
                    else:
                        _i += 1

                for _u_txt, _a_txt in _pairs[-4:]:  # max 4 pairs = 8 messages
                    _conv_history.append({
                        "role": "user",
                        "content": [{"type": "text", "text": _u_txt}]
                    })
                    _conv_history.append({
                        "role": "analyst",
                        "content": [{"type": "text", "text": _a_txt}]
                    })

            # ── 5. Call Cortex Analyst ────────────────────────────────────────
            response = call_cortex_analyst(
                query,
                conversation_history=_conv_history if _conv_history else None
            )

            # ── 6. Store in cache (non-contextual successful answers only) ────
            if _cache and not response.get("error") and not _is_contextual:
                ok = _cache.set(query, response)
                # Surface cache write errors in session state for left panel display
                if not ok and _cache.last_error:
                    st.session_state["_cache_write_error"] = _cache.last_error
                else:
                    st.session_state.pop("_cache_write_error", None)

        # ── 6. Build assistant bubble text — SHORT summary only ───────────────
        # The full Descriptive+Prescriptive+Charts render in the panel below.
        # The bubble only shows a brief status so it doesn't duplicate content.
        if from_cache:
            assistant_text = ""   # cache badge shows; result panel renders data
        else:
            _blocks = response.get("message", {}).get("content", []) if isinstance(response, dict) else []
            _full_text = next(
                (b.get("text", "") for b in _blocks if b.get("type") == "text"), ""
            )
            if _full_text:
                # Show only the first sentence as the bubble (rest is in the Descriptive panel)
                _first_sentence = _full_text.split(".")[0].strip()
                assistant_text = (_first_sentence[:120] + "…") if len(_first_sentence) > 120 else _first_sentence
            else:
                if response.get("error"):    assistant_text = str(response["error"])
                elif response.get("layout"): assistant_text = "Analysis complete."
                else:                        assistant_text = ""

        st.session_state.genie_messages.append({
            "role": "assistant", "content": assistant_text[:600],
            "timestamp": pd.Timestamp.now(), "response": response,
            "from_cache": from_cache,
        })

        # ── 7. Trim short-term window ─────────────────────────────────────────
        st.session_state.genie_messages = st.session_state.genie_messages[-(UI.Genie.SHORT_TERM_MAX_MSGS):]

        # ── 8. Recent-analyses bookkeeping + update last_custom_query ─────────
        # Always update last_custom_query so prescriptive generation uses THIS question's context
        if analysis_type == "custom":
            st.session_state.last_custom_query = query
        st.session_state.recent_analyses.insert(0, {
            "query": query, "type": analysis_type,
            "timestamp": pd.Timestamp.now(), "response": response,
        })
        st.session_state.recent_analyses = st.session_state.recent_analyses[:10]

        # ── 9. Persist question to history table ──────────────────────────────
        _append_genie_question(query, analysis_type)

        # ── 10. Persist both turns to Snowflake (chat persistence) ────────────
        _cp  = st.session_state.get("chat_persistence")
        _sid = st.session_state.get("genie_session_id", "")
        _lbl = st.session_state.get("genie_session_label", "")
        if _cp and _sid:
            try:
                _ti = st.session_state.get("chat_turn_index", 0)
                # Save user turn
                _cp.save_turn(_sid, _ti, "user", query, "", "user_input", _lbl)
                _ti += 1
                # Save assistant turn — store the FULL cortex text (not truncated bubble)
                # so resumed sessions have proper context for follow-up questions
                _sql_used  = ""
                _src       = ""
                _full_text = assistant_text  # default to bubble text
                if isinstance(response, dict):
                    _sql_used = str(response.get("sql", "") or "")[:2000]
                    _src      = response.get("source", "")
                    # Extract full text from cortex message blocks
                    _blocks_persist = response.get("message", {}).get("content", [])
                    _ft = " ".join(
                        b.get("text", "") for b in _blocks_persist if b.get("type") == "text"
                    ).strip()
                    if _ft:
                        _full_text = _ft  # full cortex text for context continuity
                _cp.save_turn(_sid, _ti, "assistant", _full_text[:3500], _sql_used, _src, _lbl)
                _ti += 1
                st.session_state.chat_turn_index = _ti
            except Exception:
                pass

        # ── 11. Refresh long-term memory every 10 messages ────────────────────
        _msg_count = len(st.session_state.get("genie_messages", []))
        _mem_obj   = st.session_state.get("genie_memory")
        if _mem_obj and _msg_count % 10 == 0:
            try:
                _mem_obj.refresh()
            except Exception:
                pass

        return response

    # When user clicked a suggested question on Cash Flow page: auto-run the query and show results (do not just fill input)
    prefill_q = st.session_state.pop("genie_prefill_question", None)
    if prefill_q and isinstance(prefill_q, str) and prefill_q.strip():
        st.session_state.selected_analysis = "custom"
        st.session_state.last_custom_query = prefill_q.strip()
        st.session_state.show_analysis = True
        with st.spinner("Analyzing..."):
            st.session_state.analyst_response = process_genie_query(prefill_q.strip())
        st.rerun()

    # ===== GENIE PAGE LAYOUT =====
    
    # Welcome Header
    st.markdown("""
    <div style="margin-bottom:8px;">
        <h1 style="font-size:28px;font-weight:900;color:#1a1a1a;margin:0 0 4px 0;">Welcome to ProcureIQ Genie</h1>
        <p style="font-size:16px;color:#64748b;margin:0;">Let Genie run one of these quick analyses for you</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Quick Analysis Cards Row — only tile background changes when selected; title/desc stay dark
    ICON_BG = "#5046e5"
    LAVENDER = "#e8e4f7"
    CARD_BORDER = "#e5e7eb"
    SELECTED_BORDER = "#5046e5"
    TEXT_TITLE = "#1a1a1a"
    TEXT_DESC = "#64748b"
    cols = st.columns(4, gap="medium")
    clicked_key = None
    sel = st.session_state.get("selected_analysis")
    show = st.session_state.get("show_analysis", False)
    st.markdown("""
    <style>
    #genie-faqs .stButton > button {
        justify-content: flex-start;
        text-align: left;
        padding-left: 28px;
        white-space: normal;
        position: relative;
    }
    #genie-faqs .stButton > button::before {
        content: "•";
        position: absolute;
        left: 12px;
        top: 50%;
        transform: translateY(-50%);
    }
    </style>
    """, unsafe_allow_html=True)

    for idx, (key, analysis) in enumerate(QUICK_ANALYSES.items()):
        with cols[idx]:
            with st.form(f"tile_{key}", border=False):
                icon_svg = analysis["icon_svg"]
                selected = bool(show and sel == key)
                bg = LAVENDER if selected else "#fff"
                border = SELECTED_BORDER if selected else CARD_BORDER
                st.markdown(f"""
                <div class="genie-tile-card" style="background:{bg};border:1.5px solid {border};
                     border-radius:0px;padding:20px;box-shadow:0 2px 8px rgba(0,0,0,.04);
                     min-height:160px;">
                    <div style="width:48px;height:48px;border-radius:12px;display:flex;align-items:center;
                         justify-content:center;margin-bottom:14px;background:{ICON_BG};">
                        {icon_svg}
                    </div>
                    <div style="font-size:16px;font-weight:800;color:{TEXT_TITLE};margin-bottom:6px;">
                        {analysis['title']}
                    </div>
                    <div style="font-size:13px;color:{TEXT_DESC};line-height:1.4;">
                        {analysis['desc']}
                    </div>
                </div>
                """, unsafe_allow_html=True)
                if st.form_submit_button("Ask Genie", use_container_width=True):
                    clicked_key = key
    if clicked_key is not None:
        a = QUICK_ANALYSES[clicked_key]
        st.session_state.selected_analysis = clicked_key
        st.session_state.show_analysis = True
        st.session_state.last_custom_query = a.get("question", "")
        # Clear prescriptive cache from previous question
        for _k in list(st.session_state.keys()):
            if str(_k).startswith("_pres_"):
                del st.session_state[_k]
        with st.spinner(f"Running {a['title']} analysis..."):
            if clicked_key == "invoice_aging":
                # Invoice Aging goes through Cortex (process_genie_query adds to genie_messages)
                cortex_result = process_genie_query(a["question"], analysis_type=clicked_key)
                st.session_state.analyst_response = cortex_result
                st.session_state.recent_analyses.insert(0, {
                    "query": a["question"], "type": clicked_key,
                    "timestamp": pd.Timestamp.now(), "response": cortex_result,
                })
            else:
                quick_result = run_quick_analysis(clicked_key)
                st.session_state.analyst_response = quick_result
                st.session_state.recent_analyses.insert(0, {
                    "query": a["question"], "type": clicked_key,
                    "timestamp": pd.Timestamp.now(), "response": quick_result,
                })
                # ── Add synthetic conversation messages so follow-ups have context ──
                # Build a summary of what was shown so Cortex knows what was answered
                _tile_summary = (
                    f"I ran a {a['title']} analysis. "
                    f"The question was: '{a['question']}'. "
                    f"The analysis covers: {a['desc']}."
                )
                _m = quick_result.get("metrics") or {}
                if "total_ytd" in _m:
                    _tile_summary += (
                        f" Total spend YTD: {abbr_currency(safe_number(_m.get('total_ytd'),0))}. "
                        f"MoM change: {_safe_pct_str(_m.get('mom_pct'),0)}. "
                        f"Top 5 vendors: {safe_int(_m.get('top5_pct'),0)}% of spend."
                    )
                # Add user bubble
                st.session_state.genie_messages.append({
                    "role": "user",
                    "content": a["question"],
                    "timestamp": pd.Timestamp.now(),
                    "response": None,
                })
                # Add assistant bubble with the summary
                st.session_state.genie_messages.append({
                    "role": "assistant",
                    "content": _tile_summary,
                    "timestamp": pd.Timestamp.now(),
                    "response": quick_result,
                    "from_cache": False,
                })

            st.session_state.recent_analyses = st.session_state.recent_analyses[:10]
            _append_genie_question(a["question"], clicked_key)
        st.rerun()

    st.markdown("<div style='height:20px;'></div>", unsafe_allow_html=True)
    
    # Two Column Layout: Sidebar + AI Assistant (pinned to top so both panels start at same level)
    left_col, right_col = st.columns([0.35, 0.65], gap="medium", vertical_alignment="top")
    
    # LEFT COLUMN: Saved Insights & Recent Analysis (pinned to top)
    with left_col:
        if st.session_state.get("genie_history_error"):
            st.warning(f"Question history not saving: {st.session_state.genie_history_error}")
        # Surface chat persistence error if table creation failed
        if st.session_state.get("_chat_persist_error"):
            st.warning(f"Chat history not saving: {st.session_state['_chat_persist_error']}")
        # Show cache write errors if any
        if st.session_state.get("_cache_write_error"):
            st.warning(f"Cache not writing: {st.session_state['_cache_write_error']}")
        # ── YAML Auto-Sync status notification ───────────────────────────────
        _yaml_sync_res = st.session_state.get("yaml_sync_result")
        if _yaml_sync_res:
            _added = _yaml_sync_res.get("added_views", [])
            _sync_status = _yaml_sync_res.get("status", "")
            if _added:
                # New views were discovered and added — show a green success banner
                st.success(
                    f"Semantic model updated — — {len(_added)} new view(s) added: "
                    f"`{'`, `'.join(_added)}`"
                )
            elif _sync_status == "error":
                # Non-fatal: show a subtle warning (Genie still works with the existing YAML)
                st.warning(f"YAML sync issue: {_yaml_sync_res.get('message', '')[:200]}")
            # "no_changes" status is silent — no notification needed

        # If viewer identity is unknown, show how to enable it (READ SESSION for warehouse runtime)
        if not _get_current_user_raw():
            owner_role = _get_app_owner_role()
            grant_sql = f'GRANT READ SESSION ON ACCOUNT TO ROLE {owner_role};' if owner_role else "GRANT READ SESSION ON ACCOUNT TO ROLE <app_owner_role>;"
            st.info(
                "**User is showing as UNKNOWN.** To record your identity in Streamlit in Snowflake (warehouse runtime), "
                "an account admin must run: `" + grant_sql + "` "
                "See: Row access policies in Streamlit in Snowflake. Container runtimes do not expose viewer identity."
            )

        with st.container(border=True):
            st.markdown(
                '<div class="genie-left-col-top" style="width:100%;height:0;margin:0;padding:0;overflow:hidden;position:absolute;pointer-events:none;"></div>',
                unsafe_allow_html=True,
            )
            # Section 1: Saved Insights
            with st.expander("Saved insights", expanded=False):
                saved_insights = _get_saved_insights_for_user(20, page="genie")
                if saved_insights:
                    for i, item in enumerate(saved_insights):
                        title = item["title"] or (item["question"][:55] + "…")
                        q = item["question"]
                        if st.button(title, key=f"saved_insight_{i}", use_container_width=True, type="secondary"):
                            st.session_state.selected_analysis = "custom"
                            st.session_state.last_custom_query = q
                            st.session_state.show_analysis = True
                            with st.spinner("Running saved insight..."):
                                st.session_state.analyst_response = process_genie_query(q)
                            st.rerun()
                else:
                    st.markdown("""
                    <div style="border:2px dashed #e2e8f0;border-radius:12px;padding:16px 12px;text-align:center;">
                        <div style="font-size:13px;color:#94a3b8;">Save any Genie answer to see it here.</div>
                    </div>
                    """, unsafe_allow_html=True)

            # Section 2: Frequently asked by you
            with st.expander("Frequently asked by you", expanded=False):
                faqs_by_you = _get_frequent_questions_by_user(5)
                st.markdown('<div id="genie-faqs">', unsafe_allow_html=True)
                if faqs_by_you:
                    for i, item in enumerate(faqs_by_you):
                        q = item["query"]
                        cnt = item["count"]
                        label = (q[:55] + "…") if len(q) > 55 else q
                        if st.button(
                            f"{label} ({cnt})",
                            key=f"faq_by_you_{i}",
                            use_container_width=True,
                            type="secondary",
                        ):
                            st.session_state.selected_analysis = "custom"
                            st.session_state.last_custom_query = q
                            st.session_state.show_analysis = True
                            with st.spinner("Running..."):
                                st.session_state.analyst_response = process_genie_query(q)
                            st.rerun()
                else:
                    st.markdown("""
                    <div style="border:2px dashed #e2e8f0;border-radius:12px;padding:16px 12px;text-align:center;">
                        <div style="font-size:13px;color:#94a3b8;">No questions yet. Ask Genie to see your frequent questions here.</div>
                    </div>
                    """, unsafe_allow_html=True)
                st.markdown("</div>", unsafe_allow_html=True)

            # Section 3: Most frequent (all)
            with st.expander("Most frequent (all)", expanded=False):
                faqs = _get_frequent_questions(5)
                if faqs:
                    for i, item in enumerate(faqs):
                        q = item["query"]
                        cnt = item["count"]
                        label = (q[:55] + "…") if len(q) > 55 else q
                        if st.button(
                            f"{label} ({cnt})",
                            key=f"faq_{i}",
                            use_container_width=True,
                            type="secondary",
                        ):
                            st.session_state.selected_analysis = "custom"
                            st.session_state.last_custom_query = q
                            st.session_state.show_analysis = True
                            with st.spinner("Running..."):
                                st.session_state.analyst_response = process_genie_query(q)
                            st.rerun()
                else:
                    st.caption("Ask questions to see most frequent across all users.")

        # Removed stray closing div that could render as text
    
    # RIGHT COLUMN: AI Assistant
    with right_col:
        with st.container(border=True):

            # ── Header row: Title | Chats | Summarize | Export | Clear ────────
            _msg_count = len(st.session_state.get("genie_messages", []))
            _hdr_left, _hdr_chats, _hdr_sum, _hdr_dl, _hdr_clr = st.columns(
                [2.2, 0.9, 1.2, 1.1, 0.9], gap="small"
            )
            with _hdr_left:
                st.markdown(
                    '<div style="font-size:15px;font-weight:800;color:#0f172a;padding-top:6px;">AI Assistant</div>',
                    unsafe_allow_html=True,
                )
            with _hdr_chats:
                _chats_clicked = st.button("Chats", use_container_width=True,
                                           help="Browse & resume previous conversations",
                                           key="btn_genie_chats")
            with _hdr_sum:
                _sum_clicked = st.button("Summarize", use_container_width=True,
                                         disabled=_msg_count < 2,
                                         help="Compress conversation into a summary",
                                         key="btn_genie_summarize")
            with _hdr_dl:
                def _build_md_export():
                    _msgs  = st.session_state.get("genie_messages", [])
                    _label = st.session_state.get("genie_session_label", "Chat")
                    _lines = [f"# ProcureIQ Genie — {_label}", "",
                              f"*Exported {datetime.now().strftime('%Y-%m-%d %H:%M')}*", "", "---", ""]
                    for _m in _msgs:
                        if not _m.get("content"): continue
                        _pfx = "**You:** " if _m["role"] == "user" else "**Genie:** "
                        _lines.append(_pfx + _m["content"]); _lines.append("")
                    return "\n".join(_lines)
                st.download_button(
                    "Export MD", data=_build_md_export(),
                    file_name="genie_chat_" + datetime.now().strftime("%Y%m%d_%H%M") + ".md",
                    mime="text/markdown", use_container_width=True,
                    disabled=_msg_count == 0, help="Download chat as Markdown",
                    key="btn_genie_export",
                )
            with _hdr_clr:
                _clr_clicked = st.button("Clear", use_container_width=True,
                                         disabled=_msg_count == 0,
                                         type="secondary", help="Clear messages & start fresh",
                                         key="btn_genie_clear")

            # ── Handle: Chats panel toggle ────────────────────────────────────
            if _chats_clicked:
                _prev = st.session_state.get("show_chats_panel", False)
                st.session_state["show_chats_panel"] = not _prev
                # Always refresh sessions list from DB when opening the panel
                if not _prev:
                    _cp_tmp = st.session_state.get("chat_persistence")
                    st.session_state["_all_sessions_cache"] = (
                        _cp_tmp.load_all_sessions() if _cp_tmp else []
                    )

            if st.session_state.get("show_chats_panel", False):
                _all_sess = st.session_state.get("_all_sessions_cache", [])
                _cp_tmp   = st.session_state.get("chat_persistence")
                with st.container(border=True):
                    st.markdown(
                        "<div style='font-size:14px;font-weight:800;color:#1e40af;"
                        "margin-bottom:12px;'>Previous Conversations</div>",
                        unsafe_allow_html=True,
                    )
                    import uuid as _uuid_panel
                    if st.button("New Conversation", key="btn_panel_new",
                                 use_container_width=True, type="primary"):
                        st.session_state.genie_messages      = []
                        st.session_state.analyst_response    = None
                        st.session_state.show_analysis       = False
                        st.session_state.selected_analysis   = None
                        st.session_state.last_custom_query   = ""
                        st.session_state.genie_session_id    = str(_uuid_panel.uuid4())
                        st.session_state.genie_session_label = "Chat on " + datetime.now().strftime("%b %d %H:%M")
                        st.session_state.chat_turn_index     = 0
                        st.session_state.restore_dismissed   = True
                        st.session_state.restore_offered     = False
                        st.session_state["show_chats_panel"] = False
                        st.rerun()
                    st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
                    if not _all_sess:
                        st.info("No previous conversations found in the last 7 days.", icon="💬")
                    else:
                        for _si, _sess in enumerate(_all_sess):
                            _age_h  = _sess.get("age_hours", 0)
                            _label  = _sess.get("session_label", "Previous chat")
                            _nturns = _sess.get("turn_count", 0)
                            _age_str = ("< 1 hr ago" if _age_h < 1 else
                                        f"{int(_age_h)}h ago" if _age_h < 24 else
                                        f"{int(_age_h/24)}d {int(_age_h%24)}h ago")
                            _is_cur = _sess.get("session_id") == st.session_state.get("genie_session_id", "")
                            _sl, _sr = st.columns([5, 2], gap="small")
                            with _sl:
                                _cur_tag = (" <span style='background:#dcfce7;color:#15803d;"
                                            "border-radius:10px;padding:1px 8px;font-size:10px;"
                                            "font-weight:700;'>Active</span>") if _is_cur else ""
                                st.markdown(
                                    f"<div style='background:{'#eff6ff' if _is_cur else '#f8fafc'};"
                                    f"border:1px solid {'#bfdbfe' if _is_cur else '#e2e8f0'};"
                                    f"border-radius:10px;padding:9px 12px;margin-bottom:4px;'>"
                                    f"<div style='font-size:13px;font-weight:700;color:#0f172a;'>"
                                    f"{html.escape(_label)}{_cur_tag}</div>"
                                    f"<div style='font-size:11px;color:#64748b;margin-top:2px;'>"
                                    f"{_nturns} messages &nbsp;·&nbsp; {_age_str}</div></div>",
                                    unsafe_allow_html=True,
                                )
                            with _sr:
                                if _is_cur:
                                    st.markdown(
                                        "<div style='padding:10px 0;text-align:center;"
                                        "font-size:12px;color:#64748b;'>Current</div>",
                                        unsafe_allow_html=True,
                                    )
                                else:
                                    if st.button("Resume", key=f"btn_panel_resume_{_si}",
                                                 use_container_width=True, type="primary"):
                                        with st.spinner("Loading conversation..."):
                                            _msgs = _cp_tmp.load_session_messages(_sess["session_id"]) if _cp_tmp else []
                                        st.session_state.genie_messages      = _msgs
                                        st.session_state.chat_turn_index     = len(_msgs)
                                        st.session_state.genie_session_id    = _sess["session_id"]
                                        st.session_state.genie_session_label = _sess["session_label"]
                                        st.session_state.restore_dismissed   = True
                                        st.session_state["show_chats_panel"] = False
                                        st.rerun()
                    st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)
                    if st.button("Close", key="btn_panel_close",
                                 use_container_width=True, type="secondary"):
                        st.session_state["show_chats_panel"] = False
                        st.rerun()

            # ── Handle: Summarize ─────────────────────────────────────────────
            if _sum_clicked:
                _transcript = "\n".join([
                    f"{'User' if _m['role']=='user' else 'Genie'}: {_m.get('content','')}"
                    for _m in st.session_state.get("genie_messages", []) if _m.get("content")
                ])
                try:
                    with st.spinner("Summarizing conversation..."):
                        _tdf = session.sql(
                            "SELECT SNOWFLAKE.CORTEX.COMPLETE(?,?) AS R",
                            params=[CORTEX_PRESCRIPTIVE_MODEL,
                                    "Summarize this procurement analytics conversation in 4-5 bullet points. "
                                    "Keep key findings, vendor names, and important numbers:\n\n"
                                    + _transcript[:3000]]
                        ).to_pandas()
                        _summary = (_tdf.at[0, "R"] if not _tdf.empty else "") or "Previous conversation summarized."
                except Exception:
                    _summary = "Previous conversation context retained."
                st.session_state.genie_messages = [{
                    "role": "assistant", "content": f"Conversation summary:\n{_summary}",
                    "timestamp": pd.Timestamp.now(), "response": None,
                }]
                st.rerun()

            # ── Handle: Clear ─────────────────────────────────────────────────
            if _clr_clicked:
                import uuid as _uuid_clr
                st.session_state.genie_messages      = []
                st.session_state.analyst_response    = None
                st.session_state.show_analysis       = False
                st.session_state.selected_analysis   = None
                st.session_state.last_custom_query   = ""
                st.session_state.genie_session_id    = str(_uuid_clr.uuid4())
                st.session_state.genie_session_label = "Chat on " + datetime.now().strftime("%b %d %H:%M")
                st.session_state.chat_turn_index     = 0
                st.session_state.restore_dismissed   = True
                st.session_state.restore_offered     = False
                st.session_state["_all_sessions_cache"] = []
                st.rerun()

            st.markdown("<div style='height:6px;'></div>", unsafe_allow_html=True)

            # ══════════════════════════════════════════════════════════════════
            # SCROLLABLE CHAT AREA  (fixed height, auto-scroll to bottom)
            # ══════════════════════════════════════════════════════════════════
            with st.container(height=UI.Layout.CHAT_SCROLL_HEIGHT, border=True):

                _all_messages = st.session_state.get("genie_messages", [])
                _cp_ref       = st.session_state.get("chat_persistence")

                # ── Session-restore picker (shown when chat is empty) ─────────
                if (not _all_messages and _cp_ref
                        and not st.session_state.get("restore_dismissed")):
                    # Always load from DB if we haven't offered yet (fresh login)
                    if not st.session_state.get("restore_offered"):
                        try:
                            _sessions = _cp_ref.load_all_sessions()
                        except Exception:
                            _sessions = []
                        st.session_state["_all_sessions_cache"] = _sessions
                        st.session_state["restore_offered"]     = True

                _all_sessions = st.session_state.get("_all_sessions_cache", [])

                if (not _all_messages and _all_sessions
                        and not st.session_state.get("restore_dismissed")):
                    st.markdown("""
                    <div class="resume-banner">
                        <div style="font-size:16px;font-weight:800;color:#1e40af;margin-bottom:4px;">
                            Resume a previous conversation
                        </div>
                        <div style="font-size:13px;color:#374151;margin-bottom:14px;">
                            You have chats from the last 7 days. Pick one to continue, or start fresh.
                        </div>
                    </div>""", unsafe_allow_html=True)
                    for _si2, _sess2 in enumerate(_all_sessions):
                        _age_h2   = _sess2.get("age_hours", 0)
                        _label2   = _sess2.get("session_label", "Previous chat")
                        _nturns2  = _sess2.get("turn_count", 0)
                        _age_str2 = ("< 1 hr ago" if _age_h2 < 1 else
                                     f"{int(_age_h2)}h ago" if _age_h2 < 24 else
                                     f"{int(_age_h2/24)}d {int(_age_h2%24)}h ago")
                        _rc_l, _rc_r = st.columns([5, 2], gap="small")
                        with _rc_l:
                            st.markdown(
                                f'<div style="background:#fff;border:1px solid #e2e8f0;'
                                f'border-radius:10px;padding:10px 14px;margin-bottom:6px;">'
                                f'<div style="font-size:13px;font-weight:700;color:#0f172a;">'
                                f'{html.escape(_label2)}</div>'
                                f'<div style="font-size:11px;color:#64748b;margin-top:2px;">'
                                f'{_nturns2} messages &nbsp;·&nbsp; {_age_str2}</div></div>',
                                unsafe_allow_html=True,
                            )
                        with _rc_r:
                            if st.button("Resume", key=f"btn_resume_{_si2}",
                                         use_container_width=True, type="primary"):
                                with st.spinner("Loading conversation..."):
                                    _msgs2 = _cp_ref.load_session_messages(_sess2["session_id"])
                                st.session_state.genie_messages      = _msgs2
                                st.session_state.chat_turn_index     = len(_msgs2)
                                st.session_state.genie_session_id    = _sess2["session_id"]
                                st.session_state.genie_session_label = _sess2["session_label"]
                                st.session_state.restore_dismissed   = True
                                st.rerun()
                    st.markdown("<div style='height:6px;'></div>", unsafe_allow_html=True)
                    if st.button("Start a new conversation", key="btn_start_fresh",
                                 use_container_width=True, type="secondary"):
                        st.session_state.restore_dismissed = True
                        st.rerun()

                elif not _all_messages:
                    # Empty state — only show if no analysis is running either
                    if not st.session_state.get("show_analysis"):
                        st.markdown("""
                        <div style="display:flex;flex-direction:column;align-items:center;
                             justify-content:center;height:500px;text-align:center;">
                            <div style="font-size:52px;margin-bottom:16px;"></div>
                            <div style="font-size:18px;font-weight:800;color:#1a1a1a;margin-bottom:8px;">
                                Start a Conversation
                            </div>
                            <div style="font-size:14px;color:#64748b;max-width:400px;">
                                Ask questions about your Procurement to Pay data,
                                or select a pre-built analysis from the library.
                            </div>
                        </div>""", unsafe_allow_html=True)

                # ── Render message bubbles ────────────────────────────────────
                for _msg_idx, _msg in enumerate(_all_messages):
                    _role     = _msg.get("role", "user")
                    _content  = _msg.get("content", "") or ""
                    _response = _msg.get("response")
                    _cached   = _msg.get("from_cache", False)

                    if _role == "user":
                        st.markdown(f"""
                        <div class="g-user">
                            <div class="g-user-inner">
                                <div class="g-user-lbl">You</div>
                                {html.escape(_content)}
                            </div>
                        </div>""", unsafe_allow_html=True)
                    else:
                        # Cache-hit badge
                        if _cached:
                            st.markdown(
                                '<span class="cache-badge">Cache hit — answered instantly</span>',
                                unsafe_allow_html=True,
                            )
                        if _content:
                            st.markdown(f"""
                            <div class="g-ai">
                                <div class="g-ai-inner">
                                    <div class="g-ai-lbl">AI Assistant</div>
                                    {html.escape(_content)}
                                </div>
                            </div>""", unsafe_allow_html=True)
                        # Full response card (charts, tables, SQL) — only for non-latest msgs
                        # Latest response renders in the full panel below to avoid duplication
                        _is_latest_msg = (_msg_idx == len(_all_messages) - 1)
                        if _response and not _is_latest_msg:
                            # For older messages: only show SQL expanders, not full text (already in bubble)
                            _old_blocks = (_response.get("message", {}).get("content", [])
                                           if isinstance(_response, dict) else [])
                            for _ob in _old_blocks:
                                if _ob.get("type") == "sql":
                                    with st.expander("View SQL used", expanded=False):
                                        st.code(_ob.get("statement", ""), language="sql")

                # ── Full result panel renders INSIDE the scroll container ────
                # Derive the active response: prefer session_state.analyst_response,
                # but fall back to the last assistant message's response object.
                # This means resumed sessions and new queries both show charts.
                _active_response = st.session_state.get("analyst_response")
                if not _active_response and _all_messages:
                    # Walk backwards to find last assistant message with a real response
                    for _lm in reversed(_all_messages):
                        if _lm.get("role") == "assistant" and _lm.get("response"):
                            _active_response = _lm["response"]
                            break

                if _active_response and not isinstance(_active_response, bool):
                    # Always show analysis panel when there is a response to display
                    st.session_state.show_analysis = True
                    st.session_state.analyst_response = _active_response

                if st.session_state.get("show_analysis") and st.session_state.get("analyst_response"):
                    _resp_inner   = st.session_state.analyst_response
                    _akey_inner   = st.session_state.selected_analysis
                    _a_inner      = QUICK_ANALYSES.get(_akey_inner, {})
                    _dtitle_inner = (
                        (st.session_state.get("last_custom_query") or "Custom Query")
                        if _akey_inner == "custom"
                        else _a_inner.get("title", "Analysis")
                    )
                    _dtitle_safe  = html.escape(str(_dtitle_inner))

                    if "error" in _resp_inner:
                        # Show a friendly error + fallback if available
                        _err_msg = str(_resp_inner["error"])
                        st.markdown(f"""
                        <div style="background:#fef2f2;border:1.5px solid #fecaca;border-radius:12px;
                             padding:14px 16px;margin:10px 0;">
                            <div style="font-size:13px;font-weight:700;color:#b91c1c;margin-bottom:4px;">
                                ⚠️ Could not get AI answer
                            </div>
                            <div style="font-size:12px;color:#7f1d1d;">{html.escape(_err_msg)}</div>
                        </div>""", unsafe_allow_html=True)
                        # Try fallback SQL for quick analyses
                        if _akey_inner and _akey_inner != "custom":
                            _fb_queries = {
                                "spending_overview": f"""
                                    SELECT TO_CHAR(POSTING_DATE,'YYYY-MM') AS MONTH,
                                           SUM(INVOICE_AMOUNT_LOCAL) AS TOTAL_SPEND,
                                           COUNT(DISTINCT INVOICE_NUMBER) AS INVOICE_COUNT
                                    FROM {DB}.{SCHEMA}.FACT_ALL_SOURCES_VW
                                    WHERE POSTING_DATE >= DATEADD('month',-6,CURRENT_DATE())
                                    GROUP BY 1 ORDER BY 1""",
                                "vendor_analysis": f"""
                                    SELECT v.VENDOR_NAME, SUM(f.INVOICE_AMOUNT_LOCAL) AS TOTAL_SPEND,
                                           COUNT(DISTINCT f.INVOICE_NUMBER) AS INVOICE_COUNT
                                    FROM {DB}.{SCHEMA}.FACT_ALL_SOURCES_VW f
                                    LEFT JOIN {DB}.{SCHEMA}.DIM_VENDOR_VW v ON f.VENDOR_ID=v.VENDOR_ID
                                    WHERE f.POSTING_DATE >= DATEADD('month',-6,CURRENT_DATE())
                                    GROUP BY 1 ORDER BY 2 DESC""",
                                "payment_performance": f"""
                                    SELECT TO_CHAR(PAYMENT_DATE,'YYYY-MM') AS MONTH,
                                           AVG(DATEDIFF('day',POSTING_DATE,PAYMENT_DATE)) AS AVG_DAYS_TO_PAY,
                                           COUNT(*) AS PAYMENT_COUNT
                                    FROM {DB}.{SCHEMA}.FACT_ALL_SOURCES_VW
                                    WHERE PAYMENT_DATE IS NOT NULL
                                    GROUP BY 1 ORDER BY 1""",
                                "invoice_aging": f"""
                                    SELECT CASE WHEN AGING_DAYS<=30 THEN '0-30 days'
                                                WHEN AGING_DAYS<=60 THEN '31-60 days'
                                                WHEN AGING_DAYS<=90 THEN '61-90 days'
                                                ELSE '90+ days' END AS AGING_BUCKET,
                                           COUNT(*) AS INVOICE_COUNT,
                                           SUM(INVOICE_AMOUNT_LOCAL) AS TOTAL_AMOUNT
                                    FROM {DB}.{SCHEMA}.FACT_ALL_SOURCES_VW
                                    WHERE UPPER(INVOICE_STATUS) IN ('OPEN','PENDING')
                                    GROUP BY 1"""
                            }
                            if _akey_inner in _fb_queries:
                                try:
                                    _fb_df = run_df(_fb_queries[_akey_inner])
                                    if _fb_df is not None and not _fb_df.empty:
                                        st.markdown("**Fallback data (direct query):**")
                                        _xc, _yc = _pick_chart_columns(_fb_df)
                                        if _xc and _yc:
                                            _use_h = str(_xc).upper() in ("AGING_BUCKET","VENDOR_NAME")
                                            alt_bar(_fb_df, x=_xc, y=_yc, color='#5046e5', height=260, horizontal=_use_h)
                                        st.dataframe(_fb_df, use_container_width=True)
                                except Exception:
                                    pass

                    elif _resp_inner.get("layout") == "quick":
                        # ── Quick analysis: render KPI tiles + all charts ──────
                        _akey_q   = st.session_state.get("selected_analysis", "")
                        _a_q      = QUICK_ANALYSES.get(_akey_q, {})
                        _qtitle   = _a_q.get("title", _dtitle_safe)

                        # Controls row: Reset | Save
                        _qc1, _qspace, _qc2 = st.columns([1, 5, 1])
                        with _qc1:
                            if st.button("Reset", key="back_btn_q"):
                                st.session_state.show_analysis = False
                                st.session_state.analyst_response = None
                                st.rerun()
                        with _qc2:
                            _q_text_save = (_a_q.get("question") or "").strip()
                            if _q_text_save and st.button("Save", key="btn_save_quick_inner"):
                                _save_insight(_q_text_save, _qtitle,
                                              analysis_type=_akey_q or "quick", page="genie")

                        st.markdown(f"""
                        <div style="margin:8px 0 14px 0;">
                            <div style="font-size:11px;font-weight:700;color:#64748b;">Your question</div>
                            <div style="font-size:17px;font-weight:800;color:#1a1a1a;">{html.escape(_qtitle)}</div>
                        </div>""", unsafe_allow_html=True)

                        # ── KPI Tiles ─────────────────────────────────────────
                        _m = _resp_inner.get("metrics") or {}
                        if "total_ytd" in _m:
                            _ytd_v = abbr_currency(safe_number(_m.get("total_ytd"), 0))
                            _mom_v = _safe_pct_str(_m.get("mom_pct"), 0)
                            _qoq_v = _safe_pct_str(_m.get("qoq_pct"), 0)
                            _top5_v = safe_int(_m.get("top5_pct"), 0)
                            _mom_num = safe_number(_m.get("mom_pct"), 0)
                            _qoq_num = safe_number(_m.get("qoq_pct"), 0)
                            def _dc(v): return "#059669" if v > 0 else ("#dc2626" if v < 0 else "#0f172a")
                            st.markdown(f"""
                            <div style="background:linear-gradient(135deg,#e0f2fe,#dbeafe);
                                 border:1.5px solid #bae6fd;border-radius:14px;padding:18px;margin-bottom:18px;">
                                <div style="font-size:14px;font-weight:800;color:#0f172a;margin-bottom:12px;">Key Insights</div>
                                <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;">
                                    <div style="background:rgba(255,255,255,.85);padding:12px;border-radius:10px;">
                                        <div style="font-size:11px;color:#64748b;font-weight:700;">Total Spend (YTD)</div>
                                        <div style="font-size:22px;font-weight:900;color:#0f172a;">{_ytd_v}</div>
                                    </div>
                                    <div style="background:rgba(255,255,255,.85);padding:12px;border-radius:10px;">
                                        <div style="font-size:11px;color:#64748b;font-weight:700;">MoM Change</div>
                                        <div style="font-size:22px;font-weight:900;color:{_dc(_mom_num)};">{_mom_v}</div>
                                    </div>
                                    <div style="background:rgba(255,255,255,.85);padding:12px;border-radius:10px;">
                                        <div style="font-size:11px;color:#64748b;font-weight:700;">Top 5 Vendors</div>
                                        <div style="font-size:22px;font-weight:900;color:#0f172a;">{_top5_v}%</div>
                                    </div>
                                    <div style="background:rgba(255,255,255,.85);padding:12px;border-radius:10px;">
                                        <div style="font-size:11px;color:#64748b;font-weight:700;">QoQ Change</div>
                                        <div style="font-size:22px;font-weight:900;color:{_dc(_qoq_num)};">{_qoq_v}</div>
                                    </div>
                                </div>
                            </div>""", unsafe_allow_html=True)
                        elif _m.get("summary"):
                            st.info(_m["summary"])

                        # Anomaly banner
                        if _resp_inner.get("anomaly"):
                            _anomaly_safe = html.escape(str(_resp_inner['anomaly']))
                            st.markdown(
                                f'<div style="background:#fffbeb;border:1.5px solid #fde68a;'
                                f'border-radius:10px;padding:12px 16px;margin-bottom:16px;">'
                                f'<div style="font-size:13px;font-weight:800;color:#0f172a;margin-bottom:4px;">'
                                f'Anomaly Detected</div>'
                                f'<div style="font-size:13px;color:#475569;">{_anomaly_safe}</div>'
                                f'</div>',
                                unsafe_allow_html=True
                            )

                        # ── Monthly trend chart ───────────────────────────────
                        _mdf = _resp_inner.get("monthly_df")
                        if _mdf is not None and not _mdf.empty and "MONTH" in _mdf.columns:
                            _vcol = ("VALUE" if "VALUE" in _mdf.columns
                                     else (_mdf.columns[1] if len(_mdf.columns) > 1 else None))
                            if _vcol:
                                st.markdown("**Monthly Trend**")
                                alt_line_monthly(_mdf, month_col="MONTH", value_col=_vcol,
                                                 height=240, title="Monthly Spend Trend")
                                if "INVOICE_COUNT" in _mdf.columns:
                                    st.markdown("**Invoice Volume by Month**")
                                    alt_bar(_mdf, x="MONTH", y="INVOICE_COUNT", color="#5046e5", height=200)
                                if "VENDOR_COUNT" in _mdf.columns:
                                    st.markdown("**Active Vendors by Month**")
                                    alt_bar(_mdf, x="MONTH", y="VENDOR_COUNT", color="#7c3aed", height=200)

                        # ── Vendor / payment / aging charts ───────────────────
                        _vdf = _resp_inner.get("vendors_df")
                        if _vdf is not None and not _vdf.empty:
                            _vc = "VENDOR_NAME" if "VENDOR_NAME" in _vdf.columns else (
                                  _vdf.columns[0] if len(_vdf.columns) > 0 else None)
                            _vs = ("TOTAL_SPEND" if "TOTAL_SPEND" in _vdf.columns
                                   else ("SPEND" if "SPEND" in _vdf.columns
                                   else (_vdf.columns[1] if len(_vdf.columns) > 1 else None)))
                            if _vc and _vs:
                                st.markdown("**Top Vendors by Spend**")
                                alt_bar(_vdf.head(15), x=_vc, y=_vs,
                                        color="#1459d2", height=260, horizontal=True)
                                st.dataframe(_vdf, use_container_width=True, height=240)

                        # ── Extra DataFrames (payment perf, aging, etc.) ──────
                        _extra = _resp_inner.get("extra_dfs") or {}
                        for _ename, _edf in _extra.items():
                            if _edf is None or _edf.empty or _ename in ("monthly_full",):
                                continue
                            st.markdown(f"**{_ename.replace('_',' ').title()}**")
                            _xc_e, _yc_e = _pick_chart_columns(_edf)
                            if _xc_e and _yc_e:
                                _use_h_e = str(_xc_e).upper() in (
                                    "AGING_BUCKET","VENDOR_NAME","OPPORTUNITY_AREA","STATUS")
                                alt_bar(_edf, x=_xc_e, y=_yc_e, color="#5046e5",
                                        height=240, horizontal=_use_h_e)
                            st.dataframe(_edf, use_container_width=True, height=220)

                        # ── Prescriptive insights from DFs ───────────────────
                        _all_dfs_q = [df for df in [_mdf, _vdf] + list(_extra.values())
                                      if df is not None and not df.empty]
                        if _all_dfs_q:
                            _pres_q = _cortex_complete_prescriptive_from_dfs(
                                _all_dfs_q,
                                _a_q.get("question", _qtitle),
                                context_text=str(_m)[:500] if _m else ""
                            )
                            if not _pres_q:
                                _pres_q = _generate_prescriptive_from_dfs(_all_dfs_q)
                            if _pres_q:
                                with st.expander("Prescriptive — Recommendations & next steps",
                                                 expanded=False):
                                    st.markdown(
                                        f'<div class="prescriptive-content">{_pres_q}</div>',
                                        unsafe_allow_html=True)

                            # ── Predictive section ────────────────────────────
                            _pred_cache_key_q = f"_pred_q_{abs(hash(_a_q.get('question','')[:60])) % 1_000_000}"
                            if _pred_cache_key_q not in st.session_state:
                                st.session_state[_pred_cache_key_q] = _generate_predictive_text(
                                    _a_q.get("question", _qtitle), _all_dfs_q, session, _m
                                )
                            _pred_q = st.session_state.get(_pred_cache_key_q, "")
                            if _pred_q:
                                with st.expander("Predictive — 30–90 Day Forecast", expanded=False):
                                    st.markdown(
                                        f'<div style="background:#f0fdf4;border-left:4px solid #22c55e;'
                                        f'border-radius:8px;padding:14px;font-size:14px;color:#0f172a;'
                                        f'line-height:1.7;">'
                                        f'{html.escape(_pred_q).replace(chr(10),"<br/>")}</div>',
                                        unsafe_allow_html=True)

                        # ── Query used ────────────────────────────────────────
                        _sql_dict_q = _resp_inner.get("sql") or {}
                        if _sql_dict_q:
                            with st.expander("Query used", expanded=False):
                                for _sname, _sstmt in _sql_dict_q.items():
                                    st.markdown(f"**{_sname.replace('_',' ').title()}**")
                                    st.code(str(_sstmt).strip(), language="sql")

                    elif "message" in _resp_inner and "content" in _resp_inner.get("message", {}):
                        # Cortex response — render Descriptive + Prescriptive + SQL results
                        _content_r = _resp_inner["message"]["content"]
                        _all_text_r = ""
                        for _blk_r in _content_r:
                            if _blk_r.get("type") == "text":
                                _all_text_r += "\n\n" + (_blk_r.get("text") or "")
                        _all_text_r = _all_text_r.strip()

                        # Use the current question for this response (not stale last_custom_query)
                        _cur_question = (
                            st.session_state.get("last_custom_query") or _dtitle_inner or ""
                        )

                        # Controls row: Reset | Save
                        _rc1, _rspace, _rc2 = st.columns([1, 5, 1])
                        with _rc1:
                            if st.button("Reset", key="back_btn_cortex"):
                                st.session_state.show_analysis = False
                                st.session_state.analyst_response = None
                                st.rerun()
                        with _rc2:
                            if _cur_question and st.button("Save", key="btn_save_cortex"):
                                _save_insight(_cur_question, _cur_question[:80],
                                              analysis_type=_akey_inner or "custom", page="genie")

                        st.markdown(f"""
                        <div style="margin:8px 0 12px 0;">
                            <div style="font-size:11px;font-weight:700;color:#64748b;">Your question</div>
                            <div style="font-size:15px;font-weight:800;color:#1a1a1a;">{_dtitle_safe}</div>
                        </div>""", unsafe_allow_html=True)

                        _desc_r, _pres_r = _parse_descriptive_prescriptive(_all_text_r) if _all_text_r else (None, None)

                        # Also try the 3-section parser to get predictive
                        _desc_3, _pres_3, _pred_3 = parse_analysis_sections(_all_text_r) if _all_text_r else ("", "", "")
                        # Prefer 3-section parse if it found content
                        if _desc_3:
                            _desc_r = _desc_3
                        if _pres_3:
                            _pres_r = _pres_3

                        _generic_pres = "See the supporting data and charts below for specific numbers to act on."

                        if _all_text_r and not _pres_r:
                            _desc_r = _desc_r or _all_text_r
                            _pres_r = _generic_pres

                        # Only call Cortex for prescriptive if the response is truly generic
                        # AND only if this is the very first render (cache it in session state)
                        _pres_cache_key = f"_pres_{abs(hash(_all_text_r[:100])) % 1_000_000}"
                        _is_generic_r = (
                            _pres_r == _generic_pres
                            or (_pres_r and any(p in (_pres_r or "").lower() for p in (
                                "see the supporting data", "review the data below",
                                "supporting data and charts", "specific numbers to act on"
                            )))
                        )
                        if _is_generic_r:
                            if _pres_cache_key not in st.session_state:
                                _cp_r = _cortex_complete_prescriptive(_content_r, run_df, _cur_question)
                                if not _cp_r:
                                    _cp_r = _generate_prescriptive_from_data(_content_r, run_df)
                                st.session_state[_pres_cache_key] = _cp_r or _pres_r
                            _pres_r = st.session_state.get(_pres_cache_key, _pres_r)

                        if _desc_r and _pres_r:
                            _desc_esc_r = html.escape(_desc_r).replace("\n", "<br/>")
                            st.markdown(f"""
                            <div style="margin-bottom:16px;">
                                <div style="padding:14px;background:#e0f2fe;border-radius:10px;
                                     border-left:4px solid #0284c7;margin-bottom:12px;">
                                    <div style="font-size:12px;font-weight:800;color:#0369a1;margin-bottom:8px;">
                                        Descriptive — What the data shows
                                    </div>
                                    <div style="color:#0f172a;font-size:14px;line-height:1.6;
                                         word-wrap:break-word;overflow-wrap:break-word;max-width:100%;">
                                        {_desc_esc_r}
                                    </div>
                                </div>
                            </div>""", unsafe_allow_html=True)
                            with st.expander("Prescriptive — Recommendations & next steps", expanded=False):
                                st.markdown(f'<div class="prescriptive-content">{_pres_r}</div>',
                                            unsafe_allow_html=True)

                            # ── Predictive section ────────────────────────────
                            _pred_text = _pred_3  # from 3-section parse
                            _pred_cache_key_r = f"_pred_{abs(hash(_all_text_r[:100])) % 1_000_000}"
                            if not _pred_text:
                                if _pred_cache_key_r not in st.session_state:
                                    # Collect any SQL result DFs for context
                                    _sql_dfs_r = []
                                    for _blk_pred in _content_r:
                                        if _blk_pred.get("type") == "sql":
                                            try:
                                                _pdf = run_df(_blk_pred.get("statement", ""))
                                                if _pdf is not None and not _pdf.empty:
                                                    _sql_dfs_r.append(_pdf)
                                            except Exception:
                                                pass
                                    st.session_state[_pred_cache_key_r] = _generate_predictive_text(
                                        _cur_question, _sql_dfs_r, session
                                    )
                                _pred_text = st.session_state.get(_pred_cache_key_r, "")
                            if _pred_text:
                                with st.expander("Predictive — 30–90 Day Forecast", expanded=False):
                                    st.markdown(
                                        f'<div style="background:#f0fdf4;border-left:4px solid #22c55e;'
                                        f'border-radius:8px;padding:14px;font-size:14px;color:#0f172a;'
                                        f'line-height:1.7;">'
                                        f'{html.escape(_pred_text).replace(chr(10),"<br/>")}</div>',
                                        unsafe_allow_html=True)

                        elif _all_text_r:
                            _raw_esc_r = html.escape(_all_text_r).replace("\n", "<br/>")
                            st.markdown(f"""
                            <div style="padding:14px;background:#f5f3ff;border-radius:10px;
                                 border-left:4px solid #5046e5;margin-bottom:16px;">
                                <div style="color:#0f172a;font-size:14px;line-height:1.6;">{_raw_esc_r}</div>
                            </div>""", unsafe_allow_html=True)

                        # Use a stable key based on the response hash to avoid duplicate widget errors
                        _resp_hash = abs(hash(str(id(_resp_inner)))) % 1_000_000
                        for _bidx_r, _blk_r2 in enumerate(_content_r):
                            if _blk_r2.get("type") == "sql":
                                _sql_r = _blk_r2.get("statement", "")
                                try:
                                    _df_r = run_df(_sql_r)
                                    if _df_r is not None and not _df_r.empty:
                                        with st.expander("View supporting data", expanded=True):
                                            _res_r = _has_comparison_columns(_df_r)
                                            _tdf_r = _df_r
                                            if _res_r[1] and _res_r[2]:
                                                _cat_r, _cur_r, _prv_r, _cl_r, _pl_r = _res_r
                                                alt_bar_comparison(_df_r, _cat_r, _cur_r, _prv_r,
                                                                   curr_label=_cl_r or "Current",
                                                                   prev_label=_pl_r or "Previous",
                                                                   height=300)
                                            else:
                                                _xc2, _yc2 = _pick_chart_columns(_df_r)
                                                if _xc2 and _yc2:
                                                    _use_h2 = str(_xc2).upper() in (
                                                        "OPPORTUNITY_AREA","AGING_BUCKET","VENDOR_NAME",
                                                        "FINDING","PO_PURPOSE","DRIVER_VALUE","DRIVER")
                                                    alt_bar(_df_r, x=_xc2, y=_yc2, color='#5046e5',
                                                            height=280, horizontal=_use_h2)
                                            st.dataframe(_tdf_r, use_container_width=True, height=280)
                                            st.download_button("Download Results",
                                                               _df_r.to_csv(index=False),
                                                               "results.csv",
                                                               key=f"dl_inner_{_resp_hash}_{_bidx_r}")
                                    with st.expander(f"View SQL used", expanded=False):
                                        st.code(_sql_r, language="sql")
                                except Exception as _eq:
                                    st.error(f"Query error: {_eq}")
                                    with st.expander("View SQL used"):
                                        st.code(_sql_r, language="sql")

                # Auto-scroll anchor
                st.markdown('<div id="genie-bottom" style="height:4px;"></div>',
                            unsafe_allow_html=True)

            # Auto-scroll JS (runs after every rerun)
            st.markdown(_build_autoscroll_js(), unsafe_allow_html=True)

            # Chat Input at bottom (form so Enter key submits the question)
            st.markdown("<div style='height:20px;'></div>", unsafe_allow_html=True)
            
            with st.form("genie_question_form", clear_on_submit=True):
                input_col, btn_col = st.columns([0.88, 0.12])
                with input_col:
                    user_query = st.text_input(
                        "Ask a question",
                        placeholder="Ask about Procurement to Pay data...",
                        label_visibility="collapsed",
                        key=f"genie_chat_input_{st.session_state.genie_input_version}"
                    )
                with btn_col:
                    send_clicked = st.form_submit_button("→")
            
            if send_clicked and user_query:
                st.session_state.selected_analysis = "custom"
                st.session_state.last_custom_query = user_query.strip()
                st.session_state.show_analysis = True
                st.session_state.genie_input_version = st.session_state.genie_input_version + 1
                # Clear any cached prescriptive/predictive from previous question
                for _k in list(st.session_state.keys()):
                    if str(_k).startswith("_pres_") or str(_k).startswith("_pred_"):
                        del st.session_state[_k]
                with st.spinner("Analyzing..."):
                    st.session_state.analyst_response = process_genie_query(user_query)
                st.rerun() 
