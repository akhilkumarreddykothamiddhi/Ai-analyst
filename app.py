"""
Techwish AI Analytics — Flask Backend (FIXED)
==============================================
Fixes applied:
  1. SQL generated & validated at schedule-creation time (NL mode)
  2. _run_due_reports now updates last_status even on crash + logs full traceback
  3. run_query_direct has login_timeout + network_timeout
  4. SMTP test endpoint added  (/api/test-email)
  5. send_scheduled_email has better error reporting
  6. Scheduler loop logs SQL before running so empty-SQL is caught early
  7. submitSchedule UI fix: NL questions get SQL resolved before saving
Run:  python app.py
Opens browser at http://localhost:5000 automatically.
"""

import os, sys, json, re as _re, uuid, datetime, threading, time, io, base64
import pathlib, smtplib, traceback, webbrowser, signal
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pandas as pd
import snowflake.connector
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
from groq import Groq
import plotly.express as px

from flask import Flask, request, jsonify, send_from_directory, Response

# ─────────────────────────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────────────────────────
BASE_DIR      = pathlib.Path(__file__).parent
SCHEDULE_FILE = BASE_DIR / "scheduled_reports.json"
LOG_FILE      = BASE_DIR / "scheduler.log"

def _read_secrets() -> dict:
    p = BASE_DIR / ".streamlit" / "secrets.toml"
    out = {}
    if p.exists():
        for line in p.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if "=" in line and not line.startswith("#") and not line.startswith("["):
                k, _, v = line.partition("=")
                out[k.strip()] = v.strip().strip('"').strip("'")
    return out

_S = _read_secrets()

def cfg(key: str, default: str = "") -> str:
    return _S.get(key) or os.environ.get(key, default)

def _clean_account(raw: str) -> str:
    raw = raw.strip()
    return _re.sub(r'\.snowflakecomputing\.com.*$', '', raw, flags=_re.IGNORECASE)

SNOWFLAKE_ACCOUNT   = _clean_account(cfg("SNOWFLAKE_ACCOUNT"))
SNOWFLAKE_USER      = cfg("SNOWFLAKE_USER").strip()
SNOWFLAKE_PASSWORD  = cfg("SNOWFLAKE_PASSWORD").strip()
SNOWFLAKE_WAREHOUSE = cfg("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH").strip()
SNOWFLAKE_ROLE      = cfg("SNOWFLAKE_ROLE", "").strip()
GROQ_API_KEY        = cfg("GROQ_API_KEY").strip()
SMTP_HOST           = cfg("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT           = int(cfg("SMTP_PORT", "587"))
SMTP_USER           = cfg("SMTP_USER", "")
SMTP_PASSWORD       = cfg("SMTP_PASSWORD", "")

GROQ_MODEL = "llama-3.1-8b-instant"

DEFAULT_CHART_COLOR   = "#1565C0"
DEFAULT_BLUE_SEQUENCE = [
    "#1565C0","#1976D2","#1E88E5","#42A5F5",
    "#90CAF9","#BBDEFB","#0D47A1","#0288D1",
]

app = Flask(__name__, static_folder=str(BASE_DIR), static_url_path='')

# ─────────────────────────────────────────────────────────────────
#  SNOWFLAKE
# ─────────────────────────────────────────────────────────────────
_conn_lock = threading.Lock()
_sf_conn   = None

def _new_conn(database: str = None):
    kw = dict(
        account=SNOWFLAKE_ACCOUNT, user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD, warehouse=SNOWFLAKE_WAREHOUSE,
        session_parameters={"CLIENT_SESSION_KEEP_ALIVE": "TRUE"},
        network_timeout=300, login_timeout=60,
    )
    if database: kw["database"] = database
    if SNOWFLAKE_ROLE: kw["role"] = SNOWFLAKE_ROLE
    return snowflake.connector.connect(**kw)

def _get_conn():
    global _sf_conn
    with _conn_lock:
        if _sf_conn is not None:
            try:
                _sf_conn.cursor().execute("SELECT 1")
                return _sf_conn
            except Exception:
                pass
        _sf_conn = _new_conn()
        return _sf_conn

def run_query(sql: str, database: str) -> pd.DataFrame:
    sql = sql.strip().rstrip(";").strip()
    def _exec(conn):
        cur = conn.cursor()
        cur.execute(f'USE DATABASE "{database}"')
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)
    conn = _get_conn()
    try:
        return _exec(conn)
    except Exception as e:
        err = str(e)
        if any(x in err for x in ("08001","390114","Authentication","connection","session")):
            global _sf_conn
            _sf_conn = _new_conn()
            return _exec(_sf_conn)
        raise

# FIX ❸: Added login_timeout + network_timeout to prevent silent hangs
def run_query_direct(sql: str, database: str) -> pd.DataFrame:
    sql = sql.strip().rstrip(";").strip()
    if not sql:
        raise ValueError("SQL is empty — cannot execute query.")
    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        role=SNOWFLAKE_ROLE if SNOWFLAKE_ROLE else None,
        database=database,
        login_timeout=30,       # FIX: was missing — prevents silent hang on suspended warehouse
        network_timeout=120,    # FIX: was missing
    )
    try:
        cur = conn.cursor()
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)
    finally:
        conn.close()

def list_databases() -> list:
    try:
        cur = _get_conn().cursor()
        cur.execute("SHOW DATABASES")
        sys_dbs = {"SNOWFLAKE", "SNOWFLAKE_SAMPLE_DATA"}
        return sorted(r[1] for r in cur.fetchall() if r[1] not in sys_dbs)
    except Exception:
        return []

# ─────────────────────────────────────────────────────────────────
#  SCHEMA / WHITELIST (in-memory cache)
# ─────────────────────────────────────────────────────────────────
_schema_cache: dict = {}
_whitelist_cache: dict = {}
_schema_dict_cache: dict = {}
_exact_columns_cache: dict = {}

def load_schema(database: str) -> str:
    if database in _schema_cache:
        return _schema_cache[database]
    sql = f"""
        SELECT TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,DATA_TYPE,IS_NULLABLE
        FROM {database}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA')
        ORDER BY TABLE_SCHEMA,TABLE_NAME,ORDINAL_POSITION"""
    try:
        df = run_query(sql, database)
        lines, cur = [], None
        for _, row in df.iterrows():
            fn = f"{row['TABLE_SCHEMA']}.{row['TABLE_NAME']}"
            if fn != cur:
                cur = fn; lines.append(f"\nTable: {fn}")
            nl = "nullable" if row["IS_NULLABLE"] == "YES" else "not null"
            lines.append(f"  - {row['COLUMN_NAME']} ({row['DATA_TYPE']}, {nl})")
        result = "\n".join(lines)
        _schema_cache[database] = result
        return result
    except Exception as e:
        return f"Schema load failed: {e}"

def build_whitelist(database: str) -> dict:
    if database in _whitelist_cache:
        return _whitelist_cache[database]
    sql = f"""
        SELECT TABLE_NAME,COLUMN_NAME
        FROM {database}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA')
        ORDER BY TABLE_NAME,ORDINAL_POSITION"""
    try:
        df = run_query(sql, database)
        wl = {}
        for _, row in df.iterrows():
            wl.setdefault(row["TABLE_NAME"], []).append(row["COLUMN_NAME"])
        _whitelist_cache[database] = wl
        return wl
    except Exception:
        return {}

def build_full_schema_dict(database: str) -> dict:
    if database in _schema_dict_cache:
        return _schema_dict_cache[database]
    sql = f"""
        SELECT TABLE_NAME,COLUMN_NAME,DATA_TYPE
        FROM {database}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA')"""
    try:
        df = run_query(sql, database)
        sd = {}
        for _, row in df.iterrows():
            sd.setdefault(row["TABLE_NAME"].upper(), {})[row["COLUMN_NAME"].upper()] = row["DATA_TYPE"].upper()
        _schema_dict_cache[database] = sd
        return sd
    except Exception:
        return {}

def get_exact_columns(database: str) -> dict:
    if database in _exact_columns_cache:
        return _exact_columns_cache[database]
    sql = f"""
        SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION
        FROM {database}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA')
        ORDER BY TABLE_NAME, ORDINAL_POSITION"""
    try:
        df = run_query(sql, database)
        result = {}
        for _, row in df.iterrows():
            tbl  = row["TABLE_NAME"]
            col  = row["COLUMN_NAME"]
            dtype = row["DATA_TYPE"]
            result.setdefault(tbl, []).append((col, dtype))
        _exact_columns_cache[database] = result
        return result
    except Exception:
        return {}

# ─────────────────────────────────────────────────────────────────
#  SCHEMA BLOCK BUILDERS
# ─────────────────────────────────────────────────────────────────

def build_exact_schema_block(database: str) -> str:
    exact = get_exact_columns(database)
    lines = []
    for tbl, cols in exact.items():
        lines.append(f'\nTABLE: {tbl}')
        lines.append(f'  Columns (copy EXACTLY, these are case-sensitive Snowflake identifiers):')
        for col, dtype in cols:
            lines.append(f'    "{col}"    {dtype}')
    return "\n".join(lines)

def build_join_hints(database: str) -> str:
    exact = get_exact_columns(database)
    col_map: dict = {}
    for tbl, cols in exact.items():
        for col, dtype in cols:
            col_map.setdefault(col.upper(), []).append((tbl, col))

    hints = []
    for tbl, cols in exact.items():
        if not tbl.upper().startswith("FACT"):
            continue
        for col, dtype in cols:
            cu = col.upper()
            if cu.endswith("_KEY") or cu.endswith("_ID") or cu.endswith("_SK"):
                candidates = []
                for dim_tbl, dim_cols in exact.items():
                    if not dim_tbl.upper().startswith("DIM"):
                        continue
                    dim_col_names_upper = [c.upper() for c, _ in dim_cols]
                    base = cu.replace("_KEY","").replace("_ID","").replace("_SK","")
                    if "ID" in dim_col_names_upper:
                        exact_id = next(c for c, _ in dim_cols if c.upper() == "ID")
                        if base in dim_tbl.upper():
                            exact_fact_col = next(c for c, _ in cols if c.upper() == cu)
                            candidates.append(f'  "{tbl}"."{exact_fact_col}" = "{dim_tbl}"."{exact_id}"')
                    if cu in dim_col_names_upper:
                        exact_dim_col = next(c for c, _ in dim_cols if c.upper() == cu)
                        exact_fact_col = next(c for c, _ in cols if c.upper() == cu)
                        if (tbl, exact_fact_col) not in [(x,y) for x,y in [(tbl,col)]]:
                            candidates.append(f'  "{tbl}"."{exact_fact_col}" = "{dim_tbl}"."{exact_dim_col}"')
                hints.extend(candidates)

    if hints:
        return "\nLIKELY JOIN CONDITIONS (use these exact column names):\n" + "\n".join(hints)
    return ""

def build_time_series_hints(schema_dict: dict) -> str:
    classified = classify_columns(schema_dict)
    lines = [
        "",
        "══════════════════════════════════════════════════════",
        "⛔ CRITICAL TEMPORAL MAPPING RULES",
        "══════════════════════════════════════════════════════",
        "1. DIMENSION PRIORITY: Always use DIM_DATE columns for Year/Month grouping if joined.",
        "2. JOIN KEYS: Use the LIKELY JOIN CONDITIONS block above for exact join keys.",
        "3. NO GUESSING: If a column is not in the EXACT SCHEMA BLOCK, do not use it.",
        "4. COPY COLUMN NAMES VERBATIM from the schema block — do not change case.",
    ]
    for tbl, cats in classified.items():
        if "DIM_DATE" in tbl.upper():
            lines.append(f"\n[{tbl}] — use these for time grouping:")
            if cats["year_cols"]:
                lines.append(f"  🎯 YEAR column(s): {', '.join(cats['year_cols'])}")
            if cats["num_cols"]:
                month_cols = [c for c in cats["num_cols"] if "month" in c.lower() or "mon" in c.lower()]
                if month_cols:
                    lines.append(f"  🎯 MONTH column(s): {', '.join(month_cols)}")
    return "\n".join(lines)

def build_date_type_hints(schema_dict: dict) -> str:
    native_date, native_ts, num_date = [], [], []
    for tbl, cols in schema_dict.items():
        for col, dtype in cols.items():
            base = dtype.split("(")[0]
            if base == "DATE": native_date.append(f"{tbl}.{col}")
            elif base in ("TIMESTAMP_NTZ","TIMESTAMP_LTZ","TIMESTAMP_TZ","TIMESTAMP","DATETIME"): native_ts.append(f"{tbl}.{col}")
            elif base in ("NUMBER","NUMERIC","INTEGER","INT","BIGINT","FLOAT","DOUBLE"):
                if any(k in col.lower() for k in ["date","time","dt","day","month","year","_at","_on"]):
                    num_date.append(f"{tbl}.{col}")
    lines = ["⛔ DATE COLUMN TYPES"]
    if native_date:
        lines.append("✅ NATIVE DATE — use directly, never wrap in TO_DATE:")
        lines += [f"   • {c}" for c in native_date]
    if native_ts:
        lines.append("✅ NATIVE TIMESTAMP — use CAST(col AS DATE):")
        lines += [f"   • {c}" for c in native_ts]
    if num_date:
        lines.append("⚠️  NUMERIC date cols — use TO_DATE(LPAD(CAST(col AS VARCHAR),8,'0'),'YYYYMMDD'):")
        lines += [f"   • {c}" for c in num_date]
    return "\n".join(lines)

# ─────────────────────────────────────────────────────────────────
#  DATE HELPERS
# ─────────────────────────────────────────────────────────────────
_date_fmt_cache: dict = {}

def detect_col_date_format(database, table_name, col_name) -> str:
    key = (database.upper(), table_name.upper(), col_name.upper())
    if key in _date_fmt_cache:
        return _date_fmt_cache[key]
    def _store(f):
        _date_fmt_cache[key] = f; return f
    try:
        tbl_only = table_name.upper().split(".")[-1]
        tdf = run_query(f"""SELECT DATA_TYPE FROM {database}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME='{tbl_only}' AND COLUMN_NAME='{col_name.upper()}' LIMIT 1""", database)
        if not tdf.empty:
            declared = str(tdf.iloc[0]["DATA_TYPE"]).upper().split("(")[0]
            if declared == "DATE": return _store("date")
            if declared in ("TIMESTAMP_NTZ","TIMESTAMP_LTZ","TIMESTAMP_TZ","TIMESTAMP","DATETIME"): return _store("timestamp")
    except Exception: pass
    try:
        sdf = run_query(f"SELECT TO_VARCHAR({col_name}) AS V FROM {table_name} WHERE {col_name} IS NOT NULL LIMIT 5", database)
        if not sdf.empty:
            val = str(sdf.iloc[0]["V"]).strip()
            if _re.match(r'^\d{4}-\d{2}-\d{2}', val): return _store("date")
            if _re.match(r'^\d{8}$', val):
                return _store("yyyymmdd_int") if 1900 <= int(val[:4]) <= 2100 else _store("ddmmyyyy_int")
            if _re.match(r'^\d{6}$', val): return _store("yyyymm_int")
    except Exception: pass
    return _store("unknown")

def _to_date_expr(col, fmt) -> str:
    if fmt == "date": return col
    if fmt == "timestamp": return f"CAST({col} AS DATE)"
    if fmt == "yyyymmdd_int": return f"TO_DATE(LPAD(CAST({col} AS VARCHAR),8,'0'),'YYYYMMDD')"
    if fmt == "ddmmyyyy_int": return f"TO_DATE(LPAD(CAST({col} AS VARCHAR),8,'0'),'DDMMYYYY')"
    if fmt == "yyyymm_int": return f"TO_DATE(CAST({col} AS VARCHAR)||'01','YYYYMMDD')"
    return f"TRY_TO_DATE(TO_VARCHAR({col}))"

def fix_date_filter_in_sql(sql: str, database: str) -> str:
    aliases = {}
    for m in _re.finditer(r'\b(?:FROM|JOIN)\s+(\S+)\s+(?:AS\s+)?([A-Za-z_][A-Za-z0-9_]*)\b', sql, _re.IGNORECASE):
        aliases[m.group(2).upper()] = m.group(1).upper()
    seen = set()
    for alias, col in _re.findall(
        r'\b([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_]*(?:KEY|DATE|TIME|DT|DAY|MONTH|YEAR|AT|ON)[A-Za-z0-9_]*)\b',
        sql, _re.IGNORECASE):
        key = f"{alias.upper()}.{col.upper()}"
        if key in seen: continue
        seen.add(key)
        table = aliases.get(alias.upper(), alias.upper())
        fmt   = detect_col_date_format(database, table, col)
        ac    = f"{alias}.{col}"
        if fmt in ("date","timestamp"):
            for pat in [
                rf"TO_DATE\s*\(\s*LPAD\s*\(\s*CAST\s*\(\s*{_re.escape(ac)}\s+AS\s+VARCHAR\s*\)\s*,\s*\d+\s*,\s*'0'\s*\)\s*,\s*'[^']+'\s*\)",
                rf"TO_DATE\s*\(\s*CAST\s*\(\s*{_re.escape(ac)}\s+AS\s+VARCHAR\s*\)\s*,\s*'[^']+'\s*\)",
                rf"TO_DATE\s*\(\s*{_re.escape(ac)}\s*,\s*'[^']+'\s*\)",
                rf"TRY_TO_DATE\s*\(\s*TO_VARCHAR\s*\(\s*{_re.escape(ac)}\s*\)\s*\)",
            ]:
                sql = _re.sub(pat, ac, sql, flags=_re.IGNORECASE)
        else:
            de = _to_date_expr(ac, fmt)
            sql = _re.sub(rf'YEAR\s*\(\s*{_re.escape(ac)}\s*\)', f'YEAR({de})', sql, flags=_re.IGNORECASE)
            sql = _re.sub(rf'MONTH\s*\(\s*{_re.escape(ac)}\s*\)', f'MONTH({de})', sql, flags=_re.IGNORECASE)
            sql = _re.sub(rf"DATE_TRUNC\s*\(\s*'([^']+)'\s*,\s*{_re.escape(ac)}\s*\)",
                          lambda m: f"DATE_TRUNC('{m.group(1)}',{de})", sql, flags=_re.IGNORECASE)
    return sql

# ─────────────────────────────────────────────────────────────────
#  NUMBER FORMATTING
# ─────────────────────────────────────────────────────────────────
_INR_KW = ["inr","rupee","rupees"]
_USD_KW = [
    "revenue","salary","price","cost","fee","earning","earnings","profit","loss",
    "budget","expense","expenses","payment","payments","usd","dollar","dollars",
    "amount","value","subtotal","grand_total","net","gross","charge",
    "charges","rate","invoice","billing","bill","balance","dues",
    "amount_paid","unit_price","sale_price","list_price","invoice_amount",
    "total_sales","total_revenue","sales",
]

def _currency_symbol(col: str):
    c = col.lower()
    if any(k in c for k in _INR_KW): return "₹"
    for kw in _USD_KW:
        if kw in c: return "$"
    return None

def format_dataframe(df: pd.DataFrame) -> list:
    display = df.copy()
    for col in display.columns:
        col_lo = col.lower()
        is_label = any(kw in col_lo for kw in ["year", "yr", "month", "id", "key", "code", "period"])
        if pd.api.types.is_numeric_dtype(display[col]):
            if is_label:
                display[col] = display[col].apply(lambda v: str(int(v)) if pd.notna(v) else "")
            else:
                sym = _currency_symbol(col)
                try:
                    rounded = display[col].round(0).fillna(0).astype(int)
                    display[col] = rounded.apply(lambda v: f"{sym}{int(v):,}" if sym else f"{int(v):,}")
                except: continue
    return display.to_dict("records")

# ─────────────────────────────────────────────────────────────────
#  SCHEMA HELPERS
# ─────────────────────────────────────────────────────────────────

def classify_columns(schema_dict: dict) -> dict:
    result = {}
    DATE_TYPES   = {"DATE"}
    TS_TYPES     = {"TIMESTAMP_NTZ","TIMESTAMP_LTZ","TIMESTAMP_TZ","TIMESTAMP","DATETIME"}
    NUM_TYPES    = {"NUMBER","NUMERIC","INTEGER","INT","BIGINT","SMALLINT","TINYINT",
                    "FLOAT","FLOAT4","FLOAT8","DOUBLE","DOUBLE PRECISION","REAL","DECIMAL"}
    TEXT_TYPES   = {"VARCHAR","TEXT","STRING","CHAR","CHARACTER","NCHAR","NVARCHAR"}
    YEAR_KW      = ("year","yr","fiscal_year","cal_year")
    DATEKEY_KW   = ("key","datekey","date_key","sk","dt","dat","_date","date_")

    for tbl, cols in schema_dict.items():
        year_cols = []; datekey_cols = []; date_cols = []
        ts_cols = []; num_cols = []; txt_cols = []
        for col, dtype in cols.items():
            base = dtype.split("(")[0].upper().strip()
            col_lo = col.lower()
            if base in DATE_TYPES: date_cols.append(col)
            elif base in TS_TYPES: ts_cols.append(col)
            elif base in NUM_TYPES:
                if any(col_lo == kw or col_lo.endswith("_"+kw) or col_lo.startswith(kw+"_") or col_lo == kw
                       for kw in YEAR_KW):
                    year_cols.append(col)
                elif any(kw in col_lo for kw in DATEKEY_KW):
                    datekey_cols.append(col)
                else:
                    num_cols.append(col)
            elif base in TEXT_TYPES:
                txt_cols.append(col)
        result[tbl] = {
            "year_cols": year_cols, "datekey_cols": datekey_cols,
            "date_cols": date_cols, "ts_cols": ts_cols,
            "num_cols": num_cols, "txt_cols": txt_cols,
        }
    return result

# ─────────────────────────────────────────────────────────────────
#  NL → SQL
# ─────────────────────────────────────────────────────────────────
COLOR_NAME_MAP = {
    "red":"#E53935","green":"#43A047","blue":"#1565C0","yellow":"#FDD835",
    "orange":"#FB8C00","purple":"#8E24AA","pink":"#E91E63","teal":"#00897B",
    "cyan":"#00ACC1","indigo":"#3949AB","lime":"#C0CA33","amber":"#FFB300",
    "brown":"#6D4C41","grey":"#757575","gray":"#757575","black":"#212121",
    "navy":"#1A237E","gold":"#F9A825","coral":"#FF7043","magenta":"#D81B60",
    "turquoise":"#00BCD4","lavender":"#7E57C2","crimson":"#B71C1C",
}

def extract_color_from_question(q: str):
    hx = _re.search(r'#?([0-9a-fA-F]{6})\b', q)
    if hx: return f"#{hx.group(1).upper()}"
    ql = q.lower()
    for name, val in COLOR_NAME_MAP.items():
        if _re.search(rf'\b{name}\b', ql): return val
    return None

def is_percentage_query(q: str) -> bool:
    return any(k in q.lower() for k in [
        "percent","percentage","proportion","share","distribution","breakdown",
        "ratio","composition","pie","donut","split","contribution"])

def nl_to_sql(question: str, history: list, database: str) -> dict:
    wl           = build_whitelist(database)
    schema_dict  = build_full_schema_dict(database)
    exact_block  = build_exact_schema_block(database)
    join_hints   = build_join_hints(database)
    date_hints   = build_date_type_hints(schema_dict)
    ts_hints     = build_time_series_hints(schema_dict)

    compact_block = "\n".join(f"  {t}: {', '.join(c)}" for t, c in wl.items())

    last_sql = ""; last_chart = "none"
    last_chart_x = last_chart_y = last_chart_title = last_summary = ""
    last_chart_color = last_title_color = None; last_df = None

    for m in reversed(history):
        if m.get("role") == "assistant" and m.get("sql"):
            last_sql         = m["sql"]
            last_chart       = m.get("chart", "none")
            last_chart_x     = m.get("chart_x", "")
            last_chart_y     = m.get("chart_y", "")
            last_chart_title = m.get("chart_title", "")
            last_chart_color = m.get("chart_color")
            last_title_color = m.get("title_color")
            last_summary     = m.get("summary", "")
            last_df          = m.get("df")
            break

    _q = question.strip().lower()
    _COLOR_ONLY  = bool(_re.search(r'\b(make|change|set|use|turn|switch|update)\b.{0,40}\b(color|colour)\b|\bcolor\b.{0,20}\b(to|as|into)\b', _q))
    _CHART_TYPE  = bool(_re.search(r'\b(make|change|convert|switch|turn)\b.{0,30}\b(bar|line|pie|area|scatter|donut|histogram|funnel|treemap|heatmap|violin|box)\b', _q))
    _TITLE_ONLY  = bool(_re.search(r'\b(change|set|update|rename)\b.{0,20}\btitle\b', _q))
    _appearance_only = last_sql and (_COLOR_ONLY or _CHART_TYPE or _TITLE_ONLY)

    if _appearance_only:
        ext_color = extract_color_from_question(question)
        new_chart_color = ext_color or last_chart_color
        new_chart = last_chart
        ct_m = _re.search(r'\b(bar|line|pie|area|scatter|donut|histogram|funnel|treemap|heatmap|violin|box)\b', _q)
        if ct_m: new_chart = ct_m.group(1)
        new_title = last_chart_title
        tm = _re.search(r'title\s+to\s+["\']?(.+?)["\']?\s*$', _q)
        if tm: new_title = tm.group(1).strip().strip("\"'")
        return {
            "sql": last_sql, "summary": last_summary, "chart": new_chart,
            "chart_x": last_chart_x, "chart_y": last_chart_y, "chart_title": new_title,
            "chart_color": new_chart_color, "title_color": last_title_color, "_reuse_df": last_df,
        }

    system_prompt = f"""You are a Snowflake SQL expert. Your job is to write a single valid Snowflake SQL query.
Strict Constraints:

Grouping Symmetry: Every non-aggregated expression in the SELECT clause MUST be identical in the GROUP BY clause. If an expression uses a function (e.g., CAST, LPAD, TRUNC), the exact same function and logic must appear in the GROUP BY or use positional referencing (e.g., GROUP BY 1).

Explicit Aliasing: Always use short, descriptive aliases for tables (e.g., f for Fact, d for Dim). Every single column reference must be prefixed with its alias.

Column Mapping: Ensure attributes belong to the correct table. Join Dim tables to Fact tables using primary/foreign key relationships.

Date Integrity: For year/month extraction, use DATE_PART or EXTRACT. If formatting as a string, ensure the CAST happens before the LPAD.

No Ambiguity: If a column name exists in multiple joined tables, you must specify the source table to avoid "Ambiguous Identifier" errors. Alias Discipline: Always use table aliases (e.g., f for Fact, d for Dim) and prefix every column reference with the appropriate alias to avoid ambiguous identifier errors.
══════════════════════════════════════════════════════
🔴 RULE 0 — MOST IMPORTANT: COPY COLUMN NAMES EXACTLY
══════════════════════════════════════════════════════
The EXACT SCHEMA BLOCK below lists every table and column with their TRUE casing.
You MUST copy every column name VERBATIM from the schema block.
- If schema says "YEAR" → write "YEAR"  (not "year", not "Year")
- If schema says "year" → write "year"  (not "YEAR", not "Year")
- If schema says "Month" → write "Month" (not "MONTH", not "month")
DO NOT GUESS OR INVENT column names. If you cannot find a column in the schema, say so.

══════════════════════════════════════════════════════
🔴 RULE 1 — QUOTING IDENTIFIERS
══════════════════════════════════════════════════════
- Wrap EVERY table name and column name in double quotes: "TABLE"."COLUMN"
- Once you assign an alias (e.g. "DIM_DATE" AS "t2"), use ONLY the alias for that table.
- RIGHT:  "t2"."YEAR"   (alias + quoted column from schema)
- WRONG:  t2."year"    (unquoted alias)
- WRONG:  "DIM_DATE"."YEAR"  (full table name after alias defined)
- WRONG:  "t2"."year"  (wrong case — schema says YEAR not year)

══════════════════════════════════════════════════════
🔴 RULE 2 — JOIN KEYS
══════════════════════════════════════════════════════
Use ONLY the join conditions listed in LIKELY JOIN CONDITIONS below.
Do NOT invent join keys.

══════════════════════════════════════════════════════
🔴 RULE 3 — TIME-SERIES QUERIES (Year / Month / Date)
══════════════════════════════════════════════════════
- For yearly data:  GROUP BY + SELECT the exact YEAR column from DIM_DATE.
- For monthly data: SELECT and GROUP BY BOTH year AND month columns.
  Create a combined period column:
  ("t2"."<YEAR_COL>" || '-' || LPAD(CAST("t2"."<MONTH_COL>" AS VARCHAR), 2, '0')) AS "period"
  Set chart_x to "period" in your JSON response.
- Always ORDER BY year ASC, month ASC for time-series.
- Use "line" chart for trends, "bar" for comparisons, "donut"/"pie" for percentages.

══════════════════════════════════════════════════════
🔴 RULE 4 — GROUP BY / ORDER BY INTEGRITY
══════════════════════════════════════════════════════
Every column in ORDER BY must also appear in SELECT and GROUP BY.

══════════════════════════════════════════════════════
EXACT SCHEMA BLOCK — COPY COLUMN NAMES VERBATIM:
══════════════════════════════════════════════════════
{exact_block}

{join_hints}

{ts_hints}

{date_hints}

══════════════════════════════════════════════════════
OUTPUT FORMAT — RETURN ONLY THIS JSON, NOTHING ELSE:
══════════════════════════════════════════════════════
{{"sql":"SELECT ...","summary":"one sentence","chart":"bar|line|pie|donut|area|scatter|histogram|funnel|treemap|none","chart_x":"column_name","chart_y":"column_name","chart_title":"title"}}

Rules for JSON output:
- No markdown, no code fences, no explanation text outside the JSON.
- No newlines or tabs inside any JSON string value (use space instead).
- The "sql" value must be a single-line string (no literal newlines).
"""

    user_msg = f"""USER QUESTION: {question}

AVAILABLE TABLES AND COLUMNS:
{compact_block}

Steps:
1. Find the exact column names in the EXACT SCHEMA BLOCK above.
2. Use only those exact names in double quotes.
3. Identify the correct join from LIKELY JOIN CONDITIONS.
4. Write the SQL.

Return ONLY the JSON object."""

    def _safe_parse_json(raw: str) -> dict:
        raw = _re.sub(r'^```[a-z]*\s*', '', raw, flags=_re.MULTILINE)
        raw = _re.sub(r'```\s*$', '', raw, flags=_re.MULTILINE)
        raw = raw.strip()
        jm = _re.search(r'\{.*\}', raw, _re.DOTALL)
        if jm: raw = jm.group(0)
        try: return json.loads(raw)
        except json.JSONDecodeError: pass

        def _fix_string_literals(s: str) -> str:
            result = []; in_str = False; escape_next = False
            for ch in s:
                if escape_next: result.append(ch); escape_next = False
                elif ch == '\\': result.append(ch); escape_next = True
                elif ch == '"': result.append(ch); in_str = not in_str
                elif in_str and ch in ('\n', '\r', '\t'): result.append(' ')
                else: result.append(ch)
            return ''.join(result)

        cleaned = _fix_string_literals(raw)
        try: return json.loads(cleaned)
        except json.JSONDecodeError: pass

        collapsed = _re.sub(r'\s+', ' ', raw)
        try: return json.loads(collapsed)
        except json.JSONDecodeError: pass

        sql_m     = _re.search(r'"sql"\s*:\s*"((?:[^"\\]|\\.)*)"', raw, _re.DOTALL)
        summary_m = _re.search(r'"summary"\s*:\s*"((?:[^"\\]|\\.)*)"', raw, _re.DOTALL)
        chart_m   = _re.search(r'"chart"\s*:\s*"([^"]*)"', raw)
        chart_x_m = _re.search(r'"chart_x"\s*:\s*"([^"]*)"', raw)
        chart_y_m = _re.search(r'"chart_y"\s*:\s*"([^"]*)"', raw)
        chart_t_m = _re.search(r'"chart_title"\s*:\s*"([^"]*)"', raw)

        if sql_m:
            return {
                "sql":         sql_m.group(1).replace('\\n',' ').replace('\\t',' '),
                "summary":     summary_m.group(1) if summary_m else "Query executed.",
                "chart":       chart_m.group(1)   if chart_m   else "none",
                "chart_x":     chart_x_m.group(1) if chart_x_m else "",
                "chart_y":     chart_y_m.group(1) if chart_y_m else "",
                "chart_title": chart_t_m.group(1) if chart_t_m else "",
            }
        raise ValueError(f"Could not parse JSON from Groq response: {raw[:300]}")

    def _call_groq(extra: str = "") -> dict:
        sys_c = system_prompt + ("\n\n" + extra if extra else "")
        msgs  = []
        for m in history[-6:]:
            msgs.append({
                "role": m["role"],
                "content": m["content"] if m["role"] == "user" else m.get("summary","")
            })
        msgs.append({"role":"user","content": user_msg})
        client = Groq(api_key=GROQ_API_KEY)
        for attempt in range(3):
            try:
                resp = client.chat.completions.create(
                    model=GROQ_MODEL,
                    messages=[{"role":"system","content":sys_c}] + msgs,
                    temperature=0.0,
                    max_tokens=1400
                )
                raw = resp.choices[0].message.content.strip()
                return _safe_parse_json(raw)
            except ValueError as e:
                if attempt == 2: raise
                continue
            except Exception as e:
                if "429" in str(e) or "rate" in str(e).lower():
                    if attempt < 2:
                        time.sleep((attempt + 1) * 20)
                        continue
                    raise RuntimeError("Rate limited. Wait 30–60s and retry.")
                raise

    _EMPTY = {
        "sql":"","chart":"none","chart_x":"","chart_y":"",
        "chart_title":"","chart_color":DEFAULT_CHART_COLOR,"title_color":None
    }
    try:
        result = _call_groq()
    except RuntimeError as e:
        return {**_EMPTY, "summary": str(e)}
    except Exception as e:
        return {**_EMPTY, "summary": f"Error generating query: {e}"}

    if not isinstance(result, dict):
        result = _EMPTY.copy()

    if is_percentage_query(question) and result.get("chart","none") not in ("pie","donut"):
        result["chart"] = "donut"

    ext_color = extract_color_from_question(question)
    result["chart_color"] = ext_color or DEFAULT_CHART_COLOR
    result["title_color"] = None

    return result


def get_sample_questions(database: str) -> list:
    wl = build_whitelist(database)
    if not wl:
        return ["How many total records do we have?","What does the overall data look like?",
                "Show me a summary of the main numbers","What are the top 5 entries?"]
    schema_lines = [f"Table '{t}': columns → {', '.join(c)}" for t,c in list(wl.items())[:20]]
    try:
        client = Groq(api_key=GROQ_API_KEY)
        resp   = client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[
                {"role":"system","content":"Generate EXACTLY 4 plain-English business questions a non-technical manager would ask. Output ONLY a JSON array of 4 strings. Under 12 words each."},
                {"role":"user","content":f"Database: {database}\n\nSchema:\n" + "\n".join(schema_lines) + "\n\nReturn JSON array only."},
            ],
            temperature=0.3, max_tokens=400)
        raw = resp.choices[0].message.content.strip()
        raw = _re.sub(r"^```[a-z]*\n?","",raw).strip("`").strip()
        m = _re.search(r'\[.*?\]', raw, _re.DOTALL)
        if m: raw = m.group(0)
        qs = json.loads(raw)
        if isinstance(qs, list) and len(qs) >= 2: return [str(q) for q in qs[:4]]
    except Exception: pass
    return ["What is our total revenue this year?","Who are our top 10 customers?",
            "Which products sell the most?","How many records are in the main table?"]

# ─────────────────────────────────────────────────────────────────
#  CHART → PNG BASE64 (for email embedding)
# ─────────────────────────────────────────────────────────────────
def render_chart_to_png_b64(df, chart_type, chart_x, chart_y,
                             chart_color=DEFAULT_CHART_COLOR, chart_title="") -> str:
    if not chart_type or chart_type == "none" or df is None or df.empty or not chart_x: return ""
    color = chart_color or DEFAULT_CHART_COLOR
    seq   = [color] + DEFAULT_BLUE_SEQUENCE
    x_col = chart_x if chart_x in df.columns else df.columns[0]
    num_c = df.select_dtypes(include="number").columns.tolist()
    y_col = chart_y if chart_y in df.columns else (num_c[0] if num_c else None)
    try:
        if chart_type.startswith(("seaborn_","matplotlib_")):
            sns.set_theme(style="whitegrid") if chart_type.startswith("seaborn_") else None
            fig, ax = plt.subplots(figsize=(10,5))
            if   chart_type == "seaborn_bar"    and y_col: sns.barplot(data=df,x=x_col,y=y_col,color=color,ax=ax)
            elif chart_type == "seaborn_line"   and y_col: sns.lineplot(data=df,x=x_col,y=y_col,color=color,ax=ax)
            elif chart_type == "seaborn_violin" and y_col: sns.violinplot(data=df,x=x_col,y=y_col,color=color,ax=ax)
            elif chart_type == "seaborn_box"    and y_col: sns.boxplot(data=df,x=x_col,y=y_col,color=color,ax=ax)
            elif chart_type == "matplotlib_bar" and y_col: ax.bar(df[x_col].astype(str),df[y_col],color=color,edgecolor="none")
            elif chart_type == "matplotlib_line"and y_col: ax.plot(df[x_col].astype(str),df[y_col],color=color,marker="o",linewidth=2)
            elif chart_type == "matplotlib_pie" and y_col:
                wc=(seq*((len(df)//len(seq))+1))[:len(df)]
                ax.pie(df[y_col],labels=df[x_col].astype(str),colors=wc,autopct="%1.1f%%",startangle=140)
            elif chart_type == "matplotlib_hist": ax.hist(df[x_col].dropna(),color=color,edgecolor="none",bins=20)
            else: plt.close(fig); return ""
            if chart_title: ax.set_title(chart_title,fontsize=14,fontweight="bold",color=color,pad=10)
            plt.tight_layout()
            buf = io.BytesIO(); fig.savefig(buf,format="png",dpi=150,bbox_inches="tight"); plt.close(fig)
            return base64.b64encode(buf.getvalue()).decode()
        df2 = df.copy()
        if pd.api.types.is_numeric_dtype(df2[x_col]): df2[x_col] = df2[x_col].astype(str)
        kw = {"title": chart_title} if chart_title else {}
        cmap = {
            "bar":       lambda: px.bar(df2,x=x_col,y=y_col,color_discrete_sequence=[color],**kw),
            "line":      lambda: px.line(df2,x=x_col,y=y_col,markers=True,color_discrete_sequence=[color],**kw),
            "area":      lambda: px.area(df2,x=x_col,y=y_col,color_discrete_sequence=[color],**kw),
            "scatter":   lambda: px.scatter(df2,x=x_col,y=y_col,color_discrete_sequence=[color],**kw),
            "pie":       lambda: px.pie(df2,names=x_col,values=y_col,color_discrete_sequence=seq,**kw),
            "donut":     lambda: px.pie(df2,names=x_col,values=y_col,hole=0.45,color_discrete_sequence=seq,**kw),
            "histogram": lambda: px.histogram(df2,x=x_col,color_discrete_sequence=[color],**kw),
            "funnel":    lambda: px.funnel(df2,x=y_col,y=x_col,color_discrete_sequence=[color],**kw),
            "treemap":   lambda: px.treemap(df2,path=[x_col],values=y_col,color_discrete_sequence=seq,**kw),
        }
        fig = cmap.get(chart_type, cmap["bar"])()
        fig.update_layout(paper_bgcolor="white",plot_bgcolor="white",margin=dict(t=60,b=40,l=40,r=40))
        try:
            png = fig.to_image(format="png",width=900,height=450,scale=2)
            return base64.b64encode(png).decode()
        except Exception:
            fig2,ax2 = plt.subplots(figsize=(10,5))
            if y_col and y_col in df2.columns: ax2.bar(df2[x_col].astype(str),df2[y_col],color=color,edgecolor="none")
            if chart_title: ax2.set_title(chart_title,fontsize=14,fontweight="bold",color=color)
            plt.tight_layout(); buf=io.BytesIO(); fig2.savefig(buf,format="png",dpi=150,bbox_inches="tight"); plt.close(fig2)
            return base64.b64encode(buf.getvalue()).decode()
    except Exception as e:
        print(f"[chart_png] {e}"); return ""

# ─────────────────────────────────────────────────────────────────
#  SCHEDULER
# ─────────────────────────────────────────────────────────────────
_sched_lock   = threading.Lock()
_sched_stop   = threading.Event()

def _log_sched(msg: str):
    ts   = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f: f.write(line+"\n")
    except Exception: pass

def get_scheduled_reports() -> list:
    try:
        if SCHEDULE_FILE.exists():
            return json.loads(SCHEDULE_FILE.read_text(encoding="utf-8"))
    except Exception: pass
    return []

def save_scheduled_reports(reports: list):
    with _sched_lock:
        try:
            SCHEDULE_FILE.write_text(json.dumps(reports,indent=2,default=str),encoding="utf-8")
        except Exception as e:
            _log_sched(f"Save failed: {e}")

def add_scheduled_report(name,question,sql,database,interval_minutes,recipients,
                          chart_type="none",chart_x="",chart_y="",
                          chart_color=DEFAULT_CHART_COLOR,chart_title="") -> dict:
    report = {
        "id": uuid.uuid4().hex[:8], "name":name, "question":question, "sql":sql,
        "database":database, "interval_minutes":interval_minutes, "recipients":recipients,
        "active":True, "created_at":datetime.datetime.now().isoformat(),
        "last_run":None, "last_status":None, "run_count":0,
        "chart_type":chart_type, "chart_x":chart_x, "chart_y":chart_y,
        "chart_color":chart_color, "chart_title":chart_title,
    }
    reports = get_scheduled_reports()
    reports.append(report)
    save_scheduled_reports(reports)
    return report

def df_to_html_table(df: pd.DataFrame, max_rows: int = 50) -> str:
    prev = df.head(max_rows)
    hdr  = "".join(f'<th style="background:#1565C0;color:#fff;padding:8px 12px;text-align:left;">{c}</th>' for c in prev.columns)
    body = ""
    for i, (_, row) in enumerate(prev.iterrows()):
        bg = "#f9f9f9" if i%2==0 else "#fff"
        cells = "".join(f'<td style="padding:7px 12px;border-bottom:1px solid #eee;">{v}</td>' for v in row)
        body += f'<tr style="background:{bg};">{cells}</tr>'
    note = f'<p style="color:gray;font-size:0.75rem;margin-top:6px;">Showing {len(prev)} of {len(df)} rows. Full data in CSV attachment.</p>' if len(df) > max_rows else ""
    return f'<table style="border-collapse:collapse;width:100%;font-size:0.85rem;"><thead><tr>{hdr}</tr></thead><tbody>{body}</tbody></table>{note}'

# FIX ❷ + ❹: Better error reporting in send_scheduled_email
def send_scheduled_email(report: dict, df: pd.DataFrame):
    if not SMTP_USER or not SMTP_PASSWORD:
        return False, "SMTP credentials not configured. Check SMTP_USER and SMTP_PASSWORD in secrets.toml."
    name  = report.get("name","Report"); database = report.get("database","")
    question = report.get("question",""); sql = report.get("sql","")
    now_str  = datetime.datetime.now().strftime("%B %d, %Y at %I:%M %p")
    table_html = df_to_html_table(df) if not df.empty else "<p><em>No data returned.</em></p>"

    chart_color = report.get("chart_color", DEFAULT_CHART_COLOR)
    chart_png_b64 = render_chart_to_png_b64(
        df, report.get("chart_type","none"),
        report.get("chart_x",""), report.get("chart_y",""),
        chart_color, report.get("chart_title",""))
    chart_html = (
        '<div style="margin:20px 0;"><img src="cid:report_chart" alt="Chart" '
        'style="max-width:100%;border-radius:8px;box-shadow:0 2px 8px rgba(0,0,0,0.12);"/></div>'
    ) if chart_png_b64 else ""

    html_body = f"""<html><body style="font-family:Arial,sans-serif;color:#333;max-width:920px;margin:0 auto;padding:20px;">
  <div style="background:#1565C0;padding:16px 24px;border-radius:10px 10px 0 0;">
    <h2 style="color:white;margin:0;font-size:1.3rem;">&#128202; Techwish AI &#8212; Scheduled Report</h2>
    <p style="color:rgba(255,255,255,0.8);margin:4px 0 0;font-size:0.85rem;">{now_str}</p>
  </div>
  <div style="background:#f9f9f9;padding:16px 24px;border:1px solid #e0e0e0;">
    <p><strong>Report:</strong> {name}</p>
    <p><strong>Database:</strong> {database}</p>
    <p><strong>Question:</strong> {question}</p>
    <p><strong>Rows returned:</strong> {len(df)}</p>
  </div>
  {chart_html}
  <div style="padding:16px 0;">{table_html}</div>
  <div style="background:#f5f5f5;padding:12px 16px;border-radius:0 0 10px 10px;margin-top:16px;">
    <p style="color:gray;font-size:0.78rem;margin:0;">SQL: <code style="background:#e8e8e8;padding:2px 6px;border-radius:4px;">{sql}</code></p>
    <p style="color:gray;font-size:0.75rem;margin:4px 0 0;">Powered by Techwish AI Analytics</p>
  </div>
</body></html>"""

    text_body = f"Techwish AI Report: {name}\n{now_str}\nQuestion: {question}\nRows: {len(df)}\n\n{df.to_string(index=False) if not df.empty else 'No data.'}"

    try:
        msg = MIMEMultipart("mixed")
        msg["Subject"] = f"Techwish AI Report: {name}"
        msg["From"]    = SMTP_USER
        msg["To"]      = ", ".join(report.get("recipients",[]))
        related = MIMEMultipart("related")
        alt     = MIMEMultipart("alternative")
        alt.attach(MIMEText(text_body,"plain"))
        alt.attach(MIMEText(html_body,"html"))
        related.attach(alt)
        if chart_png_b64:
            img = MIMEBase("image","png"); img.set_payload(base64.b64decode(chart_png_b64))
            encoders.encode_base64(img)
            img.add_header("Content-ID","<report_chart>")
            img.add_header("Content-Disposition","inline",filename="chart.png")
            related.attach(img)
        msg.attach(related)
        if not df.empty:
            csv_part = MIMEBase("application","octet-stream")
            csv_part.set_payload(df.to_csv(index=False).encode("utf-8"))
            encoders.encode_base64(csv_part)
            safe = _re.sub(r"[^a-zA-Z0-9_]","_",name)+".csv"
            csv_part.add_header("Content-Disposition",f'attachment; filename="{safe}"')
            msg.attach(csv_part)

        # FIX ❹: More robust SMTP connection with explicit timeout
        server = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30)
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.sendmail(SMTP_USER, report.get("recipients",[]), msg.as_string())
        server.quit()
        return True, ""
    except smtplib.SMTPAuthenticationError as e:
        return False, f"SMTP Auth failed — if using Gmail, you need an App Password (not your login password). Error: {e}"
    except smtplib.SMTPException as e:
        return False, f"SMTP error: {e}"
    except Exception as e:
        return False, f"Unexpected email error: {e}"

# FIX ❶ + ❷: _run_due_reports now checks for empty SQL, updates status on crash, logs full traceback
def _run_due_reports():
    reports = get_scheduled_reports()
    changed = False
    for r in reports:
        if not r.get("active"): continue
        now  = datetime.datetime.now()
        last = r.get("last_run")
        mins = r.get("interval_minutes",60)
        if last:
            try:
                if now < datetime.datetime.fromisoformat(last) + datetime.timedelta(minutes=mins): continue
            except Exception: pass

        _log_sched(f"Running '{r.get('name')}' → {r.get('recipients')}")

        # FIX ❶: Guard against empty SQL — catch it early with a clear message
        sql = r.get("sql","").strip()
        if not sql:
            _log_sched(f"  SKIP: SQL is empty for report '{r.get('name')}' (id={r.get('id')}). Regenerating from question...")
            q = r.get("question","").strip()
            db = r.get("database","")
            if q and db:
                try:
                    gen = nl_to_sql(q, [], db)
                    sql = gen.get("sql","").strip()
                    if sql:
                        r["sql"] = sql
                        _log_sched(f"  Regenerated SQL: {sql[:80]}...")
                    else:
                        r["last_run"]    = now.isoformat()
                        r["last_status"] = "Failed: Could not regenerate SQL from question"
                        changed = True
                        _log_sched(f"  FAIL: Could not generate SQL for '{r.get('name')}'")
                        continue
                except Exception as e:
                    r["last_run"]    = now.isoformat()
                    r["last_status"] = f"Failed: SQL regeneration error — {e}"
                    changed = True
                    continue
            else:
                r["last_run"]    = now.isoformat()
                r["last_status"] = "Failed: SQL is empty and no question to regenerate from"
                changed = True
                continue

        _log_sched(f"  SQL: {sql[:100]}...")

        try:
            df      = run_query_direct(sql, r.get("database",""))
            ok, err = send_scheduled_email(r, df)
        except Exception as e:
            ok  = False
            err = traceback.format_exc()   # FIX ❷: full traceback, not just str(e)

        r["last_run"]    = now.isoformat()
        r["last_status"] = "Sent" if ok else f"Failed: {str(err)[:300]}"
        r["run_count"]   = r.get("run_count",0)+1
        changed = True
        _log_sched(f"  {'OK' if ok else 'FAIL'}: {r.get('name')}" + ("" if ok else f"\n    {err[:400]}"))

    if changed: save_scheduled_reports(reports)

def _scheduler_loop():
    _log_sched("Scheduler thread started.")
    while not _sched_stop.is_set():
        try: _run_due_reports()
        except Exception as e: _log_sched(f"Scheduler loop error: {traceback.format_exc()}")
        _sched_stop.wait(60)
    _log_sched("Scheduler thread stopped.")

# ─────────────────────────────────────────────────────────────────
#  FLASK ROUTES
# ─────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory(str(BASE_DIR), "index.html")

@app.route("/<path:filename>")
def serve_static(filename):
    return send_from_directory(str(BASE_DIR), filename)

@app.route("/api/databases")
def api_databases():
    return jsonify({"databases": list_databases()})

@app.route("/api/schema")
def api_schema():
    db = request.args.get("database","")
    if not db: return jsonify({"error":"no database"}),400
    return jsonify({"schema": load_schema(db)})

@app.route("/api/questions")
def api_questions():
    db = request.args.get("database","")
    if not db: return jsonify({"questions":[]}),400
    return jsonify({"questions": get_sample_questions(db)})

@app.route("/api/query", methods=["POST"])
def api_query():
    data     = request.json or {}
    question = data.get("question","").strip()
    database = data.get("database","")
    history  = data.get("history",[])
    if not question or not database:
        return jsonify({"error":"question and database required"}),400

    result = nl_to_sql(question,history,database)
    sql    = result.get("sql","").strip().rstrip(";").strip()
    reuse  = result.get("_reuse_df")

    df = None; error = None

    if reuse and not sql:
        df = pd.DataFrame(reuse)
    elif sql:
        try:
            fixed = fix_date_filter_in_sql(sql,database)
            if fixed != sql: sql = fixed; result["sql"] = fixed
            df = run_query(sql,database)
        except Exception as e:
            error = str(e)

    if df is not None and not df.empty:
        result["rows"]      = format_dataframe(df)
        result["columns"]   = list(df.columns)
        result["row_count"] = len(df)
        result["_raw_df"]   = df.to_dict("records")
    else:
        result["rows"]      = []
        result["columns"]   = []
        result["row_count"] = 0
        result["_raw_df"]   = []

    if not result.get("chart_color"):
        result["chart_color"] = DEFAULT_CHART_COLOR

    if error: result["error"] = error
    result.pop("_reuse_df", None)
    return jsonify(result)

# FIX ❶: api_schedules_post now always resolves SQL before saving
@app.route("/api/schedules", methods=["GET"])
def api_schedules_get():
    return jsonify({"schedules": get_scheduled_reports()})

@app.route("/api/schedules", methods=["POST"])
def api_schedules_post():
    data = request.json or {}

    required = ["name","database","interval_minutes","recipients"]
    for f in required:
        if not data.get(f): return jsonify({"error":f"Missing: {f}"}),400

    question = (data.get("question") or "").strip()
    sql      = (data.get("sql") or "").strip()

    if not sql and not question:
        return jsonify({"error":"Missing: sql or question"}),400

    # FIX ❶: Always resolve SQL from question at creation time so scheduler never runs empty SQL
    if not sql and question:
        _log_sched(f"[POST /api/schedules] Generating SQL for question: {question}")
        try:
            r = nl_to_sql(question, [], data["database"])
            sql = r.get("sql","").strip()
            _log_sched(f"[POST /api/schedules] Generated SQL: {sql[:100]}")
        except Exception as e:
            return jsonify({"error":f"SQL generation failed: {e}"}),500

        if not sql:
            return jsonify({"error":"Could not generate SQL from question — please try rephrasing or use SQL mode."}),400

    # Strip trailing semicolons
    sql = sql.rstrip(";").strip()

    # Validate SQL actually runs before saving the schedule
    try:
        test_df = run_query_direct(sql + " LIMIT 1", data["database"])
        _log_sched(f"[POST /api/schedules] SQL validated OK — {len(test_df)} rows (test run)")
    except Exception as e:
        return jsonify({"error":f"SQL validation failed: {e}"}),400

    report = add_scheduled_report(
        name=data["name"],
        question=question or data.get("name",""),
        sql=sql,
        database=data["database"],
        interval_minutes=int(data["interval_minutes"]),
        recipients=[e.strip() for e in data["recipients"].split(",") if e.strip()],
        chart_type=data.get("chart_type","none"),
        chart_x=data.get("chart_x",""),
        chart_y=data.get("chart_y",""),
        chart_color=data.get("chart_color",DEFAULT_CHART_COLOR),
        chart_title=data.get("chart_title",""),
    )
    _log_sched(f"[POST /api/schedules] Created schedule '{report['name']}' id={report['id']}")
    return jsonify({"success":True,"report":report})

@app.route("/api/schedules/<report_id>", methods=["DELETE"])
def api_schedule_delete(report_id):
    reports = get_scheduled_reports()
    reports = [r for r in reports if r["id"] != report_id]
    save_scheduled_reports(reports)
    return jsonify({"success":True})

@app.route("/api/schedules/<report_id>/toggle", methods=["POST"])
def api_schedule_toggle(report_id):
    reports = get_scheduled_reports()
    for r in reports:
        if r["id"] == report_id: r["active"] = not r.get("active",True)
    save_scheduled_reports(reports)
    return jsonify({"success":True})

@app.route("/api/schedules/<report_id>/run", methods=["POST"])
def api_schedule_run(report_id):
    reports = get_scheduled_reports()
    report  = next((r for r in reports if r["id"] == report_id), None)
    if not report: return jsonify({"error":"Not found"}),404

    sql = report.get("sql","").strip()

    # FIX ❶: If SQL is empty, try to regenerate from question before running
    if not sql:
        q  = report.get("question","").strip()
        db = report.get("database","")
        if q and db:
            try:
                gen = nl_to_sql(q, [], db)
                sql = gen.get("sql","").strip()
                if sql:
                    for r in reports:
                        if r["id"] == report_id:
                            r["sql"] = sql
                    save_scheduled_reports(reports)
                    report["sql"] = sql
            except Exception as e:
                return jsonify({"error":f"SQL regeneration failed: {e}"}),500
        if not sql:
            return jsonify({"error":"SQL is empty and could not be regenerated. Please delete and re-create this schedule."}),400

    try:
        df = run_query_direct(sql, report["database"])
        ok, err = send_scheduled_email(report, df)
        now = datetime.datetime.now().isoformat()
        for r in reports:
            if r["id"] == report_id:
                r["last_run"]    = now
                r["last_status"] = "Sent" if ok else f"Failed: {err}"
                r["run_count"]   = r.get("run_count",0)+1
        save_scheduled_reports(reports)
        if ok:
            return jsonify({"success":True})
        return jsonify({"error": err}), 500
    except Exception as e:
        return jsonify({"error": traceback.format_exc()}), 500

# FIX: SMTP test endpoint — visit /api/test-email to verify credentials
@app.route("/api/test-email")
def api_test_email():
    """
    Diagnostic endpoint — visit http://localhost:5000/api/test-email
    Sends a test email to SMTP_USER to confirm credentials work.
    """
    if not SMTP_USER or not SMTP_PASSWORD:
        return jsonify({"success":False,"error":"SMTP_USER or SMTP_PASSWORD not configured in secrets.toml"}), 400
    try:
        server = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15)
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(SMTP_USER, SMTP_PASSWORD)
        test_msg = f"Subject: Techwish SMTP Test\nFrom: {SMTP_USER}\nTo: {SMTP_USER}\n\nSMTP is working correctly!"
        server.sendmail(SMTP_USER, [SMTP_USER], test_msg)
        server.quit()
        return jsonify({"success":True,"message":f"Test email sent to {SMTP_USER}. Check your inbox."})
    except smtplib.SMTPAuthenticationError as e:
        return jsonify({"success":False,"error":f"Auth failed — Gmail requires an App Password, not your login password. Details: {e}"}), 400
    except Exception as e:
        return jsonify({"success":False,"error":str(e)}), 500

@app.route("/api/health")
def api_health():
    return jsonify({"status":"ok","scheduler":"running","smtp_configured": bool(SMTP_USER and SMTP_PASSWORD)})

# ─────────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    sched_thread = threading.Thread(target=_scheduler_loop, daemon=True, name="scheduler")
    sched_thread.start()

    def _open_browser():
        time.sleep(1.5)
        webbrowser.open("http://localhost:5000")
    threading.Thread(target=_open_browser, daemon=True).start()

    print("\n" + "="*55)
    print("  Techwish AI Analytics (FIXED)")
    print("  http://localhost:5000")
    print("  Scheduler: running (background thread)")
    print(f"  SMTP: {SMTP_USER or '(NOT CONFIGURED)'}")
    print("  Test email: http://localhost:5000/api/test-email")
    print("="*55 + "\n")

    def _shutdown(sig, frame):
        print("\nShutting down...")
        _sched_stop.set()
        sys.exit(0)
    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=False)
