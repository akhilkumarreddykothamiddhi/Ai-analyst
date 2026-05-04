"""
Techwish AI — Standalone Background Scheduler
==============================================
Run this ONCE alongside your Streamlit app:
    python scheduler.py

It reads scheduled_reports.json every minute, sends any due emails,
and keeps running even when the browser / Streamlit app is closed.

To run in background on Linux/Mac:
    nohup python scheduler.py > scheduler.log 2>&1 &

To stop it:
    kill $(cat scheduler.pid)
"""

import json
import os
import pathlib
import smtplib
import time
import datetime
import io
import re
import signal
import sys
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pandas as pd
import snowflake.connector

# ── Try importing chart libs (optional — email still sends without them) ──
try:
    import matplotlib
    matplotlib.use("Agg")          # non-interactive backend for server use
    import matplotlib.pyplot as plt
    import seaborn as sns
    HAS_MPL = True
except ImportError:
    HAS_MPL = False

try:
    import plotly.express as px
    HAS_PLOTLY = True
except ImportError:
    HAS_PLOTLY = False

# ─────────────────────────────────────────────────────────────────
#  CONFIG — reads from Streamlit secrets.toml or environment vars
# ─────────────────────────────────────────────────────────────────
def _load_secrets() -> dict:
    """Load from .streamlit/secrets.toml if present, else use env vars."""
    secrets_path = pathlib.Path(__file__).parent / ".streamlit" / "secrets.toml"
    if secrets_path.exists():
        try:
            import tomllib                    # Python 3.11+
        except ImportError:
            try:
                import tomli as tomllib       # pip install tomli
            except ImportError:
                tomllib = None

        if tomllib:
            with open(secrets_path, "rb") as f:
                return tomllib.load(f)

    # Fallback — environment variables
    return {
        "SNOWFLAKE_ACCOUNT":   os.getenv("SNOWFLAKE_ACCOUNT", ""),
        "SNOWFLAKE_USER":      os.getenv("SNOWFLAKE_USER", ""),
        "SNOWFLAKE_PASSWORD":  os.getenv("SNOWFLAKE_PASSWORD", ""),
        "SNOWFLAKE_WAREHOUSE": os.getenv("SNOWFLAKE_WAREHOUSE", ""),
        "SNOWFLAKE_ROLE":      os.getenv("SNOWFLAKE_ROLE", ""),
        "SMTP_HOST":           os.getenv("SMTP_HOST", "smtp.gmail.com"),
        "SMTP_PORT":           int(os.getenv("SMTP_PORT", "587")),
        "SMTP_USER":           os.getenv("SMTP_USER", ""),
        "SMTP_PASSWORD":       os.getenv("SMTP_PASSWORD", ""),
    }

SECRETS       = _load_secrets()
SF_ACCOUNT    = SECRETS.get("SNOWFLAKE_ACCOUNT", "")
SF_USER       = SECRETS.get("SNOWFLAKE_USER", "")
SF_PASSWORD   = SECRETS.get("SNOWFLAKE_PASSWORD", "")
SF_WAREHOUSE  = SECRETS.get("SNOWFLAKE_WAREHOUSE", "")
SF_ROLE       = SECRETS.get("SNOWFLAKE_ROLE", "")
SMTP_HOST     = SECRETS.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT     = int(SECRETS.get("SMTP_PORT", 587))
SMTP_USER     = SECRETS.get("SMTP_USER", "")
SMTP_PASSWORD = SECRETS.get("SMTP_PASSWORD", "")

SCHEDULE_FILE = pathlib.Path(__file__).parent / "scheduled_reports.json"
PID_FILE      = pathlib.Path(__file__).parent / "scheduler.pid"
CHECK_INTERVAL_SECONDS = 60   # how often to check for due reports

DEFAULT_CHART_COLOR   = "#1565C0"
DEFAULT_BLUE_SEQUENCE = [
    "#1565C0","#1976D2","#1E88E5","#42A5F5",
    "#64B5F6","#0D47A1","#0288D1","#29B6F6",
]

# ─────────────────────────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────────────────────────
def log(msg: str):
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)

# ─────────────────────────────────────────────────────────────────
#  SNOWFLAKE
# ─────────────────────────────────────────────────────────────────
def run_query_direct(sql: str, database: str) -> pd.DataFrame:
    sql = sql.strip().rstrip(";").strip()
    conn = snowflake.connector.connect(
        account=SF_ACCOUNT,
        user=SF_USER,
        password=SF_PASSWORD,
        warehouse=SF_WAREHOUSE,
        role=SF_ROLE,
        database=database,
    )
    try:
        cur = conn.cursor()
        cur.execute(sql)
        cols = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        return pd.DataFrame(rows, columns=cols)
    finally:
        conn.close()

# ─────────────────────────────────────────────────────────────────
#  CHART → PNG
# ─────────────────────────────────────────────────────────────────
def render_chart_to_png(
    df: pd.DataFrame,
    chart_type: str,
    chart_x: str,
    chart_y: str,
    chart_color: str = DEFAULT_CHART_COLOR,
    chart_title: str = "",
) -> bytes | None:
    if not chart_type or chart_type == "none" or df.empty or not chart_x:
        return None

    color = chart_color or DEFAULT_CHART_COLOR
    seq   = [color] + DEFAULT_BLUE_SEQUENCE
    x_col = chart_x if chart_x in df.columns else df.columns[0]
    num_cols = df.select_dtypes(include="number").columns
    y_col = chart_y if chart_y in df.columns else (num_cols[0] if len(num_cols) > 0 else None)

    try:
        # ── Seaborn / Matplotlib charts ──────────────────────────
        if HAS_MPL and chart_type.startswith(("seaborn_", "matplotlib_")):
            if chart_type.startswith("seaborn_"):
                sns.set_theme(style="whitegrid")
            fig, ax = plt.subplots(figsize=(10, 5))

            if chart_type == "seaborn_bar" and y_col:
                sns.barplot(data=df, x=x_col, y=y_col, color=color, ax=ax)
            elif chart_type == "seaborn_line" and y_col:
                sns.lineplot(data=df, x=x_col, y=y_col, color=color, ax=ax)
            elif chart_type == "seaborn_heatmap":
                num_df = df.select_dtypes(include="number")
                if not num_df.empty:
                    sns.heatmap(num_df.corr(), annot=True, fmt=".2f", cmap="Blues", ax=ax)
            elif chart_type == "seaborn_violin" and y_col:
                sns.violinplot(data=df, x=x_col, y=y_col, color=color, ax=ax)
            elif chart_type == "seaborn_box" and y_col:
                sns.boxplot(data=df, x=x_col, y=y_col, color=color, ax=ax)
            elif chart_type == "matplotlib_bar" and y_col:
                ax.bar(df[x_col].astype(str), df[y_col], color=color, width=0.6, edgecolor="none")
            elif chart_type == "matplotlib_line" and y_col:
                ax.plot(df[x_col].astype(str), df[y_col], color=color, marker="o", linewidth=2)
            elif chart_type == "matplotlib_pie" and y_col:
                w_colors = (seq * ((len(df) // len(seq)) + 1))[:len(df)]
                ax.pie(df[y_col], labels=df[x_col].astype(str), colors=w_colors,
                       autopct="%1.1f%%", startangle=140)
            elif chart_type == "matplotlib_hist":
                ax.hist(df[x_col].dropna(), color=color, edgecolor="none", bins=20)
            else:
                plt.close(fig)
                return None

            if chart_title:
                ax.set_title(chart_title, fontsize=14, fontweight="bold", color=color, pad=10)
            plt.tight_layout()
            buf = io.BytesIO()
            fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
            plt.close(fig)
            return buf.getvalue()

        # ── Plotly charts ─────────────────────────────────────────
        if HAS_PLOTLY:
            df2 = df.copy()
            if pd.api.types.is_numeric_dtype(df2[x_col]):
                df2[x_col] = df2[x_col].astype(str)
            kw = {"title": chart_title} if chart_title else {}

            if chart_type == "bar":
                fig = px.bar(df2, x=x_col, y=y_col, color_discrete_sequence=[color], **kw)
            elif chart_type == "line":
                fig = px.line(df2, x=x_col, y=y_col, markers=True, color_discrete_sequence=[color], **kw)
            elif chart_type == "area":
                fig = px.area(df2, x=x_col, y=y_col, color_discrete_sequence=[color], **kw)
            elif chart_type == "scatter":
                fig = px.scatter(df2, x=x_col, y=y_col, color_discrete_sequence=[color], **kw)
            elif chart_type == "pie":
                fig = px.pie(df2, names=x_col, values=y_col, color_discrete_sequence=seq, **kw)
            elif chart_type == "donut":
                fig = px.pie(df2, names=x_col, values=y_col, hole=0.45, color_discrete_sequence=seq, **kw)
            elif chart_type == "histogram":
                fig = px.histogram(df2, x=x_col, color_discrete_sequence=[color], **kw)
            elif chart_type == "funnel":
                fig = px.funnel(df2, x=y_col, y=x_col, color_discrete_sequence=[color], **kw)
            elif chart_type == "treemap":
                fig = px.treemap(df2, path=[x_col], values=y_col, color_discrete_sequence=seq, **kw)
            else:
                fig = px.bar(df2, x=x_col, y=y_col, color_discrete_sequence=[color], **kw)

            fig.update_layout(
                paper_bgcolor="white", plot_bgcolor="white",
                font=dict(family="Arial"),
                margin=dict(t=60, b=40, l=40, r=40),
            )
            return fig.to_image(format="png", width=900, height=450, scale=2)

        # ── Fallback — basic matplotlib bar ──────────────────────
        if HAS_MPL and y_col:
            fig, ax = plt.subplots(figsize=(10, 5))
            ax.bar(df[x_col].astype(str), df[y_col], color=color, edgecolor="none")
            if chart_title:
                ax.set_title(chart_title, fontsize=14, fontweight="bold", color=color)
            plt.tight_layout()
            buf = io.BytesIO()
            fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
            plt.close(fig)
            return buf.getvalue()

    except Exception as e:
        log(f"[chart_png] Failed to render chart: {e}")

    return None

# ─────────────────────────────────────────────────────────────────
#  HTML TABLE
# ─────────────────────────────────────────────────────────────────
def df_to_html_table(df: pd.DataFrame, max_rows: int = 50) -> str:
    preview = df.head(max_rows)
    header  = "".join(f'<th style="background:#1565C0;color:white;padding:8px 12px;text-align:left;">{c}</th>' for c in preview.columns)
    rows_html = ""
    for i, (_, row) in enumerate(preview.iterrows()):
        bg = "#f9f9f9" if i % 2 == 0 else "#ffffff"
        cells = "".join(f'<td style="padding:7px 12px;border-bottom:1px solid #eee;">{v}</td>' for v in row)
        rows_html += f'<tr style="background:{bg};">{cells}</tr>'
    note = f'<p style="color:gray;font-size:0.75rem;margin-top:6px;">Showing {len(preview)} of {len(df)} rows. Full data in CSV attachment.</p>' if len(df) > max_rows else ""
    return f'<table style="border-collapse:collapse;width:100%;font-size:0.85rem;"><thead><tr>{header}</tr></thead><tbody>{rows_html}</tbody></table>{note}'

# ─────────────────────────────────────────────────────────────────
#  SEND EMAIL
# ─────────────────────────────────────────────────────────────────
def send_email(report: dict, df: pd.DataFrame) -> tuple[bool, str]:
    """Send report email with inline chart PNG + CSV. Returns (ok, error)."""
    if not SMTP_USER or not SMTP_PASSWORD:
        return False, "SMTP credentials not configured."

    try:
        now_str    = datetime.datetime.now().strftime("%B %d, %Y at %I:%M %p")
        row_count  = len(df)
        table_html = df_to_html_table(df) if not df.empty else "<p><em>No data returned.</em></p>"
        name       = report["name"]
        database   = report["database"]
        question   = report["question"]
        sql        = report["sql"]

        # ── Chart PNG ───────────────────────────────────────────
        chart_png = None
        chart_type = report.get("chart_type", "none")
        if chart_type and chart_type != "none" and not df.empty:
            chart_png = render_chart_to_png(
                df,
                chart_type,
                report.get("chart_x", ""),
                report.get("chart_y", ""),
                report.get("chart_color", DEFAULT_CHART_COLOR),
                report.get("chart_title", ""),
            )

        chart_html = ""
        if chart_png:
            chart_html = '''
            <div style="margin:20px 0;">
              <img src="cid:report_chart" alt="Chart"
                   style="max-width:100%;border-radius:8px;box-shadow:0 2px 8px rgba(0,0,0,0.12);" />
            </div>'''

        html_body = f"""
        <html><body style="font-family:Arial,sans-serif;color:#333;max-width:920px;margin:0 auto;padding:20px;">
          <div style="background:#1565C0;padding:16px 24px;border-radius:10px 10px 0 0;">
            <h2 style="color:white;margin:0;font-size:1.3rem;">&#128202; Techwish AI &#8212; Scheduled Report</h2>
            <p style="color:rgba(255,255,255,0.8);margin:4px 0 0 0;font-size:0.85rem;">{now_str}</p>
          </div>
          <div style="background:#f9f9f9;padding:16px 24px;border:1px solid #e0e0e0;">
            <p><strong>Report:</strong> {name}</p>
            <p><strong>Database:</strong> {database}</p>
            <p><strong>Question:</strong> {question}</p>
            <p><strong>Rows returned:</strong> {row_count}</p>
          </div>
          {chart_html}
          <div style="padding:16px 0;">{table_html}</div>
          <div style="background:#f5f5f5;padding:12px 16px;border-radius:0 0 10px 10px;margin-top:16px;">
            <p style="color:gray;font-size:0.78rem;margin:0;">
              SQL: <code style="background:#e8e8e8;padding:2px 6px;border-radius:4px;">{sql}</code>
            </p>
            <p style="color:gray;font-size:0.75rem;margin:4px 0 0 0;">Powered by Techwish AI Analytics</p>
          </div>
        </body></html>"""

        text_body = (
            f"Techwish AI Scheduled Report\n{now_str}\n\n"
            f"Report: {name}\nDatabase: {database}\nQuestion: {question}\nRows: {row_count}\n\n"
            f"{df.to_string(index=False) if not df.empty else 'No data.'}"
        )

        # Build message
        msg = MIMEMultipart("mixed")
        msg["Subject"] = f"📊 {name} — Techwish AI Report"
        msg["From"]    = SMTP_USER
        msg["To"]      = ", ".join(report["recipients"])

        related = MIMEMultipart("related")
        alt     = MIMEMultipart("alternative")
        alt.attach(MIMEText(text_body, "plain"))
        alt.attach(MIMEText(html_body, "html"))
        related.attach(alt)

        if chart_png:
            img_part = MIMEBase("image", "png")
            img_part.set_payload(chart_png)
            encoders.encode_base64(img_part)
            img_part.add_header("Content-ID", "<report_chart>")
            img_part.add_header("Content-Disposition", "inline", filename="chart.png")
            related.attach(img_part)

        msg.attach(related)

        # CSV attachment
        if not df.empty:
            csv_bytes = df.to_csv(index=False).encode("utf-8")
            csv_part  = MIMEBase("application", "octet-stream")
            csv_part.set_payload(csv_bytes)
            encoders.encode_base64(csv_part)
            safe_name = re.sub(r"[^a-zA-Z0-9_]", "_", name) + ".csv"
            csv_part.add_header("Content-Disposition", f'attachment; filename="{safe_name}"')
            msg.attach(csv_part)

        server = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30)
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.sendmail(SMTP_USER, report["recipients"], msg.as_string())
        server.quit()
        return True, ""

    except Exception as e:
        return False, str(e)

# ─────────────────────────────────────────────────────────────────
#  SCHEDULE FILE I/O
# ─────────────────────────────────────────────────────────────────
def load_reports() -> list:
    try:
        if SCHEDULE_FILE.exists():
            return json.loads(SCHEDULE_FILE.read_text(encoding="utf-8"))
    except Exception as e:
        log(f"Failed to read schedule file: {e}")
    return []

def save_reports(reports: list):
    try:
        SCHEDULE_FILE.write_text(json.dumps(reports, indent=2, default=str), encoding="utf-8")
    except Exception as e:
        log(f"Failed to write schedule file: {e}")

# ─────────────────────────────────────────────────────────────────
#  MAIN LOOP
# ─────────────────────────────────────────────────────────────────
def check_and_run():
    """Check all active schedules and run any that are due."""
    reports = load_reports()
    changed = False

    for report in reports:
        if not report.get("active"):
            continue

        now      = datetime.datetime.now()
        last_run = report.get("last_run")
        interval = report.get("interval_minutes", 60)

        if last_run:
            try:
                due = datetime.datetime.fromisoformat(last_run) + datetime.timedelta(minutes=interval)
                if now < due:
                    continue
            except Exception:
                pass  # bad timestamp — run it anyway

        log(f"Running report '{report['name']}' for {report['recipients']}")
        try:
            df = run_query_direct(report["sql"], report["database"])
            ok, err = send_email(report, df)
        except Exception as e:
            ok, err = False, str(e)

        report["last_run"]    = now.isoformat()
        report["last_status"] = "✅ Sent" if ok else f"❌ Failed: {err}"
        report["run_count"]   = report.get("run_count", 0) + 1
        changed = True

        if ok:
            log(f"  ✅ Sent '{report['name']}' to {report['recipients']}")
        else:
            log(f"  ❌ Failed '{report['name']}': {err}")

    if changed:
        save_reports(reports)

def handle_signal(sig, frame):
    log("Scheduler stopping (signal received).")
    PID_FILE.unlink(missing_ok=True)
    sys.exit(0)

def main():
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    # Write PID so the app can show scheduler status
    PID_FILE.write_text(str(os.getpid()))

    log("=" * 55)
    log("Techwish AI Scheduler started")
    log(f"Schedule file : {SCHEDULE_FILE}")
    log(f"Check interval: every {CHECK_INTERVAL_SECONDS}s")
    log(f"SMTP          : {SMTP_USER or '(not configured)'}")
    log("=" * 55)

    while True:
        try:
            check_and_run()
        except Exception as e:
            log(f"Unexpected error in check loop: {e}")
        time.sleep(CHECK_INTERVAL_SECONDS)

if __name__ == "__main__":
    main()