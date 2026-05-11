
import os
import subprocess
import sys
from flask import Flask, Response, request

from market_report.mail import send_mail
from market_report.status_summary import build_notification_text, email_subject_for_result, parse_run_output
from market_report.time_utils import timestamp_taipei

app = Flask(__name__)

VALID_REPORT_TASKS = {"all", "quotes", "revenue"}
REPORT_TASK_ALIASES = {
    "quote": "quotes",
    "price": "quotes",
    "prices": "quotes",
    "stock": "quotes",
    "stocks": "quotes",
    "daily": "quotes",
    "monthly": "revenue",
    "rev": "revenue",
}

@app.get("/")
def health():
    return "ok", 200

@app.post("/run")
def run_job():
    env = os.environ.copy()

    body = request.get_json(silent=True) or {}
    requested_task = (
        request.args.get("task")
        or request.args.get("target")
        or (body.get("task") if isinstance(body, dict) else None)
        or (body.get("target") if isinstance(body, dict) else None)
    )
    if requested_task:
        task = REPORT_TASK_ALIASES.get(requested_task.strip().lower(), requested_task.strip().lower())
        if task not in VALID_REPORT_TASKS:
            return Response(
                f"Invalid task={requested_task!r}; expected one of: all, quotes, revenue\n",
                status=400,
                mimetype="text/plain",
            )
        env["MARKET_REPORT_TASK"] = task

    # （可選但建議）快速確認 Scheduler/Cloud Run 實際有讀到哪些 env
    # 只印前幾碼避免把敏感資訊完整打到 log
    def _mask(v: str, n: int = 6) -> str:
        v = (v or "").strip()
        return (v[:n] + "..." if len(v) > n else v) if v else "(empty)"

    print(f"[ENV] GSHEET_ID={_mask(env.get('GSHEET_ID'))} | GSHEET_TAB={_mask(env.get('GSHEET_TAB'))} | GSHEET_SHEET_NAME={_mask(env.get('GSHEET_SHEET_NAME'))} | MARKET_REPORT_TASK={env.get('MARKET_REPORT_TASK', 'all')}")

    try:
        p = subprocess.run(
            [sys.executable, "daily_market_report_to_gsheet.py"],
            capture_output=True,
            text=True,
            env=env,
            timeout=850,  # < 900s：留緩衝，避免 Scheduler/Run 的 deadline 先到
        )
        out = (p.stdout or "") + "\n" + (p.stderr or "")
    except subprocess.TimeoutExpired as e:
        out = f"[TIMEOUT] daily_market_report_to_gsheet.py timeout: {e}\n"
        print(out)

        subject = "⏱️Daily Market Report 執行逾時（timeout）"
        body = build_notification_text(out, time_taipei=timestamp_taipei(), include_log_tail=True)
        send_mail(subject, body)

        return Response(out, status=504, mimetype="text/plain")

    print(out)

    summary = parse_run_output(out)
    skipped = summary.skipped or ("skipped.txt written" in out)

    notify_on_skip = os.getenv("MAIL_NOTIFY_ON_SKIP", "0").strip() == "1"

    if p.returncode == 0:
        if skipped and not notify_on_skip:
            return Response(out, status=200, mimetype="text/plain")

        result = "skip" if skipped else "success"
        subject = email_subject_for_result(result)
        body = build_notification_text(out, time_taipei=timestamp_taipei())
        send_mail(subject, body)
        return Response(out, status=200, mimetype="text/plain")

    # failure
    subject = email_subject_for_result("fail")
    body = build_notification_text(out, time_taipei=timestamp_taipei(), include_log_tail=True)
    send_mail(subject, body)
    return Response(out, status=500, mimetype="text/plain")
