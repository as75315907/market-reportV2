
import os
import subprocess
import sys
from flask import Flask, Response

from market_report.mail import send_mail
from market_report.status_summary import email_subject_for_result, parse_run_output
from market_report.time_utils import timestamp_taipei

app = Flask(__name__)

@app.get("/")
def health():
    return "ok", 200

@app.post("/run")
def run_job():
    env = os.environ.copy()

    # （可選但建議）快速確認 Scheduler/Cloud Run 實際有讀到哪些 env
    # 只印前幾碼避免把敏感資訊完整打到 log
    def _mask(v: str, n: int = 6) -> str:
        v = (v or "").strip()
        return (v[:n] + "..." if len(v) > n else v) if v else "(empty)"

    print(f"[ENV] GSHEET_ID={_mask(env.get('GSHEET_ID'))} | GSHEET_TAB={_mask(env.get('GSHEET_TAB'))} | GSHEET_SHEET_NAME={_mask(env.get('GSHEET_SHEET_NAME'))}")

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
        body = f"Time(Taipei): {timestamp_taipei()}\n\n" + out[-4000:]
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
        body = f"Time(Taipei): {timestamp_taipei()}\n\n" + out[-4000:]
        send_mail(subject, body)
        return Response(out, status=200, mimetype="text/plain")

    # failure
    subject = email_subject_for_result("fail")
    body = f"Time(Taipei): {timestamp_taipei()}\n\n" + out[-4000:]
    send_mail(subject, body)
    return Response(out, status=500, mimetype="text/plain")
