
import os
import sys
import subprocess
import smtplib
from email.mime.text import MIMEText
from datetime import datetime
from flask import Flask, Response

app = Flask(__name__)

def send_mail(subject: str, body: str):
    host = os.getenv("SMTP_HOST", "").strip()
    port = int(os.getenv("SMTP_PORT", "465").strip() or 465)
    user = os.getenv("SMTP_USER", "").strip()
    pwd  = os.getenv("SMTP_APP_PASSWORD", "").strip()
    to   = os.getenv("MAIL_TO", "").strip()

    if not (host and user and pwd and to):
        print("[MAIL] Missing SMTP_HOST/SMTP_PORT/SMTP_USER/SMTP_APP_PASSWORD/MAIL_TO -> skip")
        return

    to_list = [x.strip() for x in to.split(",") if x.strip()]

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = user
    msg["To"] = ", ".join(to_list)

    # Gmail 465: SMTP over SSL
    with smtplib.SMTP_SSL(host, port, timeout=25) as s:
        s.login(user, pwd)
        s.sendmail(user, to_list, msg.as_string())

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

        now_tw = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        subject = "⏱️Daily Market Report 執行逾時（timeout）"
        body = f"Time(Taipei): {now_tw}\n\n" + out[-4000:]
        send_mail(subject, body)

        return Response(out, status=504, mimetype="text/plain")

    print(out)

    now_tw = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    skipped = ("[DEDUP] Already updated today" in out) or ("skipped.txt written" in out)

    notify_on_skip = os.getenv("MAIL_NOTIFY_ON_SKIP", "0").strip() == "1"

    if p.returncode == 0:
        if skipped and not notify_on_skip:
            return Response(out, status=200, mimetype="text/plain")

        subject = "🟨Daily Market Report（略過：今日已更新）" if skipped else "✅Daily Market Report 更新成功"
        body = f"Time(Taipei): {now_tw}\n\n" + out[-4000:]
        send_mail(subject, body)
        return Response(out, status=200, mimetype="text/plain")

    # failure
    subject = "❌Daily Market Report 更新失敗"
    body = f"Time(Taipei): {now_tw}\n\n" + out[-4000:]
    send_mail(subject, body)
    return Response(out, status=500, mimetype="text/plain")
