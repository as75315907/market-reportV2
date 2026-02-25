import os
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
    p = subprocess.run(
        ["python", "daily_market_report_to_gsheet.py"],
        capture_output=True,
        text=True,
        env=os.environ.copy(),
    )

    out = (p.stdout or "") + "\n" + (p.stderr or "")
    print(out)

    now_tw = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    skipped = ("[DEDUP] Already updated today" in out) or ("skipped.txt written" in out)

    notify_on_skip = os.getenv("MAIL_NOTIFY_ON_SKIP", "0").strip() == "1"

    if p.returncode == 0:
        if skipped and not notify_on_skip:
            return Response(out, status=200, mimetype="text/plain")

        subject = "🟨 Daily Market Report（略過：今日已更新）" if skipped else "✅ Daily Market Report 更新成功"
        body = f"Time(Taipei): {now_tw}\n\n" + out[-4000:]
        send_mail(subject, body)
        return Response(out, status=200, mimetype="text/plain")

    # failure
    subject = "❌ Daily Market Report 更新失敗"
    body = f"Time(Taipei): {now_tw}\n\n" + out[-4000:]
    send_mail(subject, body)
    return Response(out, status=500, mimetype="text/plain")
