import json, os
from datetime import datetime
from flask import Flask, Response
from google.cloud import storage

# 你 app.py 裡應該已有 SMTP send_mail；這裡做一個最小版本（讀環境變數）
import smtplib
from email.mime.text import MIMEText

app = Flask(__name__)

def send_mail(subject: str, body: str):
    host = os.getenv("SMTP_HOST", "").strip()
    port = int(os.getenv("SMTP_PORT", "465").strip() or "465")
    user = os.getenv("SMTP_USER", "").strip()
    pwd  = os.getenv("SMTP_APP_PASSWORD", "").strip()
    to_  = os.getenv("MAIL_TO", "").strip()

    if not all([host, port, user, pwd, to_]):
        raise RuntimeError("Missing SMTP envs for sending mail")

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = user
    msg["To"] = to_

    with smtplib.SMTP_SSL(host, port, timeout=25) as s:
        s.login(user, pwd)
        s.sendmail(user, [to_], msg.as_string())

def today_tw():
    return datetime.now().strftime("%Y-%m-%d")

@app.get("/healthz")
def healthz():
    return Response("ok", status=200)

@app.post("/notify")
def notify():
    bucket = os.getenv("STATUS_BUCKET", "").strip()
    if not bucket:
        return Response("Missing STATUS_BUCKET", status=500)

    obj = f"status/{today_tw()}.json"
    client = storage.Client()
    blob = client.bucket(bucket).blob(obj)

    now_tw = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if not blob.exists(client):
        subject = "❌Daily Market Report 找不到更新摘要"
        body = f"Time(Taipei): {now_tw}\n\nMissing: gs://{bucket}/{obj}"
        send_mail(subject, body)
        return Response("status not found", status=500)

    summary = json.loads(blob.download_as_text())

    result = summary.get("result")
    if result == "success":
        subject = "✅Daily Market Report 更新完成"
    elif result == "skip":
        subject = "🟨Daily Market Report（略過：今日已更新）"
    else:
        subject = "❌Daily Market Report 更新失敗"

    body = f"Time(Taipei): {now_tw}\n\n" + json.dumps(summary, ensure_ascii=False, indent=2)
    send_mail(subject, body)
    return Response("ok", status=200)

