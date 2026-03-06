import json
import os
from flask import Flask, Response
from google.cloud import storage

from market_report.mail import send_mail
from market_report.status_summary import email_subject_for_result
from market_report.time_utils import today_taipei, timestamp_taipei

app = Flask(__name__)

@app.get("/healthz")
def healthz():
    return Response("ok", status=200)

@app.post("/notify")
def notify():
    bucket = os.getenv("STATUS_BUCKET", "").strip()
    if not bucket:
        return Response("Missing STATUS_BUCKET", status=500)

    obj = f"status/{today_taipei()}.json"
    client = storage.Client()
    blob = client.bucket(bucket).blob(obj)

    if not blob.exists(client):
        subject = "❌Daily Market Report 找不到更新摘要"
        body = f"Time(Taipei): {timestamp_taipei()}\n\nMissing: gs://{bucket}/{obj}"
        send_mail(subject, body, strict=True)
        return Response("status not found", status=500)

    summary = json.loads(blob.download_as_text())
    subject = email_subject_for_result(summary.get("result", "fail"))
    body = f"Time(Taipei): {timestamp_taipei()}\n\n" + json.dumps(summary, ensure_ascii=False, indent=2)
    send_mail(subject, body, strict=True)
    return Response("ok", status=200)
