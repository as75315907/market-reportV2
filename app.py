import os
import subprocess
from flask import Flask, Response

app = Flask(__name__)

@app.get("/")
def health():
    return "ok", 200

@app.post("/run")
def run_job():
    # 跑你的主程式
    p = subprocess.run(
        ["python", "daily_market_report_to_gsheet.py"],
        capture_output=True,
        text=True,
        env=os.environ.copy(),
    )
    # 把 stdout/stderr 回傳，方便你在 Cloud Run log 看
    out = (p.stdout or "") + "\n" + (p.stderr or "")
    return Response(out, status=(200 if p.returncode == 0 else 500), mimetype="text/plain")
