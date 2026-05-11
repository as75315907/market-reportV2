import json
import os
import sys
from google.cloud import storage

from market_report.status_summary import build_notification_text, parse_run_output
from market_report.time_utils import timestamp_taipei, today_taipei

def main():
    if len(sys.argv) < 2:
        print("usage: python status_uploader.py /tmp/run.log", file=sys.stderr)
        sys.exit(2)

    bucket = os.getenv("STATUS_BUCKET", "").strip()
    if not bucket:
        print("[STATUS] STATUS_BUCKET empty -> skip upload")
        return

    log_path = sys.argv[1]
    with open(log_path, "r", encoding="utf-8", errors="replace") as f:
        text = f.read()

    summary = parse_run_output(text)
    time_taipei = timestamp_taipei()
    notification_text = build_notification_text(text, time_taipei=time_taipei)
    payload = {
        "time_taipei": time_taipei,
        "result": summary.result,
        "skipped": summary.skipped,
        "last_updated_today": summary.last_updated,
        "notification_text": notification_text,
        "log_tail": text[-4000:],
    }

    object_name = f"status/{today_taipei()}.json"

    client = storage.Client()
    client.bucket(bucket).blob(object_name).upload_from_string(
        json.dumps(payload, ensure_ascii=False, indent=2),
        content_type="application/json",
    )
    print(f"[STATUS] uploaded gs://{bucket}/{object_name}")

if __name__ == "__main__":
    main()
